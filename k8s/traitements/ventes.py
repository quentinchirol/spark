"""Traitement « ventes » : fichiers CSV → bronze → silver → gold (tables Iceberg, catalogue Polaris).

    spark-submit ventes.py bronze|silver|gold          (lancé par Dagster via lancer.sh)

Chaque étape est un commit Iceberg atomique et peut être relancée sans doublon ni donnée partielle :
  bronze : chaque CSV de s3://sources/ventes/ chargé tel quel (texte) une seule fois (MERGE sur fichier + ligne) ;
  silver : types, lignes invalides écartées, une ligne par vente (la plus récente), MERGE sur l'id ;
  gold   : ventes par jour, table remplacée en une fois (createOrReplace).
Fichiers lus par le pilote (pyarrow) : adapté à des fichiers de quelques centaines de Mo au plus par exécution.
Variable ECHEC=gold : fait échouer l'étape gold PENDANT l'écriture (test : aucune donnée partielle).
"""
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

COLONNES = ["id", "date", "client", "produit", "quantite", "prix_unitaire"]
BRONZE, SILVER, GOLD = "lake.bronze.ventes", "lake.silver.ventes", "lake.gold.ventes_par_jour"
SOURCES = os.environ.get("SOURCES_VENTES", "sources/ventes")


def table_existe(spark, nom):
    return spark.catalog.tableExists(nom)


def bronze(spark):
    import pyarrow.csv as pacsv
    from pyarrow import fs

    s3 = fs.S3FileSystem(endpoint_override=os.environ.get("S3_ENDPOINT", "minio:9000"), scheme="http",
                         access_key=os.environ["AWS_ACCESS_KEY_ID"], secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                         region="us-east-1")
    fichiers = sorted(f.path for f in s3.get_file_info(fs.FileSelector(SOURCES, recursive=True, allow_not_found=True))
                      if f.type == fs.FileType.File and f.path.endswith(".csv"))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {BRONZE} (
        id string, date string, client string, produit string, quantite string, prix_unitaire string,
        _fichier string, _ligne int, _charge_le timestamp) USING iceberg PARTITIONED BY (days(_charge_le))""")
    deja = {r[0] for r in spark.sql(f"SELECT DISTINCT _fichier FROM {BRONZE}").collect()}
    nouveaux = [f for f in fichiers if f not in deja]
    print(f"bronze : {len(fichiers)} fichier(s) dans s3://{SOURCES}, {len(nouveaux)} nouveau(x)")
    if not nouveaux:
        return
    maintenant = datetime.now(timezone.utc)
    lignes = []
    options = pacsv.ConvertOptions(column_types={c: "string" for c in COLONNES})
    for chemin in nouveaux:
        with s3.open_input_stream(chemin) as flux:
            table = pacsv.read_csv(flux, convert_options=options)
        for numero, ligne in enumerate(table.select(COLONNES).to_pylist(), start=1):
            lignes.append({**ligne, "_fichier": chemin, "_ligne": numero, "_charge_le": maintenant})
    lot = spark.createDataFrame(lignes, spark.table(BRONZE).schema)
    lot.createOrReplaceTempView("lot_bronze")
    spark.sql(f"""MERGE INTO {BRONZE} t USING lot_bronze s
                  ON t._fichier = s._fichier AND t._ligne = s._ligne
                  WHEN NOT MATCHED THEN INSERT *""")
    print(f"bronze : {len(lignes)} ligne(s) chargée(s)")


def silver(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {SILVER} (
        id string, date date, client string, produit string, quantite int, prix_unitaire decimal(12,2),
        _charge_le timestamp) USING iceberg PARTITIONED BY (months(date))""")
    brut = spark.table(BRONZE)
    typees = brut.select(
        F.trim("id").alias("id"),
        F.col("date").try_cast("date").alias("date"),
        F.trim("client").alias("client"),
        F.trim("produit").alias("produit"),
        F.col("quantite").try_cast("int").alias("quantite"),
        F.col("prix_unitaire").try_cast("decimal(12,2)").alias("prix_unitaire"),
        "_charge_le", "_fichier", "_ligne")
    valides = typees.where("id IS NOT NULL AND id <> '' AND date IS NOT NULL AND quantite > 0 AND prix_unitaire >= 0")
    print(f"silver : {typees.count() - valides.count()} ligne(s) invalide(s) écartée(s)")
    derniere = Window.partitionBy("id").orderBy(F.desc("_charge_le"), F.desc("_fichier"), F.desc("_ligne"))
    uniques = (valides.withColumn("_rang", F.row_number().over(derniere)).where("_rang = 1")
               .drop("_rang", "_fichier", "_ligne"))
    uniques.createOrReplaceTempView("lot_silver")
    spark.sql(f"""MERGE INTO {SILVER} t USING lot_silver s ON t.id = s.id
                  WHEN MATCHED AND s._charge_le > t._charge_le THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")


def gold(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    agregat = (spark.table(SILVER).groupBy("date")
               .agg(F.count("*").alias("nb_ventes"), F.sum("quantite").alias("quantite_totale"),
                    F.sum(F.col("quantite") * F.col("prix_unitaire")).cast("decimal(18,2)").alias("chiffre_affaires")))
    if os.environ.get("ECHEC") == "gold":
        # échec provoqué PENDANT l'écriture (dans les tâches Spark) : le commit Iceberg n'a jamais lieu
        agregat = agregat.where(F.assert_true(F.lit(False), "échec provoqué (test)").isNull())
    agregat.writeTo(GOLD).using("iceberg").createOrReplace()
    print(f"gold : {spark.table(GOLD).count()} jour(s)")


if __name__ == "__main__":
    etape = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    # journal du pilote lisible dans Dagster : avertissements et erreurs de Spark, et les lignes de ce script
    spark.sparkContext.setLogLevel("WARN")
    {"bronze": bronze, "silver": silver, "gold": gold}[etape](spark)
    spark.stop()
