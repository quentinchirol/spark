"""Génération déterministe des tables du benchmark, directement sur le cluster,
et écriture de ces tables au format Iceberg."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning as P

CATALOGUE = "lake"
NB_BUCKETS = 16
TAILLE_BLOC_PARQUET = 4 * 1024 * 1024  # blocs plus petits = filtres par date plus sélectifs

PAYS = ["France", "Allemagne", "Italie", "Espagne", "Belgique", "Suisse", "Portugal", "Pays-Bas"]
CATEGORIES = ["livre", "jeu", "musique", "film", "sport", "maison", "jardin", "mode", "beaute", "auto"]
NB_PRODUITS = 1_000
LIGNES_PAR_PARTITION = 250_000


def _choix(valeurs: list[str], index) -> F.Column:
    return F.element_at(F.array(*[F.lit(v) for v in valeurs]), (index % len(valeurs) + 1).cast("int"))


def generer(spark: SparkSession, nb_ventes: int) -> dict[str, DataFrame]:
    """Tables ventes / clients / produits ; 1 client pour 100 ventes (100 minimum)."""
    nb_clients = max(nb_ventes // 100, 100)
    partitions = max(2, nb_ventes // LIGNES_PAR_PARTITION)

    clients = spark.range(nb_clients, numPartitions=max(2, partitions // 10)).select(
        F.col("id").alias("id_client"),
        F.concat(F.lit("client_"), F.col("id")).alias("nom"),
        _choix(PAYS, F.col("id")).alias("pays"),
    )
    produits = spark.range(NB_PRODUITS, numPartitions=1).select(
        F.col("id").alias("id_produit"),
        _choix(CATEGORIES, F.col("id")).alias("categorie"),
        (F.col("id") % 200 + 5).cast("double").alias("prix"),
    )
    ventes = spark.range(nb_ventes, numPartitions=partitions).select(
        F.col("id").alias("id_vente"),
        F.pmod(F.hash("id"), F.lit(nb_clients)).cast("long").alias("id_client"),
        F.pmod(F.hash("id", F.lit(1)), F.lit(NB_PRODUITS)).cast("long").alias("id_produit"),
        (F.col("id") % 5 + 1).alias("quantite"),
        (F.pmod(F.hash("id", F.lit(2)), F.lit(50_000)) / 100.0).alias("montant"),
        F.date_add(F.lit("2026-01-01").cast("date"), (F.col("id") % 365).cast("int")).alias("date"),
    )
    return {"ventes": ventes, "clients": clients, "produits": produits}


def namespace_iceberg(nb_ventes: int) -> str:
    return f"{CATALOGUE}.bench_{nb_ventes}"


def tables_iceberg_existent(spark: SparkSession, namespace: str) -> bool:
    return all(spark.catalog.tableExists(f"{namespace}.{nom}") for nom in ("ventes", "clients", "produits"))


def ecrire_iceberg(spark: SparkSession, tables: dict[str, DataFrame], namespace: str) -> None:
    """Écrit les tables en Iceberg, organisées pour accélérer les requêtes :
    - ventes et clients découpées en NB_BUCKETS groupes par id_client : jointure sans redistribution ;
    - dans chaque groupe, ventes triées par date en petits blocs Parquet : un filtre sur la date
      saute les blocs hors période grâce aux statistiques min/max.

    Pas de partitionnement par mois en plus : combiné au découpage par client, il faudrait
    activer allowJoinKeysSubsetOfPartitionKeys, qui fausse les fonctions de fenêtre.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    tables["clients"].writeTo(f"{namespace}.clients")         .partitionedBy(P.bucket(NB_BUCKETS, "id_client")).createOrReplace()
    tables["produits"].writeTo(f"{namespace}.produits").createOrReplace()

    ventes = f"{namespace}.ventes"
    tables["ventes"].limit(0).writeTo(ventes)         .partitionedBy(P.bucket(NB_BUCKETS, "id_client"))         .tableProperty("write.parquet.row-group-size-bytes", str(TAILLE_BLOC_PARQUET))         .createOrReplace()
    spark.sql(f"ALTER TABLE {ventes} WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY date")
    tables["ventes"].writeTo(ventes).append()


def lire_iceberg(spark: SparkSession, namespace: str) -> dict[str, DataFrame]:
    return {nom: spark.table(f"{namespace}.{nom}") for nom in ("ventes", "clients", "produits")}


def supprimer_iceberg(spark: SparkSession, namespace: str) -> None:
    for nom in ("ventes", "clients", "produits"):
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.{nom} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}")
