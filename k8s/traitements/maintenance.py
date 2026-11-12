"""Maintenance des tables Iceberg écrites par Spark (bronze, silver, gold…), chaque nuit (lancée par Dagster).

    spark-submit maintenance.py tout

Pour chaque table du catalogue « lake », sauf celles des namespaces exclus (NAMESPACES_EXCLUS, par défaut « app » :
tables écrites par pg_lake, qui les entretient lui-même — décision dans docs/maintenance.md) :
  1. compaction : petits fichiers regroupés (rewrite_data_files, dès 2 fichiers par groupe), manifests regroupés ;
  2. snapshots : seuls les SNAPSHOTS_GARDES derniers sont gardés (10) ;
  3. fichiers orphelins (écrits par une écriture échouée, jamais référencés) supprimés s'ils ont plus de
     ORPHELINS_APRES_H heures (72 : jamais un fichier d'une écriture en cours).
Chaque opération est un commit Iceberg distinct ; une écriture concurrente n'est jamais perdue (au pire, la
compaction d'un groupe de fichiers est abandonnée et refaite la nuit suivante). Une table en échec n'empêche pas
les autres ; le traitement échoue à la fin s'il y en a eu (Dagster le relance).
"""
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

EXCLUS = {n.strip() for n in os.environ.get("NAMESPACES_EXCLUS", "app").split(",") if n.strip()}
GARDES = int(os.environ.get("SNAPSHOTS_GARDES", "10"))
ORPHELINS_APRES_H = float(os.environ.get("ORPHELINS_APRES_H", "72"))


def tables(spark):
    for (namespace,) in spark.sql("SHOW NAMESPACES IN lake").collect():
        if namespace in EXCLUS:
            print(f"maintenance : namespace {namespace} ignoré (entretenu par son moteur d'écriture)", flush=True)
            continue
        for ligne in spark.sql(f"SHOW TABLES IN lake.{namespace}").collect():
            yield f"{namespace}.{ligne.tableName}"


def nb_snapshots(spark, table):
    return spark.sql(f"SELECT count(*) FROM lake.{table}.snapshots").first()[0]


def entretenir(spark, table):
    compaction = spark.sql(f"""CALL lake.system.rewrite_data_files(table => '{table}',
        options => map('min-input-files', '2', 'partial-progress.enabled', 'true'))""").first()
    manifests = spark.sql(f"CALL lake.system.rewrite_manifests(table => '{table}')").first()
    maintenant = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    expiration = spark.sql(f"""CALL lake.system.expire_snapshots(table => '{table}',
        older_than => TIMESTAMP '{maintenant}', retain_last => {GARDES})""").first()
    # API Java directement (et non la procédure remove_orphan_files, qui refuse un délai de moins de 24 h) ;
    # listage par l'API S3 d'Iceberg (prefix listing) : pas de système de fichiers Hadoop pour s3:// dans l'image
    jvm = spark._jvm
    table_java = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, f"lake.{table}")
    limite = datetime.now(timezone.utc) - timedelta(hours=ORPHELINS_APRES_H)
    orphelins = (jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
                 .deleteOrphanFiles(table_java)
                 .olderThan(int(limite.timestamp() * 1000))
                 .usePrefixListing(True)
                 .execute())
    supprimes, iterateur = 0, orphelins.orphanFileLocations().iterator()
    while iterateur.hasNext():
        print(f"maintenance : {table} : orphelin supprimé {iterateur.next()}", flush=True)
        supprimes += 1
    print(f"maintenance : {table} : {compaction['rewritten_data_files_count']} fichier(s) → "
          f"{compaction['added_data_files_count']} ; manifests {manifests[0]} → {manifests[1]} ; "
          f"{expiration['deleted_manifest_lists_count']} snapshot(s) expiré(s), "
          f"{expiration['deleted_data_files_count']} fichier(s) de données libéré(s) ; "
          f"{nb_snapshots(spark, table)} snapshot(s) gardé(s) ; {supprimes} orphelin(s) supprimé(s)", flush=True)


def tout(spark):
    echecs = []
    for table in tables(spark):
        try:
            entretenir(spark, table)
        except Exception as erreur:  # noqa: BLE001 : une table en échec n'empêche pas les autres
            echecs.append(table)
            print(f"maintenance : {table} : ÉCHEC {type(erreur).__name__}: {str(erreur)[:500]}", flush=True)
    if echecs:
        sys.exit(f"maintenance : {len(echecs)} table(s) en échec : {', '.join(echecs)}")
    # résumé en dernière ligne : toujours dans la fin du journal recopiée dans Dagster
    print(f"maintenance : terminée (namespaces ignorés : {', '.join(sorted(EXCLUS)) or 'aucun'})", flush=True)


if __name__ == "__main__":
    etape = sys.argv[1]
    # heure des TIMESTAMP littéraux = UTC
    spark = SparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    {"tout": tout}[etape](spark)
    spark.stop()
