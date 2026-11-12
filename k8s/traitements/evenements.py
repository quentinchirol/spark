"""Traitement « evenements » : topic Kafka « events » → lake.bronze.events (Iceberg), sans perte ni doublon.

    spark-submit evenements.py bronze          (lancé par Dagster via lancer.sh, quand le retard dépasse le seuil)

Comme un flux Spark en mode availableNow : lit ce qui est disponible au démarrage, par lots bornés, puis s'arrête.
Position de reprise = les données elles-mêmes : chaque lot est écrit en UN commit Iceberg avec la partition et
l'offset Kafka de chaque message ; au démarrage, la lecture reprend après le plus grand offset déjà présent dans
bronze, partition par partition. Un arrêt brutal à n'importe quel moment ne perd ni ne double rien : un lot non
validé est relu en entier, un lot validé ne l'est jamais deux fois. Décision : docs/decisions/003-evenements-*.md
Après chaque lot, la position est aussi enregistrée dans le groupe Kafka « lakehouse-events » : uniquement pour
mesurer le retard (capteur Dagster, kafka-consumer-groups.sh), jamais pour reprendre.

Variables : KAFKA_BOOTSTRAP, TOPIC_EVENEMENTS (events), MAX_PAR_PARTITION (messages par partition et par lot).
"""
import json
import os
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
from pyspark.sql import SparkSession

TABLE = "lake.bronze.events"
TOPIC = os.environ.get("TOPIC_EVENEMENTS", "events")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "lakehouse-kafka-bootstrap:9092")
GROUPE = "lakehouse-events"
MAX_PAR_PARTITION = int(os.environ.get("MAX_PAR_PARTITION", "500000"))


def positions_dans_bronze(spark):
    """Prochain offset à lire par partition, d'après les messages déjà écrits dans bronze."""
    # partition et offset sont aussi des mots-clés SQL : entre accents graves
    lignes = spark.sql(f"SELECT `partition`, max(`offset`) + 1 FROM {TABLE} WHERE topic = '{TOPIC}' "
                       "GROUP BY `partition`")
    return {r[0]: r[1] for r in lignes.collect()}


def ecrire_lot(spark, debut, fin):
    """Lit les offsets [debut, fin) de chaque partition et les écrit en un seul commit Iceberg."""
    plage = lambda offsets: json.dumps({TOPIC: {str(p): o for p, o in offsets.items()}})  # noqa: E731
    lot = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", plage(debut))
           .option("endingOffsets", plage(fin))
           # un message absent (supprimé par la rétention avant d'être lu) arrête le traitement : perte signalée
           .option("failOnDataLoss", "true")
           .load())
    (lot.selectExpr("CAST(key AS string) AS cle", "CAST(value AS string) AS valeur", "topic", "partition", "offset",
                    "timestamp AS horodatage", "current_timestamp() AS _charge_le")
        .writeTo(TABLE).append())


def bronze(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        cle string, valeur string, topic string, `partition` int, `offset` bigint, horodatage timestamp,
        _charge_le timestamp) USING iceberg PARTITIONED BY (days(horodatage))""")
    consommateur = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=GROUPE, enable_auto_commit=False,
                                 request_timeout_ms=30000)
    partitions = [TopicPartition(TOPIC, p) for p in sorted(consommateur.partitions_for_topic(TOPIC) or [])]
    if not partitions:
        sys.exit(f"topic {TOPIC} introuvable")
    consommateur.assign(partitions)
    premiers = consommateur.beginning_offsets(partitions)
    # ce qui est disponible AU DÉMARRAGE : les messages arrivés ensuite attendent le run suivant
    derniers = {tp.partition: o for tp, o in consommateur.end_offsets(partitions).items()}
    deja = positions_dans_bronze(spark)
    # partition déjà chargée : reprise juste après son dernier message ; nouvelle partition : depuis le début
    position = {tp.partition: deja.get(tp.partition, premiers[tp]) for tp in partitions}
    perdus = {tp.partition: premiers[tp] - position[tp.partition] for tp in partitions
              if position[tp.partition] < premiers[tp]}
    if perdus:
        # messages supprimés par la rétention de Kafka avant d'avoir été chargés : perte réelle, signalée
        sys.exit(f"evenements : messages supprimés de Kafka avant leur chargement (partition: nombre) {perdus}")
    a_lire = sum(derniers[p] - position[p] for p in position)
    print(f"evenements : {a_lire} message(s) à charger, {len(partitions)} partition(s)", flush=True)
    numero = 0
    while any(position[p] < derniers[p] for p in position):
        fin = {p: min(derniers[p], position[p] + MAX_PAR_PARTITION) for p in position}
        numero += 1
        debut_lot = datetime.now(timezone.utc)
        ecrire_lot(spark, position, fin)
        nb = sum(fin[p] - position[p] for p in position)
        position = fin
        print(f"evenements : lot {numero} écrit ({nb} messages, "
              f"{(datetime.now(timezone.utc) - debut_lot).total_seconds():.0f} s)", flush=True)
        # suivi du retard seulement (la reprise se fait d'après bronze) : un échec ici n'annule pas le lot
        try:
            consommateur.commit({TopicPartition(TOPIC, p): OffsetAndMetadata(o, "", -1) for p, o in position.items()})
        except Exception as erreur:  # noqa: BLE001
            print(f"evenements : position non enregistrée dans Kafka ({erreur})", flush=True)
    consommateur.close()
    print(f"evenements : terminé, {numero} lot(s)", flush=True)


if __name__ == "__main__":
    etape = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    {"bronze": bronze}[etape](spark)
    spark.stop()
