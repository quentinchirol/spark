#!/usr/bin/env bash
# Pilote d'un traitement Spark à la demande : exécuté dans le pod du Job Kubernetes créé par Dagster.
#   lancer.sh <script.py> <étape>      ex. lancer.sh ventes.py bronze
# Le pod est le pilote Spark (mode client) ; Spark crée ses executors et les supprime à la fin.
set -euo pipefail
conf=/tmp/spark-conf
mkdir -p "$conf"
cp /traitements/spark-defaults.conf "$conf/"
{
  echo "spark.driver.host                $(hostname -i | awk '{print $1}')"
  echo "spark.kubernetes.driver.pod.name $HOSTNAME"
  echo "spark.app.name                   ${1%.py}-${2:-}"
  echo "spark.kubernetes.container.image $SPARK_EXECUTOR_IMAGE"
  echo "spark.kubernetes.container.image.pullPolicy $SPARK_EXECUTOR_PULL_POLICY"
  echo "spark.sql.catalog.lake.credential $POLARIS_CREDENTIAL"
} >> "$conf/spark-defaults.conf"
export SPARK_CONF_DIR="$conf"
exec /opt/spark/bin/spark-submit "/traitements/$1" "${@:2}"
