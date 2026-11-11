#!/usr/bin/env bash
# Pilote d'un traitement Spark à la demande : exécuté dans le pod du Job Kubernetes créé par Dagster.
#   lancer.sh <script.py> <étape>      ex. lancer.sh ventes.py bronze
# Le pod est le pilote Spark (mode client) ; Spark crée ses executors et les supprime à la fin.
set -euo pipefail
conf=/tmp/spark-conf
mkdir -p "$conf"
cp /traitements/spark-defaults.conf "$conf/"
# adresse IP du pod (les executors joignent le pilote par elle)
ip=$(python3 -c 'import socket; print(socket.gethostbyname(socket.gethostname()))')
{
  echo "spark.driver.host                $ip"
  echo "spark.kubernetes.driver.pod.name $HOSTNAME"
  echo "spark.app.name                   ${1%.py}-${2:-}"
  echo "spark.kubernetes.container.image $SPARK_EXECUTOR_IMAGE"
  echo "spark.kubernetes.container.image.pullPolicy $SPARK_EXECUTOR_PULL_POLICY"
  echo "spark.kubernetes.executor.request.cores ${SPARK_EXECUTOR_REQUEST_CORES:-500m}"
  echo "spark.sql.catalog.lake.credential $POLARIS_CREDENTIAL"
} >> "$conf/spark-defaults.conf"
export SPARK_CONF_DIR="$conf"
exec /opt/spark/bin/spark-submit "/traitements/$1" "${@:2}"
