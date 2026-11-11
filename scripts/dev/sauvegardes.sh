#!/usr/bin/env bash
# Stockage des sauvegardes PostgreSQL HORS du cluster Kubernetes, pour kind et la CI : un conteneur MinIO
# « sauvegardes » sur le réseau Docker de kind (les pods le joignent par http://sauvegardes:9000).
# Supprimer le cluster kind ne le supprime pas : c'est ce que vérifie la restauration (docs/sauvegardes.md).
#   scripts/dev/sauvegardes.sh            démarre (idempotent)
#   scripts/dev/sauvegardes.sh --arreter  arrête et supprime le conteneur ET les sauvegardes
set -euo pipefail
cd "$(dirname "$0")/../.."
IMAGE_MINIO=pgsty/minio:RELEASE.2026-08-04T00-00-00Z
IMAGE_MC=pgsty/mc:latest

if [ "${1:-}" = "--arreter" ]; then
  docker rm -f sauvegardes >/dev/null 2>&1 || true
  docker volume rm lake-sauvegardes >/dev/null 2>&1 || true
  echo "Stockage des sauvegardes supprimé"
  exit 0
fi

[ -s k8s/secrets/sauvegardes.env ] || scripts/generer-secrets.sh --si-absents >/dev/null
set -a
# shellcheck disable=SC1091
. k8s/secrets/sauvegardes.env
set +a

docker network inspect kind >/dev/null 2>&1 || docker network create kind >/dev/null
if ! docker ps --format '{{.Names}}' | grep -qx sauvegardes; then
  docker rm -f sauvegardes >/dev/null 2>&1 || true
  docker run -d --name sauvegardes --network kind --restart unless-stopped \
    -v lake-sauvegardes:/data \
    -e MINIO_ROOT_USER="$AWS_ACCESS_KEY_ID" -e MINIO_ROOT_PASSWORD="$AWS_SECRET_ACCESS_KEY" \
    --entrypoint minio "$IMAGE_MINIO" server /data >/dev/null
fi

# bucket versionné : une sauvegarde supprimée ou écrasée par erreur reste récupérable
docker run --rm --network kind -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e WAL_S3_BUCKET \
  --entrypoint sh "$IMAGE_MC" -c '
    until mc alias set s http://sauvegardes:9000 "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null 2>&1; do sleep 1; done
    mc mb --ignore-existing "s/$WAL_S3_BUCKET" >/dev/null
    mc version enable "s/$WAL_S3_BUCKET" >/dev/null'
echo "Stockage des sauvegardes prêt : http://sauvegardes:9000/$WAL_S3_BUCKET (hors de Kubernetes)"
