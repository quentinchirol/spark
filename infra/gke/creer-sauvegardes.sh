#!/usr/bin/env bash
# Stockage des sauvegardes PostgreSQL HORS du cluster GKE : un bucket GCS (service distinct du cluster,
# conservé si le cluster est supprimé), lu et écrit par WAL-G via l'API GCS compatible S3 (clé HMAC).
#
#   PROJET=mon-projet-gcp infra/gke/creer-sauvegardes.sh
#
# Écrit k8s/secrets/sauvegardes.env (jamais versionné) ; puis : infra/gke/deployer.sh
# Variables : REGION (europe-west9), BUCKET (<projet>-lake-sauvegardes)
set -euo pipefail
cd "$(dirname "$0")/../.."

PROJET=${PROJET:?Définir PROJET (id du projet GCP)}
REGION=${REGION:-europe-west9}
BUCKET=${BUCKET:-$PROJET-lake-sauvegardes}
COMPTE=lake-sauvegardes@$PROJET.iam.gserviceaccount.com

gcloud config set project "$PROJET" >/dev/null
if ! gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
  echo "==> Bucket gs://$BUCKET ($REGION), versionné"
  gcloud storage buckets create "gs://$BUCKET" --location "$REGION" --uniform-bucket-level-access
  gcloud storage buckets update "gs://$BUCKET" --versioning
fi

if ! gcloud iam service-accounts describe "$COMPTE" >/dev/null 2>&1; then
  echo "==> Compte de service $COMPTE (droits sur ce seul bucket)"
  gcloud iam service-accounts create lake-sauvegardes --display-name "Sauvegardes PostgreSQL lakehouse"
fi
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member "serviceAccount:$COMPTE" --role roles/storage.objectAdmin >/dev/null

echo "==> Clé HMAC (API compatible S3) écrite dans k8s/secrets/sauvegardes.env"
cle=$(gcloud storage hmac create "$COMPTE" --format='value(metadata.accessId,secret)')
acces=$(printf '%s' "$cle" | cut -f1); secret=$(printf '%s' "$cle" | cut -f2)
umask 077
cat > k8s/secrets/sauvegardes.env <<FIN
WAL_S3_BUCKET=$BUCKET
AWS_ENDPOINT=https://storage.googleapis.com
CLONE_AWS_ENDPOINT=https://storage.googleapis.com
AWS_ACCESS_KEY_ID=$acces
AWS_SECRET_ACCESS_KEY=$secret
CLONE_AWS_ACCESS_KEY_ID=$acces
CLONE_AWS_SECRET_ACCESS_KEY=$secret
FIN
unset cle acces secret
echo "Fait. Ancienne clé HMAC éventuelle : la désactiver (gcloud storage hmac list / update --deactivate)."
echo "Suite : infra/gke/deployer.sh"
