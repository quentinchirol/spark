#!/usr/bin/env bash
# Supprime le cluster GKE créé par infra/gke/creer-cluster.sh (mêmes variables).
# Les disques des PersistentVolumeClaims sont supprimés avec lui : sauvegarder avant.
set -euo pipefail

PROJET=${PROJET:?Définir PROJET (id du projet GCP)}
REGION=${REGION:-europe-west9}
CLUSTER=${CLUSTER:-lakehouse}

gcloud config set project "$PROJET" >/dev/null
gcloud container clusters delete "$CLUSTER" --region "$REGION"
