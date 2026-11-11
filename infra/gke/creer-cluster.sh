#!/usr/bin/env bash
# Crée le cluster GKE régional (3 zones) et le groupe de nœuds dédié à l'analytique.
#
#   PROJET=mon-projet-gcp infra/gke/creer-cluster.sh
#
# Variables (valeurs par défaut entre parenthèses) :
#   PROJET            id du projet GCP (obligatoire)
#   REGION            région à 3 zones (europe-west9 = Paris)
#   CLUSTER           nom du cluster (lakehouse)
#   MACHINE           type de machine du groupe général (e2-standard-4 : 4 vCPU, 16 Go)
#   MACHINE_ANALYTIQUE type de machine du groupe analytique (n2-highmem-4 : 4 vCPU, 32 Go)
#   ANALYTIQUE_MIN / ANALYTIQUE_MAX  nœuds analytiques PAR ZONE (0 / 2) : à 0, le groupe ne coûte rien
#                     tant qu'aucun pod du pool analytique ne le demande
#
# Le script est idempotent : il ne recrée pas ce qui existe déjà.
# Supprimer le cluster : infra/gke/supprimer-cluster.sh
set -euo pipefail

PROJET=${PROJET:?Définir PROJET (id du projet GCP)}
REGION=${REGION:-europe-west9}
CLUSTER=${CLUSTER:-lakehouse}
MACHINE=${MACHINE:-e2-standard-4}
MACHINE_ANALYTIQUE=${MACHINE_ANALYTIQUE:-n2-highmem-4}
ANALYTIQUE_MIN=${ANALYTIQUE_MIN:-0}
ANALYTIQUE_MAX=${ANALYTIQUE_MAX:-2}

gcloud config set project "$PROJET" >/dev/null
gcloud services enable container.googleapis.com

if ! gcloud container clusters describe "$CLUSTER" --region "$REGION" >/dev/null 2>&1; then
  echo "==> Cluster régional $CLUSTER ($REGION, 3 zones)"
  # groupe général : 1 à 3 nœuds PAR ZONE, soit 3 à 9 nœuds
  gcloud container clusters create "$CLUSTER" \
    --region "$REGION" \
    --release-channel regular \
    --machine-type "$MACHINE" \
    --num-nodes 1 --enable-autoscaling --min-nodes 1 --max-nodes 3 \
    --node-labels role=general \
    --workload-pool "$PROJET.svc.id.goog" \
    --enable-ip-alias \
    --enable-shielded-nodes
fi

if ! gcloud container node-pools describe analytique --cluster "$CLUSTER" --region "$REGION" >/dev/null 2>&1; then
  echo "==> Groupe de nœuds « analytique » (isolé, vide quand il ne sert pas)"
  # la taint empêche tout autre pod de s'y installer : seul le pool analytique la tolère
  gcloud container node-pools create analytique \
    --cluster "$CLUSTER" --region "$REGION" \
    --machine-type "$MACHINE_ANALYTIQUE" \
    --num-nodes "$ANALYTIQUE_MIN" --enable-autoscaling --min-nodes "$ANALYTIQUE_MIN" --max-nodes "$ANALYTIQUE_MAX" \
    --node-labels role=analytique \
    --node-taints role=analytique:NoSchedule
fi

gcloud container clusters get-credentials "$CLUSTER" --region "$REGION"
echo "==> Groupes de nœuds"
gcloud container node-pools list --cluster "$CLUSTER" --region "$REGION"
echo "==> Nœuds (zone, rôle) : le groupe général doit apparaître dans 3 zones"
kubectl get nodes -L topology.kubernetes.io/zone,role
echo "==> Zones occupées : $(kubectl get nodes -o jsonpath='{.items[*].metadata.labels.topology\.kubernetes\.io/zone}' | tr ' ' '\n' | sort -u | wc -l)/3"
echo "Suite : infra/gke/deployer.sh"
