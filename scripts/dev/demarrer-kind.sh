#!/usr/bin/env bash
# Démarre toute la stack dans un cluster kind local (Codespaces ou poste avec Docker).
#   scripts/dev/demarrer-kind.sh
# Variables : CLUSTER (défaut lakehouse)
set -euo pipefail
cd "$(dirname "$0")/../.."
CLUSTER=${CLUSTER:-lakehouse}

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "==> Création du cluster kind « $CLUSTER »"
  kind create cluster --name "$CLUSTER" --config scripts/dev/kind.yaml
fi

echo "==> Construction des images"
docker build -t spark-lakehouse:4.1.3 image
docker build -t lakehouse-pipeline:1.0 python
docker build -t postgres-lake:18 image/postgres-lake

echo "==> Chargement des images dans le cluster (pullPolicy: Never)"
kind load docker-image --name "$CLUSTER" spark-lakehouse:4.1.3 lakehouse-pipeline:1.0 postgres-lake:18

if [ -x scripts/generer-secrets.sh ]; then
  echo "==> Secrets locaux (générés seulement s'ils manquent)"
  scripts/generer-secrets.sh --si-absents
fi

echo "==> Opérateurs (leurs CRD doivent exister avant le reste)"
kubectl apply --server-side -k k8s/operateurs
kubectl wait --for=condition=established crd/postgresqls.acid.zalan.do --timeout=2m

echo "==> Déploiement"
kubectl apply -k k8s
kubectl -n spark rollout status deploy/spark-connect --timeout=15m

cat <<'EOF'

Stack démarrée. Dans deux terminaux :
  kubectl -n spark port-forward svc/spark-connect 15002:15002
  kubectl -n spark port-forward svc/trino 8443:8443
Puis : cd python && pytest -v
EOF
