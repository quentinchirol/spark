#!/usr/bin/env bash
# Déploie la plateforme sur le cluster GKE courant (contexte kubectl posé par creer-cluster.sh).
#
#   GHCR_USER=<compte GitHub> GHCR_TOKEN=<jeton read:packages> infra/gke/deployer.sh
#   TAG=sha-1a2b3c4 infra/gke/deployer.sh     # version figée (étiquette publiée par le workflow Images)
#
# GHCR_TOKEN : jeton GitHub « classic » avec le seul droit read:packages. Nécessaire au premier
# déploiement (ou pour le renouveler) ; ensuite le secret ghcr-pull est déjà dans le cluster.
# Le jeton n'est jamais écrit sur disque ni passé en argument d'une commande.
set -euo pipefail
cd "$(dirname "$0")/../.."

TAG=${TAG:-develop}
NS=spark

echo "==> Contexte kubectl : $(kubectl config current-context)"
kubectl apply -f k8s/commun/namespace.yaml

if [ -n "${GHCR_TOKEN:-}" ]; then
  echo "==> Secret ghcr-pull (téléchargement des images privées de ghcr.io)"
  auth=$(printf '%s:%s' "${GHCR_USER:?Définir GHCR_USER (compte GitHub)}" "$GHCR_TOKEN" | base64 | tr -d '\n')
  printf '{"auths":{"ghcr.io":{"auth":"%s"}}}' "$auth" \
    | kubectl -n "$NS" create secret generic ghcr-pull \
        --type=kubernetes.io/dockerconfigjson \
        --from-file=.dockerconfigjson=/dev/stdin \
        --dry-run=client -o yaml \
    | kubectl apply -f -
  unset auth
elif ! kubectl -n "$NS" get secret ghcr-pull >/dev/null 2>&1; then
  echo "Secret ghcr-pull absent : relancer avec GHCR_USER et GHCR_TOKEN (jeton GitHub read:packages)." >&2
  exit 1
fi

echo "==> Secrets de la plateforme (générés seulement s'ils n'existent pas)"
scripts/generer-secrets.sh --si-absents
if ! grep -q '^AWS_ENDPOINT=https://storage.googleapis.com' k8s/secrets/sauvegardes.env; then
  echo "Sauvegardes PostgreSQL non configurées pour GKE : lancer d'abord PROJET=… infra/gke/creer-sauvegardes.sh" >&2
  exit 1
fi

echo "==> Opérateurs (leurs CRD doivent exister avant le reste)"
kubectl apply --server-side -k k8s/operateurs
kubectl wait --for=condition=established crd/postgresqls.acid.zalan.do crd/kafkas.kafka.strimzi.io crd/kafkanodepools.kafka.strimzi.io crd/kafkatopics.kafka.strimzi.io crd/scaledobjects.keda.sh --timeout=2m

echo "==> Manifestes (infra/gke/manifestes, images en version $TAG)"
# kustomization temporaire : fixe l'étiquette sans modifier les fichiers versionnés
travail=$(mktemp -d infra/gke/.deploiement-XXXXXX)
trap 'rm -rf "$travail"' EXIT
cat > "$travail/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../manifestes
images:
  - name: ghcr.io/quentinchirol/spark-lakehouse
    newTag: $TAG
  - name: ghcr.io/quentinchirol/lakehouse-pipeline
    newTag: $TAG
  - name: ghcr.io/quentinchirol/postgres-lake
    newTag: $TAG
EOF
# les Jobs d'initialisation sont relançables sans effet de bord, mais leur spécification est
# immutable : on les recrée à chaque déploiement. polaris-bootstrap n'est PAS recréé (l'outil
# refuse de réinitialiser une base déjà initialisée).
kubectl -n "$NS" delete job minio-init polaris-init --ignore-not-found
kubectl apply -k "$travail"

echo "==> Attente des composants"
kubectl -n "$NS" rollout status sts/minio --timeout=10m
kubectl -n "$NS" wait --for=condition=complete job/minio-init job/polaris-bootstrap job/polaris-init --timeout=10m
kubectl -n "$NS" wait --for=jsonpath='{.status.PostgresClusterStatus}'=Running postgresql/lake-principal --timeout=15m
for d in polaris spark-connect trino; do
  kubectl -n "$NS" rollout status "deploy/$d" --timeout=10m
done

echo "==> Vérification : aucune image construite à la main sur un nœud"
if kubectl -n "$NS" get pods -o jsonpath='{..imagePullPolicy}' | tr ' ' '\n' | grep -qx Never; then
  echo "Un pod utilise imagePullPolicy: Never (image locale) :" >&2
  kubectl -n "$NS" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{..imagePullPolicy}{"\n"}{end}' >&2
  exit 1
fi
kubectl -n "$NS" get pods -o custom-columns='POD:.metadata.name,NŒUD:.spec.nodeName,IMAGES:.spec.containers[*].image'
echo "Plateforme déployée (images $TAG)."
