#!/usr/bin/env bash
# Rejoue en local, sur kind, un job de .github/workflows/integration.yml : mêmes images, mêmes secrets générés,
# même déploiement, mêmes attentes, mêmes tests. Le cluster est GARDÉ à la fin pour relancer un seul test.
#
#   scripts/ci-locale.sh <stockage|postgres|ecriture|orchestration|evenements> [options]
#
#   --recreer      supprime le cluster kind et le recrée (obligatoire pour changer de job)
#   --sans-images  ne reconstruit pas les images (déjà chargées dans kind)
#   --sans-tests   s'arrête une fois tout déployé et prêt
#
# Puis, pour relancer un seul test sur le cluster gardé :
#   .venv-ci/bin/python tests/integration/tester_postgres.py        (Windows : .venv-ci/Scripts/python)
# Supprimer le cluster : scripts/dev/arreter-kind.sh   (sauvegardes hors cluster : scripts/dev/sauvegardes.sh --arreter)
#
# Prérequis : docker, kind, kubectl, python 3.11+, et le nom « minio » résolu en 127.0.0.1 (voir le README).
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=lakehouse
CONTEXTE="kind-$CLUSTER"
JOB=${1:-}
shift || true
RECREER=0 IMAGES=1 TESTS=1
for option in "$@"; do
  case "$option" in
    --recreer) RECREER=1 ;;
    --sans-images) IMAGES=0 ;;
    --sans-tests) TESTS=0 ;;
    *) echo "option inconnue : $option" >&2; exit 2 ;;
  esac
done
case "$JOB" in
  stockage|postgres|ecriture|orchestration|evenements) ;;
  *) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
esac

etape() { printf '\n==> %s\n' "$*"; }
debut=$(date +%s)

# --- Prérequis ----------------------------------------------------------------------------------------------------
for outil in docker kind kubectl; do
  command -v "$outil" >/dev/null || { echo "« $outil » est introuvable (voir le README, section CI en local)" >&2; exit 1; }
done
PYTHON=$(command -v python3 || command -v python || true)
[ -n "$PYTHON" ] || { echo "python 3.11+ est introuvable" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker ne répond pas : démarrez Docker Desktop (ou le démon)" >&2; exit 1; }
if [ "$JOB" != ecriture ] && [ "$("$PYTHON" -c 'import socket
try: print(socket.gethostbyname("minio"))
except OSError: pass')" != 127.0.0.1 ]; then
  echo "Le nom « minio » doit pointer vers 127.0.0.1 (Polaris renvoie aux clients l'adresse http://minio:9000) :" >&2
  echo "  Linux / macOS : echo '127.0.0.1 minio' | sudo tee -a /etc/hosts" >&2
  echo "  Windows (PowerShell administrateur) : Add-Content \$env:windir\\System32\\drivers\\etc\\hosts '127.0.0.1 minio'" >&2
  exit 1
fi

# --- Cluster ------------------------------------------------------------------------------------------------------
# Un cluster ne sert qu'à un job : les déploiements de tests/integration/<job> ne s'empilent pas proprement.
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  precedent=$(kubectl --context "$CONTEXTE" -n kube-system get configmap ci-locale -o jsonpath='{.data.job}' 2>/dev/null || true)
  if [ "$RECREER" = 1 ]; then
    etape "Suppression du cluster kind « $CLUSTER »"
    kind delete cluster --name "$CLUSTER"
  elif [ -n "$precedent" ] && [ "$precedent" != "$JOB" ]; then
    echo "Le cluster kind « $CLUSTER » sert déjà au job « $precedent » : relancez avec --recreer" >&2
    exit 1
  fi
fi
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  etape "Création du cluster kind « $CLUSTER »"
  kind create cluster --name "$CLUSTER" --config scripts/dev/kind.yaml
fi
# les tests appellent « kubectl » sans contexte : le contexte courant doit être celui de kind
kubectl config use-context "$CONTEXTE" >/dev/null
kubectl -n kube-system create configmap ci-locale --from-literal=job="$JOB" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "Contexte kubectl : $(kubectl config current-context)"

# --- Images -------------------------------------------------------------------------------------------------------
construire() {  # construire <tag> <contexte> [dockerfile]
  docker build -t "$1" ${3:+-f "$3"} "$2"
}
if [ "$IMAGES" = 1 ]; then
  case "$JOB" in
    postgres)
      etape "Image PostgreSQL + pg_lake"
      construire postgres-lake:18 image/postgres-lake
      images=(postgres-lake:18) ;;
    ecriture)
      etape "Images PostgreSQL + pg_lake, writers, passerelle"
      construire postgres-lake:18 image/postgres-lake
      construire lakehouse-ecrivain:1.0 python python/ecrivain/Dockerfile
      construire lakehouse-passerelle:1.0 python python/passerelle/Dockerfile
      images=(postgres-lake:18 lakehouse-ecrivain:1.0 lakehouse-passerelle:1.0) ;;
    orchestration|evenements)
      etape "Images Spark, PostgreSQL, Dagster"
      construire spark-lakehouse:4.1.3 image
      construire postgres-lake:18 image/postgres-lake
      construire lakehouse-dagster:1.0 python python/orchestration/Dockerfile
      images=(spark-lakehouse:4.1.3 postgres-lake:18 lakehouse-dagster:1.0) ;;
    *) images=() ;;
  esac
  if [ ${#images[@]} -gt 0 ]; then
    etape "Chargement des images dans kind"
    kind load docker-image "${images[@]}" --name "$CLUSTER"
  fi
fi

if [ "$JOB" = postgres ]; then
  etape "metrics-server (CPU des pods, pour l'autoscaling du pool analytique)"
  kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
  kubectl -n kube-system get deploy metrics-server -o jsonpath='{.spec.template.spec.containers[0].args}' | grep -q kubelet-insecure-tls \
    || kubectl -n kube-system patch deploy metrics-server --type=json \
         -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
fi

etape "Secrets de test (générés seulement s'ils manquent, jamais affichés)"
scripts/generer-secrets.sh --si-absents >/dev/null

if [ "$JOB" = postgres ] || [ "$JOB" = ecriture ]; then
  etape "Stockage des sauvegardes, hors de Kubernetes"
  scripts/dev/sauvegardes.sh
fi

CRD="crd/postgresqls.acid.zalan.do crd/kafkas.kafka.strimzi.io crd/kafkanodepools.kafka.strimzi.io crd/kafkatopics.kafka.strimzi.io crd/scaledobjects.keda.sh"
if [ "$JOB" != stockage ]; then
  etape "Opérateurs"
  kubectl apply --server-side -k tests/integration/operateurs
  # shellcheck disable=SC2086
  kubectl wait --for=condition=established $CRD --timeout=120s
  case "$JOB" in
    postgres|orchestration) kubectl -n spark rollout status deploy/postgres-operator --timeout=240s ;;
    ecriture)
      kubectl -n spark rollout status deploy/postgres-operator deploy/strimzi-cluster-operator --timeout=300s
      kubectl -n keda rollout status deploy/keda-operator deploy/keda-metrics-apiserver deploy/keda-admission --timeout=300s ;;
    evenements) kubectl -n spark rollout status deploy/postgres-operator deploy/strimzi-cluster-operator --timeout=300s ;;
  esac
fi

# --- Déploiement et attentes (identiques à la CI) ------------------------------------------------------------------
etape "Déploiement de tests/integration/$JOB"
kubectl apply -k "tests/integration/$JOB"

etape "Attente des composants"
k() { kubectl -n spark "$@"; }
case "$JOB" in
  stockage)
    k rollout status sts/minio --timeout=240s
    k wait --for=condition=complete job/minio-init job/polaris-bootstrap job/polaris-init --timeout=300s
    k rollout status deploy/polaris --timeout=240s ;;
  postgres)
    k rollout status sts/minio --timeout=240s
    k wait --for=condition=complete job/minio-init job/polaris-bootstrap job/polaris-init --timeout=300s
    k wait --for=jsonpath='{.status.PostgresClusterStatus}'=Running postgresql/lake-principal --timeout=600s
    k wait --for=condition=complete job/lake-schema --timeout=300s
    k rollout status deploy/lake-analytique --timeout=300s
    kubectl -n kube-system rollout status deploy/metrics-server --timeout=180s ;;
  ecriture)
    k wait --for=condition=complete job/minio-init job/polaris-bootstrap job/polaris-init --timeout=420s
    k wait --for=condition=Ready kafka/lakehouse --timeout=600s
    k wait --for=condition=Ready kafkatopic/app kafkatopic/app-rejets --timeout=300s
    k wait --for=jsonpath='{.status.PostgresClusterStatus}'=Running postgresql/lake-principal --timeout=600s
    k wait --for=condition=complete job/lake-schema --timeout=300s
    k rollout status deploy/ecrivain deploy/passerelle --timeout=300s ;;
  orchestration)
    k rollout status sts/minio --timeout=240s
    k wait --for=condition=complete job/minio-init job/polaris-bootstrap job/polaris-init --timeout=300s
    k wait --for=jsonpath='{.status.PostgresClusterStatus}'=Running postgresql/lake-outils --timeout=600s
    k rollout status deploy/dagster-code deploy/dagster-webserver deploy/dagster-daemon --timeout=300s ;;
  evenements)
    k rollout status sts/minio --timeout=240s
    k wait --for=condition=complete job/minio-init job/polaris-bootstrap job/polaris-init --timeout=420s
    k wait --for=condition=Ready kafka/lakehouse --timeout=600s
    k wait --for=condition=Ready kafkatopic/events --timeout=300s
    k wait --for=jsonpath='{.status.PostgresClusterStatus}'=Running postgresql/lake-outils --timeout=600s
    k rollout status deploy/dagster-code deploy/dagster-webserver deploy/dagster-daemon --timeout=300s ;;
esac
k get pods -o wide
echo "Déployé et prêt en $(( $(date +%s) - debut )) s"

if [ "$TESTS" = 0 ]; then
  echo "Tests non lancés (--sans-tests). Cluster gardé, contexte $CONTEXTE."
  exit 0
fi

# --- Environnement Python des tests (.venv-ci, non versionné) -----------------------------------------------------
etape "Environnement Python des tests (.venv-ci)"
[ -d .venv-ci ] || "$PYTHON" -m venv .venv-ci
if [ -x .venv-ci/Scripts/python.exe ]; then PY=.venv-ci/Scripts/python.exe; else PY=.venv-ci/bin/python; fi
case "$JOB" in
  stockage) dependances=("pyiceberg[pyarrow]") ;;
  postgres) dependances=("pyiceberg[pyarrow]" -r python/analyse/requirements.txt) ;;
  *) dependances=(httpx "pyiceberg[pyarrow]") ;;
esac
"$PY" -m pip install -q --disable-pip-version-check "${dependances[@]}"
# accents des messages des tests sur une console Windows
export PYTHONIOENCODING=utf-8 PYTHONUTF8=1

tester() {  # tester <description> <fichier>
  etape "$1"
  local t0; t0=$(date +%s)
  if ! "$PY" "tests/integration/$2"; then
    echo "ÉCHEC : tests/integration/$2 (cluster gardé ; diagnostic : kubectl -n spark get pods,jobs ; kubectl -n spark logs …)" >&2
    exit 1
  fi
  echo "-- $2 : $(( $(date +%s) - t0 )) s"
}

case "$JOB" in
  stockage) tester "Test d'intégration" tester_stockage.py ;;
  postgres)
    tester "Test d'intégration" tester_postgres.py
    tester "Accès temps réel (répliques uniquement, jamais le primaire)" tester_temps_reel.py
    tester "Pool analytique (lecture seule, Power BI / Qlik)" tester_analytique.py
    tester "Isolation analytique (file d'attente PgBouncer, coupures, autoscaling, latence de l'application)" tester_isolation.py
    tester "Sauvegardes et restauration à un instant donné" tester_restauration.py ;;
  ecriture)
    tester "Ni perte ni doublon (writer tué en plein flux), autoscaling KEDA" tester_ecritures.py
    etape "Test de charge k6 réduit (200 écritures/s, p99 < 500 ms)"
    primaire=$(k get pods -l cluster-name=lake-principal,spilo-role=master -o name)
    k exec "$primaire" -c postgres -- psql -U postgres -d lake -c "SELECT lake.creer_table('charge', 'client text, montant numeric(12,2)')"
    k port-forward svc/passerelle 18000:8000 >/dev/null 2>&1 &
    tunnel=$!
    trap 'kill $tunnel 2>/dev/null || true' EXIT
    sleep 3
    # Linux (CI) : réseau de l'hôte ; Docker Desktop (Windows, macOS) : l'hôte est joint par host.docker.internal
    if [ "$(uname -s)" = Linux ]; then reseau=(--network host); url=http://localhost:18000
    else reseau=(); url=http://host.docker.internal:18000; fi
    docker run --rm "${reseau[@]}" -i grafana/k6:1.0.0 run -e URL="$url" -e RATE=200 -e DUREE=30s - < tests/charge/ecritures.js ;;
  orchestration) tester "Test d'intégration" tester_orchestration.py ;;
  evenements) tester "Test d'intégration" tester_evenements.py ;;
esac

etape "Job « $JOB » vert en local en $(( $(date +%s) - debut )) s. Cluster gardé (contexte $CONTEXTE)."
echo "Relancer un seul test : $PY tests/integration/tester_<nom>.py"
