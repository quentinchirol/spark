#!/usr/bin/env bash
# Génère les secrets locaux dans k8s/secrets/ (dossier JAMAIS versionné, cf. .gitignore).
#
#   scripts/generer-secrets.sh               crée seulement les fichiers manquants (défaut)
#   scripts/generer-secrets.sh --si-absents  idem (utilisé par la CI et scripts/dev/demarrer-kind.sh)
#   scripts/generer-secrets.sh --rotation    régénère TOUT (nouvelles clés, nouveaux tokens),
#                                            puis : kubectl apply -k k8s && redémarrer les déploiements
#
# Fichiers produits :
#   tokens          alice=…, bob=…, pipeline=…  (Spark Connect et Trino)
#   pipeline.env    SPARK_CONNECT_TOKEN = token de pipeline
#   s3.env          clé S3 lecture + écriture (Spark, Polaris) : utilisateur MinIO « spark »
#   s3-trino.env    clé S3 en lecture seule (Trino) : utilisateur MinIO « trino »
#   minio-root.env  compte administrateur de MinIO (création du bucket et des utilisateurs)
#   polaris.env     compte root de Polaris + mot de passe de sa base PostgreSQL
#   polaris-spark.env / polaris-trino.env   identifiants OAuth2 (client:secret) de Spark et de Trino dans Polaris
#   polaris-public.pem / polaris-private.pem   clés de signature des jetons Polaris (communes aux 2 pods)
#   trino.env       secret interne de Trino
#   trino-tls.pem   clé privée + certificat HTTPS de Trino ; trino-cert.pem : certificat seul (clients)
set -euo pipefail

MODE=${1:---si-absents}
case "$MODE" in
  --si-absents|--rotation) ;;
  *) echo "usage : $0 [--si-absents | --rotation]" >&2; exit 2 ;;
esac

DOSSIER="$(cd "$(dirname "$0")/.." && pwd)/k8s/secrets"
mkdir -p "$DOSSIER"
chmod 700 "$DOSSIER"
cd "$DOSSIER"
umask 077

hex() { openssl rand -hex "$1"; }

# écrit le fichier $1 avec la sortie de la commande suivante, sauf s'il existe déjà (--si-absents)
ecrire() {
  local fichier=$1; shift
  if [ "$MODE" = "--si-absents" ] && [ -s "$fichier" ]; then
    echo "  = $fichier (déjà présent)"
    return
  fi
  "$@" > "$fichier"
  echo "  + $fichier"
}

tokens() {
  printf 'alice=%s\nbob=%s\npipeline=%s\n' "$(hex 32)" "$(hex 32)" "$(hex 32)"
}
pipeline_env() {
  printf 'SPARK_CONNECT_TOKEN=%s\n' "$(grep '^pipeline=' tokens | cut -d= -f2-)"
}
s3_env() {
  printf 'AWS_ACCESS_KEY_ID=spark%s\nAWS_SECRET_ACCESS_KEY=%s\n' "$(hex 6)" "$(hex 24)"
}
s3_trino_env() {
  printf 'TRINO_S3_ACCESS_KEY=trino%s\nTRINO_S3_SECRET_KEY=%s\n' "$(hex 6)" "$(hex 24)"
}
trino_env() {
  printf 'TRINO_SHARED_SECRET=%s\n' "$(hex 32)"
}
minio_root_env() {
  printf 'MINIO_ROOT_USER=admin%s\nMINIO_ROOT_PASSWORD=%s\n' "$(hex 4)" "$(hex 24)"
}
polaris_env() {
  printf 'POLARIS_ROOT_CLIENT_ID=root%s\nPOLARIS_ROOT_CLIENT_SECRET=%s\nPOLARIS_DB_PASSWORD=%s\n' "$(hex 4)" "$(hex 24)" "$(hex 24)"
}
polaris_client_env() {
  printf 'POLARIS_CREDENTIAL=%s:%s\n' "$(hex 8)" "$(hex 24)"
}

echo "Secrets dans $DOSSIER ($MODE)"
ecrire tokens tokens
# le token du pipeline doit toujours correspondre à la ligne pipeline= de tokens
if [ "$MODE" = "--rotation" ] || [ ! -s pipeline.env ] || \
   [ "$(cut -d= -f2- pipeline.env)" != "$(grep '^pipeline=' tokens | cut -d= -f2-)" ]; then
  pipeline_env > pipeline.env && echo "  + pipeline.env"
else
  echo "  = pipeline.env (déjà présent)"
fi
ecrire s3.env s3_env
ecrire s3-trino.env s3_trino_env
ecrire trino.env trino_env
ecrire minio-root.env minio_root_env
ecrire polaris.env polaris_env
ecrire polaris-spark.env polaris_client_env
ecrire polaris-trino.env polaris_client_env

# clés de signature des jetons Polaris : les deux pods doivent partager la même paire
if [ "$MODE" = "--rotation" ] || [ ! -s polaris-private.pem ] || [ ! -s polaris-public.pem ]; then
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out polaris-private.pem 2>/dev/null
  openssl rsa -in polaris-private.pem -pubout -out polaris-public.pem 2>/dev/null
  chmod 644 polaris-private.pem polaris-public.pem   # lus par un utilisateur non root dans le pod
  echo "  + polaris-private.pem, polaris-public.pem"
else
  echo "  = polaris-private.pem, polaris-public.pem (déjà présents)"
fi

if [ "$MODE" = "--rotation" ] || [ ! -s trino-tls.pem ] || [ ! -s trino-cert.pem ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 825 -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,DNS:trino,DNS:trino.spark.svc,IP:127.0.0.1" \
    -keyout trino-key.pem -out trino-cert.pem 2>/dev/null
  cat trino-key.pem trino-cert.pem > trino-tls.pem
  rm -f trino-key.pem
  chmod 644 trino-cert.pem
  echo "  + trino-tls.pem, trino-cert.pem"
else
  echo "  = trino-tls.pem, trino-cert.pem (déjà présents)"
fi

if [ "$MODE" = "--rotation" ]; then
  cat <<'EOF'

Rotation faite. Pour l'appliquer :
  kubectl apply -k k8s
  kubectl -n spark rollout restart sts/minio deploy/polaris deploy/trino deploy/spark-connect
Les anciennes clés et anciens tokens ne fonctionnent plus. Redistribuez les nouveaux tokens.
EOF
fi
