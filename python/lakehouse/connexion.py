"""Connexion au serveur Spark Connect.

URL : SPARK_CONNECT_URL (défaut sc://localhost:15002)
Token : SPARK_CONNECT_TOKEN, sinon le premier token de k8s/secrets/tokens
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession

URL = os.environ.get("SPARK_CONNECT_URL", "sc://localhost:15002")
FICHIER_TOKENS = Path(__file__).resolve().parents[2] / "k8s" / "secrets" / "tokens"


def lire_tokens() -> dict[str, str]:
    if not FICHIER_TOKENS.exists():
        return {}
    lignes = FICHIER_TOKENS.read_text(encoding="utf-8").splitlines()
    return dict(l.strip().split("=", 1) for l in lignes if "=" in l and not l.startswith("#"))


def token_par_defaut() -> str:
    token = os.environ.get("SPARK_CONNECT_TOKEN") or next(iter(lire_tokens().values()), None)
    if not token:
        raise RuntimeError(f"Définissez SPARK_CONNECT_TOKEN (ou créez {FICHIER_TOKENS})")
    return token


def nouvelle_session(token: str | None = None, **params: str) -> SparkSession:
    options = "".join(f";{k}={v}" for k, v in params.items())
    if token:
        options = f";token={token}" + options
    return SparkSession.builder.remote(f"{URL}/{options}").create()


# --- Trino : même identité (nom + token), en HTTPS avec le certificat de k8s/secrets/ ---
TRINO_HOTE = os.environ.get("LAKEHOUSE_TRINO_HOTE", "localhost")
TRINO_PORT = int(os.environ.get("LAKEHOUSE_TRINO_PORT", "8443"))  # (TRINO_PORT est réservé par Kubernetes)
CERTIFICAT_TRINO = FICHIER_TOKENS.parent / "trino-cert.pem"


def connexion_trino(utilisateur: str, token: str):
    """Connexion DB-API à Trino (catalogue lake) au nom de `utilisateur`."""
    from trino.auth import BasicAuthentication
    from trino.dbapi import connect

    return connect(host=TRINO_HOTE, port=TRINO_PORT, http_scheme="https", verify=str(CERTIFICAT_TRINO),
                   user=utilisateur, auth=BasicAuthentication(utilisateur, token), catalog="lake")
