"""Tests du stockage S3 : la clé de Trino est en LECTURE SEULE, celle de Spark peut écrire.

Même si les règles de Trino (rules.json) étaient mal configurées, Trino ne pourrait écrire aucun
fichier. Le stockage n'est pas exposé hors du cluster : les tests passent par kubectl port-forward.
"""
import shutil
import socket
import subprocess
import time
import uuid
from pathlib import Path

import pytest

boto3 = pytest.importorskip("boto3")
from botocore.client import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

from lakehouse.connexion import FICHIER_TOKENS  # noqa: E402

SECRETS = FICHIER_TOKENS.parent
PORT_LOCAL = 18333
BUCKET = "warehouse"


def lire_env(fichier: Path) -> dict[str, str]:
    lignes = fichier.read_text(encoding="utf-8").splitlines()
    return dict(l.split("=", 1) for l in lignes if "=" in l)


@pytest.fixture(scope="module")
def s3_local():
    if not shutil.which("kubectl"):
        pytest.skip("kubectl indisponible")
    tunnel = subprocess.Popen(["kubectl", "-n", "spark", "port-forward", "svc/seaweedfs", f"{PORT_LOCAL}:8333"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                socket.create_connection(("localhost", PORT_LOCAL), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.2)
        yield f"http://localhost:{PORT_LOCAL}"
    finally:
        tunnel.terminate()


def client(endpoint, cle, secret):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=cle, aws_secret_access_key=secret,
                        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))


@pytest.fixture(scope="module")
def s3_spark(s3_local):
    env = lire_env(SECRETS / "s3.env")
    return client(s3_local, env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"])


@pytest.fixture(scope="module")
def s3_trino(s3_local):
    env = lire_env(SECRETS / "s3-trino.env")
    return client(s3_local, env["TRINO_S3_ACCESS_KEY"], env["TRINO_S3_SECRET_KEY"])


@pytest.fixture
def objet_test(s3_spark):
    """Un fichier de test écrit avec la clé de Spark, supprimé après le test."""
    cle = f"tests-droits/{uuid.uuid4().hex}.txt"
    s3_spark.put_object(Bucket=BUCKET, Key=cle, Body=b"contenu d'origine")
    yield cle
    s3_spark.delete_object(Bucket=BUCKET, Key=cle)


def refuse(action):
    with pytest.raises(ClientError) as erreur:
        action()
    assert erreur.value.response["Error"]["Code"] == "AccessDenied"


def test_trino_peut_lister_et_lire(s3_trino, objet_test):
    cles = [o["Key"] for o in s3_trino.list_objects_v2(Bucket=BUCKET, Prefix="tests-droits/")["Contents"]]
    assert objet_test in cles
    assert s3_trino.get_object(Bucket=BUCKET, Key=objet_test)["Body"].read() == b"contenu d'origine"


def test_trino_ne_peut_pas_creer_de_fichier(s3_trino, s3_spark):
    cle = f"tests-droits/{uuid.uuid4().hex}.txt"
    refuse(lambda: s3_trino.put_object(Bucket=BUCKET, Key=cle, Body=b"pirate"))
    with pytest.raises(ClientError):
        s3_spark.head_object(Bucket=BUCKET, Key=cle)  # le fichier n'existe pas


def test_trino_ne_peut_pas_modifier_ni_supprimer(s3_trino, s3_spark, objet_test):
    refuse(lambda: s3_trino.put_object(Bucket=BUCKET, Key=objet_test, Body=b"pirate"))
    refuse(lambda: s3_trino.delete_object(Bucket=BUCKET, Key=objet_test))
    assert s3_spark.get_object(Bucket=BUCKET, Key=objet_test)["Body"].read() == b"contenu d'origine"


def test_trino_ne_peut_pas_creer_de_bucket(s3_trino):
    refuse(lambda: s3_trino.create_bucket(Bucket=f"pirate-{uuid.uuid4().hex[:8]}"))


def test_spark_peut_ecrire(s3_spark, objet_test):
    s3_spark.put_object(Bucket=BUCKET, Key=objet_test, Body=b"nouveau contenu")
    assert s3_spark.get_object(Bucket=BUCKET, Key=objet_test)["Body"].read() == b"nouveau contenu"


def test_trino_n_a_pas_la_cle_d_ecriture():
    """Le pod Trino ne reçoit que la clé en lecture seule, jamais celle de Spark."""
    if not shutil.which("kubectl"):
        pytest.skip("kubectl indisponible")
    env = subprocess.run(["kubectl", "-n", "spark", "exec", "deploy/trino", "-c", "trino", "--", "env"],
                         capture_output=True, text=True, check=True).stdout
    cle_spark = lire_env(SECRETS / "s3.env")["AWS_SECRET_ACCESS_KEY"]
    assert cle_spark not in env
    assert "TRINO_S3_SECRET_KEY=" in env
