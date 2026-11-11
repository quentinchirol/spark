"""Outils partagés par les tests d'intégration sur kind (secrets, kubectl, port-forward, catalogue Polaris)."""
import socket
import subprocess
import sys
import time
from pathlib import Path

from pyiceberg.catalog import load_catalog

SECRETS = Path(__file__).resolve().parents[2] / "k8s" / "secrets"
NS = "spark"
# Polaris renvoie aux clients l'adresse S3 du catalogue (http://minio:9000) : le nom « minio » doit
# pointer vers 127.0.0.1 sur la machine du test (echo '127.0.0.1 minio' | sudo tee -a /etc/hosts).
PORT_S3, PORT_POLARIS = 9000, 18181


def env(fichier):
    lignes = (SECRETS / fichier).read_text(encoding="utf-8").splitlines()
    return dict(ligne.split("=", 1) for ligne in lignes if "=" in ligne)


def kubectl(*args):
    return subprocess.run(["kubectl", "-n", NS, *args], check=True, text=True, capture_output=True).stdout


def tunnel(cible, local, distant):
    processus = subprocess.Popen(["kubectl", "-n", NS, "port-forward", cible, f"{local}:{distant}"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            socket.create_connection(("localhost", local), timeout=0.2).close()
            return processus
        except OSError:
            time.sleep(0.2)
    processus.terminate()
    sys.exit(f"port-forward vers {cible} impossible")


def verifier_hote_minio():
    try:
        adresse = socket.gethostbyname("minio")
    except OSError:
        adresse = None
    if adresse != "127.0.0.1":
        sys.exit("Le nom « minio » doit pointer vers 127.0.0.1 : echo '127.0.0.1 minio' | sudo tee -a /etc/hosts")


def catalogue(credential, acces_s3):
    return load_catalog("lake", **{
        "type": "rest",
        "uri": f"http://localhost:{PORT_POLARIS}/api/catalog",
        "warehouse": "lake",
        "credential": credential,
        "scope": "PRINCIPAL_ROLE:ALL",
        "oauth2-server-uri": f"http://localhost:{PORT_POLARIS}/api/catalog/v1/oauth/tokens",
        # pas d'identifiants temporaires (MinIO sans STS) : le client utilise sa propre clé S3
        "header.X-Iceberg-Access-Delegation": "",
        "s3.endpoint": f"http://minio:{PORT_S3}",
        "s3.access-key-id": acces_s3[0],
        "s3.secret-access-key": acces_s3[1],
        "s3.path-style-access": "true",
        "s3.connect-timeout": "30",
        "s3.request-timeout": "60",
        "s3.region": "us-east-1",
    })


def verifier(condition, message):
    print(("OK   " if condition else "ECHEC"), message, flush=True)
    if not condition:
        sys.exit(1)
