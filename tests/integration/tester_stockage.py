"""Test d'intégration du stockage : MinIO (4 pods) + Polaris (2 pods), sur un cluster kind.

Vérifie les critères de la US-4 qui ne dépendent pas de Spark :
  1. une table Iceberg créée via Polaris se relit à l'identique (données dans MinIO) ;
  2. un pod MinIO arrêté : lecture ET écriture continuent ;
  3. le principal « trino » (lecture seule) lit la table mais ne peut ni écrire ni créer.

    echo '127.0.0.1 minio' | sudo tee -a /etc/hosts   (une fois, voir PORT_S3 ci-dessous)
    python tests/integration/tester_stockage.py        (cluster déjà déployé, secrets dans k8s/secrets)
Dépendances : pip install "pyiceberg[pyarrow]"
"""
import socket
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import ForbiddenError, NoSuchNamespaceError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

SECRETS = Path(__file__).resolve().parents[2] / "k8s" / "secrets"
NS = "spark"
# Polaris renvoie aux clients l'adresse S3 du catalogue (http://minio:9000) : le nom « minio » doit donc
# pointer vers 127.0.0.1 sur la machine du test (echo '127.0.0.1 minio' | sudo tee -a /etc/hosts).
PORT_S3, PORT_POLARIS = 9000, 18181


def env(fichier):
    lignes = (SECRETS / fichier).read_text(encoding="utf-8").splitlines()
    return dict(ligne.split("=", 1) for ligne in lignes if "=" in ligne)


def kubectl(*args, **options):
    return subprocess.run(["kubectl", "-n", NS, *args], check=True, text=True, capture_output=True, **options).stdout


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


def catalogue(credential, acces_s3):
    return load_catalog(
        "lake",
        **{
            "type": "rest",
            "uri": f"http://localhost:{PORT_POLARIS}/api/catalog",
            "warehouse": "lake",
            "credential": credential,
            "scope": "PRINCIPAL_ROLE:ALL",
            "oauth2-server-uri": f"http://localhost:{PORT_POLARIS}/api/catalog/v1/oauth/tokens",
            # pas d'identifiants temporaires (MinIO sans STS) : comme Spark, le client utilise sa propre clé S3
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": f"http://minio:{PORT_S3}",
            "s3.access-key-id": acces_s3[0],
            "s3.secret-access-key": acces_s3[1],
            "s3.path-style-access": "true",
            # le client Arrow abandonne par défaut une requête qui met plus de 3 s à répondre :
            # trop court pendant que MinIO écarte un pod disparu
            "s3.connect-timeout": "30",
            "s3.request-timeout": "60",
            "s3.region": "us-east-1",
        },
    )


def lignes(debut, fin):
    return pa.table({"id": pa.array(range(debut, fin), pa.int64()),
                     "valeur": pa.array([f"v{i}" for i in range(debut, fin)], pa.string())})


def avec_reprises(action, delai_max=90):
    """Exécute action() en réessayant : après la perte d'un pod, MinIO met quelques secondes à écarter
    le pair disparu (les requêtes lentes expirent côté client). Affiche la durée de bascule observée."""
    debut = time.monotonic()
    while True:
        try:
            resultat = action()
            print(f"     (réussi après {time.monotonic() - debut:.0f} s)")
            return resultat
        except Exception as erreur:  # noqa: BLE001
            if time.monotonic() - debut > delai_max:
                raise
            print(f"     nouvel essai ({type(erreur).__name__})")
            time.sleep(3)


def diagnostic():
    """Santé de MinIO vue depuis minio-0, pour comprendre un échec en mode dégradé."""
    import urllib.request
    for chemin in ("/minio/health/live", "/minio/health/cluster/read", "/minio/health/cluster"):
        debut = time.monotonic()
        try:
            with urllib.request.urlopen(f"http://localhost:{PORT_S3}{chemin}", timeout=60) as reponse:
                etat = reponse.status
        except Exception as erreur:  # noqa: BLE001
            etat = type(erreur).__name__ + " " + str(getattr(erreur, "code", ""))
        print(f"DIAG {chemin} -> {etat} en {time.monotonic() - debut:.1f} s")
    sortie = subprocess.run(["kubectl", "-n", NS, "logs", "minio-0", "--tail=40"], text=True, capture_output=True).stdout
    print("DIAG journal minio-0 :\n" + sortie)


def verifier(condition, message):
    print(("OK   " if condition else "ECHEC"), message)
    if not condition:
        sys.exit(1)


def main():
    try:
        adresse = socket.gethostbyname("minio")
    except OSError:
        adresse = None
    if adresse != "127.0.0.1":
        sys.exit("Le nom « minio » doit pointer vers 127.0.0.1 : echo '127.0.0.1 minio' | sudo tee -a /etc/hosts")
    s3_spark, s3_trino = env("s3.env"), env("s3-trino.env")
    cle_spark = (s3_spark["AWS_ACCESS_KEY_ID"], s3_spark["AWS_SECRET_ACCESS_KEY"])
    cle_trino = (s3_trino["TRINO_S3_ACCESS_KEY"], s3_trino["TRINO_S3_SECRET_KEY"])
    # le tunnel S3 vise minio-0 (jamais arrêté ci-dessous) ; Polaris a 2 pods derrière le Service
    tunnels = [tunnel("pod/minio-0", PORT_S3, 9000), tunnel("svc/polaris", PORT_POLARIS, 8181)]
    try:
        spark = catalogue(env("polaris-spark.env")["POLARIS_CREDENTIAL"], cle_spark)
        trino = catalogue(env("polaris-trino.env")["POLARIS_CREDENTIAL"], cle_trino)

        try:
            spark.create_namespace("ci")
        except Exception:  # noqa: BLE001 : existe déjà lors d'un second passage
            pass
        nom = f"ci.table_{int(time.time())}"
        table = spark.create_table(nom, Schema(NestedField(1, "id", LongType(), required=False),
                                               NestedField(2, "valeur", StringType(), required=False)))
        table.append(lignes(0, 1000))
        verifier(spark.load_table(nom).scan().to_arrow().num_rows == 1000, "table créée via Polaris, 1000 lignes relues depuis MinIO")

        # --- 1 pod MinIO arrêté (minio-3 : le plus grand numéro d'un StatefulSet) -----------------
        kubectl("scale", "sts/minio", "--replicas=3")
        kubectl("wait", "--for=delete", "pod/minio-3", "--timeout=120s")
        verifier(avec_reprises(lambda: spark.load_table(nom).scan().to_arrow().num_rows) == 1000, "pod minio-3 arrêté : lecture OK")
        avec_reprises(lambda: spark.load_table(nom).append(lignes(1000, 1500)))
        verifier(spark.load_table(nom).scan().to_arrow().num_rows == 1500, "pod minio-3 arrêté : écriture OK (1500 lignes)")

        # --- principal trino : lecture seule ------------------------------------------------------
        lu = trino.load_table(nom).scan().to_arrow().num_rows
        verifier(lu == 1500, "principal trino : lecture OK")
        try:
            trino.load_table(nom).append(lignes(2000, 2010))
            verifier(False, "principal trino : écriture refusée")
        except Exception as erreur:  # noqa: BLE001 : refus Polaris (403) ou MinIO (AccessDenied)
            verifier(True, f"principal trino : écriture refusée ({type(erreur).__name__})")
        try:
            trino.create_table("ci.interdite", Schema(NestedField(1, "id", LongType(), required=False)))
            verifier(False, "principal trino : création de table refusée")
        except (ForbiddenError, NoSuchNamespaceError) as erreur:
            verifier(True, f"principal trino : création de table refusée ({type(erreur).__name__})")
        verifier(spark.load_table(nom).scan().to_arrow().num_rows == 1500, "aucune écriture de trino n'a abouti")
    except BaseException:
        diagnostic()
        raise
    finally:
        subprocess.run(["kubectl", "-n", NS, "scale", "sts/minio", "--replicas=4"], check=False)
        for processus in tunnels:
            processus.terminate()
    print("Stockage : tous les contrôles passent")


main()
