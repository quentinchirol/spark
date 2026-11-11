"""Initialise Polaris : catalogue « lake » sur MinIO, rôles, principaux spark, trino et postgres.

Exécuté par le Job polaris-init. Idempotent : une ressource déjà présente (HTTP 409) est conservée,
et les identifiants des principaux sont remis aux valeurs des Secrets à chaque exécution.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://polaris:8181/api"
CATALOGUE = "lake"
BUCKET = "s3://warehouse/"
ENDPOINT_S3 = "http://minio:9000"


def appeler(methode, chemin, corps=None, jeton=None, formulaire=None):
    donnees, entetes = None, {}
    if formulaire is not None:
        donnees = urllib.parse.urlencode(formulaire).encode()
        entetes["Content-Type"] = "application/x-www-form-urlencoded"
    elif corps is not None:
        donnees = json.dumps(corps).encode()
        entetes["Content-Type"] = "application/json"
    if jeton:
        entetes["Authorization"] = f"Bearer {jeton}"
    requete = urllib.request.Request(BASE + chemin, data=donnees, headers=entetes, method=methode)
    try:
        with urllib.request.urlopen(requete, timeout=30) as reponse:
            texte = reponse.read().decode()
            return reponse.status, (json.loads(texte) if texte else {})
    except urllib.error.HTTPError as erreur:
        texte = erreur.read().decode()
        try:
            return erreur.code, json.loads(texte)
        except ValueError:
            return erreur.code, {"erreur": texte}


def creer(chemin, corps, jeton, libelle):
    statut, reponse = appeler("POST", chemin, corps, jeton)
    if statut == 409:
        print(f"= {libelle} (existe déjà)")
    elif statut >= 300:
        sys.exit(f"échec {libelle} : HTTP {statut} {reponse}")
    else:
        print(f"+ {libelle}")


def accorder(chemin, privileges, jeton, libelle):
    for privilege in privileges:
        statut, reponse = appeler("PUT", chemin, {"grant": {"type": "catalog", "privilege": privilege}}, jeton)
        if statut >= 300:
            sys.exit(f"échec {libelle} {privilege} : HTTP {statut} {reponse}")
    print(f"+ {libelle} : {', '.join(privileges)}")


def main():
    # Polaris peut mettre quelques secondes à répondre après son démarrage
    jeton = None
    for _ in range(60):
        statut, reponse = appeler(
            "POST", "/catalog/v1/oauth/tokens",
            formulaire={
                "grant_type": "client_credentials",
                "client_id": os.environ["POLARIS_ROOT_CLIENT_ID"],
                "client_secret": os.environ["POLARIS_ROOT_CLIENT_SECRET"],
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        )
        if statut == 200:
            jeton = reponse["access_token"]
            break
        print(f"Polaris pas prêt (HTTP {statut}), nouvel essai")
        time.sleep(3)
    if not jeton:
        sys.exit("impossible d'obtenir un jeton root")

    creer("/management/v1/catalogs", {"catalog": {
        "name": CATALOGUE, "type": "INTERNAL", "readOnly": False,
        "properties": {"default-base-location": BUCKET},
        "storageConfigInfo": {
            "storageType": "S3", "allowedLocations": [BUCKET], "region": "us-east-1",
            "endpoint": ENDPOINT_S3, "endpointInternal": ENDPOINT_S3,
            "pathStyleAccess": True, "stsUnavailable": True,
        },
    }}, jeton, f"catalogue {CATALOGUE}")

    # rôles de catalogue : écriture (tout le contenu) et lecture seule
    racine = f"/management/v1/catalogs/{CATALOGUE}/catalog-roles"
    creer(racine, {"catalogRole": {"name": "ecriture"}}, jeton, "rôle de catalogue ecriture")
    creer(racine, {"catalogRole": {"name": "lecture"}}, jeton, "rôle de catalogue lecture")
    accorder(f"{racine}/ecriture/grants", ["CATALOG_MANAGE_CONTENT"], jeton, "ecriture")
    accorder(f"{racine}/lecture/grants", [
        "NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES",
        "TABLE_READ_DATA", "VIEW_LIST", "VIEW_READ_PROPERTIES",
    ], jeton, "lecture")

    # principaux : un par moteur, avec des identifiants fixés par les Secrets Kubernetes
    for nom, variable, role in (("spark", "SPARK_CREDENTIAL", "ecriture"), ("trino", "TRINO_CREDENTIAL", "lecture"),
                                ("postgres", "POSTGRES_CREDENTIAL", "ecriture")):
        identifiant, secret = os.environ[variable].split(":", 1)
        creer("/management/v1/principals", {"principal": {"name": nom}}, jeton, f"principal {nom}")
        statut, reponse = appeler("POST", f"/management/v1/principals/{nom}/reset",
                                  {"clientId": identifiant, "clientSecret": secret}, jeton)
        if statut >= 300:
            sys.exit(f"échec identifiants du principal {nom} : HTTP {statut} {reponse}")
        print(f"+ identifiants du principal {nom} alignés sur le Secret")
        creer("/management/v1/principal-roles", {"principalRole": {"name": nom}}, jeton, f"rôle de principal {nom}")
        statut, reponse = appeler("PUT", f"/management/v1/principals/{nom}/principal-roles",
                                  {"principalRole": {"name": nom}}, jeton)
        if statut >= 300:
            sys.exit(f"échec rôle de principal {nom} : HTTP {statut} {reponse}")
        statut, reponse = appeler("PUT", f"/management/v1/principal-roles/{nom}/catalog-roles/{CATALOGUE}",
                                  {"catalogRole": {"name": role}}, jeton)
        if statut >= 300:
            sys.exit(f"échec liaison {nom} → {role} : HTTP {statut} {reponse}")
        print(f"+ {nom} → rôle de catalogue {role}")
    print("Polaris initialisé")


main()
