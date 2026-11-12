"""Synchronise une instance du pool analytique avec le catalogue Polaris (conteneur « synchro », chaque minute).

Chaque instance du pool est un PostgreSQL + pg_lake SANS données propres : toutes ses tables sont des tables
Iceberg du catalogue « lake » rattachées en lecture seule (read_only), qui lisent toujours la dernière version
validée. Ce script :
  1. prépare la base (extension, serveur Polaris, rôle « analyste » en lecture seule, limites) — idempotent ;
  2. rattache chaque table du catalogue (schéma PostgreSQL = namespace Iceberg), rattache de nouveau une table
     dont les colonnes ont changé, retire celles qui n'existent plus ;
  3. pour chaque table app.<t> du PostgreSQL principal : vue.<t> = ses lignes non supprimées (comme sur le principal,
     sans les écritures de la dernière minute, pas encore fusionnées) ;
  4. écrit /var/run/postgresql/synchro-ok (sonde de disponibilité) après une synchronisation complète.

Le PostgreSQL principal n'est jamais sollicité : tout passe par Polaris (catalogue) et MinIO (fichiers, clé S3 en
lecture seule). Variables : POLARIS_URL, POLARIS_CREDENTIAL (principal en lecture seule), ANALYTIQUE_MOT_DE_PASSE,
INTERVALLE_S, STATEMENT_TIMEOUT, WORK_MEM.
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

POLARIS = os.environ.get("POLARIS_URL", "http://polaris:8181/api/catalog").rstrip("/")
CATALOGUE = "lake"
INTERVALLE_S = int(os.environ.get("INTERVALLE_S", "60"))
SOCKET = "/var/run/postgresql"
PSQL = os.environ.get("PSQL", "/usr/lib/postgresql/18/bin/psql")
TEMOIN = Path(SOCKET) / "synchro-ok"
# schémas réservés de PostgreSQL / pg_lake : jamais utilisés pour un namespace
RESERVES = {"public", "pg_catalog", "information_schema", "lake_iceberg", "lake_engine", "lake_table", "vue"}


def litteral(valeur: str) -> str:
    return "'" + str(valeur).replace("'", "''") + "'"


def ident(nom: str) -> str:
    return '"' + nom.replace('"', '""') + '"'


def psql(sql: str, base: str = "lake") -> str:
    """Exécute du SQL (envoyé sur l'entrée standard : jamais de secret dans la ligne de commande)."""
    resultat = subprocess.run([PSQL, "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-h", SOCKET, "-U",
                               "postgres", "-d", base], input=sql, text=True, capture_output=True)
    if resultat.returncode != 0:
        raise RuntimeError(resultat.stderr.strip())
    return resultat.stdout.strip()


def polaris(chemin: str, jeton: str):
    requete = urllib.request.Request(f"{POLARIS}{chemin}", headers={"Authorization": f"Bearer {jeton}"})
    with urllib.request.urlopen(requete, timeout=30) as reponse:
        return json.load(reponse)


def jeton_polaris() -> str:
    client, secret = os.environ["POLARIS_CREDENTIAL"].split(":", 1)
    corps = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client,
                                    "client_secret": secret, "scope": "PRINCIPAL_ROLE:ALL"}).encode()
    with urllib.request.urlopen(f"{POLARIS}/v1/oauth/tokens", data=corps, timeout=30) as reponse:
        return json.load(reponse)["access_token"]


def catalogue() -> dict:
    """{(namespace, table): [colonnes]} pour toutes les tables du catalogue."""
    jeton = jeton_polaris()
    prefixe = polaris(f"/v1/config?warehouse={CATALOGUE}", jeton).get("overrides", {}).get("prefix", CATALOGUE)
    tables = {}
    for (namespace, *reste) in polaris(f"/v1/{prefixe}/namespaces", jeton)["namespaces"]:
        if reste or namespace in RESERVES:
            continue                      # namespaces imbriqués ou réservés : non rattachés
        for identifiant in polaris(f"/v1/{prefixe}/namespaces/{namespace}/tables", jeton).get("identifiers", []):
            nom = identifiant["name"]
            metadonnees = polaris(f"/v1/{prefixe}/namespaces/{namespace}/tables/{nom}", jeton)["metadata"]
            schema = next(s for s in metadonnees["schemas"] if s["schema-id"] == metadonnees["current-schema-id"])
            tables[(namespace, nom)] = [champ["name"] for champ in schema["fields"]]
    return tables


def preparer():
    client, secret = os.environ["POLARIS_CREDENTIAL"].split(":", 1)
    mot_de_passe = os.environ["ANALYTIQUE_MOT_DE_PASSE"]
    if psql("SELECT 1 FROM pg_database WHERE datname = 'lake'", base="postgres") != "1":
        psql("CREATE DATABASE lake", base="postgres")
    psql(f"""
        SET default_transaction_read_only = off;
        CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE;
        CREATE SERVER IF NOT EXISTS polaris TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint {litteral(POLARIS)}, rest_auth_type 'oauth2', catalog_name {litteral(CATALOGUE)});
        CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER polaris
            OPTIONS (client_id {litteral(client)}, client_secret {litteral(secret)});
        ALTER USER MAPPING FOR PUBLIC SERVER polaris
            OPTIONS (SET client_id {litteral(client)}, SET client_secret {litteral(secret)});
        DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyste') THEN CREATE ROLE analyste LOGIN; END IF;
        END $$;
        ALTER ROLE analyste PASSWORD {litteral(mot_de_passe)};
        -- lecture seule, requêtes bornées (US-14) ; Power BI, Qlik, notebooks se connectent avec ce rôle
        ALTER ROLE analyste SET default_transaction_read_only = on;
        ALTER ROLE analyste SET statement_timeout = {litteral(os.environ.get("STATEMENT_TIMEOUT", "120s"))};
        ALTER ROLE analyste SET work_mem = {litteral(os.environ.get("WORK_MEM", "64MB"))};
        -- pas de rôle lake_read : il permettrait de lire n'importe quel fichier du stockage ; SELECT suffit
        REVOKE CREATE ON SCHEMA public FROM PUBLIC;
        CREATE SCHEMA IF NOT EXISTS vue;
        GRANT USAGE ON SCHEMA vue TO analyste;
    """)


def rattaches() -> dict:
    """{(schéma, table): [colonnes]} des tables Iceberg rattachées dans la base."""
    lignes = psql("""SELECT c.table_schema, c.table_name, string_agg(c.column_name, ',' ORDER BY c.ordinal_position)
        FROM information_schema.columns c
        JOIN information_schema.foreign_tables f
          ON f.foreign_table_schema = c.table_schema AND f.foreign_table_name = c.table_name
        GROUP BY 1, 2""")
    return {(s, t): colonnes.split(",") for s, t, colonnes in (ligne.split("|") for ligne in lignes.splitlines() if ligne)}


def synchroniser() -> tuple[int, list]:
    """Met la base en accord avec le catalogue ; une table en erreur n'empêche pas les autres."""
    voulu, present = catalogue(), rattaches()
    entete = "SET default_transaction_read_only = off; SET lock_timeout = '5s';\n"
    erreurs = []

    def executer(description, sql):
        try:
            psql(entete + sql)
        except RuntimeError as erreur:
            erreurs.append(f"{description} : {str(erreur)[:300]}")

    for (schema, table), colonnes in present.items():
        if voulu.get((schema, table)) != colonnes:
            # table disparue du catalogue, ou colonnes changées : retirée (rattachée de nouveau ci-dessous)
            executer(f"retrait {schema}.{table}", f"DROP FOREIGN TABLE IF EXISTS {ident(schema)}.{ident(table)} CASCADE;")
    for namespace in sorted({n for n, _ in voulu}):
        executer(f"schéma {namespace}", f"CREATE SCHEMA IF NOT EXISTS {ident(namespace)}; "
                                       f"GRANT USAGE ON SCHEMA {ident(namespace)} TO analyste;")
    for (namespace, table), colonnes in sorted(voulu.items()):
        nom = f"{ident(namespace)}.{ident(table)}"
        ordres = []
        if present.get((namespace, table)) != colonnes:
            ordres.append(f"""CREATE TABLE IF NOT EXISTS {nom} () USING iceberg
                WITH (catalog = 'polaris', read_only = true, catalog_name = {litteral(CATALOGUE)},
                      catalog_namespace = {litteral(namespace)}, catalog_table_name = {litteral(table)});""")
        ordres.append(f"GRANT SELECT ON {nom} TO analyste;")
        if namespace == "app" and not table.endswith("_historique") and "_supprime" in colonnes:
            ordres.append(f"CREATE OR REPLACE VIEW vue.{ident(table)} AS SELECT * FROM {nom} WHERE NOT _supprime; "
                          f"GRANT SELECT ON vue.{ident(table)} TO analyste;")
        executer(f"table {namespace}.{table}", "\n".join(ordres))
    return len(voulu), erreurs


def main():
    while True:
        try:
            preparer()
            break
        except Exception as erreur:  # noqa: BLE001 : PostgreSQL ou Polaris pas encore prêts
            print(f"synchro : préparation impossible pour l'instant ({str(erreur)[:300]})", flush=True)
            time.sleep(5)
    while True:
        debut = time.time()
        try:
            nombre, erreurs = synchroniser()
            for erreur in erreurs:
                print(f"synchro : ÉCHEC {erreur}", file=sys.stderr, flush=True)
            # disponible dès qu'une synchronisation a eu lieu : une table en erreur n'empêche pas de lire les autres
            TEMOIN.touch()
            print(f"synchro : {nombre} table(s) dans le catalogue, {len(erreurs)} erreur(s), "
                  f"{time.time() - debut:.1f} s", flush=True)
        except Exception as erreur:  # noqa: BLE001 : Polaris injoignable… on réessaie au tour suivant
            print(f"synchro : ÉCHEC ({str(erreur)[:500]})", file=sys.stderr, flush=True)
        time.sleep(INTERVALLE_S)


if __name__ == "__main__":
    main()
