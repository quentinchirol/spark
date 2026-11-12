"""Test d'intégration du pool analytique (US-13) : PostgreSQL + pg_lake en lecture seule, sur kind.

Après tests/integration/tester_postgres.py (table app.commandes du PostgreSQL principal), en client PostgreSQL
distant comme Power BI ou Qlik (connecteur PostgreSQL, utilisateur « analyste ») :
  1. une table gold créée par un autre moteur (pyiceberg, principal spark) apparaît d'elle-même dans le pool, avec
     ses données ; une ligne ajoutée ensuite est lue à la requête suivante, sans rien recharger (DirectQuery) ;
  2. une donnée écrite par l'application sur le PostgreSQL principal est lisible dans le pool en moins de 2 minutes
     (fusion planifiée vers Iceberg, chaque minute) ;
  3. le pool est en lecture seule : écrire, créer une table, contourner la lecture seule : tout est refusé ;
  4. le rôle analyste a ses limites (statement_timeout 120 s).

    echo '127.0.0.1 minio' | sudo tee -a /etc/hosts
    python tests/integration/tester_analytique.py
Dépendances : pip install "pyiceberg[pyarrow]" "psycopg[binary]"
"""
import json
import subprocess
import time
import uuid

import psycopg
import pyarrow as pa

from commun import PORT_POLARIS, PORT_S3, catalogue, env, kubectl, tunnel, verifier, verifier_hote_minio

PORT_POOL = 15432


def primaire():
    return kubectl("get", "pods", "-l", "cluster-name=lake-principal,spilo-role=master", "-o", "name").split()[0]


def sql_principal(requete):
    resultat = subprocess.run(["kubectl", "-n", "spark", "exec", primaire(), "-c", "postgres", "--", "psql", "-U",
                               "postgres", "-d", "lake", "-X", "-tA", "-v", "ON_ERROR_STOP=1", "-c", requete],
                              text=True, capture_output=True)
    if resultat.returncode != 0:
        raise SystemExit(f"ECHEC SQL principal : {requete}\n{resultat.stderr.strip()}")
    return resultat.stdout.strip()


def connexion():
    return psycopg.connect(host="localhost", port=PORT_POOL, dbname="lake", user="analyste",
                           password=env("analytique.env")["ANALYTIQUE_MOT_DE_PASSE"], autocommit=True,
                           connect_timeout=10)


def valeur(conn, requete, *parametres):
    with conn.cursor() as curseur:
        curseur.execute(requete, parametres)
        ligne = curseur.fetchone()
        return ligne[0] if ligne else None


def attendre(condition, delai):
    debut = time.monotonic()
    while time.monotonic() - debut < delai:
        try:
            if condition():
                return time.monotonic() - debut
        except psycopg.Error:
            pass
        time.sleep(2)
    return None


def refuse(conn, requete):
    try:
        with conn.cursor() as curseur:
            curseur.execute(requete)
        return False
    except psycopg.Error as erreur:
        print(f"     refusé : {str(erreur).splitlines()[0]}", flush=True)
        return True


def main():
    verifier_hote_minio()
    s3 = env("s3.env")
    tunnels = [tunnel("svc/minio", PORT_S3, 9000), tunnel("svc/polaris", PORT_POLARIS, 8181),
               tunnel("svc/lake-analytique", PORT_POOL, 5432)]
    try:
        ecriture = catalogue(env("polaris-spark.env")["POLARIS_CREDENTIAL"],
                             (s3["AWS_ACCESS_KEY_ID"], s3["AWS_SECRET_ACCESS_KEY"]))
        conn = connexion()

        print("1. Une table gold écrite par un autre moteur apparaît d'elle-même dans le pool")
        ecriture.create_namespace_if_not_exists("gold")
        schema = pa.schema([("jour", pa.string()), ("ventes", pa.int64())])
        table = ecriture.create_table_if_not_exists("gold.indicateurs", schema=schema)
        table.append(pa.table({"jour": ["2026-11-01", "2026-11-02"], "ventes": [10, 32]}, schema=schema))
        duree = attendre(lambda: valeur(conn, "SELECT sum(ventes) FROM gold.indicateurs") == 42, 120)
        verifier(duree is not None, f"gold.indicateurs visible avec ses données en {duree or 0:.0f} s (synchro du pool)")
        tables = {r for (r,) in conn.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables "
            "WHERE table_schema IN ('gold', 'app', 'vue')").fetchall()}
        verifier({"gold.indicateurs", "app.commandes", "vue.commandes"} <= tables,
                 f"tables visibles pour l'analyste : {sorted(tables)}")
        table.append(pa.table({"jour": ["2026-11-03"], "ventes": [8]}, schema=schema))
        debut = time.monotonic()
        verifier(valeur(conn, "SELECT sum(ventes) FROM gold.indicateurs") == 50,
                 f"ligne ajoutée lue à la requête suivante ({time.monotonic() - debut:.2f} s, DirectQuery)")

        print("2. Donnée écrite par l'application : dans le pool en moins de 2 minutes")
        id_ = str(uuid.uuid4())
        message = [{"id": id_, "op": "INSERT", "table": "commandes", "date_creation": "2026-11-12", "version": 9001,
                    "donnees": {"client": "pool", "montant": 7, "statut": "ouverte"}}]
        sql_principal(f"SELECT lake.ecrire_lot('commandes', '{json.dumps(message)}'::jsonb)")
        ecrit = time.monotonic()
        duree = attendre(lambda: valeur(conn, "SELECT count(*) FROM vue.commandes WHERE id = %s", id_) == 1, 150)
        verifier(duree is not None and duree < 120,
                 f"écriture de l'application lisible dans le pool {time.monotonic() - ecrit:.0f} s après (< 120 s)")

        print("3. Lecture seule")
        verifier(refuse(conn, "INSERT INTO gold.indicateurs VALUES ('2026-11-04', 1)"), "écriture dans une table refusée")
        verifier(refuse(conn, "CREATE TABLE public.essai (n int)"), "création de table refusée")
        verifier(refuse(conn, "SET default_transaction_read_only = off; DELETE FROM vue.commandes"),
                 "suppression refusée même en levant la lecture seule de la session")
        verifier(valeur(conn, "SELECT sum(ventes) FROM gold.indicateurs") == 50, "données intactes")

        print("4. Limites du rôle analyste")
        verifier(valeur(conn, "SHOW statement_timeout") == "2min", "requête interrompue au-delà de 120 s")
        conn.close()
    finally:
        for t in tunnels:
            t.terminate()
    print("Pool analytique : tous les contrôles passent")


if __name__ == "__main__":
    main()
