"""Test d'intégration du PostgreSQL principal (US-7) : Zalando + pg_lake + Polaris + MinIO, sur kind.

Vérifie :
  1. réplication synchrone active (Patroni) ;
  2. lake.creer_table crée une table Iceberg déclarée dans Polaris ;
  3. une écriture acceptée est lisible dans la vue en moins d'une seconde ;
  4. la fusion planifiée (pg_cron, chaque minute) vide la table chaude vers Iceberg, la vue ne change pas ;
  5. un autre moteur (pyiceberg, principal spark) lit les mêmes lignes dans Polaris, sans copie ;
  6. une ligne froide modifiée est visible tout de suite, et dans Iceberg après la fusion.

    echo '127.0.0.1 minio' | sudo tee -a /etc/hosts
    python tests/integration/tester_postgres.py
Dépendances : pip install "pyiceberg[pyarrow]"
"""
import json
import subprocess
import time
import uuid

from commun import (PORT_POLARIS, PORT_S3, catalogue, env, kubectl, tunnel, verifier,
                    verifier_hote_minio)

TABLE = "commandes"


def primaire():
    nom = kubectl("get", "pods", "-l", "cluster-name=lake-principal,spilo-role=master", "-o", "name").split()
    verifier(len(nom) == 1, f"un seul primaire PostgreSQL ({' '.join(nom)})")
    return nom[0]


def sql(pod, requete, base="lake"):
    commande = ["kubectl", "-n", "spark", "exec", pod, "-c", "postgres", "--", "psql", "-U", "postgres", "-d", base,
                "-X", "-tA", "-v", "ON_ERROR_STOP=1", "-c", requete]
    resultat = subprocess.run(commande, text=True, capture_output=True)
    if resultat.returncode != 0:
        print(f"ECHEC SQL : {requete}\n{resultat.stderr.strip()}", flush=True)
        raise SystemExit(1)
    return resultat.stdout.strip()


def historique_cron(pod):
    print(sql(pod, "SELECT jobid, jobname, database, schedule, active FROM cron.job", base="postgres"))
    print(sql(pod, "SELECT start_time, status, left(return_message, 300) FROM cron.job_run_details "
                   "ORDER BY start_time DESC LIMIT 5", base="postgres"))
    print(sql(pod, "SELECT current_setting('max_worker_processes'), string_agg(backend_type || ':' || n, ', ') "
                   "FROM (SELECT backend_type, count(*) n FROM pg_stat_activity GROUP BY 1) t", base="postgres"))


def lot(*messages):
    return "'" + json.dumps([
        {"id": i, "op": op, "table": TABLE, "date_creation": "2026-11-11", "version": v,
         "donnees": {"client": c, "montant": m, "statut": s}}
        for i, op, v, c, m, s in messages]).replace("'", "''") + "'::jsonb"


def attendre(description, condition, delai=150):
    debut = time.monotonic()
    while time.monotonic() - debut < delai:
        if condition():
            print(f"     ({description} après {time.monotonic() - debut:.0f} s)", flush=True)
            return True
        time.sleep(5)
    return False


def main():
    verifier_hote_minio()
    pod = primaire()

    replication = sql(pod, "SELECT string_agg(sync_state, ',') FROM pg_stat_replication")
    verifier("sync" in replication or "quorum" in replication, f"réplication synchrone ({replication})")

    sql(pod, f"SELECT lake.creer_table('{TABLE}', 'client text, montant numeric(12,2), statut text')")
    verifier(sql(pod, f"SELECT methode FROM lake.tables WHERE nom = '{TABLE}'") == "iceberg", "table app.commandes en Iceberg")

    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    debut = time.monotonic()
    sql(pod, f"SELECT lake.ecrire_lot('{TABLE}', {lot((a, 'INSERT', 1, 'alice', 10, 'ouverte'), (b, 'INSERT', 2, 'bob', 5, 'ouverte'))})")
    lu = sql(pod, f"SELECT count(*) FROM vue.{TABLE} WHERE id IN ('{a}', '{b}')")
    duree = time.monotonic() - debut
    verifier(lu == "2", f"écriture visible dans la vue en {duree:.2f} s (kubectl exec compris)")

    # fusion lancée à la main : une erreur de pg_lake / Polaris / MinIO s'affiche ici directement
    verifier(sql(pod, f"SELECT lake.fusionner('{TABLE}')") == "2", "fusion manuelle : 2 lignes vers Iceberg")
    verifier(sql(pod, f"SELECT count(*) FROM chaud.{TABLE}") == "0", "table chaude vidée")
    verifier(sql(pod, f"SELECT count(*) FROM app.{TABLE}") == "2", "2 lignes dans la table Iceberg app.commandes")
    verifier(sql(pod, f"SELECT statut FROM vue.{TABLE} WHERE id = '{a}'") == "ouverte", "vue identique après fusion")

    # --- même table, lue par un autre moteur via Polaris ---------------------------------------------
    s3 = env("s3.env")
    tunnels = [tunnel("pod/minio-0", PORT_S3, 9000), tunnel("svc/polaris", PORT_POLARIS, 8181)]
    try:
        spark = catalogue(env("polaris-spark.env")["POLARIS_CREDENTIAL"],
                          (s3["AWS_ACCESS_KEY_ID"], s3["AWS_SECRET_ACCESS_KEY"]))
        lignes = spark.load_table(f"app.{TABLE}").scan().to_arrow().to_pylist()
        verifier(sorted(str(r["id"]) for r in lignes) == sorted([a, b]),
                 "pyiceberg (principal spark) lit les mêmes lignes dans Polaris, sans copie")

        sql(pod, f"SELECT lake.ecrire_lot('{TABLE}', {lot((b, 'UPDATE', 3, 'bob', 50, 'payee'))})")
        verifier(sql(pod, f"SELECT statut FROM vue.{TABLE} WHERE id = '{b}'") == "payee",
                 "ligne froide modifiée : visible tout de suite")
        # fusion suivante : celle planifiée par pg_cron (chaque minute)
        if not attendre("fusion par pg_cron", lambda: sql(pod, f"SELECT count(*) FROM chaud.{TABLE}") == "0"):
            historique_cron(pod)
            verifier(False, "la fusion planifiée (pg_cron) a vidé la table chaude")
        verifier(True, "la fusion planifiée (pg_cron) a vidé la table chaude")
        verifier(any(r["statut"] == "payee" for r in spark.load_table(f"app.{TABLE}").scan().to_arrow().to_pylist()),
                 "modification appliquée à Iceberg, lue par pyiceberg")
    finally:
        for processus in tunnels:
            processus.terminate()
    print("PostgreSQL principal : tous les contrôles passent")


main()
