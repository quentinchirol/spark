"""Test d'intégration des sauvegardes (US-5) : restauration à un instant donné dans un cluster neuf.

Sur kind, après tester_postgres.py. Le stockage des sauvegardes (MinIO « sauvegardes ») tourne HORS de
Kubernetes (scripts/dev/sauvegardes.sh). Étapes :
  1. sauvegarde de base WAL-G du primaire, puis « avant » écrit, instant T, puis « après » écrit ;
  2. le cluster lake-principal et ses volumes sont SUPPRIMÉS ;
  3. les sauvegardes sont toujours dans le stockage hors cluster ;
  4. un cluster neuf, lake-restaure, est restauré à l'instant T : il contient « avant » et pas « après ».

    python tests/integration/tester_restauration.py
"""
import json
import subprocess
import time

from commun import env, kubectl, verifier

SOURCE, CIBLE = "lake-principal", "lake-restaure"


def primaire(cluster):
    noms = kubectl("get", "pods", "-l", f"cluster-name={cluster},spilo-role=master", "-o", "name").split()
    return noms[0] if len(noms) == 1 else None


def executer(pod, *commande):
    resultat = subprocess.run(["kubectl", "-n", "spark", "exec", pod, "-c", "postgres", "--", *commande],
                              text=True, capture_output=True)
    if resultat.returncode != 0:
        print(f"ECHEC : {' '.join(commande)}\n{resultat.stdout[-2000:]}\n{resultat.stderr[-2000:]}", flush=True)
        raise SystemExit(1)
    return resultat.stdout.strip()


def sql(pod, requete, base="postgres"):
    return executer(pod, "psql", "-U", "postgres", "-d", base, "-X", "-tA", "-v", "ON_ERROR_STOP=1", "-c", requete)


def sql_essai(pod, requete, base="postgres"):
    """Comme sql(), mais renvoie None si le pod ne répond pas encore (attente d'un démarrage)."""
    if pod is None:
        return None
    resultat = subprocess.run(["kubectl", "-n", "spark", "exec", pod, "-c", "postgres", "--", "psql", "-U", "postgres",
                               "-d", base, "-X", "-tA", "-c", requete], text=True, capture_output=True)
    return resultat.stdout.strip() if resultat.returncode == 0 else None


def walg(pod, *args):
    return executer(pod, "envdir", "/run/etc/wal-e.d/env", *args)


def attendre(description, condition, delai=600, pas=5):
    debut = time.monotonic()
    while time.monotonic() - debut < delai:
        try:
            if condition():
                print(f"     ({description} en {time.monotonic() - debut:.0f} s)", flush=True)
                return True
        except subprocess.CalledProcessError:
            pass
        time.sleep(pas)
    return False


def sauvegardes_hors_cluster(prefixe):
    """Liste, depuis l'extérieur de Kubernetes, les objets de sauvegarde de base sous le préfixe WAL-G."""
    cles = env("sauvegardes.env")
    chemin = prefixe.removeprefix("s3://").rstrip("/") + "/basebackups_005/"
    resultat = subprocess.run(
        ["docker", "run", "--rm", "--network", "kind",
         "-e", f"CLE={cles['AWS_ACCESS_KEY_ID']}", "-e", f"SECRET={cles['AWS_SECRET_ACCESS_KEY']}",
         "--entrypoint", "sh", "pgsty/mc:latest", "-c",
         f'mc alias set s http://sauvegardes:9000 "$CLE" "$SECRET" >/dev/null && mc ls "s/{chemin}"'],
        text=True, capture_output=True)
    return [ligne for ligne in resultat.stdout.splitlines() if ligne.strip()]


def main():
    pod = primaire(SOURCE)
    verifier(pod is not None, f"primaire du cluster source ({pod})")
    prefixe = executer(pod, "cat", "/run/etc/wal-e.d/env/WALG_S3_PREFIX")
    verifier(prefixe.startswith("s3://"), f"WAL-G archive vers {prefixe}")

    # --- 1. sauvegarde de base, puis « avant », instant T, « après » --------------------------------------
    walg(pod, "/scripts/postgres_backup.sh", "/home/postgres/pgdata/pgroot/data")
    liste = walg(pod, "wal-g", "backup-list")
    verifier("base_" in liste, "sauvegarde de base WAL-G présente")

    sql(pod, "CREATE TABLE IF NOT EXISTS temoin (etape text, ecrit_le timestamptz DEFAULT clock_timestamp())", "lake")
    sql(pod, "INSERT INTO temoin (etape) VALUES ('avant')", "lake")
    time.sleep(1.5)
    instant = sql(pod, "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS') || '+00:00'")
    time.sleep(2.5)
    sql(pod, "INSERT INTO temoin (etape) VALUES ('apres')", "lake")
    segment = sql(pod, "SELECT pg_walfile_name(pg_switch_wal())")
    verifier(attendre("archivage du dernier WAL",
                      lambda: sql(pod, "SELECT coalesce(last_archived_wal, '') FROM pg_stat_archiver") >= segment, 180),
             f"WAL {segment} archivé hors cluster (instant T = {instant})")

    # --- 2. suppression du cluster et de ses volumes -------------------------------------------------------
    manifeste = json.loads(kubectl("get", "postgresql", SOURCE, "-o", "json"))
    uid = manifeste["metadata"]["uid"]
    kubectl("delete", "postgresql", SOURCE, "--wait=true")
    verifier(attendre("arrêt des pods", lambda: not kubectl("get", "pods", "-l", f"cluster-name={SOURCE}", "-o", "name"), 300),
             "cluster source supprimé")
    kubectl("delete", "pvc", "-l", f"cluster-name={SOURCE}", "--wait=true")
    verifier(not kubectl("get", "pvc", "-l", f"cluster-name={SOURCE}", "-o", "name"), "volumes du cluster source supprimés")

    # --- 3. sauvegardes toujours disponibles hors cluster ---------------------------------------------------
    objets = sauvegardes_hors_cluster(prefixe)
    verifier(len(objets) > 0, f"sauvegardes toujours présentes hors du cluster ({len(objets)} entrée(s))")

    # --- 4. cluster neuf restauré à l'instant T -------------------------------------------------------------
    spec = manifeste["spec"]
    spec["numberOfInstances"] = 1
    spec["clone"] = {
        "cluster": SOURCE, "uid": uid, "timestamp": instant,
        "s3_wal_path": prefixe,   # chemin complet des WAL du cluster source (…/wal/<version>)
        "s3_endpoint": env("sauvegardes.env")["CLONE_AWS_ENDPOINT"], "s3_force_path_style": True,
    }
    nouveau = {"apiVersion": "acid.zalan.do/v1", "kind": "postgresql",
               "metadata": {"name": CIBLE, "namespace": "spark"}, "spec": spec}
    subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(nouveau), text=True, check=True)
    verifier(attendre("restauration", lambda: sql_essai(primaire(CIBLE), "SELECT NOT pg_is_in_recovery()") == "t", 900, 10),
             "cluster neuf restauré et ouvert en écriture")
    etapes = sql(primaire(CIBLE), "SELECT string_agg(etape, ',' ORDER BY ecrit_le) FROM temoin", "lake")
    verifier(etapes == "avant", f"restauré à l'instant T : contient « avant », pas « après » (trouvé : {etapes})")
    print("Sauvegardes : tous les contrôles passent")


main()
