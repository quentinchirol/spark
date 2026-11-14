"""Test d'intégration de l'isolation analytique (US-14) : la charge des analystes ne gêne pas l'application, sur kind.

Après tests/integration/tester_analytique.py (pool analytique prêt, table app.commandes) :
  1. 40 requêtes de dashboard lancées en même temps sur le pool : 8 au plus s'exécutent à la fois dans un pod
     (PgBouncer), les autres attendent leur tour ; toutes aboutissent ; pendant ce temps, les lectures de
     l'application sur le PostgreSQL principal restent rapides ;
  2. pool saturé : une requête qui attend sa place plus que le délai maximal (120 s, ramené à 20 s en CI) est
     refusée proprement, et le pool répond de nouveau ensuite ;
  3. une requête analytique de plus de 2 minutes est coupée (statement_timeout) ;
  4. charge CPU sur le pool : un pod analytique est ajouté automatiquement (HorizontalPodAutoscaler).

Sur GKE, l'isolation est aussi physique (pool sur ses propres nœuds) ; sur kind, tout partage un nœud à 2 cœurs :
le test vérifie les mécanismes (file d'attente, coupures, autoscaling) et mesure la latence de l'application.

    python tests/integration/tester_isolation.py
Dépendances : pip install "psycopg[binary]"
"""
import base64
import statistics
import subprocess
import threading
import time
import uuid

import psycopg

from commun import env, kubectl, tunnel, verifier

PORT_POOL, PORT_PRINCIPAL = 15432, 15433
PLACES = 8
DUREE_DASHBOARD = 2      # secondes par requête de dashboard simulée


def mot_de_passe_app():
    """Mot de passe du rôle lake_app (Secret créé par l'opérateur PostgreSQL), jamais affiché."""
    brut = kubectl("get", "secret", "lake-app.lake-principal.credentials.postgresql.acid.zalan.do",
                   "-o", "jsonpath={.data.password}")
    return base64.b64decode(brut).decode()


def pool():
    return psycopg.connect(host="localhost", port=PORT_POOL, dbname="lake", user="analyste",
                           password=env("analytique.env")["ANALYTIQUE_MOT_DE_PASSE"], autocommit=True,
                           connect_timeout=30)


def actives_sur_le_pod():
    """Requêtes de l'analyste en cours d'exécution dans PostgreSQL (vu depuis le pod, sans passer par PgBouncer)."""
    pod = kubectl("get", "pods", "-l", "app=lake-analytique", "-o", "name").split()[0]
    sortie = subprocess.run(["kubectl", "-n", "spark", "exec", pod, "-c", "postgres", "--",
                             "/usr/lib/postgresql/18/bin/psql", "-h", "/var/run/postgresql", "-U", "postgres", "-d",
                             "lake", "-XtA", "-c",
                             "SELECT count(*) FROM pg_stat_activity WHERE usename = 'analyste' AND state = 'active'"],
                            text=True, capture_output=True)
    return int(sortie.stdout.strip() or 0)


class LatenceApplication(threading.Thread):
    """Lectures de l'application (rôle lake_app, vue.commandes) en continu sur le principal ; durées en secondes."""
    def __init__(self, mot_de_passe):
        super().__init__(daemon=True)
        self.conn = psycopg.connect(host="localhost", port=PORT_PRINCIPAL, dbname="lake", user="lake_app",
                                    password=mot_de_passe, sslmode="require", autocommit=True)
        self.durees, self.actif = [], True

    def run(self):
        while self.actif:
            debut = time.monotonic()
            self.conn.execute("SELECT * FROM vue.commandes WHERE id = %s", (str(uuid.uuid4()),)).fetchall()
            self.durees.append(time.monotonic() - debut)
            time.sleep(0.05)

    def mesurer(self, secondes):
        avant = len(self.durees)
        time.sleep(secondes)
        return self.durees[avant:]


def p95(durees):
    return statistics.quantiles(durees, n=20)[-1] if len(durees) >= 20 else max(durees)


def executer(requete, resultats, cle):
    debut = time.monotonic()
    try:
        with pool() as conn:
            conn.execute(requete).fetchall()
        resultats[cle] = ("ok", time.monotonic() - debut)
    except psycopg.Error as erreur:
        resultats[cle] = (str(erreur).splitlines()[0], time.monotonic() - debut)


def lancer(requetes, resultats):
    fils = [threading.Thread(target=executer, args=(r, resultats, c), daemon=True) for c, r in requetes.items()]
    for f in fils:
        f.start()
    return fils


def main():
    # le Service lake-principal n'a pas de sélecteur (points d'accès tenus par Patroni) : tunnel vers le primaire
    primaire = kubectl("get", "pods", "-l", "cluster-name=lake-principal,spilo-role=master", "-o", "name").split()[0]
    tunnels = [tunnel("svc/lake-analytique", PORT_POOL, 5432), tunnel(primaire, PORT_PRINCIPAL, 5432)]
    try:
        app = LatenceApplication(mot_de_passe_app())
        # une première lecture ici : une erreur (droits, connexion) s'affiche clairement au lieu d'arrêter le fil
        try:
            app.conn.execute("SELECT count(*) FROM vue.commandes").fetchall()
        except psycopg.Error as erreur:
            verifier(False, f"l'application (rôle lake_app) lit vue.commandes ({str(erreur).splitlines()[0]})")
        verifier(True, "l'application (rôle lake_app) lit vue.commandes")
        app.start()
        avant = app.mesurer(10)
        print(f"     application seule : {len(avant)} lectures, médiane {statistics.median(avant) * 1000:.1f} ms, "
              f"p95 {p95(avant) * 1000:.1f} ms", flush=True)

        # requête longue lancée d'abord : coupée à 2 min (point 3), elle occupe une place pendant les points 1 et 2
        longue = {}
        fil_long = lancer({"longue": "SELECT pg_sleep(150)"}, longue)[0]
        time.sleep(2)

        print("1. 40 requêtes de dashboard en même temps : file d'attente, toutes aboutissent, application rapide")
        resultats = {}
        debut = time.monotonic()
        # 2 s par requête : 40 requêtes sur 7 places (la 8e tient la requête longue) → ~12 s d'attente au plus, sous
        # l'attente maximale de la CI (20 s) ; avec 4 s, les dernières attendaient plus de 20 s et étaient refusées
        fils = lancer({i: f"SELECT pg_sleep({DUREE_DASHBOARD}), (SELECT count(*) FROM app.commandes_historique)"
                       for i in range(40)}, resultats)
        maximum, pendant = 0, []
        while any(f.is_alive() for f in fils):
            maximum = max(maximum, actives_sur_le_pod())
            pendant += app.mesurer(1)
        duree = time.monotonic() - debut
        reussies = sum(1 for etat, _ in resultats.values() if etat == "ok")
        erreurs = sorted({etat for etat, _ in resultats.values() if etat != "ok"})
        verifier(reussies == 40, f"40 requêtes abouties ({reussies}) en {duree:.0f} s {erreurs or ''}")
        verifier(maximum <= PLACES, f"au plus {PLACES} en cours à la fois dans PostgreSQL (maximum observé : {maximum})")
        verifier(duree >= DUREE_DASHBOARD * 40 / PLACES * 0.8,
                 f"les autres ont attendu leur tour ({duree:.0f} s pour 40 × {DUREE_DASHBOARD} s)")
        print(f"     application pendant la charge : {len(pendant)} lectures, médiane "
              f"{statistics.median(pendant) * 1000:.1f} ms, p95 {p95(pendant) * 1000:.1f} ms", flush=True)
        verifier(p95(pendant) < 0.25, f"application toujours rapide pendant la charge analytique "
                                      f"(p95 {p95(pendant) * 1000:.0f} ms, avant {p95(avant) * 1000:.0f} ms)")

        print("2. Pool saturé : attente trop longue refusée proprement, puis le pool répond de nouveau")
        occupe = {}
        lancer({i: "SELECT pg_sleep(45)" for i in range(PLACES - 1)}, occupe)   # + la requête longue = 8 places
        time.sleep(3)
        refus = {}
        executer("SELECT 1", refus, "attente")
        etat, attente = refus["attente"]
        verifier("query_wait_timeout" in etat, f"refusée après {attente:.0f} s d'attente ({etat})")
        debut = time.monotonic()
        while time.monotonic() - debut < 90 and len(occupe) < PLACES - 1:
            time.sleep(1)
        apres = {}
        executer("SELECT 1", apres, "apres")
        verifier(apres["apres"][0] == "ok", "le pool répond de nouveau une fois les places libérées")

        print("3. Requête analytique de plus de 2 minutes coupée")
        fil_long.join(timeout=200)
        etat, duree = longue.get("longue", ("absente", 0))
        verifier("statement timeout" in etat and 110 <= duree <= 140, f"coupée après {duree:.0f} s ({etat})")

        print("4. Charge CPU : un pod analytique ajouté automatiquement")
        avant_pods = int(kubectl("get", "deploy", "lake-analytique", "-o", "jsonpath={.spec.replicas}"))
        charge, arret = {}, threading.Event()

        def calculer(cle):
            while not arret.is_set():
                executer("SELECT count(*) FROM generate_series(1, 30000000)", charge, cle)

        for i in range(PLACES):
            threading.Thread(target=calculer, args=(i,), daemon=True).start()
        debut, pods = time.monotonic(), avant_pods
        while time.monotonic() - debut < 240 and pods <= avant_pods:
            time.sleep(5)
            pods = int(kubectl("get", "deploy", "lake-analytique", "-o", "jsonpath={.spec.replicas}"))
        arret.set()
        verifier(pods > avant_pods, f"pods analytiques : {avant_pods} → {pods} en {time.monotonic() - debut:.0f} s")
        app.actif = False
    finally:
        for t in tunnels:
            t.terminate()
    print("Isolation analytique : tous les contrôles passent")


if __name__ == "__main__":
    main()
