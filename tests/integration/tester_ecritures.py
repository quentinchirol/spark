"""Test d'intégration du chemin d'écriture (US-6) : passerelle → Kafka → writers (KEDA) → PostgreSQL, sur kind.

  1. des écritures (INSERT, UPDATE, DELETE, ordre respecté par id) sont envoyées à la passerelle : toutes 202 ;
  2. pendant l'envoi, un writer est tué brutalement (crash) ;
  3. une fois tout absorbé : chaque ligne a exactement son dernier état, aucune n'est perdue ni en double,
     et l'historique contient chaque message accepté une seule fois ;
  4. le retard a fait ajouter des writers par KEDA, retirés ensuite quand le calme est revenu.

    python tests/integration/tester_ecritures.py
Dépendances : pip install httpx
"""
import asyncio
import random
import subprocess
import threading
import time
import uuid

import httpx

from commun import kubectl, tunnel, verifier

TABLE, RAFALE, PORT = "commandes", "rafale", 18000
NB_LIGNES, NB_MAJ, NB_SUPPR = 1500, 1500, 300
NB_RAFALE = 60000

# producteur lancé DANS le cluster (image du writer, qui contient aiokafka) : bien plus rapide que le port-forward,
# il crée un vrai retard que KEDA doit résorber en ajoutant des writers
SCRIPT_RAFALE = f"""
import asyncio, json, uuid
from aiokafka import AIOKafkaProducer
async def principal():
    p = AIOKafkaProducer(bootstrap_servers="lakehouse-kafka-bootstrap:9092", acks="all", enable_idempotence=True, linger_ms=20)
    await p.start()
    envois = []
    for n in range({NB_RAFALE}):
        k = str(uuid.uuid4())
        v = json.dumps({{"id": k, "op": "INSERT", "table": "{RAFALE}", "date_creation": "2026-11-11", "donnees": {{"id": k, "n": n}}}})
        envois.append(await p.send("app", v.encode(), key=k.encode()))
    await asyncio.gather(*envois)
    await p.stop()
    print("rafale envoyée")
asyncio.run(principal())
"""


def primaire():
    return kubectl("get", "pods", "-l", "cluster-name=lake-principal,spilo-role=master", "-o", "name").split()[0]


def sql(requete):
    resultat = subprocess.run(["kubectl", "-n", "spark", "exec", primaire(), "-c", "postgres", "--", "psql", "-U",
                               "postgres", "-d", "lake", "-X", "-tA", "-v", "ON_ERROR_STOP=1", "-c", requete],
                              text=True, capture_output=True)
    if resultat.returncode != 0:
        print(f"ECHEC SQL : {requete}\n{resultat.stderr}", flush=True)
        raise SystemExit(1)
    return resultat.stdout.strip()


def nb_writers():
    return int(kubectl("get", "deploy", "ecrivain", "-o", "jsonpath={.spec.replicas}") or 0)


class Observateur(threading.Thread):
    """Relève le nombre de writers voulu par KEDA toutes les 2 s."""
    def __init__(self):
        super().__init__(daemon=True)
        self.maximum, self.actif = 0, True

    def run(self):
        while self.actif:
            try:
                self.maximum = max(self.maximum, nb_writers())
            except subprocess.CalledProcessError:
                pass
            time.sleep(2)


def plan_ecritures():
    """Messages à envoyer, regroupés par id (envoyés dans l'ordre pour un même id), et état final attendu."""
    par_id, attendu = {}, {}
    ids = [str(uuid.uuid4()) for _ in range(NB_LIGNES)]
    for numero, id_ in enumerate(ids):
        par_id[id_] = [("INSERT", {"id": id_, "client": f"c{numero % 50}", "montant": 1, "statut": "ouverte"})]
    for _ in range(NB_MAJ):
        id_ = random.choice(ids)
        par_id[id_].append(("UPDATE", {"id": id_, "client": par_id[id_][0][1]["client"],
                                       "montant": random.randint(2, 999), "statut": "modifiee"}))
    for id_ in random.sample(ids, NB_SUPPR):
        par_id[id_].append(("DELETE", {}))
    for id_, messages in par_id.items():
        op, donnees = messages[-1]
        attendu[id_] = None if op == "DELETE" else donnees["montant"]
    return par_id, attendu


async def envoyer(par_id, apres_moitie):
    """Envoie tout, 64 ids en parallèle, chaque id dans l'ordre ; renvoie le nombre de messages acceptés (202)."""
    acceptes, envoyes, total = 0, 0, sum(len(m) for m in par_id.values())
    file = asyncio.Queue()
    for id_, messages in par_id.items():
        file.put_nowait((id_, messages))
    moitie_signalee = False

    async def travailleur(client):
        nonlocal acceptes, envoyes, moitie_signalee
        while not file.empty():
            id_, messages = file.get_nowait()
            for op, donnees in messages:
                corps = {"id": id_, "op": op, "table": TABLE, "date_creation": "2026-11-11", "donnees": donnees}
                for essai in range(10):            # 503 : renvoyer le MÊME message (contrat)
                    reponse = await client.post("/ecritures", json=corps)
                    if reponse.status_code != 503:
                        break
                    await asyncio.sleep(0.5)
                if reponse.status_code != 202:
                    print(f"ECHEC réponse {reponse.status_code} : {reponse.text}", flush=True)
                    raise SystemExit(1)
                acceptes += 1
                envoyes += 1
                if not moitie_signalee and envoyes >= total // 2:
                    moitie_signalee = True
                    apres_moitie()

    async with httpx.AsyncClient(base_url=f"http://localhost:{PORT}", timeout=10) as client:
        await asyncio.gather(*(travailleur(client) for _ in range(64)))
    return acceptes


def tuer_un_writer():
    pod = kubectl("get", "pods", "-l", "app=ecrivain", "-o", "name").split()[0]
    kubectl("delete", pod, "--grace-period=0", "--force", "--wait=false")
    print(f"     writer {pod} tué en plein flux", flush=True)


def main():
    sql(f"SELECT lake.creer_table('{TABLE}', 'client text, montant numeric(12,2), statut text')")
    sql(f"SELECT lake.creer_table('{RAFALE}', 'n int')")
    par_id, attendu = plan_ecritures()
    observateur = Observateur()
    observateur.start()
    kubectl("run", "rafale", "--image=lakehouse-ecrivain:1.0", "--image-pull-policy=Never", "--restart=Never",
            "--command", "--", "python", "-c", SCRIPT_RAFALE)
    print(f"     rafale de {NB_RAFALE} messages lancée dans le cluster", flush=True)
    passerelle = tunnel("svc/passerelle", PORT, 8000)
    try:
        debut = time.monotonic()
        acceptes = asyncio.run(envoyer(par_id, tuer_un_writer))
        duree = time.monotonic() - debut
        verifier(acceptes == sum(len(m) for m in par_id.values()),
                 f"{acceptes} écritures acceptées (202) en {duree:.0f} s, soit {acceptes / duree:.0f}/s via port-forward")
    finally:
        passerelle.terminate()

    verifier(kubectl("wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/rafale", "--timeout=300s") != "",
             f"rafale de {NB_RAFALE} messages acceptée par Kafka")
    vivantes = sum(1 for v in attendu.values() if v is not None)
    debut = time.monotonic()
    while time.monotonic() - debut < 420:
        if sql(f"SELECT count(*) FROM vue.{TABLE}") == str(vivantes) and \
           sql(f"SELECT count(*) FROM vue.{TABLE}_historique") == str(acceptes) and \
           sql(f"SELECT count(*) FROM vue.{RAFALE}") == str(NB_RAFALE):
            break
        time.sleep(3)
    print(f"     tout absorbé {time.monotonic() - debut:.0f} s après la fin de l'envoi", flush=True)
    verifier(sql(f"SELECT count(*) FROM vue.{RAFALE}") == str(NB_RAFALE), f"rafale : {NB_RAFALE} lignes, aucune perdue")

    lignes = dict(ligne.split("|") for ligne in sql(f"SELECT id, montant::int FROM vue.{TABLE}").splitlines())
    perdues = [i for i, v in attendu.items() if v is not None and i not in lignes]
    fausses = [i for i, v in attendu.items() if v is not None and i in lignes and int(lignes[i]) != v]
    ressuscitees = [i for i, v in attendu.items() if v is None and i in lignes]
    verifier(not perdues, f"aucune ligne perdue ({len(lignes)} lignes, {vivantes} attendues)")
    verifier(not fausses, f"chaque ligne a son dernier état ({len(fausses)} en écart)")
    verifier(not ressuscitees, f"aucune ligne supprimée visible ({len(ressuscitees)})")
    verifier(sql(f"SELECT count(*) - count(DISTINCT id) FROM vue.{TABLE}") == "0", "aucune ligne en double")
    historique = sql(f"SELECT count(*) FROM vue.{TABLE}_historique")
    verifier(historique == str(acceptes), f"historique : chaque message accepté une seule fois ({historique}/{acceptes})")

    # --- KEDA : writers ajoutés pendant le pic, retirés au calme -------------------------------------------
    observateur.actif = False
    verifier(observateur.maximum > 1, f"KEDA a ajouté des writers pendant le pic (jusqu'à {observateur.maximum})")
    debut = time.monotonic()
    while time.monotonic() - debut < 240 and nb_writers() > 1:
        time.sleep(5)
    verifier(nb_writers() == 1, f"writers retirés au calme ({nb_writers()} après {time.monotonic() - debut:.0f} s)")
    print("Chemin d'écriture : tous les contrôles passent")


main()
