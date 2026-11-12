"""Test d'intégration des événements (US-11) : topic Kafka « events » → lake.bronze.events, lancé par Dagster, sur kind.

  1. 12 000 événements envoyés : le retard dépasse le seuil (5 000 en CI), le capteur Dagster lance le traitement,
     Spark les charge par lots puis s'arrête ;
  2. pendant ce run, APRÈS qu'un premier lot a été validé, le pod pilote Spark est supprimé brutalement : l'étape
     échoue, Dagster la relance, la reprise se fait là où elle s'était arrêtée ;
  3. bronze contient exactement les 12 000 messages : chaque (partition, offset) une fois, sans trou ;
  4. 10 événements, sous le seuil : ils arrivent dans bronze en moins de 15 min (délai maximal entre deux
     chargements, réduit à 90 s en CI) ;
  5. à la fin : groupe « lakehouse-events » sans retard, plus aucun pod de traitement ni executor.

    echo '127.0.0.1 minio' | sudo tee -a /etc/hosts
    python tests/integration/tester_evenements.py     (cluster déployé : tests/integration/evenements)
Dépendances : pip install httpx "pyiceberg[pyarrow]"
"""
import time

import httpx

import dagster_api as api
from commun import PORT_POLARIS, PORT_S3, catalogue, env, kubectl, tunnel, verifier, verifier_hote_minio

PORT_DAGSTER = 13001
TABLE = "bronze.events"
NB_PIC, NB_CALME = 12000, 10
CAPTEUR = """
query($s: SensorSelector!) {
  sensorOrError(sensorSelector: $s) {
    __typename ... on Sensor { sensorState { status } } ... on SensorNotFoundError { message } ... on PythonError { message }
  }
}"""


def produire(nombre):
    """Envoie `nombre` messages dans le topic events depuis le pod Kafka (acks=all)."""
    kubectl("exec", "lakehouse-noeuds-0", "--", "bin/kafka-producer-perf-test.sh", "--topic", "events",
            "--num-records", str(nombre), "--record-size", "200", "--throughput", "-1",
            "--producer-props", "bootstrap.servers=localhost:9092", "acks=all")


def fin_du_topic():
    sortie = kubectl("exec", "lakehouse-noeuds-0", "--", "bin/kafka-get-offsets.sh", "--bootstrap-server",
                     "localhost:9092", "--topic", "events")
    return {int(ligne.split(":")[1]): int(ligne.split(":")[2]) for ligne in sortie.splitlines() if ligne.count(":") >= 2}


def retard_groupe():
    """Retard du groupe lakehouse-events (fin du topic − position enregistrée)."""
    sortie = kubectl("exec", "lakehouse-noeuds-0", "--", "bin/kafka-consumer-groups.sh", "--bootstrap-server",
                     "localhost:9092", "--describe", "--group", "lakehouse-events")
    positions = {}
    for ligne in sortie.splitlines():
        champs = ligne.split()
        if len(champs) > 3 and champs[1] == "events" and champs[3].isdigit():
            positions[int(champs[2])] = int(champs[3])
    return sum(fin - positions.get(p, 0) for p, fin in fin_du_topic().items())


def lignes_bronze(cat):
    try:
        table = cat.load_table(TABLE)
    except Exception:  # noqa: BLE001 : pas encore créée
        return []
    table.refresh()
    if table.current_snapshot() is None:
        return []
    return table.scan(selected_fields=("partition", "offset")).to_arrow().to_pylist()


def run_du_capteur(client, deja_vus, delai=600):
    """Attend un run « evenements » lancé par le capteur, qui n'est pas dans deja_vus ; renvoie son id."""
    debut = time.time()
    while time.time() - debut < delai:
        for run in api.runs(client, "evenements"):
            tags = {t["key"]: t["value"] for t in run["tags"]}
            if run["runId"] not in deja_vus and tags.get("dagster/sensor_name") == "evenements_en_attente":
                print(f"     run {run['runId'][:8]} lancé par le capteur ({tags.get('raison')}) "
                      f"après {time.time() - debut:.0f} s", flush=True)
                return run["runId"], tags.get("raison", "")
        time.sleep(5)
    raise RuntimeError(f"aucun run lancé par le capteur en {delai} s")


def pilote():
    noms = kubectl("get", "pods", "-l", "app=traitement,traitement=evenements",
                   "--field-selector=status.phase=Running", "-o", "name").split()
    return noms[0] if noms else None


def main():
    verifier_hote_minio()
    trino = env("s3-trino.env")
    tunnels = [tunnel("svc/minio", PORT_S3, 9000), tunnel("svc/polaris", PORT_POLARIS, 8181),
               tunnel("svc/dagster-webserver", PORT_DAGSTER, 3000)]
    try:
        cat = catalogue(env("polaris-trino.env")["POLARIS_CREDENTIAL"],
                        (trino["TRINO_S3_ACCESS_KEY"], trino["TRINO_S3_SECRET_KEY"]))
        client = httpx.Client(base_url=f"http://localhost:{PORT_DAGSTER}")
        api.attendre_code(client, "evenements")
        etat = api.graphql(client, CAPTEUR, s={**api.LOCALISATION, "sensorName": "evenements_en_attente"})
        verifier(etat["sensorOrError"].get("sensorState", {}).get("status") == "RUNNING", "capteur du retard actif")

        print(f"1. {NB_PIC} événements : le retard dépasse le seuil, le capteur lance le chargement")
        produire(NB_PIC)
        run1, raison = run_du_capteur(client, set())
        verifier("seuil" in raison, f"lancé parce que le retard dépasse le seuil ({raison})")

        print("2. Pilote Spark supprimé brutalement après un premier lot validé : reprise sans perte ni doublon")
        debut, partiel = time.time(), 0
        while time.time() - debut < 600:
            partiel = len(lignes_bronze(cat))
            if partiel > 0 or api.statut(client, run1) not in api.EN_COURS:
                break
            time.sleep(2)
        nom_pilote = pilote()
        verifier(0 < partiel < NB_PIC and nom_pilote is not None,
                 f"premier lot validé ({partiel} messages sur {NB_PIC}), pilote encore en cours")
        kubectl("delete", nom_pilote, "--grace-period=0", "--force", "--wait=false")
        print(f"     {nom_pilote} supprimé", flush=True)
        statut = api.attendre_run(client, run1)
        if statut != "SUCCESS":
            api.journal_echec(client, run1)
        verifier(statut == "SUCCESS", "run réussi malgré l'arrêt brutal")
        # reprise par une nouvelle tentative Dagster (Job en échec) ou par un nouveau pod du même Job : dans les deux
        # cas, evenements.py repart du dernier lot validé ; ce qui compte est le contenu de bronze (point 3)
        print(f"     tentatives : {api.essais(client, run1)}", flush=True)

        print("3. bronze.events : exactement les messages du topic")
        lignes = lignes_bronze(cat)
        cles = {(r["partition"], r["offset"]) for r in lignes}
        fins = fin_du_topic()
        attendues = {(p, o) for p, fin in fins.items() for o in range(fin)}
        verifier(len(lignes) == NB_PIC, f"{len(lignes)} lignes pour {NB_PIC} messages envoyés")
        verifier(len(cles) == len(lignes), f"aucun doublon ({len(lignes) - len(cles)})")
        verifier(cles == attendues, f"aucun manquant ({len(attendues - cles)}), aucun en trop ({len(cles - attendues)})")
        verifier(retard_groupe() == 0, "position enregistrée dans le groupe lakehouse-events : retard 0")

        print(f"4. {NB_CALME} événements sous le seuil : dans bronze en moins de 15 min")
        envoi = time.time()
        produire(NB_CALME)
        run2, raison = run_du_capteur(client, {run1})
        verifier("seuil" not in raison, f"lancé par le délai maximal, pas par le seuil ({raison})")
        verifier(api.attendre_run(client, run2) == "SUCCESS", "run réussi")
        attente = time.time() - envoi
        total = len(lignes_bronze(cat))
        verifier(total == NB_PIC + NB_CALME, f"{total} lignes dans bronze")
        verifier(attente < 900, f"arrivés dans bronze {attente:.0f} s après l'envoi (moins de 15 min)")

        print("5. Ressources éphémères")
        reste = None
        for _ in range(36):
            reste = (len(kubectl("get", "pods", "-l", "app=traitement", "-o", "name").split()),
                     len(kubectl("get", "pods", "-l", "spark-role=executor", "-o", "name").split()))
            if reste == (0, 0):
                break
            time.sleep(5)
        verifier(reste == (0, 0), f"plus aucun pilote ni executor ({reste})")
    finally:
        for t in tunnels:
            t.terminate()


if __name__ == "__main__":
    main()
