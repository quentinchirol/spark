"""Test d'intégration de l'orchestration (US-10) : Dagster lance le traitement Spark « ventes » sur kind.

  1. des fichiers CSV déposés dans s3://sources/ventes/ ; un run Dagster les passe en bronze → silver → gold
     (une étape = un Job Kubernetes éphémère : pilote Spark + executors) ; contenu exact des trois tables ;
  2. un nouveau fichier et un nouveau run : seul le nouveau fichier est chargé, la vente modifiée est mise à jour,
     aucun doublon ;
  3. échec provoqué PENDANT l'écriture de gold : l'étape est essayée 3 fois puis le run échoue ; la table gold n'a
     pas bougé (aucune donnée partielle) ; le run suivant, normal, rattrape ;
  4. à la fin, plus aucun pod de traitement ni executor, et les Jobs Kubernetes disparaissent ;
  5. dans Dagster : l'historique des runs et leur résultat ; la planification horaire s'active (prochain lancement
     prévu) et les démons (planificateur, file d'attente) sont en bonne santé. Les runs du test sont lancés par
     l'API : ils suivent le même chemin qu'un run planifié (file d'attente → démon → serveur de code → Jobs
     Kubernetes), seul le déclencheur diffère. La planification est arrêtée pendant le test (PLANIFICATIONS=arretees,
     tests/integration/orchestration) : un run planifié à h+5 se mêlerait aux runs vérifiés.

    echo '127.0.0.1 minio' | sudo tee -a /etc/hosts
    python tests/integration/tester_orchestration.py     (cluster déployé : tests/integration/orchestration)
Dépendances : pip install httpx "pyiceberg[pyarrow]"
"""
import time
from decimal import Decimal

import httpx
from pyarrow import fs

from commun import PORT_POLARIS, PORT_S3, catalogue, env, kubectl, tunnel, verifier, verifier_hote_minio

PORT_DAGSTER = 13000
SELECTEUR = {"repositoryLocationName": "lakehouse", "repositoryName": "__repository__", "jobName": "ventes"}
ETAPES = ("ventes_bronze", "ventes_silver", "ventes_gold")
EN_COURS = {"QUEUED", "NOT_STARTED", "STARTING", "STARTED", "MANAGED", "CANCELING"}

LANCER = """
mutation($p: ExecutionParams!) {
  launchRun(executionParams: $p) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
    ... on PythonError { message }
    ... on RunConfigValidationInvalid { errors { message } }
    ... on InvalidSubsetError { message }
    ... on PipelineNotFoundError { message }
    ... on RunConflict { message }
  }
}"""
ETAT = """
query($id: ID!) {
  runOrError(runId: $id) { __typename ... on Run { status startTime endTime } ... on RunNotFoundError { message } }
}"""
HISTORIQUE = """
query {
  runsOrError(filter: {pipelineName: "ventes"}, limit: 20) {
    __typename ... on Runs { results { runId status } } ... on PythonError { message }
  }
}"""
PLANIFICATION = """
query($s: ScheduleSelector!) {
  scheduleOrError(scheduleSelector: $s) {
    __typename
    ... on Schedule { cronSchedule executionTimezone scheduleState { status } futureTicks(limit: 1) { results { timestamp } } }
    ... on ScheduleNotFoundError { message }
    ... on PythonError { message }
  }
}"""
ACTIVER = """
mutation($s: ScheduleSelector!) {
  startSchedule(scheduleSelector: $s) {
    __typename ... on ScheduleStateResult { scheduleState { status } } ... on PythonError { message }
    ... on UnauthorizedError { message } ... on ScheduleNotFoundError { message }
  }
}"""
DEMONS = """
query { instance { daemonHealth { allDaemonStatuses { daemonType healthy required } } } }"""
EVENEMENTS = """
query($id: ID!, $curseur: String) {
  logsForRun(runId: $id, afterCursor: $curseur, limit: 1000) {
    __typename
    ... on EventConnection { cursor hasMore events { __typename ... on MessageEvent { eventType stepKey message } } }
    ... on PythonError { message }
  }
}"""

FICHIER_1 = """id,date,client,produit,quantite,prix_unitaire
v1,2026-11-01,alice,livre,2,10.00
v2,2026-11-01,bob,jeu,1,50.00
v3,2026-11-02,alice,jeu,3,50.00
v4,2026-11-02,carla,livre,1,12.50
v5,2026-11-02,bob,livre,abc,10.00
v6,pas-une-date,carla,jeu,1,50.00
"""
# v2 corrigée (quantité 4) et une nouvelle vente
FICHIER_2 = """id,date,client,produit,quantite,prix_unitaire
v2,2026-11-01,bob,jeu,4,50.00
v7,2026-11-03,dan,livre,5,10.00
"""
FICHIER_3 = """id,date,client,produit,quantite,prix_unitaire
v8,2026-11-03,erin,jeu,2,50.00
"""


def graphql(client, requete, **variables):
    reponse = client.post("/graphql", json={"query": requete, "variables": variables}, timeout=60)
    reponse.raise_for_status()
    corps = reponse.json()
    if corps.get("errors"):
        raise RuntimeError(f"GraphQL : {corps['errors']}")
    return corps["data"]


CODE = """
query($r: RepositorySelector!) {
  repositoryOrError(repositorySelector: $r) {
    __typename ... on Repository { jobs { name } } ... on RepositoryNotFoundError { message } ... on PythonError { message }
  }
}"""


def attendre_code(client, delai=180):
    """Attend que l'interface voie le job « ventes » (serveur de code chargé) ; recharge l'espace de travail."""
    selecteur = {"repositoryLocationName": SELECTEUR["repositoryLocationName"],
                 "repositoryName": SELECTEUR["repositoryName"]}
    debut, resultat = time.time(), None
    while time.time() - debut < delai:
        graphql(client, "mutation { reloadWorkspace { __typename } }")
        resultat = graphql(client, CODE, r=selecteur)["repositoryOrError"]
        if resultat["__typename"] == "Repository" and any(j["name"] == "ventes" for j in resultat["jobs"]):
            return
        time.sleep(5)
    raise RuntimeError(f"code Dagster non chargé après {delai} s : {resultat}")


def lancer_run(client, echec=""):
    config = {"ops": {etape: {"config": {"echec": echec}} for etape in ETAPES}}
    resultat = graphql(client, LANCER, p={"selector": SELECTEUR, "runConfigData": config})["launchRun"]
    if resultat["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(f"lancement refusé : {resultat}")
    return resultat["run"]["runId"]


def attendre_run(client, run_id, delai=1500):
    debut = time.time()
    while time.time() - debut < delai:
        run = graphql(client, ETAT, id=run_id)["runOrError"]
        if run["__typename"] != "Run":
            raise RuntimeError(f"run introuvable : {run}")
        if run["status"] not in EN_COURS:
            print(f"     run {run_id[:8]} : {run['status']} en {time.time() - debut:.0f} s", flush=True)
            return run["status"]
        time.sleep(5)
    raise RuntimeError(f"run {run_id} toujours en cours après {delai} s")


def evenements(client, run_id):
    tous, curseur = [], None
    while True:
        page = graphql(client, EVENEMENTS, id=run_id, curseur=curseur)["logsForRun"]
        if page["__typename"] != "EventConnection":
            raise RuntimeError(f"journal du run illisible : {page}")
        tous += [e for e in page["events"] if e.get("eventType")]
        curseur = page["cursor"]
        if not page["hasMore"]:
            return tous


def essais(client, run_id):
    """Nombre de tentatives de chaque étape : 1 démarrage (STEP_START) + les relances (STEP_RESTARTED)."""
    compte = {}
    for e in evenements(client, run_id):
        if e["eventType"] in ("STEP_START", "STEP_RESTARTED"):
            compte[e["stepKey"]] = compte.get(e["stepKey"], 0) + 1
    return compte


def journal_echec(client, run_id):
    for e in evenements(client, run_id):
        if e["eventType"] in ("STEP_FAILURE", "RUN_FAILURE", "ENGINE_EVENT") and e.get("message"):
            print(f"     {e['eventType']} {e.get('stepKey') or ''} : {e['message'][:600]}")


def deposer(s3, nom, contenu):
    with s3.open_output_stream(f"sources/ventes/{nom}") as flux:
        flux.write(contenu.encode())


def lignes(cat, table):
    return cat.load_table(table).scan().to_arrow().to_pylist()


def gold_par_jour(cat):
    return {str(r["date"]): (r["nb_ventes"], r["quantite_totale"], r["chiffre_affaires"])
            for r in lignes(cat, "gold.ventes_par_jour")}


def main():
    verifier_hote_minio()
    s3_spark, trino = env("s3.env"), env("s3-trino.env")
    tunnels = [tunnel("svc/minio", PORT_S3, 9000), tunnel("svc/polaris", PORT_POLARIS, 8181),
               tunnel("svc/dagster-webserver", PORT_DAGSTER, 3000)]
    try:
        s3 = fs.S3FileSystem(endpoint_override=f"localhost:{PORT_S3}", scheme="http", region="us-east-1",
                             access_key=s3_spark["AWS_ACCESS_KEY_ID"], secret_key=s3_spark["AWS_SECRET_ACCESS_KEY"])
        # lecture avec le principal « trino » (lecture seule) : ce que verront les outils d'analyse
        cat = catalogue(env("polaris-trino.env")["POLARIS_CREDENTIAL"],
                        (trino["TRINO_S3_ACCESS_KEY"], trino["TRINO_S3_SECRET_KEY"]))
        client = httpx.Client(base_url=f"http://localhost:{PORT_DAGSTER}")

        attendre_code(client)
        print("1. Fichiers → bronze → silver → gold, chaque étape dans un Job Kubernetes éphémère")
        deposer(s3, "2026-11-02.csv", FICHIER_1)
        run1 = lancer_run(client)
        statut = attendre_run(client, run1)
        if statut != "SUCCESS":
            journal_echec(client, run1)
        verifier(statut == "SUCCESS", "run ventes réussi")
        verifier(essais(client, run1) == {e: 1 for e in ETAPES}, "trois étapes, une tentative chacune")
        bronze = lignes(cat, "bronze.ventes")
        verifier(len(bronze) == 6 and all(isinstance(r["quantite"], str) for r in bronze),
                 f"bronze : les 6 lignes du fichier, telles quelles ({len(bronze)})")
        silver = {r["id"]: r for r in lignes(cat, "silver.ventes")}
        verifier(sorted(silver) == ["v1", "v2", "v3", "v4"], f"silver : 4 ventes valides, 2 écartées ({sorted(silver)})")
        verifier(silver["v4"]["prix_unitaire"] == Decimal("12.50") and silver["v3"]["quantite"] == 3, "silver : types")
        attendu = {"2026-11-01": (2, 3, Decimal("70.00")), "2026-11-02": (2, 4, Decimal("162.50"))}
        verifier(gold_par_jour(cat) == attendu, f"gold : ventes par jour exactes {gold_par_jour(cat)}")

        print("2. Nouveau fichier : seul lui est chargé, la vente corrigée est mise à jour, aucun doublon")
        deposer(s3, "2026-11-03.csv", FICHIER_2)
        run2 = lancer_run(client)
        verifier(attendre_run(client, run2) == "SUCCESS", "run ventes réussi")
        bronze = lignes(cat, "bronze.ventes")
        verifier(len(bronze) == 8, f"bronze : 6 + 2 lignes, premier fichier non rechargé ({len(bronze)})")
        silver = {r["id"]: r for r in lignes(cat, "silver.ventes")}
        verifier(sorted(silver) == ["v1", "v2", "v3", "v4", "v7"] and silver["v2"]["quantite"] == 4,
                 "silver : v2 mise à jour, v7 ajoutée, une ligne par vente")
        attendu = {"2026-11-01": (2, 6, Decimal("220.00")), "2026-11-02": (2, 4, Decimal("162.50")),
                   "2026-11-03": (1, 5, Decimal("50.00"))}
        verifier(gold_par_jour(cat) == attendu, "gold recalculé")

        print("3. Échec pendant l'écriture de gold : 3 essais, puis échec ; gold inchangé ; le run suivant rattrape")
        snapshot_gold = cat.load_table("gold.ventes_par_jour").current_snapshot().snapshot_id
        deposer(s3, "2026-11-04.csv", FICHIER_3)
        run3 = lancer_run(client, echec="gold")
        verifier(attendre_run(client, run3) == "FAILURE", "run en échec")
        compte = essais(client, run3)
        verifier(compte == {"ventes_bronze": 1, "ventes_silver": 1, "ventes_gold": 3},
                 f"gold essayé 3 fois, bronze et silver une fois ({compte})")
        verifier(cat.load_table("gold.ventes_par_jour").current_snapshot().snapshot_id == snapshot_gold,
                 "gold : aucun commit, aucune donnée partielle")
        verifier(gold_par_jour(cat) == attendu, "gold : contenu d'avant l'échec")
        verifier(len(lignes(cat, "silver.ventes")) == 6, "silver : à jour (v8), écrit avant l'échec")
        run4 = lancer_run(client)
        verifier(attendre_run(client, run4) == "SUCCESS", "run suivant réussi")
        attendu["2026-11-03"] = (2, 7, Decimal("150.00"))
        verifier(gold_par_jour(cat) == attendu, "gold rattrapé")
        verifier(len(lignes(cat, "bronze.ventes")) == 9, "bronze : aucun fichier rechargé deux fois")

        print("4. Ressources éphémères : ni pilote, ni executor, ni Job après les runs")
        reste = None
        for _ in range(36):
            pods = kubectl("get", "pods", "-l", "app=traitement", "-o", "name").split()
            executors = kubectl("get", "pods", "-l", "spark-role=executor", "-o", "name").split()
            jobs = kubectl("get", "jobs", "-l", "app=traitement", "-o", "name").split()
            reste = (len(pods), len(executors), len(jobs))
            if reste == (0, 0, 0):
                break
            time.sleep(5)
        verifier(reste == (0, 0, 0), f"plus rien ne tourne (pods, executors, Jobs : {reste})")

        print("5. Dagster : historique et résultat des runs, planification active, démons en bonne santé")
        historique = graphql(client, HISTORIQUE)["runsOrError"]["results"]
        statuts = {r["runId"]: r["status"] for r in historique}
        attendu_runs = {run1: "SUCCESS", run2: "SUCCESS", run3: "FAILURE", run4: "SUCCESS"}
        verifier(all(statuts.get(i) == s for i, s in attendu_runs.items()),
                 f"les 4 runs et leur résultat dans l'historique ({len(historique)} run(s))")
        selecteur = {"repositoryLocationName": SELECTEUR["repositoryLocationName"],
                     "repositoryName": SELECTEUR["repositoryName"], "scheduleName": "ventes_chaque_heure"}
        activation = graphql(client, ACTIVER, s=selecteur)["startSchedule"]
        verifier(activation["__typename"] == "ScheduleStateResult", f"planification activée ({activation})")
        planif = graphql(client, PLANIFICATION, s=selecteur)["scheduleOrError"]
        verifier(planif["__typename"] == "Schedule" and planif["scheduleState"]["status"] == "RUNNING"
                 and planif["cronSchedule"] == "5 * * * *" and planif["futureTicks"]["results"],
                 f"planification ventes_chaque_heure active, prochain lancement prévu ({planif.get('cronSchedule')})")
        demons = {d["daemonType"]: d["healthy"] for d in
                  graphql(client, DEMONS)["instance"]["daemonHealth"]["allDaemonStatuses"] if d["required"]}
        verifier(demons.get("SCHEDULER") and demons.get("QUEUED_RUN_COORDINATOR") and all(demons.values()),
                 f"démons Dagster en bonne santé {demons}")
    finally:
        for t in tunnels:
            t.terminate()


if __name__ == "__main__":
    main()
