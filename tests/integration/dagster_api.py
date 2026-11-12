"""Appels à l'API GraphQL de Dagster (celle de l'interface), partagés par les tests d'intégration des traitements."""
import time

LOCALISATION = {"repositoryLocationName": "lakehouse", "repositoryName": "__repository__"}
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
query($job: String) {
  runsOrError(filter: {pipelineName: $job}, limit: 50) {
    __typename ... on Runs { results { runId status startTime tags { key value } } } ... on PythonError { message }
  }
}"""
JOURNAL = """
query($id: ID!, $curseur: String) {
  logsForRun(runId: $id, afterCursor: $curseur, limit: 1000) {
    __typename
    ... on EventConnection { cursor hasMore events { __typename ... on MessageEvent { eventType stepKey message } } }
    ... on PythonError { message }
  }
}"""
CODE = """
query($r: RepositorySelector!) {
  repositoryOrError(repositorySelector: $r) {
    __typename ... on Repository { jobs { name } } ... on RepositoryNotFoundError { message } ... on PythonError { message }
  }
}"""
DEMONS = """
query { instance { daemonHealth { allDaemonStatuses { daemonType healthy required } } } }"""


def graphql(client, requete, **variables):
    reponse = client.post("/graphql", json={"query": requete, "variables": variables}, timeout=60)
    reponse.raise_for_status()
    corps = reponse.json()
    if corps.get("errors"):
        raise RuntimeError(f"GraphQL : {corps['errors']}")
    return corps["data"]


def attendre_code(client, job, delai=180):
    """Attend que l'interface voie le job (serveur de code chargé) ; recharge l'espace de travail."""
    debut, resultat = time.time(), None
    while time.time() - debut < delai:
        graphql(client, "mutation { reloadWorkspace { __typename } }")
        resultat = graphql(client, CODE, r=LOCALISATION)["repositoryOrError"]
        if resultat["__typename"] == "Repository" and any(j["name"] == job for j in resultat["jobs"]):
            return
        time.sleep(5)
    raise RuntimeError(f"code Dagster non chargé après {delai} s : {resultat}")


def lancer_run(client, job, run_config=None):
    selecteur = {**LOCALISATION, "jobName": job}
    resultat = graphql(client, LANCER, p={"selector": selecteur, "runConfigData": run_config or {}})["launchRun"]
    if resultat["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(f"lancement refusé : {resultat}")
    return resultat["run"]["runId"]


def statut(client, run_id):
    run = graphql(client, ETAT, id=run_id)["runOrError"]
    if run["__typename"] != "Run":
        raise RuntimeError(f"run introuvable : {run}")
    return run["status"]


def attendre_run(client, run_id, delai=1500):
    debut = time.time()
    while time.time() - debut < delai:
        etat = statut(client, run_id)
        if etat not in EN_COURS:
            print(f"     run {run_id[:8]} : {etat} en {time.time() - debut:.0f} s", flush=True)
            return etat
        time.sleep(5)
    raise RuntimeError(f"run {run_id} toujours en cours après {delai} s")


def runs(client, job):
    """Runs du job, du plus récent au plus ancien : [{runId, status, startTime, tags}]."""
    resultat = graphql(client, HISTORIQUE, job=job)["runsOrError"]
    if resultat["__typename"] != "Runs":
        raise RuntimeError(f"historique illisible : {resultat}")
    return resultat["results"]


def journal(client, run_id):
    tous, curseur = [], None
    while True:
        page = graphql(client, JOURNAL, id=run_id, curseur=curseur)["logsForRun"]
        if page["__typename"] != "EventConnection":
            raise RuntimeError(f"journal du run illisible : {page}")
        tous += [e for e in page["events"] if e.get("eventType")]
        curseur = page["cursor"]
        if not page["hasMore"]:
            return tous


def essais(client, run_id):
    """Nombre de tentatives de chaque étape : 1 démarrage (STEP_START) + les relances (STEP_RESTARTED)."""
    compte = {}
    for e in journal(client, run_id):
        if e["eventType"] in ("STEP_START", "STEP_RESTARTED"):
            compte[e["stepKey"]] = compte.get(e["stepKey"], 0) + 1
    return compte


def journal_echec(client, run_id):
    for e in journal(client, run_id):
        if e["eventType"] in ("STEP_FAILURE", "RUN_FAILURE", "ENGINE_EVENT") and e.get("message"):
            print(f"     {e['eventType']} {e.get('stepKey') or ''} : {e['message'][:600]}")


def demons(client):
    return {d["daemonType"]: d["healthy"] for d in
            graphql(client, DEMONS)["instance"]["daemonHealth"]["allDaemonStatuses"] if d["required"]}
