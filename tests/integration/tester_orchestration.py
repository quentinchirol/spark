"""Test d'intégration de l'orchestration (US-10) : Dagster lance le traitement Spark « ventes » sur kind.

  1. des fichiers CSV déposés dans s3://sources/ventes/ ; un run Dagster les passe en bronze → silver → gold
     (une étape = un Job Kubernetes éphémère : pilote Spark + executors) ; contenu exact des trois tables ;
  2. un nouveau fichier et un nouveau run : seul le nouveau fichier est chargé, la vente modifiée est mise à jour,
     aucun doublon ;
  3. échec provoqué PENDANT l'écriture de gold : l'étape est essayée 3 fois puis le run échoue ; la table gold n'a
     pas bougé (aucune donnée partielle) ; le run suivant, normal, rattrape ;
  4. à la fin, plus aucun pod de traitement ni executor, et les Jobs Kubernetes disparaissent ;
  5. maintenance (US-12) : 15 petits fichiers compactés en 1, 10 derniers snapshots gardés, fichier orphelin
     supprimé ; une table du namespace « app » (celui de pg_lake) n'est pas touchée ; tables ventes inchangées ;
  6. dans Dagster : l'historique des runs et leur résultat ; la planification horaire s'active (prochain lancement
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
import pyarrow as pa
from pyarrow import fs

import dagster_api as api
from commun import PORT_POLARIS, PORT_S3, catalogue, env, kubectl, tunnel, verifier, verifier_hote_minio
from dagster_api import attendre_run, essais, graphql, journal_echec

PORT_DAGSTER = 13000
ETAPES = ("ventes_bronze", "ventes_silver", "ventes_gold")
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


def lancer_run(client, echec=""):
    return api.lancer_run(client, "ventes", {"ops": {etape: {"config": {"echec": echec}} for etape in ETAPES}})


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

        api.attendre_code(client, "ventes")
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

        print("5. Maintenance : compaction, 10 snapshots gardés, orphelins supprimés ; tables app.* (pg_lake) intactes")
        ecriture = catalogue(env("polaris-spark.env")["POLARIS_CREDENTIAL"],
                             (s3_spark["AWS_ACCESS_KEY_ID"], s3_spark["AWS_SECRET_ACCESS_KEY"]))
        schema = pa.schema([("n", pa.int64())])
        for namespace in ("essai", "app"):
            ecriture.create_namespace_if_not_exists(namespace)
        petits = ecriture.create_table_if_not_exists("essai.petits_fichiers", schema=schema)
        for i in range(15):       # 15 écritures : 15 petits fichiers, 15 snapshots
            petits.append(pa.table({"n": list(range(i * 10, i * 10 + 10))}, schema=schema))
        # « app » : namespace des tables écrites par pg_lake, que la maintenance Spark ne doit jamais toucher
        temoin = ecriture.create_table_if_not_exists("app.temoin", schema=schema)
        for i in range(3):
            temoin.append(pa.table({"n": [i]}, schema=schema))
        petits.refresh()
        temoin.refresh()
        avant = len(list(petits.scan().plan_files()))
        verifier(avant == 15 and len(petits.metadata.snapshots) == 15, f"essai.petits_fichiers : {avant} fichiers, "
                 f"{len(petits.metadata.snapshots)} snapshots avant la maintenance")
        orphelin = petits.location().removeprefix("s3://") + "/data/orphelin-ecriture-echouee.parquet"
        with s3.open_output_stream(orphelin) as flux:
            flux.write(b"fichier jamais reference par un snapshot")
        temoin_avant = (temoin.current_snapshot().snapshot_id, len(temoin.metadata.snapshots),
                        len(list(temoin.scan().plan_files())))
        gold_avant, bronze_avant = gold_par_jour(cat), len(lignes(cat, "bronze.ventes"))
        run5 = api.lancer_run(client, "maintenance")
        statut5 = attendre_run(client, run5)
        if statut5 != "SUCCESS":
            journal_echec(client, run5)
        verifier(statut5 == "SUCCESS", "maintenance réussie")
        petits.refresh()
        apres = len(list(petits.scan().plan_files()))
        verifier(apres == 1, f"petits fichiers compactés ({avant} → {apres})")
        verifier(len(petits.scan().to_arrow()) == 150, "aucune ligne perdue par la compaction (150)")
        verifier(len(petits.metadata.snapshots) == 10, f"10 derniers snapshots gardés ({len(petits.metadata.snapshots)})")
        verifier(s3.get_file_info(orphelin).type == fs.FileType.NotFound, "fichier orphelin supprimé")
        temoin.refresh()
        temoin_apres = (temoin.current_snapshot().snapshot_id, len(temoin.metadata.snapshots),
                        len(list(temoin.scan().plan_files())))
        verifier(temoin_apres == temoin_avant, f"table app.temoin (pg_lake) intacte {temoin_apres}")
        messages = " ".join(e.get("message") or "" for e in api.journal(client, run5))
        verifier("namespaces ignorés : app" in messages, "namespace app ignoré par la maintenance (journal du run)")
        verifier(gold_par_jour(cat) == gold_avant and len(lignes(cat, "bronze.ventes")) == bronze_avant,
                 "tables ventes : contenu inchangé après maintenance")

        print("6. Dagster : historique et résultat des runs, planification active, démons en bonne santé")
        historique = api.runs(client, "ventes")
        statuts = {r["runId"]: r["status"] for r in historique}
        attendu_runs = {run1: "SUCCESS", run2: "SUCCESS", run3: "FAILURE", run4: "SUCCESS"}
        verifier(all(statuts.get(i) == s for i, s in attendu_runs.items()),
                 f"les 4 runs et leur résultat dans l'historique ({len(historique)} run(s))")
        selecteur = {**api.LOCALISATION, "scheduleName": "ventes_chaque_heure"}
        activation = graphql(client, ACTIVER, s=selecteur)["startSchedule"]
        verifier(activation["__typename"] == "ScheduleStateResult", f"planification activée ({activation})")
        planif = graphql(client, PLANIFICATION, s=selecteur)["scheduleOrError"]
        verifier(planif["__typename"] == "Schedule" and planif["scheduleState"]["status"] == "RUNNING"
                 and planif["cronSchedule"] == "5 * * * *" and planif["futureTicks"]["results"],
                 f"planification ventes_chaque_heure active, prochain lancement prévu ({planif.get('cronSchedule')})")
        demons = api.demons(client)
        verifier(demons.get("SCHEDULER") and demons.get("QUEUED_RUN_COORDINATOR") and all(demons.values()),
                 f"démons Dagster en bonne santé {demons}")
    finally:
        for t in tunnels:
            t.terminate()


if __name__ == "__main__":
    main()
