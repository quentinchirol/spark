"""Orchestration Dagster (python/orchestration) : étapes, relances, planification, Jobs Kubernetes — sans cluster."""
import importlib

import dagster_k8s
import pytest
from dagster import DagsterInstance

import orchestration


@pytest.fixture
def orch(monkeypatch):
    """Module rechargé sans attente entre deux essais ; execute_k8s_job remplacé par un faux qui note les appels."""
    monkeypatch.setenv("DELAI_RELANCE_S", "0")
    module = importlib.reload(orchestration)
    appels, echecs = [], {}

    def faux_job(context, **parametres):
        etape = parametres["args"][1]
        appels.append((etape, context.retry_number, parametres))
        if echecs.get(etape, 0) > 0:
            echecs[etape] -= 1
            raise RuntimeError(f"Job Kubernetes {etape} en échec")

    monkeypatch.setattr(dagster_k8s, "execute_k8s_job", faux_job)
    yield module, appels, echecs
    monkeypatch.delenv("DELAI_RELANCE_S")
    importlib.reload(orchestration)


def test_definitions_chargees():
    job = orchestration.defs.get_job_def("ventes")
    assert {n.name for n in job.graph.nodes} == {"ventes_bronze", "ventes_silver", "ventes_gold"}
    deps = job.graph.dependency_structure
    assert deps.input_to_upstream_outputs_for_node("ventes_silver")
    assert deps.input_to_upstream_outputs_for_node("ventes_gold")
    assert not deps.input_to_upstream_outputs_for_node("ventes_bronze")


def test_trois_essais_au_plus_par_etape():
    job = orchestration.defs.get_job_def("ventes")
    for noeud in job.graph.nodes:
        politique = noeud.definition.retry_policy
        assert politique is not None and politique.max_retries == 2


def test_planification_horaire():
    planif = orchestration.defs.get_schedule_def("ventes_chaque_heure")
    assert planif.cron_schedule == "5 * * * *"
    assert planif.job_name == "ventes"
    assert planif.execution_timezone == "Europe/Paris"
    assert planif.default_status.value == "RUNNING"


def test_planifications_arretees_pour_les_tests(monkeypatch):
    monkeypatch.setenv("PLANIFICATIONS", "arretees")
    try:
        module = importlib.reload(orchestration)
        assert module.defs.get_schedule_def("ventes_chaque_heure").default_status.value == "STOPPED"
    finally:
        monkeypatch.delenv("PLANIFICATIONS")
        importlib.reload(orchestration)


def test_parametres_job_kubernetes():
    p = orchestration.parametres_job("ventes.py", "silver")
    assert p["command"] == ["bash", "/traitements/lancer.sh"] and p["args"] == ["ventes.py", "silver"]
    assert p["service_account_name"] == "spark"
    assert set(p["env_secrets"]) == {"s3-credentials", "polaris-spark"}
    assert p["volumes"][0]["config_map"]["name"] == "traitements"
    # une tentative = un Job, sans relance par Kubernetes ; le Job disparaît après sa fin
    assert p["job_spec_config"]["backoff_limit"] == 0
    assert p["job_spec_config"]["ttl_seconds_after_finished"] > 0
    assert f"SPARK_EXECUTOR_IMAGE={p['image']}" in p["env_vars"]
    assert not any(e.startswith("ECHEC=") for e in p["env_vars"])
    assert "ECHEC=gold" in orchestration.parametres_job("ventes.py", "gold", "gold")["env_vars"]


def test_run_complet_dans_l_ordre(orch):
    module, appels, _ = orch
    resultat = module.ventes.execute_in_process()
    assert resultat.success
    assert [a[0] for a in appels] == ["bronze", "silver", "gold"]


def test_echec_passager_relance(orch):
    module, appels, echecs = orch
    echecs["silver"] = 1
    resultat = module.ventes.execute_in_process()
    assert resultat.success
    assert [(a[0], a[1]) for a in appels] == [("bronze", 0), ("silver", 0), ("silver", 1), ("gold", 0)]


def test_echec_definitif_apres_trois_essais(orch):
    module, appels, echecs = orch
    echecs["gold"] = 99
    config = {"ops": {n: {"config": {"echec": "gold"}} for n in ("ventes_bronze", "ventes_silver", "ventes_gold")}}
    resultat = module.ventes.execute_in_process(run_config=config, raise_on_error=False)
    assert not resultat.success
    assert [(a[0], a[1]) for a in appels] == [("bronze", 0), ("silver", 0), ("gold", 0), ("gold", 1), ("gold", 2)]
    # l'échec provoqué n'est transmis qu'à l'étape visée
    assert all("ECHEC=gold" in a[2]["env_vars"] for a in appels if a[0] == "gold")
    assert not any("ECHEC=gold" in a[2]["env_vars"] for a in appels if a[0] != "gold")


def test_journal_du_pilote_recopie(orch, monkeypatch):
    """Les lignes écrites par execute_k8s_job (journal du pod) arrivent dans le journal d'événements du run."""
    module, _, echecs = orch

    def bavard(context, **parametres):
        print(f"pilote {parametres['args'][1]} : ligne 1")
        print("ligne 2", end="")
        if echecs.get(parametres["args"][1]):
            raise RuntimeError("échec")

    monkeypatch.setattr(dagster_k8s, "execute_k8s_job", bavard)
    echecs["gold"] = 1
    instance = DagsterInstance.ephemeral()
    resultat = module.ventes.execute_in_process(raise_on_error=False, instance=instance)
    messages = [e.user_message for e in instance.all_logs(resultat.run_id) if "journal du pilote" in e.user_message]
    assert any("pilote silver : ligne 1\nligne 2" in m for m in messages)
    assert any("pilote gold : ligne 1" in m for m in messages)


def test_journal_garde_les_dernieres_lignes():
    journal = orchestration.JournalPilote(taille=3)
    journal.write("a\nb\nc\n")
    journal.write("d\ne")
    assert journal.contenu() == "c\nd\ne"


# --- Événements (US-11) -----------------------------------------------------------------------------------------
from dagster import RunRequest, SkipReason, build_sensor_context  # noqa: E402


def test_job_evenements_une_etape_relancee_trois_fois_au_plus():
    job = orchestration.defs.get_job_def("evenements")
    assert [n.name for n in job.graph.nodes] == ["evenements_bronze"]
    assert job.graph.nodes[0].definition.retry_policy.max_retries == 2
    assert job.tags["traitement"] == "evenements"


def test_job_evenements_parametres(orch):
    module, appels, _ = orch
    assert module.evenements.execute_in_process().success
    etape, essai, parametres = appels[0]
    assert parametres["args"] == ["evenements.py", "bronze"]
    env = parametres["env_vars"]
    assert any(e.startswith("KAFKA_BOOTSTRAP=") for e in env) and "TOPIC_EVENEMENTS=events" in env
    assert any(e.startswith("MAX_PAR_PARTITION=") for e in env)


@pytest.mark.parametrize("retard, depuis, attendu", [
    (0, None, False),                 # rien en attente
    (10, 30, False),                  # peu de messages, dernier chargement récent : on attend
    (10, 600, True),                  # peu de messages mais délai maximal atteint (moins de 15 min au total)
    (10, None, True),                 # jamais chargé
    (100000, 30, True),               # seuil atteint : tout de suite
])
def test_decision_evenements(retard, depuis, attendu):
    assert (orchestration.decision_evenements(retard, depuis) is not None) is attendu


def test_capteur_lance_un_run_au_dela_du_seuil(monkeypatch):
    monkeypatch.setattr(orchestration, "retard_evenements", lambda: 250000)
    with DagsterInstance.ephemeral() as instance:
        contexte = build_sensor_context(instance=instance, cursor=str(__import__("time").time()))
        resultat = orchestration.evenements_en_attente(contexte)
    assert isinstance(resultat, RunRequest) and "seuil" in resultat.tags["raison"]


def test_capteur_attend_sous_le_seuil_puis_lance_au_delai(monkeypatch):
    import time as horloge
    monkeypatch.setattr(orchestration, "retard_evenements", lambda: 5)
    with DagsterInstance.ephemeral() as instance:
        recent = orchestration.evenements_en_attente(build_sensor_context(instance=instance, cursor=str(horloge.time())))
        ancien = orchestration.evenements_en_attente(
            build_sensor_context(instance=instance, cursor=str(horloge.time() - 3600)))
    assert isinstance(recent, SkipReason)
    assert isinstance(ancien, RunRequest)


@pytest.mark.parametrize("statut", ["NOT_STARTED", "STARTED"])
def test_capteur_ne_lance_rien_pendant_un_run(monkeypatch, statut):
    from dagster import DagsterRunStatus
    monkeypatch.setattr(orchestration, "retard_evenements", lambda: 10**9)
    with DagsterInstance.ephemeral() as instance:
        instance.create_run_for_job(orchestration.evenements, status=DagsterRunStatus[statut])
        resultat = orchestration.evenements_en_attente(build_sensor_context(instance=instance))
    assert isinstance(resultat, SkipReason) and "en cours" in str(resultat.skip_message)


def test_retard_evenements_options_du_client_kafka_valides(monkeypatch):
    """Les options passées au client Kafka existent dans la version installée (kafka-python les refuse sinon)."""
    import kafka
    options_connues = set(kafka.KafkaConsumer.DEFAULT_CONFIG)
    vues = {}

    class FauxConsommateur:
        def __init__(self, **options):
            vues.update(options)

        def partitions_for_topic(self, topic):
            return {0, 1}

        def end_offsets(self, partitions, timeout_ms=None):
            return {tp: 100 for tp in partitions}

        def beginning_offsets(self, partitions, timeout_ms=None):
            return {tp: 10 for tp in partitions}

        def committed(self, tp):
            return 40 if tp.partition == 0 else None

        def close(self):
            pass

    monkeypatch.setattr(kafka, "KafkaConsumer", FauxConsommateur)
    assert orchestration.retard_evenements() == (100 - 40) + (100 - 10)
    assert set(vues) <= options_connues, set(vues) - options_connues
