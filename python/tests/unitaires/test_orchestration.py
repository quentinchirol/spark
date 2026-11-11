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
