"""Orchestration Dagster des traitements Spark à la demande (US-10).

Chaque étape d'un traitement est un Job Kubernetes éphémère : son pod est le pilote Spark (image spark-lakehouse,
k8s/traitements/lancer.sh), Spark crée puis supprime ses executors, et le Job disparaît après DUREE_VIE_JOBS_S.
Décision : docs/decisions/002-dagster-sans-spark-operator.md

    dagster code-server start -h 0.0.0.0 -p 4000 -m orchestration
"""
import collections
import contextlib
import io
import os

from dagster import (Backoff, Config, DefaultScheduleStatus, Definitions, OpExecutionContext, RetryPolicy,
                     ScheduleDefinition, job, op)

IMAGE_SPARK = os.environ.get("IMAGE_SPARK", "spark-lakehouse:4.1.3")
POLITIQUE_TIRAGE = os.environ.get("POLITIQUE_TIRAGE_IMAGES", "Never")
NAMESPACE = os.environ.get("NAMESPACE", "spark")
DUREE_VIE_JOBS_S = int(os.environ.get("DUREE_VIE_JOBS_S", "300"))
# CPU réservé par le pilote et par chaque executor (réduit en CI : tests/integration/orchestration)
CPU_PILOTE = os.environ.get("CPU_PILOTE", "250m")
CPU_EXECUTEUR = os.environ.get("CPU_EXECUTEUR", "500m")

# 3 essais au plus par étape (attente de 20 s puis 40 s) ; chaque étape est un commit Iceberg atomique, donc
# relançable sans donnée partielle
RELANCES = RetryPolicy(max_retries=2, delay=float(os.environ.get("DELAI_RELANCE_S", "20")), backoff=Backoff.EXPONENTIAL)


class EtapeConfig(Config):
    """echec : nom d'une étape à faire échouer pendant son écriture (tests de non-régression uniquement)."""
    echec: str = ""


def parametres_job(script: str, etape: str, echec: str = "") -> dict:
    """Paramètres du Job Kubernetes d'une étape Spark (séparés pour être testés sans cluster)."""
    env = [f"SPARK_EXECUTOR_IMAGE={IMAGE_SPARK}", f"SPARK_EXECUTOR_PULL_POLICY={POLITIQUE_TIRAGE}",
           f"SPARK_EXECUTOR_REQUEST_CORES={CPU_EXECUTEUR}", "S3_ENDPOINT=minio:9000"]
    if echec:
        env.append(f"ECHEC={echec}")
    return {
        "image": IMAGE_SPARK,
        "image_pull_policy": POLITIQUE_TIRAGE,
        "command": ["bash", "/traitements/lancer.sh"],
        "args": [script, etape],
        "namespace": NAMESPACE,
        "service_account_name": "spark",            # droit de créer les pods executors
        "env_vars": env,
        "env_secrets": ["s3-credentials", "polaris-spark"],
        "volumes": [{"name": "traitements", "config_map": {"name": "traitements"}}],
        "volume_mounts": [{"name": "traitements", "mount_path": "/traitements"}],
        "labels": {"app": "traitement", "traitement": script.removesuffix(".py"), "etape": etape},
        "resources": {"requests": {"cpu": CPU_PILOTE, "memory": "1536Mi"}, "limits": {"memory": "2Gi"}},
        # une tentative = un Job ; la relance est faite par Dagster. Le Job disparaît après DUREE_VIE_JOBS_S.
        "job_spec_config": {"ttl_seconds_after_finished": DUREE_VIE_JOBS_S, "backoff_limit": 0},
        "timeout": 3600,
    }


class JournalPilote(io.TextIOBase):
    """Garde les dernières lignes du journal du pod pilote (execute_k8s_job les écrit sur la sortie standard)."""

    def __init__(self, taille: int = 300):
        self.lignes: collections.deque = collections.deque(maxlen=taille)
        self._reste = ""

    def write(self, texte: str) -> int:
        *completes, self._reste = (self._reste + texte).split("\n")
        self.lignes.extend(completes)
        return len(texte)

    def contenu(self) -> str:
        lignes = [*self.lignes, self._reste] if self._reste else list(self.lignes)
        return "\n".join(lignes[-self.lignes.maxlen:])


def lancer_spark(context: OpExecutionContext, script: str, etape: str, echec: str = "") -> None:
    from dagster_k8s import execute_k8s_job
    context.log.info(f"{script} {etape} : tentative {context.retry_number + 1} sur 3")
    journal = JournalPilote()
    try:
        with contextlib.redirect_stdout(journal):
            execute_k8s_job(context=context, **parametres_job(script, etape, echec))
    except BaseException:
        context.log.error(f"journal du pilote Spark ({etape}, fin) :\n{journal.contenu()}")
        raise
    context.log.info(f"journal du pilote Spark ({etape}, fin) :\n{journal.contenu()}")


@op(retry_policy=RELANCES, description="Fichiers CSV de s3://sources/ventes → lake.bronze.ventes (une fois par fichier)")
def ventes_bronze(context: OpExecutionContext, config: EtapeConfig) -> str:
    lancer_spark(context, "ventes.py", "bronze", config.echec if config.echec == "bronze" else "")
    return "bronze"


@op(retry_policy=RELANCES, description="lake.bronze.ventes → lake.silver.ventes (typées, valides, dédoublonnées)")
def ventes_silver(context: OpExecutionContext, config: EtapeConfig, bronze: str) -> str:
    lancer_spark(context, "ventes.py", "silver", config.echec if config.echec == "silver" else "")
    return "silver"


@op(retry_policy=RELANCES, description="lake.silver.ventes → lake.gold.ventes_par_jour (remplacée en une fois)")
def ventes_gold(context: OpExecutionContext, config: EtapeConfig, silver: str) -> None:
    lancer_spark(context, "ventes.py", "gold", config.echec if config.echec == "gold" else "")


@job(description="Ventes : fichiers → bronze → silver → gold", tags={"traitement": "ventes"})
def ventes():
    ventes_gold(ventes_silver(ventes_bronze()))


chaque_heure = ScheduleDefinition(name="ventes_chaque_heure", job=ventes, cron_schedule="5 * * * *",
                                  execution_timezone="Europe/Paris", default_status=DefaultScheduleStatus.RUNNING)

defs = Definitions(jobs=[ventes], schedules=[chaque_heure])
