"""Orchestration Dagster des traitements Spark à la demande (US-10, US-11).

Chaque étape d'un traitement est un Job Kubernetes éphémère : son pod est le pilote Spark (image spark-lakehouse,
k8s/traitements/lancer.sh), Spark crée puis supprime ses executors, et le Job disparaît après DUREE_VIE_JOBS_S.
Décision : docs/decisions/002-dagster-sans-spark-operator.md

    dagster code-server start -h 0.0.0.0 -p 4000 -m orchestration
"""
import collections
import contextlib
import io
import os
import time

from dagster import (Backoff, Config, DagsterRunStatus, DefaultScheduleStatus, DefaultSensorStatus, Definitions,
                     OpExecutionContext, RetryPolicy, RunRequest, RunsFilter, ScheduleDefinition,
                     SensorEvaluationContext, SkipReason, job, op, sensor)
from dagster._core.storage.dagster_run import IN_PROGRESS_RUN_STATUSES

IMAGE_SPARK = os.environ.get("IMAGE_SPARK", "spark-lakehouse:4.1.3")
POLITIQUE_TIRAGE = os.environ.get("POLITIQUE_TIRAGE_IMAGES", "Never")
NAMESPACE = os.environ.get("NAMESPACE", "spark")
DUREE_VIE_JOBS_S = int(os.environ.get("DUREE_VIE_JOBS_S", "300"))
# CPU réservé par le pilote et par chaque executor (réduit en CI : tests/integration/orchestration)
CPU_PILOTE = os.environ.get("CPU_PILOTE", "250m")
CPU_EXECUTEUR = os.environ.get("CPU_EXECUTEUR", "500m")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "lakehouse-kafka-bootstrap:9092")

# 3 essais au plus par étape (attente de 20 s puis 40 s) ; chaque étape est un commit Iceberg atomique, donc
# relançable sans donnée partielle
RELANCES = RetryPolicy(max_retries=2, delay=float(os.environ.get("DELAI_RELANCE_S", "20")), backoff=Backoff.EXPONENTIAL)


class EtapeConfig(Config):
    """echec : nom d'une étape à faire échouer pendant son écriture (tests de non-régression uniquement)."""
    echec: str = ""


def parametres_job(script: str, etape: str, echec: str = "", env_supplementaire: tuple = ()) -> dict:
    """Paramètres du Job Kubernetes d'une étape Spark (séparés pour être testés sans cluster)."""
    env = [f"SPARK_EXECUTOR_IMAGE={IMAGE_SPARK}", f"SPARK_EXECUTOR_PULL_POLICY={POLITIQUE_TIRAGE}",
           f"SPARK_EXECUTOR_REQUEST_CORES={CPU_EXECUTEUR}", "S3_ENDPOINT=minio:9000", *env_supplementaire]
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


def lancer_spark(context: OpExecutionContext, script: str, etape: str, echec: str = "",
                 env_supplementaire: tuple = ()) -> None:
    from dagster_k8s import execute_k8s_job
    context.log.info(f"{script} {etape} : tentative {context.retry_number + 1} sur 3")
    journal = JournalPilote()
    try:
        with contextlib.redirect_stdout(journal):
            execute_k8s_job(context=context, **parametres_job(script, etape, echec, env_supplementaire))
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


# --- Événements (US-11) : topic Kafka « events » → lake.bronze.events -------------------------------------------
TOPIC_EVENEMENTS = "events"
GROUPE_EVENEMENTS = "lakehouse-events"        # position atteinte, enregistrée par k8s/traitements/evenements.py
# lancer dès que SEUIL_EVENEMENTS messages attendent, et au plus tard DELAI_MAX_EVENEMENTS_S après le dernier
# lancement s'il en reste au moins un : un événement est dans bronze en moins de 15 min (10 min + la durée du run)
SEUIL_EVENEMENTS = int(os.environ.get("SEUIL_EVENEMENTS", "100000"))
DELAI_MAX_EVENEMENTS_S = int(os.environ.get("DELAI_MAX_EVENEMENTS_S", "600"))
MAX_PAR_PARTITION = os.environ.get("MAX_PAR_PARTITION", "500000")


@op(retry_policy=RELANCES, description="Topic Kafka events → lake.bronze.events, par lots, sans perte ni doublon")
def evenements_bronze(context: OpExecutionContext) -> None:
    lancer_spark(context, "evenements.py", "bronze",
                 env_supplementaire=(f"KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP}", f"TOPIC_EVENEMENTS={TOPIC_EVENEMENTS}",
                                     f"MAX_PAR_PARTITION={MAX_PAR_PARTITION}"))


@job(description="Événements : topic Kafka events → bronze (ce qui est disponible, puis arrêt)",
     tags={"traitement": "evenements"})
def evenements():
    evenements_bronze()


def retard_evenements() -> int:
    """Messages du topic events pas encore chargés : fin du topic − position enregistrée (début du topic sinon)."""
    from kafka import KafkaConsumer, TopicPartition
    consommateur = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, group_id=GROUPE_EVENEMENTS,
                                 enable_auto_commit=False, request_timeout_ms=15000, api_version_auto_timeout_ms=10000)
    try:
        partitions = [TopicPartition(TOPIC_EVENEMENTS, p)
                      for p in sorted(consommateur.partitions_for_topic(TOPIC_EVENEMENTS) or [])]
        fins = consommateur.end_offsets(partitions, timeout_ms=15000)
        debuts = consommateur.beginning_offsets(partitions, timeout_ms=15000)
        return sum(fins[tp] - (consommateur.committed(tp) or debuts[tp]) for tp in partitions)
    finally:
        consommateur.close()


def decision_evenements(retard: int, depuis_dernier_lancement_s: float | None) -> str | None:
    """Raison de lancer un run « evenements » (None : ne rien lancer)."""
    if retard <= 0:
        return None
    if retard >= SEUIL_EVENEMENTS:
        return f"retard de {retard} messages (seuil {SEUIL_EVENEMENTS})"
    if depuis_dernier_lancement_s is None or depuis_dernier_lancement_s >= DELAI_MAX_EVENEMENTS_S:
        return f"{retard} message(s) en attente depuis le dernier chargement"
    return None


# capteurs actifs dès le déploiement ; CAPTEURS=arretes (tests qui ne les vérifient pas) : à activer dans l'interface
ETAT_CAPTEURS = (DefaultSensorStatus.STOPPED if os.environ.get("CAPTEURS") == "arretes"
                 else DefaultSensorStatus.RUNNING)


@sensor(job=evenements, minimum_interval_seconds=int(os.environ.get("INTERVALLE_CAPTEUR_S", "60")),
        default_status=ETAT_CAPTEURS, description="Lance « evenements » quand le retard du topic events le demande")
def evenements_en_attente(context: SensorEvaluationContext):
    # un run en file d'attente compte aussi : sinon chaque minute en ajouterait un
    statuts = [DagsterRunStatus.QUEUED, DagsterRunStatus.NOT_STARTED, *IN_PROGRESS_RUN_STATUSES]
    en_cours = context.instance.get_runs(RunsFilter(job_name="evenements", statuses=statuts), limit=1)
    if en_cours:
        return SkipReason("un run « evenements » est déjà en cours")
    maintenant = time.time()
    dernier = float(context.cursor) if context.cursor else None
    retard = retard_evenements()
    raison = decision_evenements(retard, None if dernier is None else maintenant - dernier)
    if raison is None:
        return SkipReason(f"{retard} message(s) en attente : pas encore de chargement")
    context.update_cursor(str(maintenant))
    return RunRequest(run_key=f"evenements-{int(maintenant)}", tags={"raison": raison})


# planifications actives dès le déploiement ; PLANIFICATIONS=arretees (tests) : à activer dans l'interface ou l'API
ETAT_PLANIFICATIONS = (DefaultScheduleStatus.STOPPED if os.environ.get("PLANIFICATIONS") == "arretees"
                       else DefaultScheduleStatus.RUNNING)

chaque_heure = ScheduleDefinition(name="ventes_chaque_heure", job=ventes, cron_schedule="5 * * * *",
                                  execution_timezone="Europe/Paris", default_status=ETAT_PLANIFICATIONS)

defs = Definitions(jobs=[ventes, evenements], schedules=[chaque_heure], sensors=[evenements_en_attente])
