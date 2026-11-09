"""Tests du partage des rôles : Spark ÉCRIT les tables agrégées, Trino les LIT.

- les agrégats donnent les bons chiffres (mini jeu de données) ;
- écrits par Spark en Iceberg, ils sont lus à l'identique par Trino ;
- une réécriture par Spark est visible dans Trino (nouvelle version de la table) ;
- Trino ne peut pas les modifier ;
- le traitement planifié s'identifie comme "pipeline".
"""
import shutil
import subprocess
import uuid
from datetime import date

import pytest
from conftest import trier

from lakehouse import agregats
from lakehouse.connexion import FICHIER_TOKENS, nouvelle_session
from lakehouse.donnees import CATALOGUE


# --- les chiffres ------------------------------------------------------------------------------

def test_ca_jour_pays_categorie(petites_tables):
    """La vente 105 (client inconnu) est exclue ; une ligne par jour, pays et catégorie."""
    res = agregats.calculer(petites_tables)["ca_jour_pays_categorie"].collect()
    assert trier(res) == trier([
        {"date": date(2026, 1, 15), "pays": "France", "categorie": "livre", "ca": 20.0, "nb_ventes": 1, "quantite": 1},
        {"date": date(2026, 2, 28), "pays": "France", "categorie": "livre", "ca": 40.0, "nb_ventes": 1, "quantite": 2},
        {"date": date(2026, 3, 2), "pays": "France", "categorie": "jeu", "ca": 150.0, "nb_ventes": 1, "quantite": 3},
        {"date": date(2026, 3, 31), "pays": "Allemagne", "categorie": "livre", "ca": 100.0, "nb_ventes": 1, "quantite": 5},
        {"date": date(2026, 4, 1), "pays": "Allemagne", "categorie": "jeu", "ca": 50.0, "nb_ventes": 1, "quantite": 1},
    ])


def test_resume_clients(petites_tables):
    """Le client 3, sans achat, est présent avec 0 achat."""
    res = agregats.calculer(petites_tables)["resume_clients"].collect()
    assert trier(res) == trier([
        {"id_client": 1, "nom": "Alice SARL", "pays": "France", "nb_achats": 3, "ca": 210.0,
         "derniere_vente": date(2026, 3, 2)},
        {"id_client": 2, "nom": "Bob GmbH", "pays": "Allemagne", "nb_achats": 2, "ca": 150.0,
         "derniere_vente": date(2026, 4, 1)},
        {"id_client": 3, "nom": "Carla SpA", "pays": "Italie", "nb_achats": 0, "ca": 0.0, "derniere_vente": None},
    ])


# --- Spark écrit, Trino lit --------------------------------------------------------------------

@pytest.fixture(scope="module")
def dashboard(spark, namespace_test):
    """Les agrégats écrits par Spark dans un schéma de test, comme le ferait pipeline.py."""
    cible = f"{CATALOGUE}.test_dashboard_{uuid.uuid4().hex[:8]}"
    tables = {nom: spark.table(f"{namespace_test}.{nom}") for nom in ("ventes", "clients", "produits")}
    lignes = agregats.ecrire(spark, tables, cible)
    try:
        yield cible, lignes
    finally:
        for nom in agregats.calculer(tables):
            spark.sql(f"DROP TABLE IF EXISTS {cible}.{nom} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {cible}")


def test_spark_ecrit_les_agregats(dashboard):
    _, lignes = dashboard
    assert lignes == {"ca_jour_pays_categorie": 5, "resume_clients": 3}


@pytest.mark.parametrize("table", ["ca_jour_pays_categorie", "resume_clients"])
def test_trino_lit_les_agregats_de_spark(table, dashboard, spark, trino):
    cible, _ = dashboard
    assert trier(trino("alice", f"SELECT * FROM {cible}.{table}")) == trier(spark.table(f"{cible}.{table}").collect())


@pytest.mark.parametrize("sql", [
    "INSERT INTO {t} SELECT * FROM {t}",
    "DELETE FROM {t}",
    "DROP TABLE {t}",
    "CREATE OR REPLACE TABLE {t} AS SELECT 1 AS x",
])
def test_trino_ne_peut_pas_modifier_les_agregats(sql, dashboard, trino):
    cible, _ = dashboard
    with pytest.raises(Exception, match="Access Denied"):
        trino("pipeline", sql.format(t=f"{cible}.resume_clients"))


def test_reecriture_spark_visible_dans_trino(dashboard, spark, trino, petites_tables):
    """Spark recalcule les agrégats sur moins de données : Trino voit aussitôt la nouvelle version."""
    cible, _ = dashboard
    avant = trino("bob", f"SELECT count(*) AS n FROM {cible}.ca_jour_pays_categorie")
    assert avant == [{"n": 5}]
    tables = dict(petites_tables, ventes=petites_tables["ventes"].where("id_vente <= 102"))
    try:
        agregats.ecrire(spark, tables, cible)
        assert trino("bob", f"SELECT count(*) AS n FROM {cible}.ca_jour_pays_categorie") == [{"n": 3}]
        versions = trino("bob", f'SELECT count(*) AS n FROM {cible}."ca_jour_pays_categorie$snapshots"')
        assert versions[0]["n"] >= 2  # l'ancienne version reste consultable (time travel)
    finally:
        agregats.ecrire(spark, petites_tables, cible)


# --- identité du traitement planifié -----------------------------------------------------------

def test_token_pipeline_coherent(tokens):
    """Le token du CronJob (secrets/pipeline.env) est bien celui de l'utilisateur pipeline."""
    env = (FICHIER_TOKENS.parent / "pipeline.env").read_text(encoding="utf-8")
    assert f"SPARK_CONNECT_TOKEN={tokens['pipeline']}" in env.splitlines()


def test_ecritures_tracees_au_nom_de_pipeline(tokens, namespace_test):
    """Une écriture faite avec le token pipeline apparaît à son nom dans le journal AUDIT."""
    if not shutil.which("kubectl"):
        pytest.skip("kubectl indisponible")
    marqueur = f"pipeline_{uuid.uuid4().hex[:8]}"
    session = nouvelle_session(tokens["pipeline"])
    try:
        session.sql(f"SELECT '{marqueur}' AS m").collect()
    finally:
        session.stop()
    logs = subprocess.run(["kubectl", "-n", "spark", "logs", "deploy/spark-connect", "--since=5m"],
                          capture_output=True, text=True, encoding="utf-8", check=True).stdout
    lignes = [l for l in logs.splitlines() if "AUDIT" in l and marqueur in l and "QUERY_START" in l]
    assert lignes and all('"user":"pipeline"' in l for l in lignes)
