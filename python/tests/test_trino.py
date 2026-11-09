"""Tests de Trino : identité individuelle, LECTURE SEULE (Spark seul écrit), mêmes résultats que Spark,
garde-fous (durée maximale, files d'attente par utilisateur)."""
import uuid

import pytest
from conftest import trier

from lakehouse import requetes
from lakehouse.requetes_sql import REQUETES_SQL


# --- identité ----------------------------------------------------------------------------------

@pytest.mark.parametrize("utilisateur", ["alice", "bob", "pipeline"])
def test_chacun_est_identifie(utilisateur, trino):
    assert trino(utilisateur, "SELECT current_user AS u") == [{"u": utilisateur}]


@pytest.mark.parametrize("utilisateur, token", [
    ("alice", "token-invalide"),
    ("inconnu", "token-invalide"),
    ("alice", "bob"),  # token de bob utilisé sous le nom d'alice
])
def test_mauvais_identifiants_refuses(utilisateur, token, trino, tokens):
    with pytest.raises(Exception, match="401|Invalid credentials"):
        trino(utilisateur, "SELECT 1", token=tokens.get(token, token))


# --- lecture seule -----------------------------------------------------------------------------

ECRITURES = {
    "create_table_as": "CREATE TABLE {ns}.essai AS SELECT 1 AS x",
    "create_table": "CREATE TABLE {ns}.essai (x integer)",
    "insert": "INSERT INTO {ns}.ventes SELECT * FROM {ns}.ventes",
    "update": "UPDATE {ns}.ventes SET montant = 0",
    "delete": "DELETE FROM {ns}.ventes",
    "merge": "MERGE INTO {ns}.clients c USING {ns}.clients s ON c.id_client = s.id_client "
             "WHEN MATCHED THEN UPDATE SET nom = 'x'",
    "truncate": "TRUNCATE TABLE {ns}.ventes",
    "drop_table": "DROP TABLE {ns}.ventes",
    "alter_table": "ALTER TABLE {ns}.ventes ADD COLUMN pirate varchar",
    "rename_table": "ALTER TABLE {ns}.ventes RENAME TO {ns}.ventes2",
    "create_view": "CREATE VIEW {ns}.vue AS SELECT 1 AS x",
    "create_schema": "CREATE SCHEMA lake.pirate",
    "optimize": "ALTER TABLE {ns}.ventes EXECUTE optimize",
    "expire_snapshots": "ALTER TABLE {ns}.ventes EXECUTE expire_snapshots(retention_threshold => '0s')",
}


def versions(spark, namespace):
    """Identifiants des versions (snapshots) de chaque table Iceberg du schéma."""
    return {nom: [r[0] for r in spark.sql(f"SELECT snapshot_id FROM {namespace}.{nom}.snapshots").collect()]
            for nom in ("ventes", "clients", "produits")}


@pytest.fixture(scope="module")
def versions_avant(spark, namespace_test):
    return versions(spark, namespace_test)


@pytest.mark.parametrize("utilisateur", ["alice", "pipeline"])
@pytest.mark.parametrize("nom", list(ECRITURES))
def test_trino_ne_peut_pas_ecrire(nom, utilisateur, trino, namespace_test, versions_avant):
    with pytest.raises(Exception, match="Access Denied"):
        trino(utilisateur, ECRITURES[nom].format(ns=namespace_test))


@pytest.mark.parametrize("utilisateur", ["alice", "pipeline"])
def test_trino_ne_peut_pas_supprimer_un_schema(utilisateur, spark, trino):
    """Sur un schéma vide (sur un schéma non vide, Trino refuse avant même de vérifier les droits)."""
    schema = f"lake.test_vide_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE NAMESPACE {schema}")
    try:
        with pytest.raises(Exception, match="Access Denied"):
            trino(utilisateur, f"DROP SCHEMA {schema}")
        assert spark.catalog.databaseExists(schema)
    finally:
        spark.sql(f"DROP NAMESPACE IF EXISTS {schema}")


def test_tables_intactes_apres_tentatives(spark, namespace_test, petites_tables, versions_avant):
    """Après toutes les tentatives d'écriture via Trino, Spark retrouve les tables d'origine :
    aucune nouvelle version, mêmes lignes, aucune table ou schéma en plus."""
    assert versions(spark, namespace_test) == versions_avant
    for nom, df in petites_tables.items():
        assert trier(spark.table(f"{namespace_test}.{nom}").collect()) == trier(df.collect())
    assert sorted(t.name for t in spark.catalog.listTables(namespace_test)) == ["clients", "produits", "ventes"]
    assert not spark.catalog.databaseExists("lake.pirate")


# --- mêmes résultats que Spark -----------------------------------------------------------------

@pytest.mark.parametrize("nom", list(REQUETES_SQL))
def test_requete_sql_memes_resultats_que_spark(nom, spark, trino, namespace_test):
    tables = {t: spark.table(f"{namespace_test}.{t}") for t in ("ventes", "clients", "produits")}
    attendu = trier(requetes.REQUETES[nom](tables).collect())
    assert trier(trino("alice", REQUETES_SQL[nom](namespace_test))) == attendu


def test_toutes_les_requetes_ont_une_version_sql():
    assert set(REQUETES_SQL) == set(requetes.REQUETES)


# --- garde-fous et historique ------------------------------------------------------------------

def test_duree_maximale_des_requetes(trino):
    """Au-delà de 2 min, une requête Trino est arrêtée : un traitement long relève de Spark."""
    session = trino("alice", "SHOW SESSION LIKE 'query_max_execution_time'")
    assert session[0]["Value"] == "2.00m"


def test_chacun_dans_sa_file_d_attente(trino):
    """Chaque utilisateur a sa propre file (3 requêtes simultanées max) : un dashboard très
    consulté ne bloque pas les autres."""
    marqueur = f"file_{uuid.uuid4().hex[:8]}"
    for utilisateur in ("alice", "bob"):
        trino(utilisateur, f"SELECT '{marqueur}'")
        groupes = trino(utilisateur, "SELECT resource_group_id AS g FROM system.runtime.queries "
                                     f"WHERE query = 'SELECT ''{marqueur}'''")
        assert groupes == [{"g": ["global", utilisateur]}]


def test_chacun_ne_voit_que_ses_requetes(trino):
    marqueur = f"marqueur_{uuid.uuid4().hex[:8]}"
    trino("alice", f"SELECT '{marqueur}'")
    historique = f"SELECT \"user\" AS u FROM system.runtime.queries WHERE query = 'SELECT ''{marqueur}'''"
    assert trino("alice", historique) == [{"u": "alice"}]
    assert trino("bob", historique) == []
