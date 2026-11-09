"""Tests de sécurité du serveur Spark Connect."""
from contextlib import suppress

import pytest
from pyspark.errors import PySparkException
from grpc import RpcError

from lakehouse.connexion import nouvelle_session, token_par_defaut

ERREURS_CONNEXION = (RpcError, PySparkException)


def verifier_refus(session):
    try:
        with pytest.raises(ERREURS_CONNEXION):
            session.sql("SELECT 1").collect()
    finally:
        # sinon cette session refusée reste la session "active" et casse les tests suivants
        with suppress(*ERREURS_CONNEXION):
            session.stop()


def test_sans_token_refuse():
    verifier_refus(nouvelle_session())


def test_token_invalide_refuse():
    verifier_refus(nouvelle_session("token-inexistant"))


def test_token_valide_accepte(spark):
    assert spark.sql("SELECT 41 + 1 AS r").first().r == 42


def test_sessions_isolees():
    a = nouvelle_session(token_par_defaut())
    b = nouvelle_session(token_par_defaut())
    try:
        a.range(3).createOrReplaceTempView("vue_privee")
        assert "vue_privee" in [t.name for t in a.catalog.listTables()]
        assert "vue_privee" not in [t.name for t in b.catalog.listTables()]
    finally:
        a.stop()
        b.stop()
