"""Synchronisation du pool analytique avec le catalogue (k8s/analytique/synchro.py) — sans PostgreSQL ni Polaris."""
import importlib.util
from pathlib import Path

import pytest

CHEMIN = Path(__file__).resolve().parents[3] / "k8s" / "analytique" / "synchro.py"


@pytest.fixture
def synchro(monkeypatch):
    spec = importlib.util.spec_from_file_location("synchro", CHEMIN)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    executes = []
    monkeypatch.setattr(module, "psql", lambda sql, base="lake": executes.append(sql) or "")
    return module, executes


def test_rattache_les_nouvelles_tables_et_cree_les_vues_app(synchro):
    module, executes = synchro
    module.catalogue = lambda: {("gold", "ventes_par_jour"): ["date", "nb_ventes"],
                                ("app", "commandes"): ["id", "client", "_version", "_supprime"],
                                ("app", "commandes_historique"): ["id", "_version", "_supprime"]}
    module.rattaches = lambda: {}
    nombre, erreurs = module.synchroniser()
    sql = "\n".join(executes)
    assert nombre == 3 and not erreurs
    assert 'CREATE TABLE IF NOT EXISTS "gold"."ventes_par_jour" () USING iceberg' in sql
    assert "read_only = true" in sql and "catalog_namespace = 'gold'" in sql
    assert 'GRANT SELECT ON "gold"."ventes_par_jour" TO analyste' in sql
    # vue des lignes non supprimées pour les tables de l'application, pas pour leur historique
    assert 'CREATE OR REPLACE VIEW vue."commandes" AS SELECT * FROM "app"."commandes" WHERE NOT _supprime' in sql
    assert 'vue."commandes_historique"' not in sql
    assert "DROP" not in sql


def test_retire_les_tables_disparues_et_rattache_celles_qui_ont_change(synchro):
    module, executes = synchro
    module.catalogue = lambda: {("gold", "a"): ["x", "y"], ("gold", "b"): ["z"]}
    module.rattaches = lambda: {("gold", "a"): ["x"], ("gold", "b"): ["z"], ("gold", "ancienne"): ["k"]}
    module.synchroniser()
    sql = "\n".join(executes)
    assert 'DROP FOREIGN TABLE IF EXISTS "gold"."ancienne" CASCADE' in sql
    assert 'DROP FOREIGN TABLE IF EXISTS "gold"."a" CASCADE' in sql          # colonnes changées
    assert 'CREATE TABLE IF NOT EXISTS "gold"."a"' in sql
    assert 'DROP FOREIGN TABLE IF EXISTS "gold"."b"' not in sql             # inchangée : laissée telle quelle
    assert 'CREATE TABLE IF NOT EXISTS "gold"."b"' not in sql


def test_une_table_en_erreur_n_empeche_pas_les_autres(synchro, monkeypatch):
    module, _ = synchro
    vus = []

    def psql(sql, base="lake"):
        vus.append(sql)
        if '"gold"."cassee"' in sql:
            raise RuntimeError("type non pris en charge")
        return ""
    monkeypatch.setattr(module, "psql", psql)
    module.catalogue = lambda: {("gold", "cassee"): ["x"], ("gold", "saine"): ["y"]}
    module.rattaches = lambda: {}
    nombre, erreurs = module.synchroniser()
    assert len(erreurs) == 1 and "gold.cassee" in erreurs[0]
    assert any('"gold"."saine"' in sql for sql in vus)


def test_noms_et_valeurs_echappes(synchro):
    module, _ = synchro
    assert module.ident('a"b') == '"a""b"'
    assert module.litteral("l'eau") == "'l''eau'"
