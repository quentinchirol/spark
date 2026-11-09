"""Fixtures pytest : session Spark Connect et mini jeu de données."""
from datetime import date

import pytest

from lakehouse.connexion import nouvelle_session, token_par_defaut


@pytest.fixture(scope="session")
def spark():
    session = nouvelle_session(token_par_defaut())
    yield session
    session.stop()


@pytest.fixture(scope="session")
def petites_tables(spark):
    """Mini jeu de données aux résultats connus.

    Le client 3 n'a rien acheté ; la vente 105 référence un client inconnu (99).
    """
    clients = spark.createDataFrame(
        [(1, "Alice SARL", "France"), (2, "Bob GmbH", "Allemagne"), (3, "Carla SpA", "Italie")],
        "id_client long, nom string, pays string",
    )
    produits = spark.createDataFrame(
        [(10, "livre", 20.0), (20, "jeu", 50.0)],
        "id_produit long, categorie string, prix double",
    )
    ventes = spark.createDataFrame(
        [
            (100, 1, 10, 1, 20.0, date(2026, 1, 15)),
            (101, 1, 20, 3, 150.0, date(2026, 3, 2)),
            (102, 2, 10, 5, 100.0, date(2026, 3, 31)),
            (103, 2, 20, 1, 50.0, date(2026, 4, 1)),
            (104, 1, 10, 2, 40.0, date(2026, 2, 28)),
            (105, 99, 10, 1, 20.0, date(2026, 3, 15)),
        ],
        "id_vente long, id_client long, id_produit long, quantite long, montant double, date date",
    )
    return {"ventes": ventes, "clients": clients, "produits": produits}


def pytest_addoption(parser):
    parser.addoption("--tailles", type=int, nargs="+", default=[100_000, 1_000_000],
                     help="nombres de ventes des tests à gros volume (pytest -m gros_volume)")


# --- fixtures communes Iceberg / Trino ---------------------------------------------------------

@pytest.fixture(scope="session")
def tokens():
    from lakehouse.connexion import lire_tokens

    t = lire_tokens()
    assert {"alice", "bob", "pipeline"} <= set(t), "k8s/secrets/tokens doit contenir alice, bob et pipeline"
    return t


@pytest.fixture(scope="session")
def trino(tokens):
    """trino(utilisateur, sql) -> liste de dictionnaires {colonne: valeur}, au nom de `utilisateur`."""
    from lakehouse.connexion import connexion_trino

    def executer(utilisateur, sql, token=None):
        curseur = connexion_trino(utilisateur, token or tokens[utilisateur]).cursor()
        curseur.execute(sql)
        lignes = curseur.fetchall()
        colonnes = [d[0] for d in curseur.description or []]
        return [dict(zip(colonnes, ligne)) for ligne in lignes]

    return executer


@pytest.fixture(scope="session")
def namespace_test(spark, petites_tables):
    """Le mini jeu de données écrit en Iceberg par Spark dans lake.test_<id> (supprimé à la fin)."""
    import uuid

    from lakehouse.donnees import CATALOGUE, ecrire_iceberg, supprimer_iceberg

    namespace = f"{CATALOGUE}.test_{uuid.uuid4().hex[:8]}"
    ecrire_iceberg(spark, petites_tables, namespace)
    try:
        yield namespace
    finally:
        supprimer_iceberg(spark, namespace)


def trier(lignes):
    """Lignes (dictionnaires ou Row Spark) dans un ordre canonique, pour comparer deux résultats."""
    dicts = [l if isinstance(l, dict) else l.asDict() for l in lignes]
    return sorted(dicts, key=lambda d: sorted((k, repr(v)) for k, v in d.items()))
