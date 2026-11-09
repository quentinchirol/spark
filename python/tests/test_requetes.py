"""Tests unitaires des requêtes, sur un mini jeu de données dont on connaît les résultats."""
import pytest

from lakehouse import requetes


@pytest.fixture(scope="module")
def tables(petites_tables):
    return petites_tables


def ids(df, colonne="id_vente"):
    return sorted(r[colonne] for r in df.select(colonne).collect())


def test_filtre(tables):
    assert ids(requetes.filtre(tables["ventes"])) == [101, 102]


def test_filtre_mois(tables):
    assert ids(requetes.filtre_mois(tables["ventes"])) == [101, 102, 105]


def test_group_by(tables):
    res = {r.id_produit: (r.ca, r.nb_ventes) for r in requetes.group_by(tables["ventes"]).collect()}
    assert res == {10: (180.0, 4), 20: (200.0, 2)}


def test_jointure(tables):
    """Les ventes dont le client est inconnu disparaissent de la jointure."""
    df = requetes.jointure(tables["ventes"], tables["clients"])
    assert ids(df) == [100, 101, 102, 103, 104]
    assert {r.id_vente: r.pays for r in df.collect()}[102] == "Allemagne"


def test_jointure_broadcast(tables):
    df = requetes.jointure_broadcast(tables["ventes"], tables["produits"])
    assert ids(df) == [100, 101, 102, 103, 104, 105]
    assert {r.id_vente: r.categorie for r in df.collect()}[101] == "jeu"


def test_jointure_group_by(tables):
    df = requetes.jointure_group_by(tables["ventes"], tables["clients"], tables["produits"])
    res = {(r.pays, r.categorie): (r.ca, r.nb_ventes) for r in df.collect()}
    assert res == {
        ("France", "livre"): (60.0, 2),
        ("France", "jeu"): (150.0, 1),
        ("Allemagne", "livre"): (100.0, 1),
        ("Allemagne", "jeu"): (50.0, 1),
    }


def test_distinct(tables):
    assert ids(requetes.distinct(tables["ventes"]), "id_client") == [1, 2, 99]


def test_tri(tables):
    df = requetes.tri(tables["ventes"], n=3)
    assert [r.id_vente for r in df.collect()] == [101, 102, 103]


def test_fenetre(tables):
    df = requetes.fenetre(tables["ventes"], n=2)
    res = {(r.id_client, r.rang): r.id_vente for r in df.collect()}
    assert res == {(1, 1): 101, (1, 2): 104, (2, 1): 102, (2, 2): 103, (99, 1): 105}


def test_toutes_les_requetes_sont_testees():
    testees = {nom.removeprefix("test_") for nom in globals() if nom.startswith("test_")}
    assert set(requetes.REQUETES) <= testees
