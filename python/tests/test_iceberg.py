"""Tests du stockage Iceberg : mêmes résultats qu'en mémoire, et plans d'exécution optimisés."""
import uuid

import pytest

from lakehouse import requetes
from lakehouse.donnees import CATALOGUE, ecrire_iceberg, lire_iceberg, supprimer_iceberg


@pytest.fixture(scope="module")
def tables_iceberg(spark, petites_tables):
    namespace = f"{CATALOGUE}.test_{uuid.uuid4().hex[:8]}"
    ecrire_iceberg(spark, petites_tables, namespace)
    try:
        yield lire_iceberg(spark, namespace)
    finally:
        supprimer_iceberg(spark, namespace)


def lignes(df):
    return sorted(tuple(r) for r in df.collect())


@pytest.mark.parametrize("nom", list(requetes.REQUETES))
def test_memes_resultats_qu_en_memoire(nom, petites_tables, tables_iceberg):
    requete = requetes.REQUETES[nom]
    assert lignes(requete(tables_iceberg)) == lignes(requete(petites_tables))


def plan(df, capsys) -> str:
    capsys.readouterr()
    df.explain()
    return capsys.readouterr().out


def test_jointure_sans_redistribution(tables_iceberg, capsys):
    """ventes et clients ont le même découpage par id_client : aucun échange de données."""
    texte = plan(requetes.jointure(tables_iceberg["ventes"], tables_iceberg["clients"]), capsys)
    assert "SortMergeJoin" in texte
    assert "Exchange" not in texte


def test_jointure_en_memoire_redistribue(petites_tables, capsys):
    """Témoin : sans découpage commun, Spark doit redistribuer les deux côtés."""
    texte = plan(requetes.jointure(petites_tables["ventes"], petites_tables["clients"]), capsys)
    assert "Exchange" in texte


def test_filtre_mois_transmis_a_iceberg(tables_iceberg, capsys):
    """Le filtre sur la date est transmis au scan Iceberg, qui écarte les autres mois."""
    texte = plan(requetes.filtre_mois(tables_iceberg["ventes"]), capsys)
    scan = next(l for l in texte.splitlines() if "IcebergScan" in l)
    assert "date >=" in scan and "date <" in scan
