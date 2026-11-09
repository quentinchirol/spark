"""Tests à gros volume : chaque requête sur les tables du benchmark, en mémoire (cache), en Iceberg
(Spark) et dans Trino (mêmes tables Iceberg, requêtes SQL de requetes_sql.py).

    pytest -m gros_volume                          # 100 000 et 1 000 000 ventes
    pytest -m gros_volume --tailles 10000000

Pour chaque taille et chaque requête :
- le résultat est identique en cache et en Iceberg ; Trino donne le même résultat que Spark ;
- le résultat est cohérent avec la façon dont les données sont générées (donnees.generer) ;
- les durées des trois sources sont enregistrées dans resultats/tests_comparaison_<date>.csv.

Les tables Iceberg sont celles du benchmark (lake.bench_<taille>) : créées si besoin, puis conservées.
Les durées viennent d'une seule exécution à chaud ; le benchmark donne des médianes plus fiables.
"""
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from benchmark import ecrire_csv, executer, executer_trino, gain_pct, mesurer
from lakehouse import requetes
from lakehouse.connexion import connexion_trino
from lakehouse.requetes_sql import REQUETES_SQL
from lakehouse.donnees import (CATEGORIES, NB_PRODUITS, PAYS, ecrire_iceberg, generer, lire_iceberg,
                               namespace_iceberg, tables_iceberg_existent)

pytestmark = pytest.mark.gros_volume

DOSSIER_RESULTATS = Path(__file__).resolve().parents[1] / "resultats"
SOURCES = ("cache", "iceberg")


def pytest_generate_tests(metafunc):
    if "taille" in metafunc.fixturenames:
        metafunc.parametrize("taille", metafunc.config.getoption("tailles"), scope="module",
                             ids=lambda t: f"{t:_}")


# --- rapport CSV -------------------------------------------------------------------------------

class Rapport:
    COLONNES = ["nb_ventes", "requete", "nb_lignes_resultat", "resultat_identique", "resultat_identique_trino",
                "coherent_cache", "coherent_iceberg", "duree_cache_s", "duree_iceberg_s", "duree_trino_s",
                "gain_iceberg_pct", "gain_trino_sur_spark_pct"]

    def __init__(self):
        self.lignes: dict[tuple[int, str], dict] = {}

    def noter(self, taille: int, requete: str, **valeurs):
        self.lignes.setdefault((taille, requete), {}).update(valeurs)

    @contextmanager
    def statut(self, taille: int, requete: str, colonne: str):
        """Note "oui" si le bloc passe, "non" s'il échoue (l'échec remonte quand même à pytest)."""
        self.noter(taille, requete, **{colonne: "non"})
        yield
        self.noter(taille, requete, **{colonne: "oui"})

    def ecrire(self) -> Path:
        DOSSIER_RESULTATS.mkdir(exist_ok=True)
        chemin = DOSSIER_RESULTATS / f"tests_comparaison_{datetime.now():%Y%m%d_%H%M%S}.csv"
        ordre = list(requetes.REQUETES)
        lignes = []
        for (taille, requete), v in sorted(self.lignes.items(), key=lambda kv: (kv[0][0], ordre.index(kv[0][1]))):
            cache, iceberg, trino = v.get("duree_cache_s"), v.get("duree_iceberg_s"), v.get("duree_trino_s")
            v["gain_iceberg_pct"] = round(gain_pct(cache, iceberg), 1) if cache and iceberg else None
            v["gain_trino_sur_spark_pct"] = round(gain_pct(iceberg, trino), 1) if iceberg and trino else None
            lignes.append([taille, requete] + [v.get(c) for c in self.COLONNES[2:]])
        ecrire_csv(chemin, self.COLONNES, lignes)
        return chemin


@pytest.fixture(scope="module")
def rapport():
    r = Rapport()
    yield r
    if r.lignes:
        print(f"\nRapport : {r.ecrire()}")


# --- données -----------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def sources(spark, taille):
    """Les mêmes tables en cache et en Iceberg."""
    generees = generer(spark, taille)
    for df in generees.values():
        df.cache().count()
    namespace = namespace_iceberg(taille)
    if not tables_iceberg_existent(spark, namespace):
        ecrire_iceberg(spark, generees, namespace)
    try:
        yield {"cache": generees, "iceberg": lire_iceberg(spark, namespace)}
    finally:
        for df in generees.values():
            df.unpersist()


def empreinte(df) -> tuple[int, int]:
    """Nombre de lignes et somme des hachages des lignes, calculés sur le cluster.

    Ne dépend pas de l'ordre des lignes. Les doubles sont arrondis au centime : une somme de doubles
    varie très légèrement selon l'ordre des additions, donc selon la source.
    """
    colonnes = [F.round(c, 2) if t == "double" else F.col(c) for c, t in df.dtypes]
    r = df.select(F.xxhash64(*colonnes).cast("decimal(38,0)").alias("h")) \
        .agg(F.count("*").alias("n"), F.sum("h").alias("s")).first()
    return r.n, r.s


# --- 1. même résultat en cache et en Iceberg (+ durées) ----------------------------------------

@pytest.mark.parametrize("nom", list(requetes.REQUETES))
def test_memes_resultats_cache_et_iceberg(nom, taille, sources, rapport):
    requete = requetes.REQUETES[nom]
    attendu = empreinte(requete(sources["cache"]))
    with rapport.statut(taille, nom, "resultat_identique"):
        assert empreinte(requete(sources["iceberg"])) == attendu
    rapport.noter(taille, nom, nb_lignes_resultat=attendu[0])

    # tables déjà lues une fois ci-dessus : mesure à chaud
    for source in SOURCES:
        duree, _ = mesurer(lambda: executer(requete(sources[source])))
        rapport.noter(taille, nom, **{f"duree_{source}_s": duree})


# --- 1 bis. Trino donne le même résultat que Spark (+ durée) ------------------------------------

@pytest.fixture(scope="module")
def curseur_trino(tokens):
    return connexion_trino("alice", tokens["alice"]).cursor()


def empreinte_numerique_spark(df) -> list:
    """Nombre de lignes, somme de chaque colonne numérique, min/max des dates et des textes :
    calculable à l'identique par Spark et par Trino (leurs fonctions de hachage diffèrent)."""
    aggs = [F.count("*")]
    for c, t in df.dtypes:
        if t in ("bigint", "int", "double"):
            aggs.append(F.sum(c).cast("double"))
        else:
            aggs += [F.min(c), F.max(c)]
    return list(df.agg(*aggs).first())


def empreinte_numerique_trino(curseur, sql: str, dtypes) -> list:
    aggs = ["count(*)"]
    for c, t in dtypes:
        if t in ("bigint", "int", "double"):
            aggs.append(f"CAST(sum({c}) AS double)")
        else:
            aggs += [f"min({c})", f"max({c})"]
    curseur.execute(f"SELECT {', '.join(aggs)} FROM ({sql})")
    return list(curseur.fetchall()[0])


@pytest.mark.parametrize("nom", list(requetes.REQUETES))
def test_trino_memes_resultats_que_spark(nom, taille, sources, curseur_trino, rapport):
    df = requetes.REQUETES[nom](sources["iceberg"])
    sql = REQUETES_SQL[nom](namespace_iceberg(taille))
    attendu = empreinte_numerique_spark(df)
    with rapport.statut(taille, nom, "resultat_identique_trino"):
        # sommes de doubles : l'ordre des additions diffère d'un moteur à l'autre
        obtenu = empreinte_numerique_trino(curseur_trino, sql, df.dtypes)
        assert [pytest.approx(v, rel=1e-9) if isinstance(v, float) else v for v in obtenu] == attendu
    duree, _ = mesurer(lambda: executer_trino(curseur_trino, sql))
    rapport.noter(taille, nom, duree_trino_s=duree)


# --- 2. résultat cohérent avec les données générées --------------------------------------------
# generer() : id_client = hash % nb_clients, id_produit = hash % 1000, quantite = id % 5 + 1,
# montant = hash % 50000 / 100, date = 2026-01-01 + id % 365 ; nb_clients = max(n / 100, 100).

def nb_clients(taille: int) -> int:
    return max(taille // 100, 100)


def aucune(df) -> bool:
    return df.limit(1).count() == 0


def verifier_filtre(res, tables, taille):
    condition = (F.col("quantite") >= 3) & (F.col("montant") > 50)
    assert aucune(res.where(~condition))
    assert res.count() + tables["ventes"].where(~condition).count() == taille  # aucune vente perdue
    assert res.count() > 0


def verifier_filtre_mois(res, tables, taille):
    # mars 2026 = jours 59 à 89 de l'année : 31 ventes par tranche de 365 identifiants
    attendu = taille // 365 * 31 + max(0, min(taille % 365, 90) - 59)
    assert aucune(res.where((F.year("date") != 2026) | (F.month("date") != 3)))
    assert res.count() == attendu


def verifier_group_by(res, tables, taille):
    total = res.agg(F.sum("nb_ventes").alias("n"), F.sum("ca").alias("ca"),
                    F.count("*").alias("groupes"), F.count_distinct("id_produit").alias("ids")).first()
    assert total.n == taille
    assert total.ca == pytest.approx(tables["ventes"].agg(F.sum("montant")).first()[0])
    assert total.groupes == total.ids <= NB_PRODUITS


def verifier_jointure(res, tables, taille):
    # chaque vente a un client existant : la jointure n'en perd aucune
    assert res.count() == taille
    assert aucune(res.where(~F.col("pays").isin(PAYS)))


def verifier_jointure_broadcast(res, tables, taille):
    assert res.count() == taille
    categorie_attendue = F.element_at(F.array(*map(F.lit, CATEGORIES)), (F.col("id_produit") % 10 + 1).cast("int"))
    assert aucune(res.where(F.col("categorie") != categorie_attendue))


def verifier_jointure_group_by(res, tables, taille):
    total = res.agg(F.sum("nb_ventes").alias("n"), F.sum("ca").alias("ca"), F.count("*").alias("groupes")).first()
    assert total.n == taille
    assert total.ca == pytest.approx(tables["ventes"].agg(F.sum("montant")).first()[0])
    assert total.groupes <= len(PAYS) * len(CATEGORIES)
    assert aucune(res.where(~F.col("pays").isin(PAYS) | ~F.col("categorie").isin(CATEGORIES)))


def verifier_distinct(res, tables, taille):
    # ~100 ventes par client en moyenne : tous les clients ont acheté
    assert res.count() == nb_clients(taille)
    assert aucune(res.where((F.col("id_client") < 0) | (F.col("id_client") >= nb_clients(taille))))


def verifier_tri(res, tables, taille):
    lignes = res.collect()
    assert len(lignes) == 100
    cles = [(-r.montant, r.id_vente) for r in lignes]
    assert cles == sorted(cles)
    assert lignes[0].montant == tables["ventes"].agg(F.max("montant")).first()[0]


def verifier_fenetre(res, tables, taille):
    # chaque client a au moins 3 ventes : exactement les rangs 1, 2, 3, et le n°1 est sa plus grosse vente
    par_client = res.groupBy("id_client").agg(
        F.count("*").alias("n"), F.min("rang").alias("rmin"), F.max("rang").alias("rmax"),
        F.max(F.when(F.col("rang") == 1, F.col("montant"))).alias("premier"),
        F.max("montant").alias("maxi_garde"),
    )
    maxi = tables["ventes"].groupBy("id_client").agg(F.max("montant").alias("maxi"))
    assert par_client.count() == nb_clients(taille)
    assert aucune(par_client.join(maxi, "id_client").where(
        (F.col("n") != 3) | (F.col("rmin") != 1) | (F.col("rmax") != 3)
        | (F.col("premier") != F.col("maxi")) | (F.col("maxi_garde") != F.col("maxi"))
    ))


VERIFICATIONS = {
    "filtre": verifier_filtre,
    "filtre_mois": verifier_filtre_mois,
    "group_by": verifier_group_by,
    "jointure": verifier_jointure,
    "jointure_broadcast": verifier_jointure_broadcast,
    "jointure_group_by": verifier_jointure_group_by,
    "distinct": verifier_distinct,
    "tri": verifier_tri,
    "fenetre": verifier_fenetre,
}


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("nom", list(requetes.REQUETES))
def test_resultat_coherent(nom, source, taille, sources, rapport):
    tables = sources[source]
    with rapport.statut(taille, nom, f"coherent_{source}"):
        VERIFICATIONS[nom](requetes.REQUETES[nom](tables), tables, taille)


def test_toutes_les_requetes_sont_verifiees():
    assert set(requetes.REQUETES) <= set(VERIFICATIONS)


# --- 3. plans d'exécution à grande échelle -----------------------------------------------------

def plan(df, capsys) -> str:
    capsys.readouterr()
    df.explain()
    return capsys.readouterr().out


def test_jointure_iceberg_sans_redistribution(taille, sources, capsys):
    texte = plan(requetes.jointure(sources["iceberg"]["ventes"], sources["iceberg"]["clients"]), capsys)
    assert "SortMergeJoin" in texte
    assert "Exchange" not in texte


def test_jointure_cache_redistribue(taille, sources, capsys):
    assert "Exchange" in plan(requetes.jointure(sources["cache"]["ventes"], sources["cache"]["clients"]), capsys)


def test_filtre_mois_transmis_a_iceberg(taille, sources, capsys):
    texte = plan(requetes.filtre_mois(sources["iceberg"]["ventes"]), capsys)
    scan = next(l for l in texte.splitlines() if "IcebergScan" in l)
    assert "date >=" in scan and "date <" in scan
