"""Requêtes mesurées par le benchmark et vérifiées par les tests unitaires.

Chaque requête prend les tables (DataFrames) en entrée et renvoie un DataFrame, sans l'exécuter.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def filtre(ventes: DataFrame) -> DataFrame:
    """Ventes d'au moins 3 articles et de plus de 50 €."""
    return ventes.where((F.col("quantite") >= 3) & (F.col("montant") > 50))


def filtre_mois(ventes: DataFrame, debut: str = "2026-03-01", fin: str = "2026-04-01") -> DataFrame:
    """Ventes d'un mois (dates dans [debut, fin[) : avec Iceberg, seuls les fichiers du mois sont lus."""
    return ventes.where((F.col("date") >= F.lit(debut).cast("date")) & (F.col("date") < F.lit(fin).cast("date")))


def group_by(ventes: DataFrame) -> DataFrame:
    """Chiffre d'affaires et nombre de ventes par produit."""
    return ventes.groupBy("id_produit").agg(
        F.sum("montant").alias("ca"),
        F.count("*").alias("nb_ventes"),
    )


def jointure(ventes: DataFrame, clients: DataFrame) -> DataFrame:
    """Ventes enrichies du client (sort-merge join : les deux côtés sont redistribués)."""
    return ventes.join(clients.hint("merge"), "id_client")


def jointure_broadcast(ventes: DataFrame, produits: DataFrame) -> DataFrame:
    """Ventes enrichies du produit (petite table diffusée à chaque executor)."""
    return ventes.join(F.broadcast(produits), "id_produit")


def jointure_group_by(ventes: DataFrame, clients: DataFrame, produits: DataFrame) -> DataFrame:
    """Chiffre d'affaires par pays et par catégorie de produit."""
    return (
        ventes.join(clients.hint("merge"), "id_client")
        .join(F.broadcast(produits), "id_produit")
        .groupBy("pays", "categorie")
        .agg(F.sum("montant").alias("ca"), F.count("*").alias("nb_ventes"))
    )


def distinct(ventes: DataFrame) -> DataFrame:
    """Clients ayant acheté au moins une fois."""
    return ventes.select("id_client").distinct()


def tri(ventes: DataFrame, n: int = 100) -> DataFrame:
    """Les n plus grosses ventes."""
    return ventes.orderBy(F.desc("montant"), "id_vente").limit(n)


def fenetre(ventes: DataFrame, n: int = 3) -> DataFrame:
    """Les n plus grosses ventes de chaque client (window function)."""
    w = Window.partitionBy("id_client").orderBy(F.desc("montant"), "id_vente")
    return ventes.withColumn("rang", F.row_number().over(w)).where(F.col("rang") <= n)


# nom -> requête à partir du dictionnaire de tables (ventes, clients, produits)
REQUETES = {
    "filtre": lambda t: filtre(t["ventes"]),
    "filtre_mois": lambda t: filtre_mois(t["ventes"]),
    "group_by": lambda t: group_by(t["ventes"]),
    "jointure": lambda t: jointure(t["ventes"], t["clients"]),
    "jointure_broadcast": lambda t: jointure_broadcast(t["ventes"], t["produits"]),
    "jointure_group_by": lambda t: jointure_group_by(t["ventes"], t["clients"], t["produits"]),
    "distinct": lambda t: distinct(t["ventes"]),
    "tri": lambda t: tri(t["ventes"]),
    "fenetre": lambda t: fenetre(t["ventes"]),
}
