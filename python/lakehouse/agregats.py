"""Tables agrégées pour les dashboards : calculées et écrites par Spark, lues par Trino.

Spark fait le calcul lourd (jointures sur toutes les ventes) une fois par exécution du traitement
planifié ; les dashboards lisent ensuite quelques milliers de lignes via Trino.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning as P

NAMESPACE_DASHBOARD = "lake.dashboard"


def ca_jour_pays_categorie(ventes: DataFrame, clients: DataFrame, produits: DataFrame) -> DataFrame:
    """Chiffre d'affaires, nombre de ventes et articles vendus par jour, pays et catégorie."""
    return (
        ventes.join(clients, "id_client")
        .join(F.broadcast(produits), "id_produit")
        .groupBy("date", "pays", "categorie")
        .agg(
            F.round(F.sum("montant"), 2).alias("ca"),
            F.count("*").alias("nb_ventes"),
            F.sum("quantite").alias("quantite"),
        )
    )


def resume_clients(ventes: DataFrame, clients: DataFrame) -> DataFrame:
    """Une ligne par client : nombre d'achats, chiffre d'affaires, dernière vente.
    Les clients sans achat sont présents avec 0 achat."""
    achats = ventes.groupBy("id_client").agg(
        F.count("*").alias("nb_achats"),
        F.round(F.sum("montant"), 2).alias("ca"),
        F.max("date").alias("derniere_vente"),
    )
    return (
        clients.join(achats, "id_client", "left")
        .select("id_client", "nom", "pays",
                F.coalesce("nb_achats", F.lit(0)).alias("nb_achats"),
                F.coalesce("ca", F.lit(0.0)).alias("ca"),
                "derniere_vente")
    )


def calculer(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    return {
        "ca_jour_pays_categorie": ca_jour_pays_categorie(tables["ventes"], tables["clients"], tables["produits"]),
        "resume_clients": resume_clients(tables["ventes"], tables["clients"]),
    }


def ecrire(spark: SparkSession, tables: dict[str, DataFrame], namespace: str = NAMESPACE_DASHBOARD) -> dict[str, int]:
    """Recalcule et remplace les tables agrégées ; renvoie le nombre de lignes de chacune.

    createOrReplace crée une nouvelle version (snapshot) de chaque table : pendant le calcul,
    Trino continue de lire la version précédente complète, jamais un état à moitié écrit.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    agregats = calculer(tables)
    agregats["ca_jour_pays_categorie"].writeTo(f"{namespace}.ca_jour_pays_categorie") \
        .partitionedBy(P.months("date")).createOrReplace()
    agregats["resume_clients"].writeTo(f"{namespace}.resume_clients").createOrReplace()
    return {nom: spark.table(f"{namespace}.{nom}").count() for nom in agregats}
