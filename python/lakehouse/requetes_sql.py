"""Les requêtes de requetes.py écrites en SQL, pour Trino (lecture seule).

Chaque fonction prend le schéma Iceberg (ex. "lake.bench_1000000") et renvoie le texte SQL.
Les résultats sont identiques à ceux des versions Spark (vérifié par tests/test_trino.py).
"""


def filtre(ns: str) -> str:
    return f"SELECT * FROM {ns}.ventes WHERE quantite >= 3 AND montant > 50"


def filtre_mois(ns: str, debut: str = "2026-03-01", fin: str = "2026-04-01") -> str:
    return f"SELECT * FROM {ns}.ventes WHERE date >= DATE '{debut}' AND date < DATE '{fin}'"


def group_by(ns: str) -> str:
    return f"""SELECT id_produit, sum(montant) AS ca, count(*) AS nb_ventes
FROM {ns}.ventes GROUP BY id_produit"""


def jointure(ns: str) -> str:
    return f"""SELECT v.id_client, v.id_vente, v.id_produit, v.quantite, v.montant, v.date, c.nom, c.pays
FROM {ns}.ventes v JOIN {ns}.clients c ON v.id_client = c.id_client"""


def jointure_broadcast(ns: str) -> str:
    return f"""SELECT v.id_produit, v.id_vente, v.id_client, v.quantite, v.montant, v.date, p.categorie, p.prix
FROM {ns}.ventes v JOIN {ns}.produits p ON v.id_produit = p.id_produit"""


def jointure_group_by(ns: str) -> str:
    return f"""SELECT c.pays, p.categorie, sum(v.montant) AS ca, count(*) AS nb_ventes
FROM {ns}.ventes v
JOIN {ns}.clients c ON v.id_client = c.id_client
JOIN {ns}.produits p ON v.id_produit = p.id_produit
GROUP BY c.pays, p.categorie"""


def distinct(ns: str) -> str:
    return f"SELECT DISTINCT id_client FROM {ns}.ventes"


def tri(ns: str, n: int = 100) -> str:
    return f"SELECT * FROM {ns}.ventes ORDER BY montant DESC, id_vente LIMIT {n}"


def fenetre(ns: str, n: int = 3) -> str:
    return f"""SELECT * FROM (
  SELECT v.*, row_number() OVER (PARTITION BY id_client ORDER BY montant DESC, id_vente) AS rang
  FROM {ns}.ventes v
) WHERE rang <= {n}"""


REQUETES_SQL = {
    "filtre": filtre,
    "filtre_mois": filtre_mois,
    "group_by": group_by,
    "jointure": jointure,
    "jointure_broadcast": jointure_broadcast,
    "jointure_group_by": jointure_group_by,
    "distinct": distinct,
    "tri": tri,
    "fenetre": fenetre,
}
