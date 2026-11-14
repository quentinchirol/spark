"""Lire le lakehouse depuis un notebook et exporter des fichiers (US-15) : pool analytique, en lecture seule.

    from analyse import tables, lire, exporter
    tables()                                                    # ce que je peux lire
    df = lire("SELECT * FROM gold.ventes_par_jour WHERE date >= %s", ("2026-11-01",))
    exporter("SELECT * FROM silver.ventes", "ventes.parquet")   # .csv, .xlsx ou .parquet, selon l'extension

Connexion au pool analytique (docs/analytique.md) : variables LAKE_HOTE (localhost), LAKE_PORT (5432),
LAKE_BASE (lake), LAKE_UTILISATEUR (analyste), LAKE_MOT_DE_PASSE. Les exports CSV et Parquet sont écrits par lots,
sans charger tout le résultat en mémoire ; Excel est limité à 1 048 575 lignes (limite du format).
Dépendances : pip install -r python/analyse/requirements.txt
"""
import datetime
import decimal
import json
import os
import uuid
from pathlib import Path

import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

LIGNES_MAX_EXCEL = 1_048_575          # 1 048 576 lignes par feuille, en-tête compris
TAILLE_LOT = 50_000

# types PostgreSQL (oid) → types Parquet ; tout autre type est exporté en texte
TYPES_PARQUET = {
    16: pa.bool_(), 20: pa.int64(), 21: pa.int16(), 23: pa.int32(), 700: pa.float32(), 701: pa.float64(),
    1082: pa.date32(), 1114: pa.timestamp("us"), 1184: pa.timestamp("us", tz="UTC"), 17: pa.binary(),
}
NUMERIC = 1700


def connexion(**options) -> psycopg.Connection:
    """Connexion au pool analytique (paramètres : arguments, sinon variables LAKE_*)."""
    parametres = {
        "host": os.environ.get("LAKE_HOTE", "localhost"),
        "port": int(os.environ.get("LAKE_PORT", "5432")),
        "dbname": os.environ.get("LAKE_BASE", "lake"),
        "user": os.environ.get("LAKE_UTILISATEUR", "analyste"),
        "password": os.environ.get("LAKE_MOT_DE_PASSE"),
        "connect_timeout": 30,
    }
    parametres.update(options)
    return psycopg.connect(**parametres)


def _avec_connexion(fonction):
    def enveloppe(*args, conn=None, **kwargs):
        if conn is not None:
            return fonction(*args, conn=conn, **kwargs)
        with connexion() as nouvelle:
            return fonction(*args, conn=nouvelle, **kwargs)
    enveloppe.__doc__, enveloppe.__name__ = fonction.__doc__, fonction.__name__
    return enveloppe


@_avec_connexion
def tables(conn=None) -> pd.DataFrame:
    """Tables et vues lisibles par l'utilisateur connecté (schéma, table, type)."""
    # par oid (et non par nom) : aucune erreur sur les schémas internes que l'utilisateur ne peut pas ouvrir
    requete = """SELECT n.nspname AS schema, c.relname AS "table",
                        CASE WHEN c.relkind IN ('v', 'm') THEN 'vue' ELSE 'table' END AS type
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\\_%'
                   AND has_schema_privilege(n.oid, 'USAGE') AND has_table_privilege(c.oid, 'SELECT')
                 ORDER BY 1, 2"""
    return lire(requete, conn=conn)


def _valeur_simple(valeur):
    """Valeur exportable telle quelle en CSV, Excel ou Parquet (UUID, JSON → texte)."""
    if isinstance(valeur, uuid.UUID):
        return str(valeur)
    if isinstance(valeur, (dict, list)):
        return json.dumps(valeur, ensure_ascii=False, default=str)
    return valeur


@_avec_connexion
def lire(requete: str, params=None, conn=None) -> pd.DataFrame:
    """Résultat d'une requête dans un DataFrame pandas (tout en mémoire : pour un gros volume, voir exporter)."""
    with conn.cursor() as curseur:
        curseur.execute(requete, params)
        colonnes = [c.name for c in curseur.description]
        lignes = [[_valeur_simple(v) for v in ligne] for ligne in curseur.fetchall()]
    return pd.DataFrame(lignes, columns=colonnes)


def _schema_parquet(description) -> pa.Schema:
    champs = []
    for colonne in description:
        if colonne.type_code == NUMERIC and colonne.precision and colonne.precision <= 38:
            type_arrow = pa.decimal128(colonne.precision, colonne.scale or 0)
        else:
            type_arrow = TYPES_PARQUET.get(colonne.type_code, pa.string())
        champs.append(pa.field(colonne.name, type_arrow))
    return pa.schema(champs)


def _en_texte(valeur):
    if valeur is None or isinstance(valeur, str):
        return valeur
    if isinstance(valeur, (dict, list)):
        return json.dumps(valeur, ensure_ascii=False, default=str)
    return str(valeur)


def _lot_arrow(lignes, schema: pa.Schema) -> pa.Table:
    colonnes = []
    for i, champ in enumerate(schema):
        valeurs = [ligne[i] for ligne in lignes]
        if pa.types.is_string(champ.type):
            valeurs = [_en_texte(v) for v in valeurs]
        elif pa.types.is_decimal(champ.type):
            valeurs = [None if v is None else decimal.Decimal(v) for v in valeurs]
        elif pa.types.is_timestamp(champ.type) and champ.type.tz:
            valeurs = [None if v is None else v.astimezone(datetime.timezone.utc) for v in valeurs]
        colonnes.append(pa.array(valeurs, type=champ.type))
    return pa.Table.from_arrays(colonnes, schema=schema)


@_avec_connexion
def exporter(requete: str, chemin, params=None, conn=None, taille_lot: int = TAILLE_LOT) -> int:
    """Exporte le résultat d'une requête dans un fichier .csv, .xlsx ou .parquet ; renvoie le nombre de lignes."""
    chemin = Path(chemin)
    format_ = chemin.suffix.lower()
    if format_ not in (".csv", ".xlsx", ".parquet"):
        raise ValueError(f"format non pris en charge : {format_ or 'aucune extension'} (.csv, .xlsx ou .parquet)")
    chemin.parent.mkdir(parents=True, exist_ok=True)
    if format_ == ".csv":
        return _exporter_csv(conn, requete, params, chemin)
    if format_ == ".xlsx":
        df = lire(requete, params, conn=conn)
        if len(df) > LIGNES_MAX_EXCEL:
            raise ValueError(f"{len(df)} lignes : au-delà de la limite d'Excel ({LIGNES_MAX_EXCEL}) ; "
                             "exporter en .csv ou en .parquet")
        for colonne in df.columns:     # Excel ne connaît pas les fuseaux horaires
            if isinstance(df[colonne].dtype, pd.DatetimeTZDtype):
                df[colonne] = df[colonne].dt.tz_convert("UTC").dt.tz_localize(None)
        df.to_excel(chemin, index=False, engine="openpyxl")
        return len(df)
    return _exporter_parquet(conn, requete, params, chemin, taille_lot)


def _exporter_csv(conn, requete, params, chemin) -> int:
    """COPY … TO STDOUT : le serveur produit le CSV, écrit au fil de l'eau (UTF-8, en-tête, séparateur virgule)."""
    with psycopg.ClientCursor(conn) as client:
        texte = client.mogrify(requete, params) if params else requete
    with conn.cursor() as curseur, open(chemin, "wb") as fichier:
        with curseur.copy(f"COPY ({texte}) TO STDOUT WITH (FORMAT csv, HEADER)") as copie:
            for bloc in copie:
                fichier.write(bloc)
        return curseur.rowcount        # « COPY n » renvoyé par le serveur


def _exporter_parquet(conn, requete, params, chemin, taille_lot) -> int:
    """Curseur côté serveur : lots de taille_lot lignes, écrits un par un dans le fichier Parquet."""
    nombre, ecrivain = 0, None
    with conn.transaction(), conn.cursor(name="export_parquet") as curseur:
        curseur.itersize = taille_lot
        curseur.execute(requete, params)
        schema = None
        while True:
            lignes = curseur.fetchmany(taille_lot)
            if schema is None:
                schema = _schema_parquet(curseur.description)
                ecrivain = pq.ParquetWriter(chemin, schema, compression="zstd")
            if not lignes:
                break
            ecrivain.write_table(_lot_arrow(lignes, schema))
            nombre += len(lignes)
    if ecrivain is not None:
        ecrivain.close()
    return nombre
