"""Client des notebooks (python/analyse) : types Parquet, conversions, formats — sans base de données."""
import datetime
import decimal
import uuid
from collections import namedtuple

import pandas as pd
import pyarrow as pa
import pytest

import analyse

Colonne = namedtuple("Colonne", "name type_code precision scale")


def test_schema_parquet_suit_les_types_postgresql():
    schema = analyse._schema_parquet([
        Colonne("n", 20, None, None), Colonne("montant", 1700, 12, 2), Colonne("brut", 1700, None, None),
        Colonne("jour", 1082, None, None), Colonne("quand", 1184, None, None), Colonne("id", 2950, None, None),
        Colonne("ok", 16, None, None)])
    assert schema.field("n").type == pa.int64()
    assert schema.field("montant").type == pa.decimal128(12, 2)
    assert schema.field("brut").type == pa.string()         # numeric sans précision : texte exact
    assert schema.field("jour").type == pa.date32()
    assert schema.field("quand").type == pa.timestamp("us", tz="UTC")
    assert schema.field("id").type == pa.string()           # uuid et autres types : texte
    assert schema.field("ok").type == pa.bool_()


def test_lot_arrow_convertit_uuid_json_et_fuseaux():
    schema = pa.schema([("id", pa.string()), ("info", pa.string()), ("quand", pa.timestamp("us", tz="UTC")),
                        ("montant", pa.decimal128(12, 2))])
    i = uuid.uuid4()
    paris = datetime.timezone(datetime.timedelta(hours=1))
    lot = analyse._lot_arrow([(i, {"a": 1}, datetime.datetime(2026, 11, 1, 10, tzinfo=paris), decimal.Decimal("1.25")),
                              (None, None, None, None)], schema)
    lignes = lot.to_pylist()
    assert lignes[0]["id"] == str(i) and lignes[0]["info"] == '{"a": 1}'
    assert lignes[0]["quand"] == datetime.datetime(2026, 11, 1, 9, tzinfo=datetime.timezone.utc)
    assert lignes[0]["montant"] == decimal.Decimal("1.25")
    assert lignes[1] == {"id": None, "info": None, "quand": None, "montant": None}


def test_format_inconnu_refuse(tmp_path):
    with pytest.raises(ValueError, match=".json"):
        analyse.exporter("SELECT 1", tmp_path / "x.json", conn=object())


def test_excel_refuse_au_dela_de_sa_limite(tmp_path, monkeypatch):
    monkeypatch.setattr(analyse, "LIGNES_MAX_EXCEL", 3)
    monkeypatch.setattr(analyse, "lire", lambda requete, params=None, conn=None: pd.DataFrame({"n": range(4)}))
    with pytest.raises(ValueError, match="limite d'Excel"):
        analyse.exporter("SELECT n", tmp_path / "x.xlsx", conn=object())


def test_excel_sans_fuseau_horaire(tmp_path, monkeypatch):
    df = pd.DataFrame({"quand": pd.to_datetime(["2026-11-01 10:00+01:00"], utc=True), "n": [1]})
    monkeypatch.setattr(analyse, "lire", lambda requete, params=None, conn=None: df.copy())
    assert analyse.exporter("SELECT 1", tmp_path / "x.xlsx", conn=object()) == 1
    relu = pd.read_excel(tmp_path / "x.xlsx")
    assert str(relu["quand"][0]) == "2026-11-01 09:00:00"
