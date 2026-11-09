"""Contrat d'écriture application → lakehouse (US-9) : tests sans cluster."""
import json
import uuid
from datetime import date

import pytest

from lakehouse.contrat import TOPIC, ContratInvalide, Ecriture, valider

ID = "0b6f3a52-6f1c-4f0e-9a43-3d6f2a0c9b11"


def message(**changements):
    m = {
        "id": ID,
        "op": "INSERT",
        "table": "ventes",
        "date_creation": "2026-11-09",
        "donnees": {"id_client": 7, "montant": 19.9},
    }
    m.update(changements)
    return m


def test_insert_valide():
    e = valider(message())
    assert e == Ecriture(uuid.UUID(ID), "INSERT", "ventes", date(2026, 11, 9), {"id_client": 7, "montant": 19.9})


def test_cle_kafka_est_l_id():
    """Clé = id : toutes les modifications d'une ligne vont dans la même partition, dans l'ordre."""
    assert valider(message()).cle_kafka == ID.encode()
    assert TOPIC == "app"


@pytest.mark.parametrize("brut", [json.dumps(message()), json.dumps(message()).encode()])
def test_accepte_le_json_brut(brut):
    assert valider(brut).id == uuid.UUID(ID)


def test_aller_retour_json():
    e = valider(message(op="UPDATE"))
    assert valider(e.en_json()) == e


def test_delete_sans_donnees():
    assert valider(message(op="DELETE", donnees={})).donnees == {}
    assert valider(message(op="DELETE", donnees=None)).donnees == {}


@pytest.mark.parametrize(
    "changements, erreur",
    [
        ({"id": "42"}, "id : UUID"),
        ({"id": None}, "id : UUID"),
        ({"op": "UPSERT"}, "op :"),
        ({"table": "Ventes-2026"}, "table :"),
        ({"date_creation": "09/11/2026"}, "date_creation"),
        ({"donnees": {}}, "ligne complète obligatoire"),
        ({"donnees": [1, 2]}, "doit être un objet"),
        ({"op": "UPDATE", "donnees": {"montant": {"$inc": 2}, "$inc": 1}}, "pas un incrément"),
        ({"donnees": {"id": "autre", "montant": 1}}, "donnees.id"),
        ({"op": "DELETE"}, "doit être vide pour un DELETE"),
        ({"version": 3}, "champs inconnus : version"),
    ],
)
def test_messages_refuses(changements, erreur):
    with pytest.raises(ContratInvalide, match=erreur):
        valider(message(**changements))


def test_toutes_les_erreurs_sont_listees():
    with pytest.raises(ContratInvalide) as e:
        valider({"id": "x", "op": "?", "table": "", "date_creation": ""})
    assert str(e.value).count(";") >= 3


@pytest.mark.parametrize("brut", [b"{pas du json", "[1, 2]"])
def test_json_invalide(brut):
    with pytest.raises(ContratInvalide):
        valider(brut)


def test_le_json_schema_decrit_le_meme_contrat():
    """contrats/ecriture.schema.json (pour l'application) et lakehouse/contrat.py restent alignés."""
    from pathlib import Path

    from lakehouse.contrat import OPERATIONS

    schema = json.loads((Path(__file__).resolve().parents[3] / "contrats" / "ecriture.schema.json").read_text())
    assert set(schema["properties"]) == {"id", "op", "table", "date_creation", "donnees"}
    assert tuple(schema["properties"]["op"]["enum"]) == OPERATIONS
    assert schema["additionalProperties"] is False
