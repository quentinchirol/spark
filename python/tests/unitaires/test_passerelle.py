"""Passerelle d'écriture : codes de réponse du contrat (202, 422, 503), message publié avec la clé = id."""
import asyncio
import json
import uuid

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from passerelle.app import creer_app  # noqa: E402


class FauxProducteur:
    def __init__(self, panne=False, lent=False):
        self.envois, self.panne, self.lent = [], panne, lent

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send_and_wait(self, topic, valeur, key):
        if self.panne:
            raise ConnectionError("Kafka indisponible")
        if self.lent:
            await asyncio.sleep(5)
        self.envois.append((topic, valeur, key))


def client(producteur):
    return TestClient(creer_app(lambda: producteur))


def ecriture(**changements):
    id_ = str(uuid.uuid4())
    corps = {"id": id_, "op": "INSERT", "table": "ventes", "date_creation": "2026-11-11", "donnees": {"id": id_, "montant": 3}}
    corps.update(changements)
    return corps


def test_ecriture_valide_acceptee_et_publiee_avec_cle_id():
    producteur = FauxProducteur()
    corps = ecriture()
    with client(producteur) as c:
        reponse = c.post("/ecritures", json=corps)
    assert reponse.status_code == 202 and reponse.json() == {"statut": "accepté", "id": corps["id"]}
    topic, valeur, cle = producteur.envois[0]
    assert topic == "app" and cle == corps["id"].encode() and json.loads(valeur)["donnees"]["montant"] == 3


def test_ecriture_hors_contrat_refusee_422_sans_publication():
    producteur = FauxProducteur()
    with client(producteur) as c:
        reponse = c.post("/ecritures", json=ecriture(op="MERGE"))
    assert reponse.status_code == 422 and "op" in reponse.json()["raison"]
    assert not producteur.envois


def test_kafka_indisponible_503_avec_le_meme_id():
    corps = ecriture()
    with client(FauxProducteur(panne=True)) as c:
        reponse = c.post("/ecritures", json=corps)
    assert reponse.status_code == 503 and reponse.json()["id"] == corps["id"]
    assert reponse.headers["retry-after"] == "1"


def test_kafka_trop_lent_503(monkeypatch):
    monkeypatch.setattr("passerelle.app.DELAI_ENVOI_S", 0.1)
    with client(FauxProducteur(lent=True)) as c:
        assert c.post("/ecritures", json=ecriture()).status_code == 503


class ProducteurSansKafka(FauxProducteur):
    async def start(self):
        raise ConnectionError("Kafka injoignable")


def test_sans_kafka_la_passerelle_demarre_et_repond_503():
    corps = ecriture()
    with client(ProducteurSansKafka()) as c:
        assert c.get("/sante").status_code == 503
        reponse = c.post("/ecritures", json=corps)
    assert reponse.status_code == 503 and reponse.json()["id"] == corps["id"]


def test_prete_des_le_demarrage_quand_kafka_est_joignable():
    with client(FauxProducteur()) as c:
        assert c.get("/sante").status_code == 200
