"""Passerelle d'écriture : codes de réponse du contrat (202, 422, 503), message publié avec la clé = id."""
import asyncio
import json
import time
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


# --- chemin critique : POST /ecritures/critiques --------------------------------------------------------------
import asyncpg  # noqa: E402


class FausseConnexion:
    def __init__(self, base):
        self.base = base

    async def fetchval(self, requete, table, message):
        if self.base.lente:
            await asyncio.sleep(5)
        if table not in self.base.critiques:
            raise asyncpg.exceptions.DataError(f"table {table} non critique")
        self.base.ecrites.append((table, json.loads(message)))
        return len(self.base.ecrites)


class FauxPool:
    def __init__(self, critiques=("paiements",), lente=False):
        self.critiques, self.lente, self.ecrites = set(critiques), lente, []

    def acquire(self):
        base = self

        class Contexte:
            async def __aenter__(self):
                return FausseConnexion(base)

            async def __aexit__(self, *exc):
                return False
        return Contexte()

    async def close(self):
        pass


def client_critique(pool=None, panne=False):
    async def fabrique():
        if panne:
            raise OSError("PostgreSQL injoignable")
        return pool
    return TestClient(creer_app(lambda: FauxProducteur(), fabrique))


def test_action_critique_enregistree_puis_201_avec_version():
    pool, corps = FauxPool(), ecriture(table="paiements")
    with client_critique(pool) as c:
        reponse = c.post("/ecritures/critiques", json=corps)
    assert reponse.status_code == 201
    assert reponse.json() == {"statut": "enregistré", "id": corps["id"], "version": 1}
    table, message = pool.ecrites[0]
    assert table == "paiements" and message["id"] == corps["id"] and message["donnees"]["montant"] == 3


def test_action_critique_sur_table_normale_422():
    with client_critique(FauxPool()) as c:
        reponse = c.post("/ecritures/critiques", json=ecriture(table="ventes"))
    assert reponse.status_code == 422 and "non critique" in reponse.json()["raison"]


def test_action_critique_hors_contrat_422_sans_ecriture():
    pool = FauxPool()
    with client_critique(pool) as c:
        assert c.post("/ecritures/critiques", json=ecriture(table="paiements", op="MERGE")).status_code == 422
    assert not pool.ecrites


def test_action_critique_base_injoignable_503_avec_le_meme_id():
    corps = ecriture(table="paiements")
    with client_critique(panne=True) as c:
        reponse = c.post("/ecritures/critiques", json=corps)
    assert reponse.status_code == 503 and reponse.json()["id"] == corps["id"]


def test_action_critique_trop_lente_503(monkeypatch):
    monkeypatch.setattr("passerelle.app.DELAI_CRITIQUE_S", 0.1)
    with client_critique(FauxPool(lente=True)) as c:
        assert c.post("/ecritures/critiques", json=ecriture(table="paiements")).status_code == 503


def test_le_chemin_critique_ne_passe_pas_par_kafka():
    producteur, pool = FauxProducteur(), FauxPool()

    async def fabrique():
        return pool
    with TestClient(creer_app(lambda: producteur, fabrique)) as c:
        assert c.post("/ecritures/critiques", json=ecriture(table="paiements")).status_code == 201
    assert not producteur.envois and len(pool.ecrites) == 1


def test_pool_critique_ouvert_des_le_demarrage():
    ouvertures = []

    async def fabrique():
        ouvertures.append(1)
        return FauxPool()
    with TestClient(creer_app(lambda: FauxProducteur(), fabrique)) as c:
        assert c.get("/sante").status_code == 200
        for _ in range(50):
            if ouvertures:
                break
            time.sleep(0.02)
        assert ouvertures == [1]
        assert c.post("/ecritures/critiques", json=ecriture(table="paiements")).status_code == 201
    assert ouvertures == [1]


def test_base_injoignable_au_demarrage_la_passerelle_demarre(caplog):
    with client_critique(panne=True) as c:
        assert c.get("/sante").status_code == 200
        assert c.post("/ecritures/critiques", json=ecriture(table="paiements")).status_code == 503
    assert any("503 (OSError" in m for m in caplog.messages)


def test_identifiants_critiques_lus_dans_le_volume(tmp_path, monkeypatch):
    from passerelle import app as module
    monkeypatch.setattr(module, "IDENTIFIANTS_CRITIQUES", tmp_path)
    monkeypatch.delenv("PGUSER", raising=False)
    with pytest.raises(RuntimeError, match="absents"):
        module.identifiants_critiques()
    # le Secret arrive après le démarrage : lu à la tentative suivante
    (tmp_path / "username").write_text("lake_critique\n")
    (tmp_path / "password").write_text("secret\n")
    assert module.identifiants_critiques() == {"user": "lake_critique", "password": "secret"}


def test_identifiants_critiques_par_variables_sans_volume(tmp_path, monkeypatch):
    from passerelle import app as module
    monkeypatch.setattr(module, "IDENTIFIANTS_CRITIQUES", tmp_path / "absent")
    monkeypatch.setenv("PGUSER", "moi")
    assert module.identifiants_critiques() == {}
