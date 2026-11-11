"""Préparation des lots du writer : validation, clé Kafka, version = offset, tables inconnues, regroupement."""
import json
import uuid
from dataclasses import dataclass

from ecrivain.lots import preparer, tables_citees


@dataclass
class Message:
    key: bytes | None
    value: bytes | None
    partition: int = 0
    offset: int = 0


def message(table="ventes", op="INSERT", offset=0, cle=None, **changements):
    id_ = str(uuid.uuid4())
    corps = {"id": id_, "op": op, "table": table, "date_creation": "2026-11-11",
             "donnees": {} if op == "DELETE" else {"id": id_, "montant": 10}}
    corps.update(changements)
    return Message(key=(cle if cle is not None else corps["id"]).encode(), value=json.dumps(corps).encode(), offset=offset)


def test_message_valide_regroupe_par_table_avec_offset_comme_version():
    m1, m2, m3 = message(offset=7), message(table="clients", offset=8), message(offset=9)
    lot = preparer([m1, m2, m3], {"ventes", "clients"})
    assert not lot.rejets
    assert [ligne.donnees["version"] for ligne in lot.par_table["ventes"]] == [7, 9]
    assert [ligne.donnees["version"] for ligne in lot.par_table["clients"]] == [8]
    assert lot.par_table["ventes"][0].message is m1
    assert lot.nb_valides == 3


def test_le_message_garde_l_etat_complet_et_la_date():
    m = message(offset=3)
    ligne = preparer([m], {"ventes"}).par_table["ventes"][0].donnees
    corps = json.loads(m.value)
    assert ligne["id"] == corps["id"] and ligne["donnees"] == corps["donnees"]
    assert ligne["date_creation"] == "2026-11-11" and ligne["op"] == "INSERT"


def test_message_hors_contrat_rejete_avec_la_raison():
    m = Message(key=b"x", value=b"pas du json", offset=1)
    lot = preparer([m], {"ventes"})
    assert not lot.par_table and len(lot.rejets) == 1
    assert lot.rejets[0].raison.startswith("contrat")


def test_increment_rejete():
    m = message(donnees={"$inc": {"montant": 2}})
    assert preparer([m], {"ventes"}).rejets


def test_cle_differente_de_l_id_rejetee():
    lot = preparer([message(cle=str(uuid.uuid4()))], {"ventes"})
    assert "clé Kafka" in lot.rejets[0].raison


def test_message_sans_cle_rejete():
    m = message()
    m.key = None
    assert preparer([m], {"ventes"}).rejets


def test_table_inconnue_rejetee():
    lot = preparer([message(table="inconnue")], {"ventes"})
    assert "table inconnue" in lot.rejets[0].raison


def test_un_rejet_n_empeche_pas_les_autres_messages():
    lot = preparer([message(offset=1), Message(key=b"x", value=b"{}", offset=2), message(offset=3)], {"ventes"})
    assert lot.nb_valides == 2 and len(lot.rejets) == 1


def test_tables_citees_ignore_les_messages_invalides():
    assert tables_citees([message(table="clients"), Message(key=b"x", value=b"{")]) == {"clients"}


def test_table_critique_rejetee_par_le_chemin_kafka():
    lot = preparer([message(table="paiements"), message(offset=2)], {"ventes", "paiements"}, frozenset({"paiements"}))
    assert lot.nb_valides == 1 and "critique" in lot.rejets[0].raison
