"""Préparation d'un lot de messages Kafka pour lake.ecrire_lot (logique pure, sans Kafka ni PostgreSQL).

- chaque message est validé par le contrat d'écriture (lakehouse.contrat) ;
- la clé Kafka doit être l'id de la ligne : c'est elle qui garantit l'ordre des modifications d'une ligne ;
- la version d'un message = son offset Kafka. Une ligne est toujours dans la même partition (clé = id),
  donc ses versions sont comparables et croissantes ;
- les messages sont regroupés par table ; ceux qui ne peuvent pas être écrits vont dans les rejets, avec la raison.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Protocol

from lakehouse.contrat import ContratInvalide, valider


class MessageKafka(Protocol):
    key: bytes | None
    value: bytes | None
    partition: int
    offset: int


@dataclass
class Rejet:
    message: MessageKafka
    raison: str


@dataclass
class Ligne:
    """Message valide prêt pour lake.ecrire_lot, avec le message Kafka d'origine (pour un rejet éventuel)."""
    donnees: dict
    message: MessageKafka


@dataclass
class Lot:
    par_table: dict[str, list[Ligne]] = field(default_factory=dict)
    rejets: list[Rejet] = field(default_factory=list)

    @property
    def nb_valides(self) -> int:
        return sum(len(messages) for messages in self.par_table.values())


def preparer(messages: Iterable[MessageKafka], tables_connues: set[str]) -> Lot:
    lot = Lot()
    for message in messages:
        try:
            ecriture = valider(message.value or b"")
        except ContratInvalide as erreur:
            lot.rejets.append(Rejet(message, f"contrat : {erreur}"))
            continue
        if message.key is None or message.key.decode(errors="replace") != str(ecriture.id):
            lot.rejets.append(Rejet(message, "clé Kafka différente de l'id : l'ordre de la ligne n'est pas garanti"))
            continue
        if ecriture.table not in tables_connues:
            lot.rejets.append(Rejet(message, f"table inconnue : {ecriture.table} (lake.creer_table)"))
            continue
        lot.par_table.setdefault(ecriture.table, []).append(Ligne({
            "id": str(ecriture.id),
            "op": ecriture.op,
            "table": ecriture.table,
            "date_creation": ecriture.date_creation.isoformat(),
            "donnees": ecriture.donnees,
            "version": message.offset,
        }, message))
    return lot


def tables_citees(messages: Iterable[MessageKafka]) -> set[str]:
    """Tables nommées par des messages valides : pour rafraîchir la liste des tables connues avant de rejeter."""
    noms = set()
    for message in messages:
        try:
            noms.add(valider(message.value or b"").table)
        except ContratInvalide:
            pass
    return noms
