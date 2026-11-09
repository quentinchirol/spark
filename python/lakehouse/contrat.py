"""Contrat d'écriture entre l'application et le lakehouse (topic Kafka « app »).

Un message = l'état complet d'UNE ligne après une action de l'utilisateur :

    {
      "id": "0b6f…",                     # UUID généré par le front pour un INSERT, puis réutilisé
      "op": "INSERT" | "UPDATE" | "DELETE",
      "table": "ventes",                 # table de faits ou d'entités côté lakehouse
      "date_creation": "2026-11-09",     # date de création de la ligne (partitionnement des tables froides)
      "donnees": {...}                   # ligne COMPLÈTE (jamais un incrément) ; absent ou {} pour DELETE
    }

Règles (voir docs/contrat-ecriture.md) :
  - clé Kafka = id : toutes les modifications d'une ligne passent par la même partition, dans l'ordre ;
  - renvoyer le même message est sans effet (idempotence) : une ligne n'existe qu'une fois par id ;
  - `donnees` contient l'état final, jamais « +2 » ou « -1 ».
"""
from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import Any

TOPIC = "app"
OPERATIONS = ("INSERT", "UPDATE", "DELETE")
_NOM_TABLE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
# clés qui trahissent un incrément au lieu d'un état complet
_MOTS_INCREMENT = ("$inc", "$add", "increment", "delta")


class ContratInvalide(ValueError):
    """Le message ne respecte pas le contrat d'écriture."""


@dataclass(frozen=True)
class Ecriture:
    id: uuid.UUID
    op: str
    table: str
    date_creation: date
    donnees: dict[str, Any] = field(default_factory=dict)

    @property
    def cle_kafka(self) -> bytes:
        """Clé du message Kafka : l'id de la ligne (ordre garanti par ligne)."""
        return str(self.id).encode()

    def en_json(self) -> bytes:
        return json.dumps(
            {
                "id": str(self.id),
                "op": self.op,
                "table": self.table,
                "date_creation": self.date_creation.isoformat(),
                "donnees": self.donnees,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode()


def valider(message: dict[str, Any] | bytes | str) -> Ecriture:
    """Valide un message brut (dict ou JSON) et renvoie l'écriture typée.

    Lève ContratInvalide avec la liste de toutes les erreurs trouvées.
    """
    if isinstance(message, (bytes, str)):
        try:
            message = json.loads(message)
        except json.JSONDecodeError as e:
            raise ContratInvalide(f"JSON invalide : {e}") from e
    if not isinstance(message, dict):
        raise ContratInvalide("le message doit être un objet JSON")

    erreurs: list[str] = []
    inconnus = set(message) - {"id", "op", "table", "date_creation", "donnees"}
    if inconnus:
        erreurs.append(f"champs inconnus : {', '.join(sorted(inconnus))}")

    try:
        id_ = uuid.UUID(str(message.get("id")))
    except ValueError:
        id_ = None
        erreurs.append("id : UUID obligatoire (généré par le front pour un INSERT)")

    op = message.get("op")
    if op not in OPERATIONS:
        erreurs.append(f"op : doit valoir {', '.join(OPERATIONS)}")

    table = message.get("table")
    if not isinstance(table, str) or not _NOM_TABLE.match(table):
        erreurs.append("table : nom en minuscules (lettres, chiffres, _)")

    try:
        date_creation = date.fromisoformat(str(message.get("date_creation")))
    except ValueError:
        date_creation = None
        erreurs.append("date_creation : date AAAA-MM-JJ obligatoire (partitionnement)")

    donnees = message.get("donnees", {})
    if donnees is None:
        donnees = {}
    if not isinstance(donnees, dict):
        erreurs.append("donnees : doit être un objet (la ligne complète)")
        donnees = {}
    elif op in ("INSERT", "UPDATE"):
        if not donnees:
            erreurs.append(f"donnees : ligne complète obligatoire pour un {op}")
        if any(k.lower() in _MOTS_INCREMENT for k in donnees):
            erreurs.append("donnees : état complet attendu, pas un incrément")
        if "id" in donnees and str(donnees["id"]) != str(message.get("id")):
            erreurs.append("donnees.id : doit être identique à id")
    elif op == "DELETE" and donnees:
        erreurs.append("donnees : doit être vide pour un DELETE")

    if erreurs:
        raise ContratInvalide("; ".join(erreurs))
    return Ecriture(id=id_, op=op, table=table, date_creation=date_creation, donnees=dict(donnees))
