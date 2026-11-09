# Contrat d'écriture : application → lakehouse

L'application (React + FastAPI, déployée à part) n'écrit **jamais** directement dans le lakehouse.
Elle publie chaque action de l'utilisateur dans Kafka ; les writers du lakehouse s'occupent du reste.
Ce contrat est la seule chose que l'application doit connaître.

## Le message

Topic Kafka : **`app`** · clé du message : **l'`id` de la ligne** · valeur : JSON UTF-8.

```json
{
  "id": "0b6f3a52-6f1c-4f0e-9a43-3d6f2a0c9b11",
  "op": "UPDATE",
  "table": "ventes",
  "date_creation": "2026-11-09",
  "donnees": { "id_client": 7, "montant": 21.50, "statut": "payee" }
}
```

| Champ           | Obligatoire | Règle |
|-----------------|-------------|-------|
| `id`            | oui | UUID. **Généré par le front** au moment d'un `INSERT`, puis réutilisé pour tous les `UPDATE` / `DELETE` de cette ligne |
| `op`            | oui | `INSERT`, `UPDATE` ou `DELETE` |
| `table`         | oui | nom de la table côté lakehouse (minuscules, chiffres, `_`) |
| `date_creation` | oui | date de création de la ligne (`AAAA-MM-JJ`), **identique** dans tous les messages de cette ligne : elle sert au partitionnement des tables froides |
| `donnees`       | `INSERT`, `UPDATE` | **la ligne complète**, telle qu'elle doit être après l'action ; vide pour un `DELETE` |

Aucun autre champ n'est accepté. Le schéma est aussi disponible en JSON Schema :
[`contrats/ecriture.schema.json`](../contrats/ecriture.schema.json). La validation de référence est
`python/lakehouse/contrat.py` (utilisée par les writers, testée dans `python/tests/unitaires/test_contrat.py`).

## Les 4 règles

1. **Le front génère l'`id`.** Une requête renvoyée (réseau coupé, double clic, nouvelle tentative)
   retombe sur la même ligne : jamais de doublon.
2. **Toujours l'état complet, jamais un incrément.** Envoyer `"montant": 21.50`, pas « +2 ». Rejouer un
   message ne doit rien fausser.
3. **Clé Kafka = `id`.** Toutes les modifications d'une même ligne passent par la même partition et sont
   appliquées **dans l'ordre**. Le nombre de partitions du topic `app` est figé (24) : ne jamais le changer.
4. **Réponse `202 Accepted`.** L'API répond dès que Kafka a confirmé (3 copies, `acks=all`), avec l'`id`.
   La ligne est lisible environ 1 seconde plus tard. Une action qui exige une confirmation définitive
   immédiate (un paiement) passe par le chemin synchrone (US-8), pas par ce contrat.

## Réponses de l'API

| Code  | Quand | Que fait le front |
|-------|-------|-------------------|
| `202` | message accepté par Kafka | affiche l'action comme faite |
| `422` | message hors contrat (détail des erreurs dans la réponse) | corrige, ne réessaie pas tel quel |
| `429` / `503` | surcharge ou Kafka momentanément indisponible | réessaie **avec le même `id`** après 1 s, 2 s, 4 s… |

## Exemple côté application (FastAPI)

```python
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException

from lakehouse.contrat import TOPIC, ContratInvalide, valider

producer: AIOKafkaProducer


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(bootstrap_servers="lakehouse-kafka-bootstrap:9092",
                                acks="all", enable_idempotence=True, linger_ms=5)
    await producer.start()
    yield
    await producer.stop()


app = FastAPI(lifespan=lifespan)


@app.post("/ecritures", status_code=202)
async def ecrire(message: dict):
    try:
        e = valider(message)
    except ContratInvalide as err:
        raise HTTPException(status_code=422, detail=str(err))
    await producer.send_and_wait(TOPIC, e.en_json(), key=e.cle_kafka)
    return {"statut": "accepté", "id": str(e.id)}
```
