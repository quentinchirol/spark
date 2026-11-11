"""Passerelle d'écriture de référence (FastAPI) : POST /ecritures → topic Kafka « app ».

Implémente le contrat de docs/contrat-ecriture.md, pour les tests de charge et comme modèle pour l'application
(dont l'API d'écriture est déployée à part) :
  - 202 : message valide, enregistré dans Kafka sur au moins 2 nœuds (acks=all) ; rien d'autre à attendre ;
  - 422 : message hors contrat (raison détaillée), inutile de le renvoyer tel quel ;
  - 503 : Kafka indisponible ou trop lent ; renvoyer le MÊME message (même id) plus tard, sans risque de doublon.

    uvicorn passerelle.app:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from lakehouse.contrat import TOPIC, ContratInvalide, valider

DELAI_ENVOI_S = float(os.environ.get("DELAI_ENVOI_S", "2"))


def creer_producteur():
    from aiokafka import AIOKafkaProducer
    return AIOKafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "lakehouse-kafka-bootstrap:9092"),
        acks="all", enable_idempotence=True, linger_ms=5,
    )


def creer_app(fabrique_producteur=creer_producteur) -> FastAPI:
    @asynccontextmanager
    async def cycle_de_vie(app: FastAPI):
        app.state.producteur = fabrique_producteur()
        await app.state.producteur.start()
        yield
        await app.state.producteur.stop()

    app = FastAPI(title="Passerelle d'écriture", lifespan=cycle_de_vie)

    @app.post("/ecritures", status_code=202)
    async def ecrire(requete: Request):
        corps = await requete.body()
        try:
            ecriture = valider(corps)
        except ContratInvalide as erreur:
            return JSONResponse(status_code=422, content={"statut": "refusé", "raison": str(erreur)})
        try:
            await asyncio.wait_for(
                app.state.producteur.send_and_wait(TOPIC, ecriture.en_json(), key=ecriture.cle_kafka), DELAI_ENVOI_S)
        except Exception:  # noqa: BLE001 : Kafka absent, délai dépassé… le client renvoie le même message
            return JSONResponse(status_code=503, content={"statut": "réessayer", "id": str(ecriture.id)},
                                headers={"Retry-After": "1"})
        return {"statut": "accepté", "id": str(ecriture.id)}

    @app.get("/sante")
    async def sante():
        return {"statut": "ok"}

    return app


app = creer_app()
