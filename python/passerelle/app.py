"""Passerelle d'écriture de référence (FastAPI) : POST /ecritures → topic Kafka « app ».

Implémente le contrat de docs/contrat-ecriture.md, pour les tests de charge et comme modèle pour l'application
(dont l'API d'écriture est déployée à part) :
  - 202 : message valide, enregistré dans Kafka sur au moins 2 nœuds (acks=all) ; rien d'autre à attendre ;
  - 422 : message hors contrat (raison détaillée), inutile de le renvoyer tel quel ;
  - 503 : Kafka indisponible ou trop lent ; renvoyer le MÊME message (même id) plus tard, sans risque de doublon.

POST /ecritures/critiques : actions critiques (paiement…), pour les tables déclarées critiques. Pas de Kafka : écriture
directe dans le PostgreSQL principal (lake.ecrire_critique) et réponse APRÈS le COMMIT (réplication synchrone) :
  - 201 : enregistré en base, avec la version attribuée ;
  - 422 : hors contrat, ou table non critique (elle passe par POST /ecritures) ;
  - 503 : PostgreSQL indisponible ou trop lent ; renvoyer le MÊME message (même id), sans risque de doublon.

    uvicorn passerelle.app:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager

import asyncpg

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from lakehouse.contrat import TOPIC, ContratInvalide, valider

DELAI_ENVOI_S = float(os.environ.get("DELAI_ENVOI_S", "2"))
DELAI_CRITIQUE_S = float(os.environ.get("DELAI_CRITIQUE_S", "5"))


async def creer_pool():
    """Connexions au PostgreSQL principal pour le chemin critique (variables PGHOST, PGUSER, PGPASSWORD…)."""
    return await asyncpg.create_pool(min_size=1, max_size=int(os.environ.get("CONNEXIONS_CRITIQUES", "10")),
                                     ssl=os.environ.get("PGSSLMODE", "require"), timeout=5)


def creer_producteur():
    from aiokafka import AIOKafkaProducer
    return AIOKafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "lakehouse-kafka-bootstrap:9092"),
        acks="all", enable_idempotence=True, linger_ms=5,
    )


def creer_app(fabrique_producteur=creer_producteur, fabrique_pool=creer_pool) -> FastAPI:
    @asynccontextmanager
    async def cycle_de_vie(app: FastAPI):
        # Kafka peut être absent au démarrage (déploiement simultané, bascule) : la passerelle démarre quand même,
        # se déclare « pas prête » et répond 503 jusqu'à ce que le producteur soit connecté
        app.state.pret = asyncio.Event()
        # pool du chemin critique : créé à la première action critique (PostgreSQL n'est pas requis pour démarrer)
        app.state.pool, app.state.verrou_pool = None, asyncio.Lock()

        async def essayer() -> bool:
            producteur = fabrique_producteur()
            try:
                await producteur.start()
            except Exception:  # noqa: BLE001
                await producteur.stop()
                return False
            app.state.producteur = producteur
            app.state.pret.set()
            return True

        async def reessayer():
            while not await essayer():
                await asyncio.sleep(2)

        # premier essai avant d'accepter du trafic ; en cas d'échec, nouveaux essais en arrière-plan
        tache = None if await essayer() else asyncio.create_task(reessayer())
        yield
        if tache:
            tache.cancel()
        if app.state.pool is not None:
            await app.state.pool.close()
        if app.state.pret.is_set():
            await app.state.producteur.stop()

    app = FastAPI(title="Passerelle d'écriture", lifespan=cycle_de_vie)

    @app.post("/ecritures", status_code=202)
    async def ecrire(requete: Request):
        corps = await requete.body()
        try:
            ecriture = valider(corps)
        except ContratInvalide as erreur:
            return JSONResponse(status_code=422, content={"statut": "refusé", "raison": str(erreur)})
        if not app.state.pret.is_set():
            return JSONResponse(status_code=503, content={"statut": "réessayer", "id": str(ecriture.id)},
                                headers={"Retry-After": "1"})
        try:
            await asyncio.wait_for(
                app.state.producteur.send_and_wait(TOPIC, ecriture.en_json(), key=ecriture.cle_kafka), DELAI_ENVOI_S)
        except Exception:  # noqa: BLE001 : Kafka absent, délai dépassé… le client renvoie le même message
            return JSONResponse(status_code=503, content={"statut": "réessayer", "id": str(ecriture.id)},
                                headers={"Retry-After": "1"})
        return {"statut": "accepté", "id": str(ecriture.id)}

    async def pool_critique():
        async with app.state.verrou_pool:
            if app.state.pool is None:
                app.state.pool = await fabrique_pool()
        return app.state.pool

    @app.post("/ecritures/critiques", status_code=201)
    async def ecrire_critique(requete: Request):
        corps = await requete.body()
        try:
            ecriture = valider(corps)
        except ContratInvalide as erreur:
            return JSONResponse(status_code=422, content={"statut": "refusé", "raison": str(erreur)})
        message = json.loads(ecriture.en_json())

        async def ecrire():
            pool = await pool_critique()
            async with pool.acquire() as connexion:
                return await connexion.fetchval("SELECT lake.ecrire_critique($1, $2::jsonb)", ecriture.table,
                                                json.dumps(message))
        try:
            version = await asyncio.wait_for(ecrire(), DELAI_CRITIQUE_S)
        except asyncpg.exceptions.DataError as erreur:      # table non critique, données refusées
            return JSONResponse(status_code=422, content={"statut": "refusé", "raison": str(erreur)})
        except Exception:  # noqa: BLE001 : base absente, bascule, délai dépassé… le client renvoie le même message
            return JSONResponse(status_code=503, content={"statut": "réessayer", "id": str(ecriture.id)},
                                headers={"Retry-After": "1"})
        return {"statut": "enregistré", "id": str(ecriture.id), "version": version}

    @app.get("/sante")
    async def sante():
        # sonde de disponibilité : pas de trafic tant que Kafka n'est pas joignable
        if not app.state.pret.is_set():
            return JSONResponse(status_code=503, content={"statut": "kafka indisponible"})
        return {"statut": "ok"}

    return app


app = creer_app()
