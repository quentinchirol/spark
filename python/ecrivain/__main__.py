"""Writer Kafka « app » → PostgreSQL principal.

    python -m ecrivain

Boucle : lire jusqu'à TAILLE_LOT messages → les écrire en UNE transaction (lake.ecrire_lot par table) →
envoyer les rejets dans « app-rejets » → SEULEMENT ENSUITE valider les offsets dans Kafka.
Une erreur arrête le processus : Kubernetes le relance et la lecture reprend au dernier offset validé.
Un lot peut donc être rejoué, jamais perdu ; le rejeu est sans effet (version = offset, historique idempotent).

Variables : KAFKA_BOOTSTRAP, KAFKA_GROUPE, TAILLE_LOT, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ecrivain.lots import Rejet, preparer, tables_citees

TOPIC, TOPIC_REJETS = "app", "app-rejets"
journal = logging.getLogger("ecrivain")


async def ecrire(pool, lot) -> None:
    """Écrit le lot en une transaction. Si une ligne a des données incompatibles avec sa table (type, contrainte),
    le lot est rejoué ligne par ligne (un point de sauvegarde chacune) : seules les lignes fautives vont en rejet,
    au lieu de bloquer tout le topic sur un message toxique."""
    async with pool.acquire() as connexion:
        try:
            async with connexion.transaction():
                for table, lignes in lot.par_table.items():
                    await connexion.fetchval("SELECT lake.ecrire_lot($1, $2::jsonb)", table,
                                             json.dumps([ligne.donnees for ligne in lignes]))
            return
        except (asyncpg.exceptions.DataError, asyncpg.exceptions.IntegrityConstraintViolationError) as erreur:
            journal.warning("lot refusé (%s) : écriture ligne par ligne pour isoler les messages fautifs", erreur)
        async with connexion.transaction():
            for table, lignes in lot.par_table.items():
                for ligne in lignes:
                    try:
                        async with connexion.transaction():          # point de sauvegarde
                            await connexion.fetchval("SELECT lake.ecrire_lot($1, $2::jsonb)", table,
                                                     json.dumps([ligne.donnees]))
                    except (asyncpg.exceptions.DataError, asyncpg.exceptions.IntegrityConstraintViolationError) as erreur:
                        lot.rejets.append(Rejet(ligne.message, f"données refusées par PostgreSQL : {erreur}"))


async def tables_connues(connexion) -> tuple[set[str], frozenset[str]]:
    """(toutes les tables, tables critiques : écrites seulement par le chemin synchrone)."""
    lignes = await connexion.fetch("SELECT nom, critique FROM lake.tables")
    return {ligne["nom"] for ligne in lignes}, frozenset(ligne["nom"] for ligne in lignes if ligne["critique"])


async def principal() -> None:
    taille_lot = int(os.environ.get("TAILLE_LOT", "5000"))
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "lakehouse-kafka-bootstrap:9092")
    consommateur = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap,
        group_id=os.environ.get("KAFKA_GROUPE", "pg-writer"),
        enable_auto_commit=False,          # offsets validés à la main, après l'écriture en base
        auto_offset_reset="earliest",
        max_poll_records=taille_lot,
    )
    producteur = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all", enable_idempotence=True)
    # paramètres de connexion lus dans PGHOST, PGUSER, PGPASSWORD… (variables libpq)
    pool = await asyncpg.create_pool(min_size=1, max_size=2, ssl=os.environ.get("PGSSLMODE", "require"))
    arret = asyncio.Event()
    boucle = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        boucle.add_signal_handler(sig, arret.set)

    # Kafka peut être absent au démarrage : on réessaie au lieu de s'arrêter (sinon l'attente de Kubernetes entre
    # deux redémarrages s'allonge jusqu'à 5 min)
    for client in (consommateur, producteur):
        while not arret.is_set():
            try:
                await client.start()
                break
            except Exception as erreur:  # noqa: BLE001
                journal.warning("Kafka injoignable (%s), nouvel essai dans 3 s", erreur)
                await client.stop()
                await asyncio.sleep(3)
    async with pool.acquire() as connexion:
        connues, critiques = await tables_connues(connexion)
    total, depuis = 0, time.monotonic()
    try:
        while not arret.is_set():
            par_partition = await consommateur.getmany(timeout_ms=200, max_records=taille_lot)
            messages = [m for liste in par_partition.values() for m in liste]
            if not messages:
                continue
            if not tables_citees(messages) <= connues:      # table créée depuis le démarrage ?
                async with pool.acquire() as connexion:
                    connues, critiques = await tables_connues(connexion)
            lot = preparer(messages, connues, critiques)

            await ecrire(pool, lot)
            for rejet in lot.rejets:
                await producteur.send_and_wait(
                    TOPIC_REJETS, rejet.message.value, key=rejet.message.key,
                    headers=[("raison", rejet.raison.encode()), ("origine", f"{rejet.message.partition}:{rejet.message.offset}".encode())])
            await consommateur.commit()                      # seulement maintenant : rien n'est perdu

            total += len(messages)
            if time.monotonic() - depuis >= 10:
                journal.info("%d messages écrits en %.0f s (dernier lot : %d valides, %d rejets)",
                             total, time.monotonic() - depuis, lot.nb_valides, len(lot.rejets))
                total, depuis = 0, time.monotonic()
    finally:
        await consommateur.stop()
        await producteur.stop()
        await pool.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(principal())
