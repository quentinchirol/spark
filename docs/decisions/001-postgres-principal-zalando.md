# Décision 001 : PostgreSQL principal sur l'opérateur Zalando (Patroni), pas CloudNativePG

**Date** : 11/11/2026 · **Statut** : acceptée · **Story** : US-7

## Contexte

L'architecture v5 prévoyait le PostgreSQL principal (écritures de l'application, table chaude, fusion vers Iceberg)
sur CloudNativePG, 3 instances en réplication synchrone, avec l'extension **pg_lake** pour lire et écrire les tables
Iceberg via Polaris.

pg_lake a besoin d'un **second processus**, `pgduck_server` (moteur DuckDB), sur chaque hôte PostgreSQL, joint par un
socket Unix. CloudNativePG ne permet pas d'ajouter un conteneur aux pods d'une instance sans écrire un plugin Go (CNPG-I).

## Décision

PostgreSQL principal géré par **l'opérateur Zalando** (`postgres-operator` v2.0.1, Patroni) :

- 3 instances, `synchronous_mode` Patroni (aucune écriture confirmée perdue lors d'une bascule) ;
- PostgreSQL **18** : image Spilo `spilo-18:4.1-p2` (image par défaut de l'opérateur ; aucun tag `spilo-17` n'existe
  pour Spilo 4.1) + paquet `postgresql-18-pg-lake` (dépôt Pigsty) : `image/postgres-lake/` ;
- `pgduck_server` en **sidecar** (champ `sidecars` du manifeste), socket partagé par un volume `emptyDir` sur `/tmp` ;
- base nommée **`lake`** : pg_lake utilise le nom de la base comme préfixe de catalogue REST, il doit être égal au
  catalogue Polaris `lake` ;
- sauvegardes continues (WAL-G) vers un stockage hors cluster : US-5.

## Options écartées

| Option | Pourquoi |
|--------|----------|
| CloudNativePG + plugin CNPG-I maison | Un plugin Go à écrire et maintenir pour un seul conteneur. |
| CloudNativePG sans pg_lake au principal | La fusion vers Iceberg et la vue chaud + froid passeraient par un pod à part : flux de données v5 modifié. |

## Conséquences

- Remplacer « CloudNativePG » par « Zalando postgres-operator (Patroni) » dans le schéma v5 (Notion).
- Le pool analytique (US-13) n'est pas concerné : pods PostgreSQL + pg_lake en lecture seule, sans opérateur.
- La base de Polaris (`polaris-db`, 1 pod) pourra passer sur le même opérateur.
