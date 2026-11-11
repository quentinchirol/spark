# Décision 002 : Dagster lance Spark directement (pods éphémères), sans Spark Operator

**Date** : 11/11/2026 · **Statut** : acceptée · **Story** : US-10

## Contexte

L'architecture v5 prévoyait des traitements Spark « à la demande » lancés par Dagster via le **Spark Operator**
(ressource `SparkApplication`). Le Spark Operator ne s'installe que par Helm et ajoute un opérateur, un CRD et un
webhook à maintenir.

## Décision

Dagster lance chaque étape Spark comme un **Job Kubernetes éphémère** (`execute_k8s_job` de `dagster-k8s`) :

- le pod du Job est le pilote Spark (`spark-submit`, mode client, image `spark-lakehouse`) ; Spark crée ses executors
  et les supprime à la fin ; le Job disparaît ensuite (`ttlSecondsAfterFinished`) ;
- relance : `RetryPolicy` de Dagster, 3 essais au plus ;
- pas de donnée partielle : chaque écriture est un commit Iceberg atomique (`MERGE INTO`, `createOrReplace`) ; une
  étape relancée réécrit sans doublon ;
- historique et journaux : dans Dagster (et le journal du pod pendant sa durée de vie).

## Conséquences

- Un composant de moins (ni Spark Operator, ni CRD, ni Helm).
- Les traitements n'ont pas d'objet Kubernetes dédié à surveiller : on suit les runs dans Dagster.
- Si un jour il faut des fonctions propres au Spark Operator (réessais côté Kubernetes, planification hors Dagster),
  l'ajouter ne change pas les scripts Spark.
