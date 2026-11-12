# Décision 003 : événements Kafka → bronze, position de reprise dans les données (pas de checkpoint Spark)

**Date** : 12/11/2026 · **Statut** : acceptée · **Story** : US-11

## Contexte

Le backlog prévoyait un flux Spark Structured Streaming en mode `availableNow`, avec son répertoire de checkpoint
dans MinIO. Spark écrit un checkpoint par l'API de fichiers Hadoop : dans MinIO, cela demande `hadoop-aws` et une
partie du SDK AWS v2. L'image contient déjà ce SDK, non relocalisé, dans `iceberg-aws-bundle`, mais pas les modules
dont `hadoop-aws` a besoin pour écrire (gestionnaire de transferts, client asynchrone et Netty). Les ajouter
mettrait deux versions des mêmes classes dans l'image (risque d'erreurs au chargement), ou alourdirait l'image
d'environ 500 Mo avec le paquet complet du SDK.

## Décision

`k8s/traitements/evenements.py` lit Kafka **par lots bornés** avec le connecteur Kafka de Spark (lecture « batch »
d'une plage d'offsets), et écrit chaque lot dans `lake.bronze.events` en **un commit Iceberg** contenant la
partition et l'offset Kafka de chaque message.

- **Reprise** : au démarrage, la lecture repart du plus grand offset déjà présent dans bronze, partition par
  partition. La position est donc écrite **dans le même commit que les données** (dans MinIO, avec elles) : un lot
  non validé est relu en entier, un lot validé ne l'est jamais deux fois.
- **Comme `availableNow`** : le traitement lit ce qui est disponible à son démarrage, puis s'arrête.
- **Retard** : après chaque lot, la position est aussi enregistrée dans le groupe Kafka `lakehouse-events`,
  uniquement pour mesurer le retard (capteur Dagster, `kafka-consumer-groups.sh`) ; elle ne sert jamais à reprendre.
- **Perte** : si la rétention de Kafka a supprimé des messages avant leur chargement, le traitement s'arrête en
  erreur au lieu de les sauter.

## Conséquences

- Aucune dépendance Hadoop S3 ni répertoire de checkpoint à gérer ou à nettoyer : une seule source de vérité, la
  table.
- Garantie « exactement une fois » vérifiable directement : chaque `(partition, offset)` du topic apparaît une fois
  dans bronze (test d'intégration avec arrêt brutal du pilote).
- Le calcul de la position lit les colonnes `partition` et `offset` de bronze à chaque run : coût qui grandit avec
  la table. À surveiller ; si besoin, limiter la lecture aux derniers jours (les offsets croissent avec le temps).
- Pour un vrai flux continu (latence de quelques secondes), il faudrait Structured Streaming avec checkpoint, donc
  `hadoop-aws` : non nécessaire pour l'objectif de moins de 15 minutes.
