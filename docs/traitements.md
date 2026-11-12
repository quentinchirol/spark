# Traitements Spark à la demande (Dagster)

Un traitement lit des fichiers déposés dans `s3://sources/`, les passe en **bronze → silver → gold** (tables Iceberg du
catalogue `lake`) et s'arrête : aucune ressource ne reste allumée entre deux exécutions.

```
Dagster (planification ou clic) ─► Job Kubernetes « bronze » ─► Job « silver » ─► Job « gold »
                                     pilote Spark + executors      (une étape = un pod pilote, ses executors,
                                     supprimés à la fin             puis plus rien après DUREE_VIE_JOBS_S)
```

- Dagster : `python/orchestration` (définitions), `k8s/dagster` (déploiement, base `lake-outils`) ;
- traitements : `k8s/traitements` (scripts Spark, montés dans les pods pilotes par la ConfigMap `traitements`) ;
- sans Spark Operator : [décision 002](decisions/002-dagster-sans-spark-operator.md).

## Traitement « ventes »

Fichiers CSV dans `s3://sources/ventes/`, colonnes `id,date,client,produit,quantite,prix_unitaire`.

| Étape | Table | Règle |
|-------|-------|-------|
| bronze | `lake.bronze.ventes` | chaque fichier chargé **une seule fois**, tel quel (texte), avec son nom, son numéro de ligne et l'heure de chargement |
| silver | `lake.silver.ventes` | types appliqués ; ligne invalide (date, quantité ≤ 0 ou illisible, prix négatif, id vide) écartée ; une ligne par `id` : la plus récente gagne |
| gold | `lake.gold.ventes_par_jour` | nombre de ventes, quantité et chiffre d'affaires par jour, table remplacée en une fois |

Planifié chaque heure (à h+5, heure de Paris). Déposer un fichier :

```bash
kubectl -n spark port-forward svc/minio 9000:9000
mc alias set lake http://localhost:9000 <AWS_ACCESS_KEY_ID> <AWS_SECRET_ACCESS_KEY>   # k8s/secrets/s3.env
mc cp ventes-2026-11-11.csv lake/sources/ventes/
```

## Traitement « evenements » (topic Kafka)

Les événements (IoT, journaux, clics) sont envoyés par leurs producteurs dans le topic Kafka `events` (12 partitions).
Le traitement les charge dans `lake.bronze.events` : `cle`, `valeur` (message brut), `topic`, `partition`, `offset`,
`horodatage` (Kafka), `_charge_le`.

| Quand | Effet |
|-------|-------|
| retard ≥ `SEUIL_EVENEMENTS` (100 000) | le capteur `evenements_en_attente` lance le chargement à sa vérification suivante (chaque minute) |
| retard plus faible | lancement au plus tard `DELAI_MAX_EVENEMENTS_S` (10 min) après le précédent : un événement est dans bronze en **moins de 15 min** |
| run en cours ou en file | aucun nouveau run |
| un run démarre | il lit ce qui est disponible à son démarrage, par lots (`MAX_PAR_PARTITION` messages par partition), un commit Iceberg par lot, puis s'arrête |
| arrêt brutal (pod, nœud) | la tentative suivante reprend après le dernier lot validé : **aucune perte, aucun doublon** ([décision 003](decisions/003-evenements-position-dans-bronze.md)) |
| messages supprimés par la rétention Kafka avant chargement | le traitement s'arrête en erreur (perte signalée, jamais sautée) |

Retard actuel (position enregistrée après chaque lot) :

```bash
kubectl -n spark exec lakehouse-noeuds-0 -- bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group lakehouse-events
```

## Lancer, suivre, relancer

```bash
kubectl -n spark port-forward svc/dagster-webserver 3000:3000     # http://localhost:3000
```

- **lancer** : *Jobs → ventes → Launch run* (ou attendre la planification) ;
- **suivre** : chaque run montre ses 3 étapes, leurs tentatives et la fin du journal de chaque pilote Spark ;
  `kubectl -n spark get jobs,pods -l app=traitement` pendant l'exécution ;
- **relancer** : *Re-execute* sur un run en échec (tout le run, ou à partir de l'étape en échec).

Garanties :

| Situation | Effet |
|-----------|-------|
| une étape échoue (pod tué, executor perdu, erreur Spark) | Dagster la relance, **3 essais** au plus (attente 20 s puis 40 s) ; ensuite le run est en échec |
| échec pendant une écriture | **aucune donnée partielle** : une écriture est un commit Iceberg, fait en entier ou pas du tout |
| étape relancée après un succès partiel du run | pas de doublon : bronze ignore les fichiers déjà chargés, silver fusionne par `id`, gold est remplacée |
| deux runs « ventes » en même temps | le second attend la fin du premier (un run par traitement) |
| fin d'une étape | executors supprimés par Spark ; Job et pod pilote supprimés 5 min après (tout de suite en cas d'échec) |

## Ajouter un traitement

1. un script Spark dans `k8s/traitements/` (une fonction par étape, chaque étape écrit en un commit Iceberg :
   `MERGE INTO`, `writeTo(...).createOrReplace()`, `INSERT OVERWRITE`) et son entrée dans le `configMapGenerator`
   `traitements` de `k8s/kustomization.yaml` ;
2. ses étapes dans `python/orchestration/__init__.py` (`@op` qui appelle `lancer_spark("script.py", "étape")`, avec
   `retry_policy=RELANCES`), un `@job` avec le tag `traitement` et, si besoin, une `ScheduleDefinition` ;
3. un test dans `python/tests/unitaires/test_orchestration.py`.

## Ressources

| Pod | CPU réservé | Mémoire |
|-----|-------------|---------|
| pilote Spark (un par étape en cours) | `CPU_PILOTE`, 250m | 1,5 Gi (limite 2 Gi) |
| executor (1 à 4 par étape, selon le travail) | `CPU_EXECUTEUR`, 500m | 1,5 Gi |
| dagster-code / webserver / daemon | 100m / 50m / 50m | 512 Mi / 256 Mi / 256 Mi |
| lake-outils (2 instances) | 100m | 256 Mi |

Variables de `dagster-code` : `CPU_PILOTE`, `CPU_EXECUTEUR`, `DUREE_VIE_JOBS_S` (300), `IMAGE_SPARK` (remplacée par
kustomize comme les autres images).

## Mesuré en CI (kind, runner GitHub à 2 cœurs)

`tests/integration/tester_orchestration.py`, à chaque pull request qui touche ces fichiers :

| Vérifié | Résultat |
|---------|----------|
| run complet (3 étapes, chacune : pod pilote + executor créés puis supprimés) | réussi en **~2 min 40** (161 s, puis 151 s) |
| contenu de bronze / silver / gold | exact (lignes invalides écartées, vente corrigée mise à jour, aucun doublon) |
| nouveau fichier | seul lui est chargé |
| échec provoqué pendant l'écriture de gold | 3 tentatives puis échec (317 s) ; snapshot gold **inchangé** ; le run suivant rattrape |
| après les runs | 0 pod pilote, 0 executor, 0 Job |
| Dagster | 4 runs et leur résultat dans l'historique ; planification active ; démons en bonne santé |

Sur kind, l'essentiel de la durée est le démarrage des JVM (pilote puis executor) à chaque étape.
