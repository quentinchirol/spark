# Cluster Spark 4.1.3 + Apache Iceberg

**Versions** : Spark **4.1.3** (version contre laquelle Iceberg 1.12.0 est compilé et testé),
Iceberg **1.12.0**, MinIO (fork pgsty, stockage S3, 4 pods), catalogue Apache Polaris **1.8.0** (2 pods).

```
spark/
├── compose/              cluster standalone simple (Docker Compose)
│   ├── docker-compose.yml
│   ├── .env
│   └── data/             monté dans /opt/spark-data
├── image/                image spark-lakehouse:4.1.3 (serveur et executors)
│   ├── Dockerfile        Spark 4.1.3 + jars Iceberg + extension Java
│   ├── src/fr/spark/auth/  TokenAuthInterceptor (tokens), AuditListener (audit)
│   └── postgres-lake/    image PostgreSQL 18 (Spilo) + pg_lake du PostgreSQL principal
├── k8s/                  déploiement Kubernetes : kubectl apply -k k8s
│   ├── kustomization.yaml
│   ├── commun/           namespace, droits (RBAC)
│   ├── spark-connect/    serveur : deployment.yaml, spark-defaults.conf, fairscheduler.xml
│   ├── stockage/         minio.yaml (S3, 4 pods), polaris.yaml (catalogue, 2 pods + PostgreSQL), polaris-init.py
│   ├── trino/            SQL interactif en LECTURE SEULE (DBeaver, dashboards)
│   ├── postgres/         PostgreSQL principal (3 instances synchrones) + sql/ : modèle chaud + froid
│   ├── operateurs/       opérateur Zalando postgres-operator : kubectl apply --server-side -k k8s/operateurs (avant k8s/)
│   ├── pipeline/         CronJob : Spark recalcule les tables agrégées chaque heure
│   ├── config-kustomize/ images désignées dans des variables (image des executors)
│   └── secrets/          tokens, clés S3, token pipeline, certificat Trino  (non versionnés)
├── infra/gke/            cluster GKE 3 zones + déploiement avec les images de ghcr.io
│   ├── creer-cluster.sh, supprimer-cluster.sh, deployer.sh
│   └── manifestes/       overlay kustomize de k8s/ pour GKE
├── contrats/             contrat d'écriture application → lakehouse (JSON Schema)
├── docs/                 documentation (contrat d'écriture, PostgreSQL principal, décisions…)
├── .github/workflows/    CI (lint, tests, manifestes, images) ; publication des images (images.yml)
├── .devcontainer/        environnement prêt pour GitHub Codespaces
├── scripts/dev/          démarrer / arrêter la stack dans un cluster kind local
└── python/               tests et benchmark
    ├── lakehouse/        code partagé : connexion, donnees, requetes, requetes_sql, agregats
    ├── tests/            tests pytest
    ├── benchmark.py      chronométrage cache / iceberg / trino
    ├── pipeline.py       traitement planifié (tables agrégées)
    ├── Dockerfile        image lakehouse-pipeline du CronJob
    ├── requirements.txt
    └── resultats/        CSV du benchmark (non versionnés ; archives/ : anciens runs)
```

---

# Branches, CI et Codespaces

**Branches** : `main` (version stable) ← `develop` (intégration) ← `feature/US-<n>-<story>`.
Chaque user story est développée sur sa propre branche, puis fusionnée dans `develop` par pull request.

**CI** (`.github/workflows/ci.yml`, à chaque push et pull request) :

| Job          | Vérifie                                                                    |
|--------------|----------------------------------------------------------------------------|
| `python`     | lint (`ruff`), compilation, tests unitaires sans cluster (`python/tests/unitaires`) |
| `manifestes` | chaque `kustomization.yaml` se construit et respecte les schémas Kubernetes (`kubeconform`) |
| `images`     | les images Docker se construisent                                          |

**Images** (`.github/workflows/images.yml`, à chaque push sur `develop` et `main`) : publication de
`ghcr.io/quentinchirol/spark-lakehouse` et `ghcr.io/quentinchirol/lakehouse-pipeline`, étiquetées
par branche (`develop`, `main`) et par commit (`sha-1a2b3c4`).

**Codespaces** : *Code → Codespaces → Create codespace* sur le dépôt (machine 4 cœurs / 16 Go minimum).
Python, Java 21, Docker, kubectl, Helm et kind sont installés. Pour lancer toute la stack :

```bash
scripts/dev/demarrer-kind.sh     # crée le cluster kind, construit et charge les images, déploie
scripts/dev/arreter-kind.sh      # supprime le cluster
```

---

# Docker Compose (cluster standalone simple)

Un master et N workers en mode standalone, basés sur l'image officielle `apache/spark:4.1.3-python3`
(sans Iceberg).

```bash
docker compose -f compose/docker-compose.yml up -d
```

- UI du master : http://localhost:8080
- Endpoint du master : `spark://spark-master:7077` (depuis le réseau Docker)

| Variable (`compose/.env`) | Défaut                       |
|---------------------------|------------------------------|
| `SPARK_IMAGE`           | `apache/spark:4.1.3-python3` |
| `SPARK_WORKER_REPLICAS` | `2`                          |
| `SPARK_WORKER_CORES`    | `2`                          |
| `SPARK_WORKER_MEMORY`   | `2g`                         |

Changer le nombre de workers à chaud : `docker compose -f compose/docker-compose.yml up -d --scale spark-worker=4`.
Le dossier `compose/data` est monté dans `/opt/spark-data`. Shell interactif :

```bash
docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077
```

Arrêter : `docker compose -f compose/docker-compose.yml down`

---

# Kubernetes : Spark Connect partagé, authentifié par token, tables Iceberg

```
 PC d'alice ──┐  sc://…:15002            ┌─────────── pod spark-connect ───────────┐
 (token A)    ├─ Authorization: Bearer ─►│ 1. TokenAuthInterceptor : token → alice │
 PC de bob  ──┘                          │ 2. session alice / session bob (isolées)│
 (token B)                               │ 3. AuditListener → logs "AUDIT {...}"   │
                                         └───────┬────────────────────┬────────────┘
                                                 ▼                    ▼
                          executors à la demande (2 → 3)      catalogue Polaris (OAuth2)
                                                 │                    │
                                                 └──► MinIO (S3) ◄────┘  s3://warehouse/
```

- **Authentification** : un appel sans token, ou avec un token inconnu, est refusé (`UNAUTHENTICATED`).
- **Identité imposée** : l'utilisateur vient du token, jamais de ce qu'envoie le client.
  Si bob se connecte avec son token et `user_id=alice`, la requête est enregistrée au nom de **bob**,
  et il ne peut pas accéder aux sessions d'alice.
- **Sessions isolées** : chacun a ses propres vues temporaires et réglages de session.
- **Executors partagés** : créés quand des requêtes attendent, supprimés après environ 1 min
  d'inactivité. Le planificateur `FAIR` les répartit entre les requêtes simultanées.
- **Tables Iceberg** : catalogue `lake`, par exemple `spark.table("lake.bench_1000000.ventes")`.

## Fichiers

| Fichier                                   | Rôle                                                       |
|-------------------------------------------|------------------------------------------------------------|
| `image/`                                  | Image `spark-lakehouse:4.1.3` (serveur **et** executors) : Spark 4.1.3 + jars Iceberg + extension Java auth/audit |
| `k8s/kustomization.yaml`                  | Point d'entrée, génère la ConfigMap de conf et les Secrets |
| `k8s/commun/`                             | Namespace `spark` et droits pour créer/supprimer les pods executors |
| `k8s/spark-connect/deployment.yaml`       | Deployment du serveur + Service (port 15002)               |
| `k8s/spark-connect/spark-defaults.conf`   | Config Spark : executors, auth, audit, catalogue Iceberg, jointures sans redistribution |
| `k8s/spark-connect/fairscheduler.xml`     | Partage équitable des executors                            |
| `k8s/stockage/minio.yaml`                 | Stockage S3 : MinIO distribué (4 pods, codage d'effacement) + Job de création du bucket et des utilisateurs |
| `k8s/stockage/polaris.yaml`               | Catalogue Apache Polaris (2 pods) + base PostgreSQL + Jobs d'initialisation (`polaris-init.py`) |
| `k8s/trino/`                              | Trino : config, catalogue `lake` (même Iceberg), droits par utilisateur (`rules.json`) + Service (port 8443) |
| `k8s/secrets/tokens`                      | Tokens `utilisateur=token` (Spark Connect **et** Trino), **non versionné** |
| `k8s/pipeline/cronjob.yaml`               | Traitement planifié (identité `pipeline`) : `python/pipeline.py` chaque heure |
| `k8s/secrets/s3-trino.env`                | Clé S3 **en lecture seule** de Trino (`TRINO_S3_ACCESS_KEY`, `TRINO_S3_SECRET_KEY`), **non versionné** |
| `k8s/secrets/pipeline.env`                | `SPARK_CONNECT_TOKEN` du traitement planifié (= ligne `pipeline=` de `tokens`), **non versionné** |
| `k8s/secrets/trino.env`, `trino-tls.pem`, `trino-cert.pem` | Secret interne Trino, certificat HTTPS (clé + certificat ; certificat seul pour les clients), **non versionnés** |
| `k8s/secrets/s3.env`                      | Identifiants S3 (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), **non versionné** |
| `python/`                                 | Tests unitaires et benchmark (voir plus bas)               |

## Déployer

```bash
docker build -t spark-lakehouse:4.1.3 image
docker build -t lakehouse-pipeline:1.0 python
docker build -t postgres-lake:18 image/postgres-lake

# secrets locaux (k8s/secrets/ n'est jamais versionné) : crée seulement les fichiers manquants
scripts/generer-secrets.sh

# opérateurs d'abord : leurs CRD doivent exister avant le reste
kubectl apply --server-side -k k8s/operateurs
kubectl wait --for=condition=established crd/postgresqls.acid.zalan.do
kubectl apply -k k8s
```

Les tables Iceberg (MinIO) et le catalogue (PostgreSQL de Polaris) sont sur des volumes persistants : ils survivent aux
redémarrages des pods (mais pas à `kubectl delete -k k8s`, qui supprime les volumes).

L'image des executors n'est écrite nulle part en dur : c'est toujours celle du serveur Spark Connect
(variable `SPARK_EXECUTOR_IMAGE`, mise à jour par kustomize en même temps que l'image du conteneur).

## Déployer sur GKE (3 zones)

Cluster régional : nœuds répartis sur 3 zones, plus un groupe de nœuds **analytique** réservé
(taint `role=analytique:NoSchedule`, vide tant qu'il ne sert pas). Les images viennent de ghcr.io,
aucune n'est construite sur un nœud.

```bash
# 1. cluster (idempotent ; région par défaut europe-west9 = Paris)
PROJET=mon-projet-gcp infra/gke/creer-cluster.sh

# 2. plateforme : jeton GitHub « classic » avec le seul droit read:packages (premier déploiement)
read -rs GHCR_TOKEN && export GHCR_TOKEN
GHCR_USER=quentinchirol infra/gke/deployer.sh

# version figée plutôt que la dernière image de develop
TAG=sha-1a2b3c4 infra/gke/deployer.sh
```

Vérifier : `kubectl get nodes -L topology.kubernetes.io/zone,role` (3 zones) et
`gcloud container node-pools list --cluster lakehouse --region europe-west9` (groupe `analytique`).
`deployer.sh` échoue si un pod utilise encore une image locale (`imagePullPolicy: Never`).
Supprimer : `PROJET=mon-projet-gcp infra/gke/supprimer-cluster.sh`.

## Secrets et rotation

Les secrets (tokens, clés S3, certificat Trino) vivent uniquement dans `k8s/secrets/`, exclu de Git.
La CI échoue si un fichier de ce dossier est versionné ou si une clé est détectée dans le dépôt (gitleaks).

```bash
scripts/generer-secrets.sh --rotation        # régénère tous les secrets
kubectl apply -k k8s
kubectl -n spark rollout restart sts/minio deploy/polaris deploy/trino deploy/spark-connect
```

Après une rotation, les anciennes clés et anciens tokens ne fonctionnent plus : redistribuez les nouveaux
tokens (`k8s/secrets/tokens`) et le certificat (`k8s/secrets/trino-cert.pem`).

## PostgreSQL principal : écritures de l'application

`lake-principal` (base `lake`) : 3 instances PostgreSQL 18 en réplication synchrone (opérateur Zalando), extension
pg_lake. L'application lit `vue.<table>` (dernier état, visible tout de suite) ; chaque minute, la table chaude est
fusionnée dans la table Iceberg `app.<table>`, lue par Spark, Trino et le pool analytique. Détails :
[docs/postgres-principal.md](docs/postgres-principal.md).

Sauvegardé en continu (WAL-G) vers un stockage **hors du cluster**, restaurable à la seconde sur 7 jours :
[docs/sauvegardes.md](docs/sauvegardes.md).

## Stockage et catalogue : MinIO + Polaris

- **MinIO** (`http://minio:9000`) : 4 pods, 1 volume chacun, codage d'effacement. Un pod arrêté ne gêne ni la lecture ni
  l'écriture. Deux utilisateurs S3 : `spark` (lecture + écriture, `k8s/secrets/s3.env`) et `trino` (lecture seule).
- **Polaris** (`http://polaris:8181/api/catalog`) : catalogue Iceberg REST du bucket `warehouse`, 2 pods sur la même base.
  Spark s'authentifie comme principal `spark` (rôle `ecriture`), Trino comme `trino` (rôle `lecture`) ; PostgreSQL + pg_lake
  lira les mêmes tables (US-7). Les identifiants sont dans `k8s/secrets/polaris-*.env`.
- **Initialisation** (Jobs lancés par `kubectl apply -k k8s`) : `minio-init` (bucket, utilisateurs), `polaris-bootstrap`
  (schéma et compte root), `polaris-init` (catalogue `lake`, rôles, principaux).
- Console MinIO : `kubectl -n spark port-forward svc/minio 9001:9001` (compte `k8s/secrets/minio-root.env`).

## Gérer les utilisateurs

`k8s/secrets/tokens` contient une ligne par personne :

```
alice=<token de 64 caractères hexadécimaux>
bob=<token de 64 caractères hexadécimaux>
```

Pour ajouter ou révoquer quelqu'un, modifiez ce fichier puis lancez `kubectl apply -k k8s`.
La modification est prise en compte **à chaud** en 1 min environ, sans redémarrer le serveur
ni couper les sessions en cours. Pour générer un token : `openssl rand -hex 32` (ou `scripts/generer-secrets.sh --rotation` pour tout régénérer).

Trino utilise le même fichier, mais ne le relit qu'à son démarrage :
`kubectl -n spark rollout restart deploy/trino` après une modification.

## Se connecter (côté utilisateur)

```bash
pip install pyspark-client==4.1.3
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002/;token=<votre token>").getOrCreate()
spark.sql("SHOW NAMESPACES IN lake").show()
spark.table("lake.bench_1000000.ventes").groupBy("id_produit").count().show()
```

## Rôles : Spark écrit, Trino lit

```
               ┌──────────────── Spark (écrit) ─────────────────┐
               │ lake.bench_*.ventes / clients / produits        │
               │        │  CronJob pipeline-dashboard (chaque     │
               │        ▼  heure, identité "pipeline")           │
               │ lake.dashboard.ca_jour_pays_categorie           │  tables agrégées
               │ lake.dashboard.resume_clients                   │
               └───────────────────────────┬─────────────────────┘
                                           │ lecture seule
               ┌────────────── Trino (lit) ┴─────────────────────┐
               │ DBeaver, dashboards, SQL ad hoc                  │
               └──────────────────────────────────────────────────┘
```

| Besoin                                                        | Moteur |
|---------------------------------------------------------------|--------|
| Écrire ou transformer des tables, préparer des agrégats, Python/DataFrame, traitements longs | **Spark** (Spark Connect) |
| Lire : SQL ad hoc, DBeaver, dashboards, réponses en secondes  | **Trino** |

La lecture seule de Trino est appliquée à **trois niveaux** (vérifiés par les tests) :

1. `k8s/trino/rules.json` : lecture seule sur `lake`, personne n'est propriétaire d'un schéma ;
2. `iceberg.security=read_only` dans `k8s/trino/catalog/lake.properties` : le connecteur refuse aussi
   les procédures de maintenance (`ALTER TABLE … EXECUTE optimize`, `expire_snapshots`), que
   `rules.json` **ne sait pas interdire** (limite documentée de Trino) ;
3. **clé S3 en lecture seule** (`k8s/secrets/s3-trino.env`) : même mal configuré, Trino ne peut écrire
   aucun fichier. Le pod Trino ne reçoit jamais la clé d'écriture de Spark.

Garde-fous Trino (`k8s/trino/config.properties`, `resource-groups.json`) : une requête est arrêtée
au-delà de **2 min** (un traitement plus long relève de Spark : en faire une table agrégée), et chaque
utilisateur a sa file d'attente (3 requêtes simultanées au plus) pour qu'un dashboard très consulté
ne bloque pas les autres.

**Ajouter une table pour un dashboard trop lent** : écrire son calcul dans `python/lakehouse/agregats.py`,
ajouter un test dans `tests/test_pipeline.py`, puis reconstruire l'image du CronJob
(`docker build -t lakehouse-pipeline:1.0 python`). Lancer le calcul sans attendre l'heure :

```bash
kubectl -n spark create job --from=cronjob/pipeline-dashboard pipeline-manuel
kubectl -n spark logs -f job/pipeline-manuel -c pipeline
```

Pendant le calcul, Trino continue de lire la version précédente complète des tables (Iceberg).

## Trino : SQL interactif (DBeaver, dashboards)

Trino lit les **mêmes tables Iceberg** que Spark (catalogue `lake`), avec un temps de réponse
adapté aux outils interactifs. Chaque personne se connecte **avec son propre nom et son propre token**
(le même que pour Spark Connect) : aucun compte partagé.

| Paramètre      | Valeur                                                  |
|----------------|---------------------------------------------------------|
| Adresse        | `https://localhost:8443` (HTTPS obligatoire : Trino refuse les mots de passe en clair) |
| Utilisateur    | `alice`                                                 |
| Mot de passe   | le token d'alice (`k8s/secrets/tokens`)                 |
| Catalogue      | `lake`                                                  |
| Certificat     | `k8s/secrets/trino-cert.pem` (auto-signé)               |

**DBeaver** : Nouvelle connexion → *Trino* → hôte `localhost`, port `8443`, utilisateur et token ;
onglet *Propriétés du pilote* : `SSL=true`, `SSLTrustStorePath=<chemin>/k8s/secrets/trino-cert.pem`.

**Python** :

```python
from lakehouse.connexion import connexion_trino
curseur = connexion_trino("alice", "<token d'alice>").cursor()
curseur.execute("SELECT pays, count(*) FROM lake.bench_1000000.clients GROUP BY pays")
print(curseur.fetchall())
```

**Droits** (`k8s/trino/rules.json`, relu automatiquement en 1 min après `kubectl apply -k k8s`) :
lecture seule pour tous sur `lake` (seul Spark écrit les tables, voir plus haut) et sur `system` ;
chacun ne voit que ses propres requêtes. On peut y ajouter des droits par utilisateur, par table, des filtres de
lignes ou des masques de colonnes ([documentation](https://trino.io/docs/current/security/file-system-access-control.html)).

**Qui a exécuté quoi** : chaque requête est enregistrée avec son utilisateur, visible dans
`system.runtime.queries` (ses propres requêtes) et dans l'interface web https://localhost:8443/ui/.
Cet historique est **en mémoire** (perdu au redémarrage de Trino) ; un journal permanent équivalent
au journal AUDIT de Spark nécessiterait un *event listener* Trino.

## Qui a exécuté quoi

Journal d'audit, une ligne JSON par événement :

```bash
kubectl -n spark logs deploy/spark-connect | grep AUDIT
```

```json
{"time":"2026-11-08T18:59:32.581Z","event":"QUERY_START","user":"alice","session":"a8f1…","operation":"3bd4…","statement":"sql_command { … query: \"SELECT count(*) FROM ventes WHERE id % 7 = 0\" … }"}
{"time":"2026-11-08T18:59:32.806Z","event":"QUERY_END","user":"alice","operation":"3bd4…","rows":"0"}
```

Événements : `SESSION_START`, `QUERY_START`, `QUERY_END`, `QUERY_FAILED`, `QUERY_CANCELED`.
Les tentatives refusées apparaissent en `WARN TokenAuthInterceptor: Appel refusé`.

L'UI Spark (onglet **Connect**) liste aussi les sessions et requêtes par utilisateur :
`kubectl -n spark port-forward deploy/spark-connect 4040:4040` → http://localhost:4040

## Organisation des tables Iceberg

Les tables du benchmark sont écrites par `python/lakehouse/donnees.py` pour profiter d'Iceberg :

| Table      | Organisation                                         | Effet                                   |
|------------|------------------------------------------------------|-----------------------------------------|
| `clients`  | `bucket(16, id_client)`                              | jointure avec `ventes` sans redistribution |
| `ventes`   | `bucket(16, id_client)`, triée par `date` dans chaque bucket, blocs Parquet de 4 Mo | jointure sans redistribution ; un filtre sur la date saute les blocs hors période |
| `produits` | non partitionnée (1 000 lignes)                      | diffusée à tous les executors (broadcast) |

> **Piège évité** : partitionner `ventes` par mois **et** par client obligerait à activer
> `spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled`. Sur Spark 4.1.3,
> cette option **fausse les fonctions de fenêtre** (tous les rangs valent 1). Le test
> `python/tests/test_iceberg.py::test_memes_resultats_qu_en_memoire[fenetre]` détecte ce problème.

## Tests et benchmark (`python/`)

| Fichier             | Rôle                                                                   |
|--------------------------|-------------------------------------------------------------------|
| `lakehouse/requetes.py`  | Les requêtes mesurées : filtre, filtre par mois, group by, jointures, distinct, tri, fenêtre |
| `lakehouse/donnees.py`   | Génération des tables sur le cluster, écriture/lecture Iceberg          |
| `tests/test_requetes.py` | Chaque requête sur un mini jeu de données au résultat connu            |
| `tests/test_iceberg.py`  | Mêmes résultats en Iceberg qu'en mémoire ; jointure sans `Exchange` ; filtre transmis au scan |
| `tests/test_auth.py`     | Sans token ou token invalide refusés, sessions isolées                 |
| `tests/test_trino.py`    | Trino : chacun identifié par son token, mauvais identifiants refusés, **15 types d'écriture refusés** (tables intactes ensuite), les 9 requêtes SQL donnent les mêmes résultats que Spark, durée maximale, file par utilisateur, historique séparé |
| `tests/test_stockage.py` | Clé S3 de Trino en lecture seule (créer, modifier, supprimer refusés), clé de Spark en écriture, pod Trino sans clé d'écriture |
| `tests/test_pipeline.py` | Agrégats exacts ; écrits par Spark, lus à l'identique par Trino, non modifiables par Trino ; réécriture visible dans Trino ; écritures tracées au nom de `pipeline` |
| `tests/test_comparaison.py` | Gros volume (`-m gros_volume`) : chaque requête sur les tables du benchmark, résultat identique cache/Iceberg, cohérent avec les données générées, durées en CSV |
| `benchmark.py`           | Chronométrage cache / Iceberg (Spark) / Trino, export CSV               |
| `lakehouse/requetes_sql.py` | Les 9 requêtes en SQL pour Trino                                    |
| `lakehouse/agregats.py`, `pipeline.py` | Tables agrégées des dashboards, traitement planifié          |
| `lakehouse/connexion.py` | Connexion Spark : `SPARK_CONNECT_URL`, `SPARK_CONNECT_TOKEN` (sinon 1er token de `k8s/secrets/tokens`) ; Trino : `connexion_trino()` |
| `tests/conftest.py`      | Fixtures pytest : session Spark et mini jeu de données |

```bash
cd python
pip install -r requirements.txt

pytest -v                                          # tests (sur le serveur Spark Connect)
pytest -m gros_volume                              # tests à gros volume : 100k et 1M ventes (~1 min 30)
pytest -m gros_volume --tailles 10000000           # ... ou les tailles voulues
python benchmark.py                                # 100k, 1M, 10M ventes, cache + iceberg + trino
python benchmark.py --sources iceberg --tailles 1000000 --repetitions 5
python benchmark.py --recreer-iceberg              # réécrit les tables Iceberg
```

Le benchmark écrit dans `python/resultats/` (séparateur `;` et décimales `,` pour Excel) :

- `benchmark_<date>.csv` : **toutes** les exécutions, une ligne chacune
  (`date;session_id;source;requete;nb_ventes;execution;a_froid;duree_s;nb_lignes_resultat;nb_executors_max`),
  préparation des données comprise (`requete=preparation`) ;
- `resume_<date>.csv` : à froid / min / médiane / moyenne / max par source, requête et taille ;
- `comparaison_<date>.csv` : les 3 sources côte à côte ; `gain_iceberg_sur_cache_pct` et
  `gain_trino_sur_spark_iceberg_pct` (même stockage, moteur différent), positif = plus rapide ;
- une requête en échec (ex. limite mémoire ou durée de Trino) est notée dans la colonne `erreur`.

Les tests à gros volume écrivent `tests_comparaison_<date>.csv`, une ligne par taille et requête
(`nb_lignes_resultat;resultat_identique;coherent_cache;coherent_iceberg;duree_cache_s;duree_iceberg_s;gain_iceberg_pct`).
Ils réutilisent les tables Iceberg du benchmark (`lake.bench_<taille>`) et les créent si elles manquent.

`resultats/optimisation/` garde le benchmark avant / après le réglage des executors
(2 toujours prêts, 2 cœurs, `shuffle.partitions` 32, cache des métadonnées Iceberg) et `avant_apres.csv`.

Comment sont mesurés les temps :
- `cache` : tables générées puis mises en cache **en mémoire** avant les mesures ;
- `iceberg` : tables lues **depuis le stockage S3** à chaque exécution (écrites une seule fois,
  puis réutilisées aux lancements suivants) ;
- `trino` : mêmes tables Iceberg ; `EXPLAIN ANALYZE` exécute toute la requête sans renvoyer ses lignes ;
- le calcul est forcé par `write.format("noop")`, sans rapatrier le résultat sur le poste ;
- `a_froid=oui` désigne la 1re exécution de chaque requête ;
- `nb_executors_max` est relevé via `kubectl` pendant la mesure (vide si `kubectl` est indisponible) ;
- `session_id` permet de retrouver l'utilisateur authentifié dans le journal d'audit.

## Limites à connaître

- **TLS** : le client Python n'envoie un token en clair que vers `localhost`. Pour des postes
  distants, il faut exposer le port 15002 derrière une terminaison TLS (Ingress gRPC, load
  balancer…) et ajouter `;use_ssl=true` à l'URL.
- **Les logs du pod disparaissent avec lui** : en production, envoyez-les vers un collecteur
  (Loki, ELK…) pour conserver l'audit.
- **Isolation logique seulement** : une requête très lourde ralentit les autres, et un serveur
  qui plante coupe toutes les sessions (1 seul replica, les sessions sont en mémoire).
- **Base de Polaris** : le catalogue vit dans un PostgreSQL à 1 pod (`polaris-db`) : s'il est indisponible, Polaris
  l'est aussi. Il sera remplacé par le PostgreSQL à haute disponibilité (CloudNativePG) de la US-7.
- **MinIO sur 3 zones** : 4 pods = 2 + 1 + 1 par zone. La perte d'un pod ou de la zone à 1 pod laisse lecture et écriture ;
  la perte de la zone à 2 pods laisse le stockage en lecture seule (aucune donnée perdue).
- **Droits sur les tables** : côté Spark, tous les utilisateurs authentifiés peuvent lire **et écrire**
  toutes les tables `lake.*` (pas seulement `pipeline`) ; les droits par utilisateur ne s'appliquent
  qu'à Trino (`rules.json`).
- **Mémoire** : Kubernetes dispose d'environ 11,5 Go (Docker Desktop). Serveur Spark, stockage,
  Trino (2 Gi) et 3 executors au maximum (1792 Mi chacun) en réservent ~95 %. Pour plus d'executors,
  augmenter la mémoire de Docker Desktop puis `spark.dynamicAllocation.maxExecutors`.
- **Certificat Trino auto-signé** : à remplacer par un certificat d'une autorité reconnue pour des postes distants.
- Au démarrage, une ligne `ERROR … Timed out waiting for [Pod]` est normale : Spark attend
  1 s que son propre pod soit prêt, ce qui n'arrive qu'une fois le port 15002 ouvert.

## Supprimer

```bash
kubectl delete -k k8s
```
