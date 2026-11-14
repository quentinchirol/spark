# Pool analytique : SQL en lecture seule pour Power BI, Qlik et les notebooks

Le pool analytique (`lake-analytique`) est un groupe de PostgreSQL + pg_lake **sans données propres** : chaque pod lit
les tables Iceberg du catalogue `lake` (Polaris) directement dans MinIO. Les analystes s'y connectent avec n'importe
quel outil qui parle PostgreSQL ; l'application et son PostgreSQL principal ne sont jamais sollicités.

```
Power BI / Qlik / DBeaver / notebook ──PostgreSQL──► lake-analytique (2 à N pods, nœuds « analytique »)
                                                        │  catalogue : Polaris (principal en lecture seule)
                                                        └─ fichiers  : MinIO (clé S3 en lecture seule), via DuckDB
```

## Ce que l'on voit

| Schéma | Contenu | Fraîcheur |
|--------|---------|-----------|
| `gold`, `silver`, `bronze` | tables des traitements Spark ([traitements.md](traitements.md)) | dernière version validée, à chaque requête |
| `app` | tables de l'application, fusionnées par le PostgreSQL principal ; `_supprime` = ligne supprimée | **moins de 2 minutes** (fusion chaque minute) |
| `vue` | `vue.<table>` : lignes non supprimées de `app.<table>` (comme `vue.*` sur le principal, sans la dernière minute) | idem |
| tout autre namespace du catalogue | rattaché automatiquement | idem |

Une nouvelle table du catalogue apparaît dans le pool en moins d'une minute ; une table dont les colonnes changent
est rattachée de nouveau. Chaque requête lit la dernière version validée de la table : pas d'import, pas de
rafraîchissement à planifier (mode **DirectQuery** de Power BI).

## Se connecter

Paramètres (identiques pour tous les outils) :

| Paramètre | Valeur |
|-----------|--------|
| Serveur | `lake-analytique` dans le cluster ; depuis un poste : voir ci-dessous |
| Port | `5432` |
| Base | `lake` |
| Utilisateur | `analyste` |
| Mot de passe | `ANALYTIQUE_MOT_DE_PASSE` de `k8s/secrets/analytique.env` (jamais versionné) |

Depuis un poste, en attendant l'accès réseau sécurisé (US-19) :

```bash
kubectl -n spark port-forward svc/lake-analytique 5432:5432     # serveur : localhost
```

### Power BI Desktop

1. *Obtenir des données → Base de données PostgreSQL*.
2. Serveur `localhost` (ou l'adresse du pool), base `lake`, mode **DirectQuery**.
3. Identifiants : *Base de données*, utilisateur `analyste`.
4. Choisir les tables du schéma `gold` (par exemple `gold.ventes_par_jour`).

Power BI utilise le pilote Npgsql intégré : rien à installer. Pour le service Power BI (publication), passer par une
passerelle de données locale qui joint le pool.

### Qlik Sense

*Ajouter des données → PostgreSQL* (connecteur ODBC Qlik) : hôte, port `5432`, base `lake`, utilisateur `analyste`.

### Python / notebook (lire, exporter en CSV, Excel, Parquet)

Le module `python/analyse` (exemple : `notebooks/exploration.ipynb`) :

```bash
pip install -r python/analyse/requirements.txt
export LAKE_MOT_DE_PASSE=…          # LAKE_HOTE (localhost), LAKE_PORT (5432) si besoin
```

```python
from analyse import tables, lire, exporter
tables()                                                         # tables et vues que je peux lire
df = lire("SELECT * FROM gold.ventes_par_jour WHERE date >= %s", ("2026-11-01",))   # DataFrame pandas
exporter("SELECT * FROM silver.ventes", "ventes.parquet")        # .csv, .xlsx ou .parquet selon l'extension
```

| Format | Écriture | Types |
|--------|----------|-------|
| `.csv` | produit par le serveur (`COPY`), écrit au fil de l'eau ; UTF-8, en-tête, séparateur virgule | texte |
| `.parquet` | par lots de 50 000 lignes (curseur côté serveur), compression zstd | ceux de PostgreSQL : entiers, décimaux exacts, dates, horodatages UTC ; uuid et JSON en texte |
| `.xlsx` | en mémoire, 1 048 575 lignes au plus (limite d'Excel) | horodatages en UTC, sans fuseau |

## Beaucoup d'analystes en même temps

| Situation | Effet |
|-----------|-------|
| jusqu'à 8 requêtes à la fois par pod | exécutées tout de suite (PgBouncer : une connexion PostgreSQL par transaction) |
| davantage | elles **attendent leur tour** dans PgBouncer, sans erreur |
| attente de plus de **2 minutes** | refus propre : erreur `query_wait_timeout`, à relancer plus tard ; rien ne plante |
| requête de plus de **2 minutes** | coupée (`statement_timeout`) |
| CPU du pool au-delà de 70 % de ce qui est réservé | un pod de plus (2 → 8, `HorizontalPodAutoscaler`) ; retour au minimum après 5 min de calme |

Les outils de BI se connectent à PgBouncer comme à PostgreSQL (même port 5432, même base) ; les requêtes préparées
des pilotes (Npgsql de Power BI, JDBC de Qlik) sont prises en charge. Un paramètre de session (`SET …`) ne dure que le
temps d'une transaction : passer par les options de connexion de l'outil.

## Garanties

| Garantie | Comment |
|----------|---------|
| lecture seule | rôle `analyste` sans aucun droit d'écriture, transactions en lecture seule par défaut, tables rattachées `read_only`, principal Polaris et clé S3 en lecture seule |
| requêtes bornées | `statement_timeout` de 120 s, `work_mem` de 64 Mo par opération ; 8 requêtes à la fois par pod ; DuckDB limité à 1 Go par pod, cache disque local |
| l'application n'est jamais ralentie | aucune connexion au PostgreSQL principal ; pods sur le groupe de nœuds `analytique` (obligatoire sur GKE) |
| pods jetables | aucun volume persistant : un pod redémarré se resynchronise en quelques secondes |

## Mesuré en CI (kind)

`tests/integration/tester_analytique.py` (job « postgres ») : une table gold créée par un autre moteur apparaît d'elle-même
avec ses données ; une ligne ajoutée est lue à la requête suivante ; une écriture de l'application est lisible dans le pool
en moins de 2 minutes ; écriture, création de table et contournement de la lecture seule sont refusés. Les durées
mesurées sont dans le journal du workflow Integration.

## Exploitation

```bash
kubectl -n spark logs deploy/lake-analytique -c synchro --tail=20     # « synchro : N table(s)… »
kubectl -n spark get hpa lake-analytique                               # pods voulus selon le CPU
kubectl -n spark exec deploy/lake-analytique -c postgres -- /usr/lib/postgresql/18/bin/psql \
  -h /var/run/postgresql -U postgres -d lake -c "SELECT count(*) FROM pg_stat_activity WHERE usename = 'analyste'"
```

## Limites par rôle (toutes les bases)

| Rôle | Base | `statement_timeout` | `work_mem` |
|------|------|---------------------|------------|
| `analyste` | pool analytique | 120 s | 64 Mo |
| `lake_app` (lecture de l'application) | principal | 2 s | 16 Mo |
| `lake_critique` (actions critiques) | principal | 5 s | 8 Mo |
| `lake_writer` (writers Kafka) | principal | 60 s | 32 Mo |

DuckDB (`pgduck_server`) : mémoire bornée (`--memory_limit`, 1 Go) et cache disque local (`--cache_dir`) sur le principal
et dans chaque pod du pool : jamais les 80 % de la mémoire du nœud qu'il prendrait par défaut.
