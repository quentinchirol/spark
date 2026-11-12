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

### Python / notebook

```python
import pandas as pd, psycopg
with psycopg.connect(host="localhost", dbname="lake", user="analyste", password="…") as conn:
    df = pd.read_sql("SELECT * FROM gold.ventes_par_jour", conn)
```

## Garanties

| Garantie | Comment |
|----------|---------|
| lecture seule | rôle `analyste` sans aucun droit d'écriture, transactions en lecture seule par défaut, tables rattachées `read_only`, principal Polaris et clé S3 en lecture seule |
| requêtes bornées | `statement_timeout` de 120 s, `work_mem` de 64 Mo par opération ; DuckDB limité à 1 Go par pod |
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
kubectl -n spark scale deploy/lake-analytique --replicas=4             # plus d'analystes (autoscaling : US-14)
```
