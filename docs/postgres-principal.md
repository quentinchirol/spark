# PostgreSQL principal : écritures de l'application, chaud + froid

Le PostgreSQL principal (`lake-principal`, base `lake`) reçoit les écritures de l'application et les rend lisibles
tout de suite. Chaque minute, il les fusionne dans des tables **Iceberg** (`app.*`), déclarées dans Polaris et lues
par Spark, Trino et le pool analytique, sans copie.

- 3 instances, opérateur Zalando (Patroni), réplication **synchrone** : [décision 001](decisions/001-postgres-principal-zalando.md)
- extension **pg_lake** (tables Iceberg dans PostgreSQL) + `pgduck_server` en sidecar
- modèle : `k8s/postgres/sql/02-modele.sql` ; tests : `tests/sql/modele.sql` (CI) et `tests/integration/tester_postgres.py` (kind)

## Pour chaque table de l'application

```sql
SELECT lake.creer_table('commandes', 'client text, montant numeric(12,2), statut text');
```

| Objet | Rôle |
|-------|------|
| `chaud.commandes` | dernières écritures, une ligne par `id` (jusqu'à la fusion) |
| `app.commandes` | table **Iceberg** (Polaris `lake.app.commandes`) : état fusionné ; une suppression y devient `_supprime = true` |
| `vue.commandes` | **ce que lit l'application** : dernier état de chaque ligne, chaud ou froid, sans les lignes supprimées |
| `chaud.commandes_historique` → `app.commandes_historique` | chaque écriture reçue |
| `vue.commandes_historique` | tous les changements d'une ligne |

## Écrire (writer Kafka, US-6)

```sql
SELECT lake.ecrire_lot('commandes', '[{"id": "…", "op": "UPDATE", "table": "commandes",
  "date_creation": "2026-11-11", "version": 42, "donnees": {"client": "alice", "montant": 30, "statut": "payee"}}]');
```

- un message = l'état **complet** de la ligne ([contrat d'écriture](contrat-ecriture.md)) ;
- `version` = offset Kafka : dans un lot, la plus récente gagne ; une version plus ancienne que celle déjà enregistrée
  est ignorée (rejeu, désordre) ; une ligne supprimée ne peut pas être ressuscitée par un rejeu ancien ;
- tout message reçu va dans l'historique, même s'il est ignoré.

## Lire (application)

Le rôle `lake_app` ne lit que les vues `vue.*`, avec un `statement_timeout` de 2 s :

```sql
SELECT * FROM vue.commandes WHERE id = '…';
```

## Fusion vers Iceberg

`pg_cron` lance `CALL lake.fusionner_tout()` chaque minute : les lignes chaudes reçues avant le début de la fusion
passent dans Iceberg, une ligne réécrite pendant la fusion reste chaude. Lancer à la main :

```sql
SELECT lake.fusionner('commandes');
```

## Limites constatées

- **Mémoire de DuckDB** (`PGDUCK_MEMORY_LIMIT`, 1 Go) : à 256 Mo, la fusion vers Iceberg manque de mémoire et
  `pgduck_server` s'arrête entièrement (toutes les requêtes pg_lake du pod échouent jusqu'à son redémarrage).
  À dimensionner avec le volume de chaque fusion (US-15).

## Accès

```bash
# primaire (écriture)
kubectl -n spark exec -it $(kubectl -n spark get pods -l cluster-name=lake-principal,spilo-role=master -o name) \
  -c postgres -- psql -U postgres -d lake
# état du cluster Patroni (primaire, répliques synchrones)
kubectl -n spark exec -it lake-principal-0 -c postgres -- patronictl list
```
