# Maintenance des tables Iceberg

Chaque écriture dans une table Iceberg ajoute des fichiers et un snapshot. Sans entretien, les petits fichiers et
les métadonnées s'accumulent et les requêtes ralentissent au fil des mois. Deux mécanismes, selon le moteur qui écrit
la table, ne touchent jamais aux mêmes tables :

| Tables | Écrites par | Entretenues par | Quand |
|--------|-------------|-----------------|-------|
| `bronze.*`, `silver.*`, `gold.*`… (tous les namespaces sauf `app`) | Spark (traitements Dagster) | job Dagster `maintenance` (`k8s/traitements/maintenance.py`) | chaque nuit à 3 h 15 (heure de Paris) |
| `app.*` | PostgreSQL principal (pg_lake) | pg_lake lui-même (autovacuum) | toutes les 10 min |

## Tables écrites par Spark : job `maintenance`

Pour chaque table, en commits Iceberg séparés :

1. **compaction** : les petits fichiers de données sont regroupés (`rewrite_data_files`, dès 2 fichiers dans un
   groupe, fichiers cibles de 512 Mo), puis les manifests (`rewrite_manifests`) ;
2. **snapshots** : seuls les **10 derniers** sont gardés (`expire_snapshots`, `SNAPSHOTS_GARDES`) ; les fichiers
   qu'eux seuls référençaient sont supprimés ;
3. **fichiers orphelins** (laissés par une écriture échouée, jamais référencés) : supprimés s'ils ont plus de
   **72 h** (`ORPHELINS_APRES_H`), pour ne jamais toucher aux fichiers d'une écriture en cours.

Une écriture concurrente (chargement d'événements, traitement en retard) n'est jamais perdue : au pire, un groupe de
fichiers n'est pas compacté et le sera la nuit suivante. Une table en échec n'empêche pas les autres ; le run échoue
à la fin s'il y en a eu, et Dagster le relance (3 essais).

```bash
kubectl -n spark port-forward svc/dagster-webserver 3000:3000   # Jobs → maintenance → Launch run
```

Remarque : les 10 snapshots gardés limitent le retour dans le temps (`VERSION AS OF`) aux 10 dernières écritures de
chaque table.

## Tables `app.*` : pg_lake s'en charge (réponse à LH-26)

**Oui, pg_lake entretient lui-même ses tables Iceberg** (version 3.6, `k8s/postgres/principal.yaml`) : un processus
de fond lance `VACUUM` sur chaque table Iceberg toutes les 10 min (`pg_lake_iceberg.autovacuum_naptime`). Il compacte
les petits fichiers, expire les snapshots de plus de 30 min (`pg_lake_iceberg.max_snapshot_age`) et supprime les
fichiers qui ne sont plus référencés depuis 10 jours (`pg_lake_engine.orphaned_file_retention_period`). D'après la
documentation de pg_lake, ce `VACUUM` valide chaque étape séparément et ne bloque ni les lectures ni les écritures :
la fusion planifiée chaque minute continue pendant ce temps.

La maintenance Spark **ne touche donc jamais** aux tables `app.*` (`NAMESPACES_EXCLUS=app`) : deux moteurs qui
compactent ou suppriment des fichiers de la même table risqueraient d'entrer en conflit, ou de supprimer un fichier
que pg_lake garde volontairement 10 jours.

```sql
-- à la main, sur le primaire (base lake)
VACUUM (ICEBERG, VERBOSE);
```

## Vérifié en CI

- `tests/integration/tester_orchestration.py` (job « orchestration ») : 15 petits fichiers compactés en 1, sans perte
  de ligne ; 10 derniers snapshots gardés (sur 15 écritures + la maintenance) ; fichier orphelin supprimé (délai de 72 h ramené à 0 en CI) ; une table du
  namespace `app` n'est pas touchée ; tables « ventes » inchangées.
- `tests/integration/tester_postgres.py` (job « postgres ») : après 8 fusions, l'autovacuum de pg_lake (chaque minute
  en CI) compacte `app.commandes` sans perte de ligne, pendant que la fusion planifiée continue.
