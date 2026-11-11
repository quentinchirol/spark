# Sauvegardes et restauration du PostgreSQL principal

Le PostgreSQL principal (`lake-principal`) est sauvegardé **en continu** par WAL-G, vers un stockage objet
**hors du cluster Kubernetes** : la perte du cluster entier ne fait pas perdre les sauvegardes.

| Quoi | Quand | Conservation |
|------|-------|--------------|
| Chaque segment WAL (toutes les écritures) | en continu, dès qu'il est rempli | jusqu'à la plus ancienne sauvegarde de base conservée |
| Sauvegarde de base (copie complète) | chaque nuit à 01:00 (`BACKUP_SCHEDULE`) | 7 (`BACKUP_NUM_TO_RETAIN`) |

On peut donc restaurer **à n'importe quel instant** des 7 derniers jours (à la seconde).

## Où vont les sauvegardes

| Environnement | Stockage | Mise en place |
|---------------|----------|---------------|
| kind, Codespaces, CI | conteneur MinIO `sauvegardes` sur le réseau Docker de kind, **hors de Kubernetes**, bucket versionné | `scripts/dev/sauvegardes.sh` (lancé par `scripts/dev/demarrer-kind.sh`) |
| GKE | bucket GCS `<projet>-lake-sauvegardes` (service distinct du cluster), versionné, compte de service limité à ce bucket | `PROJET=… infra/gke/creer-sauvegardes.sh` puis `infra/gke/deployer.sh` |

Destination et clé : `k8s/secrets/sauvegardes.env` → Secret `sauvegardes-postgres` (jamais versionné).

## Vérifier

```bash
POD=$(kubectl -n spark get pods -l cluster-name=lake-principal,spilo-role=master -o name)
kubectl -n spark exec $POD -c postgres -- envdir /run/etc/wal-e.d/env wal-g backup-list     # sauvegardes de base
kubectl -n spark exec $POD -c postgres -- psql -U postgres -c "SELECT * FROM pg_stat_archiver" # WAL archivés / en échec
# sauvegarde de base immédiate
kubectl -n spark exec $POD -c postgres -- envdir /run/etc/wal-e.d/env /scripts/postgres_backup.sh /home/postgres/pgdata/pgroot/data
```

## Restaurer à un instant donné (cluster neuf)

La restauration crée un **nouveau** cluster à partir des sauvegardes ; l'ancien n'est pas touché (il peut avoir disparu).

1. Noter l'`uid` du cluster d'origine (`kubectl -n spark get postgresql lake-principal -o jsonpath='{.metadata.uid}'`)
   et le préfixe WAL-G (`cat /run/etc/wal-e.d/env/WALG_S3_PREFIX` dans un pod, ou : `s3://<bucket>/spilo/lake-principal/<uid>/wal/18`).
   **À conserver hors du cluster** (ex. dans la documentation d'exploitation) : sans eux, il faut lister le bucket.
2. Créer le cluster restauré : copie du manifeste `k8s/postgres/principal.yaml` avec un autre nom et un bloc `clone` :

   ```yaml
   metadata:
     name: lake-restaure
   spec:
     # … même spec que lake-principal …
     clone:
       cluster: lake-principal
       uid: "<uid du cluster d'origine>"
       timestamp: "2026-11-11T15:25:45+00:00"     # instant visé, fuseau obligatoire
       s3_wal_path: "s3://<bucket>/spilo/lake-principal/<uid>/wal/18"
       s3_endpoint: "<AWS_ENDPOINT de sauvegardes.env>"
       s3_force_path_style: true
   ```

   La clé de lecture vient des variables `CLONE_AWS_*` du Secret `sauvegardes-postgres` (déjà dans la spec).
3. Attendre `kubectl -n spark get postgresql lake-restaure` → `Running`, vérifier les données, puis faire pointer
   l'application sur le nouveau cluster (ou le renommer en recréant `lake-principal` par clonage de `lake-restaure`).

Cette procédure est exécutée à chaque CI par `tests/integration/tester_restauration.py` (cluster supprimé, sauvegardes
vérifiées depuis l'extérieur de Kubernetes, restauration à un instant T entre deux écritures).

## Ce qui n'est PAS couvert ici

- **Les données Iceberg dans MinIO** (`app.*`, bronze / silver / gold) : elles ne sont pas dans PostgreSQL. Leur
  protection (réplication du bucket MinIO vers un stockage externe) reste à faire.
- **La base du catalogue Polaris** (`polaris-db`, 1 pod) : pas encore sauvegardée. En cas de perte, les tables peuvent
  être réenregistrées depuis leurs fichiers de métadonnées (`register_table`, voir `docs/migration-polaris.md`).
