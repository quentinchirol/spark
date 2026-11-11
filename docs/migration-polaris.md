# Migrer les tables existantes vers MinIO + Polaris

Avant la US-4, les tables Iceberg étaient dans SeaweedFS avec un catalogue Iceberg REST (SQLite).
Elles sont maintenant dans MinIO, déclarées dans Polaris (catalogue `lake`). Le nom des tables ne change pas
(`lake.<schéma>.<table>`).

> **Statut** : la voie A est la voie recommandée. La voie B n'a pas été exécutée de bout en bout ; la tester
> sur une copie avant de supprimer l'ancien stockage.

## Voie A : tout régénérer (recommandée)

Toutes les tables actuelles se recalculent :

| Tables | Régénération |
|--------|--------------|
| `lake.bench_<n>.*` (ventes, clients, produits) | `benchmark.py` écrit les tables Iceberg absentes (`--sources iceberg --tailles <n>`) |
| `lake.dashboard.*` | `kubectl -n spark create job --from=cronjob/pipeline-dashboard pipeline-manuel` |

```bash
scripts/generer-secrets.sh          # nouveaux secrets MinIO / Polaris si absents
kubectl apply -k k8s                # MinIO, Polaris, Spark, Trino
kubectl -n spark wait --for=condition=complete job/minio-init job/polaris-bootstrap job/polaris-init --timeout=10m
(cd python && python benchmark.py --sources iceberg --tailles 1000000 --repetitions 1)   # recrée lake.bench_1000000
kubectl -n spark create job --from=cronjob/pipeline-dashboard pipeline-manuel   # recrée lake.dashboard.*
```

Vérifier : `spark.table("lake.dashboard.<table>").count()` (Spark) et la même requête dans Trino donnent le même nombre.

## Voie B : copier des tables qui ne se régénèrent pas

Les chemins des fichiers dans les métadonnées Iceberg sont absolus (`s3://warehouse/...`). Le bucket garde le même
nom dans MinIO : il suffit de **copier les objets** puis d'**enregistrer** chaque table dans Polaris.

1. Garder l'ancien stockage lisible (ancienne version du dépôt, ou `kubectl port-forward` vers SeaweedFS).
2. Copier le bucket avec `mc` (alias `ancien` = SeaweedFS, alias `nouveau` = MinIO, compte `k8s/secrets/minio-root.env`) :

   ```bash
   mc mirror --preserve ancien/warehouse nouveau/warehouse
   ```

3. Pour chaque table, retrouver sa dernière version de métadonnées (le fichier `v*.metadata.json` ou
   `*.metadata.json` le plus récent du dossier `metadata/` de la table), puis, depuis Spark Connect :

   ```sql
   CALL lake.system.register_table(
     table => 'dashboard.ventes_par_mois',
     metadata_file => 's3://warehouse/dashboard/ventes_par_mois/metadata/00012-….metadata.json');
   ```

4. Vérifier que le nombre de lignes est identique dans l'ancien et le nouveau catalogue avant de supprimer
   l'ancien stockage.

Une table enregistrée de cette façon est ensuite modifiée normalement : les nouvelles versions s'écrivent dans MinIO.
