# Secrets locaux : jamais versionnés

Ce dossier contient les clés et tokens de **votre** installation. Il est exclu de Git (`.gitignore`)
et la CI échoue si un fichier y est versionné.

```bash
scripts/generer-secrets.sh              # crée les fichiers manquants
scripts/generer-secrets.sh --rotation   # régénère tout (après une fuite, ou périodiquement)
```

| Fichier         | Contenu |
|-----------------|---------|
| `tokens`        | `utilisateur=token` (Spark Connect et Trino) : alice, bob, pipeline |
| `pipeline.env`  | `SPARK_CONNECT_TOKEN` du traitement planifié (= ligne `pipeline=` de `tokens`) |
| `s3.env`        | clé S3 lecture + écriture (Spark, catalogue) |
| `s3-trino.env`  | clé S3 en lecture seule (Trino) |
| `trino.env`     | secret interne de Trino |
| `trino-tls.pem` | clé privée + certificat HTTPS de Trino ; `trino-cert.pem` : certificat seul, pour les clients |
