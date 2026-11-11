-- Fusion de la table chaude vers Iceberg chaque minute (pg_cron, base lake). cron.schedule remplace
-- une tâche existante du même nom : idempotent.
SELECT cron.schedule('fusion-iceberg', '* * * * *', 'CALL lake.fusionner_tout()');
