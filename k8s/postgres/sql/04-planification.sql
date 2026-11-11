-- Fusion de la table chaude vers Iceberg chaque minute, dans la base lake.
-- pg_cron est installé par Spilo dans la base postgres (cron.database_name par défaut) : la tâche y est
-- déclarée avec schedule_in_database. Une tâche du même nom est remplacée : idempotent.
\connect postgres
CREATE EXTENSION IF NOT EXISTS pg_cron;
SELECT cron.schedule_in_database('fusion-iceberg', '* * * * *', 'CALL lake.fusionner_tout()', 'lake');
