-- Rôle de l'application (FastAPI) : lit les vues, jamais les tables chaudes ou froides directement.
-- Une requête de l'application ne doit jamais occuper le serveur : 2 s maximum (la fusion n'est pas concernée).
ALTER ROLE lake_app SET statement_timeout = '2s';
GRANT USAGE ON SCHEMA vue TO lake_app;
GRANT SELECT ON ALL TABLES IN SCHEMA vue TO lake_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA vue GRANT SELECT ON TABLES TO lake_app;

-- Rôle des writers (k8s/ecrivain) : écrit les tables chaudes via lake.ecrire_lot, rien d'autre.
-- Les tables chaudes créées plus tard par lake.creer_table (en tant que postgres) reçoivent les mêmes droits.
ALTER ROLE lake_writer SET statement_timeout = '60s';
GRANT USAGE ON SCHEMA lake, chaud TO lake_writer;
GRANT SELECT ON lake.tables TO lake_writer;
GRANT EXECUTE ON FUNCTION lake.ecrire_lot(text, jsonb) TO lake_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA chaud TO lake_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA chaud GRANT SELECT, INSERT, UPDATE ON TABLES TO lake_writer;
