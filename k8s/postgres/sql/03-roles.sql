-- Rôle de l'application (FastAPI) : lit les vues, jamais les tables chaudes ou froides directement.
-- Une requête de l'application ne doit jamais occuper le serveur : 2 s maximum (la fusion n'est pas concernée).
ALTER ROLE lake_app SET statement_timeout = '2s';
GRANT USAGE ON SCHEMA vue TO lake_app;
GRANT SELECT ON ALL TABLES IN SCHEMA vue TO lake_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA vue GRANT SELECT ON TABLES TO lake_app;
