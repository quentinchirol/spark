-- Rôle de l'application (FastAPI) : lit les vues, jamais les tables chaudes ou froides directement.
-- Une requête de l'application ne doit jamais occuper le serveur : 2 s maximum (la fusion n'est pas concernée).
ALTER ROLE lake_app SET statement_timeout = '2s';
-- mémoire par opération (tri, hachage) bornée pour chaque rôle : une requête ne peut pas épuiser la mémoire du serveur
ALTER ROLE lake_app SET work_mem = '16MB';
GRANT USAGE ON SCHEMA vue TO lake_app;
GRANT SELECT ON ALL TABLES IN SCHEMA vue TO lake_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA vue GRANT SELECT ON TABLES TO lake_app;

-- Les fonctions sont exécutables par tous (PUBLIC) par défaut : seuls les rôles prévus y ont droit.
REVOKE ALL ON FUNCTION lake.ecrire_lot(text, jsonb), lake.ecrire_critique(text, jsonb) FROM PUBLIC;

-- Rôle du chemin critique (passerelle, POST /ecritures/critiques) : uniquement lake.ecrire_critique
-- (SECURITY DEFINER), aucun droit direct sur les tables.
ALTER ROLE lake_critique SET statement_timeout = '5s';
ALTER ROLE lake_critique SET work_mem = '8MB';
GRANT USAGE ON SCHEMA lake TO lake_critique;
GRANT EXECUTE ON FUNCTION lake.ecrire_critique(text, jsonb) TO lake_critique;

-- Rôle des writers (k8s/ecrivain) : écrit les tables chaudes via lake.ecrire_lot, rien d'autre.
-- Les tables chaudes créées plus tard par lake.creer_table (en tant que postgres) reçoivent les mêmes droits.
ALTER ROLE lake_writer SET statement_timeout = '60s';
ALTER ROLE lake_writer SET work_mem = '32MB';
GRANT USAGE ON SCHEMA lake, chaud TO lake_writer;
GRANT SELECT ON lake.tables TO lake_writer;
GRANT EXECUTE ON FUNCTION lake.ecrire_lot(text, jsonb) TO lake_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA chaud TO lake_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA chaud GRANT SELECT, INSERT, UPDATE ON TABLES TO lake_writer;
