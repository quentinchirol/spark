-- Exécuté une seule fois, à la création de la base polaris (dossier docker-entrypoint-initdb.d).
-- Polaris range ses tables dans le schéma polaris_schema : il doit exister et être le schéma courant,
-- sinon le bootstrap échoue avec « no schema has been selected to create in ».
CREATE SCHEMA IF NOT EXISTS polaris_schema AUTHORIZATION polaris;
ALTER ROLE polaris SET search_path TO polaris_schema, public;
