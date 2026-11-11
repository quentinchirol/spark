-- Extensions et accès au catalogue Polaris. Idempotent.
-- Variables psql : polaris_client_id, polaris_client_secret (Secret polaris-postgres, principal « postgres »).
CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- tables Iceberg déclarées dans Polaris : le catalogue REST porte le nom de la base (lake)
CREATE SERVER IF NOT EXISTS polaris TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
    OPTIONS (rest_endpoint 'http://polaris:8181/api/catalog', rest_auth_type 'oauth2');
CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER polaris
    OPTIONS (client_id :'polaris_client_id', client_secret :'polaris_client_secret');
-- après une rotation des secrets
ALTER USER MAPPING FOR PUBLIC SERVER polaris
    OPTIONS (SET client_id :'polaris_client_id', SET client_secret :'polaris_client_secret');
