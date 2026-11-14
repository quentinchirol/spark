-- Test de la garde de l'accès temps réel (k8s/postgres/sql/garde-temps-reel.psql) sur un PostgreSQL ordinaire,
-- qui est un PRIMAIRE : un membre de temps_reel y est refusé à la connexion, tout autre utilisateur passe.
-- Le cas de la réplique (connexion acceptée, lecture des vues) est vérifié sur kind : tests/integration/tester_temps_reel.py
--   psql -v ON_ERROR_STOP=1 -f tests/sql/temps-reel.sql
\set ON_ERROR_STOP 1
\set QUIET 1
\i k8s/postgres/sql/garde-temps-reel.psql
\i k8s/postgres/sql/garde-temps-reel.psql
CREATE EXTENSION IF NOT EXISTS dblink;

DROP ROLE IF EXISTS essai_temps_reel, essai_temps_reel_indirect, essai_sans_acces, essai_admin, essai_equipe;
CREATE ROLE essai_temps_reel LOGIN IN ROLE temps_reel;
CREATE ROLE essai_equipe NOLOGIN IN ROLE temps_reel;
CREATE ROLE essai_temps_reel_indirect LOGIN NOINHERIT IN ROLE essai_equipe;
CREATE ROLE essai_sans_acces LOGIN;
CREATE ROLE essai_admin LOGIN SUPERUSER;

-- connexion d'un utilisateur à cette base par le socket local (sans mot de passe) : 'acceptée' ou le motif du refus
CREATE FUNCTION pg_temp.connexion(utilisateur text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    detail text;
BEGIN
    PERFORM dblink_connect('essai', format('host=%s port=%s dbname=%s user=%s',
        split_part(current_setting('unix_socket_directories'), ',', 1), current_setting('port'),
        current_database(), utilisateur));
    PERFORM dblink_disconnect('essai');
    RETURN 'acceptée';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS detail = PG_EXCEPTION_DETAIL;
    RETURN coalesce(detail, SQLERRM);
END $$;

DO $$
DECLARE
    resultat text;
BEGIN
    -- 1. membre de temps_reel : refusé sur le primaire, avec la marche à suivre
    resultat := pg_temp.connexion('essai_temps_reel');
    ASSERT resultat LIKE '%connexion refusée sur le primaire%', '1. membre accepté sur le primaire : ' || resultat;
    -- 2. membre par un rôle intermédiaire, même sans héritage des droits : refusé aussi
    resultat := pg_temp.connexion('essai_temps_reel_indirect');
    ASSERT resultat LIKE '%connexion refusée sur le primaire%', '2. membre indirect accepté : ' || resultat;
    -- 3. utilisateur sans accès temps réel, superutilisateur : acceptés (l'application, les writers, Patroni…)
    ASSERT pg_temp.connexion('essai_sans_acces') = 'acceptée', '3. utilisateur ordinaire refusé';
    ASSERT pg_temp.connexion('essai_admin') = 'acceptée', '3. superutilisateur refusé';
END $$;

-- 4. accès retiré : de nouveau accepté sur cette base (la lecture des données reste refusée, faute de droits)
REVOKE temps_reel FROM essai_temps_reel;
DO $$ BEGIN
    ASSERT pg_temp.connexion('essai_temps_reel') = 'acceptée', '4. ancien membre toujours refusé';
END $$;

-- 5. rôle de groupe absent : la garde ne bloque personne (aucune erreur qui fermerait la base à tous)
DROP ROLE essai_temps_reel_indirect, essai_equipe;
DROP ROLE temps_reel;
DO $$ BEGIN
    ASSERT pg_temp.connexion('essai_sans_acces') = 'acceptée', '5. connexions bloquées sans le rôle temps_reel';
END $$;
\i k8s/postgres/sql/garde-temps-reel.psql

DROP ROLE essai_temps_reel, essai_sans_acces, essai_admin;
\echo 'garde temps réel : tous les contrôles passent'
