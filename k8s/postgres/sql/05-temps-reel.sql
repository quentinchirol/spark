-- Accès temps réel exceptionnel (US-16) : un responsable métier lit les vues vue.* « à la seconde » (écritures de
-- l'application visibles aussitôt), sur une RÉPLIQUE du PostgreSQL principal, jamais sur le primaire.
-- Idempotent. docs/temps-reel.md
--
--   rôle de groupe temps_reel : lecture de vue.* (et des tables Iceberg app.* qu'elles lisent), rien d'autre ;
--   utilisateurs temps_reel_<nom> : créés et retirés par scripts/temps-reel.py, pour une durée limitée ;
--   jamais sur le primaire, trois protections :
--     1. on se connecte par le Service des répliques (lake-principal-repl) ;
--     2. une connexion d'un membre au primaire est refusée (garde-temps-reel.psql, dans chaque base) ;
--     3. pg_cron, qui ne tourne que sur le primaire, coupe toutes les 30 s les sessions de membres qui s'y
--        trouveraient : session ouverte sur une réplique promue primaire par une bascule (Patroni).

\connect lake
\ir garde-temps-reel.psql
GRANT USAGE ON SCHEMA vue TO temps_reel;
GRANT SELECT ON ALL TABLES IN SCHEMA vue TO temps_reel;
ALTER DEFAULT PRIVILEGES IN SCHEMA vue GRANT SELECT ON TABLES TO temps_reel;
-- pg_lake lit les tables Iceberg app.* avec les droits de l'utilisateur qui lit la vue (voir 03-roles.sql)
GRANT USAGE ON SCHEMA app TO temps_reel;
GRANT SELECT ON ALL TABLES IN SCHEMA app TO temps_reel;
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO temps_reel;

-- les autres bases où l'on peut se connecter ; template1 : toute base créée plus tard reçoit la garde
\connect template1
\ir garde-temps-reel.psql
\connect postgres
\ir garde-temps-reel.psql

-- 3. sessions de membres sur le primaire coupées (pg_cron ne lance rien sur une réplique). Par oid : aucune erreur
-- si le rôle de groupe n'existe pas. Une tâche du même nom est remplacée : idempotent.
SELECT cron.schedule_in_database('temps-reel-hors-primaire', '30 seconds', $tache$
    SELECT count(pg_terminate_backend(a.pid))
    FROM pg_stat_activity a JOIN pg_roles r ON r.oid = a.usesysid
    WHERE NOT r.rolsuper
      AND pg_has_role(r.oid, (SELECT oid FROM pg_roles WHERE rolname = 'temps_reel'), 'MEMBER')
$tache$, 'postgres');
