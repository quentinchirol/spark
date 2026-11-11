-- Test du modèle chaud + froid (k8s/postgres/sql/02-modele.sql) sur un PostgreSQL ordinaire :
-- tables « heap » à la place d'Iceberg (la logique est la même). Échoue au premier ASSERT faux.
--   psql -v ON_ERROR_STOP=1 -f tests/sql/modele.sql
\set ON_ERROR_STOP 1
\set QUIET 1
\i k8s/postgres/sql/02-modele.sql
\i k8s/postgres/sql/02-modele.sql
SELECT lake.creer_table('commandes', 'client text, montant numeric(12,2), statut text', 'heap');
SELECT lake.creer_table('commandes', 'client text, montant numeric(12,2), statut text', 'heap');

CREATE FUNCTION pg_temp.msg(id text, op text, version int, donnees jsonb) RETURNS jsonb
LANGUAGE sql AS $$ SELECT jsonb_build_array(jsonb_build_object(
    'id', id, 'op', op, 'table', 'commandes', 'date_creation', '2026-11-11', 'version', version, 'donnees', donnees)) $$;
\set A '11111111-1111-4111-8111-111111111111'
\set B '22222222-2222-4222-8222-222222222222'

-- 1. une écriture acceptée est visible immédiatement
SELECT lake.ecrire_lot('commandes', pg_temp.msg(:'A', 'INSERT', 1, '{"client":"alice","montant":10,"statut":"ouverte"}'));
DO $$ BEGIN ASSERT (SELECT statut FROM vue.commandes WHERE client = 'alice') = 'ouverte', '1. écriture invisible'; END $$;

-- 2. dans un lot, la version la plus récente gagne ; une version plus ancienne est ignorée
SELECT lake.ecrire_lot('commandes', pg_temp.msg(:'A', 'UPDATE', 3, '{"client":"alice","montant":30,"statut":"payee"}')
                                 || pg_temp.msg(:'A', 'UPDATE', 2, '{"client":"alice","montant":20,"statut":"ouverte"}')
                                 || pg_temp.msg(:'B', 'INSERT', 4, '{"client":"bob","montant":5,"statut":"ouverte"}'));
DO $$ BEGIN ASSERT (SELECT lake.ecrire_lot('commandes', pg_temp.msg('11111111-1111-4111-8111-111111111111', 'UPDATE', 2,
    '{"client":"alice","montant":999,"statut":"ancienne"}'))) = 0, '2. version ancienne appliquée'; END $$;
DO $$ BEGIN
    ASSERT (SELECT montant FROM vue.commandes WHERE client = 'alice') = 30, '2. mauvaise version gagnante';
    ASSERT (SELECT count(*) FROM vue.commandes) = 2, '2. doublon visible';
END $$;

-- 3. fusion : tout passe dans le froid, la vue ne change pas
DO $$ BEGIN
    ASSERT lake.fusionner('commandes') = 2, '3. fusion incomplète';
    ASSERT (SELECT count(*) FROM chaud.commandes) = 0, '3. chaud non vidé';
    ASSERT (SELECT count(*) FROM app.commandes) = 2, '3. froid incomplet';
    ASSERT (SELECT montant FROM vue.commandes WHERE client = 'alice') = 30, '3. vue modifiée par la fusion';
END $$;

-- 4. ligne déjà froide modifiée : visible tout de suite, appliquée à la fusion suivante
SELECT lake.ecrire_lot('commandes', pg_temp.msg(:'B', 'UPDATE', 5, '{"client":"bob","montant":50,"statut":"payee"}'));
DO $$ BEGIN
    ASSERT (SELECT montant FROM vue.commandes WHERE client = 'bob') = 50, '4. modification invisible avant fusion';
    PERFORM lake.fusionner('commandes');
    ASSERT (SELECT montant FROM app.commandes WHERE client = 'bob') = 50, '4. modification non fusionnée';
    ASSERT (SELECT count(*) FROM app.commandes WHERE client = 'bob') = 1, '4. doublon dans le froid';
END $$;

-- 5. suppression : invisible avant et après fusion ; un rejeu plus ancien ne la ressuscite pas
SELECT lake.ecrire_lot('commandes', pg_temp.msg(:'A', 'DELETE', 6, '{}'));
DO $$ BEGIN ASSERT NOT EXISTS (SELECT 1 FROM vue.commandes WHERE client = 'alice'), '5. supprimée visible'; END $$;
SELECT lake.fusionner('commandes');
SELECT lake.ecrire_lot('commandes', pg_temp.msg(:'A', 'UPDATE', 2, '{"client":"alice","montant":1,"statut":"zombie"}'));
DO $$ BEGIN
    ASSERT NOT EXISTS (SELECT 1 FROM vue.commandes WHERE statut = 'zombie'), '5. rejeu ancien visible';
    PERFORM lake.fusionner('commandes');
    ASSERT NOT EXISTS (SELECT 1 FROM vue.commandes WHERE statut = 'zombie'), '5. rejeu ancien ressuscité par la fusion';
END $$;

-- 6. historique : chaque écriture reçue, chaude ou fusionnée. (id, version) identifie un message Kafka : les
--    « versions anciennes » reçues plus haut (2 deux fois) sont des rejeux du message de version 2, comptés une fois.
--    Pour alice : versions 1, 2, 3, 6.
DO $$ BEGIN
    ASSERT (SELECT count(*) FROM vue.commandes_historique WHERE id = '11111111-1111-4111-8111-111111111111') = 4,
        '6. historique incomplet';
    ASSERT (SELECT count(*) FROM chaud.commandes_historique) = 0, '6. historique non fusionné';
END $$;

-- 7. lot rejoué par Kafka (writer arrêté entre l'écriture et le commit de l'offset) : rien n'est enregistré deux fois
\set C '33333333-3333-4333-8333-333333333333'
SELECT lake.ecrire_lot('commandes', pg_temp.msg(:'C', 'INSERT', 10, '{"client":"chloe","montant":7,"statut":"ouverte"}'));
SELECT lake.ecrire_lot('commandes', pg_temp.msg(:'C', 'INSERT', 10, '{"client":"chloe","montant":7,"statut":"ouverte"}'));
DO $$ BEGIN
    ASSERT (SELECT count(*) FROM vue.commandes WHERE client = 'chloe') = 1, '7. ligne en double après rejeu';
    ASSERT (SELECT count(*) FROM vue.commandes_historique WHERE id = '33333333-3333-4333-8333-333333333333') = 1,
        '7. historique en double après rejeu';
    PERFORM lake.fusionner('commandes');
END $$;
SELECT lake.ecrire_lot('commandes', pg_temp.msg(:'C', 'INSERT', 10, '{"client":"chloe","montant":7,"statut":"ouverte"}'));
DO $$ BEGIN
    ASSERT (SELECT count(*) FROM vue.commandes WHERE client = 'chloe') = 1, '7. ligne en double après rejeu post-fusion';
    ASSERT (SELECT count(*) FROM vue.commandes_historique WHERE id = '33333333-3333-4333-8333-333333333333') = 1,
        '7. historique en double après rejeu post-fusion';
END $$;

-- 8. fusion de toutes les tables (procédure lancée par pg_cron)
CALL lake.fusionner_tout();
\echo 'Modèle chaud + froid : tous les contrôles passent'
