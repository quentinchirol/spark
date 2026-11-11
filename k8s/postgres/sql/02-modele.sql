-- Modèle « chaud + froid » du PostgreSQL principal (US-7). Idempotent : peut être rejoué.
--
-- Pour chaque table <t> de l'application (lake.creer_table('<t>', '<colonnes>')) :
--   chaud.<t>              dernières écritures, 1 ligne par id (UPSERT versionné par l'offset Kafka) ; une
--                          suppression y reste comme ligne « DELETE » jusqu'à la fusion ;
--   app.<t>                table Iceberg (pg_lake, catalogue Polaris) : état fusionné, lisible par Spark, Trino,
--                          le pool analytique ; une suppression y devient une ligne _supprime = true ;
--   chaud.<t>_historique   chaque écriture reçue (append) → app.<t>_historique (Iceberg) à la fusion ;
--   vue.<t>                ce que lit l'application : pour chaque id, la version la plus récente entre chaud et froid,
--                          sans les lignes supprimées. Une écriture acceptée y est visible immédiatement ;
--   vue.<t>_historique     tous les changements d'une ligne, chauds et fusionnés.
-- lake.fusionner_tout(), lancé chaque minute par pg_cron, déplace le chaud vers Iceberg.
--
-- Règles (docs/contrat-ecriture.md) : l'id vient du front (UUID), chaque message porte l'état COMPLET de la ligne,
-- jamais un incrément ; la version (offset Kafka) départage les messages : une version plus ancienne est ignorée.

CREATE SCHEMA IF NOT EXISTS lake;
CREATE SCHEMA IF NOT EXISTS chaud;
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS vue;

-- registre des tables gérées et de leur méthode de stockage froid
CREATE TABLE IF NOT EXISTS lake.tables (
    nom        text PRIMARY KEY CHECK (nom ~ '^[a-z_][a-z0-9_]{0,50}$'),
    colonnes   text NOT NULL,
    methode    text NOT NULL DEFAULT 'iceberg' CHECK (methode IN ('iceberg', 'heap')),
    creee_le   timestamptz NOT NULL DEFAULT now()
);

-- Crée (si absentes) les tables chaude, froide, d'historique et les vues d'une table de l'application.
--   nom      : nom de la table (même règle que le contrat d'écriture)
--   colonnes : colonnes métier, ex. 'client text, montant numeric(12,2), statut text'
--   methode  : 'iceberg' (production) ou 'heap' (tests sans pg_lake)
CREATE OR REPLACE FUNCTION lake.creer_table(nom text, colonnes text, methode text DEFAULT 'iceberg')
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    stockage text := CASE methode
        WHEN 'iceberg' THEN 'USING iceberg WITH (catalog = ''polaris'', partition_by = ''month(date_creation)'')'
        ELSE '' END;
    stockage_hist text := CASE methode
        WHEN 'iceberg' THEN 'USING iceberg WITH (catalog = ''polaris'', partition_by = ''day(_recu)'')'
        ELSE '' END;
    liste text;
BEGIN
    INSERT INTO lake.tables (nom, colonnes, methode) VALUES (nom, colonnes, methode)
    ON CONFLICT DO NOTHING;

    EXECUTE format($f$
        CREATE TABLE IF NOT EXISTS chaud.%1$I (
            id            uuid PRIMARY KEY,
            date_creation date NOT NULL,
            %2$s,
            _op           text NOT NULL CHECK (_op IN ('INSERT', 'UPDATE', 'DELETE')),
            _version      bigint NOT NULL,
            _recu         timestamptz NOT NULL DEFAULT clock_timestamp()
        )$f$, nom, colonnes);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON chaud.%I (_recu)', nom || '_recu', nom);

    EXECUTE format($f$
        CREATE TABLE IF NOT EXISTS app.%1$I (
            id            uuid NOT NULL,
            date_creation date NOT NULL,
            %2$s,
            _version      bigint NOT NULL,
            _supprime     boolean NOT NULL
        ) %3$s$f$, nom, colonnes, stockage);

    EXECUTE format($f$
        CREATE TABLE IF NOT EXISTS chaud.%1$I (
            id        uuid NOT NULL,
            _op       text NOT NULL,
            _version  bigint NOT NULL,
            _recu     timestamptz NOT NULL DEFAULT clock_timestamp(),
            donnees   jsonb NOT NULL
        )$f$, nom || '_historique');
    EXECUTE format($f$
        CREATE TABLE IF NOT EXISTS app.%1$I (
            id        uuid NOT NULL,
            _op       text NOT NULL,
            _version  bigint NOT NULL,
            _recu     timestamptz NOT NULL,
            donnees   text NOT NULL
        ) %2$s$f$, nom || '_historique', stockage_hist);

    -- colonnes métier, dans l'ordre de la table chaude
    SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum) INTO liste
    FROM pg_attribute a
    WHERE a.attrelid = format('chaud.%I', nom)::regclass AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attname NOT IN ('id', 'date_creation', '_op', '_version', '_recu');

    EXECUTE format($f$
        CREATE OR REPLACE VIEW vue.%1$I AS
        SELECT id, date_creation, %2$s, _version
        FROM (
            SELECT DISTINCT ON (id) *
            FROM (
                SELECT id, date_creation, %2$s, _version, (_op = 'DELETE') AS _supprime FROM chaud.%1$I
                UNION ALL
                SELECT id, date_creation, %2$s, _version, _supprime FROM app.%1$I
            ) toutes
            ORDER BY id, _version DESC
        ) derniere
        WHERE NOT _supprime$f$, nom, liste);

    EXECUTE format($f$
        CREATE OR REPLACE VIEW vue.%1$I AS
        SELECT id, _op, _version, _recu, donnees FROM chaud.%1$I
        UNION ALL
        SELECT id, _op, _version, _recu, donnees::jsonb FROM app.%1$I$f$, nom || '_historique');
END $$;

-- Écrit un lot de messages du contrat d'écriture dans la table chaude et l'historique.
--   lot : tableau JSON de messages {id, op, table, date_creation, donnees, version}
-- Dans un lot, seule la version la plus récente de chaque id est appliquée ; une version plus ancienne que
-- celle déjà en table est ignorée (rejeu Kafka, désordre). Renvoie le nombre de lignes appliquées.
CREATE OR REPLACE FUNCTION lake.ecrire_lot(nom text, lot jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    liste text;
    valeurs text;
    maj text;
    appliquees bigint;
BEGIN
    SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum),
           string_agg('d.' || quote_ident(a.attname), ', ' ORDER BY a.attnum),
           string_agg(format('%1$I = EXCLUDED.%1$I', a.attname), ', ' ORDER BY a.attnum)
      INTO liste, valeurs, maj
    FROM pg_attribute a
    WHERE a.attrelid = format('chaud.%I', nom)::regclass AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attname NOT IN ('id', 'date_creation', '_op', '_version', '_recu');

    -- historique : chaque message reçu, même ceux qui ne gagnent pas
    EXECUTE format($f$
        INSERT INTO chaud.%I (id, _op, _version, donnees)
        SELECT (m->>'id')::uuid, m->>'op', (m->>'version')::bigint, coalesce(m->'donnees', '{}'::jsonb)
        FROM jsonb_array_elements($1) m$f$, nom || '_historique')
    USING lot;

    EXECUTE format($f$
        WITH messages AS (
            SELECT DISTINCT ON ((m->>'id')::uuid)
                   (m->>'id')::uuid AS id, m->>'op' AS op, (m->>'version')::bigint AS version,
                   (m->>'date_creation')::date AS date_creation, coalesce(m->'donnees', '{}'::jsonb) AS donnees
            FROM jsonb_array_elements($1) m
            ORDER BY (m->>'id')::uuid, (m->>'version')::bigint DESC
        )
        INSERT INTO chaud.%1$I (id, date_creation, %2$s, _op, _version)
        SELECT m.id, m.date_creation, %3$s, m.op, m.version
        FROM messages m
        CROSS JOIN LATERAL jsonb_populate_record(NULL::chaud.%1$I, m.donnees) d
        ON CONFLICT (id) DO UPDATE
           SET date_creation = EXCLUDED.date_creation, %4$s,
               _op = EXCLUDED._op, _version = EXCLUDED._version, _recu = clock_timestamp()
         WHERE chaud.%1$I._version < EXCLUDED._version$f$, nom, liste, valeurs, maj)
    USING lot;
    GET DIAGNOSTICS appliquees = ROW_COUNT;
    RETURN appliquees;
END $$;

-- Déplace vers Iceberg les lignes chaudes reçues avant « coupure ». Une ligne réécrite pendant la fusion
-- reste chaude (son _recu est postérieur). Une version plus ancienne que celle déjà fusionnée est ignorée.
CREATE OR REPLACE FUNCTION lake.fusionner(nom text, coupure timestamptz DEFAULT clock_timestamp())
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    liste text;
    fusionnees bigint;
BEGIN
    SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum) INTO liste
    FROM pg_attribute a
    WHERE a.attrelid = format('chaud.%I', nom)::regclass AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attname NOT IN ('id', 'date_creation', '_op', '_version', '_recu');

    DROP TABLE IF EXISTS pg_temp.lot_fusion;
    -- les lignes sorties de la table chaude sont verrouillées : une écriture concurrente attend la fin de la fusion
    EXECUTE format($f$
        CREATE TEMP TABLE lot_fusion ON COMMIT DROP AS
        WITH sorties AS (DELETE FROM chaud.%1$I WHERE _recu <= $1 RETURNING *)
        SELECT * FROM sorties$f$, nom)
    USING coupure;
    -- versions déjà dépassées dans Iceberg : ignorées
    EXECUTE format('DELETE FROM lot_fusion l USING app.%I c WHERE c.id = l.id AND c._version >= l._version', nom);
    EXECUTE format('DELETE FROM app.%I c USING lot_fusion l WHERE c.id = l.id', nom);
    EXECUTE format($f$
        INSERT INTO app.%1$I (id, date_creation, %2$s, _version, _supprime)
        SELECT id, date_creation, %2$s, _version, _op = 'DELETE' FROM lot_fusion$f$, nom, liste);
    GET DIAGNOSTICS fusionnees = ROW_COUNT;

    EXECUTE format($f$
        WITH sorties AS (DELETE FROM chaud.%1$I WHERE _recu <= $1 RETURNING *)
        INSERT INTO app.%1$I (id, _op, _version, _recu, donnees)
        SELECT id, _op, _version, _recu, donnees::text FROM sorties$f$, nom || '_historique')
    USING coupure;
    RETURN fusionnees;
END $$;

-- Fusionne toutes les tables gérées, chacune dans sa propre transaction (une table en échec n'arrête pas les autres).
CREATE OR REPLACE PROCEDURE lake.fusionner_tout()
LANGUAGE plpgsql AS $$
DECLARE
    t record;
    n bigint;
BEGIN
    FOR t IN SELECT nom FROM lake.tables ORDER BY nom LOOP
        BEGIN
            n := lake.fusionner(t.nom);
            RAISE LOG 'fusion %: % ligne(s) vers Iceberg', t.nom, n;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'fusion % en échec : %', t.nom, SQLERRM;
        END;
        COMMIT;
    END LOOP;
END $$;
