-- tests/pgtap/00_schema.sql
-- pgTAP tests: sqitch tracking schema structure
--
-- Verifies that after sqlever init + first deploy the sqitch.* schema
-- has exactly the structure mandated by SPEC R3 / Sqitch compatibility.
--
-- These tests are derived from behavioral assertions in sqitchers/sqitch
-- t/pg.t (DBIEngineTest) and Sqitch's own pg.sql DDL.
--
-- Run with:
--   pg_prove -h localhost -p 5418 -U postgres -d postgres tests/pgtap/00_schema.sql

BEGIN;

-- We need pgtap loaded before plan().
SELECT plan(60);

-- ---------------------------------------------------------------------------
-- Schema exists
-- ---------------------------------------------------------------------------

SELECT has_schema('sqitch', 'Schema sqitch should exist');

-- ---------------------------------------------------------------------------
-- sqitch.projects
-- ---------------------------------------------------------------------------

SELECT has_table('sqitch', 'projects', 'Table sqitch.projects should exist');
SELECT has_pk('sqitch', 'projects', 'sqitch.projects should have a primary key');

SELECT has_column('sqitch', 'projects', 'project',       'projects.project column should exist');
SELECT has_column('sqitch', 'projects', 'uri',           'projects.uri column should exist');
SELECT has_column('sqitch', 'projects', 'created_at',    'projects.created_at column should exist');
SELECT has_column('sqitch', 'projects', 'creator_name',  'projects.creator_name column should exist');
SELECT has_column('sqitch', 'projects', 'creator_email', 'projects.creator_email column should exist');

SELECT col_type_is('sqitch', 'projects', 'project',       'text',                        'projects.project is TEXT');
SELECT col_type_is('sqitch', 'projects', 'uri',           'text',                        'projects.uri is TEXT');
SELECT col_type_is('sqitch', 'projects', 'created_at',    'timestamp with time zone',    'projects.created_at is TIMESTAMPTZ');
SELECT col_type_is('sqitch', 'projects', 'creator_name',  'text',                        'projects.creator_name is TEXT');
SELECT col_type_is('sqitch', 'projects', 'creator_email', 'text',                        'projects.creator_email is TEXT');

SELECT col_not_null('sqitch', 'projects', 'project',       'projects.project is NOT NULL');
SELECT col_not_null('sqitch', 'projects', 'created_at',    'projects.created_at is NOT NULL');
SELECT col_not_null('sqitch', 'projects', 'creator_name',  'projects.creator_name is NOT NULL');
SELECT col_not_null('sqitch', 'projects', 'creator_email', 'projects.creator_email is NOT NULL');
SELECT col_is_null( 'sqitch', 'projects', 'uri',           'projects.uri is NULLable');

-- ---------------------------------------------------------------------------
-- sqitch.releases
-- ---------------------------------------------------------------------------

SELECT has_table('sqitch', 'releases', 'Table sqitch.releases should exist');
SELECT has_pk('sqitch', 'releases', 'sqitch.releases should have a primary key');

SELECT has_column('sqitch', 'releases', 'version',         'releases.version column should exist');
SELECT has_column('sqitch', 'releases', 'installed_at',    'releases.installed_at column should exist');
SELECT has_column('sqitch', 'releases', 'installer_name',  'releases.installer_name column should exist');
SELECT has_column('sqitch', 'releases', 'installer_email', 'releases.installer_email column should exist');

SELECT col_type_is('sqitch', 'releases', 'version',         'real',                     'releases.version is REAL');
SELECT col_type_is('sqitch', 'releases', 'installed_at',    'timestamp with time zone', 'releases.installed_at is TIMESTAMPTZ');
SELECT col_type_is('sqitch', 'releases', 'installer_name',  'text',                     'releases.installer_name is TEXT');
SELECT col_type_is('sqitch', 'releases', 'installer_email', 'text',                     'releases.installer_email is TEXT');

SELECT col_not_null('sqitch', 'releases', 'version',         'releases.version is NOT NULL');
SELECT col_not_null('sqitch', 'releases', 'installed_at',    'releases.installed_at is NOT NULL');
SELECT col_not_null('sqitch', 'releases', 'installer_name',  'releases.installer_name is NOT NULL');
SELECT col_not_null('sqitch', 'releases', 'installer_email', 'releases.installer_email is NOT NULL');

-- ---------------------------------------------------------------------------
-- sqitch.changes
-- ---------------------------------------------------------------------------

SELECT has_table('sqitch', 'changes', 'Table sqitch.changes should exist');
SELECT has_pk('sqitch', 'changes', 'sqitch.changes should have a primary key');

SELECT has_column('sqitch', 'changes', 'change_id',       'changes.change_id column should exist');
SELECT has_column('sqitch', 'changes', 'script_hash',     'changes.script_hash column should exist');
SELECT has_column('sqitch', 'changes', 'change',          'changes.change column should exist');
SELECT has_column('sqitch', 'changes', 'project',         'changes.project column should exist');
SELECT has_column('sqitch', 'changes', 'note',            'changes.note column should exist');
SELECT has_column('sqitch', 'changes', 'committed_at',    'changes.committed_at column should exist');
SELECT has_column('sqitch', 'changes', 'committer_name',  'changes.committer_name column should exist');
SELECT has_column('sqitch', 'changes', 'committer_email', 'changes.committer_email column should exist');
SELECT has_column('sqitch', 'changes', 'planned_at',      'changes.planned_at column should exist');
SELECT has_column('sqitch', 'changes', 'planner_name',    'changes.planner_name column should exist');
SELECT has_column('sqitch', 'changes', 'planner_email',   'changes.planner_email column should exist');

-- Sqitch compatibility: planned_at must be timestamptz (not plain timestamp)
SELECT col_type_is('sqitch', 'changes', 'planned_at',   'timestamp with time zone', 'changes.planned_at is TIMESTAMPTZ');
SELECT col_type_is('sqitch', 'changes', 'committed_at', 'timestamp with time zone', 'changes.committed_at is TIMESTAMPTZ');
SELECT col_type_is('sqitch', 'changes', 'change_id',    'text',                     'changes.change_id is TEXT');
SELECT col_type_is('sqitch', 'changes', 'change',       'text',                     'changes.change is TEXT');
SELECT col_type_is('sqitch', 'changes', 'project',      'text',                     'changes.project is TEXT');

-- planner_name + planner_email must be NOT NULL (from Sqitch spec)
SELECT col_not_null('sqitch', 'changes', 'planner_name',    'changes.planner_name is NOT NULL');
SELECT col_not_null('sqitch', 'changes', 'planner_email',   'changes.planner_email is NOT NULL');
SELECT col_not_null('sqitch', 'changes', 'committer_name',  'changes.committer_name is NOT NULL');
SELECT col_not_null('sqitch', 'changes', 'committer_email', 'changes.committer_email is NOT NULL');
SELECT col_not_null('sqitch', 'changes', 'committed_at',    'changes.committed_at is NOT NULL');
SELECT col_not_null('sqitch', 'changes', 'planned_at',      'changes.planned_at is NOT NULL');

-- FK: changes.project → projects.project
SELECT fk_ok(
    'sqitch', 'changes',  ARRAY['project'],
    'sqitch', 'projects', ARRAY['project'],
    'changes.project FK to projects.project'
);

-- ---------------------------------------------------------------------------
-- sqitch.tags
-- ---------------------------------------------------------------------------

SELECT has_table('sqitch', 'tags', 'Table sqitch.tags should exist');
SELECT has_pk('sqitch', 'tags', 'sqitch.tags should have a primary key');

SELECT has_column('sqitch', 'tags', 'tag_id',        'tags.tag_id column should exist');
SELECT has_column('sqitch', 'tags', 'tag',            'tags.tag column should exist');
SELECT has_column('sqitch', 'tags', 'project',        'tags.project column should exist');
SELECT has_column('sqitch', 'tags', 'change_id',      'tags.change_id column should exist');
SELECT has_column('sqitch', 'tags', 'planned_at',     'tags.planned_at column should exist');
SELECT has_column('sqitch', 'tags', 'planner_name',   'tags.planner_name column should exist');
SELECT has_column('sqitch', 'tags', 'planner_email',  'tags.planner_email column should exist');

SELECT col_type_is('sqitch', 'tags', 'planned_at', 'timestamp with time zone', 'tags.planned_at is TIMESTAMPTZ');
SELECT col_not_null('sqitch', 'tags', 'planner_name',  'tags.planner_name is NOT NULL');
SELECT col_not_null('sqitch', 'tags', 'planner_email', 'tags.planner_email is NOT NULL');

-- FK: tags.project → projects.project
SELECT fk_ok(
    'sqitch', 'tags',     ARRAY['project'],
    'sqitch', 'projects', ARRAY['project'],
    'tags.project FK to projects.project'
);

-- FK: tags.change_id → changes.change_id
SELECT fk_ok(
    'sqitch', 'tags',    ARRAY['change_id'],
    'sqitch', 'changes', ARRAY['change_id'],
    'tags.change_id FK to changes.change_id'
);

-- ---------------------------------------------------------------------------
-- sqitch.events
-- ---------------------------------------------------------------------------

SELECT has_table('sqitch', 'events', 'Table sqitch.events should exist');

SELECT has_column('sqitch', 'events', 'event',          'events.event column should exist');
SELECT has_column('sqitch', 'events', 'change_id',      'events.change_id column should exist');
SELECT has_column('sqitch', 'events', 'change',         'events.change column should exist');
SELECT has_column('sqitch', 'events', 'project',        'events.project column should exist');
SELECT has_column('sqitch', 'events', 'requires',       'events.requires column should exist');
SELECT has_column('sqitch', 'events', 'conflicts',      'events.conflicts column should exist');
SELECT has_column('sqitch', 'events', 'tags',           'events.tags column should exist');
SELECT has_column('sqitch', 'events', 'committed_at',   'events.committed_at column should exist');
SELECT has_column('sqitch', 'events', 'planned_at',     'events.planned_at column should exist');
SELECT has_column('sqitch', 'events', 'planner_name',   'events.planner_name column should exist');
SELECT has_column('sqitch', 'events', 'planner_email',  'events.planner_email column should exist');

SELECT col_type_is('sqitch', 'events', 'planned_at',   'timestamp with time zone', 'events.planned_at is TIMESTAMPTZ');
SELECT col_type_is('sqitch', 'events', 'committed_at', 'timestamp with time zone', 'events.committed_at is TIMESTAMPTZ');
SELECT col_type_is('sqitch', 'events', 'requires',     'text[]',                   'events.requires is TEXT[]');
SELECT col_type_is('sqitch', 'events', 'conflicts',    'text[]',                   'events.conflicts is TEXT[]');
SELECT col_type_is('sqitch', 'events', 'tags',         'text[]',                   'events.tags is TEXT[]');

-- event column CHECK constraint: only valid event types
SELECT col_has_check('sqitch', 'events', 'event', 'events.event has CHECK constraint');

-- FK: events.project → projects.project
SELECT fk_ok(
    'sqitch', 'events',   ARRAY['project'],
    'sqitch', 'projects', ARRAY['project'],
    'events.project FK to projects.project'
);

-- ---------------------------------------------------------------------------
-- sqitch.dependencies
-- ---------------------------------------------------------------------------

SELECT has_table('sqitch', 'dependencies', 'Table sqitch.dependencies should exist');

SELECT has_column('sqitch', 'dependencies', 'change_id',     'dependencies.change_id column should exist');
SELECT has_column('sqitch', 'dependencies', 'type',          'dependencies.type column should exist');
SELECT has_column('sqitch', 'dependencies', 'dependency',    'dependencies.dependency column should exist');
SELECT has_column('sqitch', 'dependencies', 'dependency_id', 'dependencies.dependency_id column should exist');

-- FK: dependencies.change_id → changes.change_id (with CASCADE DELETE)
SELECT fk_ok(
    'sqitch', 'dependencies', ARRAY['change_id'],
    'sqitch', 'changes',      ARRAY['change_id'],
    'dependencies.change_id FK to changes.change_id'
);

SELECT * FROM finish();

ROLLBACK;
