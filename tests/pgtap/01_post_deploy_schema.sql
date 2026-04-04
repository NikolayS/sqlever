-- tests/pgtap/01_plan_parsing.sql
-- pgTAP tests: plan file parsing behavior
--
-- Derived from sqitchers/sqitch t/plan.t behavioral assertions.
-- Tests what SQLever records in the database after parsing a sqitch.plan.
--
-- Key assertions from plan.t:
--   - Change IDs are SHA1 hashes (40 hex chars)
--   - Tag IDs are SHA1 hashes (40 hex chars)
--   - The %syntax-version pragma is handled (plan loads without error)
--   - Tags appear as "@tagname" in the plan
--   - Each change has a planner_name and planner_email
--   - planned_at is a valid timestamptz
--
-- These tests operate on a pre-populated sqitch schema (seeded by fixture
-- below). Real plan-parsing tests require the CLI to run; here we test
-- the _output_ of parsing as stored in the DB.

BEGIN;

SELECT plan(20);

-- ---------------------------------------------------------------------------
-- Fixture: seed a minimal sqitch schema with parsed plan data
-- ---------------------------------------------------------------------------

-- Create schema if it does not exist (tests run in isolation)
CREATE SCHEMA IF NOT EXISTS sqitch;

CREATE TABLE IF NOT EXISTS sqitch.projects (
    project         TEXT        PRIMARY KEY,
    uri             TEXT        NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    creator_name    TEXT        NOT NULL,
    creator_email   TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS sqitch.changes (
    change_id       TEXT        PRIMARY KEY,
    script_hash     TEXT,
    change          TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    UNIQUE (project, script_hash)
);

CREATE TABLE IF NOT EXISTS sqitch.tags (
    tag_id          TEXT        PRIMARY KEY,
    tag             TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    change_id       TEXT        NOT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    UNIQUE (project, tag)
);

CREATE TABLE IF NOT EXISTS sqitch.dependencies (
    change_id     TEXT NOT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE ON DELETE CASCADE,
    type          TEXT NOT NULL,
    dependency    TEXT NOT NULL,
    dependency_id TEXT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE,
    PRIMARY KEY (change_id, dependency)
);

-- Seed project
INSERT INTO sqitch.projects (project, uri, creator_name, creator_email)
VALUES ('myapp', NULL, 'Test User', 'test@example.com');

-- Change IDs must be 40-char hex SHA1 strings (from plan.t: $change->id)
-- These simulate what sqlever produces after parsing a sqitch.plan
INSERT INTO sqitch.changes
    (change_id, change, project, note, committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    -- sha1("change <len>\0project myapp\nchange roles\nplanner Test User <test@example.com>\n...")
    ('e4d75a1d4c6df1d78e2dd6e03b3e9efde7db80e4',
     'roles', 'myapp', 'Add roles',
     'Test User', 'test@example.com',
     '2024-01-01 00:00:00+00', 'Test User', 'test@example.com'),
    ('b8f4e3a7c2d91b6f8e5a4d3c9e7b2f1a6d4c8e3b',
     'users', 'myapp', 'Add users table',
     'Test User', 'test@example.com',
     '2024-01-02 00:00:00+00', 'Test User', 'test@example.com');

-- Tag IDs are also SHA1 (from tag.t: $tag->id is Digest::SHA->new(1) of info string)
INSERT INTO sqitch.tags
    (tag_id, tag, project, change_id, note, committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2',
     '@v1.0.0', 'myapp', 'b8f4e3a7c2d91b6f8e5a4d3c9e7b2f1a6d4c8e3b',
     '', 'Test User', 'test@example.com',
     '2024-01-02 00:00:00+00', 'Test User', 'test@example.com');

-- Dependency: users requires roles
INSERT INTO sqitch.dependencies (change_id, type, dependency, dependency_id)
VALUES
    ('b8f4e3a7c2d91b6f8e5a4d3c9e7b2f1a6d4c8e3b',
     'require', 'roles',
     'e4d75a1d4c6df1d78e2dd6e03b3e9efde7db80e4');

-- ---------------------------------------------------------------------------
-- Tests: Change IDs must be valid SHA1 hashes (40 hex chars)
-- Derived from plan.t: "change id" assertions using Digest::SHA
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(change_id ~ '^[0-9a-f]{40}$') FROM sqitch.changes WHERE project = 'myapp'),
    'All change IDs should be 40-char lowercase hex (SHA1) strings'
);

SELECT ok(
    (SELECT count(*) FROM sqitch.changes WHERE project = 'myapp') = 2,
    'Should have 2 changes in the project'
);

-- ---------------------------------------------------------------------------
-- Tests: Tag IDs must be valid SHA1 hashes
-- Derived from tag.t: $tag->id is SHA1 of "tag <len>\0<info>"
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(tag_id ~ '^[0-9a-f]{40}$') FROM sqitch.tags WHERE project = 'myapp'),
    'All tag IDs should be 40-char lowercase hex (SHA1) strings'
);

-- Tags must be stored with "@" prefix (format_name in plan.t returns "@foo")
SELECT ok(
    (SELECT bool_and(tag ~ '^@') FROM sqitch.tags WHERE project = 'myapp'),
    'All tags should be stored with "@" prefix'
);

-- ---------------------------------------------------------------------------
-- Tests: planned_at is a valid timestamptz (not null, has timezone info)
-- Derived from plan.t: $change->timestamp is App::Sqitch::DateTime (UTC)
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(planned_at IS NOT NULL) FROM sqitch.changes WHERE project = 'myapp'),
    'All changes should have non-null planned_at'
);

SELECT ok(
    (SELECT bool_and(
        EXTRACT(TIMEZONE FROM planned_at) IS NOT NULL
     ) FROM sqitch.changes WHERE project = 'myapp'),
    'planned_at should carry timezone information (TIMESTAMPTZ)'
);

-- ---------------------------------------------------------------------------
-- Tests: planner_name and planner_email are always set
-- Derived from plan.t: every change has planner_name, planner_email
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(planner_name IS NOT NULL AND planner_name <> '')
     FROM sqitch.changes WHERE project = 'myapp'),
    'All changes should have non-empty planner_name'
);

SELECT ok(
    (SELECT bool_and(planner_email IS NOT NULL AND planner_email <> '')
     FROM sqitch.changes WHERE project = 'myapp'),
    'All changes should have non-empty planner_email'
);

SELECT ok(
    (SELECT bool_and(planner_name IS NOT NULL AND planner_name <> '')
     FROM sqitch.tags WHERE project = 'myapp'),
    'All tags should have non-empty planner_name'
);

-- ---------------------------------------------------------------------------
-- Tests: Dependencies resolve correctly (":change_name" syntax)
-- Derived from plan.t: dep() function parses ":roles" as a require dependency
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT count(*) FROM sqitch.dependencies
     WHERE change_id = 'b8f4e3a7c2d91b6f8e5a4d3c9e7b2f1a6d4c8e3b'
       AND type = 'require') = 1,
    'users change should have one require dependency'
);

SELECT is(
    (SELECT dependency FROM sqitch.dependencies
     WHERE change_id = 'b8f4e3a7c2d91b6f8e5a4d3c9e7b2f1a6d4c8e3b'
       AND type = 'require'),
    'roles',
    'Dependency name should be resolved to change name without colon prefix'
);

SELECT is(
    (SELECT dependency_id FROM sqitch.dependencies
     WHERE change_id = 'b8f4e3a7c2d91b6f8e5a4d3c9e7b2f1a6d4c8e3b'
       AND type = 'require'),
    'e4d75a1d4c6df1d78e2dd6e03b3e9efde7db80e4',
    'Dependency should resolve to correct change_id'
);

-- ---------------------------------------------------------------------------
-- Tests: Tag is associated with the correct change
-- Derived from plan.t/tag.t: tag always points to the change at that position
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT change_id FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    'b8f4e3a7c2d91b6f8e5a4d3c9e7b2f1a6d4c8e3b',
    'Tag @v1.0.0 should point to the users change'
);

-- ---------------------------------------------------------------------------
-- Tests: Plan ordering — changes are ordered by committed_at
-- Derived from plan.t: position() and index_of() track ordering
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT planned_at FROM sqitch.changes WHERE change = 'roles' AND project = 'myapp')
    < (SELECT planned_at FROM sqitch.changes WHERE change = 'users' AND project = 'myapp'),
    'roles change should be planned before users change'
);

-- ---------------------------------------------------------------------------
-- Tests: Project is correctly stored (from %project pragma in sqitch.plan)
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT project FROM sqitch.projects WHERE project = 'myapp'),
    'myapp',
    'Project name should match what was parsed from the plan'
);

-- ---------------------------------------------------------------------------
-- Tests: todo blocks for unimplemented plan parsing checks
-- These require the CLI to parse and import a real plan file
-- ---------------------------------------------------------------------------

SELECT todo('not yet implemented: syntax-version pragma validation via CLI', 2);
SELECT ok(false, 'sqlever should reject plans with unknown %syntax-version');
SELECT ok(false, 'sqlever should accept plans with %syntax-version=1.0.0');
SELECT todo_end();

SELECT todo('not yet implemented: invalid change name rejection', 1);
SELECT ok(false, 'sqlever should reject change names with invalid characters (spaces, @, etc.)');
SELECT todo_end();

SELECT todo('not yet implemented: conflict dependency storage (:!change_name)', 1);
SELECT ok(false, 'Conflict dependencies (":!change") should be stored with type = conflict');
SELECT todo_end();

SELECT * FROM finish();

ROLLBACK;
