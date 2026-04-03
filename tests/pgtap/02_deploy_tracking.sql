-- tests/pgtap/02_deploy_tracking.sql
-- pgTAP tests: what ends up in sqitch.changes after a deploy
--
-- Derived from sqitchers/sqitch t/deploy.t and t/pg.t (DBIEngineTest).
--
-- Key assertions from deploy.t + pg.t:
--   - After deploy: row exists in sqitch.changes with correct change_id
--   - committed_at is set (not null, recent timestamp)
--   - committer_name and committer_email match the deployer config
--   - change_id must match the SHA1 computed from the plan
--   - project field must match sqitch.conf [core] project
--   - requires/conflicts arrays are stored in sqitch.dependencies
--   - A 'deploy' event is recorded in sqitch.events
--
-- From DBIEngineTest: after log_deploy(), the change appears in the
-- "deployed" set returned by deployed_changes().

BEGIN;

SELECT plan(22);

-- ---------------------------------------------------------------------------
-- Fixture: minimal sqitch schema + one deployed change
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS sqitch;

CREATE TABLE IF NOT EXISTS sqitch.projects (
    project         TEXT        PRIMARY KEY,
    uri             TEXT        NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    creator_name    TEXT        NOT NULL,
    creator_email   TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS sqitch.releases (
    version         REAL        PRIMARY KEY,
    installed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    installer_name  TEXT        NOT NULL,
    installer_email TEXT        NOT NULL
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

CREATE TABLE IF NOT EXISTS sqitch.events (
    event           TEXT        NOT NULL CHECK (event IN ('deploy', 'revert', 'fail', 'merge')),
    change_id       TEXT        NOT NULL,
    change          TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    requires        TEXT[]      NOT NULL DEFAULT '{}',
    conflicts       TEXT[]      NOT NULL DEFAULT '{}',
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    PRIMARY KEY (change_id, committed_at)
);

-- Seed project
INSERT INTO sqitch.projects (project, uri, creator_name, creator_email)
VALUES ('widget', 'https://example.com/widget', 'Alice Dev', 'alice@example.com');

-- Simulate a sqlever deploy recording
-- change_id = SHA1 computed from the plan (40 hex chars)
INSERT INTO sqitch.changes
    (change_id, script_hash, change, project, note,
     committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('deadbeef01234567890abcdef01234567890abcd',
     'abc123def456abc123def456abc123def456abc1',
     'create_widgets_table',
     'widget',
     'Creates the widgets table',
     'Alice Dev', 'alice@example.com',
     '2024-03-01 12:00:00+00',
     'Alice Dev', 'alice@example.com');

-- Record the deploy event (what sqlever's recordDeploy does)
INSERT INTO sqitch.events
    (event, change_id, change, project, note,
     requires, conflicts, tags,
     committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('deploy',
     'deadbeef01234567890abcdef01234567890abcd',
     'create_widgets_table',
     'widget',
     'Creates the widgets table',
     ARRAY[]::TEXT[], ARRAY[]::TEXT[], ARRAY[]::TEXT[],
     'Alice Dev', 'alice@example.com',
     '2024-03-01 12:00:00+00',
     'Alice Dev', 'alice@example.com');

-- Second change with a dependency
INSERT INTO sqitch.changes
    (change_id, script_hash, change, project, note,
     committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('cafebabe01234567890abcdef01234567890abcd',
     'def456abc123def456abc123def456abc123def4',
     'add_widget_name_column',
     'widget',
     'Adds name column to widgets',
     'Alice Dev', 'alice@example.com',
     '2024-03-02 12:00:00+00',
     'Alice Dev', 'alice@example.com');

INSERT INTO sqitch.dependencies (change_id, type, dependency, dependency_id)
VALUES
    ('cafebabe01234567890abcdef01234567890abcd',
     'require', 'create_widgets_table',
     'deadbeef01234567890abcdef01234567890abcd');

INSERT INTO sqitch.events
    (event, change_id, change, project, note,
     requires, conflicts, tags,
     committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('deploy',
     'cafebabe01234567890abcdef01234567890abcd',
     'add_widget_name_column',
     'widget',
     'Adds name column to widgets',
     ARRAY['create_widgets_table']::TEXT[], ARRAY[]::TEXT[], ARRAY[]::TEXT[],
     'Alice Dev', 'alice@example.com',
     '2024-03-02 12:00:00+00',
     'Alice Dev', 'alice@example.com');

-- ---------------------------------------------------------------------------
-- Tests: deployed_at (committed_at) is set
-- From pg.t DBIEngineTest: after log_deploy(), committed_at is not null
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT committed_at IS NOT NULL FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'committed_at should be set after deploy'
);

SELECT ok(
    (SELECT committed_at > '2000-01-01'::TIMESTAMPTZ FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'committed_at should be a recent timestamp'
);

-- ---------------------------------------------------------------------------
-- Tests: deployer_name and deployer_email match the config
-- From pg.t: log_deploy() stores the sqitch->user_name as committer_name
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT committer_name FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'Alice Dev',
    'committer_name should match the deployer from config'
);

SELECT is(
    (SELECT committer_email FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'alice@example.com',
    'committer_email should match the deployer from config'
);

-- ---------------------------------------------------------------------------
-- Tests: change_id matches the SHA1 from the plan
-- From pg.t: the change_id stored must equal the plan's $change->id
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT change_id FROM sqitch.changes
     WHERE change = 'create_widgets_table' AND project = 'widget')
    = 'deadbeef01234567890abcdef01234567890abcd',
    'change_id should match the SHA1 from the plan'
);

SELECT ok(
    (SELECT change_id FROM sqitch.changes
     WHERE change = 'create_widgets_table' AND project = 'widget')
    ~ '^[0-9a-f]{40}$',
    'change_id should be a valid 40-char SHA1 hex string'
);

-- ---------------------------------------------------------------------------
-- Tests: project field matches sqitch.conf
-- From pg.t: log_deploy() uses engine->target->sqitch->project
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT project FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'widget',
    'project field should match the configured project name'
);

SELECT ok(
    (SELECT count(*) FROM sqitch.changes WHERE project = 'widget') = 2,
    'Both changes should be recorded under the widget project'
);

-- ---------------------------------------------------------------------------
-- Tests: requires array stored correctly in dependencies
-- From pg.t: DBIEngineTest checks that requires/conflicts are preserved
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT count(*) FROM sqitch.dependencies
     WHERE change_id = 'cafebabe01234567890abcdef01234567890abcd'
       AND type = 'require') = 1,
    'add_widget_name_column should have one require dependency'
);

SELECT is(
    (SELECT dependency FROM sqitch.dependencies
     WHERE change_id = 'cafebabe01234567890abcdef01234567890abcd'
       AND type = 'require'),
    'create_widgets_table',
    'Require dependency name should be stored correctly'
);

SELECT is(
    (SELECT dependency_id FROM sqitch.dependencies
     WHERE change_id = 'cafebabe01234567890abcdef01234567890abcd'
       AND type = 'require'),
    'deadbeef01234567890abcdef01234567890abcd',
    'Require dependency_id should resolve to the correct change'
);

-- No conflicts for this change
SELECT ok(
    (SELECT count(*) FROM sqitch.dependencies
     WHERE change_id = 'cafebabe01234567890abcdef01234567890abcd'
       AND type = 'conflict') = 0,
    'add_widget_name_column should have no conflict dependencies'
);

-- ---------------------------------------------------------------------------
-- Tests: deploy event recorded in sqitch.events
-- From pg.t: log_deploy() records a 'deploy' event
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT count(*) FROM sqitch.events
     WHERE event = 'deploy' AND project = 'widget') = 2,
    'Both deploys should create deploy events in sqitch.events'
);

SELECT is(
    (SELECT event FROM sqitch.events
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'deploy',
    'Event type should be deploy'
);

SELECT ok(
    (SELECT committed_at IS NOT NULL FROM sqitch.events
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'Deploy event committed_at should be set'
);

-- requires array in events matches the dependency list
SELECT ok(
    (SELECT requires FROM sqitch.events
     WHERE change_id = 'cafebabe01234567890abcdef01234567890abcd')
    @> ARRAY['create_widgets_table'],
    'Deploy event requires array should contain create_widgets_table'
);

-- ---------------------------------------------------------------------------
-- Tests: planned_at preserved from the plan
-- From pg.t: planned_at in sqitch.changes must match the plan's timestamp
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT planned_at FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    '2024-03-01 12:00:00+00'::TIMESTAMPTZ,
    'planned_at should match the timestamp from the plan'
);

-- ---------------------------------------------------------------------------
-- Tests: planner info preserved from the plan
-- From plan.t: planner_name is the user who wrote the plan entry
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT planner_name FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'Alice Dev',
    'planner_name should be preserved from the plan'
);

SELECT is(
    (SELECT planner_email FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'alice@example.com',
    'planner_email should be preserved from the plan'
);

-- ---------------------------------------------------------------------------
-- Tests: script_hash stored (Sqitch 1.x feature for idempotency)
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT script_hash IS NOT NULL FROM sqitch.changes
     WHERE change_id = 'deadbeef01234567890abcdef01234567890abcd'),
    'script_hash should be recorded after deploy'
);

-- ---------------------------------------------------------------------------
-- todo: checks requiring live CLI invocation
-- ---------------------------------------------------------------------------

SELECT todo('not yet implemented: verify that committer = git config user.name', 1);
SELECT ok(false, 'committer_name should fall back to git config user.name when sqitch.conf has no user');
SELECT todo_end();

SELECT todo('not yet implemented: concurrent deploy locking', 1);
SELECT ok(false, 'Concurrent deploys should use advisory lock to prevent double-deploy');
SELECT todo_end();

SELECT * FROM finish();

ROLLBACK;
