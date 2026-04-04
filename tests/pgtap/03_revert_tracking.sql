-- tests/pgtap/03_revert_tracking.sql
-- pgTAP tests: what happens in sqitch.* after a revert
--
-- Derived from sqitchers/sqitch t/revert.t and t/pg.t (DBIEngineTest).
--
-- Key assertions from revert.t + pg.t:
--   - After revert: row is REMOVED from sqitch.changes
--   - After revert: a 'revert' event appears in sqitch.events
--   - Dependencies are removed (CASCADE DELETE from sqitch.dependencies)
--   - Dependent changes are reverted in reverse order (LIFO)
--   - reverted change should NOT appear in deployed set
--   - sqitch.tags referencing the reverted change are removed first
--     (or revert is blocked if a tag still references the change)
--
-- From DBIEngineTest: log_revert() removes from changes, records in events.

BEGIN;

SELECT plan(18);

-- ---------------------------------------------------------------------------
-- Fixture: schema + two deployed changes, then revert the second
-- ---------------------------------------------------------------------------

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

-- Seed project
INSERT INTO sqitch.projects (project, uri, creator_name, creator_email)
VALUES ('myapp', NULL, 'Dev User', 'dev@example.com');

-- Deploy two changes
INSERT INTO sqitch.changes
    (change_id, change, project, note, committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('aaaa0000111122223333444455556666777788889',
     'create_schema', 'myapp', '', 'Dev User', 'dev@example.com',
     '2024-01-01 00:00:00+00', 'Dev User', 'dev@example.com'),
    ('bbbb0000111122223333444455556666777788889',
     'create_users', 'myapp', '', 'Dev User', 'dev@example.com',
     '2024-01-02 00:00:00+00', 'Dev User', 'dev@example.com');

-- create_users depends on create_schema
INSERT INTO sqitch.dependencies (change_id, type, dependency, dependency_id)
VALUES
    ('bbbb0000111122223333444455556666777788889',
     'require', 'create_schema',
     'aaaa0000111122223333444455556666777788889');

-- Deploy events
INSERT INTO sqitch.events
    (event, change_id, change, project, note, requires, conflicts, tags,
     committer_name, committer_email, planned_at, planner_name, planner_email)
VALUES
    ('deploy', 'aaaa0000111122223333444455556666777788889',
     'create_schema', 'myapp', '', '{}', '{}', '{}',
     'Dev User', 'dev@example.com',
     '2024-01-01 00:00:00+00', 'Dev User', 'dev@example.com'),
    ('deploy', 'bbbb0000111122223333444455556666777788889',
     'create_users', 'myapp', '', '{create_schema}', '{}', '{}',
     'Dev User', 'dev@example.com',
     '2024-01-02 00:00:00+00', 'Dev User', 'dev@example.com');

-- Sanity: both changes deployed
SELECT is(
    (SELECT count(*)::INT FROM sqitch.changes WHERE project = 'myapp'),
    2,
    'Should start with 2 deployed changes'
);

-- ---------------------------------------------------------------------------
-- Simulate revert of create_users (the most recent change)
-- This mirrors what sqlever's recordRevert() does
-- ---------------------------------------------------------------------------

-- Step 1: Insert revert event BEFORE deleting (no FK from events to changes)
INSERT INTO sqitch.events
    (event, change_id, change, project, note, requires, conflicts, tags,
     committer_name, committer_email, planned_at, planner_name, planner_email)
VALUES
    ('revert', 'bbbb0000111122223333444455556666777788889',
     'create_users', 'myapp', '', '{create_schema}', '{}', '{}',
     'Dev User', 'dev@example.com',
     '2024-01-02 00:00:00+00', 'Dev User', 'dev@example.com');

-- Step 2: Delete dependencies (cascades automatically too, but explicit)
DELETE FROM sqitch.dependencies WHERE change_id = 'bbbb0000111122223333444455556666777788889';

-- Step 3: Delete the change
DELETE FROM sqitch.changes WHERE change_id = 'bbbb0000111122223333444455556666777788889';

-- ---------------------------------------------------------------------------
-- Tests: Row removed from sqitch.changes after revert
-- From revert.t: after revert, change is no longer in deployed set
-- ---------------------------------------------------------------------------

SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM sqitch.changes
        WHERE change_id = 'bbbb0000111122223333444455556666777788889'
    ),
    'Reverted change should be removed from sqitch.changes'
);

SELECT is(
    (SELECT count(*)::INT FROM sqitch.changes WHERE project = 'myapp'),
    1,
    'Only one change should remain after revert'
);

SELECT is(
    (SELECT change FROM sqitch.changes WHERE project = 'myapp'),
    'create_schema',
    'create_schema should still be deployed after reverting create_users'
);

-- ---------------------------------------------------------------------------
-- Tests: Dependencies cascade-deleted with the change
-- From pg.t: ON DELETE CASCADE on sqitch.dependencies
-- ---------------------------------------------------------------------------

SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM sqitch.dependencies
        WHERE change_id = 'bbbb0000111122223333444455556666777788889'
    ),
    'Dependencies should be removed when change is reverted'
);

-- ---------------------------------------------------------------------------
-- Tests: 'revert' event appears in sqitch.events
-- From pg.t: log_revert() inserts a revert event
-- ---------------------------------------------------------------------------

SELECT ok(
    EXISTS (
        SELECT 1 FROM sqitch.events
        WHERE event = 'revert'
          AND change_id = 'bbbb0000111122223333444455556666777788889'
          AND project = 'myapp'
    ),
    'A revert event should appear in sqitch.events'
);

SELECT is(
    (SELECT count(*)::INT FROM sqitch.events
     WHERE event = 'revert' AND project = 'myapp'),
    1,
    'Exactly one revert event should exist'
);

-- Events persist even after the change is deleted
SELECT ok(
    (SELECT count(*)::INT FROM sqitch.events WHERE project = 'myapp') = 3,
    'sqitch.events should have all 3 events (2 deploys + 1 revert)'
);

-- ---------------------------------------------------------------------------
-- Tests: Revert event has correct metadata
-- From pg.t: committer info is stored in the revert event
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT committer_name FROM sqitch.events
     WHERE event = 'revert' AND change_id = 'bbbb0000111122223333444455556666777788889'),
    'Dev User',
    'Revert event should record the committer_name'
);

SELECT is(
    (SELECT change FROM sqitch.events
     WHERE event = 'revert' AND change_id = 'bbbb0000111122223333444455556666777788889'),
    'create_users',
    'Revert event should record the change name'
);

SELECT ok(
    (SELECT committed_at IS NOT NULL FROM sqitch.events
     WHERE event = 'revert' AND change_id = 'bbbb0000111122223333444455556666777788889'),
    'Revert event should have committed_at set'
);

-- requires preserved in event even after change deleted
SELECT ok(
    (SELECT requires FROM sqitch.events
     WHERE event = 'revert' AND change_id = 'bbbb0000111122223333444455556666777788889')
    @> ARRAY['create_schema'],
    'Revert event should preserve the requires array'
);

-- ---------------------------------------------------------------------------
-- Tests: Ordering — events are in chronological order
-- From log.t: events ordered by committed_at
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT array_agg(event ORDER BY committed_at ASC) FROM sqitch.events WHERE project = 'myapp')
    = ARRAY['deploy', 'deploy', 'revert'],
    'Events should be ordered: deploy, deploy, revert (chronological)'
);

-- ---------------------------------------------------------------------------
-- Tests: The remaining deployed change is intact
-- ---------------------------------------------------------------------------

SELECT ok(
    EXISTS (
        SELECT 1 FROM sqitch.changes
        WHERE change_id = 'aaaa0000111122223333444455556666777788889'
          AND change = 'create_schema'
    ),
    'create_schema should still be in sqitch.changes after reverting create_users'
);

-- ---------------------------------------------------------------------------
-- todo: checks that require live CLI (LIFO revert ordering)
-- From revert.t: when reverting multiple changes, revert in reverse order
-- ---------------------------------------------------------------------------

SELECT todo('not yet implemented: LIFO revert ordering enforcement', 2);
SELECT ok(false, 'Reverting to an older change should automatically revert dependent changes first');
SELECT ok(false, 'sqlever revert should process changes in reverse committed_at order (LIFO)');
SELECT todo_end();

SELECT todo('not yet implemented: tag-blocked revert', 1);
SELECT ok(false, 'Reverting past a tag should warn or require --to flag');
SELECT todo_end();

SELECT * FROM finish();

ROLLBACK;
