-- tests/pgtap/05_events_log.sql
-- pgTAP tests: sqitch.events ordering and content
--
-- Derived from sqitchers/sqitch t/log.t and t/pg.t (DBIEngineTest).
--
-- Key assertions from log.t + pg.t:
--   - Every deploy creates a 'deploy' event in sqitch.events
--   - Every revert creates a 'revert' event in sqitch.events
--   - Events have logged_at (committed_at) TIMESTAMPTZ set automatically
--   - Events are ordered by committed_at (DESC by default in `sqitch log`)
--   - event column is constrained to: 'deploy', 'revert', 'fail', 'merge'
--   - requires, conflicts, tags arrays are TEXT[] (preserved from deploy)
--   - Primary key is (change_id, committed_at) — same change can appear
--     multiple times (deploy then revert)
--
-- From log.t: format options (full, oneline, etc.) are computed from
-- events query. The underlying data must have the right shape.
-- From pg.t: after log_deploy() + log_revert(), events has 2 rows for
-- the same change_id (with different committed_at and event type).

BEGIN;

SELECT plan(24);

-- ---------------------------------------------------------------------------
-- Fixture: schema + a complete deploy+revert cycle + a fail event
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
VALUES ('logtest', NULL, 'Logger User', 'logger@example.com');

-- Three changes deployed
INSERT INTO sqitch.changes
    (change_id, change, project, note, committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('aaa1111aaa2222aaa3333aaa4444aaa5555aaa66',
     'alpha', 'logtest', 'First change',
     'Logger User', 'logger@example.com',
     '2024-01-01 00:00:00+00', 'Logger User', 'logger@example.com'),
    ('bbb1111bbb2222bbb3333bbb4444bbb5555bbb66',
     'beta', 'logtest', 'Second change',
     'Logger User', 'logger@example.com',
     '2024-01-02 00:00:00+00', 'Logger User', 'logger@example.com');

-- Deploy events (alpha, beta)
INSERT INTO sqitch.events
    (event, change_id, change, project, note, requires, conflicts, tags,
     committer_name, committer_email, planned_at, planner_name, planner_email,
     committed_at)
VALUES
    ('deploy', 'aaa1111aaa2222aaa3333aaa4444aaa5555aaa66',
     'alpha', 'logtest', 'First change', '{}', '{}', '{}',
     'Logger User', 'logger@example.com',
     '2024-01-01 00:00:00+00', 'Logger User', 'logger@example.com',
     '2024-01-01 01:00:00+00'),
    ('deploy', 'bbb1111bbb2222bbb3333bbb4444bbb5555bbb66',
     'beta', 'logtest', 'Second change', '{alpha}', '{}', '{}',
     'Logger User', 'logger@example.com',
     '2024-01-02 00:00:00+00', 'Logger User', 'logger@example.com',
     '2024-01-02 01:00:00+00');

-- Revert of beta
INSERT INTO sqitch.events
    (event, change_id, change, project, note, requires, conflicts, tags,
     committer_name, committer_email, planned_at, planner_name, planner_email,
     committed_at)
VALUES
    ('revert', 'bbb1111bbb2222bbb3333bbb4444bbb5555bbb66',
     'beta', 'logtest', 'Second change', '{alpha}', '{}', '{}',
     'Logger User', 'logger@example.com',
     '2024-01-02 00:00:00+00', 'Logger User', 'logger@example.com',
     '2024-01-03 01:00:00+00');

-- Re-deploy beta (after fix)
INSERT INTO sqitch.events
    (event, change_id, change, project, note, requires, conflicts, tags,
     committer_name, committer_email, planned_at, planner_name, planner_email,
     committed_at)
VALUES
    ('deploy', 'bbb1111bbb2222bbb3333bbb4444bbb5555bbb66',
     'beta', 'logtest', 'Second change', '{alpha}', '{}', '{}',
     'Logger User', 'logger@example.com',
     '2024-01-02 00:00:00+00', 'Logger User', 'logger@example.com',
     '2024-01-04 01:00:00+00');

-- Fail event (attempted gamma, failed)
INSERT INTO sqitch.events
    (event, change_id, change, project, note, requires, conflicts, tags,
     committer_name, committer_email, planned_at, planner_name, planner_email,
     committed_at)
VALUES
    ('fail', 'ccc1111ccc2222ccc3333ccc4444ccc5555ccc66',
     'gamma', 'logtest', 'Third change (failed)', '{}', '{}', '{}',
     'Logger User', 'logger@example.com',
     '2024-01-05 00:00:00+00', 'Logger User', 'logger@example.com',
     '2024-01-05 01:00:00+00');

-- ---------------------------------------------------------------------------
-- Tests: Every deploy creates a 'deploy' event
-- From pg.t: after log_deploy() each change has an event row
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT count(*)::INT FROM sqitch.events WHERE event = 'deploy' AND project = 'logtest') = 3,
    'Should have 3 deploy events (alpha, beta, beta re-deploy)'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM sqitch.events
        WHERE event = 'deploy'
          AND change_id = 'aaa1111aaa2222aaa3333aaa4444aaa5555aaa66'
    ),
    'alpha deploy event should exist'
);

SELECT ok(
    (SELECT count(*)::INT FROM sqitch.events
     WHERE event = 'deploy'
       AND change_id = 'bbb1111bbb2222bbb3333bbb4444bbb5555bbb66') = 2,
    'beta should have 2 deploy events (initial + re-deploy)'
);

-- ---------------------------------------------------------------------------
-- Tests: Every revert creates a 'revert' event
-- From pg.t: log_revert() records in sqitch.events with event = 'revert'
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT count(*)::INT FROM sqitch.events WHERE event = 'revert' AND project = 'logtest') = 1,
    'Should have exactly 1 revert event'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM sqitch.events
        WHERE event = 'revert'
          AND change_id = 'bbb1111bbb2222bbb3333bbb4444bbb5555bbb66'
          AND project = 'logtest'
    ),
    'beta revert event should exist'
);

-- ---------------------------------------------------------------------------
-- Tests: Events have committed_at (logged_at) TIMESTAMPTZ set
-- From log.t: each event has a date shown in sqitch log output
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(committed_at IS NOT NULL) FROM sqitch.events WHERE project = 'logtest'),
    'All events should have committed_at set'
);

SELECT ok(
    (SELECT bool_and(
        EXTRACT(TIMEZONE FROM committed_at) IS NOT NULL
     ) FROM sqitch.events WHERE project = 'logtest'),
    'committed_at should carry timezone info (TIMESTAMPTZ)'
);

-- ---------------------------------------------------------------------------
-- Tests: Events ordered correctly (DESC by default = most recent first)
-- From log.t: sqitch log shows events in reverse chronological order
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT array_agg(event ORDER BY committed_at DESC) FROM sqitch.events WHERE project = 'logtest')
    = ARRAY['fail', 'deploy', 'revert', 'deploy', 'deploy'],
    'Events ordered DESC by committed_at: fail, deploy, revert, deploy, deploy'
);

SELECT ok(
    (SELECT array_agg(change ORDER BY committed_at DESC) FROM sqitch.events WHERE project = 'logtest')
    = ARRAY['gamma', 'beta', 'beta', 'beta', 'alpha'],
    'Change names ordered DESC: gamma, beta, beta, beta, alpha'
);

-- ---------------------------------------------------------------------------
-- Tests: Same change can appear multiple times (no uniqueness on change_id alone)
-- From pg.t: PK is (change_id, committed_at) — allows deploy+revert+deploy
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT count(*)::INT FROM sqitch.events
     WHERE change_id = 'bbb1111bbb2222bbb3333bbb4444bbb5555bbb66') = 3,
    'beta should appear 3 times in events: deploy, revert, deploy'
);

-- ---------------------------------------------------------------------------
-- Tests: event column CHECK constraint enforces valid event types
-- From pg.t: only 'deploy', 'revert', 'fail', 'merge' are valid
-- ---------------------------------------------------------------------------

SELECT throws_ok(
    $$ INSERT INTO sqitch.events
           (event, change_id, change, project, note, requires, conflicts, tags,
            committer_name, committer_email, planned_at, planner_name, planner_email)
       VALUES
           ('invalid_event', 'aaa1111aaa2222aaa3333aaa4444aaa5555aaa66',
            'alpha', 'logtest', '', '{}', '{}', '{}',
            'User', 'u@x.com', NOW(), 'User', 'u@x.com') $$,
    '23514',
    'Invalid event type should fail the CHECK constraint'
);

-- ---------------------------------------------------------------------------
-- Tests: requires, conflicts, tags arrays are correct type and preserved
-- From log.t: format strings use these fields
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT requires FROM sqitch.events
     WHERE change_id = 'bbb1111bbb2222bbb3333bbb4444bbb5555bbb66'
     LIMIT 1) @> ARRAY['alpha'],
    'beta events should preserve requires = {alpha}'
);

SELECT ok(
    (SELECT bool_and(conflicts = '{}') FROM sqitch.events WHERE project = 'logtest'),
    'All events in this fixture should have empty conflicts array'
);

SELECT ok(
    (SELECT bool_and(tags = '{}') FROM sqitch.events WHERE project = 'logtest'),
    'All events in this fixture should have empty tags array'
);

-- ---------------------------------------------------------------------------
-- Tests: fail event is distinct from revert
-- From pg.t: log_fail() records event = 'fail'; no change deleted
-- ---------------------------------------------------------------------------

SELECT ok(
    EXISTS (
        SELECT 1 FROM sqitch.events
        WHERE event = 'fail'
          AND change_id = 'ccc1111ccc2222ccc3333ccc4444ccc5555ccc66'
          AND project = 'logtest'
    ),
    'fail event should be recorded for gamma'
);

-- fail event does NOT create a row in sqitch.changes
-- (gamma never deployed successfully)
SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM sqitch.changes
        WHERE change_id = 'ccc1111ccc2222ccc3333ccc4444ccc5555ccc66'
    ),
    'A failed deploy should NOT appear in sqitch.changes'
);

-- ---------------------------------------------------------------------------
-- Tests: planner info preserved in events
-- From log.t: --format full shows planner name + email
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(planner_name IS NOT NULL AND planner_name <> '')
     FROM sqitch.events WHERE project = 'logtest'),
    'All events should have non-empty planner_name'
);

SELECT ok(
    (SELECT bool_and(planner_email IS NOT NULL AND planner_email <> '')
     FROM sqitch.events WHERE project = 'logtest'),
    'All events should have non-empty planner_email'
);

-- ---------------------------------------------------------------------------
-- Tests: Total event count
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT count(*)::INT FROM sqitch.events WHERE project = 'logtest'),
    5,
    'Should have exactly 5 events total (3 deploy + 1 revert + 1 fail)'
);

-- ---------------------------------------------------------------------------
-- Tests: committer info in events
-- From log.t: format strings %{committer}n, %{committer}e
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(committer_name IS NOT NULL AND committer_name <> '')
     FROM sqitch.events WHERE project = 'logtest'),
    'All events should have non-empty committer_name'
);

-- ---------------------------------------------------------------------------
-- todo: checks requiring live CLI
-- ---------------------------------------------------------------------------

SELECT todo('not yet implemented: sqlever log command output format', 2);
SELECT ok(false, 'sqlever log should output events in reverse chronological order by default');
SELECT ok(false, 'sqlever log --format oneline should produce single-line output per event');
SELECT todo_end();

SELECT todo('not yet implemented: sqitch log --event filter', 2);
SELECT ok(false, 'sqlever log --event deploy should show only deploy events');
SELECT ok(false, 'sqlever log --event revert should show only revert events');
SELECT todo_end();

SELECT * FROM finish();

ROLLBACK;
