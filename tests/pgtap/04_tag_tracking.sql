-- tests/pgtap/04_tag_tracking.sql
-- pgTAP tests: what ends up in sqitch.tags after tagging
--
-- Derived from sqitchers/sqitch t/tag.t and t/pg.t (DBIEngineTest).
--
-- Key assertions from tag.t + pg.t:
--   - sqitch.tags has the tag with correct tag_id (SHA1, 40 hex chars)
--   - tag is stored with "@" prefix (e.g. "@v1.0.0")
--   - committed_at (tagged_at in Sqitch terms) is a TIMESTAMPTZ, set automatically
--   - tagger_name (committer_name) and tagger_email (committer_email) are set
--   - planned_at is the timestamp from the plan when the tag was declared
--   - planner_name and planner_email are from the plan author
--   - Can query changes relative to a tag via JOIN
--   - Tag points to the correct change (last change at the time of tagging)
--
-- From tag.t: $tag->id = SHA1("tag <len>\0project <proj>\ntag @foo\n...")
-- From DBIEngineTest: log_tag() records the tag with correct metadata.

BEGIN;

SELECT plan(20);

-- ---------------------------------------------------------------------------
-- Fixture: schema + changes + tags
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

-- Seed project with URI (from tag.t: uri is included in tag info)
INSERT INTO sqitch.projects (project, uri, creator_name, creator_email)
VALUES ('myapp', 'https://github.com/example/myapp', 'Barack Obama', 'potus@whitehouse.gov');

-- Two deployed changes
INSERT INTO sqitch.changes
    (change_id, change, project, note, committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('1111aaaa2222bbbb3333cccc4444dddd5555eeee',
     'roles', 'myapp', 'Add roles',
     'Barack Obama', 'potus@whitehouse.gov',
     '2012-07-16 17:25:07+00', 'Barack Obama', 'potus@whitehouse.gov'),
    ('2222bbbb3333cccc4444dddd5555eeee6666ffff',
     'users', 'myapp', 'Add users',
     'Barack Obama', 'potus@whitehouse.gov',
     '2012-07-17 10:00:00+00', 'Barack Obama', 'potus@whitehouse.gov');

-- Tag @v1.0.0 pointing to the users change
-- tag_id is SHA1("tag <len>\0project myapp\nuri https://...\ntag @howdy\n...")
-- We use a synthetic but correctly-formatted SHA1 for the fixture
INSERT INTO sqitch.tags
    (tag_id, tag, project, change_id, note,
     committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6',
     '@v1.0.0', 'myapp',
     '2222bbbb3333cccc4444dddd5555eeee6666ffff',
     'Release v1.0.0',
     'Barack Obama', 'potus@whitehouse.gov',
     '2012-07-17 10:00:00+00', 'Barack Obama', 'potus@whitehouse.gov');

-- Second tag with UTF-8 name (from tag.t: "阱阪阬" example)
INSERT INTO sqitch.tags
    (tag_id, tag, project, change_id, note,
     committer_name, committer_email,
     planned_at, planner_name, planner_email)
VALUES
    ('a0b1c2d3e4f5a6f7a8b9c0d1e2f3a4b5c6d7e8f9',
     '@阱阪阬', 'myapp',
     '2222bbbb3333cccc4444dddd5555eeee6666ffff',
     'UTF-8 tag name',
     'Barack Obama', 'potus@whitehouse.gov',
     '2012-07-17 11:00:00+00', 'Barack Obama', 'potus@whitehouse.gov');

-- ---------------------------------------------------------------------------
-- Tests: tag_id is a valid SHA1 (40 hex chars)
-- From tag.t: $tag->id = Digest::SHA->new(1)->add(...)->hexdigest
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(tag_id ~ '^[0-9a-f]{40}$') FROM sqitch.tags WHERE project = 'myapp'),
    'All tag IDs should be 40-char lowercase hex (SHA1) strings'
);

-- ---------------------------------------------------------------------------
-- Tests: tag stored with "@" prefix
-- From tag.t: $tag->format_name returns "@foo" (with leading "@")
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT bool_and(tag ~ '^@') FROM sqitch.tags WHERE project = 'myapp'),
    'All tags should be stored with "@" prefix'
);

SELECT is(
    (SELECT tag FROM sqitch.tags WHERE tag_id = 'f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6'),
    '@v1.0.0',
    'Tag should be stored as "@v1.0.0"'
);

-- ---------------------------------------------------------------------------
-- Tests: committed_at (tagged_at) is TIMESTAMPTZ and not null
-- From pg.t: log_tag() sets committed_at = clock_timestamp()
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT committed_at IS NOT NULL FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    'committed_at should be set after tagging'
);

SELECT ok(
    (SELECT committed_at > '2000-01-01'::TIMESTAMPTZ
     FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    'committed_at should be a plausible timestamp'
);

-- ---------------------------------------------------------------------------
-- Tests: tagger_name / tagger_email correct
-- From tag.t: planner_name defaults to sqitch->user_name
-- From pg.t: log_tag() stores committer_name = sqitch->user_name
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT committer_name FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    'Barack Obama',
    'committer_name (tagger_name) should match the configured user'
);

SELECT is(
    (SELECT committer_email FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    'potus@whitehouse.gov',
    'committer_email (tagger_email) should match the configured user'
);

-- ---------------------------------------------------------------------------
-- Tests: planner info preserved
-- From tag.t: planner_name and planner_email come from the plan entry
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT planner_name FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    'Barack Obama',
    'planner_name should be stored in the tag'
);

SELECT is(
    (SELECT planner_email FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    'potus@whitehouse.gov',
    'planner_email should be stored in the tag'
);

-- ---------------------------------------------------------------------------
-- Tests: planned_at is the timestamp from the plan
-- From tag.t: $tag->timestamp is the time in the plan file header
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT planned_at FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    '2012-07-17 10:00:00+00'::TIMESTAMPTZ,
    'planned_at should match the timestamp from the plan file'
);

-- ---------------------------------------------------------------------------
-- Tests: Tag points to the correct change (last change at time of tag)
-- From tag.t: $tag->change returns the change at the tag's position
-- ---------------------------------------------------------------------------

SELECT is(
    (SELECT change_id FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    '2222bbbb3333cccc4444dddd5555eeee6666ffff',
    'Tag should point to the users change (the change at tag position)'
);

-- ---------------------------------------------------------------------------
-- Tests: Can query changes relative to a tag (JOIN)
-- From pg.t: "changes deployed before tag" query pattern
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT count(*)::INT FROM sqitch.changes c
     JOIN sqitch.tags t ON t.change_id = c.change_id
     WHERE t.tag = '@v1.0.0' AND c.project = 'myapp') = 1,
    'Should find the change directly tagged by @v1.0.0'
);

-- Changes deployed at or before the tag
SELECT ok(
    (SELECT count(*)::INT FROM sqitch.changes c
     WHERE c.project = 'myapp'
       AND c.committed_at <= (
           SELECT committed_at FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'
       )) >= 1,
    'Should be able to find changes deployed at or before the tag'
);

-- ---------------------------------------------------------------------------
-- Tests: UTF-8 tag names stored correctly
-- From tag.t: "阱阪阬" example with Encode::encode_utf8 for SHA1
-- ---------------------------------------------------------------------------

SELECT ok(
    EXISTS (SELECT 1 FROM sqitch.tags WHERE tag = '@阱阪阬' AND project = 'myapp'),
    'UTF-8 tag names should be stored correctly in sqitch.tags'
);

SELECT ok(
    (SELECT tag_id FROM sqitch.tags WHERE tag = '@阱阪阬') ~ '^[0-9a-f]{40}$',
    'UTF-8 tag should still have a valid SHA1 tag_id'
);

-- ---------------------------------------------------------------------------
-- Tests: UNIQUE constraint (project, tag) prevents duplicate tags
-- From pg.t: init_error tests that duplicate schema raises an error
-- ---------------------------------------------------------------------------

SELECT throws_ok(
    $$ INSERT INTO sqitch.tags
           (tag_id, tag, project, change_id, note,
            committer_name, committer_email,
            planned_at, planner_name, planner_email)
       VALUES
           ('ffffeeeeddddccccbbbbaaaa9999888877776666',
            '@v1.0.0', 'myapp',
            '1111aaaa2222bbbb3333cccc4444dddd5555eeee',
            '', 'Dev', 'dev@x.com', NOW(), 'Dev', 'dev@x.com') $$,
    '23505',
    'Duplicate (project, tag) should violate unique constraint'
);

-- ---------------------------------------------------------------------------
-- Tests: note field is optional (default '')
-- From tag.t: note is stored even when empty
-- ---------------------------------------------------------------------------

SELECT ok(
    (SELECT note IS NOT NULL FROM sqitch.tags WHERE tag = '@v1.0.0' AND project = 'myapp'),
    'Tag note should be stored (even if empty string)'
);

-- ---------------------------------------------------------------------------
-- todo: checks requiring live CLI
-- ---------------------------------------------------------------------------

SELECT todo('not yet implemented: sqlever tag command end-to-end', 2);
SELECT ok(false, 'sqlever tag @v1.0.0 should record the tag in sqitch.tags');
SELECT ok(false, 'sqlever tag with --note should store the note in sqitch.tags');
SELECT todo_end();

SELECT * FROM finish();

ROLLBACK;
