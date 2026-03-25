# sqlever -- safe Postgres migrations with static analysis

[![CI](https://github.com/NikolayS/sqlever/actions/workflows/ci.yml/badge.svg)](https://github.com/NikolayS/sqlever/actions/workflows/ci.yml) [![npm version](https://img.shields.io/npm/v/sqlever)](https://www.npmjs.com/package/sqlever) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE) [![Bun](https://img.shields.io/badge/bun-1.1%2B-orange.svg)](https://bun.sh)

43 static analysis rules catch dangerous migration patterns -- table rewrites, missing lock timeouts, data loss, non-concurrent index builds -- before they hit production. Full Sqitch compatibility. Single binary. No Perl, no JVM.

<!-- TODO: GIF demo rendered from demos/*.tape with https://github.com/charmbracelet/vhs -->

---

## Table of contents

- [Why sqlever](#why-sqlever)
- [Quick start](#quick-start)
- [Use with your existing tools](#use-with-your-existing-tools)
- [Snapshot includes](#snapshot-includes)
- [Features](#features)
- [Commands](#commands)
- [Analysis rules](#analysis-rules)
- [CI integration](#ci-integration)
- [Comparison](#comparison)
- [Migration from Sqitch](#migration-from-sqitch)
- [Prerequisites](#prerequisites)
- [Distribution](#distribution)
- [Contributing](#contributing)
- [License](#license)

## Why sqlever

- **Static analysis built in** -- 43 rules catch dangerous migration patterns (lock-heavy DDL, data loss, table rewrites) before deploy, with actionable fix suggestions for every finding.
- **Works with any migration tool** -- run `npx sqlever analyze` on Flyway, Rails, Alembic, or raw SQL files. Zero config, zero project setup.
- **Sqitch compatible** -- drop-in CLI replacement. Existing `sqitch.plan` files, tracking schemas, and workflows work unchanged. Byte-identical change IDs verified by oracle tests.
- **Machine-readable output** -- `--format json`, `--format github-annotations`, and `--format gitlab-codequality` for native CI integration.
- **Single binary** -- compiled with Bun. Sub-50ms startup. No Perl, no JVM, no Docker required.
- **Expand/contract and batched DML** -- generate zero-downtime migration pairs with sync triggers. Backfill millions of rows without locking, with replication lag monitoring.
- **100% open source** -- every feature ships under Apache 2.0, including all safety rules and CI integrations.

## Quick start

Install (see [Distribution](#distribution) for all options):

```bash
# Run without installing
npx sqlever --help

# Install globally
npm install -g sqlever

# Or download binary from GitHub Releases
# https://github.com/NikolayS/sqlever/releases
```

Create a project, add a migration, analyze, and deploy:

```bash
# Initialize a new project
sqlever init myapp --engine pg

# Add a migration
sqlever add create_users -n "Create users table"

# Edit the generated deploy script
cat > deploy/create_users.sql << 'SQL'
SET lock_timeout = '5s';
CREATE TABLE users (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
SQL

# Analyze before deploying -- catch problems early
sqlever analyze
# No issues found.

# Deploy to the database
sqlever deploy db:pg://localhost/myapp
# Deploying 1 change(s)...
# [+] create_users -- done (42ms)
# Deploy complete: 1 deployed in 58ms

# Verify the deployment
sqlever verify db:pg://localhost/myapp
```

## Use with your existing tools

sqlever's static analysis works standalone -- no `sqitch.plan` needed, no project setup required. Add it to any CI pipeline in one step, regardless of what migration tool you use.

### For Flyway users

```yaml
# .github/workflows/ci.yml -- add after your Flyway steps
- name: Analyze migrations
  run: npx sqlever analyze sql/ --format github-annotations
```

Keep Flyway for execution. Add sqlever for safety. 43 rules catch dangerous patterns Flyway cannot see.

### For Liquibase users

```yaml
- name: Analyze migrations
  run: npx sqlever analyze src/main/resources/db/changelog/ --format github-annotations
```

Liquibase manages your changelogs. sqlever catches the `ALTER TABLE` without `lock_timeout` that takes down production.

### For Ruby on Rails / ActiveRecord users

```yaml
- name: Analyze migrations
  run: npx sqlever analyze db/migrate/ --format github-annotations
```

`rails db:migrate` does not know that your `ADD COLUMN NOT NULL` will lock the table for 10 minutes. sqlever does.

### For Django users

```yaml
- name: Analyze migrations
  run: npx sqlever analyze myapp/migrations/ --format github-annotations
```

Django generates SQL behind the scenes. Export it with `sqlmigrate`, then analyze. Catch the implicit table rewrite before it hits production.

### For Alembic users

```yaml
- name: Analyze migrations
  run: npx sqlever analyze alembic/versions/ --format github-annotations
```

Alembic handles schema versioning. sqlever handles schema safety.

### For raw SQL / psql users

```bash
npx sqlever analyze my-migration.sql
```

No migration framework? No problem. Point sqlever at any `.sql` file.

### How it works everywhere

- **Zero config** -- works on any `.sql` file, no project setup
- **Zero install** -- `npx` downloads and runs it in one command
- **GitHub Actions** -- `--format github-annotations` shows findings inline in PR diffs
- **GitLab CI** -- `--format gitlab-codequality` feeds the Code Quality widget
- **Exit code 2** on errors -- blocks your CI pipeline automatically

### Example output

Given a typical migration with three common problems:

```sql
-- V42__add_status_to_orders.sql
ALTER TABLE orders ADD COLUMN status text NOT NULL;
CREATE INDEX idx_orders_status ON orders(status);
ALTER TABLE orders ALTER COLUMN total TYPE numeric(12,2);
```

```
$ sqlever analyze V42__add_status_to_orders.sql

  error SA001: Adding NOT NULL column "status" to table "orders" without a DEFAULT
               will fail on populated tables.
    at V42__add_status_to_orders.sql:1:1
    suggestion: Add a DEFAULT value, or add the column as nullable first, backfill,
                then set NOT NULL.

  error SA003: Changing type of column "total" on table "orders" to numeric(12,2)
               may cause a full table rewrite with AccessExclusiveLock.
    at V42__add_status_to_orders.sql:3:50
    suggestion: Consider the expand/contract pattern: add a new column with the new
                type, backfill in batches, then swap.

  warn  SA004: CREATE INDEX "idx_orders_status" on table "orders" without CONCURRENTLY
               takes a ShareLock, blocking writes for the duration.
    at V42__add_status_to_orders.sql:2:52
    suggestion: Use CREATE INDEX CONCURRENTLY to avoid blocking writes.

  warn  SA013: ALTER TABLE on "orders" without a preceding SET lock_timeout.
    at V42__add_status_to_orders.sql:1:1
    suggestion: Add SET lock_timeout = '5s'; before risky DDL to prevent
                runaway lock waits.

2 errors, 4 warnings
```

Every finding is actionable -- each includes a concrete suggestion for how to fix it.

## Snapshot includes

Deploy scripts that use `\i` or `\ir` to include shared SQL files get automatic git-correlated resolution. When sqlever deploys a migration, each `\i` resolves to the file version from when the migration was written, not the current HEAD.

### The problem

A deploy script includes a shared view definition:

```sql
-- deploy/create_schema.sql (written in January)
BEGIN;
CREATE TABLE users (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL,
    phone text
);
\i shared/user_summary.sql
COMMIT;
```

In January, `shared/user_summary.sql` creates a view that includes the `phone` column. By March, a GDPR cleanup removes `phone` from the shared file. A new migration `add_orders` is added in March that also uses `\i shared/user_summary.sql`.

Now you deploy both migrations on a fresh database in March. What happens to `create_schema`?

| Deploy mode | `\i` resolves to | Verify result |
|---|---|---|
| Default (snapshot) | January version of `user_summary.sql` (with `phone`) | PASSES -- view matches the schema the migration created |
| `--no-snapshot` | March version of `user_summary.sql` (without `phone`) | FAILS -- view references `phone` which no longer exists in the shared file's definition |

Without snapshot includes, deploying on a fresh database produces a different result than deploying when the migration was originally written. This is a subtle, dangerous class of bugs that only surfaces on new environments.

### How it works

1. Each migration in `sqitch.plan` has a `planned_at` timestamp.
2. On deploy, sqlever resolves `\i`/`\ir` includes via `git show <commit>:<path>`, using the commit that existed at `planned_at`.
3. The assembled SQL (with inlined historical includes) is piped to psql.
4. Pass `--no-snapshot` to disable this and use current HEAD versions (Sqitch-compatible behavior).

This ensures that deploying historical migrations on a fresh database produces the same result as deploying them when they were originally written -- even if shared files have changed since.

## Features

### Static analysis at deploy time

`sqlever deploy` runs all 43 analysis rules before executing SQL and blocks on error-severity findings. Run standalone with `sqlever analyze` against any `.sql` file or directory -- no `sqitch.plan` required. See the full [analysis rules table](#analysis-rules) below.

### Expand/contract migrations

Generate paired expand + contract changes for zero-downtime schema changes:

```bash
sqlever add rename_users_name --expand \
  --table users --operation rename_col \
  --old-col name --new-col full_name
```

This creates two linked migrations:

- **Expand** (backward-compatible): adds the new column alongside the old one and installs a bidirectional sync trigger with recursion guard.
- **Contract** (after full app rollout): verifies all rows are backfilled, drops the sync trigger, and drops the old column.

### Batched DML

Backfill millions of rows without locking, with replication lag monitoring and backpressure:

```bash
sqlever batch start \
  --table users \
  --set "tier = 'free'" \
  --where "tier IS NULL" \
  --batch-size 5000
```

Uses a PGQ-style 3-partition rotating queue for bloat-free operation. `SELECT ... FOR UPDATE SKIP LOCKED` enables concurrent workers.

### TUI deploy dashboard

When stdout is a TTY, `sqlever deploy` shows a live-updating progress dashboard with per-change status, timing, analysis warnings, and a progress bar. Pipe-friendly plain text output is used automatically when stdout is not a TTY, or when `--no-tui` is passed.

### Lock timeout guard

sqlever auto-prepends `SET lock_timeout` before risky DDL statements to prevent migrations from waiting indefinitely for locks. Configurable in `sqlever.toml`:

```toml
[deploy]
lock_timeout = 5000  # milliseconds
```

If a deploy script already sets `lock_timeout`, the auto-prepend is skipped.

### AI explain and review

```bash
# Plain-English summary of a migration
sqlever explain deploy/add_users.sql

# Structured review for PR comments
sqlever review --format markdown | gh pr comment --body-file -
```

Requires an LLM API key. No telemetry, no hidden network calls -- LLM requests are made only when `explain` or `review` is explicitly invoked.

### Project doctor

`sqlever doctor` validates your project setup in one command: plan file parsing, change ID chain consistency, script file presence, psql metacommand detection, and syntax version checks.

## Commands

Implemented commands with identical flags and semantics to Sqitch, plus sqlever extensions:

| Command | Description |
|---------|-------------|
| `sqlever init` | Initialize project, create `sqitch.conf` and `sqitch.plan` |
| `sqlever add` | Add a new migration change |
| `sqlever deploy` | Deploy changes to a database (runs analysis first) |
| `sqlever revert` | Revert changes from a database |
| `sqlever verify` | Run verify scripts against a database |
| `sqlever status` | Show deployment status |
| `sqlever log` | Show deployment history |
| `sqlever tag` | Tag the current deployment state |
| `sqlever rework` | Rework an existing change |
| `sqlever show` | Display change/tag details or script contents |
| `sqlever plan` | Display plan contents |
| `sqlever diff` | Show pending changes or differences between tags |
| `sqlever analyze` | Analyze migration SQL for dangerous patterns |
| `sqlever doctor` | Validate project setup, plan file, and script consistency |
| `sqlever batch` | Run batched DML with progress, lag monitoring, and backpressure |
| `sqlever explain` | AI-powered plain-English summary of a migration file |
| `sqlever review` | Generate structured review comments from analysis findings |
| `sqlever deploy --dblab` | Deploy against a DBLab thin clone for safe testing |

The following Sqitch commands are recognized but not yet implemented: `rebase`, `bundle`, `checkout`, `upgrade`, `engine`, `target`, `config`.

## Analysis rules

`sqlever analyze` runs 43 rules against your migration SQL. Rules are classified as **static** (SQL-only, no database connection needed), **connected** (requires live database), or **hybrid** (static check always runs; connected check refines when a database is available).

| Rule | Severity | Type | Description |
|------|----------|------|-------------|
| SA001 | error | static | `ADD COLUMN ... NOT NULL` without `DEFAULT` -- fails on populated tables |
| SA002 | error | static | `ADD COLUMN ... DEFAULT <volatile>` -- full table rewrite on all PG versions |
| SA002b | warn | static | `ADD COLUMN ... DEFAULT` on PG < 11 -- table rewrite on older versions |
| SA003 | error | static | `ALTER COLUMN ... TYPE` with unsafe cast -- table rewrite + `AccessExclusiveLock` |
| SA004 | warn | static | `CREATE INDEX` without `CONCURRENTLY` -- blocks writes for duration |
| SA005 | warn | static | `DROP INDEX` without `CONCURRENTLY` -- takes `AccessExclusiveLock` |
| SA006 | warn | static | `DROP COLUMN` -- irreversible data loss |
| SA007 | error | static | `DROP TABLE` -- data loss (exempt in revert scripts) |
| SA008 | warn | static | `TRUNCATE` -- data loss |
| SA009 | warn | hybrid | `ADD FOREIGN KEY` without `NOT VALID` -- holds locks on both tables |
| SA010 | warn | static | `UPDATE` / `DELETE` without `WHERE` -- full table DML |
| SA011 | warn | connected | `UPDATE` / `DELETE` on large table -- needs row count from `pg_class` |
| SA012 | info | static | `ALTER SEQUENCE RESTART` -- may break application assumptions |
| SA013 | warn | static | Missing `SET lock_timeout` before risky DDL |
| SA014 | warn | static | `VACUUM FULL` / `CLUSTER` -- full table lock and rewrite |
| SA015 | warn | static | `ALTER TABLE ... RENAME` -- breaks running applications |
| SA016 | error | static | `ADD CONSTRAINT ... CHECK` without `NOT VALID` -- full table scan under lock |
| SA017 | warn | hybrid | `ALTER COLUMN ... SET NOT NULL` -- table scan on PG < 12; safe with valid CHECK |
| SA018 | warn | hybrid | `ADD PRIMARY KEY` without pre-existing index -- extends lock duration |
| SA019 | warn | static | `REINDEX` without `CONCURRENTLY` -- takes `AccessExclusiveLock` |
| SA020 | error | static | `CONCURRENTLY` inside transactional deploy -- fails at runtime |
| SA021 | warn | static | `LOCK TABLE` -- explicit locking is a code smell in migrations |
| SA022 | error | static | `DROP SCHEMA` -- destroys all objects in the schema (exempt in revert scripts) |
| SA023 | error | static | `DROP DATABASE` -- irreversible destruction of the entire database |
| SA024 | error | static | `DROP ... CASCADE` -- silently destroys dependent objects |
| SA025 | warn | static | `BEGIN` / `START TRANSACTION` inside a migration -- likely a bug, runner already wraps in a transaction |
| SA026 | warn | static | Missing `SET statement_timeout` before long-running DML (`UPDATE`, `DELETE`, `INSERT ... SELECT`) |
| SA027 | warn | static | `ALTER COLUMN ... DROP NOT NULL` -- may break application assumptions |
| SA028 | warn | static | `TRUNCATE ... CASCADE` -- cascades to all referencing tables |
| SA029 | info | static | `serial` / `bigserial` / `smallserial` column type -- prefer IDENTITY columns (default: off) |
| SA030 | warn | static | `ADD UNIQUE` constraint or `CREATE UNIQUE INDEX` -- may fail if duplicates exist |
| SA031 | error | static | `ALTER TYPE ... ADD VALUE` inside a transaction on PG < 12 -- fails at runtime |
| SA032 | warn | static | `BEGIN` without `COMMIT` or `ROLLBACK` -- transaction left open |
| SA033 | info | connected | Missing index on foreign key referencing column -- causes sequential scans on referenced table |
| SA034 | info | static | `CREATE INDEX CONCURRENTLY` can silently produce an INVALID index -- verify `pg_index.indisvalid` |
| SA035 | warn | static | `DROP PRIMARY KEY` constraint -- may break logical replication replica identity |
| SA036 | warn | connected | Large `UPDATE` / `INSERT ... SELECT` without batching -- consider `sqlever batch` |
| SA037 | info | static | `int4` / `integer` primary key -- limited to ~2.1 billion values, prefer `bigint` |
| SA038 | info | static | `varchar(n)` column type -- prefer `text` with a CHECK constraint if length limit is needed |
| SA039 | info | static | `int4` / `int2` primary key column -- prefer `bigint` to avoid capacity issues |
| SA040 | info | static | `serial` / `bigserial` / `smallserial` column type -- prefer `GENERATED ALWAYS AS IDENTITY` |
| SA041 | info | static | `timestamp` without time zone -- prefer `timestamptz` to avoid timezone bugs |
| SA042 | info | static | `CREATE` / `DROP` without `IF NOT EXISTS` / `IF EXISTS` -- prefer idempotent migrations |

### Suppressing rules

Per-statement with SQL comments:

```sql
-- sqlever:disable SA010
UPDATE users SET tier = 'free';
-- sqlever:enable SA010
```

Single-line: `UPDATE users SET tier = 'free'; -- sqlever:disable SA010`

Per-invocation:

```bash
sqlever analyze migration.sql --force-rule SA010
```

Per-file in `sqlever.toml`:

```toml
[analysis.overrides."deploy/backfill_tiers.sql"]
skip = ["SA010"]
```

Globally:

```toml
[analysis]
skip = ["SA002b"]
pg_version = "14"
```

## CI integration

### GitHub Actions

Findings appear as inline annotations directly on the PR diff:

```yaml
name: Migration safety
on: pull_request

jobs:
  analyze:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: npx sqlever analyze migrations/ --format github-annotations
```

### GitLab CI

Findings appear in the Code Quality widget on merge requests:

```yaml
migration-analysis:
  image: node:22
  script:
    - npx sqlever analyze migrations/ --format gitlab-codequality > gl-code-quality-report.json
  artifacts:
    reports:
      codequality: gl-code-quality-report.json
```

### JSON for any CI system

```yaml
- run: npx sqlever analyze migrations/ --format json
```

Returns structured output:

```json
{
  "version": 1,
  "metadata": {
    "files_analyzed": 1,
    "rules_checked": 43,
    "duration_ms": 2
  },
  "findings": [
    {
      "ruleId": "SA001",
      "severity": "error",
      "message": "Adding NOT NULL column ...",
      "location": { "file": "...", "line": 1, "column": 1 },
      "suggestion": "Add a DEFAULT value, or ..."
    }
  ],
  "summary": { "errors": 2, "warnings": 4, "info": 0 }
}
```

## Comparison

| | sqlever | Sqitch | Atlas | Flyway | dbmate |
|---|---------|--------|-------|--------|--------|
| Migration style | Imperative (plain SQL) | Imperative (plain SQL) | Declarative + versioned | Sequential numbered files | Sequential numbered files |
| Static analysis | 43 rules, built in | None | ~12 rules (Pro edition) | None | None |
| CI annotations | GitHub + GitLab native | None | GitHub (Pro) | None | None |
| Postgres depth | Advisory locks, PgBouncer detection, replication lag monitoring | Basic | Good | Basic | Basic |
| Sqitch compatibility | Full | -- | None | None | None |
| TUI deploy dashboard | Built in | None | None | None | None |
| Runtime | Single binary (Bun) | Perl + CPAN | Go binary | JVM | Go binary |
| Expand/contract | Built in (sync triggers, phase tracking) | None | None | None | None |
| Batched DML | Built in (PGQ, lag monitoring, backpressure) | None | None | None | None |
| AI explanations | Built in (`explain`, `review`) | None | None | None | None |
| License | Apache 2.0 (all features) | MIT | Apache 2.0 (core) + proprietary Pro | Apache 2.0 (Community) | MIT |

## Migration from Sqitch

sqlever reads `sqitch.conf`, `sqitch.plan`, and the `sqitch.*` tracking schema without modification. To switch:

```bash
alias sqitch=sqlever
```

**What works unchanged:**

- All plan file formats, pragmas, and dependency syntax
- Deploy/revert/verify workflows with identical flags
- Tracking schema -- sqlever reads and writes the same `sqitch.changes`, `sqitch.tags`, `sqitch.events` tables
- `--db-uri`, `--target`, `--set`, `--registry`, and all other standard flags
- `rework`, cross-project dependencies, `@tag` references
- Merkle tree change ID chain -- sqlever produces byte-identical change IDs, verified by [oracle tests](tests/compat/oracle.test.ts) that deploy the same project with both Sqitch and sqlever and diff every tracking table row
- psql metacommand compatibility (`\i`, `\ir`, `\set`, `\copy`, `\if`/`\endif`) -- migrations execute via psql, not an internal SQL parser

**What sqlever adds:**

- `deploy` runs static analysis before executing SQL and blocks on error-severity findings
- `analyze` command for standalone linting -- works without a `sqitch.plan`, point it at any `.sql` file or directory
- `--format github-annotations` and `--format gitlab-codequality` for native CI annotations
- Snapshot includes for deterministic deploys of shared SQL files
- Lock timeout guard auto-prepended before risky DDL (configurable in `sqlever.toml`)
- Expand/contract migration generator and batched DML
- AI-powered `explain` and `review` commands

## Prerequisites

- **psql** on `PATH` -- sqlever shells out to psql for migration script execution. Any version that supports `--single-transaction` works (9.0+).
- **Postgres 14+** -- tested against PG 14, 15, 16, 17, and 18 in CI.
- **Bun 1.1+** -- required only if running from source or via `bunx`. Not needed for the compiled binary or npm install.

## Distribution

### npm / npx

```bash
# Run without installing
npx sqlever analyze migration.sql

# Install globally
npm install -g sqlever

# Or with Bun
bunx sqlever analyze migration.sql
```

### GitHub releases

Pre-built binaries for 4 platforms are attached to every [GitHub Release](https://github.com/NikolayS/sqlever/releases):

| Binary | Platform |
|--------|----------|
| `sqlever-linux-amd64` | Linux x86_64 |
| `sqlever-linux-arm64` | Linux ARM64 |
| `sqlever-macos-amd64` | macOS x86_64 |
| `sqlever-macos-arm64` | macOS Apple Silicon |

### Build from source

```bash
bun install
bun build src/cli.ts --compile --outfile dist/sqlever
```

The output is a single self-contained binary with no runtime dependencies.

## Configuration

### `sqitch.conf`

Standard Sqitch INI-format configuration. sqlever reads it as-is:

```ini
[core]
    engine = pg
    plan_file = sqitch.plan
    top_dir = .

[engine "pg"]
    target = db:pg://localhost/myapp
    registry = sqitch
```

### `sqlever.toml`

sqlever-specific configuration. Optional -- sensible defaults apply:

```toml
[analysis]
pg_version = "14"             # minimum PG version to target
error_on_warn = false          # treat warnings as errors
skip = []                      # globally skip these rules
max_affected_rows = 10_000     # threshold for SA011

[analysis.rules.SA002b]
severity = "off"               # disable a specific rule

[analysis.overrides."deploy/seed_data.sql"]
skip = ["SA010"]               # suppress per file
```

## Contributing

See [spec/SPEC.md](spec/SPEC.md) for the full design specification.

```bash
bun install

# Run tests
bun test                       # all tests
bun test tests/unit/           # unit tests only (fast, no DB needed)
bun test tests/integration/    # integration tests (requires Postgres via Docker)
bun test tests/compat/         # Sqitch oracle compatibility tests

# Type-check
bun x tsc --noEmit

# Build
bun run build                  # produces dist/sqlever
```

For integration tests, start Postgres first:

```bash
docker compose up -d           # starts PG 17 on port 5417
bun test tests/integration/
docker compose down -v
```

## License

[Apache 2.0](LICENSE)
