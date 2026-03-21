# sqlever

Sqitch-compatible PostgreSQL migration tool with static analysis.

[![CI](https://github.com/NikolayS/sqlever/actions/workflows/ci.yml/badge.svg)](https://github.com/NikolayS/sqlever/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/sqlever)](https://www.npmjs.com/package/sqlever)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## Why sqlever

- **Sqitch compatible** -- drop-in CLI replacement. Existing `sqitch.plan` files, tracking schemas, and workflows work unchanged.
- **Static analysis built in** -- 22 rules catch dangerous migration patterns (lock-heavy DDL, data loss, table rewrites) before deploy, not after.
- **Single binary** -- compiled with Bun, no runtime dependencies. Sub-50ms startup. No Perl, no JVM, no Docker required.
- **100% open source** -- every feature ships under Apache 2.0. No paywalled "Pro" tier for safety rules or CI integrations.

## Quick start

Install:

```bash
npm install -g sqlever
# or
brew install sqlever
```

Create a project, add a migration, deploy, and analyze:

```bash
# Initialize a new project
sqlever init myapp --engine pg

# Add a migration
sqlever add create_users -n "Create users table"

# Edit the generated SQL files
cat > deploy/create_users.sql << 'SQL'
CREATE TABLE users (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
SQL

# Analyze before deploying -- catch problems early
sqlever analyze

# Deploy to the database
sqlever deploy db:pg://localhost/myapp

# Verify the deployment
sqlever verify db:pg://localhost/myapp

# Check status
sqlever status db:pg://localhost/myapp
```

## Commands

All Sqitch commands are supported with identical flags and semantics, plus sqlever extensions.

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
| `sqlever analyze` | Analyze migration SQL for dangerous patterns |

All commands support `--format json` for machine-readable output.

## Analysis rules

`sqlever analyze` runs 22 rules against your migration SQL. Rules are classified as **static** (SQL-only, no database connection needed), **connected** (requires live database), or **hybrid** (static check always runs; connected check refines when a database is available).

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

### Suppressing rules

Per-statement with SQL comments:

```sql
-- sqlever:disable SA010
UPDATE users SET tier = 'free';
-- sqlever:enable SA010
```

Single-line: `UPDATE users SET tier = 'free'; -- sqlever:disable SA010`

Per-file in `sqlever.toml`:

```toml
[analysis.overrides."deploy/backfill_tiers.sql"]
skip = ["SA010"]
```

Globally:

```toml
[analysis]
skip = ["SA002b"]
pg_version = 14
```

## Migration from Sqitch

sqlever reads `sqitch.conf`, `sqitch.plan`, and the `sqitch.*` tracking schema without modification. To switch:

```bash
alias sqitch=sqlever
```

**What works unchanged:**

- All plan file formats, pragmas, and dependency syntax
- Deploy/revert/verify workflows with identical flags
- Tracking schema -- sqlever reads and writes the same `sqitch.changes`, `sqitch.tags`, `sqitch.events` tables
- `--db-uri`, `--target`, `--set`, `--log-only`, `--registry`, and all other standard flags
- `rework`, cross-project dependencies, `@tag` references

**What sqlever adds:**

- `deploy` runs static analysis before executing SQL and blocks on error-severity findings (bypass with `--force`)
- `analyze` command for standalone linting (works without a `sqitch.plan` -- point it at any `.sql` file or directory)
- `--format json` on all commands for CI integration
- `--format github-annotations` and `--format gitlab-codequality` for native CI annotations
- Lock timeout guard auto-prepended before risky DDL (configurable in `sqlever.toml`)

## Comparison

| | sqlever | Sqitch | Atlas | Flyway |
|---|---------|--------|-------|--------|
| Migration style | Imperative (plain SQL) | Imperative (plain SQL) | Declarative + versioned | Sequential numbered files |
| Static analysis | 22 rules, built in | None | ~12 rules (Pro-only for PG) | None |
| PostgreSQL depth | Advisory locks, PgBouncer detection, replication lag monitoring | Basic | Good | Basic |
| Sqitch compatibility | Full | -- | None | None |
| Runtime | Single binary (Bun) | Perl + CPAN | Go binary | JVM |
| License | Apache 2.0 (all features) | MIT | Apache 2.0 (core) + proprietary Pro | Apache 2.0 (Community) |
| Non-transactional DDL | Write-ahead tracking with crash recovery | Manual | `--tx-mode none` (no recovery) | Manual |
| Expand/contract | Planned (v2.0) | None | None | None |
| Batched DML | Planned (v2.1) | None | None | None |

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
pg_version = 14               # minimum PG version to target
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

Run tests:

```bash
bun install
bun test                       # all tests
bun test tests/unit/           # unit tests only
bun test tests/integration/    # integration tests (requires PostgreSQL)
```

Type-check:

```bash
bun x tsc --noEmit
```

Build:

```bash
bun run build                  # produces dist/sqlever
```

## License

[Apache 2.0](LICENSE)
