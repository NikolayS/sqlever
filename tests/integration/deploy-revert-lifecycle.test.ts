// tests/integration/deploy-revert-lifecycle.test.ts — Full deploy + revert lifecycle
//
// Exercises the complete deploy/revert cycle against a real PostgreSQL database.
// Covers the full issue #141 requirements:
//   1. Deploy 3 migrations (create_users, create_orders with FK, create_indexes with CONCURRENTLY)
//   2. Verify deployed state (tables, indexes, tracking tables)
//   3. Status shows 0 pending
//   4. Verify command passes
//   5. Revert all, verify tables gone and tracking tables updated
//   6. Re-deploy after revert (idempotent)
//   7. Partial deploy (--to) and partial revert (--to)
//
// Prerequisites:
//   - PostgreSQL reachable at localhost:5417 (docker compose up)
//   - Password: test, user: postgres
//
// See: https://github.com/NikolayS/sqlever/issues/141

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { resolve, join } from "node:path";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";

import {
  setupTestDb,
  teardownTestDb,
  queryDb,
  pgUri,
  runSqlever,
  hasPg,
} from "./helpers";

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const FIXTURES_DIR = resolve(
  new URL("../fixtures", import.meta.url).pathname,
);
const PROJECT_DIR = join(FIXTURES_DIR, "deploy-revert-project");

// ---------------------------------------------------------------------------
// Full lifecycle: deploy -> verify -> status -> revert -> re-deploy
// ---------------------------------------------------------------------------

describe.skipIf(!hasPg)("deploy + revert full lifecycle (#141)", () => {
  let dbName: string;
  let tmpDir: string;

  beforeEach(async () => {
    dbName = await setupTestDb();
    tmpDir = await mkdtemp(join(tmpdir(), "dr-lifecycle-"));
    await cp(PROJECT_DIR, tmpDir, { recursive: true });
  });

  afterEach(async () => {
    await teardownTestDb(dbName);
    await rm(tmpDir, { recursive: true, force: true });
  });

  // -------------------------------------------------------------------------
  // Step 1-2: Deploy all 3 changes, verify database state
  // -------------------------------------------------------------------------

  test("deploy creates tables, indexes, and tracking entries", async () => {
    const dbUri = pgUri(dbName);

    // Deploy all changes
    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(deployResult.exitCode).toBe(0);
    expect(deployResult.stdout).toContain("Deploying change: create_users");
    expect(deployResult.stdout).toContain("Deploying change: create_orders");
    expect(deployResult.stdout).toContain("Deploying change: create_indexes");

    // Verify: users table exists with correct columns
    const userCols = await queryDb<{ column_name: string; data_type: string }>(
      dbName,
      `select column_name, data_type
       from information_schema.columns
       where table_schema = 'public' and table_name = 'users'
       order by ordinal_position`,
    );
    const userColNames = userCols.map((c) => c.column_name);
    expect(userColNames).toEqual(["id", "username", "email", "created_at"]);

    // Verify: orders table exists with FK to users
    const orderCols = await queryDb<{ column_name: string }>(
      dbName,
      `select column_name
       from information_schema.columns
       where table_schema = 'public' and table_name = 'orders'
       order by ordinal_position`,
    );
    expect(orderCols.map((c) => c.column_name)).toEqual([
      "id", "user_id", "total_cents", "status", "created_at",
    ]);

    // Verify FK constraint exists
    const fks = await queryDb<{ constraint_name: string }>(
      dbName,
      `select constraint_name
       from information_schema.table_constraints
       where table_name = 'orders'
         and constraint_type = 'FOREIGN KEY'`,
    );
    expect(fks).toHaveLength(1);

    // Verify: indexes created by create_indexes (CONCURRENTLY)
    const indexes = await queryDb<{ indexname: string }>(
      dbName,
      `select indexname from pg_indexes
       where schemaname = 'public'
         and indexname in ('idx_users_email', 'idx_orders_user_id', 'idx_orders_status')
       order by indexname`,
    );
    expect(indexes).toHaveLength(3);
    expect(indexes.map((i) => i.indexname)).toEqual([
      "idx_orders_status",
      "idx_orders_user_id",
      "idx_users_email",
    ]);

    // Verify: sqitch.changes has 3 entries
    const changes = await queryDb<{ change: string; project: string }>(
      dbName,
      `select change, project from sqitch.changes
       where project = 'deploy_revert_test'
       order by committed_at`,
    );
    expect(changes).toHaveLength(3);
    expect(changes.map((c) => c.change)).toEqual([
      "create_users",
      "create_orders",
      "create_indexes",
    ]);

    // Verify: sqitch.events has 3 deploy events
    const events = await queryDb<{ event: string; change: string }>(
      dbName,
      `select event, change from sqitch.events
       where project = 'deploy_revert_test'
       order by committed_at`,
    );
    expect(events).toHaveLength(3);
    for (const ev of events) {
      expect(ev.event).toBe("deploy");
    }
    expect(events.map((e) => e.change)).toEqual([
      "create_users",
      "create_orders",
      "create_indexes",
    ]);

    // Verify: sqitch.dependencies tracks the dependency graph
    const deps = await queryDb<{
      change_id: string;
      dependency: string;
      type: string;
    }>(
      dbName,
      `select d.change_id, d.dependency, d.type
       from sqitch.dependencies as d
       join sqitch.changes as c on c.change_id = d.change_id
       where c.project = 'deploy_revert_test'
       order by c.committed_at, d.dependency`,
    );
    // create_orders requires create_users (1 dep)
    // create_indexes requires create_users, create_orders (2 deps)
    // Total: 3
    expect(deps).toHaveLength(3);
    const requireDeps = deps.filter((d) => d.type === "require");
    expect(requireDeps).toHaveLength(3);
  }, 30_000);

  // -------------------------------------------------------------------------
  // Step 3: Status shows 0 pending
  // -------------------------------------------------------------------------

  test("status shows 0 pending after full deploy", async () => {
    const dbUri = pgUri(dbName);

    // Deploy first
    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(deployResult.exitCode).toBe(0);

    // Status in JSON format
    const statusResult = await runSqlever(
      ["status", "--db-uri", dbUri, "--top-dir", tmpDir, "--format", "json"],
      { cwd: tmpDir },
    );
    expect(statusResult.exitCode).toBe(0);

    const parsed = JSON.parse(statusResult.stdout);
    expect(parsed.project).toBe("deploy_revert_test");
    expect(parsed.deployed_count).toBe(3);
    expect(parsed.pending_count).toBe(0);
    expect(parsed.pending_changes).toEqual([]);
    expect(parsed.last_deployed).toBeDefined();
    expect(parsed.last_deployed.change).toBe("create_indexes");
  }, 30_000);

  // -------------------------------------------------------------------------
  // Step 4: Verify command passes
  // -------------------------------------------------------------------------

  test("verify passes after deploy", async () => {
    const dbUri = pgUri(dbName);

    // Deploy first
    await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );

    // Verify
    const verifyResult = await runSqlever(
      ["verify", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );
    expect(verifyResult.exitCode).toBe(0);
    expect(verifyResult.stdout).toContain("create_users");
    expect(verifyResult.stdout).toContain("create_orders");
    expect(verifyResult.stdout).toContain("create_indexes");
    expect(verifyResult.stdout).toContain("3 passed");
  }, 30_000);

  // -------------------------------------------------------------------------
  // Step 5: Revert all, verify tables gone and tracking tables updated
  // -------------------------------------------------------------------------

  test("revert removes all tables and updates tracking", async () => {
    const dbUri = pgUri(dbName);

    // Deploy first
    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(deployResult.exitCode).toBe(0);

    // Revert all
    const revertResult = await runSqlever(
      ["revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );
    expect(revertResult.exitCode).toBe(0);
    expect(revertResult.stdout).toContain("3 change(s) reverted");

    // Verify: tables are gone
    const tables = await queryDb<{ tablename: string }>(
      dbName,
      `select tablename from pg_tables
       where schemaname = 'public'
         and tablename in ('users', 'orders')`,
    );
    expect(tables).toHaveLength(0);

    // Verify: indexes are gone
    const indexes = await queryDb<{ indexname: string }>(
      dbName,
      `select indexname from pg_indexes
       where schemaname = 'public'
         and indexname in ('idx_users_email', 'idx_orders_user_id', 'idx_orders_status')`,
    );
    expect(indexes).toHaveLength(0);

    // Verify: sqitch.changes is empty for this project
    const changes = await queryDb<{ change: string }>(
      dbName,
      `select change from sqitch.changes
       where project = 'deploy_revert_test'`,
    );
    expect(changes).toHaveLength(0);

    // Verify: sqitch.events has both deploy and revert events
    const events = await queryDb<{ event: string; change: string }>(
      dbName,
      `select event, change from sqitch.events
       where project = 'deploy_revert_test'
       order by committed_at`,
    );
    // 3 deploy + 3 revert = 6 events
    expect(events).toHaveLength(6);

    const deployEvents = events.filter((e) => e.event === "deploy");
    const revertEvents = events.filter((e) => e.event === "revert");
    expect(deployEvents).toHaveLength(3);
    expect(revertEvents).toHaveLength(3);

    // Revert events should be in reverse order
    expect(revertEvents.map((e) => e.change)).toEqual([
      "create_indexes",
      "create_orders",
      "create_users",
    ]);
  }, 30_000);

  // -------------------------------------------------------------------------
  // Step 6: Re-deploy after revert (idempotent)
  // -------------------------------------------------------------------------

  test("re-deploy after revert succeeds", async () => {
    const dbUri = pgUri(dbName);

    // Deploy
    const first = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(first.exitCode).toBe(0);

    // Revert all
    const revert = await runSqlever(
      ["revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );
    expect(revert.exitCode).toBe(0);

    // Re-deploy
    const second = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(second.exitCode).toBe(0);
    expect(second.stdout).toContain("Deploying change: create_users");
    expect(second.stdout).toContain("Deploying change: create_orders");
    expect(second.stdout).toContain("Deploying change: create_indexes");

    // Verify tables are back
    const tables = await queryDb<{ tablename: string }>(
      dbName,
      `select tablename from pg_tables
       where schemaname = 'public'
         and tablename in ('users', 'orders')
       order by tablename`,
    );
    expect(tables).toHaveLength(2);
    expect(tables.map((t) => t.tablename)).toEqual(["orders", "users"]);

    // Verify indexes are back
    const indexes = await queryDb<{ indexname: string }>(
      dbName,
      `select indexname from pg_indexes
       where schemaname = 'public'
         and indexname in ('idx_users_email', 'idx_orders_user_id', 'idx_orders_status')
       order by indexname`,
    );
    expect(indexes).toHaveLength(3);

    // Verify tracking state
    const changes = await queryDb<{ change: string }>(
      dbName,
      `select change from sqitch.changes
       where project = 'deploy_revert_test'
       order by committed_at`,
    );
    expect(changes).toHaveLength(3);

    // Second deploy should be a no-op now
    const third = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(third.exitCode).toBe(0);
    expect(third.stdout).toContain("Nothing to deploy");
  }, 60_000);

  // -------------------------------------------------------------------------
  // Step 7: Partial deploy (--to) and partial revert (--to)
  // -------------------------------------------------------------------------

  test("partial deploy with --to deploys subset of changes", async () => {
    const dbUri = pgUri(dbName);

    // Deploy only up to create_orders (skip create_indexes)
    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis", "--to", "create_orders"],
      { cwd: tmpDir },
    );
    expect(deployResult.exitCode).toBe(0);
    expect(deployResult.stdout).toContain("Deploying change: create_users");
    expect(deployResult.stdout).toContain("Deploying change: create_orders");
    expect(deployResult.stdout).not.toContain("create_indexes");

    // Verify: only 2 changes deployed
    const changes = await queryDb<{ change: string }>(
      dbName,
      `select change from sqitch.changes
       where project = 'deploy_revert_test'
       order by committed_at`,
    );
    expect(changes).toHaveLength(2);
    expect(changes.map((c) => c.change)).toEqual([
      "create_users",
      "create_orders",
    ]);

    // Verify: users and orders tables exist, but no custom indexes
    const tables = await queryDb<{ tablename: string }>(
      dbName,
      `select tablename from pg_tables
       where schemaname = 'public'
         and tablename in ('users', 'orders')
       order by tablename`,
    );
    expect(tables).toHaveLength(2);

    const customIndexes = await queryDb<{ indexname: string }>(
      dbName,
      `select indexname from pg_indexes
       where schemaname = 'public'
         and indexname in ('idx_users_email', 'idx_orders_user_id', 'idx_orders_status')`,
    );
    expect(customIndexes).toHaveLength(0);

    // Status should show 1 pending
    const statusResult = await runSqlever(
      ["status", "--db-uri", dbUri, "--top-dir", tmpDir, "--format", "json"],
      { cwd: tmpDir },
    );
    expect(statusResult.exitCode).toBe(0);
    const parsed = JSON.parse(statusResult.stdout);
    expect(parsed.deployed_count).toBe(2);
    expect(parsed.pending_count).toBe(1);
    expect(parsed.pending_changes).toEqual(["create_indexes"]);
  }, 30_000);

  test("partial revert with --to reverts down to specified change", async () => {
    const dbUri = pgUri(dbName);

    // Deploy all 3
    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(deployResult.exitCode).toBe(0);

    // Revert down to create_users (revert create_indexes + create_orders, keep create_users)
    const revertResult = await runSqlever(
      ["revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir, "--to", "create_users"],
      { cwd: tmpDir },
    );
    expect(revertResult.exitCode).toBe(0);
    expect(revertResult.stdout).toContain("2 change(s) reverted");

    // Verify: users table still exists
    const usersTables = await queryDb<{ tablename: string }>(
      dbName,
      `select tablename from pg_tables
       where schemaname = 'public' and tablename = 'users'`,
    );
    expect(usersTables).toHaveLength(1);

    // Verify: orders table is gone
    const ordersTables = await queryDb<{ tablename: string }>(
      dbName,
      `select tablename from pg_tables
       where schemaname = 'public' and tablename = 'orders'`,
    );
    expect(ordersTables).toHaveLength(0);

    // Verify: custom indexes are gone
    const indexes = await queryDb<{ indexname: string }>(
      dbName,
      `select indexname from pg_indexes
       where schemaname = 'public'
         and indexname in ('idx_users_email', 'idx_orders_user_id', 'idx_orders_status')`,
    );
    expect(indexes).toHaveLength(0);

    // Verify: only create_users remains in sqitch.changes
    const changes = await queryDb<{ change: string }>(
      dbName,
      `select change from sqitch.changes
       where project = 'deploy_revert_test'`,
    );
    expect(changes).toHaveLength(1);
    expect(changes[0]!.change).toBe("create_users");

    // Verify: events show 3 deploys + 2 reverts
    const events = await queryDb<{ event: string; change: string }>(
      dbName,
      `select event, change from sqitch.events
       where project = 'deploy_revert_test'
       order by committed_at`,
    );
    expect(events).toHaveLength(5);

    const revertEvents = events.filter((e) => e.event === "revert");
    expect(revertEvents).toHaveLength(2);
    // Revert order: create_indexes first, then create_orders
    expect(revertEvents.map((e) => e.change)).toEqual([
      "create_indexes",
      "create_orders",
    ]);
  }, 30_000);

  // -------------------------------------------------------------------------
  // Incremental deploy after partial revert
  // -------------------------------------------------------------------------

  test("deploy after partial revert deploys only missing changes", async () => {
    const dbUri = pgUri(dbName);

    // Deploy all 3
    await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );

    // Revert down to create_users
    await runSqlever(
      ["revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir, "--to", "create_users"],
      { cwd: tmpDir },
    );

    // Deploy again -- should only deploy create_orders + create_indexes
    const redeployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(redeployResult.exitCode).toBe(0);
    expect(redeployResult.stdout).not.toContain("Deploying change: create_users");
    expect(redeployResult.stdout).toContain("Deploying change: create_orders");
    expect(redeployResult.stdout).toContain("Deploying change: create_indexes");

    // Verify: all 3 changes deployed again
    const changes = await queryDb<{ change: string }>(
      dbName,
      `select change from sqitch.changes
       where project = 'deploy_revert_test'
       order by committed_at`,
    );
    expect(changes).toHaveLength(3);
    expect(changes.map((c) => c.change)).toEqual([
      "create_users",
      "create_orders",
      "create_indexes",
    ]);
  }, 30_000);

  // -------------------------------------------------------------------------
  // Idempotent second deploy is a no-op
  // -------------------------------------------------------------------------

  test("second deploy is a no-op", async () => {
    const dbUri = pgUri(dbName);

    // First deploy
    const first = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(first.exitCode).toBe(0);

    // Second deploy -- no-op
    const second = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    expect(second.exitCode).toBe(0);
    expect(second.stdout).toContain("Nothing to deploy");

    // Verify: still only 3 changes and 3 events (no duplicates)
    const changes = await queryDb<{ change: string }>(
      dbName,
      `select change from sqitch.changes
       where project = 'deploy_revert_test'`,
    );
    expect(changes).toHaveLength(3);

    const events = await queryDb<{ event: string }>(
      dbName,
      `select event from sqitch.events
       where project = 'deploy_revert_test'`,
    );
    expect(events).toHaveLength(3);
  }, 30_000);

  // -------------------------------------------------------------------------
  // Revert when nothing deployed errors (no sqitch schema)
  // -------------------------------------------------------------------------

  test("revert on fresh database (no prior deploy) returns error", async () => {
    const dbUri = pgUri(dbName);

    // On a fresh database with no sqitch schema, revert fails because
    // the tracking tables do not exist yet -- this is expected behavior.
    const result = await runSqlever(
      ["revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );
    expect(result.exitCode).toBe(1);
  }, 30_000);

  // -------------------------------------------------------------------------
  // Revert after deploy + full revert is a no-op
  // -------------------------------------------------------------------------

  test("revert after full revert reports nothing to revert", async () => {
    const dbUri = pgUri(dbName);

    // Deploy all, then revert all
    await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir, "--skip-analysis"],
      { cwd: tmpDir },
    );
    await runSqlever(
      ["revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );

    // Second revert should be a no-op (sqitch schema exists but no changes)
    const result = await runSqlever(
      ["revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Nothing to revert");
  }, 30_000);
});
