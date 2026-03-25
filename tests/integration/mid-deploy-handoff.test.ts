// tests/integration/mid-deploy-handoff.test.ts — REAL-TEST-4
//
// Mid-deploy handoff: simulate Sqitch deploying changes 1-3, then use
// sqlever to deploy changes 4-6. Verifies that sqlever correctly reads
// Sqitch's tracking state and picks up where it left off.
//
// Instead of requiring the sqitch/sqitch Docker image, this test simulates
// Sqitch's deploy by:
//   1. Running deploy SQL scripts via psql (same execution path Sqitch uses)
//   2. Inserting tracking records directly into sqitch.* tables
//
// This is valid because sqlever's compatibility contract (DD3) is defined
// by the sqitch.* table schema, not by Sqitch's implementation details.
//
// Prerequisites:
//   - PostgreSQL reachable at localhost:5417 (docker compose up)
//   - psql available on PATH
//
// See: https://github.com/NikolayS/sqlever/issues/143

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtemp, rm, writeFile, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  setupTestDb,
  teardownTestDb,
  runSqlever,
  queryDb,
  pgUri,
  hasPg,
} from "./helpers";
import { parsePlan } from "../../src/plan/parser";
import { computeScriptHashFromBytes } from "../../src/plan/types";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PROJECT_NAME = "handoff_test";
const NUM_CHANGES = 6;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function makeTempDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "sqlever-midhandoff-"));
}

/**
 * Generate a project with N changes using sqlever init + add.
 * Each change creates a simple table: create table public.t_N (id int).
 */
async function createProject(
  tmpDir: string,
  n: number,
): Promise<void> {
  const initResult = await runSqlever([
    "init", PROJECT_NAME, "--top-dir", tmpDir,
  ]);
  if (initResult.exitCode !== 0) {
    throw new Error(`init failed: ${initResult.stderr}`);
  }

  for (let i = 1; i <= n; i++) {
    const changeName = `change_${String(i).padStart(3, "0")}`;
    const addResult = await runSqlever([
      "add", changeName, "-n", `create table t${i}`, "--top-dir", tmpDir,
    ]);
    if (addResult.exitCode !== 0) {
      throw new Error(`add ${changeName} failed: ${addResult.stderr}`);
    }

    // Write real deploy script
    await writeFile(
      join(tmpDir, "deploy", `${changeName}.sql`),
      `-- Deploy ${changeName}\ncreate table public.t${i} (id int);\n`,
    );

    // Write real revert script
    await writeFile(
      join(tmpDir, "revert", `${changeName}.sql`),
      `-- Revert ${changeName}\ndrop table if exists public.t${i};\n`,
    );

    // Write verify script
    await writeFile(
      join(tmpDir, "verify", `${changeName}.sql`),
      `-- Verify ${changeName}\nselect id from public.t${i} limit 0;\n`,
    );
  }
}

/**
 * Simulate Sqitch deploying changes by:
 *   1. Creating the sqitch registry schema
 *   2. Registering the project
 *   3. Running each deploy script via psql
 *   4. Inserting tracking records into sqitch.changes and sqitch.events
 *
 * The change_ids are read from the parsed plan file so they match exactly
 * what sqlever would compute (both use the same Sqitch-compatible algorithm).
 */
async function simulateSqitchDeploy(
  dbName: string,
  tmpDir: string,
  changeCount: number,
): Promise<void> {
  const dbUri = pgUri(dbName);

  // Parse the plan file to get correct change_ids
  const planContent = await readFile(join(tmpDir, "sqitch.plan"), "utf-8");
  const plan = parsePlan(planContent);

  // Create the sqitch registry schema (same DDL that both Sqitch and sqlever use)
  await queryDb(dbName, `
    create schema if not exists sqitch;

    create table if not exists sqitch.projects (
      project         text        primary key,
      uri             text        null unique,
      created_at      timestamptz not null default clock_timestamp(),
      creator_name    text        not null,
      creator_email   text        not null
    );

    create table if not exists sqitch.releases (
      version         real        primary key,
      installed_at    timestamptz not null default clock_timestamp(),
      installer_name  text        not null,
      installer_email text        not null
    );

    create table if not exists sqitch.changes (
      change_id       text        primary key,
      script_hash     text,
      change          text        not null,
      project         text        not null references sqitch.projects(project) on update cascade,
      note            text        not null default '',
      committed_at    timestamptz not null default clock_timestamp(),
      committer_name  text        not null,
      committer_email text        not null,
      planned_at      timestamptz not null,
      planner_name    text        not null,
      planner_email   text        not null,
      unique (project, script_hash)
    );

    create table if not exists sqitch.tags (
      tag_id          text        primary key,
      tag             text        not null,
      project         text        not null references sqitch.projects(project) on update cascade,
      change_id       text        not null references sqitch.changes(change_id) on update cascade,
      note            text        not null default '',
      committed_at    timestamptz not null default clock_timestamp(),
      committer_name  text        not null,
      committer_email text        not null,
      planned_at      timestamptz not null,
      planner_name    text        not null,
      planner_email   text        not null,
      unique (project, tag)
    );

    create table if not exists sqitch.dependencies (
      change_id     text not null references sqitch.changes(change_id) on update cascade on delete cascade,
      type          text not null,
      dependency    text not null,
      dependency_id text null references sqitch.changes(change_id) on update cascade,
      primary key (change_id, dependency)
    );

    create table if not exists sqitch.events (
      event           text        not null check (event in ('deploy', 'revert', 'fail', 'merge')),
      change_id       text        not null,
      change          text        not null,
      project         text        not null references sqitch.projects(project) on update cascade,
      note            text        not null default '',
      requires        text[]      not null default '{}',
      conflicts       text[]      not null default '{}',
      tags            text[]      not null default '{}',
      committed_at    timestamptz not null default clock_timestamp(),
      committer_name  text        not null,
      committer_email text        not null,
      planned_at      timestamptz not null,
      planner_name    text        not null,
      planner_email   text        not null,
      primary key (change_id, committed_at)
    );
  `);

  // Register the project (simulating what Sqitch does)
  await queryDb(
    dbName,
    `insert into sqitch.projects (project, uri, creator_name, creator_email)
     values ($1, $2, $3, $4)`,
    [PROJECT_NAME, null, "Sqitch Deployer", "sqitch@test.local"],
  );

  // Deploy each change
  for (let i = 0; i < changeCount; i++) {
    const change = plan.changes[i]!;
    const changeName = change.name;

    // Run the deploy script via psql (same execution path Sqitch uses -- DD12)
    const deployScript = join(tmpDir, "deploy", `${changeName}.sql`);
    const proc = Bun.spawn(["psql", dbUri, "-f", deployScript], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const exitCode = await proc.exited;
    if (exitCode !== 0) {
      const stderr = await new Response(proc.stderr).text();
      throw new Error(`psql deploy of ${changeName} failed: ${stderr}`);
    }

    // Compute script hash (same algorithm as sqlever)
    const scriptContent = await readFile(deployScript);
    const scriptHash = computeScriptHashFromBytes(scriptContent);

    // Insert tracking record into sqitch.changes
    await queryDb(
      dbName,
      `insert into sqitch.changes
        (change_id, script_hash, change, project, note,
         committer_name, committer_email, planned_at, planner_name, planner_email)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        change.change_id,
        scriptHash,
        changeName,
        PROJECT_NAME,
        change.note,
        "Sqitch Deployer",
        "sqitch@test.local",
        new Date(change.planned_at),
        change.planner_name,
        change.planner_email,
      ],
    );

    // Insert tracking record into sqitch.events
    await queryDb(
      dbName,
      `insert into sqitch.events
        (event, change_id, change, project, note,
         requires, conflicts, tags,
         committer_name, committer_email, planned_at, planner_name, planner_email)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
      [
        "deploy",
        change.change_id,
        changeName,
        PROJECT_NAME,
        change.note,
        change.requires,
        change.conflicts,
        [],
        "Sqitch Deployer",
        "sqitch@test.local",
        new Date(change.planned_at),
        change.planner_name,
        change.planner_email,
      ],
    );
  }
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe.skipIf(!hasPg)("integration: mid-deploy handoff (REAL-TEST-4)", () => {
  let tmpDir: string;
  let dbName: string;

  beforeEach(async () => {
    tmpDir = await makeTempDir();
    dbName = await setupTestDb();
  });

  afterEach(async () => {
    await teardownTestDb(dbName);
    await rm(tmpDir, { recursive: true, force: true });
  });

  // -------------------------------------------------------------------------
  // Core handoff test: Sqitch deploys 1-3, sqlever deploys 4-6
  // -------------------------------------------------------------------------

  test("Sqitch deploys changes 1-3, sqlever deploys changes 4-6", async () => {
    // 1. Create project with 6 changes
    await createProject(tmpDir, NUM_CHANGES);

    const dbUri = pgUri(dbName);

    // 2. Simulate Sqitch deploying changes 1-3
    await simulateSqitchDeploy(dbName, tmpDir, 3);

    // 3. Verify Sqitch-deployed state: 3 changes in tracking tables
    const changesAfterSqitch = await queryDb<{ change: string }>(
      dbName,
      "select change from sqitch.changes order by committed_at asc",
    );
    expect(changesAfterSqitch).toHaveLength(3);
    for (let i = 1; i <= 3; i++) {
      expect(changesAfterSqitch[i - 1]!.change).toBe(
        `change_${String(i).padStart(3, "0")}`,
      );
    }

    // Verify 3 tables were created by psql
    const tablesAfterSqitch = await queryDb<{ tablename: string }>(
      dbName,
      `select tablename from pg_tables
       where schemaname = 'public' and tablename like 't%'
       order by tablename`,
    );
    expect(tablesAfterSqitch).toHaveLength(3);

    // 4. Deploy remaining changes 4-6 with sqlever
    const sqleverResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
    );

    expect(sqleverResult.exitCode).toBe(0);
    // sqlever should only deploy 3 new changes, not all 6
    expect(sqleverResult.stdout).toContain("change_004");
    expect(sqleverResult.stdout).toContain("change_005");
    expect(sqleverResult.stdout).toContain("change_006");
    expect(sqleverResult.stdout).not.toContain("Deploying change: change_001");
    expect(sqleverResult.stdout).not.toContain("Deploying change: change_002");
    expect(sqleverResult.stdout).not.toContain("Deploying change: change_003");

    // 5. Verify all 6 changes are in sqitch.changes
    const allChanges = await queryDb<{ change: string; project: string }>(
      dbName,
      "select change, project from sqitch.changes order by committed_at asc",
    );
    expect(allChanges).toHaveLength(6);
    for (let i = 1; i <= 6; i++) {
      const expected = `change_${String(i).padStart(3, "0")}`;
      expect(allChanges[i - 1]!.change).toBe(expected);
      expect(allChanges[i - 1]!.project).toBe(PROJECT_NAME);
    }

    // 6. Verify all 6 events are deploy events
    const allEvents = await queryDb<{ event: string; change: string }>(
      dbName,
      `select event, change from sqitch.events
       where project = $1
       order by committed_at asc`,
      [PROJECT_NAME],
    );
    expect(allEvents).toHaveLength(6);
    for (let i = 0; i < 6; i++) {
      expect(allEvents[i]!.event).toBe("deploy");
      expect(allEvents[i]!.change).toBe(
        `change_${String(i + 1).padStart(3, "0")}`,
      );
    }

    // 7. Verify all 6 tables exist in the database
    const allTables = await queryDb<{ tablename: string }>(
      dbName,
      `select tablename from pg_tables
       where schemaname = 'public' and tablename like 't%'
       order by tablename`,
    );
    expect(allTables).toHaveLength(6);
    for (let i = 1; i <= 6; i++) {
      expect(allTables[i - 1]!.tablename).toBe(`t${i}`);
    }

    // 8. Verify committer attribution: first 3 by Sqitch, last 3 by sqlever
    //    Deploy resolves committer from SQITCH_FULLNAME > USER env, so we
    //    just verify the Sqitch-simulated entries have "Sqitch Deployer"
    //    and the sqlever-deployed entries have a different committer.
    const committers = await queryDb<{
      change: string;
      committer_name: string;
    }>(
      dbName,
      `select change, committer_name from sqitch.changes
       order by committed_at asc`,
    );
    for (let i = 0; i < 3; i++) {
      expect(committers[i]!.committer_name).toBe("Sqitch Deployer");
    }
    // sqlever-deployed changes should NOT have "Sqitch Deployer"
    for (let i = 3; i < 6; i++) {
      expect(committers[i]!.committer_name).not.toBe("Sqitch Deployer");
      // Committer name should be non-empty
      expect(committers[i]!.committer_name.length).toBeGreaterThan(0);
    }
  }, 30_000);

  // -------------------------------------------------------------------------
  // Verify sqlever status sees all changes after handoff
  // -------------------------------------------------------------------------

  test("sqlever status reports correct state after handoff deploy", async () => {
    await createProject(tmpDir, NUM_CHANGES);

    const dbUri = pgUri(dbName);

    // Simulate Sqitch deploying 1-3, then sqlever deploys 4-6
    await simulateSqitchDeploy(dbName, tmpDir, 3);

    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
    );
    expect(deployResult.exitCode).toBe(0);

    // Status in JSON format should show 6 deployed, 0 pending
    const statusResult = await runSqlever(
      ["status", "--db-uri", dbUri, "--top-dir", tmpDir, "--format", "json"],
    );
    expect(statusResult.exitCode).toBe(0);

    const parsed = JSON.parse(statusResult.stdout);
    expect(parsed.project).toBe(PROJECT_NAME);
    expect(parsed.deployed_count).toBe(6);
    expect(parsed.pending_count).toBe(0);
    expect(parsed.pending_changes).toEqual([]);
    expect(parsed.last_deployed).toBeDefined();
    expect(parsed.last_deployed.change).toBe("change_006");
  }, 30_000);

  // -------------------------------------------------------------------------
  // Second deploy after handoff is a no-op
  // -------------------------------------------------------------------------

  test("second deploy after handoff is a no-op", async () => {
    await createProject(tmpDir, NUM_CHANGES);

    const dbUri = pgUri(dbName);

    // Simulate Sqitch deploying 1-3, then sqlever deploys 4-6
    await simulateSqitchDeploy(dbName, tmpDir, 3);

    const firstDeploy = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
    );
    expect(firstDeploy.exitCode).toBe(0);

    // Second deploy should be a no-op
    const secondDeploy = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
    );
    expect(secondDeploy.exitCode).toBe(0);
    expect(secondDeploy.stdout).toContain("Nothing to deploy");

    // Still 6 changes
    const changes = await queryDb(
      dbName,
      "select change from sqitch.changes order by committed_at",
    );
    expect(changes).toHaveLength(6);

    // Still 6 deploy events (no duplicates)
    const events = await queryDb(
      dbName,
      `select event from sqitch.events where project = $1`,
      [PROJECT_NAME],
    );
    expect(events).toHaveLength(6);
  }, 30_000);

  // -------------------------------------------------------------------------
  // Verify after handoff passes
  // -------------------------------------------------------------------------

  test("verify passes after handoff deploy", async () => {
    await createProject(tmpDir, NUM_CHANGES);

    const dbUri = pgUri(dbName);

    // Simulate Sqitch deploying 1-3, then sqlever deploys 4-6
    await simulateSqitchDeploy(dbName, tmpDir, 3);

    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
    );
    expect(deployResult.exitCode).toBe(0);

    // Verify all 6 changes
    const verifyResult = await runSqlever(
      ["verify", "--db-uri", dbUri, "--top-dir", tmpDir],
    );
    expect(verifyResult.exitCode).toBe(0);
  }, 30_000);

  // -------------------------------------------------------------------------
  // change_id values match between Sqitch-inserted and sqlever-inserted
  // -------------------------------------------------------------------------

  test("change_ids from plan file match those in tracking tables", async () => {
    await createProject(tmpDir, NUM_CHANGES);

    const dbUri = pgUri(dbName);

    // Parse the plan to get expected change_ids
    const planContent = await readFile(join(tmpDir, "sqitch.plan"), "utf-8");
    const plan = parsePlan(planContent);

    // Simulate Sqitch deploying 1-3, then sqlever deploys 4-6
    await simulateSqitchDeploy(dbName, tmpDir, 3);

    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
    );
    expect(deployResult.exitCode).toBe(0);

    // All 6 change_ids in the database should match those from the plan
    const dbChanges = await queryDb<{ change_id: string; change: string }>(
      dbName,
      "select change_id, change from sqitch.changes order by committed_at asc",
    );
    expect(dbChanges).toHaveLength(6);

    for (let i = 0; i < 6; i++) {
      expect(dbChanges[i]!.change_id).toBe(plan.changes[i]!.change_id);
      expect(dbChanges[i]!.change).toBe(plan.changes[i]!.name);
    }
  }, 30_000);

  // -------------------------------------------------------------------------
  // Revert after handoff works correctly
  // -------------------------------------------------------------------------

  test("revert after handoff removes all 6 changes", async () => {
    await createProject(tmpDir, NUM_CHANGES);

    const dbUri = pgUri(dbName);

    // Simulate Sqitch deploying 1-3, then sqlever deploys 4-6
    await simulateSqitchDeploy(dbName, tmpDir, 3);

    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
    );
    expect(deployResult.exitCode).toBe(0);

    // Revert all changes
    const revertResult = await runSqlever(
      ["revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir],
    );
    expect(revertResult.exitCode).toBe(0);

    // No changes should remain in sqitch.changes
    const changes = await queryDb(
      dbName,
      `select change from sqitch.changes where project = $1`,
      [PROJECT_NAME],
    );
    expect(changes).toHaveLength(0);

    // All 6 tables should be dropped
    const tables = await queryDb<{ tablename: string }>(
      dbName,
      `select tablename from pg_tables
       where schemaname = 'public' and tablename like 't%'`,
    );
    expect(tables).toHaveLength(0);

    // Events should include 6 deploy + 6 revert events = 12 total
    const events = await queryDb<{ event: string }>(
      dbName,
      `select event from sqitch.events where project = $1`,
      [PROJECT_NAME],
    );
    expect(events).toHaveLength(12);
    const deployEvents = events.filter((e) => e.event === "deploy");
    const revertEvents = events.filter((e) => e.event === "revert");
    expect(deployEvents).toHaveLength(6);
    expect(revertEvents).toHaveLength(6);
  }, 30_000);
});
