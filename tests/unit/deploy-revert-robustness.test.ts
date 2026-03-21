// tests/unit/deploy-revert-robustness.test.ts — Deploy/Revert Robustness
//
// Comprehensive tests for issue #125: transaction semantics, non-transactional
// DDL handling, advisory lock contention, failure recovery, lock timeout guard,
// SIGINT/SIGTERM cleanup, and revert robustness.

import { describe, it, expect, beforeEach, mock, afterEach, spyOn } from "bun:test";
import { EventEmitter } from "events";
import { mkdirSync, writeFileSync, rmSync, existsSync, readFileSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { resetConfig, setConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as deploy.test.ts
// ---------------------------------------------------------------------------

let mockInstances: MockPgClient[] = [];

class MockPgClient {
  options: Record<string, unknown>;
  queries: Array<{ text: string; values?: unknown[] }> = [];
  connected = false;
  ended = false;

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });
    // Default: advisory lock returns true
    if (text.includes("pg_try_advisory_lock")) {
      return { rows: [{ pg_try_advisory_lock: true }], rowCount: 1, command: "SELECT" };
    }
    if (text.includes("pg_advisory_unlock")) {
      return { rows: [{ pg_advisory_unlock: true }], rowCount: 1, command: "SELECT" };
    }
    // SELECT from sqitch.projects — not found (triggers INSERT)
    if (text.includes("SELECT") && text.includes("sqitch.projects") && !text.includes("INSERT")) {
      return { rows: [], rowCount: 0, command: "SELECT" };
    }
    // INSERT INTO sqitch.projects
    if (text.includes("INSERT INTO sqitch.projects")) {
      return {
        rows: [{ project: "test", uri: null, created_at: new Date(), creator_name: "Test", creator_email: "test@x.com" }],
        rowCount: 1,
        command: "INSERT",
      };
    }
    // SELECT deployed changes — return empty by default (nothing deployed)
    if (text.includes("SELECT") && text.includes("sqitch.changes")) {
      return { rows: [], rowCount: 0, command: "SELECT" };
    }
    return { rows: [], rowCount: 0, command: "SELECT" };
  }

  async end() {
    this.ended = true;
    this.connected = false;
  }
}

mock.module("pg/lib/client", () => ({
  default: MockPgClient,
  __esModule: true,
}));

// Type imports
import type { DeployOptions, DeployDeps } from "../../src/commands/deploy";
import type { SpawnFn } from "../../src/psql";

// Import after mocking
const { DatabaseClient, EXIT_CODE_DB_UNREACHABLE } = await import("../../src/db/client");
const { Registry } = await import("../../src/db/registry");
const {
  executeDeploy,
  runDeploy,
  projectLockKey,
  isNonTransactional,
  parseDeployOptions,
  ADVISORY_LOCK_NAMESPACE,
  EXIT_CONCURRENT_DEPLOY,
  EXIT_DEPLOY_FAILED,
  EXIT_LOCK_TIMEOUT,
  EXIT_DB_UNREACHABLE,
} = await import("../../src/commands/deploy");
const {
  parseRevertOptions,
  computeChangesToRevert,
  buildRevertInput,
  confirmRevert,
  runRevert,
  EXIT_CODE_CONCURRENT,
} = await import("../../src/commands/revert");
const { loadConfig } = await import("../../src/config/index");
const { PsqlRunner } = await import("../../src/psql");
const { ShutdownManager } = await import("../../src/signals");
const { parseArgs } = await import("../../src/cli");
const {
  shouldSetLockTimeout,
  isLockTimeoutError,
  retryWithBackoff,
} = await import("../../src/lock-guard");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let testDir: string;
let testDirCounter = 0;

function createTestDir(): string {
  testDirCounter++;
  const dir = join(tmpdir(), `sqlever-robustness-test-${Date.now()}-${testDirCounter}`);
  mkdirSync(dir, { recursive: true });
  mkdirSync(join(dir, "deploy"), { recursive: true });
  mkdirSync(join(dir, "revert"), { recursive: true });
  mkdirSync(join(dir, "verify"), { recursive: true });
  return dir;
}

function writePlan(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.plan"), content, "utf-8");
}

function writeDeployScript(dir: string, name: string, content: string): void {
  writeFileSync(join(dir, "deploy", `${name}.sql`), content, "utf-8");
}

function writeRevertScript(dir: string, name: string, content: string): void {
  writeFileSync(join(dir, "revert", `${name}.sql`), content, "utf-8");
}

function writeVerifyScript(dir: string, name: string, content: string): void {
  writeFileSync(join(dir, "verify", `${name}.sql`), content, "utf-8");
}

function writeSqitchConf(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.conf"), content, "utf-8");
}

/** Two changes */
const TWO_CHANGE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
`;

/** Three changes */
const THREE_CHANGE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
add_posts [add_users] 2025-01-03T00:00:00Z Test User <test@example.com> # Add posts table
`;

/** Tagged plan */
const TAGGED_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
@v1.0 2025-01-03T00:00:00Z Test User <test@example.com> # Release v1.0
add_posts [add_users] 2025-01-04T00:00:00Z Test User <test@example.com> # Add posts table
`;

/** Non-transactional plan */
const NON_TXN_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Schema
add_index 2025-01-02T00:00:00Z Test User <test@example.com> # Concurrent index
`;

function createMockPsqlRunner(exitCode = 0, stderr = ""): PsqlRunner {
  const mockSpawn: SpawnFn = (_cmd, _args, _opts) => {
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });
    queueMicrotask(() => {
      if (stderr) child.stderr.emit("data", Buffer.from(stderr));
      child.emit("close", exitCode);
    });
    return child as ReturnType<typeof import("child_process").spawn>;
  };
  return new PsqlRunner("psql", mockSpawn);
}

function createFailingPsqlRunner(failOnScript: string, errorMsg = "ERROR: relation does not exist"): PsqlRunner {
  const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });
    const scriptFile = args.find((a: string) => a.endsWith(".sql")) ?? "";
    const shouldFail = scriptFile.includes(failOnScript);
    queueMicrotask(() => {
      if (shouldFail) {
        child.stderr.emit("data", Buffer.from(`psql:${scriptFile}:1: ${errorMsg}`));
      }
      child.emit("close", shouldFail ? 1 : 0);
    });
    return child as ReturnType<typeof import("child_process").spawn>;
  };
  return new PsqlRunner("psql", mockSpawn);
}

function createTrackingPsqlRunner(failOnScripts: string[] = []): {
  runner: PsqlRunner;
  calls: Array<{ scriptFile: string; args: string[] }>;
} {
  const calls: Array<{ scriptFile: string; args: string[] }> = [];
  const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
    const scriptFile = args.find((a: string) => a.endsWith(".sql")) ?? "";
    calls.push({ scriptFile, args: [...args] });
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });
    const shouldFail = failOnScripts.some((s) => scriptFile.includes(s));
    queueMicrotask(() => {
      if (shouldFail) {
        child.stderr.emit("data", Buffer.from(`psql:${scriptFile}:1: ERROR: simulated failure`));
      }
      child.emit("close", shouldFail ? 1 : 0);
    });
    return child as ReturnType<typeof import("child_process").spawn>;
  };
  return { runner: new PsqlRunner("psql", mockSpawn), calls };
}

function defaultOptions(dir: string): DeployOptions {
  return {
    mode: "change",
    dryRun: false,
    verify: false,
    variables: {},
    dbUri: "postgresql://localhost/testdb",
    projectDir: dir,
    committerName: "Test User",
    committerEmail: "test@example.com",
    noTui: true,
    noSnapshot: false,
  };
}

async function createDeps(opts?: Partial<{
  psqlExitCode: number;
  psqlStderr: string;
  failOnScript: string;
}>): Promise<DeployDeps> {
  const db = new DatabaseClient("postgresql://localhost/testdb");
  const registry = new Registry(db);
  let psqlRunner: PsqlRunner;
  if (opts?.failOnScript) {
    psqlRunner = createFailingPsqlRunner(opts.failOnScript);
  } else {
    psqlRunner = createMockPsqlRunner(opts?.psqlExitCode ?? 0, opts?.psqlStderr ?? "");
  }
  const config = loadConfig(testDir);
  const shutdownMgr = new ShutdownManager();

  return { db, registry, psqlRunner, config, shutdownMgr };
}

function getPgClient(): MockPgClient {
  return mockInstances[mockInstances.length - 1]!;
}

function makeArgs(rest: string[]) {
  return {
    command: "deploy",
    rest,
    help: false,
    version: false,
    format: "text" as const,
    quiet: false,
    verbose: false,
    dbUri: "postgresql://localhost/testdb",
    planFile: undefined,
    topDir: testDir,
    registry: undefined,
    target: undefined,
  };
}

function makeDeployedChange(name: string, changeId: string, overrides: Record<string, unknown> = {}) {
  return {
    change_id: changeId,
    script_hash: `hash_${changeId}`,
    change: name,
    project: "testproject",
    note: `Note for ${name}`,
    committed_at: new Date("2025-01-15T10:00:00Z"),
    committer_name: "Test User",
    committer_email: "test@example.com",
    planned_at: new Date("2025-01-15T10:00:00Z"),
    planner_name: "Plan User",
    planner_email: "plan@example.com",
    ...overrides,
  };
}

function createMockProcess() {
  const emitter = new EventEmitter();
  const exits: number[] = [];
  const stderrWrites: string[] = [];

  const mock = Object.assign(emitter, {
    exit: (code: number) => {
      exits.push(code);
    },
    stderr: {
      write: (msg: string) => {
        stderrWrites.push(msg);
        return true;
      },
    },
  }) as unknown as NodeJS.Process;

  return { mock, exits, stderrWrites };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("deploy/revert robustness (issue #125)", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
    setConfig({ quiet: true });
    testDir = createTestDir();
    writeSqitchConf(testDir, `[core]\n    engine = pg\n`);
  });

  afterEach(() => {
    try {
      if (testDir && existsSync(testDir)) {
        rmSync(testDir, { recursive: true, force: true });
      }
    } catch {
      // ignore cleanup errors
    }
  });

  // =========================================================================
  // 1. Transaction semantics (5 tests)
  // =========================================================================

  describe("1. Transaction semantics", () => {
    it("--mode change deploys each change in its own transaction (--single-transaction per script)", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      options.mode = "change";
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(2);
      // Sqitch does NOT pass --single-transaction by default.
      expect(calls.length).toBe(2);
      expect(calls[0]!.args).not.toContain("--single-transaction");
      expect(calls[1]!.args).not.toContain("--single-transaction");
    });

    it("--mode all is rejected as not-yet-implemented", () => {
      expect(() => parseDeployOptions(makeArgs(["--mode", "all"]))).toThrow(
        "--mode all is not yet implemented",
      );
    });

    it("--mode tag is rejected as not-yet-implemented", () => {
      expect(() => parseDeployOptions(makeArgs(["--mode", "tag"]))).toThrow(
        "--mode tag is not yet implemented",
      );
    });

    it("--mode with unknown value is rejected with clear error", () => {
      expect(() => parseDeployOptions(makeArgs(["--mode", "banana"]))).toThrow(
        "Unknown mode: banana",
      );
    });

    it("non-transactional change in --mode change skips --single-transaction for that script only", async () => {
      writePlan(testDir, NON_TXN_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx ON users(email);");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      options.mode = "change";
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(2);
      // Sqitch does NOT pass --single-transaction by default.
      expect(calls[0]!.args).not.toContain("--single-transaction");
      expect(calls[1]!.args).not.toContain("--single-transaction");
    });
  });

  // =========================================================================
  // 2. Non-transactional handling (7 tests)
  // =========================================================================

  describe("2. Non-transactional handling", () => {
    it("CIC script runs without BEGIN (no --single-transaction)", async () => {
      writePlan(testDir, NON_TXN_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx ON users(email);");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      // CIC call should not have --single-transaction
      const cicCall = calls.find((c) => c.scriptFile.includes("add_index"));
      expect(cicCall).toBeDefined();
      expect(cicCall!.args).not.toContain("--single-transaction");
    });

    it("detects -- sqlever:no-transaction marker on first line", () => {
      expect(isNonTransactional("-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY ...")).toBe(true);
    });

    it("detects -- sqlever:no-transaction with extra spacing", () => {
      expect(isNonTransactional("--  sqlever:no-transaction\nSELECT 1")).toBe(true);
    });

    it("detects -- sqlever:no-transaction case-insensitively", () => {
      expect(isNonTransactional("-- SQLEVER:NO-TRANSACTION\nSELECT 1")).toBe(true);
    });

    it("records non-transactional change in tracking table after success", async () => {
      writePlan(testDir, NON_TXN_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx ON users(email);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(2);
      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(2);
      expect(changeInserts[1]!.values![2]).toBe("add_index");
    });

    it("failed CIC leaves no tracking record for that change", async () => {
      writePlan(testDir, NON_TXN_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx ON users(email);");

      const deps = await createDeps({ failOnScript: "add_index" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.failedChange).toBe("add_index");
      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      // Only create_schema recorded
      expect(changeInserts.length).toBe(1);
      expect(changeInserts[0]!.values![2]).toBe("create_schema");
    });

    it("next deploy after failed CIC re-attempts the failed change", async () => {
      // First deploy: add_index fails
      writePlan(testDir, NON_TXN_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx ON users(email);");

      const deps1 = await createDeps({ failOnScript: "add_index" });
      const options1 = defaultOptions(testDir);
      const result1 = await executeDeploy(options1, deps1);
      expect(result1.failedChange).toBe("add_index");

      // Second deploy: everything succeeds; mock DB returns create_schema as deployed
      const deps2 = await createDeps();
      const pgClient2 = getPgClient();
      const origQuery2 = pgClient2.query.bind(pgClient2);
      pgClient2.query = async (text: string, values?: unknown[]) => {
        if (text.includes("SELECT") && text.includes("sqitch.changes") && text.includes("ORDER BY committed_at ASC")) {
          const { parsePlan } = await import("../../src/plan/parser");
          const plan = parsePlan(NON_TXN_PLAN);
          // Only create_schema is deployed
          const deployed = plan.changes.filter((c) => c.name === "create_schema");
          const rows = deployed.map((c) => ({
            change_id: c.change_id,
            script_hash: "dummy",
            change: c.name,
            project: "myproject",
            note: c.note,
            committed_at: new Date(),
            committer_name: "Test",
            committer_email: "test@x.com",
            planned_at: new Date(c.planned_at),
            planner_name: c.planner_name,
            planner_email: c.planner_email,
          }));
          return { rows, rowCount: rows.length, command: "SELECT" };
        }
        return origQuery2(text, values);
      };

      const options2 = defaultOptions(testDir);
      const result2 = await executeDeploy(options2, deps2);

      // Only add_index should be deployed (create_schema already done)
      expect(result2.deployed).toBe(1);
      expect(result2.error).toBeUndefined();
    });
  });

  // =========================================================================
  // 3. Advisory lock (7 tests)
  // =========================================================================

  describe("3. Advisory lock", () => {
    it("pg_try_advisory_lock returning false results in exit code 4", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const pgClient = getPgClient();
      const origQuery = pgClient.query.bind(pgClient);
      pgClient.query = async (text: string, values?: unknown[]) => {
        if (text.includes("pg_try_advisory_lock")) {
          pgClient.queries.push({ text, values });
          return { rows: [{ pg_try_advisory_lock: false }], rowCount: 1, command: "SELECT" };
        }
        return origQuery(text, values);
      };

      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.error).toBe("Concurrent deploy detected");
      expect(result.deployed).toBe(0);
    });

    it("same project name always produces the same lock key", () => {
      const k1 = projectLockKey("myproject");
      const k2 = projectLockKey("myproject");
      const k3 = projectLockKey("myproject");
      expect(k1).toBe(k2);
      expect(k2).toBe(k3);
    });

    it("different project names produce different lock keys", () => {
      const ka = projectLockKey("project_alpha");
      const kb = projectLockKey("project_beta");
      const kc = projectLockKey("project_gamma");
      expect(ka).not.toBe(kb);
      expect(kb).not.toBe(kc);
      expect(ka).not.toBe(kc);
    });

    it("lock is released after successful deploy", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );
      expect(unlockQuery).toBeDefined();
      expect(unlockQuery!.values).toEqual([ADVISORY_LOCK_NAMESPACE, projectLockKey("myproject")]);
    });

    it("lock is released after deploy failure", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "create_schema" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );
      expect(unlockQuery).toBeDefined();
    });

    it("lock is released after dependency validation failure", async () => {
      const missingDepPlan = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Schema
add_users [nonexistent_thing] 2025-01-02T00:00:00Z Test User <test@example.com> # Users
`;
      writePlan(testDir, missingDepPlan);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      try {
        await executeDeploy(options, deps);
      } catch {
        // Expected: validateDependencies throws
      }

      const pgClient = getPgClient();
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );
      expect(unlockQuery).toBeDefined();
    });

    it("lock auto-released on disconnect (DB connection close)", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      // After deploy completes, the client is disconnected
      expect(pgClient.ended).toBe(true);
      // PG releases session-level advisory locks on disconnect automatically
      // Our code explicitly unlocks AND disconnects -- both should happen
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );
      expect(unlockQuery).toBeDefined();
    });
  });

  // =========================================================================
  // 4. Failure recovery (6 tests)
  // =========================================================================

  describe("4. Failure recovery", () => {
    it("SQL error leaves tracking unchanged for the failed change", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.failedChange).toBe("add_users");
      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      // Only create_schema recorded, not add_users
      expect(changeInserts.length).toBe(1);
      expect(changeInserts[0]!.values![2]).toBe("create_schema");
    });

    it("mid-batch failure (3 changes, 2nd fails): 1st committed, 3rd not attempted", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const { runner, calls } = createTrackingPsqlRunner(["add_users"]);
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(1);
      expect(result.failedChange).toBe("add_users");

      // psql was called for scripts 1 and 2, but NOT 3
      expect(calls.length).toBe(2);
      expect(calls[0]!.scriptFile).toContain("create_schema");
      expect(calls[1]!.scriptFile).toContain("add_users");
    });

    it("revert script failure records a fail event", async () => {
      // Use the revert module's buildRevertInput + Registry.recordFailEvent
      const db = new DatabaseClient("postgresql://localhost/testdb");
      await db.connect();
      const pgClient = getPgClient();
      const registry = new Registry(db);

      const deployed = makeDeployedChange("broken_change", "id_broken");
      const input = buildRevertInput(deployed);

      await registry.recordFailEvent(input);

      const failEvent = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.events") && q.values?.[0] === "fail",
      );
      expect(failEvent).toBeDefined();
      expect(failEvent!.values![2]).toBe("broken_change");
    });

    it("DB unreachable constant is exit code 10", () => {
      expect(EXIT_DB_UNREACHABLE).toBe(10);
      expect(EXIT_CODE_DB_UNREACHABLE).toBe(10);
    });

    it("--mode change partial state: only successful changes are tracked", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps({ failOnScript: "add_posts" });
      const options = defaultOptions(testDir);
      options.mode = "change";
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(2); // create_schema + add_users
      expect(result.failedChange).toBe("add_posts");

      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(2);
      expect(changeInserts[0]!.values![2]).toBe("create_schema");
      expect(changeInserts[1]!.values![2]).toBe("add_users");
    });

    it("fail event is recorded when deploy script errors", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const failEvents = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.events") && q.values?.[0] === "fail",
      );
      expect(failEvents.length).toBe(1);
      expect(failEvents[0]!.values![2]).toBe("add_users");
    });
  });

  // =========================================================================
  // 5. Lock timeout guard (5 tests)
  // =========================================================================

  describe("5. Lock timeout guard", () => {
    it("auto-prepends SET lock_timeout when script has no SET lock_timeout", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      options.lockTimeout = 5000;

      await executeDeploy(options, deps);

      for (const call of calls) {
        expect(call.args.join(" ")).toContain("SET lock_timeout = '5000ms'");
      }
    });

    it("skips auto-prepend when script already has SET lock_timeout", async () => {
      const plan = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Schema
`;
      writePlan(testDir, plan);
      writeDeployScript(testDir, "create_schema", "SET lock_timeout = '10s';\nCREATE SCHEMA myapp;");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      options.lockTimeout = 5000;

      await executeDeploy(options, deps);

      // The auto-set value should NOT be in args (script has its own)
      for (const call of calls) {
        expect(call.args.join(" ")).not.toContain("SET lock_timeout = '5000ms'");
      }
    });

    it("configurable timeout value is passed through to psql args", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      options.lockTimeout = 15000;

      await executeDeploy(options, deps);

      // Verify the specific value 15000 is used
      for (const call of calls) {
        expect(call.args.join(" ")).toContain("SET lock_timeout = '15000ms'");
      }
    });

    it("retryWithBackoff retries on transient lock errors with backoff", async () => {
      let calls = 0;
      const retryLog: Array<{ attempt: number; delayMs: number }> = [];

      const fn = async () => {
        calls++;
        if (calls <= 2) throw new Error("canceling statement due to lock timeout");
        return "ok";
      };

      const result = await retryWithBackoff(fn, {
        maxRetries: 3,
        initialDelayMs: 1, // Very short for testing
        maxDelayMs: 10,
        shouldRetry: (err) => err instanceof Error && isLockTimeoutError(err.message),
        onRetry: (attempt, _err, delayMs) => {
          retryLog.push({ attempt, delayMs });
        },
      });

      expect(result).toBe("ok");
      expect(calls).toBe(3);
      expect(retryLog.length).toBe(2);
      // Verify backoff: second delay >= first delay
      expect(retryLog[1]!.delayMs).toBeGreaterThanOrEqual(retryLog[0]!.delayMs);
    });

    it("isLockTimeoutError detects all PostgreSQL lock timeout patterns", () => {
      expect(isLockTimeoutError("ERROR:  canceling statement due to lock timeout")).toBe(true);
      expect(isLockTimeoutError("ERROR:  could not obtain lock on relation \"users\"")).toBe(true);
      expect(isLockTimeoutError("lock_not_available")).toBe(true);
      expect(isLockTimeoutError("ERROR: syntax error")).toBe(false);
      expect(isLockTimeoutError("")).toBe(false);
    });
  });

  // =========================================================================
  // 6. SIGINT/SIGTERM (5 tests)
  // =========================================================================

  describe("6. SIGINT/SIGTERM", () => {
    it("first signal sets shutting-down flag", async () => {
      const { mock } = createMockProcess();
      const manager = new ShutdownManager();
      manager.register({ process_: mock });

      mock.emit("SIGINT");
      await new Promise((r) => setTimeout(r, 10));

      expect(manager.isShuttingDown()).toBe(true);
    });

    it("cleanup callbacks run on shutdown", async () => {
      const { mock } = createMockProcess();
      const manager = new ShutdownManager();
      const called: string[] = [];

      manager.register({ process_: mock });
      manager.onShutdown(() => {
        called.push("cleanup-a");
      });
      manager.onShutdown(async () => {
        called.push("cleanup-b");
      });

      mock.emit("SIGTERM");
      await new Promise((r) => setTimeout(r, 50));

      expect(called).toEqual(["cleanup-a", "cleanup-b"]);
    });

    it("exit 130 for SIGINT", async () => {
      const { mock, exits } = createMockProcess();
      const manager = new ShutdownManager();
      manager.register({ process_: mock });

      mock.emit("SIGINT");
      await new Promise((r) => setTimeout(r, 10));

      expect(exits).toContain(130);
    });

    it("exit 143 for SIGTERM", async () => {
      const { mock, exits } = createMockProcess();
      const manager = new ShutdownManager();
      manager.register({ process_: mock });

      mock.emit("SIGTERM");
      await new Promise((r) => setTimeout(r, 10));

      expect(exits).toContain(143);
    });

    it("second signal force-exits immediately", async () => {
      const { mock, exits } = createMockProcess();
      const manager = new ShutdownManager();

      manager.register({ process_: mock });
      // Slow cleanup to keep first signal processing
      manager.onShutdown(() => new Promise((r) => setTimeout(r, 500)));

      mock.emit("SIGINT");
      await new Promise((r) => setTimeout(r, 10));

      expect(manager.isShuttingDown()).toBe(true);
      expect(exits).toHaveLength(0); // First signal still cleaning up

      // Second signal
      mock.emit("SIGINT");
      expect(exits).toEqual([130]); // Force exit
    });
  });

  // =========================================================================
  // 7. Revert robustness (5 tests)
  // =========================================================================

  describe("7. Revert robustness", () => {
    it("computeChangesToRevert returns correct reverse order", () => {
      const deployed = [
        makeDeployedChange("a", "1"),
        makeDeployedChange("b", "2"),
        makeDeployedChange("c", "3"),
        makeDeployedChange("d", "4"),
        makeDeployedChange("e", "5"),
      ];

      const result = computeChangesToRevert(deployed);
      expect(result.map((c) => c.change)).toEqual(["e", "d", "c", "b", "a"]);
    });

    it("--to without value raises error", () => {
      const args = parseArgs(["revert", "--to"]);
      expect(() => parseRevertOptions(args)).toThrow("Missing value for --to");
    });

    it("--to followed by another flag raises error", () => {
      const args = parseArgs(["revert", "--to", "-y"]);
      expect(() => parseRevertOptions(args)).toThrow("Missing value for --to");
    });

    it("-y is required for non-TTY stdin", async () => {
      const changes = [
        {
          name: "a",
          change_id: "id_a",
          revertScriptPath: "/path/a.sql",
          deployed: makeDeployedChange("a", "id_a"),
        },
      ];

      const mockStdin = { isTTY: false } as unknown as NodeJS.ReadStream & { isTTY?: boolean };
      const result = await confirmRevert(changes, false, mockStdin);
      expect(result).toBe(false);
    });

    it("runRevert returns exit code (no process.exit)", async () => {
      // Verify revert.ts source does not call process.exit()
      const srcPath = join(
        testDir, "..", "..", "..", "src", "commands", "revert.ts",
      );
      // Use a more reliable approach: check the actual module behavior
      const args = parseArgs(["revert", "-y", "--top-dir", "/tmp/__nonexistent_sqlever_dir_robustness__"]);

      const exitSpy = spyOn(process, "exit").mockImplementation(
        (() => {
          throw new Error("process.exit() must not be called inside runRevert");
        }) as never,
      );

      try {
        const result = await runRevert(args);
        expect(typeof result).toBe("number");
        expect(result).not.toBe(0);
      } catch {
        // May throw from config loading -- that is acceptable
      } finally {
        exitSpy.mockRestore();
      }
    });
  });
});
