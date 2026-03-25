// tests/unit/deploy-analysis.test.ts — Tests for static analysis integration
// in deploy (R4)
//
// Validates:
// - Analysis runs before each script during deploy
// - Error-severity findings block deploy (exit code 2)
// - Warning-severity findings are displayed but do not block
// - --skip-analysis bypasses analysis entirely
// - --force bypasses analysis errors
// - --force-rule bypasses a specific rule
// - Auto-commit detection prevents SA020 false positives
// - Inline suppressions are honored

import { describe, it, expect, beforeEach, afterEach, mock } from "bun:test";
import { EventEmitter } from "events";
import { mkdirSync, writeFileSync, rmSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { resetConfig, setConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client
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
    if (text.includes("pg_try_advisory_lock")) {
      return { rows: [{ pg_try_advisory_lock: true }], rowCount: 1, command: "SELECT" };
    }
    if (text.includes("pg_advisory_unlock")) {
      return { rows: [{ pg_advisory_unlock: true }], rowCount: 1, command: "SELECT" };
    }
    if (text.includes("SELECT") && text.includes("sqitch.projects") && !text.includes("INSERT")) {
      return { rows: [], rowCount: 0, command: "SELECT" };
    }
    if (text.includes("INSERT INTO sqitch.projects")) {
      return {
        rows: [{ project: "test", uri: null, created_at: new Date(), creator_name: "Test", creator_email: "test@x.com" }],
        rowCount: 1,
        command: "INSERT",
      };
    }
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
const { DatabaseClient } = await import("../../src/db/client");
const { Registry } = await import("../../src/db/registry");
const {
  executeDeploy,
  parseDeployOptions,
  EXIT_ANALYSIS_FAILED,
  EXIT_DEPLOY_FAILED,
} = await import("../../src/commands/deploy");
const { loadConfig } = await import("../../src/config/index");
const { PsqlRunner } = await import("../../src/psql");
const { ShutdownManager } = await import("../../src/signals");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let testDir: string;
let testDirCounter = 0;

function createTestDir(): string {
  testDirCounter++;
  const dir = join(tmpdir(), `sqlever-deploy-analysis-test-${Date.now()}-${testDirCounter}`);
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

function writeSqitchConf(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.conf"), content, "utf-8");
}

const SINGLE_CHANGE_PLAN = `%syntax-version=1.0.0
%project=myproject

add_users 2025-01-01T00:00:00Z Test User <test@example.com> # Add users table
`;

const TWO_CHANGE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
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
    noSnapshot: true,
    skipAnalysis: false,
    force: false,
    forceRules: [],
  };
}

async function createDeps(): Promise<DeployDeps> {
  const db = new DatabaseClient("postgresql://localhost/testdb");
  const registry = new Registry(db);
  const psqlRunner = createMockPsqlRunner(0);
  const config = loadConfig(testDir);
  const shutdownMgr = new ShutdownManager();

  return { db, registry, psqlRunner, config, shutdownMgr };
}

function makeArgs(rest: string[]) {
  return {
    command: "deploy" as string | undefined,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("deploy: static analysis integration (R4)", () => {
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

  // -----------------------------------------------------------------------
  // Exit code
  // -----------------------------------------------------------------------

  describe("EXIT_ANALYSIS_FAILED", () => {
    it("is exit code 2", () => {
      expect(EXIT_ANALYSIS_FAILED).toBe(2);
    });
  });

  // -----------------------------------------------------------------------
  // Option parsing
  // -----------------------------------------------------------------------

  describe("parseDeployOptions — analysis flags", () => {
    it("parses --skip-analysis", () => {
      const options = parseDeployOptions(makeArgs(["--skip-analysis"]));
      expect(options.skipAnalysis).toBe(true);
    });

    it("defaults skipAnalysis to false", () => {
      const options = parseDeployOptions(makeArgs([]));
      expect(options.skipAnalysis).toBe(false);
    });

    it("parses --force", () => {
      const options = parseDeployOptions(makeArgs(["--force"]));
      expect(options.force).toBe(true);
    });

    it("defaults force to false", () => {
      const options = parseDeployOptions(makeArgs([]));
      expect(options.force).toBe(false);
    });

    it("parses --force-rule with a single rule", () => {
      const options = parseDeployOptions(makeArgs(["--force-rule", "SA003"]));
      expect(options.forceRules).toEqual(["SA003"]);
    });

    it("parses --force-rule with multiple rules", () => {
      const options = parseDeployOptions(
        makeArgs(["--force-rule", "SA003", "--force-rule", "SA007"]),
      );
      expect(options.forceRules).toEqual(["SA003", "SA007"]);
    });

    it("defaults forceRules to empty array", () => {
      const options = parseDeployOptions(makeArgs([]));
      expect(options.forceRules).toEqual([]);
    });

    it("throws when --force-rule is missing a value", () => {
      expect(() => parseDeployOptions(makeArgs(["--force-rule"]))).toThrow(
        "--force-rule requires a rule ID argument",
      );
    });

    it("combines --force-rule with other flags", () => {
      const options = parseDeployOptions(
        makeArgs(["--force-rule", "SA003", "--dry-run", "--verify"]),
      );
      expect(options.forceRules).toEqual(["SA003"]);
      expect(options.dryRun).toBe(true);
      expect(options.verify).toBe(true);
    });
  });

  // -----------------------------------------------------------------------
  // Analysis blocks deploy on error findings
  // -----------------------------------------------------------------------

  describe("analysis blocks deploy on error findings", () => {
    it("blocks deploy when script has error-level finding", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      // SA007: DROP TABLE without IF EXISTS triggers an error
      writeDeployScript(testDir, "add_users", "DROP TABLE users;\n");

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(0);
      expect(result.error).toBeDefined();
      expect(result.analysisBlocked).toBe(true);
      expect(result.failedChange).toBe("add_users");
    });

    it("reports correct error message mentioning error count", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      writeDeployScript(testDir, "add_users", "DROP TABLE users;\n");

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      expect(result.error).toContain("Static analysis blocked deploy");
      expect(result.error).toContain("add_users");
    });
  });

  // -----------------------------------------------------------------------
  // Warnings do not block deploy
  // -----------------------------------------------------------------------

  describe("warnings do not block deploy", () => {
    it("deploys successfully despite warning-level findings", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      // A simple CREATE TABLE that should not trigger error-level findings
      writeDeployScript(
        testDir,
        "add_users",
        "create table users (id int8 generated always as identity primary key, name text not null);\n",
      );

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(1);
      expect(result.error).toBeUndefined();
      expect(result.analysisBlocked).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // --skip-analysis bypasses analysis
  // -----------------------------------------------------------------------

  describe("--skip-analysis flag", () => {
    it("skips analysis entirely when --skip-analysis is set", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      // Script that would normally trigger an error finding
      writeDeployScript(testDir, "add_users", "DROP TABLE users;\n");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.skipAnalysis = true;

      const result = await executeDeploy(options, deps);

      // Deploy proceeds despite the risky SQL (analysis was skipped)
      expect(result.deployed).toBe(1);
      expect(result.analysisBlocked).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // --force bypasses analysis errors
  // -----------------------------------------------------------------------

  describe("--force flag", () => {
    it("allows deploy to proceed despite error-level findings", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      writeDeployScript(testDir, "add_users", "DROP TABLE users;\n");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.force = true;

      const result = await executeDeploy(options, deps);

      // Deploy proceeds because --force was set
      expect(result.deployed).toBe(1);
      expect(result.analysisBlocked).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // --force-rule bypasses specific rules
  // -----------------------------------------------------------------------

  describe("--force-rule flag", () => {
    it("bypasses a specific rule while keeping other guards active", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      // SA007 fires on DROP TABLE without IF EXISTS
      writeDeployScript(testDir, "add_users", "DROP TABLE users;\n");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.forceRules = ["SA007"];

      const result = await executeDeploy(options, deps);

      // Deploy proceeds because SA007 was force-skipped
      expect(result.deployed).toBe(1);
      expect(result.analysisBlocked).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // Auto-commit detection prevents SA020 false positives
  // -----------------------------------------------------------------------

  describe("auto-commit detection for SA020", () => {
    it("does not false-positive on CIC scripts marked as auto-commit", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      // Script with auto-commit directive and CONCURRENTLY operation
      writeDeployScript(
        testDir,
        "add_users",
        "-- sqlever:auto-commit\ncreate index concurrently idx_users_name on users (name);\n",
      );

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      // Should deploy without SA020 blocking
      expect(result.deployed).toBe(1);
      expect(result.analysisBlocked).toBeUndefined();
    });

    it("flags CIC in transactional context (not marked auto-commit)", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      // No auto-commit directive -- isTransactional will be true
      writeDeployScript(
        testDir,
        "add_users",
        "create index concurrently idx_users_name on users (name);\n",
      );

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      // SA020 should fire because script lacks auto-commit and
      // isTransactional is true in the analysis context.
      // However, SA020 only fires when there's a transactional context
      // (explicit BEGIN or isTransactional=true). Since the script has no
      // BEGIN statement, SA020 uses the isTransactional flag from deploy.
      expect(result.deployed).toBe(0);
      expect(result.analysisBlocked).toBe(true);
    });
  });

  // -----------------------------------------------------------------------
  // Inline suppression is honored
  // -----------------------------------------------------------------------

  describe("inline suppression", () => {
    it("honors -- sqlever:disable comments", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      writeDeployScript(
        testDir,
        "add_users",
        "-- sqlever:disable SA007\nDROP TABLE users;\n",
      );

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      // SA007 suppressed by inline comment -- deploy proceeds
      expect(result.deployed).toBe(1);
      expect(result.analysisBlocked).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // Multi-change deploy: first change passes, second blocked
  // -----------------------------------------------------------------------

  describe("multi-change deploy", () => {
    it("deploys first change then blocks on second change with errors", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(
        testDir,
        "create_schema",
        "create schema if not exists app;\n",
      );
      writeDeployScript(testDir, "add_users", "DROP TABLE users;\n");

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(1);
      expect(result.failedChange).toBe("add_users");
      expect(result.analysisBlocked).toBe(true);
    });
  });

  // -----------------------------------------------------------------------
  // Clean script deploys without issues
  // -----------------------------------------------------------------------

  describe("clean script", () => {
    it("deploys a clean script without analysis blocking", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      writeDeployScript(
        testDir,
        "add_users",
        "create table if not exists users (\n  id int8 generated always as identity primary key,\n  name text not null\n);\n",
      );

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(1);
      expect(result.error).toBeUndefined();
      expect(result.analysisBlocked).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // DeployResult.analysisBlocked flag
  // -----------------------------------------------------------------------

  describe("DeployResult.analysisBlocked", () => {
    it("is true when analysis blocks deploy", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      writeDeployScript(testDir, "add_users", "DROP TABLE users;\n");

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      expect(result.analysisBlocked).toBe(true);
    });

    it("is undefined when deploy succeeds", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      writeDeployScript(
        testDir,
        "add_users",
        "create table if not exists users (id int8 generated always as identity primary key);\n",
      );

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      const result = await executeDeploy(options, deps);

      expect(result.analysisBlocked).toBeUndefined();
    });

    it("is undefined when --skip-analysis is used", async () => {
      writePlan(testDir, SINGLE_CHANGE_PLAN);
      writeDeployScript(testDir, "add_users", "DROP TABLE users;\n");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.skipAnalysis = true;

      const result = await executeDeploy(options, deps);

      expect(result.analysisBlocked).toBeUndefined();
    });
  });
});
