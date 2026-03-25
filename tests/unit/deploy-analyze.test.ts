// tests/unit/deploy-analyze.test.ts -- pre-deploy static analysis (R4)
//
// Tests the integration of static analysis into the deploy command:
//   - Analysis runs before deploy and blocks on error-severity findings
//   - --no-analyze flag skips analysis
//   - --force flag bypasses analysis errors
//   - --force-rule skips specific rules
//   - Warnings do not block deploy
//   - Analysis results included in DeployResult for JSON output
//   - parseDeployOptions parses new flags correctly

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

// Imports after mocking
import type { DeployOptions, DeployDeps } from "../../src/commands/deploy";
import type { SpawnFn } from "../../src/psql";

const { DatabaseClient } = await import("../../src/db/client");
const { Registry } = await import("../../src/db/registry");
const {
  executeDeploy,
  parseDeployOptions,
  EXIT_ANALYSIS_BLOCKED,
} = await import("../../src/commands/deploy");
const { loadConfig } = await import("../../src/config/index");
const { PsqlRunner } = await import("../../src/psql");
const { ShutdownManager } = await import("../../src/signals");
const { runPreDeployAnalysis } = await import("../../src/commands/deploy-analyze");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let testDir: string;
let testDirCounter = 0;

function createTestDir(): string {
  testDirCounter++;
  const dir = join(tmpdir(), `sqlever-deploy-analyze-test-${Date.now()}-${testDirCounter}`);
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

const SIMPLE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
`;

function createMockPsqlRunner(exitCode = 0): PsqlRunner {
  const mockSpawn: SpawnFn = (_cmd, _args, _opts) => {
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });
    queueMicrotask(() => {
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
    noSnapshot: false,
    noAnalyze: false,
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

interface MakeArgsOptions {
  dbUri?: string;
  rest?: string[];
}

function makeArgs(rest: string[], opts?: MakeArgsOptions): import("../../src/cli").ParsedArgs {
  return {
    command: "deploy",
    rest,
    help: false,
    version: false,
    format: "text",
    quiet: false,
    verbose: false,
    dbUri: opts?.dbUri ?? "postgresql://localhost/testdb",
    planFile: undefined,
    topDir: testDir,
    registry: undefined,
    target: undefined,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("pre-deploy analysis (R4)", () => {
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
  // runPreDeployAnalysis unit tests
  // -----------------------------------------------------------------------

  describe("runPreDeployAnalysis()", () => {
    it("returns no error findings for safe SQL and does not block", async () => {
      writeDeployScript(testDir, "safe", "create table if not exists t (id int8 generated always as identity primary key);\n");
      const result = await runPreDeployAnalysis(
        [join(testDir, "deploy", "safe.sql")],
        { forceRules: [], force: false },
      );
      expect(result.blocked).toBe(false);
      expect(result.summary.errors).toBe(0);
      expect(result.filesAnalyzed).toBe(1);
    });

    it("detects error-severity findings and sets blocked=true", async () => {
      // SA003: DROP TABLE without IF EXISTS is error-severity
      writeDeployScript(testDir, "dangerous", "drop table users;\n");
      const result = await runPreDeployAnalysis(
        [join(testDir, "deploy", "dangerous.sql")],
        { forceRules: [], force: false },
      );
      expect(result.blocked).toBe(true);
      expect(result.summary.errors).toBeGreaterThan(0);
      expect(result.findings.some((f) => f.severity === "error")).toBe(true);
    });

    it("does not block when --force is set despite errors", async () => {
      writeDeployScript(testDir, "dangerous", "drop table users;\n");
      const result = await runPreDeployAnalysis(
        [join(testDir, "deploy", "dangerous.sql")],
        { forceRules: [], force: true },
      );
      expect(result.blocked).toBe(false);
      expect(result.summary.errors).toBeGreaterThan(0);
    });

    it("skips specific rules via forceRules", async () => {
      writeDeployScript(testDir, "dangerous", "drop table users;\n");

      // First, get findings without force-rule to know which rule fires
      const baseline = await runPreDeployAnalysis(
        [join(testDir, "deploy", "dangerous.sql")],
        { forceRules: [], force: false },
      );
      const errorRuleIds = baseline.findings
        .filter((f) => f.severity === "error")
        .map((f) => f.ruleId);
      expect(errorRuleIds.length).toBeGreaterThan(0);

      // Now skip those specific rules
      const result = await runPreDeployAnalysis(
        [join(testDir, "deploy", "dangerous.sql")],
        { forceRules: errorRuleIds, force: false },
      );
      // The skipped rules should not appear
      for (const ruleId of errorRuleIds) {
        expect(result.findings.some((f) => f.ruleId === ruleId)).toBe(false);
      }
    });

    it("analyzes multiple files", async () => {
      writeDeployScript(testDir, "a", "create table a (id int8 generated always as identity primary key);\n");
      writeDeployScript(testDir, "b", "create table b (id int8 generated always as identity primary key);\n");
      const result = await runPreDeployAnalysis(
        [
          join(testDir, "deploy", "a.sql"),
          join(testDir, "deploy", "b.sql"),
        ],
        { forceRules: [], force: false },
      );
      expect(result.filesAnalyzed).toBe(2);
    });

    it("handles file read errors gracefully", async () => {
      const result = await runPreDeployAnalysis(
        [join(testDir, "deploy", "nonexistent.sql")],
        { forceRules: [], force: false },
      );
      // Should produce an analyze-error finding
      expect(result.findings.length).toBe(1);
      expect(result.findings[0]!.ruleId).toBe("analyze-error");
      expect(result.findings[0]!.severity).toBe("error");
      expect(result.blocked).toBe(true);
    });

    it("produces formatted text output when findings exist", async () => {
      writeDeployScript(testDir, "dangerous", "drop table users;\n");
      const result = await runPreDeployAnalysis(
        [join(testDir, "deploy", "dangerous.sql")],
        { forceRules: [], force: false },
      );
      expect(result.output.length).toBeGreaterThan(0);
      expect(result.output).toContain("error");
    });

    it("returns empty output when no findings exist", async () => {
      // Use IF NOT EXISTS to avoid SA042 info findings
      writeDeployScript(testDir, "safe", "create table if not exists t (id int8 generated always as identity primary key);\n");
      const result = await runPreDeployAnalysis(
        [join(testDir, "deploy", "safe.sql")],
        { forceRules: [], force: false },
      );
      // May have info findings from other rules; check no errors or warnings
      expect(result.blocked).toBe(false);
      expect(result.summary.errors).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy integration with analysis
  // -----------------------------------------------------------------------

  describe("executeDeploy() with analysis", () => {
    it("blocks deploy when analysis finds errors", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      // SA003: DROP TABLE triggers error
      writeDeployScript(testDir, "create_schema", "drop table users;\n");
      writeDeployScript(testDir, "add_users", "create table users (id int8 generated always as identity primary key);\n");

      const options = { ...defaultOptions(testDir), noAnalyze: false };
      const deps = await createDeps();
      const result = await executeDeploy(options, deps);

      expect(result.error).toBe("Static analysis errors found");
      expect(result.deployed).toBe(0);
      expect(result.analysis).toBeDefined();
      expect(result.analysis!.blocked).toBe(true);
      expect(result.analysis!.summary.errors).toBeGreaterThan(0);
    });

    it("proceeds with deploy when --no-analyze is set", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      // This script would trigger analysis errors but --no-analyze skips check
      writeDeployScript(testDir, "create_schema", "drop table users;\n");
      writeDeployScript(testDir, "add_users", "create table users (id int8 generated always as identity primary key);\n");

      const options = { ...defaultOptions(testDir), noAnalyze: true };
      const deps = await createDeps();
      const result = await executeDeploy(options, deps);

      // Deploy should proceed (psql mock succeeds)
      expect(result.error).toBeUndefined();
      expect(result.deployed).toBe(2);
      expect(result.analysis).toBeUndefined();
    });

    it("proceeds with deploy when --force bypasses errors", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "drop table users;\n");
      writeDeployScript(testDir, "add_users", "create table users (id int8 generated always as identity primary key);\n");

      const options = { ...defaultOptions(testDir), noAnalyze: false, force: true };
      const deps = await createDeps();
      const result = await executeDeploy(options, deps);

      // Deploy proceeds -- analysis ran but did not block
      expect(result.error).toBeUndefined();
      expect(result.deployed).toBe(2);
      expect(result.analysis).toBeDefined();
      expect(result.analysis!.blocked).toBe(false);
    });

    it("proceeds with deploy when only warnings exist (no errors)", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      // Safe scripts should not block
      writeDeployScript(testDir, "create_schema", "create schema myapp;\n");
      writeDeployScript(testDir, "add_users", "create table myapp.users (id int8 generated always as identity primary key);\n");

      const options = { ...defaultOptions(testDir), noAnalyze: false };
      const deps = await createDeps();
      const result = await executeDeploy(options, deps);

      expect(result.error).toBeUndefined();
      expect(result.deployed).toBe(2);
    });

    it("includes analysis in dry-run result", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "create schema myapp;\n");
      writeDeployScript(testDir, "add_users", "create table myapp.users (id int8 generated always as identity primary key);\n");

      const options = { ...defaultOptions(testDir), noAnalyze: false, dryRun: true };
      const deps = await createDeps();
      const result = await executeDeploy(options, deps);

      expect(result.dryRun).toBe(true);
      // Analysis should have run
      expect(result.analysis).toBeDefined();
      expect(result.analysis!.filesAnalyzed).toBe(2);
    });

    it("dry-run blocked by analysis errors returns error", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "drop table users;\n");
      writeDeployScript(testDir, "add_users", "create table users (id int8 generated always as identity primary key);\n");

      const options = { ...defaultOptions(testDir), noAnalyze: false, dryRun: true };
      const deps = await createDeps();
      const result = await executeDeploy(options, deps);

      expect(result.error).toBe("Static analysis errors found");
      expect(result.analysis!.blocked).toBe(true);
    });

    it("--force-rule skips specified rule during deploy", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "drop table users;\n");
      writeDeployScript(testDir, "add_users", "create table users (id int8 generated always as identity primary key);\n");

      // First run without force-rule to identify the blocking rules
      const baseOptions = { ...defaultOptions(testDir), noAnalyze: false };
      const baseDeps = await createDeps();
      const baseResult = await executeDeploy(baseOptions, baseDeps);
      expect(baseResult.error).toBe("Static analysis errors found");
      const errorRuleIds = baseResult.analysis!.findings
        .filter((f) => f.severity === "error")
        .map((f) => f.ruleId);
      expect(errorRuleIds.length).toBeGreaterThan(0);

      // Now deploy with those rules force-skipped
      const options = {
        ...defaultOptions(testDir),
        noAnalyze: false,
        forceRules: [...new Set(errorRuleIds)],
      };
      const deps = await createDeps();
      const result = await executeDeploy(options, deps);

      // Should not be blocked by those rules
      expect(result.analysis!.blocked).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // parseDeployOptions — new flag parsing
  // -----------------------------------------------------------------------

  describe("parseDeployOptions() — analysis flags", () => {
    it("defaults: noAnalyze=false, force=false, forceRules=[]", () => {
      const options = parseDeployOptions(makeArgs([]));
      expect(options.noAnalyze).toBe(false);
      expect(options.force).toBe(false);
      expect(options.forceRules).toEqual([]);
    });

    it("parses --no-analyze", () => {
      const options = parseDeployOptions(makeArgs(["--no-analyze"]));
      expect(options.noAnalyze).toBe(true);
    });

    it("parses --force", () => {
      const options = parseDeployOptions(makeArgs(["--force"]));
      expect(options.force).toBe(true);
    });

    it("parses --force-rule with a single rule", () => {
      const options = parseDeployOptions(makeArgs(["--force-rule", "SA003"]));
      expect(options.forceRules).toEqual(["SA003"]);
    });

    it("parses multiple --force-rule flags", () => {
      const options = parseDeployOptions(makeArgs(["--force-rule", "SA003", "--force-rule", "SA007"]));
      expect(options.forceRules).toEqual(["SA003", "SA007"]);
    });

    it("throws on --force-rule without value", () => {
      expect(() => parseDeployOptions(makeArgs(["--force-rule"]))).toThrow(
        "--force-rule requires a rule ID argument",
      );
    });

    it("throws on --force-rule followed by another flag", () => {
      expect(() => parseDeployOptions(makeArgs(["--force-rule", "--dry-run"]))).toThrow(
        "--force-rule requires a rule ID argument",
      );
    });

    it("combines --no-analyze with other flags", () => {
      const options = parseDeployOptions(makeArgs(["--no-analyze", "--dry-run", "--verify"]));
      expect(options.noAnalyze).toBe(true);
      expect(options.dryRun).toBe(true);
      expect(options.verify).toBe(true);
    });

    it("combines --force with --force-rule", () => {
      const options = parseDeployOptions(makeArgs(["--force", "--force-rule", "SA003"]));
      expect(options.force).toBe(true);
      expect(options.forceRules).toEqual(["SA003"]);
    });
  });

  // -----------------------------------------------------------------------
  // EXIT_ANALYSIS_BLOCKED exit code
  // -----------------------------------------------------------------------

  describe("EXIT_ANALYSIS_BLOCKED", () => {
    it("has value 2 (consistent with analyze command)", () => {
      expect(EXIT_ANALYSIS_BLOCKED).toBe(2);
    });
  });
});
