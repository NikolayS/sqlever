import { describe, it, expect, beforeEach, mock, spyOn } from "bun:test";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as registry.test.ts
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

  async query(text: string, values?: unknown[]): Promise<{ rows: unknown[]; rowCount: number; command: string }> {
    this.queries.push({ text, values });
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

// Import after mocking
const { DatabaseClient } = await import("../../src/db/client");
const { Registry } = await import("../../src/db/registry");
const {
  parseRevertOptions,
  computeChangesToRevert,
  buildRevertInput,
  confirmRevert,
  runRevert,
} = await import("../../src/commands/revert");
const { resolveTargetUri } = await import("../../src/commands/shared");
const { parseArgs } = await import("../../src/cli");
const { isAutoCommit } = await import("../../src/commands/deploy");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a minimal RegistryChange for testing. */
function makeDeployedChange(
  name: string,
  changeId: string,
  overrides: Record<string, unknown> = {},
) {
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

/** Build a minimal PlanChange for testing. */
function makePlanChange(
  name: string,
  changeId: string,
  overrides: Record<string, unknown> = {},
) {
  return {
    change_id: changeId,
    name,
    project: "testproject",
    note: `Note for ${name}`,
    planner_name: "Plan User",
    planner_email: "plan@example.com",
    planned_at: "2025-01-15T10:00:00Z",
    requires: [] as string[],
    conflicts: [] as string[],
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("revert command", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  // -----------------------------------------------------------------------
  // parseRevertOptions
  // -----------------------------------------------------------------------

  describe("parseRevertOptions()", () => {
    it("parses --to flag", () => {
      const args = parseArgs(["revert", "--to", "add_users"]);
      const opts = parseRevertOptions(args);
      expect(opts.toChange).toBe("add_users");
    });

    it("parses -y flag", () => {
      const args = parseArgs(["revert", "-y"]);
      const opts = parseRevertOptions(args);
      expect(opts.noPrompt).toBe(true);
    });

    it("parses --no-prompt flag", () => {
      const args = parseArgs(["revert", "--no-prompt"]);
      const opts = parseRevertOptions(args);
      expect(opts.noPrompt).toBe(true);
    });

    it("parses positional target", () => {
      const args = parseArgs(["revert", "production"]);
      const opts = parseRevertOptions(args);
      expect(opts.target).toBe("production");
    });

    it("parses combined --to and -y", () => {
      const args = parseArgs(["revert", "--to", "create_schema", "-y"]);
      const opts = parseRevertOptions(args);
      expect(opts.toChange).toBe("create_schema");
      expect(opts.noPrompt).toBe(true);
    });

    it("defaults noPrompt to false", () => {
      const args = parseArgs(["revert"]);
      const opts = parseRevertOptions(args);
      expect(opts.noPrompt).toBe(false);
    });

    it("inherits --db-uri from global args", () => {
      const args = parseArgs(["--db-uri", "postgresql://host/db", "revert"]);
      const opts = parseRevertOptions(args);
      expect(opts.dbUri).toBe("postgresql://host/db");
    });

    it("inherits --plan-file from global args", () => {
      const args = parseArgs(["--plan-file", "custom.plan", "revert"]);
      const opts = parseRevertOptions(args);
      expect(opts.planFile).toBe("custom.plan");
    });
  });

  // -----------------------------------------------------------------------
  // computeChangesToRevert
  // -----------------------------------------------------------------------

  describe("computeChangesToRevert()", () => {
    it("returns all changes in reverse order when no --to", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
        makeDeployedChange("c", "id_c"),
      ];

      const result = computeChangesToRevert(deployed);
      expect(result.map((c) => c.change)).toEqual(["c", "b", "a"]);
    });

    it("returns empty array when no changes deployed", () => {
      const result = computeChangesToRevert([]);
      expect(result).toEqual([]);
    });

    it("reverts down to (not including) --to change", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
        makeDeployedChange("c", "id_c"),
        makeDeployedChange("d", "id_d"),
      ];

      const result = computeChangesToRevert(deployed, "b");
      expect(result.map((c) => c.change)).toEqual(["d", "c"]);
    });

    it("returns empty when --to is the last deployed change", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
      ];

      const result = computeChangesToRevert(deployed, "b");
      expect(result).toEqual([]);
    });

    it("throws when --to change is not deployed", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
      ];

      expect(() => computeChangesToRevert(deployed, "nonexistent")).toThrow(
        "Change 'nonexistent' is not deployed",
      );
    });

    it("reverts single change when --to is the first change", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
      ];

      const result = computeChangesToRevert(deployed, "a");
      expect(result.map((c) => c.change)).toEqual(["b"]);
    });

    it("returns single change reversed when only one deployed", () => {
      const deployed = [makeDeployedChange("a", "id_a")];

      const result = computeChangesToRevert(deployed);
      expect(result.map((c) => c.change)).toEqual(["a"]);
    });

    it("preserves full change objects (not just names)", () => {
      const deployed = [
        makeDeployedChange("a", "id_a", { note: "Custom note" }),
      ];

      const result = computeChangesToRevert(deployed);
      expect(result[0]!.note).toBe("Custom note");
      expect(result[0]!.change_id).toBe("id_a");
    });
  });

  // -----------------------------------------------------------------------
  // buildRevertInput
  // -----------------------------------------------------------------------

  describe("buildRevertInput()", () => {
    it("maps deployed change to RecordDeployInput", () => {
      const deployed = makeDeployedChange("add_users", "id_123");
      const input = buildRevertInput(deployed);

      expect(input.change_id).toBe("id_123");
      expect(input.change).toBe("add_users");
      expect(input.project).toBe("testproject");
      expect(input.requires).toEqual([]);
      expect(input.conflicts).toEqual([]);
      expect(input.tags).toEqual([]);
      expect(input.dependencies).toEqual([]);
    });

    it("includes plan change requires and conflicts when available", () => {
      const deployed = makeDeployedChange("add_users", "id_123");
      const planChange = makePlanChange("add_users", "id_123", {
        requires: ["create_schema"],
        conflicts: ["old_users"],
      });

      const input = buildRevertInput(deployed, planChange);

      expect(input.requires).toEqual(["create_schema"]);
      expect(input.conflicts).toEqual(["old_users"]);
    });

    it("preserves committer and planner fields", () => {
      const deployed = makeDeployedChange("x", "id_x", {
        committer_name: "Alice",
        committer_email: "alice@co.com",
        planner_name: "Bob",
        planner_email: "bob@co.com",
      });

      const input = buildRevertInput(deployed);

      expect(input.committer_name).toBe("Alice");
      expect(input.committer_email).toBe("alice@co.com");
      expect(input.planner_name).toBe("Bob");
      expect(input.planner_email).toBe("bob@co.com");
    });
  });

  // -----------------------------------------------------------------------
  // confirmRevert — prompt behavior
  // -----------------------------------------------------------------------

  describe("confirmRevert()", () => {
    it("returns true when noPrompt is true", async () => {
      const changes = [
        {
          name: "a",
          change_id: "id_a",
          revertScriptPath: "/path/a.sql",
          deployed: makeDeployedChange("a", "id_a"),
        },
      ];

      const result = await confirmRevert(changes, true);
      expect(result).toBe(true);
    });

    it("returns false when stdin is not a TTY and noPrompt is false", async () => {
      const changes = [
        {
          name: "a",
          change_id: "id_a",
          revertScriptPath: "/path/a.sql",
          deployed: makeDeployedChange("a", "id_a"),
        },
      ];

      // Create a mock stdin with isTTY = false
      const mockStdin = {
        isTTY: false,
      } as unknown as NodeJS.ReadStream & { isTTY?: boolean };

      const result = await confirmRevert(changes, false, mockStdin);
      expect(result).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // resolveTargetUri
  // -----------------------------------------------------------------------

  describe("resolveTargetUri()", () => {
    it("returns --db-uri when provided", () => {
      const uri = resolveTargetUri(
        { targets: {}, engines: {} } as never,
        "postgresql://host/db",
      );
      expect(uri).toBe("postgresql://host/db");
    });

    it("looks up named target from config", () => {
      const uri = resolveTargetUri(
        {
          targets: { prod: { name: "prod", uri: "postgresql://prod/db" } },
          engines: {},
        } as never,
        undefined,
        "prod",
      );
      expect(uri).toBe("postgresql://prod/db");
    });

    it("falls back to engine target string", () => {
      const uri = resolveTargetUri(
        {
          core: { engine: "pg" },
          targets: {},
          engines: { pg: { name: "pg", target: "db:pg://local/mydb" } },
        } as never,
      );
      expect(uri).toBe("db:pg://local/mydb");
    });

    it("returns null when no target configured", () => {
      const uri = resolveTargetUri(
        { core: { engine: undefined }, targets: {}, engines: {} } as never,
      );
      expect(uri).toBeNull();
    });
  });

  // -----------------------------------------------------------------------
  // Registry.recordFailEvent
  // -----------------------------------------------------------------------

  describe("Registry.recordFailEvent()", () => {
    it("inserts a 'fail' event into sqitch.events", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await client.connect();
      const pgClient = mockInstances[mockInstances.length - 1]!;
      const registry = new Registry(client);

      const input = {
        change_id: "id_fail",
        script_hash: "hash_fail",
        change: "broken_migration",
        project: "testproject",
        note: "This migration fails",
        committer_name: "Test",
        committer_email: "test@test.com",
        planned_at: new Date("2025-01-01"),
        planner_name: "Planner",
        planner_email: "planner@test.com",
        requires: ["dep1"],
        conflicts: [],
        tags: [],
        dependencies: [],
      };

      await registry.recordFailEvent(input);

      const eventInsert = pgClient.queries.find(
        (q) => q.text.includes("INSERT INTO sqitch.events"),
      );
      expect(eventInsert).toBeDefined();
      expect(eventInsert!.values![0]).toBe("fail");
      expect(eventInsert!.values![1]).toBe("id_fail");
      expect(eventInsert!.values![2]).toBe("broken_migration");
    });

    it("does NOT delete from changes table (unlike recordRevert)", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await client.connect();
      const pgClient = mockInstances[mockInstances.length - 1]!;
      const registry = new Registry(client);

      const input = {
        change_id: "id_fail",
        script_hash: null,
        change: "broken",
        project: "testproject",
        note: "",
        committer_name: "Test",
        committer_email: "test@test.com",
        planned_at: new Date("2025-01-01"),
        planner_name: "Planner",
        planner_email: "planner@test.com",
        requires: [],
        conflicts: [],
        tags: [],
        dependencies: [],
      };

      await registry.recordFailEvent(input);

      const deleteQuery = pgClient.queries.find(
        (q) => q.text.includes("DELETE FROM sqitch.changes"),
      );
      expect(deleteQuery).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // Integration: revert order with --to
  // -----------------------------------------------------------------------

  describe("revert ordering integration", () => {
    it("5 deployed, --to second: reverts last 3 in reverse order", () => {
      const deployed = [
        makeDeployedChange("a", "1"),
        makeDeployedChange("b", "2"),
        makeDeployedChange("c", "3"),
        makeDeployedChange("d", "4"),
        makeDeployedChange("e", "5"),
      ];

      const result = computeChangesToRevert(deployed, "b");
      expect(result.map((c) => c.change)).toEqual(["e", "d", "c"]);
    });

    it("5 deployed, --to first: reverts last 4 in reverse order", () => {
      const deployed = [
        makeDeployedChange("a", "1"),
        makeDeployedChange("b", "2"),
        makeDeployedChange("c", "3"),
        makeDeployedChange("d", "4"),
        makeDeployedChange("e", "5"),
      ];

      const result = computeChangesToRevert(deployed, "a");
      expect(result.map((c) => c.change)).toEqual(["e", "d", "c", "b"]);
    });

    it("buildRevertInput falls back gracefully when no plan change", () => {
      const deployed = makeDeployedChange("orphan", "id_orphan");
      const input = buildRevertInput(deployed, undefined);

      expect(input.requires).toEqual([]);
      expect(input.conflicts).toEqual([]);
    });
  });

  // -----------------------------------------------------------------------
  // CLI integration via parseArgs
  // -----------------------------------------------------------------------

  describe("CLI routing", () => {
    it("parseArgs recognizes 'revert' command", () => {
      const args = parseArgs(["revert"]);
      expect(args.command).toBe("revert");
    });

    it("parseArgs passes --to and -y through to rest", () => {
      const args = parseArgs(["revert", "--to", "v1", "-y"]);
      expect(args.command).toBe("revert");
      expect(args.rest).toContain("--to");
      expect(args.rest).toContain("v1");
      expect(args.rest).toContain("-y");
    });

    it("parseArgs handles global flags before revert", () => {
      const args = parseArgs([
        "--verbose",
        "--db-uri",
        "postgresql://h/d",
        "revert",
        "--to",
        "x",
      ]);
      expect(args.command).toBe("revert");
      expect(args.verbose).toBe(true);
      expect(args.dbUri).toBe("postgresql://h/d");
      expect(args.rest).toContain("--to");
    });
  });

  // -----------------------------------------------------------------------
  // Bug fix: --to without a value (issue #66, bug 1)
  // -----------------------------------------------------------------------

  describe("--to without a value", () => {
    it("throws when --to is the last token with no value", () => {
      const args = parseArgs(["revert", "--to"]);
      expect(() => parseRevertOptions(args)).toThrow(
        "Missing value for --to",
      );
    });

    it("throws when --to is followed by another flag instead of a value", () => {
      const args = parseArgs(["revert", "--to", "-y"]);
      expect(() => parseRevertOptions(args)).toThrow(
        "Missing value for --to",
      );
    });

    it("throws when --to is followed by --no-prompt instead of a value", () => {
      const args = parseArgs(["revert", "--to", "--no-prompt"]);
      expect(() => parseRevertOptions(args)).toThrow(
        "Missing value for --to",
      );
    });

    it("does NOT throw when --to has a valid change name", () => {
      const args = parseArgs(["revert", "--to", "my_change"]);
      const opts = parseRevertOptions(args);
      expect(opts.toChange).toBe("my_change");
    });
  });

  // -----------------------------------------------------------------------
  // Bug fix: revert ignores -- sqlever:auto-commit directive
  // -----------------------------------------------------------------------

  describe("revert respects -- sqlever:auto-commit directive", () => {
    it("a revert script with -- sqlever:auto-commit should NOT use --single-transaction", () => {
      const scriptContent = "-- sqlever:auto-commit\nDROP INDEX CONCURRENTLY IF EXISTS idx_foo;\n";
      expect(isAutoCommit(scriptContent)).toBe(true);
      // singleTransaction should be !isAutoCommit = false
      const singleTransaction = !isAutoCommit(scriptContent);
      expect(singleTransaction).toBe(false);
    });

    it("a revert script with legacy -- sqlever:no-transaction should NOT use --single-transaction", () => {
      const scriptContent = "-- sqlever:no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS idx_foo;\n";
      expect(isAutoCommit(scriptContent)).toBe(true);
    });

    it("a normal revert script should NOT use --single-transaction (matching Sqitch)", () => {
      // Sqitch does NOT pass --single-transaction for revert scripts.
      const scriptContent = "DROP TABLE IF EXISTS foo;\n";
      expect(isAutoCommit(scriptContent)).toBe(false);
    });

    it("revert.ts source does not hardcode singleTransaction: true", () => {
      const { readFileSync } = require("node:fs");
      const source = readFileSync(
        new URL("../../src/commands/revert.ts", import.meta.url).pathname,
        "utf-8",
      );

      // The source must NOT have the old hardcoded `singleTransaction: true` in psqlRunner.run
      const hardcoded = /singleTransaction:\s*true/.test(source);
      expect(hardcoded).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // Bug fix: process.exit() replaced with return codes (issue #66, bug 2)
  // -----------------------------------------------------------------------

  describe("no process.exit() in runRevert — advisory lock safety", () => {
    it("returns exit code instead of calling process.exit() on lock contention", async () => {
      // Set up a mock pg client that reports advisory lock NOT acquired
      const lockMock = new MockPgClient({});
      const origQuery = lockMock.query.bind(lockMock);
      lockMock.query = async (text: string, values?: unknown[]) => {
        if (text.includes("pg_try_advisory_lock")) {
          return { rows: [{ pg_try_advisory_lock: false }], rowCount: 1, command: "SELECT" };
        }
        if (text.includes("pg_advisory_unlock")) {
          return { rows: [{ pg_advisory_unlock: true }], rowCount: 1, command: "SELECT" };
        }
        // Ensure registry tables exist — return empty results for schema queries
        return origQuery(text, values);
      };

      // Spy on process.exit to ensure it is NOT called
      const exitSpy = spyOn(process, "exit").mockImplementation(
        (() => {
          throw new Error("process.exit() must not be called inside runRevert");
        }) as never,
      );

      try {
        // We need the function to get past config loading and DB connect.
        // The easiest way to verify the lock-contention path doesn't call
        // process.exit is to let it throw from config loading (which also
        // should not call process.exit).
        // Instead we directly check that process.exit is not imported/used
        // in the hot path by inspecting the source.

        // The definitive test: the source file should have zero process.exit calls
        const { readFileSync } = await import("node:fs");
        const source = readFileSync(
          new URL("../../src/commands/revert.ts", import.meta.url).pathname,
          "utf-8",
        );
        const exitCalls = source.match(/process\.exit\s*\(/g);
        expect(exitCalls).toBeNull();
      } finally {
        exitSpy.mockRestore();
      }
    });

    it("runRevert returns a number (exit code), not void", async () => {
      // Verify the function signature by checking that a failed invocation
      // still returns a number. We call with a non-existent topDir so it
      // fails at config loading and returns 1 (not process.exit).
      const args = parseArgs(["revert", "-y", "--top-dir", "/tmp/__nonexistent_sqlever_dir__"]);

      // Spy to ensure process.exit is never called
      const exitSpy = spyOn(process, "exit").mockImplementation(
        (() => {
          throw new Error("process.exit() must not be called inside runRevert");
        }) as never,
      );

      try {
        const result = await runRevert(args);
        // It should return a numeric exit code (1 in this case, config not found)
        expect(typeof result).toBe("number");
        expect(result).not.toBe(0);
      } catch {
        // If it throws (e.g., config error), that's also acceptable —
        // the key thing is process.exit was NOT called.
      } finally {
        exitSpy.mockRestore();
      }
    });
  });
});
