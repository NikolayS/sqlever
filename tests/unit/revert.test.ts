import { describe, it, expect, beforeEach, mock } from "bun:test";
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

  async query(text: string, values?: unknown[]) {
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
  resolveTargetUri,
} = await import("../../src/commands/revert");
const { parseArgs } = await import("../../src/cli");

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
        { dbUri: "postgresql://host/db", noPrompt: false, topDir: "." },
        { targets: {}, engines: {} } as never,
      );
      expect(uri).toBe("postgresql://host/db");
    });

    it("looks up named target from config", () => {
      const uri = resolveTargetUri(
        { target: "prod", noPrompt: false, topDir: "." },
        {
          targets: { prod: { name: "prod", uri: "postgresql://prod/db" } },
          engines: {},
        } as never,
      );
      expect(uri).toBe("postgresql://prod/db");
    });

    it("falls back to engine target string", () => {
      const uri = resolveTargetUri(
        { noPrompt: false, topDir: "." },
        {
          targets: {},
          engines: { pg: { name: "pg", target: "db:pg://local/mydb" } },
        } as never,
      );
      expect(uri).toBe("db:pg://local/mydb");
    });

    it("returns undefined when no target configured", () => {
      const uri = resolveTargetUri(
        { noPrompt: false, topDir: "." },
        { targets: {}, engines: {} } as never,
      );
      expect(uri).toBeUndefined();
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
});
