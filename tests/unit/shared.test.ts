import { describe, it, expect, beforeEach, mock } from "bun:test";
import { resetConfig } from "../../src/output";

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
const { resolveTargetUri, withDatabase } = await import(
  "../../src/commands/shared"
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("shared command helpers", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  // -----------------------------------------------------------------------
  // resolveTargetUri
  // -----------------------------------------------------------------------

  describe("resolveTargetUri()", () => {
    it("returns dbUri when provided", () => {
      const uri = resolveTargetUri(
        { core: {}, targets: {}, engines: {} } as never,
        "postgresql://host/db",
      );
      expect(uri).toBe("postgresql://host/db");
    });

    it("returns null when dbUri is undefined and no config", () => {
      const uri = resolveTargetUri(
        { core: {}, targets: {}, engines: {} } as never,
      );
      expect(uri).toBeNull();
    });

    it("looks up named target from config", () => {
      const uri = resolveTargetUri(
        {
          core: {},
          targets: { prod: { name: "prod", uri: "postgresql://prod/db" } },
          engines: {},
        } as never,
        undefined,
        "prod",
      );
      expect(uri).toBe("postgresql://prod/db");
    });

    it("treats target name as URI if it contains ://", () => {
      const uri = resolveTargetUri(
        { core: {}, targets: {}, engines: {} } as never,
        undefined,
        "postgresql://direct/db",
      );
      expect(uri).toBe("postgresql://direct/db");
    });

    it("returns null for unknown target name without ://", () => {
      const uri = resolveTargetUri(
        { core: {}, targets: {}, engines: {} } as never,
        undefined,
        "nonexistent",
      );
      expect(uri).toBeNull();
    });

    it("falls back to engine target (named target)", () => {
      const uri = resolveTargetUri(
        {
          core: { engine: "pg" },
          targets: { mydb: { name: "mydb", uri: "postgresql://mydb/x" } },
          engines: { pg: { name: "pg", target: "mydb" } },
        } as never,
      );
      expect(uri).toBe("postgresql://mydb/x");
    });

    it("falls back to engine target (URI string)", () => {
      const uri = resolveTargetUri(
        {
          core: { engine: "pg" },
          targets: {},
          engines: { pg: { name: "pg", target: "db:pg://local/mydb" } },
        } as never,
      );
      expect(uri).toBe("db:pg://local/mydb");
    });

    it("dbUri takes precedence over target name", () => {
      const uri = resolveTargetUri(
        {
          core: {},
          targets: { prod: { name: "prod", uri: "postgresql://prod/db" } },
          engines: {},
        } as never,
        "postgresql://explicit/db",
        "prod",
      );
      expect(uri).toBe("postgresql://explicit/db");
    });
  });

  // -----------------------------------------------------------------------
  // withDatabase
  // -----------------------------------------------------------------------

  describe("withDatabase()", () => {
    it("connects, runs callback, and disconnects", async () => {
      let callbackCalled = false;

      await withDatabase(
        "postgresql://host/db",
        { command: "test" },
        async (db) => {
          callbackCalled = true;
          expect(db.isConnected).toBe(true);
        },
      );

      expect(callbackCalled).toBe(true);
      // Client should be disconnected after withDatabase returns
      const pgClient = mockInstances[mockInstances.length - 1]!;
      expect(pgClient.ended).toBe(true);
    });

    it("returns the callback's return value", async () => {
      const result = await withDatabase(
        "postgresql://host/db",
        { command: "test" },
        async () => {
          return 42;
        },
      );

      expect(result).toBe(42);
    });

    it("disconnects even when callback throws", async () => {
      const err = new Error("boom");

      try {
        await withDatabase(
          "postgresql://host/db",
          { command: "test" },
          async () => {
            throw err;
          },
        );
        // Should not reach here
        expect(true).toBe(false);
      } catch (e) {
        expect(e).toBe(err);
      }

      const pgClient = mockInstances[mockInstances.length - 1]!;
      expect(pgClient.ended).toBe(true);
    });

    it("passes session settings to DatabaseClient", async () => {
      await withDatabase(
        "postgresql://host/db",
        { command: "deploy", project: "myproject" },
        async () => {},
      );

      // The mock client should have received SET commands for session settings
      const pgClient = mockInstances[mockInstances.length - 1]!;
      const setQueries = pgClient.queries.filter((q) =>
        q.text.startsWith("SET ") || q.text.includes("application_name"),
      );
      expect(setQueries.length).toBeGreaterThan(0);
    });
  });
});
