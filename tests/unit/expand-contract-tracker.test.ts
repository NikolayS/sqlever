import { describe, it, expect, beforeEach, mock } from "bun:test";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as registry.test.ts
// ---------------------------------------------------------------------------

let mockInstances: MockPgClient[] = [];

/** Tracks queries with both text and values for assertion. */
interface QueryRecord {
  text: string;
  values?: unknown[];
}

class MockPgClient {
  options: Record<string, unknown>;
  queries: QueryRecord[] = [];
  connected = false;
  ended = false;

  /**
   * Map query text -> result. Supports exact match and prefix match.
   * If a handler function is provided, it receives (text, values) and
   * returns the result dynamically.
   */
  queryResults: Record<
    string,
    | { rows: unknown[]; rowCount: number; command: string }
    | ((text: string, values?: unknown[]) => { rows: unknown[]; rowCount: number; command: string })
  > = {};

  /** If set, the next query matching this text throws the given error. */
  queryErrors: Record<string, Error> = {};

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });

    // Check for programmed errors
    if (this.queryErrors[text]) {
      throw this.queryErrors[text];
    }

    // Check for exact match first
    const handler = this.queryResults[text];
    if (handler) {
      if (typeof handler === "function") {
        return handler(text, values);
      }
      return handler;
    }

    // Check for prefix match (useful for dynamic SQL)
    for (const [key, val] of Object.entries(this.queryResults)) {
      if (text.startsWith(key)) {
        if (typeof val === "function") {
          return val(text, values);
        }
        return val;
      }
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

// Import after mocking
const { DatabaseClient } = await import("../../src/db/client");
const {
  ExpandContractTracker,
  EXPAND_CONTRACT_DDL,
  EC_LOCK_NAMESPACE,
  VALID_TRANSITIONS,
} = await import("../../src/expand-contract/tracker");

// Also import types for type checking
import type {
  Phase,
  ExpandContractState,
  BackfillCheckInput,
} from "../../src/expand-contract/tracker";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function createConnectedClient(): Promise<InstanceType<typeof DatabaseClient>> {
  const client = new DatabaseClient("postgresql://host/db");
  await client.connect();
  return client;
}

function getPgClient(): MockPgClient {
  return mockInstances[mockInstances.length - 1]!;
}

function queryTexts(pgClient: MockPgClient): string[] {
  return pgClient.queries.map((q) => q.text);
}

/** Builds a mock ExpandContractState row. */
function mockStateRow(overrides: Partial<ExpandContractState> = {}): ExpandContractState {
  return {
    id: 1,
    change_name: "rename_users_name",
    project: "myproject",
    phase: "expanding" as Phase,
    table_schema: "public",
    table_name: "users",
    started_at: new Date("2025-06-01T10:00:00Z"),
    updated_at: new Date("2025-06-01T10:00:00Z"),
    started_by: "deployer@example.com",
    ...overrides,
  };
}

const sampleInput = {
  change_name: "rename_users_name",
  project: "myproject",
  table_schema: "public",
  table_name: "users",
  started_by: "deployer@example.com",
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("ExpandContractTracker", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  // -----------------------------------------------------------------------
  // DDL and constants
  // -----------------------------------------------------------------------

  describe("DDL and constants", () => {
    it("DDL creates sqlever schema and expand_contract_state table", () => {
      expect(EXPAND_CONTRACT_DDL).toContain("CREATE SCHEMA IF NOT EXISTS sqlever");
      expect(EXPAND_CONTRACT_DDL).toContain("CREATE TABLE IF NOT EXISTS sqlever.expand_contract_state");
    });

    it("DDL includes all required columns", () => {
      const requiredColumns = [
        "id", "change_name", "project", "phase",
        "table_schema", "table_name",
        "started_at", "updated_at", "started_by",
      ];
      for (const col of requiredColumns) {
        expect(EXPAND_CONTRACT_DDL).toContain(col);
      }
    });

    it("DDL includes CHECK constraint for valid phases", () => {
      expect(EXPAND_CONTRACT_DDL).toContain("expanding");
      expect(EXPAND_CONTRACT_DDL).toContain("expanded");
      expect(EXPAND_CONTRACT_DDL).toContain("contracting");
      expect(EXPAND_CONTRACT_DDL).toContain("completed");
      expect(EXPAND_CONTRACT_DDL).toContain("CHECK");
    });

    it("DDL includes UNIQUE constraint on (project, change_name)", () => {
      expect(EXPAND_CONTRACT_DDL).toContain("UNIQUE (project, change_name)");
    });

    it("VALID_TRANSITIONS defines correct state machine", () => {
      expect(VALID_TRANSITIONS.expanding).toEqual(["expanded"]);
      expect(VALID_TRANSITIONS.expanded).toEqual(["contracting"]);
      expect(VALID_TRANSITIONS.contracting).toEqual(["completed"]);
      expect(VALID_TRANSITIONS.completed).toEqual([]);
    });
  });

  // -----------------------------------------------------------------------
  // ensureSchema()
  // -----------------------------------------------------------------------

  describe("ensureSchema()", () => {
    it("acquires advisory lock, runs DDL, then releases lock", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      await tracker.ensureSchema();

      const texts = queryTexts(pgClient);
      // Should see: advisory_lock, DDL, advisory_unlock (after session setup queries)
      expect(texts).toContain("SELECT pg_advisory_lock($1)");
      expect(texts).toContain(EXPAND_CONTRACT_DDL);
      expect(texts).toContain("SELECT pg_advisory_unlock($1)");

      // Lock should be acquired before DDL and released after
      const lockIdx = texts.indexOf("SELECT pg_advisory_lock($1)");
      const ddlIdx = texts.indexOf(EXPAND_CONTRACT_DDL);
      const unlockIdx = texts.indexOf("SELECT pg_advisory_unlock($1)");
      expect(lockIdx).toBeLessThan(ddlIdx);
      expect(ddlIdx).toBeLessThan(unlockIdx);
    });

    it("releases advisory lock even if DDL fails", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryErrors[EXPAND_CONTRACT_DDL] = new Error("DDL failed");

      await expect(tracker.ensureSchema()).rejects.toThrow("DDL failed");

      const texts = queryTexts(pgClient);
      expect(texts).toContain("SELECT pg_advisory_unlock($1)");
    });
  });

  // -----------------------------------------------------------------------
  // createOperation()
  // -----------------------------------------------------------------------

  describe("createOperation()", () => {
    it("inserts a new operation in expanding phase", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      const row = mockStateRow();

      // Mock: no existing active operation
      // Mock: INSERT returns new row
      pgClient.queryResults["INSERT INTO sqlever.expand_contract_state"] = {
        rows: [row],
        rowCount: 1,
        command: "INSERT",
      };

      const result = await tracker.createOperation(sampleInput);

      expect(result.change_name).toBe("rename_users_name");
      expect(result.project).toBe("myproject");
      expect(result.phase).toBe("expanding");

      // Verify INSERT was executed with correct params
      const insertQuery = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqlever.expand_contract_state"),
      );
      expect(insertQuery).toBeDefined();
      expect(insertQuery!.values).toEqual([
        "rename_users_name",
        "myproject",
        "public",
        "users",
        "deployer@example.com",
      ]);
    });

    it("throws if active operation already exists", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      // Mock: existing active operation found
      pgClient.queryResults["SELECT id, change_name, project, phase"] = (text: string) => {
        if (text.includes("phase != 'completed'")) {
          return {
            rows: [mockStateRow({ phase: "expanding" as Phase })],
            rowCount: 1,
            command: "SELECT",
          };
        }
        return { rows: [], rowCount: 0, command: "SELECT" };
      };

      await expect(tracker.createOperation(sampleInput)).rejects.toThrow(
        /Active expand\/contract operation already exists/,
      );
    });
  });

  // -----------------------------------------------------------------------
  // getOperation()
  // -----------------------------------------------------------------------

  describe("getOperation()", () => {
    it("returns operation by ID", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      const row = mockStateRow({ id: 42 });
      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [row],
        rowCount: 1,
        command: "SELECT",
      };

      const result = await tracker.getOperation(42);
      expect(result).not.toBeNull();
      expect(result!.id).toBe(42);
    });

    it("returns null for non-existent operation", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      const result = await tracker.getOperation(999);
      expect(result).toBeNull();
    });
  });

  // -----------------------------------------------------------------------
  // getOperationByName()
  // -----------------------------------------------------------------------

  describe("getOperationByName()", () => {
    it("returns operation by project and change name", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      const row = mockStateRow();
      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [row],
        rowCount: 1,
        command: "SELECT",
      };

      const result = await tracker.getOperationByName("myproject", "rename_users_name");
      expect(result).not.toBeNull();
      expect(result!.change_name).toBe("rename_users_name");
    });
  });

  // -----------------------------------------------------------------------
  // listOperations()
  // -----------------------------------------------------------------------

  describe("listOperations()", () => {
    it("lists all operations for a project", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      const rows = [
        mockStateRow({ id: 1, change_name: "op1" }),
        mockStateRow({ id: 2, change_name: "op2", phase: "completed" as Phase }),
      ];
      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows,
        rowCount: 2,
        command: "SELECT",
      };

      const result = await tracker.listOperations("myproject");
      expect(result).toHaveLength(2);
    });

    it("lists operations filtered by phase", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      const rows = [mockStateRow({ phase: "expanding" as Phase })];
      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows,
        rowCount: 1,
        command: "SELECT",
      };

      const result = await tracker.listOperations("myproject", "expanding");
      expect(result).toHaveLength(1);

      // Verify the query includes phase filter
      const query = pgClient.queries.find(
        (q) => q.text.includes("phase = $2"),
      );
      expect(query).toBeDefined();
      expect(query!.values).toEqual(["myproject", "expanding"]);
    });
  });

  // -----------------------------------------------------------------------
  // listActiveOperations()
  // -----------------------------------------------------------------------

  describe("listActiveOperations()", () => {
    it("returns only non-completed operations", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      const rows = [
        mockStateRow({ id: 1, phase: "expanding" as Phase }),
        mockStateRow({ id: 2, phase: "expanded" as Phase }),
      ];
      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows,
        rowCount: 2,
        command: "SELECT",
      };

      const result = await tracker.listActiveOperations("myproject");
      expect(result).toHaveLength(2);

      // Verify query filters out completed
      const query = pgClient.queries.find(
        (q) => q.text.includes("phase != 'completed'"),
      );
      expect(query).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // deleteOperation()
  // -----------------------------------------------------------------------

  describe("deleteOperation()", () => {
    it("deletes a completed operation", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ phase: "completed" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };
      pgClient.queryResults["DELETE FROM sqlever.expand_contract_state"] = {
        rows: [],
        rowCount: 1,
        command: "DELETE",
      };

      const result = await tracker.deleteOperation(1);
      expect(result).toBe(true);
    });

    it("deletes an expanding operation (cancel)", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ phase: "expanding" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };
      pgClient.queryResults["DELETE FROM sqlever.expand_contract_state"] = {
        rows: [],
        rowCount: 1,
        command: "DELETE",
      };

      const result = await tracker.deleteOperation(1);
      expect(result).toBe(true);
    });

    it("throws when deleting expanded operation", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ phase: "expanded" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };

      await expect(tracker.deleteOperation(1)).rejects.toThrow(
        /Cannot delete operation.*"expanded"/,
      );
    });

    it("throws when deleting contracting operation", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ phase: "contracting" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };

      await expect(tracker.deleteOperation(1)).rejects.toThrow(
        /Cannot delete operation.*"contracting"/,
      );
    });

    it("returns false for non-existent operation", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      const result = await tracker.deleteOperation(999);
      expect(result).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // validateTransition()
  // -----------------------------------------------------------------------

  describe("validateTransition()", () => {
    it("allows expanding -> expanded", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      expect(() => tracker.validateTransition("expanding", "expanded")).not.toThrow();
    });

    it("allows expanded -> contracting", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      expect(() => tracker.validateTransition("expanded", "contracting")).not.toThrow();
    });

    it("allows contracting -> completed", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      expect(() => tracker.validateTransition("contracting", "completed")).not.toThrow();
    });

    it("rejects expanding -> contracting (skip)", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      expect(() => tracker.validateTransition("expanding", "contracting")).toThrow(
        /Invalid phase transition.*"expanding".*"contracting"/,
      );
    });

    it("rejects completed -> expanding (cycle)", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      expect(() => tracker.validateTransition("completed", "expanding")).toThrow(
        /Invalid phase transition.*"completed".*"expanding"/,
      );
    });

    it("rejects expanded -> expanding (backward)", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      expect(() => tracker.validateTransition("expanded", "expanding")).toThrow(
        /Invalid phase transition/,
      );
    });

    it("error message includes valid targets for non-terminal state", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      try {
        tracker.validateTransition("expanding", "completed");
        expect(true).toBe(false); // should not reach
      } catch (err: unknown) {
        const msg = (err as Error).message;
        expect(msg).toContain("expanded");
      }
    });

    it("error message says 'none' for terminal state", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      try {
        tracker.validateTransition("completed", "expanding");
        expect(true).toBe(false); // should not reach
      } catch (err: unknown) {
        const msg = (err as Error).message;
        expect(msg).toContain("none (terminal state)");
      }
    });
  });

  // -----------------------------------------------------------------------
  // transitionPhase()
  // -----------------------------------------------------------------------

  describe("transitionPhase()", () => {
    it("transitions expanding -> expanded with advisory lock", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      // Mock: lock acquired
      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: true }],
        rowCount: 1,
        command: "SELECT",
      };

      // Mock: current state is expanding
      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ id: 1, phase: "expanding" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };

      // Mock: update returns new state
      pgClient.queryResults["UPDATE sqlever.expand_contract_state"] = {
        rows: [mockStateRow({ id: 1, phase: "expanded" as Phase })],
        rowCount: 1,
        command: "UPDATE",
      };

      const result = await tracker.transitionPhase(1, "expanded");
      expect(result.phase).toBe("expanded");

      // Verify advisory lock was acquired and released
      const texts = queryTexts(pgClient);
      expect(texts).toContain("SELECT pg_try_advisory_lock($1, $2)");
      expect(texts).toContain("SELECT pg_advisory_unlock($1, $2)");
    });

    it("throws on lock contention (advisory lock not acquired)", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      // Mock: lock NOT acquired (another process holds it)
      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: false }],
        rowCount: 1,
        command: "SELECT",
      };

      await expect(tracker.transitionPhase(1, "expanded")).rejects.toThrow(
        /another process is currently performing a phase transition/,
      );
    });

    it("throws if operation not found", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: true }],
        rowCount: 1,
        command: "SELECT",
      };

      // No rows returned for getOperation
      await expect(tracker.transitionPhase(999, "expanded")).rejects.toThrow(
        /Operation 999 not found/,
      );

      // Lock should still be released
      const texts = queryTexts(pgClient);
      expect(texts).toContain("SELECT pg_advisory_unlock($1, $2)");
    });

    it("releases advisory lock even on transition error", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: true }],
        rowCount: 1,
        command: "SELECT",
      };

      // Current state is expanding, trying invalid transition
      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ id: 1, phase: "expanding" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };

      await expect(tracker.transitionPhase(1, "contracting")).rejects.toThrow(
        /Invalid phase transition/,
      );

      // Lock should still be released
      const texts = queryTexts(pgClient);
      expect(texts).toContain("SELECT pg_advisory_unlock($1, $2)");
    });

    it("requires backfill check when transitioning to contracting", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: true }],
        rowCount: 1,
        command: "SELECT",
      };

      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ id: 1, phase: "expanded" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };

      // No backfillCheck provided
      await expect(tracker.transitionPhase(1, "contracting")).rejects.toThrow(
        /Backfill check is required/,
      );
    });

    it("rejects transition to contracting when backfill is incomplete", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: true }],
        rowCount: 1,
        command: "SELECT",
      };

      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ id: 1, phase: "expanded" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };

      // Mock: backfill counts — 100 total, only 80 backfilled
      pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = (text: string) => {
        if (text.includes("IS NOT NULL")) {
          return { rows: [{ cnt: 80 }], rowCount: 1, command: "SELECT" };
        }
        return { rows: [{ cnt: 100 }], rowCount: 1, command: "SELECT" };
      };

      const backfillCheck: BackfillCheckInput = {
        table_schema: "public",
        table_name: "users",
        new_column: "full_name",
      };

      await expect(
        tracker.transitionPhase(1, "contracting", backfillCheck),
      ).rejects.toThrow(/backfill is not complete.*80\/100/);
    });

    it("allows transition to contracting when backfill is complete", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: true }],
        rowCount: 1,
        command: "SELECT",
      };

      pgClient.queryResults["SELECT id, change_name, project, phase"] = {
        rows: [mockStateRow({ id: 1, phase: "expanded" as Phase })],
        rowCount: 1,
        command: "SELECT",
      };

      // Mock: backfill complete — 100 total, 100 backfilled
      pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = (text: string) => {
        if (text.includes("IS NOT NULL")) {
          return { rows: [{ cnt: 100 }], rowCount: 1, command: "SELECT" };
        }
        return { rows: [{ cnt: 100 }], rowCount: 1, command: "SELECT" };
      };

      pgClient.queryResults["UPDATE sqlever.expand_contract_state"] = {
        rows: [mockStateRow({ id: 1, phase: "contracting" as Phase })],
        rowCount: 1,
        command: "UPDATE",
      };

      const backfillCheck: BackfillCheckInput = {
        table_schema: "public",
        table_name: "users",
        new_column: "full_name",
      };

      const result = await tracker.transitionPhase(1, "contracting", backfillCheck);
      expect(result.phase).toBe("contracting");
    });
  });

  // -----------------------------------------------------------------------
  // checkBackfill()
  // -----------------------------------------------------------------------

  describe("checkBackfill()", () => {
    it("reports complete backfill when counts match", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = (text: string) => {
        if (text.includes("IS NOT NULL")) {
          return { rows: [{ cnt: 50 }], rowCount: 1, command: "SELECT" };
        }
        return { rows: [{ cnt: 50 }], rowCount: 1, command: "SELECT" };
      };

      const result = await tracker.checkBackfill({
        table_schema: "public",
        table_name: "users",
        new_column: "full_name",
      });

      expect(result.total_rows).toBe(50);
      expect(result.backfilled_rows).toBe(50);
      expect(result.is_complete).toBe(true);
    });

    it("reports incomplete backfill when counts differ", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = (text: string) => {
        if (text.includes("IS NOT NULL")) {
          return { rows: [{ cnt: 25 }], rowCount: 1, command: "SELECT" };
        }
        return { rows: [{ cnt: 100 }], rowCount: 1, command: "SELECT" };
      };

      const result = await tracker.checkBackfill({
        table_schema: "public",
        table_name: "users",
        new_column: "full_name",
      });

      expect(result.total_rows).toBe(100);
      expect(result.backfilled_rows).toBe(25);
      expect(result.is_complete).toBe(false);
    });

    it("handles empty table (0 rows = complete)", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = {
        rows: [{ cnt: 0 }],
        rowCount: 1,
        command: "SELECT",
      };

      const result = await tracker.checkBackfill({
        table_schema: "public",
        table_name: "empty_table",
        new_column: "new_col",
      });

      expect(result.total_rows).toBe(0);
      expect(result.backfilled_rows).toBe(0);
      expect(result.is_complete).toBe(true);
    });

    it("ignores source_filter to prevent SQL injection", async () => {
      // SECURITY: source_filter was removed because it allowed raw SQL
      // injection. This test verifies that even when provided, it is
      // not included in the generated queries.
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = {
        rows: [{ cnt: 10 }],
        rowCount: 1,
        command: "SELECT",
      };

      await tracker.checkBackfill({
        table_schema: "public",
        table_name: "users",
        new_column: "full_name",
        source_filter: "name IS NOT NULL",
      });

      // Neither query should include the source_filter text
      const countQueries = pgClient.queries.filter((q) =>
        q.text.includes("COUNT(*)"),
      );
      expect(countQueries.length).toBeGreaterThanOrEqual(2);

      for (const query of countQueries) {
        expect(query.text).not.toContain("name IS NOT NULL");
      }
    });

    it("escapes identifiers to prevent SQL injection", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = {
        rows: [{ cnt: 0 }],
        rowCount: 1,
        command: "SELECT",
      };

      await tracker.checkBackfill({
        table_schema: "my schema",
        table_name: 'table"name',
        new_column: "col",
      });

      // Verify identifiers are properly quoted
      const countQuery = pgClient.queries.find((q) =>
        q.text.includes("COUNT(*)"),
      );
      expect(countQuery?.text).toContain('"my schema"');
      expect(countQuery?.text).toContain('"table""name"');
    });
  });

  // -----------------------------------------------------------------------
  // Advisory locking
  // -----------------------------------------------------------------------

  describe("advisory locking", () => {
    it("tryAcquireLock returns true when lock is acquired", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: true }],
        rowCount: 1,
        command: "SELECT",
      };

      const acquired = await tracker.tryAcquireLock(1);
      expect(acquired).toBe(true);

      // Verify lock parameters
      const lockQuery = pgClient.queries.find((q) =>
        q.text === "SELECT pg_try_advisory_lock($1, $2)",
      );
      expect(lockQuery?.values).toEqual([EC_LOCK_NAMESPACE, 1]);
    });

    it("tryAcquireLock returns false when lock is held", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
        rows: [{ pg_try_advisory_lock: false }],
        rowCount: 1,
        command: "SELECT",
      };

      const acquired = await tracker.tryAcquireLock(1);
      expect(acquired).toBe(false);
    });

    it("releaseLock calls pg_advisory_unlock with correct params", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);
      const pgClient = getPgClient();

      await tracker.releaseLock(42);

      const unlockQuery = pgClient.queries.find((q) =>
        q.text === "SELECT pg_advisory_unlock($1, $2)",
      );
      expect(unlockQuery).toBeDefined();
      expect(unlockQuery!.values).toEqual([EC_LOCK_NAMESPACE, 42]);
    });
  });

  // -----------------------------------------------------------------------
  // Full lifecycle
  // -----------------------------------------------------------------------

  describe("full lifecycle", () => {
    it("validates complete state machine: expanding -> expanded -> contracting -> completed", async () => {
      const db = await createConnectedClient();
      const tracker = new ExpandContractTracker(db);

      // All valid forward transitions should not throw
      expect(() => tracker.validateTransition("expanding", "expanded")).not.toThrow();
      expect(() => tracker.validateTransition("expanded", "contracting")).not.toThrow();
      expect(() => tracker.validateTransition("contracting", "completed")).not.toThrow();

      // All backward transitions should throw
      expect(() => tracker.validateTransition("expanded", "expanding")).toThrow();
      expect(() => tracker.validateTransition("contracting", "expanded")).toThrow();
      expect(() => tracker.validateTransition("completed", "contracting")).toThrow();

      // All skip transitions should throw
      expect(() => tracker.validateTransition("expanding", "contracting")).toThrow();
      expect(() => tracker.validateTransition("expanding", "completed")).toThrow();
      expect(() => tracker.validateTransition("expanded", "completed")).toThrow();
    });
  });
});
