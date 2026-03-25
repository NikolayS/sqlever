import { describe, it, expect, beforeEach, mock } from "bun:test";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — identical pattern to batch-queue.test.ts
// ---------------------------------------------------------------------------

let mockInstances: MockPgClient[] = [];

class MockPgClient {
  options: Record<string, unknown>;
  queries: Array<{ text: string; values?: unknown[] }> = [];
  connected = false;
  ended = false;
  queryResults: Record<string, { rows: unknown[]; rowCount: number; command: string }> = {};
  queryShouldFail: Record<string, Error> = {};

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });
    if (this.queryShouldFail[text]) {
      throw this.queryShouldFail[text];
    }
    return (
      this.queryResults[text] ?? { rows: [], rowCount: 0, command: "SELECT" }
    );
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

const { DatabaseClient } = await import("../../src/db/client");
const {
  DEFAULT_BATCH_SIZE,
  DEFAULT_SLEEP_MS,
  DEFAULT_MAX_RETRIES,
} = await import("../../src/batch/queue");

const {
  BatchWorker,
  DEFAULT_LOCK_TIMEOUT_MS,
  DEFAULT_STATEMENT_TIMEOUT_MS,
  DEFAULT_SEARCH_PATH,
} = await import("../../src/batch/worker");

import type { BatchJob, PartitionId } from "../../src/batch/queue";
import type {
  DmlExecutor,
  SignalCheckFn,
  BatchWorkerConfig,
} from "../../src/batch/worker";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function makeClient(): Promise<InstanceType<typeof DatabaseClient>> {
  const client = new DatabaseClient("postgresql://user@localhost/testdb");
  await client.connect();
  return client;
}

function latestPgClient(): MockPgClient {
  return mockInstances[mockInstances.length - 1]!;
}

function findQuery(pgClient: MockPgClient, pattern: string | RegExp) {
  return pgClient.queries.find((q) =>
    typeof pattern === "string"
      ? q.text.includes(pattern)
      : pattern.test(q.text),
  );
}

function allQueriesMatching(pgClient: MockPgClient, pattern: string | RegExp) {
  return pgClient.queries.filter((q) =>
    typeof pattern === "string"
      ? q.text.includes(pattern)
      : pattern.test(q.text),
  );
}

/** Create a mock job row for test returns. */
function mockJob(overrides: Partial<BatchJob> = {}): BatchJob {
  return {
    id: 1,
    name: "backfill_tiers",
    status: "running",
    partition_id: 0 as PartitionId,
    table_name: "users",
    batch_size: DEFAULT_BATCH_SIZE,
    sleep_ms: DEFAULT_SLEEP_MS,
    last_pk: null,
    attempt: 1,
    max_retries: DEFAULT_MAX_RETRIES,
    error_message: null,
    heartbeat_at: new Date(),
    created_at: new Date("2025-01-01"),
    updated_at: new Date("2025-01-01"),
    ...overrides,
  };
}

/**
 * Set up a MockPgClient so that queries matching a pattern return specific
 * rows. The response is consumed on first match and subsequent matches
 * for the same pattern cycle through.
 */
function setupQueryResponses(
  pgClient: MockPgClient,
  responses: Array<{
    pattern: string | RegExp;
    rows: unknown[];
    rowCount?: number;
    command?: string;
  }>,
) {
  const origQuery = pgClient.query.bind(pgClient);
  pgClient.query = async (text: string, values?: unknown[]) => {
    for (const r of responses) {
      const match =
        typeof r.pattern === "string"
          ? text.includes(r.pattern)
          : r.pattern.test(text);
      if (match) {
        pgClient.queries.push({ text, values });
        return {
          rows: r.rows,
          rowCount: r.rowCount ?? r.rows.length,
          command: r.command ?? "SELECT",
        };
      }
    }
    return origQuery(text, values);
  };
}

/** No-op sleep for tests. */
const instantSleep = async (_ms: number) => {};

/** DML executor that returns a fixed number of rows affected per call. */
function fixedDml(
  schedule: Array<{ rowsAffected: number; lastPk: string | null }>,
): DmlExecutor {
  let callIndex = 0;
  return async (_db, _job, _lastPk) => {
    const result = schedule[callIndex] ?? { rowsAffected: 0, lastPk: null };
    callIndex++;
    return result;
  };
}

/** DML executor that throws on the Nth call. */
function failingDml(failOnCall: number, errorMsg: string): DmlExecutor {
  let callIndex = 0;
  return async (_db, _job, _lastPk) => {
    callIndex++;
    if (callIndex === failOnCall) {
      throw new Error(errorMsg);
    }
    return { rowsAffected: 100, lastPk: String(callIndex * 100) };
  };
}

/**
 * Set up standard query responses for a worker run.
 * This sets up the dequeue, heartbeat, progress update, and completion queries.
 */
function setupWorkerQueries(
  pgClient: MockPgClient,
  job: BatchJob,
  extraResponses: Array<{
    pattern: string | RegExp;
    rows: unknown[];
    rowCount?: number;
    command?: string;
  }> = [],
) {
  setupQueryResponses(pgClient, [
    // getActivePartition
    { pattern: "batch_queue_meta", rows: [{ value: String(job.partition_id) }] },
    // dequeueJob (SKIP LOCKED)
    { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
    // BEGIN/COMMIT
    { pattern: "BEGIN", rows: [], command: "BEGIN" },
    { pattern: "COMMIT", rows: [], command: "COMMIT" },
    { pattern: "ROLLBACK", rows: [], command: "ROLLBACK" },
    // SET statements (batch settings)
    { pattern: "SET lock_timeout", rows: [] },
    { pattern: "SET statement_timeout", rows: [] },
    { pattern: "SET search_path", rows: [] },
    // updateHeartbeat
    {
      pattern: "heartbeat_at = now()",
      rows: [],
      rowCount: 1,
      command: "UPDATE",
    },
    // updateJobProgress (last_pk update)
    {
      pattern: /UPDATE.*last_pk = \$1/,
      rows: [],
      rowCount: 1,
      command: "UPDATE",
    },
    // completeJob: getJob (SELECT) then transition (UPDATE)
    { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
    {
      pattern: /UPDATE.*status = \$3/,
      rows: [{ ...job, status: "done" }],
      rowCount: 1,
      command: "UPDATE",
    },
    // failJob: this is also used for pause/cancel
    ...extraResponses,
  ]);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("batch/worker", () => {
  beforeEach(() => {
    mockInstances = [];
  });

  // -----------------------------------------------------------------------
  // Constants
  // -----------------------------------------------------------------------

  describe("constants", () => {
    it("DEFAULT_LOCK_TIMEOUT_MS is 5000 (5 seconds)", () => {
      expect(DEFAULT_LOCK_TIMEOUT_MS).toBe(5_000);
    });

    it("DEFAULT_STATEMENT_TIMEOUT_MS is 0 (disabled)", () => {
      expect(DEFAULT_STATEMENT_TIMEOUT_MS).toBe(0);
    });

    it("DEFAULT_SEARCH_PATH is 'public'", () => {
      expect(DEFAULT_SEARCH_PATH).toBe("public");
    });
  });

  // -----------------------------------------------------------------------
  // No work available
  // -----------------------------------------------------------------------

  describe("no pending jobs", () => {
    it("returns no_work when queue is empty", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [] },
      ]);

      const dml = fixedDml([]);
      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      const result = await worker.run();

      expect(result.status).toBe("no_work");
      expect(result.jobId).toBeNull();
      expect(result.batchesProcessed).toBe(0);
      expect(result.totalRowsAffected).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // Basic batch execution
  // -----------------------------------------------------------------------

  describe("batch execution loop", () => {
    it("processes batches until DML returns 0 rows", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      // 3 batches of 100 rows, then 0 (done)
      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 100, lastPk: "200" },
        { rowsAffected: 100, lastPk: "300" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      const result = await worker.run();

      expect(result.status).toBe("completed");
      expect(result.jobId).toBe(1);
      expect(result.batchesProcessed).toBe(4);
      expect(result.totalRowsAffected).toBe(300);
      expect(result.lastPk).toBe("300");
    });

    it("completes immediately when first batch returns 0 rows", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([{ rowsAffected: 0, lastPk: null }]);

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      const result = await worker.run();

      expect(result.status).toBe("completed");
      expect(result.batchesProcessed).toBe(1);
      expect(result.totalRowsAffected).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // PK resume tracking (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  describe("PK resume tracking", () => {
    it("resumes from job's last_pk when retrying", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      // Job was previously interrupted at PK "500"
      const job = mockJob({ last_pk: "500" });
      setupWorkerQueries(pgClient, job);

      let receivedLastPk: string | null = null;
      const dml: DmlExecutor = async (_db, _job, lastPk) => {
        receivedLastPk = lastPk;
        return { rowsAffected: 0, lastPk: null };
      };

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      await worker.run();

      // The DML executor should have received "500" as the starting PK
      expect(receivedLastPk as string | null).toBe("500");
    });

    it("passes updated lastPk to successive batch calls", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const receivedPks: Array<string | null> = [];
      let callIndex = 0;
      const dml: DmlExecutor = async (_db, _job, lastPk) => {
        receivedPks.push(lastPk);
        callIndex++;
        if (callIndex <= 3) {
          return { rowsAffected: 50, lastPk: String(callIndex * 50) };
        }
        return { rowsAffected: 0, lastPk: null };
      };

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      await worker.run();

      expect(receivedPks).toEqual([null, "50", "100", "150"]);
    });

    it("updates last_pk in job row after each batch", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      await worker.run();

      // Verify a last_pk update query was issued
      const pkUpdate = allQueriesMatching(pgClient, /last_pk = \$1/);
      expect(pkUpdate.length).toBeGreaterThanOrEqual(1);
      // The first update should have set last_pk to "100"
      const firstUpdate = pkUpdate.find(
        (q) => q.values && q.values[0] === "100",
      );
      expect(firstUpdate).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // SET statements per batch (DD13, SPEC Section 5.5)
  // -----------------------------------------------------------------------

  describe("per-batch SET statements", () => {
    it("re-issues SET lock_timeout at the start of each batch", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([
        { rowsAffected: 50, lastPk: "50" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(
        client,
        dml,
        { lockTimeoutMs: 3000 },
        undefined,
        instantSleep,
      );
      await worker.run();

      const setQueries = allQueriesMatching(pgClient, "SET lock_timeout");
      // Should have at least 2 SET lock_timeout queries (one per batch)
      // plus the one from initial session setup
      expect(setQueries.length).toBeGreaterThanOrEqual(2);
      // Verify the value used
      const batchSet = setQueries.find((q) => q.text.includes("3000"));
      expect(batchSet).toBeDefined();
    });

    it("re-issues SET statement_timeout at the start of each batch", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([{ rowsAffected: 0, lastPk: null }]);

      const worker = new BatchWorker(
        client,
        dml,
        { statementTimeoutMs: 30000 },
        undefined,
        instantSleep,
      );
      await worker.run();

      const setQueries = allQueriesMatching(pgClient, "SET statement_timeout");
      const batchSet = setQueries.find((q) => q.text.includes("30000"));
      expect(batchSet).toBeDefined();
    });

    it("re-issues SET search_path at the start of each batch", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([{ rowsAffected: 0, lastPk: null }]);

      const worker = new BatchWorker(
        client,
        dml,
        { searchPath: "myschema" },
        undefined,
        instantSleep,
      );
      await worker.run();

      const setQueries = allQueriesMatching(pgClient, "SET search_path");
      const batchSet = setQueries.find((q) => q.text.includes("myschema"));
      expect(batchSet).toBeDefined();
    });

    it("uses default search_path of 'public' when not configured", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([{ rowsAffected: 0, lastPk: null }]);

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      await worker.run();

      const setQueries = allQueriesMatching(pgClient, "SET search_path");
      const batchSet = setQueries.find((q) => q.text.includes("public"));
      expect(batchSet).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // Per-batch transactions (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  describe("per-batch transactions", () => {
    it("wraps each batch in BEGIN/COMMIT", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      await worker.run();

      const begins = allQueriesMatching(pgClient, "BEGIN");
      const commits = allQueriesMatching(pgClient, "COMMIT");
      // At least 2 batches -> 2 BEGINs and 2 COMMITs
      expect(begins.length).toBeGreaterThanOrEqual(2);
      expect(commits.length).toBeGreaterThanOrEqual(2);
    });
  });

  // -----------------------------------------------------------------------
  // Heartbeat updates (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  describe("heartbeat updates", () => {
    it("updates heartbeat after each batch", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 100, lastPk: "200" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      await worker.run();

      const heartbeats = allQueriesMatching(pgClient, "heartbeat_at = now()");
      // Should have at least 3 heartbeat updates (one per batch)
      expect(heartbeats.length).toBeGreaterThanOrEqual(3);
    });
  });

  // -----------------------------------------------------------------------
  // Sleep/throttling (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  describe("throttling behavior", () => {
    it("sleeps for configured interval between batches", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ sleep_ms: 200 });

      setupWorkerQueries(pgClient, job);

      const sleepCalls: number[] = [];
      const trackingSleep = async (ms: number) => {
        sleepCalls.push(ms);
      };

      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 100, lastPk: "200" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(
        client,
        dml,
        {},
        undefined,
        trackingSleep,
      );
      await worker.run();

      // Sleep called between batches (not after the final 0-row batch)
      // Batches: [100 rows -> sleep] [100 rows -> sleep] [0 rows -> done]
      expect(sleepCalls.length).toBe(2);
      expect(sleepCalls[0]).toBe(200);
      expect(sleepCalls[1]).toBe(200);
    });

    it("uses config sleepMs over job sleep_ms", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ sleep_ms: 200 });

      setupWorkerQueries(pgClient, job);

      const sleepCalls: number[] = [];
      const trackingSleep = async (ms: number) => {
        sleepCalls.push(ms);
      };

      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(
        client,
        dml,
        { sleepMs: 500 },
        undefined,
        trackingSleep,
      );
      await worker.run();

      expect(sleepCalls[0]).toBe(500);
    });

    it("does not sleep after the final batch (0 rows)", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const sleepCalls: number[] = [];
      const trackingSleep = async (ms: number) => {
        sleepCalls.push(ms);
      };

      const dml = fixedDml([{ rowsAffected: 0, lastPk: null }]);

      const worker = new BatchWorker(
        client,
        dml,
        {},
        undefined,
        trackingSleep,
      );
      await worker.run();

      // No sleep because the first batch returned 0 rows -> immediate done
      expect(sleepCalls.length).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // Configurable batch_size (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  describe("configurable batch_size", () => {
    it("passes config batchSize to DML executor over job default", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ batch_size: 1000 });

      setupWorkerQueries(pgClient, job);

      let receivedBatchSize = 0;
      const dml: DmlExecutor = async (_db, receivedJob, _lastPk) => {
        receivedBatchSize = receivedJob.batch_size;
        return { rowsAffected: 0, lastPk: null };
      };

      const worker = new BatchWorker(
        client,
        dml,
        { batchSize: 250 },
        undefined,
        instantSleep,
      );
      await worker.run();

      expect(receivedBatchSize).toBe(250);
    });

    it("falls back to job batch_size when config does not specify", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ batch_size: 777 });

      setupWorkerQueries(pgClient, job);

      let receivedBatchSize = 0;
      const dml: DmlExecutor = async (_db, receivedJob, _lastPk) => {
        receivedBatchSize = receivedJob.batch_size;
        return { rowsAffected: 0, lastPk: null };
      };

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      await worker.run();

      expect(receivedBatchSize).toBe(777);
    });
  });

  // -----------------------------------------------------------------------
  // Pause signal (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  describe("pause signal", () => {
    it("stops processing when signal returns pause", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ attempt: 1, max_retries: 3 });

      // For failJob, we need the SELECT and UPDATE for the fail transition
      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        { pattern: "BEGIN", rows: [], command: "BEGIN" },
        { pattern: "COMMIT", rows: [], command: "COMMIT" },
        { pattern: "SET lock_timeout", rows: [] },
        { pattern: "SET statement_timeout", rows: [] },
        { pattern: "SET search_path", rows: [] },
        { pattern: "heartbeat_at = now()", rows: [], rowCount: 1, command: "UPDATE" },
        { pattern: /last_pk = \$1/, rows: [], rowCount: 1, command: "UPDATE" },
        // failJob needs: getJob (SELECT) -> transition (UPDATE)
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      let callCount = 0;
      const signalCheck: SignalCheckFn = () => {
        callCount++;
        // Pause on second check (after first batch completes)
        return callCount >= 2 ? "pause" : "continue";
      };

      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 100, lastPk: "200" }, // should not reach
      ]);

      const worker = new BatchWorker(
        client,
        dml,
        {},
        signalCheck,
        instantSleep,
      );
      const result = await worker.run();

      expect(result.status).toBe("paused");
      expect(result.batchesProcessed).toBe(1);
    });
  });

  // -----------------------------------------------------------------------
  // Cancel signal (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  describe("cancel signal", () => {
    it("stops processing when signal returns cancel", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ attempt: 1, max_retries: 3 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        // failJob: getJob then transition
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      const signalCheck: SignalCheckFn = () => "cancel";

      const dml = fixedDml([]);

      const worker = new BatchWorker(
        client,
        dml,
        {},
        signalCheck,
        instantSleep,
      );
      const result = await worker.run();

      expect(result.status).toBe("cancelled");
      expect(result.batchesProcessed).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // Error handling
  // -----------------------------------------------------------------------

  describe("error handling", () => {
    it("marks job as failed when DML throws an error", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ attempt: 1, max_retries: 3 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        { pattern: "BEGIN", rows: [], command: "BEGIN" },
        { pattern: "ROLLBACK", rows: [], command: "ROLLBACK" },
        { pattern: "SET lock_timeout", rows: [] },
        { pattern: "SET statement_timeout", rows: [] },
        { pattern: "SET search_path", rows: [] },
        // failJob: getJob then transition
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      const dml = failingDml(1, "deadlock detected");

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      const result = await worker.run();

      expect(result.status).toBe("failed");
      expect(result.error).toBe("deadlock detected");
      expect(result.jobId).toBe(1);
    });

    it("reports error message in the result", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ attempt: 1, max_retries: 3 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        { pattern: "BEGIN", rows: [], command: "BEGIN" },
        { pattern: "ROLLBACK", rows: [], command: "ROLLBACK" },
        { pattern: "SET lock_timeout", rows: [] },
        { pattern: "SET statement_timeout", rows: [] },
        { pattern: "SET search_path", rows: [] },
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      const dml = failingDml(1, "statement timeout exceeded");

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      const result = await worker.run();

      expect(result.error).toBe("statement timeout exceeded");
    });

    it("preserves partial progress on failure", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ attempt: 1, max_retries: 3 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        { pattern: "BEGIN", rows: [], command: "BEGIN" },
        { pattern: "COMMIT", rows: [], command: "COMMIT" },
        { pattern: "ROLLBACK", rows: [], command: "ROLLBACK" },
        { pattern: "SET lock_timeout", rows: [] },
        { pattern: "SET statement_timeout", rows: [] },
        { pattern: "SET search_path", rows: [] },
        { pattern: "heartbeat_at = now()", rows: [], rowCount: 1, command: "UPDATE" },
        { pattern: /last_pk = \$1/, rows: [], rowCount: 1, command: "UPDATE" },
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      // Succeeds twice, then fails on 3rd call
      const dml = failingDml(3, "connection lost");

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      const result = await worker.run();

      expect(result.status).toBe("failed");
      expect(result.batchesProcessed).toBe(2);
      expect(result.totalRowsAffected).toBe(200);
      expect(result.lastPk).toBe("200");
    });
  });

  // -----------------------------------------------------------------------
  // Config overrides
  // -----------------------------------------------------------------------

  describe("configuration", () => {
    it("uses custom schema for queue operations", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [] },
      ]);

      const dml = fixedDml([]);
      const worker = new BatchWorker(
        client,
        dml,
        { schema: "custom_schema" },
        undefined,
        instantSleep,
      );
      const result = await worker.run();

      expect(result.status).toBe("no_work");
      // Verify the queue was constructed with the custom schema by
      // checking that queries reference it
      const schemaQ = findQuery(pgClient, "custom_schema");
      expect(schemaQ).toBeDefined();
    });

    it("accepts all config options without error", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [] },
      ]);

      const config: BatchWorkerConfig = {
        batchSize: 500,
        sleepMs: 50,
        lockTimeoutMs: 10000,
        statementTimeoutMs: 60000,
        searchPath: "app,public",
        schema: "sqlever",
      };

      const dml = fixedDml([]);
      const worker = new BatchWorker(
        client,
        dml,
        config,
        undefined,
        instantSleep,
      );
      const result = await worker.run();
      expect(result.status).toBe("no_work");
    });
  });

  // -----------------------------------------------------------------------
  // Signal check is async-safe
  // -----------------------------------------------------------------------

  describe("async signal check", () => {
    it("supports async signal check functions", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ attempt: 1, max_retries: 3 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      const signalCheck: SignalCheckFn = async () => {
        // Simulate async check (e.g., reading a file or querying DB)
        return "cancel" as const;
      };

      const dml = fixedDml([]);
      const worker = new BatchWorker(
        client,
        dml,
        {},
        signalCheck,
        instantSleep,
      );
      const result = await worker.run();

      expect(result.status).toBe("cancelled");
    });
  });

  // -----------------------------------------------------------------------
  // Search path quoting
  // -----------------------------------------------------------------------

  describe("search_path quoting", () => {
    it("quotes multi-schema search_path correctly", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob();

      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([{ rowsAffected: 0, lastPk: null }]);

      const worker = new BatchWorker(
        client,
        dml,
        { searchPath: "app,public" },
        undefined,
        instantSleep,
      );
      await worker.run();

      const setQueries = allQueriesMatching(pgClient, "SET search_path");
      // Should have quoted both schemas
      const multiSchemaSet = setQueries.find(
        (q) => q.text.includes('"app"') && q.text.includes('"public"'),
      );
      expect(multiSchemaSet).toBeDefined();
    });
  });
});
