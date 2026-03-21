import { describe, it, expect } from "bun:test";

import {
  parseBatchAddArgs,
  parseBatchNameArgs,
  parseSleepValue,
  formatJobText,
  formatJobListText,
  formatJobJson,
  BATCH_SUBCOMMANDS,
} from "../../src/commands/batch";

import type { BatchJob, PartitionId } from "../../src/batch/queue";
import { DEFAULT_BATCH_SIZE, DEFAULT_SLEEP_MS, DEFAULT_MAX_RETRIES } from "../../src/batch/queue";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function mockJob(overrides: Partial<BatchJob> = {}): BatchJob {
  return {
    id: 1,
    name: "backfill_tiers",
    status: "pending",
    partition_id: 0 as PartitionId,
    table_name: "users",
    batch_size: DEFAULT_BATCH_SIZE,
    sleep_ms: DEFAULT_SLEEP_MS,
    last_pk: null,
    attempt: 0,
    max_retries: DEFAULT_MAX_RETRIES,
    error_message: null,
    heartbeat_at: null,
    created_at: new Date("2025-01-01T00:00:00Z"),
    updated_at: new Date("2025-01-01T00:00:00Z"),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// parseSleepValue
// ---------------------------------------------------------------------------

describe("parseSleepValue()", () => {
  it("parses plain number as milliseconds", () => {
    expect(parseSleepValue("100")).toBe(100);
  });

  it("parses value with ms suffix", () => {
    expect(parseSleepValue("200ms")).toBe(200);
  });

  it("parses value with s suffix (seconds to ms)", () => {
    expect(parseSleepValue("2s")).toBe(2000);
  });

  it("parses value with sec suffix", () => {
    expect(parseSleepValue("3sec")).toBe(3000);
  });

  it("handles whitespace", () => {
    expect(parseSleepValue("  500ms  ")).toBe(500);
  });

  it("throws on invalid format", () => {
    expect(() => parseSleepValue("abc")).toThrow("Invalid --sleep value");
  });

  it("throws on empty string", () => {
    expect(() => parseSleepValue("")).toThrow("Invalid --sleep value");
  });
});

// ---------------------------------------------------------------------------
// parseBatchAddArgs
// ---------------------------------------------------------------------------

describe("parseBatchAddArgs()", () => {
  it("parses minimal add args (name + --table)", () => {
    const result = parseBatchAddArgs(["my_job", "--table", "users"]);
    expect(result.name).toBe("my_job");
    expect(result.table).toBe("users");
    expect(result.batchSize).toBe(1000);
    expect(result.sleepMs).toBe(100);
    expect(result.maxRetries).toBe(3);
  });

  it("parses all options", () => {
    const result = parseBatchAddArgs([
      "backfill_tier",
      "--table", "users",
      "--batch-size", "500",
      "--sleep", "200ms",
      "--max-retries", "5",
    ]);
    expect(result.name).toBe("backfill_tier");
    expect(result.table).toBe("users");
    expect(result.batchSize).toBe(500);
    expect(result.sleepMs).toBe(200);
    expect(result.maxRetries).toBe(5);
  });

  it("parses --table with short flag -t", () => {
    const result = parseBatchAddArgs(["my_job", "-t", "orders"]);
    expect(result.table).toBe("orders");
  });

  it("throws when name is missing", () => {
    expect(() => parseBatchAddArgs(["--table", "users"])).toThrow(
      "Missing required argument: <name>",
    );
  });

  it("throws when --table is missing", () => {
    expect(() => parseBatchAddArgs(["my_job"])).toThrow(
      "Missing required option: --table",
    );
  });

  it("throws on invalid --batch-size", () => {
    expect(() =>
      parseBatchAddArgs(["my_job", "--table", "t", "--batch-size", "-1"]),
    ).toThrow("Invalid --batch-size");
  });

  it("throws on non-numeric --batch-size", () => {
    expect(() =>
      parseBatchAddArgs(["my_job", "--table", "t", "--batch-size", "abc"]),
    ).toThrow("Invalid --batch-size");
  });

  it("throws on unknown option", () => {
    expect(() =>
      parseBatchAddArgs(["my_job", "--table", "t", "--unknown"]),
    ).toThrow("Unknown option: --unknown");
  });

  it("skips --format flag (handled by top-level parser)", () => {
    const result = parseBatchAddArgs([
      "my_job",
      "--table", "users",
      "--format", "json",
    ]);
    expect(result.name).toBe("my_job");
    expect(result.table).toBe("users");
  });

  it("parses --sleep with seconds", () => {
    const result = parseBatchAddArgs([
      "my_job",
      "--table", "users",
      "--sleep", "2s",
    ]);
    expect(result.sleepMs).toBe(2000);
  });
});

// ---------------------------------------------------------------------------
// parseBatchNameArgs
// ---------------------------------------------------------------------------

describe("parseBatchNameArgs()", () => {
  it("parses a single name argument", () => {
    const result = parseBatchNameArgs(["my_job"]);
    expect(result.name).toBe("my_job");
  });

  it("throws when name is missing", () => {
    expect(() => parseBatchNameArgs([])).toThrow(
      "Missing required argument: <name>",
    );
  });

  it("throws on unexpected extra arguments", () => {
    expect(() => parseBatchNameArgs(["job1", "job2"])).toThrow(
      "Unexpected argument: job2",
    );
  });

  it("throws on unknown flags", () => {
    expect(() => parseBatchNameArgs(["--unknown"])).toThrow(
      "Unknown option: --unknown",
    );
  });

  it("skips --format flag", () => {
    const result = parseBatchNameArgs(["my_job", "--format", "json"]);
    expect(result.name).toBe("my_job");
  });
});

// ---------------------------------------------------------------------------
// BATCH_SUBCOMMANDS
// ---------------------------------------------------------------------------

describe("BATCH_SUBCOMMANDS", () => {
  it("includes all 7 subcommands", () => {
    expect(BATCH_SUBCOMMANDS).toHaveLength(7);
    expect(BATCH_SUBCOMMANDS).toContain("add");
    expect(BATCH_SUBCOMMANDS).toContain("list");
    expect(BATCH_SUBCOMMANDS).toContain("status");
    expect(BATCH_SUBCOMMANDS).toContain("pause");
    expect(BATCH_SUBCOMMANDS).toContain("resume");
    expect(BATCH_SUBCOMMANDS).toContain("cancel");
    expect(BATCH_SUBCOMMANDS).toContain("retry");
  });
});

// ---------------------------------------------------------------------------
// formatJobText
// ---------------------------------------------------------------------------

describe("formatJobText()", () => {
  it("formats a basic job", () => {
    const job = mockJob();
    const text = formatJobText(job);
    expect(text).toContain("Name:        backfill_tiers");
    expect(text).toContain("Status:      pending");
    expect(text).toContain("Table:       users");
    expect(text).toContain("Batch size:  1000");
    expect(text).toContain("Sleep:       100ms");
    expect(text).toContain("Attempt:     0/3");
  });

  it("includes last_pk when present", () => {
    const job = mockJob({ last_pk: "99999" });
    const text = formatJobText(job);
    expect(text).toContain("Last PK:     99999");
  });

  it("excludes last_pk when null", () => {
    const job = mockJob({ last_pk: null });
    const text = formatJobText(job);
    expect(text).not.toContain("Last PK:");
  });

  it("includes error message when present", () => {
    const job = mockJob({ error_message: "OOM killed" });
    const text = formatJobText(job);
    expect(text).toContain("Error:       OOM killed");
  });

  it("includes heartbeat when present", () => {
    const job = mockJob({ heartbeat_at: new Date("2025-06-15T12:00:00Z") });
    const text = formatJobText(job);
    expect(text).toContain("Heartbeat:");
    expect(text).toContain("2025-06-15");
  });
});

// ---------------------------------------------------------------------------
// formatJobListText
// ---------------------------------------------------------------------------

describe("formatJobListText()", () => {
  it("returns 'No batch jobs found.' for empty list", () => {
    const text = formatJobListText([]);
    expect(text).toBe("No batch jobs found.");
  });

  it("formats a table with headers", () => {
    const jobs = [mockJob(), mockJob({ id: 2, name: "backfill_orders", table_name: "orders" })];
    const text = formatJobListText(jobs);
    expect(text).toContain("NAME");
    expect(text).toContain("STATUS");
    expect(text).toContain("TABLE");
    expect(text).toContain("backfill_tiers");
    expect(text).toContain("backfill_orders");
    expect(text).toContain("users");
    expect(text).toContain("orders");
  });

  it("includes separator line", () => {
    const jobs = [mockJob()];
    const text = formatJobListText(jobs);
    const lines = text.split("\n");
    // Second line should be all dashes and spaces
    expect(lines[1]).toMatch(/^[-\s]+$/);
  });
});

// ---------------------------------------------------------------------------
// formatJobJson
// ---------------------------------------------------------------------------

describe("formatJobJson()", () => {
  it("returns a plain object with expected keys", () => {
    const job = mockJob();
    const json = formatJobJson(job);
    expect(json.name).toBe("backfill_tiers");
    expect(json.status).toBe("pending");
    expect(json.table).toBe("users");
    expect(json.batch_size).toBe(1000);
    expect(json.sleep_ms).toBe(100);
    expect(json.attempt).toBe(0);
    expect(json.max_retries).toBe(3);
    expect(json.last_pk).toBeNull();
    expect(json.error_message).toBeNull();
    expect(json.heartbeat_at).toBeNull();
  });

  it("formats timestamps as ISO strings", () => {
    const job = mockJob();
    const json = formatJobJson(job);
    expect(json.created_at).toBe("2025-01-01T00:00:00.000Z");
  });

  it("includes heartbeat_at when present", () => {
    const job = mockJob({ heartbeat_at: new Date("2025-06-15T12:00:00Z") });
    const json = formatJobJson(job);
    expect(json.heartbeat_at).toBe("2025-06-15T12:00:00.000Z");
  });
});

// ---------------------------------------------------------------------------
// CLI subprocess tests (batch subcommand routing)
// ---------------------------------------------------------------------------

const CWD = import.meta.dir + "/../..";

async function run(
  ...args: string[]
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const proc = Bun.spawn(["bun", "run", "src/cli.ts", ...args], {
    cwd: CWD,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
}

describe("sqlever batch (CLI subprocess)", () => {
  it("shows help when no subcommand given", async () => {
    const { stdout, exitCode } = await run("batch");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("sqlever batch");
    expect(stdout).toContain("Subcommands:");
    expect(stdout).toContain("add");
    expect(stdout).toContain("list");
    expect(stdout).toContain("status");
    expect(stdout).toContain("pause");
    expect(stdout).toContain("resume");
    expect(stdout).toContain("cancel");
    expect(stdout).toContain("retry");
  });

  it("shows help with --help flag (top-level handler)", async () => {
    const { stdout, exitCode } = await run("batch", "--help");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("sqlever batch");
  });

  it("reports error for unknown subcommand", async () => {
    const { stderr, exitCode } = await run("batch", "nonexistent");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("unknown subcommand");
    expect(stderr).toContain("nonexistent");
  });

  it("reports error for add without required args", async () => {
    // 'add' with no args fails because of missing name/table, but the error
    // comes from parseBatchAddArgs (requires DB connection for the actual
    // operation, but arg parsing happens first).
    const { stderr, exitCode } = await run("batch", "add");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Missing required argument: <name>");
  });

  it("reports error for add with name but no --table", async () => {
    const { stderr, exitCode } = await run("batch", "add", "my_job");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Missing required option: --table");
  });

  it("reports error for status without name", async () => {
    const { stderr, exitCode } = await run("batch", "status");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Missing required argument: <name>");
  });

  it("reports error for pause without name", async () => {
    const { stderr, exitCode } = await run("batch", "pause");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Missing required argument: <name>");
  });

  it("reports error for resume without name", async () => {
    const { stderr, exitCode } = await run("batch", "resume");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Missing required argument: <name>");
  });

  it("reports error for cancel without name", async () => {
    const { stderr, exitCode } = await run("batch", "cancel");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Missing required argument: <name>");
  });

  it("reports error for retry without name", async () => {
    const { stderr, exitCode } = await run("batch", "retry");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Missing required argument: <name>");
  });
});

// ---------------------------------------------------------------------------
// Queue state extension tests (paused/cancelled transitions)
// ---------------------------------------------------------------------------

describe("extended job status transitions", () => {
  // These test the VALID_TRANSITIONS map additions
  it("running -> paused is valid", async () => {
    const { isValidTransition } = await import("../../src/batch/queue");
    expect(isValidTransition("running", "paused")).toBe(true);
  });

  it("paused -> running is valid (resume)", async () => {
    const { isValidTransition } = await import("../../src/batch/queue");
    expect(isValidTransition("paused", "running")).toBe(true);
  });

  it("running -> cancelled is valid", async () => {
    const { isValidTransition } = await import("../../src/batch/queue");
    expect(isValidTransition("running", "cancelled")).toBe(true);
  });

  it("pending -> cancelled is valid", async () => {
    const { isValidTransition } = await import("../../src/batch/queue");
    expect(isValidTransition("pending", "cancelled")).toBe(true);
  });

  it("paused -> cancelled is valid", async () => {
    const { isValidTransition } = await import("../../src/batch/queue");
    expect(isValidTransition("paused", "cancelled")).toBe(true);
  });

  it("cancelled -> running is not valid (terminal state)", async () => {
    const { isValidTransition } = await import("../../src/batch/queue");
    expect(isValidTransition("cancelled", "running")).toBe(false);
  });

  it("done -> paused is not valid", async () => {
    const { isValidTransition } = await import("../../src/batch/queue");
    expect(isValidTransition("done", "paused")).toBe(false);
  });
});
