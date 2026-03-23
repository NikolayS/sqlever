// src/commands/batch.ts — CLI for batch job management (SPEC Section 5.5)
//
// Subcommands:
//   sqlever batch add <name> --table <t> --batch-size 500 --sleep 100ms
//   sqlever batch list
//   sqlever batch status <name>
//   sqlever batch pause <name>
//   sqlever batch resume <name>
//   sqlever batch cancel <name>
//   sqlever batch retry <name>
//
// All subcommands support --format json for machine-readable output.

import type { ParsedArgs } from "../cli";
import type { BatchJob, BatchQueue } from "../batch/queue";
import { info, json as jsonOut } from "../output";
import { withDatabase } from "./shared";

// ---------------------------------------------------------------------------
// Subcommand argument types
// ---------------------------------------------------------------------------

export interface BatchAddArgs {
  name: string;
  table: string;
  batchSize: number;
  sleepMs: number;
  maxRetries: number;
}

export interface BatchNameArgs {
  name: string;
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

/** Batch subcommands recognized by the router. */
export const BATCH_SUBCOMMANDS = [
  "add",
  "list",
  "status",
  "pause",
  "resume",
  "cancel",
  "retry",
] as const;

export type BatchSubcommand = (typeof BATCH_SUBCOMMANDS)[number];

/**
 * Parse the `--sleep` value, which may include a unit suffix.
 * Accepts: "100", "100ms", "2s", "2sec"
 * Returns milliseconds.
 */
export function parseSleepValue(raw: string): number {
  const trimmed = raw.trim().toLowerCase();

  // Match number + optional unit
  const match = trimmed.match(/^(\d+(?:\.\d+)?)\s*(ms|s|sec)?$/);
  if (!match) {
    throw new Error(
      `Invalid --sleep value '${raw}'. Expected a number with optional unit (e.g., 100, 100ms, 2s).`,
    );
  }

  const value = parseFloat(match[1]!);
  const unit = match[2] ?? "ms"; // default to milliseconds

  if (unit === "s" || unit === "sec") {
    return Math.round(value * 1000);
  }
  return Math.round(value);
}

/**
 * Parse the argv rest array for the `batch add` subcommand.
 */
export function parseBatchAddArgs(rest: string[]): BatchAddArgs {
  let name: string | undefined;
  let tableName: string | undefined;
  let batchSize = 1000;
  let sleepMs = 100;
  let maxRetries = 3;

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "--table" || arg === "-t") {
      tableName = rest[++i];
      if (!tableName) {
        throw new Error("--table requires a value");
      }
      i++;
      continue;
    }

    if (arg === "--batch-size") {
      const val = rest[++i];
      if (!val) throw new Error("--batch-size requires a value");
      batchSize = parseInt(val, 10);
      if (isNaN(batchSize) || batchSize <= 0) {
        throw new Error(
          `Invalid --batch-size '${val}'. Must be a positive integer.`,
        );
      }
      i++;
      continue;
    }

    if (arg === "--sleep") {
      const val = rest[++i];
      if (!val) throw new Error("--sleep requires a value");
      sleepMs = parseSleepValue(val);
      i++;
      continue;
    }

    if (arg === "--max-retries") {
      const val = rest[++i];
      if (!val) throw new Error("--max-retries requires a value");
      maxRetries = parseInt(val, 10);
      if (isNaN(maxRetries) || maxRetries < 0) {
        throw new Error(
          `Invalid --max-retries '${val}'. Must be a non-negative integer.`,
        );
      }
      i++;
      continue;
    }

    // Skip --format (handled by top-level parser)
    if (arg === "--format") {
      i += 2;
      continue;
    }

    // First non-flag argument is the name
    if (!name && !arg.startsWith("-")) {
      name = arg;
      i++;
      continue;
    }

    throw new Error(`Unknown option: ${arg}`);
  }

  if (!name) {
    throw new Error("Missing required argument: <name>");
  }
  if (!tableName) {
    throw new Error("Missing required option: --table <table_name>");
  }

  return { name, table: tableName, batchSize, sleepMs, maxRetries };
}

/**
 * Parse the argv rest array for subcommands that take a single name argument.
 * Used by: status, pause, resume, cancel, retry
 */
export function parseBatchNameArgs(rest: string[]): BatchNameArgs {
  let name: string | undefined;

  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i]!;

    // Skip --format (handled by top-level parser)
    if (arg === "--format") {
      i++;
      continue;
    }

    if (arg.startsWith("-")) {
      throw new Error(`Unknown option: ${arg}`);
    }

    if (!name) {
      name = arg;
      continue;
    }

    throw new Error(`Unexpected argument: ${arg}`);
  }

  if (!name) {
    throw new Error("Missing required argument: <name>");
  }

  return { name };
}

// ---------------------------------------------------------------------------
// Formatters
// ---------------------------------------------------------------------------

/**
 * Format a single batch job for text output.
 */
export function formatJobText(job: BatchJob): string {
  const lines: string[] = [];
  lines.push(`Name:        ${job.name}`);
  lines.push(`Status:      ${job.status}`);
  lines.push(`Table:       ${job.table_name}`);
  lines.push(`Batch size:  ${job.batch_size}`);
  lines.push(`Sleep:       ${job.sleep_ms}ms`);
  lines.push(`Attempt:     ${job.attempt}/${job.max_retries}`);
  if (job.last_pk !== null) {
    lines.push(`Last PK:     ${job.last_pk}`);
  }
  if (job.error_message) {
    lines.push(`Error:       ${job.error_message}`);
  }
  if (job.heartbeat_at) {
    lines.push(`Heartbeat:   ${formatTimestamp(job.heartbeat_at)}`);
  }
  lines.push(`Created:     ${formatTimestamp(job.created_at)}`);
  lines.push(`Updated:     ${formatTimestamp(job.updated_at)}`);
  return lines.join("\n");
}

/**
 * Format a list of batch jobs as a table for text output.
 */
export function formatJobListText(jobs: BatchJob[]): string {
  if (jobs.length === 0) {
    return "No batch jobs found.";
  }

  const headers = ["NAME", "STATUS", "TABLE", "BATCH_SIZE", "ATTEMPT", "UPDATED"];
  const rows = jobs.map((j) => [
    j.name,
    j.status,
    j.table_name,
    String(j.batch_size),
    `${j.attempt}/${j.max_retries}`,
    formatTimestamp(j.updated_at),
  ]);

  return formatTextTable(headers, rows);
}

/**
 * Format a batch job as a JSON-serializable object.
 */
export function formatJobJson(job: BatchJob): Record<string, unknown> {
  return {
    name: job.name,
    status: job.status,
    table: job.table_name,
    batch_size: job.batch_size,
    sleep_ms: job.sleep_ms,
    attempt: job.attempt,
    max_retries: job.max_retries,
    last_pk: job.last_pk,
    error_message: job.error_message,
    heartbeat_at: job.heartbeat_at ? formatTimestamp(job.heartbeat_at) : null,
    created_at: formatTimestamp(job.created_at),
    updated_at: formatTimestamp(job.updated_at),
  };
}

/**
 * Format a timestamp as ISO string.
 */
function formatTimestamp(d: Date | string): string {
  if (d instanceof Date) return d.toISOString();
  return String(d);
}

/**
 * Format a simple text table with aligned columns.
 */
function formatTextTable(headers: string[], rows: string[][]): string {
  const widths = headers.map((h) => h.length);
  for (const row of rows) {
    for (let i = 0; i < headers.length; i++) {
      const cell = row[i] ?? "";
      if (cell.length > widths[i]!) {
        widths[i] = cell.length;
      }
    }
  }

  const pad = (s: string, w: number) =>
    s + " ".repeat(Math.max(0, w - s.length));

  const lines: string[] = [];
  lines.push(headers.map((h, i) => pad(h, widths[i]!)).join("  "));
  lines.push(widths.map((w) => "-".repeat(w)).join("  "));
  for (const row of rows) {
    lines.push(
      headers.map((_, i) => pad(row[i] ?? "", widths[i]!)).join("  "),
    );
  }

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Help text
// ---------------------------------------------------------------------------

const BATCH_HELP = `sqlever batch — Manage batched background data migrations

Usage:
  sqlever batch <subcommand> [options]

Subcommands:
  add <name> --table <t>   Register a new batch job
  list                     Show all jobs and their state
  status <name>            Show detailed status for a job
  pause <name>             Pause a running job
  resume <name>            Resume a paused job
  cancel <name>            Cancel a job
  retry <name>             Retry a failed/dead job

Options for 'add':
  --table, -t <name>       Target table (required)
  --batch-size <n>         Rows per batch (default: 1000)
  --sleep <duration>       Sleep between batches (default: 100ms)
  --max-retries <n>        Max retry attempts (default: 3)

Global options:
  --format json            Output as JSON
`;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/**
 * Route a batch subcommand to the appropriate handler.
 *
 * This is the main entry point called from cli.ts.
 * It parses the first element of args.rest as the subcommand and
 * dispatches accordingly.
 */
export async function runBatch(args: ParsedArgs): Promise<void> {
  const subcommand = args.rest[0];
  const subRest = args.rest.slice(1);

  if (!subcommand || args.help) {
    process.stdout.write(BATCH_HELP);
    return;
  }

  if (!BATCH_SUBCOMMANDS.includes(subcommand as BatchSubcommand)) {
    process.stdout.write(BATCH_HELP);
    throw new Error(`unknown subcommand '${subcommand}'`);
  }

  const format = args.format;

  switch (subcommand as BatchSubcommand) {
    case "add":
      return handleBatchAdd(subRest, format);
    case "list":
      return handleBatchList(subRest, format);
    case "status":
      return handleBatchStatus(subRest, format);
    case "pause":
      return handleBatchPause(subRest, format);
    case "resume":
      return handleBatchResume(subRest, format);
    case "cancel":
      return handleBatchCancel(subRest, format);
    case "retry":
      return handleBatchRetry(subRest, format);
  }
}

// ---------------------------------------------------------------------------
// Handlers — each connects to DB via withDatabase, calls queue, outputs result
// ---------------------------------------------------------------------------

/**
 * Resolve the batch database URI from environment variables.
 */
function resolveBatchDbUri(): string {
  const dbUri =
    process.env.SQLEVER_DB_URI ?? process.env.DATABASE_URL ?? "";
  if (!dbUri) {
    throw new Error(
      "No database URI configured. Set SQLEVER_DB_URI or DATABASE_URL, or use --db-uri.",
    );
  }
  return dbUri;
}

/**
 * Run a callback with a connected BatchQueue, handling connect/disconnect
 * via withDatabase from shared.ts.
 */
async function withQueue<T>(
  fn: (queue: BatchQueue) => Promise<T>,
): Promise<T> {
  const dbUri = resolveBatchDbUri();
  const { BatchQueue: BQ } = await import("../batch/queue");

  return withDatabase(dbUri, { command: "batch" }, async (db) => {
    const queue = new BQ(db);
    await queue.ensureSchema();
    return fn(queue);
  });
}

async function handleBatchAdd(
  rest: string[],
  format: "text" | "json",
): Promise<void> {
  const addArgs = parseBatchAddArgs(rest);

  await withQueue(async (queue) => {
    const job = await queue.createJob({
      name: addArgs.name,
      tableName: addArgs.table,
      batchSize: addArgs.batchSize,
      sleepMs: addArgs.sleepMs,
      maxRetries: addArgs.maxRetries,
    });

    if (format === "json") {
      jsonOut(formatJobJson(job));
    } else {
      info(`Batch job '${job.name}' created.`);
      info(formatJobText(job));
    }
  });
}

async function handleBatchList(
  rest: string[],
  format: "text" | "json",
): Promise<void> {
  // list takes no positional args, only --format
  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i]!;
    if (arg === "--format") {
      i++;
      continue;
    }
    if (arg.startsWith("-")) {
      throw new Error(`Unknown option: ${arg}`);
    }
    throw new Error(`Unexpected argument: ${arg}`);
  }

  await withQueue(async (queue) => {
    const jobs = await queue.listJobs();

    if (format === "json") {
      jsonOut(jobs.map(formatJobJson));
    } else {
      info(formatJobListText(jobs));
    }
  });
}

async function handleBatchStatus(
  rest: string[],
  format: "text" | "json",
): Promise<void> {
  const nameArgs = parseBatchNameArgs(rest);

  await withQueue(async (queue) => {
    const job = await queue.getJobByName(nameArgs.name);
    if (!job) {
      throw new Error(`Batch job '${nameArgs.name}' not found.`);
    }

    if (format === "json") {
      jsonOut(formatJobJson(job));
    } else {
      info(formatJobText(job));
    }
  });
}

async function handleBatchPause(
  rest: string[],
  format: "text" | "json",
): Promise<void> {
  const nameArgs = parseBatchNameArgs(rest);

  await withQueue(async (queue) => {
    const job = await queue.getJobByName(nameArgs.name);
    if (!job) {
      throw new Error(`Batch job '${nameArgs.name}' not found.`);
    }

    const updated = await queue.pauseJob(job.id, job.partition_id);

    if (format === "json") {
      jsonOut(formatJobJson(updated));
    } else {
      info(`Batch job '${updated.name}' paused.`);
    }
  });
}

async function handleBatchResume(
  rest: string[],
  format: "text" | "json",
): Promise<void> {
  const nameArgs = parseBatchNameArgs(rest);

  await withQueue(async (queue) => {
    const job = await queue.getJobByName(nameArgs.name);
    if (!job) {
      throw new Error(`Batch job '${nameArgs.name}' not found.`);
    }

    const updated = await queue.resumeJob(job.id, job.partition_id);

    if (format === "json") {
      jsonOut(formatJobJson(updated));
    } else {
      info(`Batch job '${updated.name}' resumed.`);
    }
  });
}

async function handleBatchCancel(
  rest: string[],
  format: "text" | "json",
): Promise<void> {
  const nameArgs = parseBatchNameArgs(rest);

  await withQueue(async (queue) => {
    const job = await queue.getJobByName(nameArgs.name);
    if (!job) {
      throw new Error(`Batch job '${nameArgs.name}' not found.`);
    }

    const updated = await queue.cancelJob(job.id, job.partition_id);

    if (format === "json") {
      jsonOut(formatJobJson(updated));
    } else {
      info(`Batch job '${updated.name}' cancelled.`);
    }
  });
}

async function handleBatchRetry(
  rest: string[],
  format: "text" | "json",
): Promise<void> {
  const nameArgs = parseBatchNameArgs(rest);

  await withQueue(async (queue) => {
    const job = await queue.getJobByName(nameArgs.name);
    if (!job) {
      throw new Error(`Batch job '${nameArgs.name}' not found.`);
    }

    const updated = await queue.retryJob(job.id, job.partition_id);

    if (format === "json") {
      jsonOut(formatJobJson(updated));
    } else {
      info(`Batch job '${updated.name}' retried (now running).`);
    }
  });
}
