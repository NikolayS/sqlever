// src/batch/worker.ts — Batch execution loop for batched DML jobs
//
// Implements the batch worker described in SPEC Section 5.5:
//
// - Dequeues jobs via SKIP LOCKED from the queue
// - Executes DML (UPDATE/DELETE with LIMIT based on batch_size)
// - Commits per-batch transactions independently
// - Updates heartbeat at the start of each batch
// - Tracks last processed PK for resume (retried jobs resume from
//   where they stopped, not from the beginning)
// - Sleeps for a configured interval between batches
// - Repeats until no more rows or paused/cancelled
//
// Connection requirement: the batch worker requires a direct PostgreSQL
// connection (not PgBouncer in transaction mode) because it uses
// session-level settings and the connection must persist across sleep
// intervals (DD13).
//
// SET statements (lock_timeout, statement_timeout, search_path) are
// re-issued at the start of each batch transaction as a safety measure
// (DD13, DD14).
//
// Inspired by GitLab BatchedMigration framework.

import type { DatabaseClient, QueryResult } from "../db/client";
import type { BatchJob, PartitionId } from "./queue";
import { BatchQueue } from "./queue";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Status of a worker after processing. */
export type WorkerStatus =
  | "completed"   // All rows processed
  | "paused"      // Externally paused
  | "cancelled"   // Externally cancelled
  | "failed"      // Error during batch execution
  | "no_work";    // No pending jobs in the queue

/** Result returned when the worker finishes. */
export interface WorkerResult {
  status: WorkerStatus;
  jobId: number | null;
  batchesProcessed: number;
  totalRowsAffected: number;
  lastPk: string | null;
  error?: string;
}

/** Per-batch configuration options (SPEC Section 5.5). */
export interface BatchWorkerConfig {
  /** Rows per batch (LIMIT clause value). Default: from job. */
  batchSize?: number;
  /** Pause between batches in milliseconds. Default: from job. */
  sleepMs?: number;
  /** Per-batch lock_timeout in milliseconds. Default: 5000 (5s). */
  lockTimeoutMs?: number;
  /** Per-batch statement_timeout in milliseconds. Default: 0 (disabled). */
  statementTimeoutMs?: number;
  /** Schema search_path to set per batch. Default: "public". */
  searchPath?: string;
  /** Schema for the batch queue tables. Default: "sqlever". */
  schema?: string;
}

/**
 * Callback that checks whether the worker should pause or cancel.
 * Called between batches. Returns "continue", "pause", or "cancel".
 */
export type SignalCheckFn = () =>
  | "continue"
  | "pause"
  | "cancel"
  | Promise<"continue" | "pause" | "cancel">;

/**
 * DML executor: a function that executes the batch DML statement.
 *
 * Receives:
 * - `db`: the DatabaseClient (inside an active transaction)
 * - `job`: the current BatchJob (for table_name, batch_size, etc.)
 * - `lastPk`: the last processed PK (null on first batch)
 *
 * Must return:
 * - `rowsAffected`: number of rows modified in this batch
 * - `lastPk`: the PK of the last row processed (for resume tracking)
 *
 * The DML should use:
 * - WHERE pk > $lastPk (or no PK filter if lastPk is null)
 * - ORDER BY pk
 * - LIMIT $batchSize
 */
export type DmlExecutor = (
  db: DatabaseClient,
  job: BatchJob,
  lastPk: string | null,
) => Promise<{ rowsAffected: number; lastPk: string | null }>;

/**
 * Sleep function — injectable for testing. Default: real setTimeout.
 */
export type SleepFn = (ms: number) => Promise<void>;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default lock_timeout per batch (5 seconds). */
export const DEFAULT_LOCK_TIMEOUT_MS = 5_000;

/** Default statement_timeout per batch (disabled). */
export const DEFAULT_STATEMENT_TIMEOUT_MS = 0;

/** Default search_path. */
export const DEFAULT_SEARCH_PATH = "public";

// ---------------------------------------------------------------------------
// BatchWorker
// ---------------------------------------------------------------------------

/**
 * Batch execution loop for batched DML jobs (SPEC Section 5.5).
 *
 * The worker:
 * 1. Dequeues the next pending job via SKIP LOCKED
 * 2. For each batch:
 *    a. BEGIN transaction
 *    b. Re-issue SET statements (lock_timeout, statement_timeout,
 *       search_path) as a safety measure
 *    c. Execute the DML via the provided DmlExecutor
 *    d. Update the job's last_pk and heartbeat
 *    e. COMMIT
 *    f. Sleep for the configured interval
 *    g. Check for pause/cancel signals
 * 3. When DML returns 0 rows, mark job as done
 * 4. On error, mark job as failed (or dead if retries exhausted)
 */
export class BatchWorker {
  private db: DatabaseClient;
  private queue: BatchQueue;
  private config: Required<
    Pick<
      BatchWorkerConfig,
      "lockTimeoutMs" | "statementTimeoutMs" | "searchPath" | "schema"
    >
  > &
    Pick<BatchWorkerConfig, "batchSize" | "sleepMs">;
  private dmlExecutor: DmlExecutor;
  private signalCheck: SignalCheckFn;
  private sleepFn: SleepFn;

  constructor(
    db: DatabaseClient,
    dmlExecutor: DmlExecutor,
    options: BatchWorkerConfig = {},
    signalCheck?: SignalCheckFn,
    sleepFn?: SleepFn,
  ) {
    this.db = db;
    this.dmlExecutor = dmlExecutor;
    this.signalCheck = signalCheck ?? (() => "continue");
    this.sleepFn = sleepFn ?? defaultSleep;

    this.config = {
      batchSize: options.batchSize,
      sleepMs: options.sleepMs,
      lockTimeoutMs: options.lockTimeoutMs ?? DEFAULT_LOCK_TIMEOUT_MS,
      statementTimeoutMs:
        options.statementTimeoutMs ?? DEFAULT_STATEMENT_TIMEOUT_MS,
      searchPath: options.searchPath ?? DEFAULT_SEARCH_PATH,
      schema: options.schema ?? "sqlever",
    };

    this.queue = new BatchQueue(db, { schema: this.config.schema });
  }

  /**
   * Run the batch execution loop.
   *
   * Dequeues a job and processes it batch-by-batch until completion,
   * pause, cancel, or error.
   */
  async run(): Promise<WorkerResult> {
    // 1. Dequeue the next pending job
    const job = await this.queue.dequeueJob();
    if (!job) {
      return {
        status: "no_work",
        jobId: null,
        batchesProcessed: 0,
        totalRowsAffected: 0,
        lastPk: null,
      };
    }

    // Resolve effective config: explicit config overrides > job defaults
    const batchSize = this.config.batchSize ?? job.batch_size;
    const sleepMs = this.config.sleepMs ?? job.sleep_ms;

    let batchesProcessed = 0;
    let totalRowsAffected = 0;
    let lastPk = job.last_pk; // Resume from where the job left off

    try {
      // 2. Batch loop
      while (true) {
        // 2a. Check for external signals before starting a batch
        const signal = await this.signalCheck();
        if (signal === "pause") {
          // Transition to failed with a pause message so it can be resumed
          await this.queue.failJob(
            job.id,
            job.partition_id,
            "Paused by operator",
          );
          return {
            status: "paused",
            jobId: job.id,
            batchesProcessed,
            totalRowsAffected,
            lastPk,
          };
        }
        if (signal === "cancel") {
          await this.queue.failJob(
            job.id,
            job.partition_id,
            "Cancelled by operator",
          );
          return {
            status: "cancelled",
            jobId: job.id,
            batchesProcessed,
            totalRowsAffected,
            lastPk,
          };
        }

        // 2b. Execute one batch inside a transaction
        const batchResult = await this.executeBatch(job, lastPk, batchSize);

        batchesProcessed++;
        totalRowsAffected += batchResult.rowsAffected;
        lastPk = batchResult.lastPk ?? lastPk;

        // 2c. Update heartbeat and last_pk in the job row
        await this.queue.updateHeartbeat(job.id, job.partition_id);
        await this.updateJobProgress(job.id, job.partition_id, lastPk);

        // 2d. If no rows affected, we are done
        if (batchResult.rowsAffected === 0) {
          await this.queue.completeJob(job.id, job.partition_id, lastPk ?? undefined);
          return {
            status: "completed",
            jobId: job.id,
            batchesProcessed,
            totalRowsAffected,
            lastPk,
          };
        }

        // 2e. Sleep between batches
        await this.sleepFn(sleepMs);
      }
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      try {
        await this.queue.failJob(job.id, job.partition_id, errorMessage);
      } catch {
        // If we can't even fail the job, swallow and report the original error
      }
      return {
        status: "failed",
        jobId: job.id,
        batchesProcessed,
        totalRowsAffected,
        lastPk,
        error: errorMessage,
      };
    }
  }

  /**
   * Execute a single batch inside a transaction.
   *
   * 1. BEGIN
   * 2. Re-issue SET statements (lock_timeout, statement_timeout,
   *    search_path) as a safety measure (DD13)
   * 3. Execute the DML via the DmlExecutor
   * 4. COMMIT
   */
  private async executeBatch(
    job: BatchJob,
    lastPk: string | null,
    batchSize: number,
  ): Promise<{ rowsAffected: number; lastPk: string | null }> {
    // Use the DatabaseClient's transaction wrapper: BEGIN + COMMIT/ROLLBACK
    return this.db.transaction(async (txClient) => {
      // Re-issue SET statements at the start of each batch transaction
      // as a safety measure (DD13, SPEC Section 5.5)
      await this.applyBatchSettings(txClient);

      // Execute the actual DML
      const result = await this.dmlExecutor(
        txClient,
        { ...job, batch_size: batchSize },
        lastPk,
      );

      return result;
    });
  }

  /**
   * Re-issue SET statements at the start of each batch transaction.
   *
   * Per DD13 and SPEC Section 5.5: SET statements (lock_timeout,
   * statement_timeout, search_path) are re-issued as a safety measure
   * because session-level settings may be lost or leaked when using
   * connection poolers.
   */
  private async applyBatchSettings(db: DatabaseClient): Promise<void> {
    await db.query(`SET lock_timeout = ${this.config.lockTimeoutMs}`);
    await db.query(
      `SET statement_timeout = ${this.config.statementTimeoutMs}`,
    );
    await db.query(`SET search_path = ${quoteIdentList(this.config.searchPath)}`);
  }

  /**
   * Update the last_pk on the job row to track progress for resume.
   */
  private async updateJobProgress(
    jobId: number,
    partitionId: PartitionId,
    lastPk: string | null,
  ): Promise<void> {
    if (lastPk === null) return;

    const schema = quoteIdent(this.config.schema);
    await this.db.query(
      `UPDATE ${schema}."batch_jobs"
       SET last_pk = $1, updated_at = now()
       WHERE id = $2 AND partition_id = $3`,
      [lastPk, jobId, partitionId],
    );
  }
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/** Default sleep implementation using setTimeout. */
function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Simple SQL identifier quoting. Double-quotes the identifier and
 * escapes any embedded double quotes.
 */
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Quote a comma-separated list of schema names for SET search_path.
 * Each schema name is individually quoted.
 */
function quoteIdentList(schemas: string): string {
  return schemas
    .split(",")
    .map((s) => s.trim())
    .map(quoteIdent)
    .join(", ");
}
