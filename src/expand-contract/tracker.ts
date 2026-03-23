// src/expand-contract/tracker.ts — Expand/contract phase state tracker
//
// Tracks expand/contract phase state in the `sqlever.expand_contract_state`
// table. Implements the state machine:
//
//   expanding -> expanded -> contracting -> completed
//
// All state lives in PostgreSQL (DD8). Phase transitions use advisory locks
// to prevent concurrent operations (SPEC 5.4, point 6). Contract phase
// requires backfill verification before proceeding.
//
// Uses DatabaseClient from src/db/client.ts — never executes migration
// scripts directly.

import type { DatabaseClient } from "../db/client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Valid phases in the expand/contract lifecycle. */
export type Phase = "expanding" | "expanded" | "contracting" | "completed";

/** A row from sqlever.expand_contract_state. */
export interface ExpandContractState {
  id: number;
  change_name: string;
  project: string;
  phase: Phase;
  table_schema: string;
  table_name: string;
  started_at: Date;
  updated_at: Date;
  started_by: string;
}

/** Input for creating a new expand/contract operation. */
export interface CreateOperationInput {
  change_name: string;
  project: string;
  table_schema: string;
  table_name: string;
  started_by: string;
}

/** Backfill verification result. */
export interface BackfillStatus {
  total_rows: number;
  backfilled_rows: number;
  is_complete: boolean;
}

/** Input for verifying backfill completeness. */
export interface BackfillCheckInput {
  table_schema: string;
  table_name: string;
  new_column: string;
  /** Optional WHERE clause fragment for the old column (e.g., "old_col IS NOT NULL"). */
  source_filter?: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Advisory lock key namespace for expand/contract phase transitions.
 * Uses the two-argument form: pg_advisory_lock(namespace, key).
 * The namespace is a fixed constant; the key is derived from the operation ID.
 *
 * Value: ASCII for "exco" (expand/contract).
 */
export const EC_LOCK_NAMESPACE = 0x6578_636f;

/**
 * Valid phase transitions. Each key maps to the set of valid next phases.
 */
export const VALID_TRANSITIONS: Record<Phase, Phase[]> = {
  expanding: ["expanded"],
  expanded: ["contracting"],
  contracting: ["completed"],
  completed: [],
};

// ---------------------------------------------------------------------------
// DDL — sqlever.expand_contract_state table
// ---------------------------------------------------------------------------

/**
 * DDL for the expand_contract_state tracking table.
 *
 * Lives in the sqlever schema (created on first use per SPEC DD8).
 * Uses IF NOT EXISTS for idempotent creation.
 */
export const EXPAND_CONTRACT_DDL = `
CREATE SCHEMA IF NOT EXISTS sqlever;

CREATE TABLE IF NOT EXISTS sqlever.expand_contract_state (
    id              SERIAL      PRIMARY KEY,
    change_name     TEXT        NOT NULL,
    project         TEXT        NOT NULL,
    phase           TEXT        NOT NULL DEFAULT 'expanding'
                    CHECK (phase IN ('expanding', 'expanded', 'contracting', 'completed')),
    table_schema    TEXT        NOT NULL DEFAULT 'public',
    table_name      TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_by      TEXT        NOT NULL,
    UNIQUE (project, change_name)
);
`.trim();

// ---------------------------------------------------------------------------
// ExpandContractTracker
// ---------------------------------------------------------------------------

/**
 * ExpandContractTracker manages expand/contract phase state in PostgreSQL.
 *
 * Responsibilities:
 * - Schema/table creation (idempotent)
 * - CRUD operations on expand_contract_state rows
 * - Phase transitions with advisory lock protection
 * - Backfill verification before contract phase
 *
 * All state lives in the database (DD8). Advisory locks prevent concurrent
 * phase transitions (SPEC 5.4, point 6).
 */
export class ExpandContractTracker {
  constructor(private readonly db: DatabaseClient) {}

  // -----------------------------------------------------------------------
  // Schema creation
  // -----------------------------------------------------------------------

  /**
   * Create the sqlever schema and expand_contract_state table.
   *
   * Uses advisory lock to serialize concurrent schema creation attempts.
   * All DDL uses IF NOT EXISTS for idempotency.
   */
  async ensureSchema(): Promise<void> {
    await this.db.query("SELECT pg_advisory_lock($1)", [EC_LOCK_NAMESPACE]);
    try {
      await this.db.query(EXPAND_CONTRACT_DDL);
    } finally {
      await this.db.query("SELECT pg_advisory_unlock($1)", [EC_LOCK_NAMESPACE]);
    }
  }

  // -----------------------------------------------------------------------
  // CRUD operations
  // -----------------------------------------------------------------------

  /**
   * Create a new expand/contract operation in the "expanding" phase.
   *
   * @param input - Operation details
   * @returns The newly created state record
   * @throws If an active operation already exists for this project + change_name
   */
  async createOperation(input: CreateOperationInput): Promise<ExpandContractState> {
    // Check for existing active operation (not completed)
    const existing = await this.db.query<ExpandContractState>(
      `SELECT id, change_name, project, phase, table_schema, table_name,
              started_at, updated_at, started_by
       FROM sqlever.expand_contract_state
       WHERE project = $1 AND change_name = $2 AND phase != 'completed'`,
      [input.project, input.change_name],
    );

    if (existing.rows.length > 0) {
      const row = existing.rows[0]!;
      throw new Error(
        `Active expand/contract operation already exists for ` +
        `"${input.change_name}" in project "${input.project}" (phase: ${row.phase})`,
      );
    }

    const result = await this.db.query<ExpandContractState>(
      `INSERT INTO sqlever.expand_contract_state
         (change_name, project, table_schema, table_name, started_by)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, change_name, project, phase, table_schema, table_name,
                 started_at, updated_at, started_by`,
      [
        input.change_name,
        input.project,
        input.table_schema,
        input.table_name,
        input.started_by,
      ],
    );

    return result.rows[0]!;
  }

  /**
   * Get an operation by ID.
   *
   * @returns The state record, or null if not found
   */
  async getOperation(id: number): Promise<ExpandContractState | null> {
    const result = await this.db.query<ExpandContractState>(
      `SELECT id, change_name, project, phase, table_schema, table_name,
              started_at, updated_at, started_by
       FROM sqlever.expand_contract_state
       WHERE id = $1`,
      [id],
    );
    return result.rows[0] ?? null;
  }

  /**
   * Get an operation by project and change name.
   *
   * @returns The state record, or null if not found
   */
  async getOperationByName(
    project: string,
    changeName: string,
  ): Promise<ExpandContractState | null> {
    const result = await this.db.query<ExpandContractState>(
      `SELECT id, change_name, project, phase, table_schema, table_name,
              started_at, updated_at, started_by
       FROM sqlever.expand_contract_state
       WHERE project = $1 AND change_name = $2
       ORDER BY started_at DESC
       LIMIT 1`,
      [project, changeName],
    );
    return result.rows[0] ?? null;
  }

  /**
   * List all operations for a project, optionally filtered by phase.
   */
  async listOperations(
    project: string,
    phase?: Phase,
  ): Promise<ExpandContractState[]> {
    if (phase) {
      const result = await this.db.query<ExpandContractState>(
        `SELECT id, change_name, project, phase, table_schema, table_name,
                started_at, updated_at, started_by
         FROM sqlever.expand_contract_state
         WHERE project = $1 AND phase = $2
         ORDER BY started_at DESC`,
        [project, phase],
      );
      return result.rows;
    }

    const result = await this.db.query<ExpandContractState>(
      `SELECT id, change_name, project, phase, table_schema, table_name,
              started_at, updated_at, started_by
       FROM sqlever.expand_contract_state
       WHERE project = $1
       ORDER BY started_at DESC`,
      [project],
    );
    return result.rows;
  }

  /**
   * List all active (non-completed) operations for a project.
   */
  async listActiveOperations(project: string): Promise<ExpandContractState[]> {
    const result = await this.db.query<ExpandContractState>(
      `SELECT id, change_name, project, phase, table_schema, table_name,
              started_at, updated_at, started_by
       FROM sqlever.expand_contract_state
       WHERE project = $1 AND phase != 'completed'
       ORDER BY started_at DESC`,
      [project],
    );
    return result.rows;
  }

  /**
   * Delete an operation record. Only allowed for completed operations
   * (cleanup) or for cancelling an operation that hasn't progressed
   * past expanding.
   *
   * @throws If the operation is in expanded or contracting phase
   */
  async deleteOperation(id: number): Promise<boolean> {
    const op = await this.getOperation(id);
    if (!op) {
      return false;
    }

    if (op.phase === "expanded" || op.phase === "contracting") {
      throw new Error(
        `Cannot delete operation ${id} in phase "${op.phase}". ` +
        `Only "expanding" or "completed" operations can be deleted.`,
      );
    }

    const result = await this.db.query(
      "DELETE FROM sqlever.expand_contract_state WHERE id = $1",
      [id],
    );

    return (result.rowCount ?? 0) > 0;
  }

  // -----------------------------------------------------------------------
  // Phase transitions
  // -----------------------------------------------------------------------

  /**
   * Transition an operation to the next phase.
   *
   * Acquires an advisory lock (non-blocking) to prevent concurrent
   * transitions. If the lock is already held, throws immediately.
   *
   * The transition from "expanded" to "contracting" requires backfill
   * verification — the caller must provide a BackfillCheckInput and
   * the backfill must be complete.
   *
   * @param id - Operation ID
   * @param targetPhase - The phase to transition to
   * @param backfillCheck - Required when transitioning to "contracting"
   * @returns The updated state record
   * @throws On invalid transition, lock contention, or incomplete backfill
   */
  async transitionPhase(
    id: number,
    targetPhase: Phase,
    backfillCheck?: BackfillCheckInput,
  ): Promise<ExpandContractState> {
    // Acquire advisory lock (non-blocking)
    const lockAcquired = await this.tryAcquireLock(id);
    if (!lockAcquired) {
      throw new Error(
        `Cannot transition operation ${id}: another process is currently ` +
        `performing a phase transition. Try again later.`,
      );
    }

    try {
      // Fetch current state
      const current = await this.getOperation(id);
      if (!current) {
        throw new Error(`Operation ${id} not found`);
      }

      // Validate the transition
      this.validateTransition(current.phase, targetPhase);

      // If transitioning to contracting, verify backfill
      if (targetPhase === "contracting") {
        if (!backfillCheck) {
          throw new Error(
            `Backfill check is required when transitioning to "contracting". ` +
            `Provide a BackfillCheckInput to verify all rows are backfilled.`,
          );
        }
        const status = await this.checkBackfill(backfillCheck);
        if (!status.is_complete) {
          throw new Error(
            `Cannot transition to "contracting": backfill is not complete. ` +
            `${status.backfilled_rows}/${status.total_rows} rows backfilled.`,
          );
        }
      }

      // Perform the transition
      const result = await this.db.query<ExpandContractState>(
        `UPDATE sqlever.expand_contract_state
         SET phase = $1, updated_at = clock_timestamp()
         WHERE id = $2
         RETURNING id, change_name, project, phase, table_schema, table_name,
                   started_at, updated_at, started_by`,
        [targetPhase, id],
      );

      return result.rows[0]!;
    } finally {
      // Always release the advisory lock
      await this.releaseLock(id);
    }
  }

  /**
   * Validate that a phase transition is allowed.
   *
   * @throws If the transition is invalid
   */
  validateTransition(currentPhase: Phase, targetPhase: Phase): void {
    const validTargets = VALID_TRANSITIONS[currentPhase];

    if (!validTargets || !validTargets.includes(targetPhase)) {
      throw new Error(
        `Invalid phase transition: "${currentPhase}" -> "${targetPhase}". ` +
        `Valid transitions from "${currentPhase}": ` +
        `${validTargets && validTargets.length > 0 ? validTargets.join(", ") : "none (terminal state)"}`,
      );
    }
  }

  // -----------------------------------------------------------------------
  // Advisory locking
  // -----------------------------------------------------------------------

  /**
   * Try to acquire a session-level advisory lock for the given operation.
   *
   * Uses the two-argument form: pg_try_advisory_lock(namespace, key).
   * Returns false immediately if the lock is already held.
   */
  async tryAcquireLock(operationId: number): Promise<boolean> {
    const result = await this.db.query<{ pg_try_advisory_lock: boolean }>(
      "SELECT pg_try_advisory_lock($1, $2)",
      [EC_LOCK_NAMESPACE, operationId],
    );
    return result.rows[0]?.pg_try_advisory_lock === true;
  }

  /**
   * Release the session-level advisory lock for the given operation.
   */
  async releaseLock(operationId: number): Promise<void> {
    await this.db.query("SELECT pg_advisory_unlock($1, $2)", [
      EC_LOCK_NAMESPACE,
      operationId,
    ]);
  }

  // -----------------------------------------------------------------------
  // Backfill verification
  // -----------------------------------------------------------------------

  /**
   * Check whether all rows in a table have been backfilled.
   *
   * Compares the total row count against the count of rows where the
   * new column is NOT NULL (i.e., has been backfilled).
   *
   * SECURITY: Identifiers (schema, table, column) are escaped via
   * escapeIdentifier() which double-quotes and escapes embedded quotes.
   * The source_filter field is intentionally ignored — it previously
   * allowed raw SQL injection. Callers that need filtered backfill
   * checks should implement the filtering in their own migration SQL.
   *
   * @param input - Table and column information
   * @returns Backfill status with counts and completion flag
   */
  async checkBackfill(input: BackfillCheckInput): Promise<BackfillStatus> {
    const schema = this.escapeIdentifier(input.table_schema);
    const table = this.escapeIdentifier(input.table_name);
    const column = this.escapeIdentifier(input.new_column);

    // SECURITY: source_filter is not used — it was a raw SQL injection
    // vector. All queries use only escaped identifiers.

    // Count total rows
    const totalSql = `SELECT COUNT(*)::int AS cnt FROM ${schema}.${table}`;

    const totalResult = await this.db.query<{ cnt: number }>(totalSql);
    const total_rows = totalResult.rows[0]?.cnt ?? 0;

    // Count backfilled rows (new column IS NOT NULL)
    const backfilledSql = `SELECT COUNT(*)::int AS cnt FROM ${schema}.${table} WHERE ${column} IS NOT NULL`;

    const backfilledResult = await this.db.query<{ cnt: number }>(backfilledSql);
    const backfilled_rows = backfilledResult.rows[0]?.cnt ?? 0;

    return {
      total_rows,
      backfilled_rows,
      is_complete: total_rows === backfilled_rows,
    };
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  /**
   * Escape a SQL identifier by wrapping in double quotes and escaping
   * any embedded double quotes.
   *
   * This provides client-side identifier quoting for dynamic SQL.
   */
  private escapeIdentifier(identifier: string): string {
    if (!identifier || identifier.length === 0) {
      throw new Error("Identifier cannot be empty");
    }
    // Replace any embedded double quotes with doubled quotes, then wrap
    return `"${identifier.replace(/"/g, '""')}"`;
  }
}
