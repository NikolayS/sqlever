// src/commands/shared.ts -- shared helpers for command implementations
//
// Extracted patterns used by 3+ commands to reduce duplication:
//   - resolveTargetUri: resolve DB connection URI from config/flags
//   - withDatabase: connect, run callback, disconnect (always)

import { DatabaseClient, type SessionSettings } from "../db/client";
import type { MergedConfig } from "../config/index";

// ---------------------------------------------------------------------------
// resolveTargetUri -- shared URI resolution
// ---------------------------------------------------------------------------

/**
 * Resolve the database connection URI from config and CLI overrides.
 *
 * Precedence:
 *   1. Explicit --db-uri flag
 *   2. --target flag => look up in config targets
 *   3. Default engine target from config
 *   4. null (no target configured)
 *
 * Used by: status, log, diff, verify, revert.
 */
export function resolveTargetUri(
  config: MergedConfig,
  dbUri?: string,
  targetName?: string,
): string | null {
  // 1. Explicit --db-uri
  if (dbUri) return dbUri;

  // 2. Explicit --target => look up in config
  if (targetName) {
    const t = config.targets[targetName];
    if (t?.uri) return t.uri;
    // Target name might itself be a URI
    if (targetName.includes("://")) return targetName;
    return null;
  }

  // 3. Default engine target
  const engineName = config.core.engine;
  if (engineName && config.engines[engineName]) {
    const engineTarget = config.engines[engineName]!.target;
    if (engineTarget) {
      const t = config.targets[engineTarget];
      if (t?.uri) return t.uri;
      if (engineTarget.includes("://")) return engineTarget;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// withDatabase -- connect, run, disconnect
// ---------------------------------------------------------------------------

/**
 * Open a database connection, run a callback, and ensure the connection
 * is closed afterward (even on error).
 *
 * This eliminates the duplicated connect/try/finally/disconnect pattern
 * found in log, status, diff, verify, and batch commands.
 *
 * @param uri      - Connection URI (db:pg:// or postgresql://)
 * @param settings - Session settings (command name, project, timeouts)
 * @param fn       - Async callback that receives the connected client
 * @returns The return value of fn
 */
export async function withDatabase<T>(
  uri: string,
  settings: SessionSettings,
  fn: (db: DatabaseClient) => Promise<T>,
): Promise<T> {
  const db = new DatabaseClient(uri, settings);
  await db.connect();

  try {
    return await fn(db);
  } finally {
    await db.disconnect();
  }
}
