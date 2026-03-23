// src/commands/shared.ts -- shared helpers for command implementations
//
// Extracted patterns used by 3+ commands to reduce duplication:
//   - resolveTargetUri: resolve DB connection URI from config/flags
//   - withDatabase: connect, run callback, disconnect (always)
//   - loadPlan: resolve plan file path, check existence, read, parse
//   - resolveFromPlan: get migration file paths from sqitch.plan
//   - resolveChangedFiles: get .sql files changed in git diff

import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { DatabaseClient, type SessionSettings } from "../db/client";
import type { MergedConfig } from "../config/index";
import { parsePlan } from "../plan/parser";
import type { Plan } from "../plan/types";

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

// ---------------------------------------------------------------------------
// loadPlan -- resolve, check, read, parse
// ---------------------------------------------------------------------------

/**
 * Resolve the plan file path, verify it exists, read it, and parse it.
 *
 * This eliminates the duplicated 4-step pattern (resolve path, check
 * existence, readFileSync, parsePlan) found in deploy, revert, verify,
 * status, diff, show, tag, rework, doctor, and plan commands.
 *
 * @param topDir           - Resolved project root directory
 * @param config           - Merged configuration (provides core.plan_file)
 * @param planFileOverride - Explicit --plan-file value (takes precedence)
 * @returns The parsed Plan
 * @throws Error if the plan file does not exist or cannot be parsed
 */
export function loadPlan(
  topDir: string,
  config: MergedConfig,
  planFileOverride?: string,
): Plan {
  const planFilePath = planFileOverride
    ? resolve(planFileOverride)
    : join(topDir, config.core.plan_file);

  if (!existsSync(planFilePath)) {
    throw new Error(
      `plan file not found: ${planFilePath}`,
    );
  }

  const planContent = readFileSync(planFilePath, "utf-8");
  return parsePlan(planContent);
}

// ---------------------------------------------------------------------------
// resolveFromPlan -- migration file paths from sqitch.plan
// ---------------------------------------------------------------------------

/**
 * Get migration file paths from sqitch.plan.
 *
 * Without a database connection we cannot determine deployment state,
 * so all change deploy scripts are returned.
 *
 * Used by: analyze, review.
 */
export function resolveFromPlan(
  planPath: string,
  deployDir: string,
): string[] {
  if (!existsSync(planPath)) {
    throw new Error(`plan file not found: ${planPath}`);
  }

  const planContent = readFileSync(planPath, "utf-8");
  const plan = parsePlan(planContent);

  const files: string[] = [];
  for (const change of plan.changes) {
    const deployFile = join(deployDir, `${change.name}.sql`);
    if (existsSync(deployFile)) {
      files.push(resolve(deployFile));
    }
  }

  return files;
}

// ---------------------------------------------------------------------------
// resolveChangedFiles -- .sql files changed in git
// ---------------------------------------------------------------------------

/**
 * Get files changed in git diff (unstaged + staged vs HEAD).
 * Also includes untracked .sql files. Only returns .sql files.
 *
 * Used by: analyze, review.
 */
export function resolveChangedFiles(): string[] {
  try {
    const proc = Bun.spawnSync(["git", "diff", "--name-only", "HEAD"], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const diffOutput = proc.stdout.toString().trim();

    // Also include staged files
    const stagedProc = Bun.spawnSync(
      ["git", "diff", "--name-only", "--cached"],
      { stdout: "pipe", stderr: "pipe" },
    );
    const stagedOutput = stagedProc.stdout.toString().trim();

    // Also include untracked files
    const untrackedProc = Bun.spawnSync(
      ["git", "ls-files", "--others", "--exclude-standard"],
      { stdout: "pipe", stderr: "pipe" },
    );
    const untrackedOutput = untrackedProc.stdout.toString().trim();

    const allFiles = new Set<string>();
    for (const output of [diffOutput, stagedOutput, untrackedOutput]) {
      if (output) {
        for (const f of output.split("\n")) {
          if (f.endsWith(".sql")) {
            const abs = resolve(f);
            if (existsSync(abs)) {
              allFiles.add(abs);
            }
          }
        }
      }
    }

    return Array.from(allFiles).sort();
  } catch {
    throw new Error("Failed to determine changed files from git");
  }
}
