// src/commands/revert.ts — sqlever revert command
//
// Reverts deployed changes in reverse order, updating tracking tables.
// Implements SPEC R1 `revert` semantics:
//   - Connect, acquire advisory lock
//   - Read deployed changes from tracking tables
//   - Compute changes to revert (reverse of deploy order)
//   - If --to <change>: revert down to (but not including) the specified change
//   - If no --to: revert all deployed changes
//   - Prompt for confirmation unless -y/--no-prompt (or non-TTY stdin)
//   - For each change (in reverse): execute revert script, record in tracking
//   - Release advisory lock, print summary

import { resolve, join } from "node:path";
import { readFileSync } from "node:fs";
import type { ParsedArgs } from "../cli";
import { loadConfig, type MergedConfig } from "../config/index";
import { parsePlan } from "../plan/parser";
import type { Plan, Change as PlanChange } from "../plan/types";
import { DatabaseClient } from "../db/client";
import {
  Registry,
  REGISTRY_LOCK_KEY,
  type Change as RegistryChange,
  type RecordDeployInput,
} from "../db/registry";
import { PsqlRunner, type PsqlRunResult } from "../psql";
import { ShutdownManager } from "../signals";
import { info, error as logError, verbose } from "../output";

// ---------------------------------------------------------------------------
// Exit codes (SPEC R6)
// ---------------------------------------------------------------------------

/** Exit code for concurrent deploy/revert detected (advisory lock not acquired). */
export const EXIT_CODE_CONCURRENT = 4;

// ---------------------------------------------------------------------------
// Revert-specific argument parsing
// ---------------------------------------------------------------------------

export interface RevertOptions {
  /** Target database URI (from --db-uri or config). */
  dbUri?: string;
  /** Revert down to (but not including) this change name. */
  toChange?: string;
  /** Skip confirmation prompt (-y / --no-prompt). */
  noPrompt: boolean;
  /** Project root directory. */
  topDir: string;
  /** Target name (from --target). */
  target?: string;
  /** Plan file path override (from --plan-file). */
  planFile?: string;
}

/**
 * Parse revert-specific options from the CLI's parsed args.
 *
 * Usage: sqlever revert [target] [--to change] [-y] [--no-prompt]
 */
export function parseRevertOptions(args: ParsedArgs): RevertOptions {
  const opts: RevertOptions = {
    dbUri: args.dbUri,
    noPrompt: false,
    topDir: args.topDir ?? ".",
    target: args.target,
    planFile: args.planFile,
  };

  const rest = args.rest;
  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--to") {
      opts.toChange = rest[++i];
      i++;
      continue;
    }
    if (token === "-y" || token === "--no-prompt") {
      opts.noPrompt = true;
      i++;
      continue;
    }

    // First non-flag token could be a target name
    if (opts.target === undefined) {
      opts.target = token;
    }
    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Resolve target URI from config and options
// ---------------------------------------------------------------------------

/**
 * Resolve the database connection URI from the combined config sources.
 *
 * Precedence: --db-uri > target lookup > engine default target.
 */
export function resolveTargetUri(
  opts: RevertOptions,
  config: MergedConfig,
): string | undefined {
  // CLI --db-uri takes precedence
  if (opts.dbUri) return opts.dbUri;

  // Named target lookup
  const targetName = opts.target ?? config.engines.pg?.target;
  if (targetName && config.targets[targetName]) {
    return config.targets[targetName]!.uri;
  }

  // Fall back to engine target (which may be a URI like db:pg://...)
  if (targetName) return targetName;

  return undefined;
}

// ---------------------------------------------------------------------------
// Confirmation prompt
// ---------------------------------------------------------------------------

/**
 * Prompt the user for confirmation before reverting.
 *
 * If stdin is not a TTY (CI environment), requires -y or errors out.
 * Returns true if the user confirms, false otherwise.
 */
export async function confirmRevert(
  changes: RevertableChange[],
  noPrompt: boolean,
  stdin: NodeJS.ReadStream & { isTTY?: boolean } = process.stdin,
  stdout: NodeJS.WriteStream = process.stdout,
): Promise<boolean> {
  if (noPrompt) return true;

  // Non-TTY (piped input / CI): require -y
  if (!stdin.isTTY) {
    logError(
      "Revert requires confirmation. Pass -y or --no-prompt to proceed in non-interactive mode.",
    );
    return false;
  }

  // Show what will be reverted
  info(`The following changes will be reverted (in this order):`);
  for (const c of changes) {
    info(`  - ${c.name}`);
  }

  // Interactive prompt
  stdout.write("\nProceed with revert? [y/N] ");

  return new Promise<boolean>((resolve) => {
    stdin.resume();
    stdin.setEncoding("utf-8");
    stdin.once("data", (data: string) => {
      stdin.pause();
      const answer = data.toString().trim().toLowerCase();
      resolve(answer === "y" || answer === "yes");
    });
  });
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A change that can be reverted, combining deployed info with plan info. */
export interface RevertableChange {
  /** Change name. */
  name: string;
  /** Change ID from the tracking table. */
  change_id: string;
  /** Path to the revert script file. */
  revertScriptPath: string;
  /** The deployed change record (for building RecordDeployInput). */
  deployed: RegistryChange;
  /** Matching plan change (if found), for requires/conflicts/tags. */
  planChange?: PlanChange;
}

// ---------------------------------------------------------------------------
// Core revert logic
// ---------------------------------------------------------------------------

/**
 * Compute the list of changes to revert based on deployed state and --to flag.
 *
 * Returns changes in reverse deployment order (last deployed first).
 * If toChange is specified, reverts down to (but NOT including) that change.
 * If toChange is not specified, reverts ALL deployed changes.
 *
 * @throws Error if --to change is not found in the deployed list
 */
export function computeChangesToRevert(
  deployedChanges: RegistryChange[],
  toChange?: string,
): RegistryChange[] {
  if (deployedChanges.length === 0) return [];

  // Reverse order: last deployed first
  const reversed = [...deployedChanges].reverse();

  if (!toChange) {
    return reversed;
  }

  // Find the --to change in the deployed list
  const toIndex = deployedChanges.findIndex((c) => c.change === toChange);
  if (toIndex === -1) {
    throw new Error(
      `Change '${toChange}' is not deployed. Cannot use as --to target.`,
    );
  }

  // Revert everything AFTER the --to change (not including it)
  // In the original order, we want everything from toIndex+1 onwards.
  // Then reverse it.
  const toRevert = deployedChanges.slice(toIndex + 1);
  return toRevert.reverse();
}

/**
 * Build the RecordDeployInput for a revert event.
 *
 * Maps deployed change info + plan metadata into the input shape
 * expected by Registry.recordRevert().
 */
export function buildRevertInput(
  deployed: RegistryChange,
  planChange?: PlanChange,
): RecordDeployInput {
  return {
    change_id: deployed.change_id,
    script_hash: deployed.script_hash,
    change: deployed.change,
    project: deployed.project,
    note: deployed.note,
    committer_name: deployed.committer_name,
    committer_email: deployed.committer_email,
    planned_at: deployed.planned_at,
    planner_name: deployed.planner_name,
    planner_email: deployed.planner_email,
    requires: planChange?.requires ?? [],
    conflicts: planChange?.conflicts ?? [],
    tags: [],
    dependencies: [],
  };
}

// ---------------------------------------------------------------------------
// Main revert command
// ---------------------------------------------------------------------------

/**
 * Execute the `revert` command.
 *
 * Flow:
 * 1. Parse config, connect to database
 * 2. Acquire advisory lock
 * 3. Read deployed changes from tracking tables
 * 4. Compute changes to revert
 * 5. Prompt for confirmation unless -y
 * 6. For each change: execute revert script, record in tracking
 * 7. Release advisory lock
 * 8. Print summary
 */
export async function runRevert(
  args: ParsedArgs,
  opts?: {
    shutdownManager?: ShutdownManager;
    psqlRunner?: PsqlRunner;
    stdin?: NodeJS.ReadStream & { isTTY?: boolean };
  },
): Promise<void> {
  const options = parseRevertOptions(args);
  const topDir = resolve(options.topDir);

  // 1. Load config
  const config = loadConfig(topDir);

  // Load plan file
  const planFilePath = options.planFile
    ? resolve(options.planFile)
    : join(topDir, config.core.plan_file);

  let plan: Plan;
  try {
    const planContent = readFileSync(planFilePath, "utf-8");
    plan = parsePlan(planContent);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logError(`Failed to read plan file: ${msg}`);
    process.exit(1);
  }

  // Resolve target URI
  const targetUri = resolveTargetUri(options, config);
  if (!targetUri) {
    logError(
      "No database target specified. Use --db-uri or configure a target in sqitch.conf.",
    );
    process.exit(1);
  }

  // 2. Connect to database
  const db = new DatabaseClient(targetUri, {
    command: "revert",
    project: plan.project.name,
  });

  const shutdown = opts?.shutdownManager ?? new ShutdownManager();

  // Register signal handlers for cleanup
  shutdown.register({ quiet: true });
  shutdown.onShutdown(async () => {
    try {
      await db.query("SELECT pg_advisory_unlock($1)", [REGISTRY_LOCK_KEY]);
    } catch {
      // Best-effort unlock
    }
    await db.disconnect();
  });

  await db.connect();

  const registry = new Registry(db);
  let lockAcquired = false;

  try {
    // 3. Acquire advisory lock (non-blocking)
    const lockResult = await db.query<{ pg_try_advisory_lock: boolean }>(
      "SELECT pg_try_advisory_lock($1)",
      [REGISTRY_LOCK_KEY],
    );
    lockAcquired = lockResult.rows[0]?.pg_try_advisory_lock === true;

    if (!lockAcquired) {
      logError(
        "Another deploy/revert operation is in progress. Aborting.",
      );
      process.exit(EXIT_CODE_CONCURRENT);
    }

    // 4. Read deployed changes
    const deployedChanges = await registry.getDeployedChanges(plan.project.name);

    if (deployedChanges.length === 0) {
      info("Nothing to revert. No changes are deployed.");
      return;
    }

    // 5. Compute changes to revert
    let changesToRevert: RegistryChange[];
    try {
      changesToRevert = computeChangesToRevert(
        deployedChanges,
        options.toChange,
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logError(msg);
      process.exit(1);
    }

    if (changesToRevert.length === 0) {
      info("Nothing to revert.");
      return;
    }

    // Build revertable change list with script paths and plan metadata
    const revertDir = join(topDir, config.core.revert_dir);
    const planChangeMap = new Map(
      plan.changes.map((c) => [c.change_id, c]),
    );

    const revertableChanges: RevertableChange[] = changesToRevert.map(
      (deployed) => ({
        name: deployed.change,
        change_id: deployed.change_id,
        revertScriptPath: join(revertDir, `${deployed.change}.sql`),
        deployed,
        planChange: planChangeMap.get(deployed.change_id),
      }),
    );

    // 6. Prompt for confirmation
    const confirmed = await confirmRevert(
      revertableChanges,
      options.noPrompt,
      opts?.stdin,
    );
    if (!confirmed) {
      info("Revert cancelled.");
      process.exit(0);
    }

    // 7. Execute reverts
    const psqlRunner = opts?.psqlRunner ?? new PsqlRunner();
    let successCount = 0;
    let failCount = 0;

    for (const change of revertableChanges) {
      if (shutdown.isShuttingDown()) {
        logError("Shutdown requested. Stopping revert.");
        break;
      }

      verbose(`Reverting: ${change.name}`);

      let result: PsqlRunResult;
      try {
        result = await psqlRunner.run(change.revertScriptPath, {
          uri: targetUri,
          singleTransaction: true,
          workingDir: topDir,
        });
      } catch (err) {
        // Spawn failure (e.g., psql not found)
        const msg = err instanceof Error ? err.message : String(err);
        logError(`Failed to execute revert script for '${change.name}': ${msg}`);

        // Record fail event
        const failInput = buildRevertInput(change.deployed, change.planChange);
        await safeRecordFail(registry, failInput, change.name);
        failCount++;
        break;
      }

      if (result.exitCode !== 0) {
        // Revert script raised an exception (non-revertable migration)
        const errMsg =
          result.error?.message ?? result.stderr.trim() ?? "unknown error";
        logError(
          `Revert failed for '${change.name}': ${errMsg}`,
        );

        // Record fail event — tracking state stays consistent
        const failInput = buildRevertInput(change.deployed, change.planChange);
        await safeRecordFail(registry, failInput, change.name);
        failCount++;
        break;
      }

      // Success — record the revert in tracking tables
      const revertInput = buildRevertInput(change.deployed, change.planChange);
      await registry.recordRevert(revertInput);

      // Also delete associated tags for this change
      await deleteTagsForChange(db, change.change_id);

      info(`  - ${change.name}`);
      successCount++;
    }

    // 8. Print summary
    if (failCount > 0) {
      logError(
        `Revert incomplete: ${successCount} reverted, ${failCount} failed.`,
      );
      process.exit(1);
    }

    info(`Revert complete: ${successCount} change(s) reverted.`);
  } finally {
    // 9. Release advisory lock (always)
    if (lockAcquired) {
      try {
        await db.query("SELECT pg_advisory_unlock($1)", [REGISTRY_LOCK_KEY]);
      } catch {
        // Best-effort unlock
      }
    }

    await db.disconnect();
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Safely record a 'fail' event. Wraps Registry.recordFailEvent() with
 * error handling so that a failure to record doesn't crash the revert flow.
 */
async function safeRecordFail(
  registry: Registry,
  input: RecordDeployInput,
  changeName: string,
): Promise<void> {
  try {
    await registry.recordFailEvent(input);
  } catch {
    logError(`Warning: Could not record fail event for '${changeName}'.`);
  }
}

/**
 * Delete tags associated with a reverted change.
 */
async function deleteTagsForChange(
  db: DatabaseClient,
  changeId: string,
): Promise<void> {
  try {
    await db.query("DELETE FROM sqitch.tags WHERE change_id = $1", [changeId]);
  } catch {
    // Best-effort — tags table may not exist yet
  }
}
