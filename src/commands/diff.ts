// src/commands/diff.ts — sqlever diff command
//
// Shows changes that are pending (in the plan but not yet deployed), or
// shows the changes between two tags in the plan.
//
// This is a read-only command: it queries the plan file and tracking
// tables but never modifies anything.
//
// Implements GitHub issue #85.

import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { loadConfig } from "../config/index";
import { parsePlan } from "../plan/parser";
import type { Plan } from "../plan/types";
import { info, json as jsonOut } from "../output";
import type { ParsedArgs } from "../cli";
import { resolveTargetUri, withDatabase } from "./shared";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Status of a change relative to the deployed state. */
export type ChangeStatus = "pending" | "deployed";

/** A single entry in the diff output. */
export interface DiffEntry {
  /** Change name. */
  name: string;
  /** Whether the change is pending or deployed. */
  status: ChangeStatus;
  /** Required dependencies (from the plan). */
  requires: string[];
  /** Conflict dependencies (from the plan). */
  conflicts: string[];
  /** Note from the plan. */
  note: string;
}

/** Full diff result used for both text and JSON output. */
export interface DiffResult {
  /** Project name from the plan. */
  project: string;
  /** The --from tag, if provided. */
  from_tag: string | null;
  /** The --to tag, if provided. */
  to_tag: string | null;
  /** Diff entries. */
  changes: DiffEntry[];
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface DiffOptions {
  /** Project root directory. */
  topDir: string;
  /** Format: "text" or "json". */
  format: "text" | "json";
  /** Database URI override. */
  dbUri?: string;
  /** Target name override. */
  target?: string;
  /** Plan file override. */
  planFile?: string;
  /** --from tag name (without @ prefix). */
  fromTag?: string;
  /** --to tag name (without @ prefix). */
  toTag?: string;
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

/**
 * Parse diff-specific options from the `rest` array of CLI parsed args.
 *
 * Expected usage:
 *   sqlever diff [--from tag_a] [--to tag_b] [--format json]
 */
export function parseDiffArgs(rest: string[]): { fromTag?: string; toTag?: string; format?: "text" | "json" } {
  const opts: { fromTag?: string; toTag?: string; format?: "text" | "json" } = {};

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "--from") {
      const val = rest[++i];
      if (!val) {
        throw new Error("--from requires a tag name");
      }
      // Strip leading @ if present
      opts.fromTag = val.startsWith("@") ? val.slice(1) : val;
      i++;
      continue;
    }

    if (arg === "--to") {
      const val = rest[++i];
      if (!val) {
        throw new Error("--to requires a tag name");
      }
      // Strip leading @ if present
      opts.toTag = val.startsWith("@") ? val.slice(1) : val;
      i++;
      continue;
    }

    if (arg === "--format") {
      const val = rest[++i];
      if (val === "json" || val === "text") {
        opts.format = val;
      } else {
        throw new Error(
          `Invalid --format value '${val ?? ""}'. Expected 'text' or 'json'.`,
        );
      }
      i++;
      continue;
    }

    // Unknown flag — skip
    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Core logic (pure, testable)
// ---------------------------------------------------------------------------

/**
 * Find the index of the change that a tag is attached to.
 *
 * Tags reference a change_id. This finds the index of that change in
 * the plan's changes array.
 *
 * @returns The index of the change, or -1 if not found.
 */
export function findTagChangeIndex(plan: Plan, tagName: string): number {
  const tag = plan.tags.find((t) => t.name === tagName);
  if (!tag) return -1;

  return plan.changes.findIndex((c) => c.change_id === tag.change_id);
}

/**
 * Compute the diff for "pending" mode (no --from/--to).
 *
 * Returns all changes from the plan, each annotated with whether it
 * is "deployed" or "pending" based on the set of deployed change IDs.
 */
export function computePendingDiff(
  plan: Plan,
  deployedChangeIds: Set<string>,
): DiffResult {
  const entries: DiffEntry[] = plan.changes.map((c) => ({
    name: c.name,
    status: deployedChangeIds.has(c.change_id) ? "deployed" as const : "pending" as const,
    requires: c.requires,
    conflicts: c.conflicts,
    note: c.note,
  }));

  // Filter to only pending changes
  const pendingEntries = entries.filter((e) => e.status === "pending");

  return {
    project: plan.project.name,
    from_tag: null,
    to_tag: null,
    changes: pendingEntries,
  };
}

/**
 * Compute the diff for "range" mode (--from tag_a --to tag_b).
 *
 * Returns all changes between the two tags (exclusive of the `from`
 * tag's change, inclusive of the `to` tag's change). Each change is
 * annotated with whether it is "deployed" or "pending".
 *
 * @throws Error if either tag is not found in the plan.
 * @throws Error if --from tag appears after --to tag in the plan.
 */
export function computeRangeDiff(
  plan: Plan,
  fromTag: string,
  toTag: string,
  deployedChangeIds: Set<string>,
): DiffResult {
  const fromIdx = findTagChangeIndex(plan, fromTag);
  if (fromIdx === -1) {
    throw new Error(`Tag "${fromTag}" not found in plan`);
  }

  const toIdx = findTagChangeIndex(plan, toTag);
  if (toIdx === -1) {
    throw new Error(`Tag "${toTag}" not found in plan`);
  }

  if (fromIdx > toIdx) {
    throw new Error(
      `Tag "${fromTag}" appears after "${toTag}" in the plan. ` +
      `Swap --from and --to to see changes in this range.`,
    );
  }

  // Changes between fromIdx (exclusive) and toIdx (inclusive)
  const rangeChanges = plan.changes.slice(fromIdx + 1, toIdx + 1);

  const entries: DiffEntry[] = rangeChanges.map((c) => ({
    name: c.name,
    status: deployedChangeIds.has(c.change_id) ? "deployed" as const : "pending" as const,
    requires: c.requires,
    conflicts: c.conflicts,
    note: c.note,
  }));

  return {
    project: plan.project.name,
    from_tag: fromTag,
    to_tag: toTag,
    changes: entries,
  };
}

// ---------------------------------------------------------------------------
// Text formatting
// ---------------------------------------------------------------------------

/**
 * Format a DiffResult as human-readable text lines.
 */
export function formatDiffText(result: DiffResult): string {
  const lines: string[] = [];

  lines.push(`# Project: ${result.project}`);

  if (result.from_tag && result.to_tag) {
    lines.push(`# Range: @${result.from_tag} .. @${result.to_tag}`);
  } else {
    lines.push(`# Showing pending changes`);
  }

  lines.push("");

  if (result.changes.length === 0) {
    lines.push("No changes found.");
    return lines.join("\n");
  }

  for (const entry of result.changes) {
    const marker = entry.status === "pending" ? "+" : " ";
    lines.push(`  ${marker} ${entry.name} [${entry.status}]`);

    if (entry.requires.length > 0) {
      lines.push(`      requires: ${entry.requires.join(", ")}`);
    }

    if (entry.conflicts.length > 0) {
      lines.push(`      conflicts: ${entry.conflicts.join(", ")}`);
    }

    if (entry.note) {
      lines.push(`      note: ${entry.note}`);
    }
  }

  lines.push("");
  const pendingCount = result.changes.filter((c) => c.status === "pending").length;
  const deployedCount = result.changes.filter((c) => c.status === "deployed").length;
  lines.push(`Total: ${result.changes.length} change(s) — ${pendingCount} pending, ${deployedCount} deployed`);

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Main command runner
// ---------------------------------------------------------------------------

/**
 * Run the diff command.
 *
 * Loads the plan from disk, optionally connects to the database to
 * determine which changes are deployed, then computes and prints the diff.
 */
export async function runDiff(args: ParsedArgs): Promise<void> {
  const diffOpts = parseDiffArgs(args.rest);
  const topDir = resolve(args.topDir ?? ".");
  const format = diffOpts.format ?? args.format;

  // Load config
  const config = loadConfig(topDir);

  // Read the plan file
  const planFilePath = args.planFile
    ? resolve(args.planFile)
    : join(topDir, config.core.plan_file);

  if (!existsSync(planFilePath)) {
    throw new Error(`plan file not found: ${planFilePath}. Run 'sqlever init' to initialize a project.`);
  }

  const planContent = readFileSync(planFilePath, "utf-8");
  const plan = parsePlan(planContent);

  // Validate tag arguments
  if (diffOpts.fromTag && !diffOpts.toTag) {
    throw new Error("--from requires --to");
  }
  if (diffOpts.toTag && !diffOpts.fromTag) {
    throw new Error("--to requires --from");
  }

  // Resolve target URI for DB connection
  const targetUri = resolveTargetUri(config, args.dbUri, args.target);

  // Get deployed change IDs (empty set if no DB connection)
  let deployedChangeIds = new Set<string>();

  if (targetUri) {
    const { Registry } = await import("../db/registry");

    deployedChangeIds = await withDatabase(
      targetUri,
      { command: "diff", project: plan.project.name },
      async (db) => {
        const registry = new Registry(db);
        const deployedChanges = await registry.getDeployedChanges(plan.project.name);
        return new Set(deployedChanges.map((c) => c.change_id));
      },
    );
  }

  // Compute the diff
  let result: DiffResult;

  if (diffOpts.fromTag && diffOpts.toTag) {
    result = computeRangeDiff(plan, diffOpts.fromTag, diffOpts.toTag, deployedChangeIds);
  } else {
    result = computePendingDiff(plan, deployedChangeIds);
  }

  // Output
  if (format === "json") {
    jsonOut(result);
  } else {
    info(formatDiffText(result));
  }
}
