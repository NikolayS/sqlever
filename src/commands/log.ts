// src/commands/log.ts — sqlever log command
//
// Shows deployment history from sqitch.events. Supports filtering by
// event type, pagination (limit/offset), ordering, and JSON output.

import { Registry, type Event } from "../db/registry";
import { info, json, table, verbose, getConfig } from "../output";
import type { ParsedArgs } from "../cli";
import { loadConfig } from "../config/index";
import { resolveTargetUri, withDatabase } from "./shared";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type EventFilter = "deploy" | "revert" | "fail";

export interface LogOptions {
  /** Database connection URI. */
  dbUri: string;
  /** Project name (resolved from config/target). */
  project: string;
  /** Filter by event type. */
  event?: EventFilter;
  /** Maximum number of events to return. */
  limit?: number;
  /** Number of events to skip. */
  offset?: number;
  /** If true, show oldest-first (ASC); default is newest-first (DESC). */
  reverse?: boolean;
  /** Output format override. */
  format?: "text" | "json";
}

// ---------------------------------------------------------------------------
// Argument parsing for the log subcommand
// ---------------------------------------------------------------------------

/**
 * Parse the `rest` array from the CLI into LogOptions.
 *
 * Expected usage:
 *   sqlever log [--event deploy|revert|fail] [--limit N] [--offset N] [--reverse] [--format json]
 */
export function parseLogArgs(
  rest: string[],
  args: ParsedArgs,
): Omit<LogOptions, "dbUri" | "project"> {
  const opts: Omit<LogOptions, "dbUri" | "project"> = {};

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "--event") {
      const val = rest[++i];
      if (val === "deploy" || val === "revert" || val === "fail") {
        opts.event = val;
      } else {
        throw new Error(
          `Invalid --event value '${val ?? ""}'. Expected 'deploy', 'revert', or 'fail'.`,
        );
      }
      i++;
      continue;
    }

    if (arg === "--limit") {
      const val = rest[++i];
      const num = Number(val);
      if (!Number.isInteger(num) || num < 0) {
        throw new Error(
          `Invalid --limit value '${val ?? ""}'. Expected a non-negative integer.`,
        );
      }
      opts.limit = num;
      i++;
      continue;
    }

    if (arg === "--offset") {
      const val = rest[++i];
      const num = Number(val);
      if (!Number.isInteger(num) || num < 0) {
        throw new Error(
          `Invalid --offset value '${val ?? ""}'. Expected a non-negative integer.`,
        );
      }
      opts.offset = num;
      i++;
      continue;
    }

    if (arg === "--reverse") {
      opts.reverse = true;
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
// Format helpers
// ---------------------------------------------------------------------------

/**
 * Format a Date as a human-readable string for text output.
 */
function formatDate(d: Date): string {
  if (!(d instanceof Date) || isNaN(d.getTime())) {
    return String(d);
  }
  return d.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, "Z");
}

/**
 * Format events as a text table for terminal display.
 */
export function formatEventsText(events: Event[]): void {
  if (events.length === 0) {
    info("No events found.");
    return;
  }

  const rows = events.map((e) => ({
    event: e.event,
    change: e.change,
    committed_at: formatDate(e.committed_at),
    committer: e.committer_name,
    note: e.note || "",
  }));

  table(rows, ["event", "change", "committed_at", "committer", "note"]);
}

/**
 * Format events as JSON.
 */
export function formatEventsJson(events: Event[]): void {
  json(events);
}

// ---------------------------------------------------------------------------
// Main log logic
// ---------------------------------------------------------------------------

/**
 * Execute the `log` command.
 *
 * Connects to the database, queries sqitch.events with the given filters,
 * and prints results in the requested format.
 */
export async function runLog(
  opts: LogOptions,
): Promise<void> {
  verbose(`Connecting to database for log...`);

  await withDatabase(opts.dbUri, { command: "log", project: opts.project }, async (db) => {
    const registry = new Registry(db);

    const events = await registry.getEvents(opts.project, {
      event: opts.event,
      limit: opts.limit,
      offset: opts.offset,
      reverse: opts.reverse,
    });

    const config = getConfig();
    const format = opts.format ?? config.format;

    if (format === "json") {
      formatEventsJson(events);
    } else {
      formatEventsText(events);
    }
  });
}

/**
 * Entry point called from CLI dispatch. Resolves config, parses
 * subcommand flags, and delegates to runLog.
 */
export async function runLogCommand(args: ParsedArgs): Promise<void> {
  const logOpts = parseLogArgs(args.rest, args);

  // Resolve database URI from args or config
  const config = loadConfig(args.topDir);

  const dbUri = resolveTargetUri(config, args.dbUri, args.target)
    ?? undefined;

  if (!dbUri) {
    throw new Error("no database URI specified. Use --db-uri or configure a target.");
  }

  // Resolve project name from config
  const project = resolveProjectName(config);

  await runLog({
    dbUri,
    project,
    ...logOpts,
  });
}

// ---------------------------------------------------------------------------
// Config resolution helpers
// ---------------------------------------------------------------------------

/**
 * Resolve project name from sqitch.conf or fallback to directory name.
 */
function resolveProjectName(
  config: ReturnType<typeof loadConfig>,
): string {
  // Try to get from sqitch.conf entries
  for (const entry of config.sqitchConf.entries) {
    if (entry.key.toLowerCase() === "core.project") {
      return entry.value;
    }
  }

  // Fallback: use the current directory name
  const cwd = process.cwd();
  return cwd.split("/").pop() ?? "unknown";
}
