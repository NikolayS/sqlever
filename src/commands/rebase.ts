// src/commands/rebase.ts — sqlever rebase command
//
// Rebase is a composite command: revert to a given change, then re-deploy.
// Equivalent to `sqlever revert --to <change> -y && sqlever deploy`.
//
// Usage:
//   sqlever rebase --onto <change>           # revert to <change>, then deploy all
//   sqlever rebase --onto <change> --to <to> # revert to <change>, then deploy to <to>
//   sqlever rebase                           # revert all, then deploy all
//
// Flags inherited from revert:
//   -y / --no-prompt     Skip confirmation prompt
//
// Flags inherited from deploy:
//   --to <change>        Deploy up to this change after revert
//   --verify / --no-verify
//   --dry-run

import type { ParsedArgs } from "../cli";
import { runRevert } from "./revert";
import { runDeploy } from "./deploy";
import { info, error as logError } from "../output";

// ---------------------------------------------------------------------------
// Rebase-specific argument parsing
// ---------------------------------------------------------------------------

export interface RebaseOptions {
  /** Change to revert down to (--onto). Omit to revert all. */
  ontoChange?: string;
  /** Change to deploy up to (--to). Omit to deploy all pending. */
  toChange?: string;
  /** Skip confirmation prompt (-y / --no-prompt). */
  noPrompt: boolean;
}

/**
 * Parse rebase-specific options from CLI rest args.
 *
 * Extracts --onto and passes the remaining flags through to revert/deploy.
 */
export function parseRebaseOptions(rest: string[]): RebaseOptions {
  const opts: RebaseOptions = {
    noPrompt: false,
  };

  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--onto") {
      const val = rest[++i];
      if (val === undefined || val.startsWith("-")) {
        throw new Error(
          "Missing value for --onto. Usage: rebase --onto <change>",
        );
      }
      opts.ontoChange = val;
      i++;
      continue;
    }
    if (token === "--to") {
      const val = rest[++i];
      if (val === undefined || val.startsWith("-")) {
        throw new Error(
          "Missing value for --to. Usage: rebase --to <change>",
        );
      }
      opts.toChange = val;
      i++;
      continue;
    }
    if (token === "-y" || token === "--no-prompt") {
      opts.noPrompt = true;
      i++;
      continue;
    }

    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Main rebase command
// ---------------------------------------------------------------------------

/**
 * Execute the `rebase` command.
 *
 * Composes revert + deploy:
 * 1. Revert to the --onto change (or revert all if not specified)
 * 2. Deploy pending changes (up to --to if specified)
 *
 * Both steps reuse the existing runRevert/runDeploy implementations
 * by constructing synthetic ParsedArgs for each phase.
 */
export async function runRebase(args: ParsedArgs): Promise<number> {
  const opts = parseRebaseOptions(args.rest);

  // --- Phase 1: Revert ---
  info("Rebase phase 1/2: reverting...");

  const revertRest: string[] = [];
  if (opts.ontoChange) {
    revertRest.push("--to", opts.ontoChange);
  }
  // Always suppress prompt during rebase -- rebase is already a deliberate
  // composite action. Without -y the revert phase returns 0 on declined
  // confirmation (non-TTY / CI), which is indistinguishable from success,
  // causing rebase to silently skip revert and proceed to deploy.
  revertRest.push("-y");

  const revertArgs: ParsedArgs = {
    ...args,
    command: "revert",
    rest: revertRest,
  };

  const revertCode = await runRevert(revertArgs);
  if (revertCode !== 0) {
    logError("Rebase aborted: revert phase failed.");
    return revertCode;
  }

  // --- Phase 2: Deploy ---
  info("Rebase phase 2/2: deploying...");

  const deployRest: string[] = [];
  if (opts.toChange) {
    deployRest.push("--to", opts.toChange);
  }

  const deployArgs: ParsedArgs = {
    ...args,
    command: "deploy",
    rest: deployRest,
  };

  const deployCode = await runDeploy(deployArgs);
  if (deployCode !== 0) {
    logError("Rebase failed: deploy phase failed.");
    return deployCode;
  }

  info("Rebase complete.");
  return 0;
}
