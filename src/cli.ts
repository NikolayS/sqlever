#!/usr/bin/env bun
// sqlever — Sqitch-compatible PostgreSQL migration tool

import packageJson from "../package.json";
import { runInit } from "./commands/init";
import { runStatus } from "./commands/status";
import { runDeploy } from "./commands/deploy";
import { setConfig, type OutputFormat } from "./output";
import { parseAddArgs, runAdd } from "./commands/add";
import { parseExpandArgs, runExpandAdd } from "./expand-contract/generator";
import { runLogCommand } from "./commands/log";
import { runRevert } from "./commands/revert";
import { parseTagArgs, runTag } from "./commands/tag";
import { parseReworkArgs, runRework } from "./commands/rework";
import { parseShowArgs, runShow } from "./commands/show";
import { runPlan } from "./commands/plan";
import { runVerify } from "./commands/verify";
import { parseAnalyzeArgs, runAnalyze } from "./commands/analyze";
import { parseExplainArgs, runExplain } from "./commands/explain";
import { runDoctor } from "./commands/doctor";
import { runDiff } from "./commands/diff";
import { parseReviewArgs, runReviewCommand } from "./commands/review";
import { runBatch } from "./commands/batch";
import { runRebase } from "./commands/rebase";
import { runConfig } from "./commands/config";

// ---------------------------------------------------------------------------
// Command registry — all commands from SPEC R1 plus sqlever extensions
// ---------------------------------------------------------------------------

/** Description for each supported command, used in --help output. */
const COMMANDS: Record<string, string> = {
  init: "Initialize a new project directory",
  add: "Add a new migration change",
  deploy: "Deploy pending changes to a database",
  revert: "Revert deployed changes from a database",
  verify: "Verify deployed changes against a database",
  status: "Show current deployment status",
  log: "Show deployment event history",
  tag: "Tag the latest change in the plan",
  rework: "Rework an existing change in the plan",
  rebase: "Revert and re-deploy changes",
  bundle: "Package project for distribution",
  checkout: "Deploy or revert to match a VCS branch",
  show: "Show change, tag, or script details",
  plan: "Show the contents of the plan file",
  upgrade: "Upgrade the registry schema",
  engine: "Manage database engine configuration",
  target: "Manage deploy target configuration",
  config: "Get or set configuration values",
  analyze: "Analyze migration SQL for risky patterns",
  explain: "Explain a migration in plain language",
  review: "Review migrations for common issues",
  batch: "Manage batched background data migrations",
  diff: "Show differences between plan states",
  doctor: "Check project setup for problems",
  help: "Show help for a command",
};

/** Sorted command names for display. */
const COMMAND_NAMES = Object.keys(COMMANDS).sort();

// ---------------------------------------------------------------------------
// Flag parsing
// ---------------------------------------------------------------------------

export interface ParsedArgs {
  /** The command to run (e.g. "deploy"), or undefined if none given. */
  command: string | undefined;
  /** Remaining positional arguments after the command. */
  rest: string[];
  /** --help / -h */
  help: boolean;
  /** --version / -V */
  version: boolean;
  /** --format json|text */
  format: OutputFormat;
  /** --quiet / -q */
  quiet: boolean;
  /** --verbose / -v */
  verbose: boolean;
  /** --db-uri <uri> */
  dbUri: string | undefined;
  /** --plan-file <path> */
  planFile: string | undefined;
  /** --top-dir <path> */
  topDir: string | undefined;
  /** --registry <name> */
  registry: string | undefined;
  /** --target <target> */
  target: string | undefined;
}

/**
 * Parse argv into structured args. Extracts top-level flags that appear
 * before or after the command. The first non-flag token is treated as the
 * command; everything after it goes into `rest`.
 */
export function parseArgs(argv: string[]): ParsedArgs {
  const result: ParsedArgs = {
    command: undefined,
    rest: [],
    help: false,
    version: false,
    format: "text",
    quiet: false,
    verbose: false,
    dbUri: undefined,
    planFile: undefined,
    topDir: undefined,
    registry: undefined,
    target: undefined,
  };

  let i = 0;
  while (i < argv.length) {
    const arg = argv[i]!;

    // --- Boolean flags ---
    if (arg === "--help" || arg === "-h") {
      result.help = true;
      i++;
      continue;
    }
    if (arg === "--version" || arg === "-V") {
      result.version = true;
      i++;
      continue;
    }
    if (arg === "--quiet" || arg === "-q") {
      result.quiet = true;
      i++;
      continue;
    }
    if (arg === "--verbose" || arg === "-v") {
      result.verbose = true;
      i++;
      continue;
    }

    // --- Value flags ---
    if (arg === "--format") {
      const val = argv[i + 1];
      if (val === "json" || val === "text") {
        result.format = val;
        // When a command is already set, also forward to rest so that
        // command-specific parsers (e.g. analyze) can see the flag.
        if (result.command !== undefined) {
          result.rest.push(arg, val);
        }
      } else if (result.command !== undefined) {
        // Command-specific format value (e.g. github-annotations,
        // gitlab-codequality for the analyze command) — pass through to rest
        // without rejecting.
        result.rest.push(arg, val ?? "");
      } else {
        process.stderr.write(
          `sqlever: invalid --format value '${val ?? ""}'. Expected 'text' or 'json'.\n`,
        );
        process.exit(1);
      }
      i += 2;
      continue;
    }
    if (arg === "--db-uri") {
      result.dbUri = argv[++i];
      i++;
      continue;
    }
    if (arg === "--plan-file") {
      result.planFile = argv[++i];
      i++;
      continue;
    }
    if (arg === "--top-dir") {
      result.topDir = argv[++i];
      i++;
      continue;
    }
    if (arg === "--registry") {
      result.registry = argv[++i];
      i++;
      continue;
    }
    if (arg === "--target") {
      result.target = argv[++i];
      i++;
      continue;
    }

    // --- Command or positional argument ---
    if (result.command === undefined) {
      result.command = arg;
    } else {
      result.rest.push(arg);
    }
    i++;
  }

  return result;
}

// ---------------------------------------------------------------------------
// Help text
// ---------------------------------------------------------------------------

function printTopLevelHelp(): void {
  const maxLen = Math.max(...COMMAND_NAMES.map((c) => c.length));
  const cmdLines = COMMAND_NAMES.map(
    (c) => `  ${c.padEnd(maxLen)}  ${COMMANDS[c]}`,
  ).join("\n");

  process.stdout.write(`sqlever — Sqitch-compatible PostgreSQL migration tool

Usage:
  sqlever <command> [options]

Commands:
${cmdLines}

Global options:
  --help, -h         Show this help message
  --version, -V      Show version number
  --format <fmt>     Output format: text (default) or json
  --quiet, -q        Suppress informational output
  --verbose, -v      Show verbose/debug output
  --db-uri <uri>     Database connection URI
  --plan-file <path> Path to plan file (default: sqitch.plan)
  --top-dir <path>   Path to project top directory
  --registry <name>  Registry schema name (default: sqitch)
  --target <target>  Deploy target name

https://github.com/NikolayS/sqlever
`);
}

function printCommandHelp(command: string): void {
  if (!(command in COMMANDS)) {
    process.stderr.write(`sqlever: unknown command '${command}'\n`);
    process.exit(1);
  }
  process.stdout.write(
    `sqlever ${command} — ${COMMANDS[command]}\n\nNo detailed help available yet.\n`,
  );
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

function stubHandler(command: string): never {
  process.stderr.write(`sqlever ${command}: not yet implemented -- planned for v1.1\n`);
  process.exit(1);
  // TypeScript needs this even though process.exit() is noreturn
  throw new Error("unreachable");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

export async function main(argv: string[] = process.argv.slice(2)): Promise<void> {
  const args = parseArgs(argv);

  // --version takes precedence (matches Sqitch behavior)
  if (args.version) {
    process.stdout.write(packageJson.version + "\n");
    process.exit(0);
  }

  // Wire up the output module based on parsed flags
  setConfig({
    format: args.format,
    quiet: args.quiet,
    verbose: args.verbose,
  });

  // --help with no command => top-level help
  if (args.help && !args.command) {
    printTopLevelHelp();
    process.exit(0);
  }

  // No command at all => top-level help
  if (!args.command) {
    printTopLevelHelp();
    process.exit(0);
  }

  // --help with a command => command-specific help
  if (args.help) {
    printCommandHelp(args.command);
    process.exit(0);
  }

  // "help" command — treat like --help for the next argument
  if (args.command === "help") {
    const subcommand = args.rest[0];
    if (subcommand) {
      printCommandHelp(subcommand);
    } else {
      printTopLevelHelp();
    }
    process.exit(0);
  }

  // Unknown command
  if (!(args.command in COMMANDS)) {
    process.stderr.write(`sqlever: unknown command '${args.command}'\n`);
    process.exit(1);
  }

  // --- Dispatch to implemented commands ---
  // All commands are dispatched via a single try/catch with consistent
  // error formatting: "sqlever <command>: <message>".
  const command = args.command;
  try {
    const exitCode = await dispatchCommand(command, args);
    if (exitCode !== 0) process.exit(exitCode);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    process.stderr.write(`sqlever ${command}: ${msg}\n`);
    process.exit(1);
  }
}

/**
 * Dispatch a command and return its exit code (0 = success).
 *
 * Commands that don't return an exit code return 0 on success.
 * Errors are propagated as exceptions to the caller.
 */
async function dispatchCommand(command: string, args: ParsedArgs): Promise<number> {
  switch (command) {
    case "init":
      await runInit(args);
      return 0;

    case "add": {
      if (args.rest.includes("--expand")) {
        const expandOpts = parseExpandArgs(args.rest);
        expandOpts.topDir = args.topDir;
        await runExpandAdd(expandOpts);
      } else {
        const addOpts = parseAddArgs(args.rest);
        addOpts.topDir = args.topDir;
        await runAdd(addOpts);
      }
      return 0;
    }

    case "deploy":
      return await runDeploy(args);

    case "log":
      await runLogCommand(args);
      return 0;

    case "revert":
      return await runRevert(args);

    case "tag": {
      const tagOpts = parseTagArgs(args.rest);
      tagOpts.topDir = args.topDir;
      await runTag(tagOpts);
      return 0;
    }

    case "rework": {
      const reworkOpts = parseReworkArgs(args.rest);
      reworkOpts.topDir = args.topDir;
      await runRework(reworkOpts);
      return 0;
    }

    case "show": {
      const showOpts = parseShowArgs(args.rest);
      if (args.topDir !== undefined) showOpts.topDir = args.topDir;
      if (args.planFile !== undefined) showOpts.planFile = args.planFile;
      runShow(showOpts);
      return 0;
    }

    case "verify":
      return await runVerify(args);

    case "status":
      await runStatus(args);
      return 0;

    case "plan":
      runPlan(args);
      return 0;

    case "analyze": {
      const analyzeOpts = parseAnalyzeArgs(args.rest);
      if (args.topDir !== undefined) analyzeOpts.topDir = args.topDir;
      if (args.planFile !== undefined) analyzeOpts.planFile = args.planFile;
      const result = await runAnalyze(analyzeOpts);
      return result.exitCode;
    }

    case "explain": {
      const explainOpts = parseExplainArgs(args.rest);
      return await runExplain(explainOpts);
    }

    case "doctor":
      return runDoctor(args);

    case "diff":
      await runDiff(args);
      return 0;

    case "review": {
      const reviewOpts = parseReviewArgs(args.rest);
      if (args.topDir !== undefined) reviewOpts.topDir = args.topDir;
      if (args.planFile !== undefined) reviewOpts.planFile = args.planFile;
      const reviewResult = await runReviewCommand(reviewOpts);
      return reviewResult.exitCode;
    }

    case "batch":
      await runBatch(args);
      return 0;

    case "rebase":
      return await runRebase(args);

    case "config":
      return runConfig(args);

    default:
      stubHandler(command);
  }
}

// Run when executed directly (not when imported by tests)
if (import.meta.main) {
  main();
}
