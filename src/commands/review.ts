// src/commands/review.ts — sqlever review command
//
// Structured risk report suitable for posting as a PR comment (Markdown output).
// Designed to be called by AI coding agents reviewing PRs.
//
// Usage:
//   sqlever review file.sql              — review a single migration
//   sqlever review dir/                  — review all .sql files in a directory
//   sqlever review                       — review pending migrations from sqitch.plan
//   sqlever review --all                 — review all migrations from sqitch.plan
//   sqlever review --changed             — review files changed in git diff
//
// Options:
//   --format markdown|text|json          — output format (default: markdown)
//
// The markdown output is ready to pipe to `gh pr comment`:
//   sqlever review --format markdown | gh pr comment --body-file -
//
// Implements SPEC Section 5.7 (GitHub issue #107).

import { existsSync, readFileSync, statSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { Analyzer } from "../analysis/index";
import { defaultRegistry } from "../analysis/registry";
import { allRules } from "../analysis/rules/index";
import type { Finding } from "../analysis/types";
import type { AnalysisConfig } from "../analysis/types";
import {
  runReview,
  formatReview,
  type ReviewResult,
  type ReviewOptions,
  type LLMProvider,
} from "../ai/review";
import { resolveFromPlan, resolveChangedFiles } from "./shared";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type ReviewFormat = "markdown" | "text" | "json";

export interface ReviewCommandOptions {
  /** Positional arguments (file paths / directories). */
  targets: string[];
  /** Output format. */
  format: ReviewFormat;
  /** Review all migrations, not just pending. */
  all: boolean;
  /** Review files changed in git diff. */
  changed: boolean;
  /** Rules to forcibly skip (--force-rule). */
  forceRules: string[];
  /** Project top directory. */
  topDir?: string;
  /** Plan file path override. */
  planFile?: string;
  /** LLM provider (optional, for explanations). */
  llm?: LLMProvider;
}

export interface ReviewCommandResult {
  /** The review result. */
  review: ReviewResult;
  /** Exit code: 0 if no errors, 2 if errors found. */
  exitCode: number;
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

/**
 * Parse review-specific arguments from the rest array.
 */
export function parseReviewArgs(rest: string[]): ReviewCommandOptions {
  const opts: ReviewCommandOptions = {
    targets: [],
    format: "markdown",
    all: false,
    changed: false,
    forceRules: [],
  };

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "--format") {
      const val = rest[i + 1];
      if (val === "markdown" || val === "text" || val === "json") {
        opts.format = val;
      } else {
        throw new Error(
          `Invalid --format value '${val ?? ""}'. Expected markdown, text, or json.`,
        );
      }
      i += 2;
      continue;
    }

    if (arg === "--all") {
      opts.all = true;
      i++;
      continue;
    }

    if (arg === "--changed") {
      opts.changed = true;
      i++;
      continue;
    }

    if (arg === "--force-rule") {
      const val = rest[i + 1];
      if (!val) {
        throw new Error("--force-rule requires a rule ID argument");
      }
      opts.forceRules.push(val);
      i += 2;
      continue;
    }

    if (arg === "--top-dir") {
      opts.topDir = rest[i + 1];
      i += 2;
      continue;
    }

    if (arg === "--plan-file") {
      opts.planFile = rest[i + 1];
      i += 2;
      continue;
    }

    // Positional argument — file or directory target
    opts.targets.push(arg);
    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// File resolution
// ---------------------------------------------------------------------------

/**
 * Collect all .sql files from a directory (non-recursive).
 */
function collectSqlFiles(dirPath: string): string[] {
  const entries = readdirSync(dirPath, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.endsWith(".sql"))
    .map((e) => join(dirPath, e.name))
    .sort();
}

/**
 * Resolve explicit targets (files and directories) to a list of .sql file paths.
 */
function resolveExplicitTargets(targets: string[]): string[] {
  const files: string[] = [];
  for (const target of targets) {
    const resolved = resolve(target);
    if (!existsSync(resolved)) {
      throw new Error(`Path not found: ${target}`);
    }
    const stat = statSync(resolved);
    if (stat.isDirectory()) {
      files.push(...collectSqlFiles(resolved));
    } else {
      files.push(resolved);
    }
  }
  return files;
}

// resolveFromPlan and resolveChangedFiles moved to shared.ts

// ---------------------------------------------------------------------------
// Core review command
// ---------------------------------------------------------------------------

/**
 * Run the review command.
 *
 * @returns ReviewCommandResult with review data and exit code.
 */
export async function runReviewCommand(
  opts: ReviewCommandOptions,
): Promise<ReviewCommandResult> {
  // Register all rules into the default registry (idempotent)
  for (const rule of allRules) {
    if (!defaultRegistry.has(rule.id)) {
      defaultRegistry.register(rule);
    }
  }

  const analyzer = new Analyzer(defaultRegistry);
  await analyzer.ensureWasm();

  // Resolve file list
  let files: string[];

  if (opts.targets.length > 0) {
    files = resolveExplicitTargets(opts.targets);
  } else if (opts.changed) {
    files = resolveChangedFiles();
  } else {
    const topDir = opts.topDir ?? ".";
    const planFile = opts.planFile ?? join(topDir, "sqitch.plan");
    const deployDir = join(topDir, "deploy");

    if (!existsSync(planFile)) {
      if (opts.planFile) {
        throw new Error(`plan file not found: ${planFile}`);
      }
      // No plan file and no explicit targets — empty review
      const emptyResult: ReviewResult = {
        risk: "low",
        findings: [],
        filesReviewed: [],
        errorCount: 0,
        warnCount: 0,
        infoCount: 0,
      };
      const output = formatReview(emptyResult, opts.format);
      process.stdout.write(output);
      return { review: emptyResult, exitCode: 0 };
    }

    files = resolveFromPlan(planFile, deployDir);
  }

  // Build analysis config
  const config: AnalysisConfig = {
    skip: [...opts.forceRules],
  };

  // Read SQL contents for LLM context
  const sqlContents = new Map<string, string>();
  const allFindings: Finding[] = [];

  for (const filePath of files) {
    try {
      const sql = readFileSync(filePath, "utf-8");
      sqlContents.set(filePath, sql);
      const findings = analyzer.analyze(filePath, { config });
      allFindings.push(...findings);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      allFindings.push({
        ruleId: "analyze-error",
        severity: "error",
        message: `Failed to analyze file: ${message}`,
        location: { file: filePath, line: 1, column: 1 },
      });
    }
  }

  // Run review
  const reviewOpts: ReviewOptions = {
    format: opts.format,
    llm: opts.llm,
    sqlContents,
  };

  const result = await runReview(allFindings, files, reviewOpts);

  // Format and print output
  const output = formatReview(result, opts.format);
  process.stdout.write(output);

  // Exit code: 2 if errors found
  const exitCode = result.errorCount > 0 ? 2 : 0;

  return { review: result, exitCode };
}
