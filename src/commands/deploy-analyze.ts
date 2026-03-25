// src/commands/deploy-analyze.ts -- pre-deploy static analysis (R4)
//
// Runs the analysis engine on pending deploy scripts before migration
// execution. Returns findings and whether deployment should be blocked.
//
// Separated from deploy.ts to keep the deploy module focused on execution
// and to make the analysis integration independently testable.

import { Analyzer } from "../analysis/index";
import { defaultRegistry } from "../analysis/registry";
import { allRules } from "../analysis/rules/index";
import {
  formatText,
  computeSummary,
  type Finding,
} from "../analysis/reporter";
import type { AnalysisConfig } from "../analysis/types";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PreDeployAnalysisOptions {
  /** Rules to forcibly skip (--force-rule). */
  forceRules: string[];
  /** Bypass all analysis errors (--force). */
  force: boolean;
}

export interface PreDeployAnalysisResult {
  /** All findings across analyzed scripts. */
  findings: Finding[];
  /** Number of files analyzed. */
  filesAnalyzed: number;
  /** Whether deploy should be blocked (error-severity findings exist and --force was not set). */
  blocked: boolean;
  /** Formatted text output (for non-JSON mode). */
  output: string;
  /** Summary counts. */
  summary: { errors: number; warnings: number; info: number };
}

// ---------------------------------------------------------------------------
// Core function
// ---------------------------------------------------------------------------

/**
 * Run static analysis on deploy scripts before migration execution.
 *
 * Per spec R4:
 *   - error-severity findings block deploy (unless --force)
 *   - warn/info findings are displayed but do not block
 *   - --force-rule skips specific rules
 *   - --force bypasses all analysis errors
 *
 * @param scriptPaths - absolute paths to deploy .sql files
 * @param options - force/forceRules configuration
 * @returns analysis result with findings, block status, and formatted output
 */
export async function runPreDeployAnalysis(
  scriptPaths: string[],
  options: PreDeployAnalysisOptions,
): Promise<PreDeployAnalysisResult> {
  // Register all rules into the default registry (idempotent)
  for (const rule of allRules) {
    if (!defaultRegistry.has(rule.id)) {
      defaultRegistry.register(rule);
    }
  }

  const analyzer = new Analyzer(defaultRegistry);
  await analyzer.ensureWasm();

  // Build analysis config
  const config: AnalysisConfig = {
    skip: [...options.forceRules],
  };

  // Analyze all pending deploy scripts
  const allFindings: Finding[] = [];

  for (const filePath of scriptPaths) {
    try {
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

  const summary = computeSummary(allFindings);
  const blocked = summary.errors > 0 && !options.force;

  // Format findings for text output (only if there are findings to show)
  let output = "";
  if (allFindings.length > 0) {
    output = formatText(allFindings, undefined, process.stdout.isTTY ?? false);
  }

  return {
    findings: allFindings,
    filesAnalyzed: scriptPaths.length,
    blocked,
    output,
    summary,
  };
}
