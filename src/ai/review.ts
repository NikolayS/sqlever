// src/ai/review.ts — AI-powered migration review engine
//
// Combines static analysis findings (from Analyzer) with LLM explanation
// to produce a structured risk report suitable for posting as a PR comment.
//
// Implements SPEC Section 5.7 — `sqlever review`.

import type { Finding, Severity } from "../analysis/types";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Risk level for the overall migration review. */
export type RiskLevel = "high" | "medium" | "low";

/** A single entry in the findings table. */
export interface FindingEntry {
  ruleId: string;
  severity: Severity;
  message: string;
  file: string;
  line: number;
  suggestion?: string;
}

/** LLM-generated explanation of what the migration does. */
export interface LLMExplanation {
  /** Plain-English summary of the migration. */
  summary: string;
  /** Suggested improvements from the LLM. */
  suggestedImprovements: string[];
}

/** Interface for LLM providers — allows mocking in tests. */
export interface LLMProvider {
  /**
   * Generate an explanation for the given SQL migration(s).
   *
   * @param sql - The SQL text of the migration(s)
   * @param findings - Static analysis findings for context
   * @returns LLM-generated explanation
   */
  explain(sql: string, findings: FindingEntry[]): Promise<LLMExplanation>;
}

/** Result of the review process. */
export interface ReviewResult {
  /** Overall risk level. */
  risk: RiskLevel;
  /** Static analysis findings. */
  findings: FindingEntry[];
  /** LLM-generated explanation (present when LLM provider is available). */
  explanation?: LLMExplanation;
  /** Files that were reviewed. */
  filesReviewed: string[];
  /** Total count of errors. */
  errorCount: number;
  /** Total count of warnings. */
  warnCount: number;
  /** Total count of info findings. */
  infoCount: number;
}

/** Options for the review engine. */
export interface ReviewOptions {
  /** Output format. */
  format: "markdown" | "text" | "json";
  /** Optional LLM provider for explanations. */
  llm?: LLMProvider;
  /** SQL content of the files being reviewed (keyed by file path). */
  sqlContents?: Map<string, string>;
}

// ---------------------------------------------------------------------------
// Risk assessment
// ---------------------------------------------------------------------------

/**
 * Determine the overall risk level based on findings.
 *
 * - high: any error-level findings
 * - medium: any warn-level findings but no errors
 * - low: only info-level findings or no findings
 */
export function assessRisk(findings: FindingEntry[]): RiskLevel {
  let hasError = false;
  let hasWarn = false;

  for (const f of findings) {
    if (f.severity === "error") hasError = true;
    if (f.severity === "warn") hasWarn = true;
  }

  if (hasError) return "high";
  if (hasWarn) return "medium";
  return "low";
}

// ---------------------------------------------------------------------------
// Convert analysis findings to review entries
// ---------------------------------------------------------------------------

/**
 * Convert raw analysis findings to structured review entries.
 */
export function toFindingEntries(findings: readonly Finding[]): FindingEntry[] {
  return findings.map((f) => ({
    ruleId: f.ruleId,
    severity: f.severity,
    message: f.message,
    file: f.location.file,
    line: f.location.line,
    suggestion: f.suggestion,
  }));
}

// ---------------------------------------------------------------------------
// Count helpers
// ---------------------------------------------------------------------------

function countBySeverity(
  entries: FindingEntry[],
  severity: Severity,
): number {
  return entries.filter((e) => e.severity === severity).length;
}

// ---------------------------------------------------------------------------
// Review engine
// ---------------------------------------------------------------------------

/**
 * Run a review: combine static analysis findings with optional LLM explanation.
 *
 * @param analysisFindings - Findings from the Analyzer
 * @param filesReviewed - List of files that were analyzed
 * @param options - Review options (format, LLM provider, SQL contents)
 * @returns ReviewResult
 */
export async function runReview(
  analysisFindings: readonly Finding[],
  filesReviewed: string[],
  options: ReviewOptions,
): Promise<ReviewResult> {
  const entries = toFindingEntries(analysisFindings);
  const risk = assessRisk(entries);

  let explanation: LLMExplanation | undefined;

  if (options.llm && options.sqlContents && options.sqlContents.size > 0) {
    // Concatenate all SQL for LLM context
    const allSql = Array.from(options.sqlContents.entries())
      .map(([path, sql]) => `-- File: ${path}\n${sql}`)
      .join("\n\n");

    try {
      explanation = await options.llm.explain(allSql, entries);
    } catch {
      // LLM failure is non-fatal — review still works without explanation
    }
  }

  return {
    risk,
    findings: entries,
    explanation,
    filesReviewed,
    errorCount: countBySeverity(entries, "error"),
    warnCount: countBySeverity(entries, "warn"),
    infoCount: countBySeverity(entries, "info"),
  };
}

// ---------------------------------------------------------------------------
// Markdown formatter
// ---------------------------------------------------------------------------

const RISK_EMOJI: Record<RiskLevel, string> = {
  high: "\u{1F534}",   // red circle
  medium: "\u{1F7E1}", // yellow circle
  low: "\u{1F7E2}",    // green circle
};

/**
 * Format a review result as Markdown suitable for a PR comment.
 */
export function formatReviewMarkdown(result: ReviewResult): string {
  const lines: string[] = [];

  // Header
  lines.push("## sqlever review");
  lines.push("");

  // Risk summary
  const emoji = RISK_EMOJI[result.risk];
  const riskLabel = result.risk.toUpperCase();
  lines.push(`**Risk: ${emoji} ${riskLabel}**`);
  lines.push("");

  // Counts
  const counts: string[] = [];
  if (result.errorCount > 0) {
    counts.push(
      `${result.errorCount} error${result.errorCount !== 1 ? "s" : ""}`,
    );
  }
  if (result.warnCount > 0) {
    counts.push(
      `${result.warnCount} warning${result.warnCount !== 1 ? "s" : ""}`,
    );
  }
  if (result.infoCount > 0) {
    counts.push(
      `${result.infoCount} info`,
    );
  }
  if (counts.length > 0) {
    lines.push(`${counts.join(", ")} across ${result.filesReviewed.length} file${result.filesReviewed.length !== 1 ? "s" : ""}`);
  } else {
    lines.push(`No issues found across ${result.filesReviewed.length} file${result.filesReviewed.length !== 1 ? "s" : ""}`);
  }
  lines.push("");

  // Findings table
  if (result.findings.length > 0) {
    lines.push("### Analysis findings");
    lines.push("");
    lines.push("| Severity | Rule | Message | File | Line |");
    lines.push("|----------|------|---------|------|------|");

    for (const f of result.findings) {
      const sevIcon = severityIcon(f.severity);
      const escapedMsg = escapeMarkdown(f.message);
      const shortFile = shortenPath(f.file);
      lines.push(
        `| ${sevIcon} ${f.severity} | ${f.ruleId} | ${escapedMsg} | ${shortFile} | ${f.line} |`,
      );
    }
    lines.push("");
  }

  // Suggestions from static analysis
  const suggestions = result.findings.filter((f) => f.suggestion);
  if (suggestions.length > 0) {
    lines.push("### Suggested improvements");
    lines.push("");
    for (const f of suggestions) {
      lines.push(`- **${f.ruleId}**: ${f.suggestion}`);
    }
    lines.push("");
  }

  // LLM explanation
  if (result.explanation) {
    lines.push("### What this migration does");
    lines.push("");
    lines.push(result.explanation.summary);
    lines.push("");

    if (result.explanation.suggestedImprovements.length > 0) {
      lines.push("### AI-suggested improvements");
      lines.push("");
      for (const improvement of result.explanation.suggestedImprovements) {
        lines.push(`- ${improvement}`);
      }
      lines.push("");
    }
  }

  // Footer
  lines.push("---");
  lines.push("*Generated by sqlever review*");

  return lines.join("\n") + "\n";
}

// ---------------------------------------------------------------------------
// Text formatter
// ---------------------------------------------------------------------------

/**
 * Format a review result as plain text.
 */
export function formatReviewText(result: ReviewResult): string {
  const lines: string[] = [];

  lines.push(`Risk: ${result.risk.toUpperCase()}`);
  lines.push("");

  // Counts
  const counts: string[] = [];
  if (result.errorCount > 0) counts.push(`${result.errorCount} error(s)`);
  if (result.warnCount > 0) counts.push(`${result.warnCount} warning(s)`);
  if (result.infoCount > 0) counts.push(`${result.infoCount} info`);
  if (counts.length > 0) {
    lines.push(`${counts.join(", ")} across ${result.filesReviewed.length} file(s)`);
  } else {
    lines.push(`No issues found across ${result.filesReviewed.length} file(s)`);
  }
  lines.push("");

  // Findings
  if (result.findings.length > 0) {
    lines.push("Findings:");
    for (const f of result.findings) {
      lines.push(`  ${f.severity} ${f.ruleId}: ${f.message}`);
      lines.push(`    at ${f.file}:${f.line}`);
      if (f.suggestion) {
        lines.push(`    suggestion: ${f.suggestion}`);
      }
    }
    lines.push("");
  }

  // LLM explanation
  if (result.explanation) {
    lines.push("Explanation:");
    lines.push(`  ${result.explanation.summary}`);
    if (result.explanation.suggestedImprovements.length > 0) {
      lines.push("");
      lines.push("AI-suggested improvements:");
      for (const s of result.explanation.suggestedImprovements) {
        lines.push(`  - ${s}`);
      }
    }
    lines.push("");
  }

  return lines.join("\n") + "\n";
}

// ---------------------------------------------------------------------------
// JSON formatter
// ---------------------------------------------------------------------------

/**
 * Format a review result as JSON.
 */
export function formatReviewJson(result: ReviewResult): string {
  return JSON.stringify(result, null, 2) + "\n";
}

// ---------------------------------------------------------------------------
// Unified format dispatcher
// ---------------------------------------------------------------------------

/**
 * Format a review result using the specified format.
 */
export function formatReview(
  result: ReviewResult,
  format: "markdown" | "text" | "json",
): string {
  switch (format) {
    case "markdown":
      return formatReviewMarkdown(result);
    case "text":
      return formatReviewText(result);
    case "json":
      return formatReviewJson(result);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function severityIcon(severity: Severity): string {
  switch (severity) {
    case "error":
      return "\u{274C}";  // red cross
    case "warn":
      return "\u{26A0}\u{FE0F}";  // warning sign
    case "info":
      return "\u{2139}\u{FE0F}";  // info icon
  }
}

/**
 * Escape pipe characters in Markdown table cells.
 */
function escapeMarkdown(text: string): string {
  return text.replace(/\|/g, "\\|");
}

/**
 * Shorten a file path for display by taking only the last 2 segments.
 */
export function shortenPath(filePath: string): string {
  const parts = filePath.split("/");
  if (parts.length <= 2) return filePath;
  return parts.slice(-2).join("/");
}
