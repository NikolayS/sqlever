// src/analysis/reporter.ts — Output formatting for analysis findings
//
// Supports four output formats:
//   text              — human-readable with ANSI colors (when TTY)
//   json              — structured JSON per SPEC schema
//   github-annotations — GitHub Actions workflow commands
//   gitlab-codequality — GitLab Code Quality JSON report

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type Severity = "error" | "warn" | "info";

export interface Location {
  file: string;
  line: number;
  column: number;
}

export interface Finding {
  ruleId: string;
  severity: Severity;
  message: string;
  location: Location;
  suggestion?: string;
}

export interface ReportMetadata {
  files_analyzed: number;
  rules_checked: number;
  duration_ms: number;
}

export interface ReportSummary {
  errors: number;
  warnings: number;
  info: number;
}

export interface JsonReport {
  version: 1;
  metadata: ReportMetadata;
  findings: Array<{
    ruleId: string;
    severity: Severity;
    message: string;
    location: Location;
    suggestion?: string;
  }>;
  summary: ReportSummary;
}

export interface GitLabCodeQualityEntry {
  description: string;
  check_name: string;
  fingerprint: string;
  severity: "critical" | "major" | "minor";
  location: {
    path: string;
    lines: {
      begin: number;
    };
  };
}

export type ReportFormat =
  | "text"
  | "json"
  | "github-annotations"
  | "gitlab-codequality";

// ---------------------------------------------------------------------------
// ANSI helpers
// ---------------------------------------------------------------------------

const ANSI = {
  red: "\x1b[31m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  reset: "\x1b[0m",
} as const;

function colorize(text: string, ...codes: string[]): string {
  return codes.join("") + text + ANSI.reset;
}

// ---------------------------------------------------------------------------
// Summary computation
// ---------------------------------------------------------------------------

export function computeSummary(findings: readonly Finding[]): ReportSummary {
  let errors = 0;
  let warnings = 0;
  let info = 0;
  for (const f of findings) {
    switch (f.severity) {
      case "error":
        errors++;
        break;
      case "warn":
        warnings++;
        break;
      case "info":
        info++;
        break;
    }
  }
  return { errors, warnings, info };
}

// ---------------------------------------------------------------------------
// Text formatter
// ---------------------------------------------------------------------------

/**
 * Format findings as human-readable text with ANSI colors.
 *
 * Each finding is rendered as:
 *   <severity> <ruleId>: <message>
 *     at <file>:<line>:<column>
 *     suggestion: <suggestion>
 *
 * A summary line follows all findings.
 *
 * @param useColors - Whether to use ANSI color codes (default: true)
 */
export function formatText(
  findings: readonly Finding[],
  filePath?: string,
  useColors: boolean = true,
): string {
  const lines: string[] = [];

  if (filePath) {
    const header = useColors
      ? colorize(filePath, ANSI.bold)
      : filePath;
    lines.push(header);
  }

  for (const f of findings) {
    const sevLabel = severityLabel(f.severity, useColors);
    const ruleId = useColors
      ? colorize(f.ruleId, ANSI.dim)
      : f.ruleId;
    lines.push(`  ${sevLabel} ${ruleId}: ${f.message}`);

    const loc = `${f.location.file}:${f.location.line}:${f.location.column}`;
    const locStr = useColors ? colorize(loc, ANSI.dim) : loc;
    lines.push(`    at ${locStr}`);

    if (f.suggestion) {
      const sug = useColors
        ? colorize(`suggestion: ${f.suggestion}`, ANSI.dim)
        : `suggestion: ${f.suggestion}`;
      lines.push(`    ${sug}`);
    }
  }

  // Summary line
  const summary = computeSummary(findings);
  const parts: string[] = [];
  if (summary.errors > 0) {
    const t = `${summary.errors} error${summary.errors !== 1 ? "s" : ""}`;
    parts.push(useColors ? colorize(t, ANSI.red) : t);
  }
  if (summary.warnings > 0) {
    const t = `${summary.warnings} warning${summary.warnings !== 1 ? "s" : ""}`;
    parts.push(useColors ? colorize(t, ANSI.yellow) : t);
  }
  if (summary.info > 0) {
    const t = `${summary.info} info`;
    parts.push(useColors ? colorize(t, ANSI.blue) : t);
  }

  if (parts.length > 0) {
    lines.push("");
    lines.push(parts.join(", "));
  } else {
    lines.push("");
    lines.push("No issues found.");
  }

  return lines.join("\n") + "\n";
}

function severityLabel(severity: Severity, useColors: boolean): string {
  switch (severity) {
    case "error":
      return useColors ? colorize("error", ANSI.red, ANSI.bold) : "error";
    case "warn":
      return useColors ? colorize("warn ", ANSI.yellow) : "warn ";
    case "info":
      return useColors ? colorize("info ", ANSI.blue) : "info ";
  }
}

// ---------------------------------------------------------------------------
// JSON formatter
// ---------------------------------------------------------------------------

/**
 * Format findings as structured JSON per SPEC schema.
 *
 * Returns a JSON string with the following shape:
 *   { version: 1, metadata, findings, summary }
 */
export function formatJson(
  findings: readonly Finding[],
  metadata: ReportMetadata,
): string {
  const summary = computeSummary(findings);
  const report: JsonReport = {
    version: 1,
    metadata,
    findings: findings.map((f) => {
      const entry: JsonReport["findings"][number] = {
        ruleId: f.ruleId,
        severity: f.severity,
        message: f.message,
        location: { ...f.location },
      };
      if (f.suggestion !== undefined) {
        entry.suggestion = f.suggestion;
      }
      return entry;
    }),
    summary,
  };
  return JSON.stringify(report, null, 2) + "\n";
}

// ---------------------------------------------------------------------------
// GitHub Annotations formatter
// ---------------------------------------------------------------------------

/**
 * Format findings as GitHub Actions workflow commands.
 *
 * Each finding becomes a line like:
 *   ::error file=<path>,line=<line>,col=<col>::<ruleId>: <message>
 *   ::warning file=<path>,line=<line>,col=<col>::<ruleId>: <message>
 *   ::notice file=<path>,line=<line>,col=<col>::<ruleId>: <message>
 *
 * Severity mapping: error → error, warn → warning, info → notice
 */
export function formatGithubAnnotations(findings: readonly Finding[]): string {
  const lines: string[] = [];
  for (const f of findings) {
    const level = ghAnnotationLevel(f.severity);
    const loc = `file=${f.location.file},line=${f.location.line},col=${f.location.column}`;
    const msg = f.suggestion
      ? `${f.ruleId}: ${f.message} — ${f.suggestion}`
      : `${f.ruleId}: ${f.message}`;
    lines.push(`::${level} ${loc}::${msg}`);
  }
  return lines.join("\n") + (lines.length > 0 ? "\n" : "");
}

function ghAnnotationLevel(severity: Severity): string {
  switch (severity) {
    case "error":
      return "error";
    case "warn":
      return "warning";
    case "info":
      return "notice";
  }
}

// ---------------------------------------------------------------------------
// GitLab Code Quality formatter
// ---------------------------------------------------------------------------

/**
 * Format findings as GitLab Code Quality JSON.
 *
 * Returns a JSON array where each element has:
 *   description, check_name, fingerprint, severity, location
 *
 * Severity mapping: error → critical, warn → major, info → minor
 * Fingerprint: SHA-1 of (ruleId, filePath, line)
 */
export function formatGitlabCodeQuality(
  findings: readonly Finding[],
): string {
  const entries: GitLabCodeQualityEntry[] = findings.map((f) => ({
    description: f.suggestion
      ? `${f.message} — ${f.suggestion}`
      : f.message,
    check_name: f.ruleId,
    fingerprint: computeFingerprint(f.ruleId, f.location.file, f.location.line),
    severity: glSeverity(f.severity),
    location: {
      path: f.location.file,
      lines: {
        begin: f.location.line,
      },
    },
  }));
  return JSON.stringify(entries, null, 2) + "\n";
}

function glSeverity(severity: Severity): "critical" | "major" | "minor" {
  switch (severity) {
    case "error":
      return "critical";
    case "warn":
      return "major";
    case "info":
      return "minor";
  }
}

/**
 * Compute a stable SHA-1 fingerprint for deduplication across CI runs.
 * Input: concatenation of ruleId, filePath, and line number.
 */
export function computeFingerprint(
  ruleId: string,
  filePath: string,
  line: number,
): string {
  const hasher = new Bun.CryptoHasher("sha1");
  hasher.update(`${ruleId}:${filePath}:${line}`);
  return hasher.digest("hex");
}

// ---------------------------------------------------------------------------
// Unified format dispatcher
// ---------------------------------------------------------------------------

/**
 * Format findings using the specified format.
 * Convenience wrapper that dispatches to the appropriate formatter.
 */
export function formatFindings(
  format: ReportFormat,
  findings: readonly Finding[],
  options?: {
    filePath?: string;
    metadata?: ReportMetadata;
    useColors?: boolean;
  },
): string {
  switch (format) {
    case "text":
      return formatText(
        findings,
        options?.filePath,
        options?.useColors ?? true,
      );
    case "json":
      return formatJson(
        findings,
        options?.metadata ?? {
          files_analyzed: 0,
          rules_checked: 0,
          duration_ms: 0,
        },
      );
    case "github-annotations":
      return formatGithubAnnotations(findings);
    case "gitlab-codequality":
      return formatGitlabCodeQuality(findings);
  }
}
