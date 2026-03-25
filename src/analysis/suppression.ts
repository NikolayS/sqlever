// src/analysis/suppression.ts — Parse and apply inline suppression comments
//
// Supports two forms per SPEC section 5.1:
//
// Block form:
//   -- sqlever:disable SA010
//   UPDATE users SET tier = 'free';
//   -- sqlever:enable SA010
//
// Single-line form (trailing comment on the same line as a statement):
//   UPDATE users SET tier = 'free'; -- sqlever:disable SA010
//
// Rules:
// - Comma-separated rule IDs or names: -- sqlever:disable SA010,dml-without-where
// - Human-readable kebab-case names are resolved to rule IDs
// - "all" keyword is NOT supported (too dangerous)
// - Unknown rule IDs/names produce warnings
// - Unclosed blocks extend to EOF and produce a warning
// - Unused suppressions produce warnings

import type { Finding } from "./types";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A parsed suppression directive. */
export interface SuppressionDirective {
  /** The action: disable or enable. */
  action: "disable" | "enable";
  /** Rule IDs being suppressed/re-enabled. */
  ruleIds: string[];
  /** 1-indexed line number where the directive appears. */
  line: number;
}

/** A resolved suppression range: lines [startLine, endLine] are suppressed for the given rule. */
export interface SuppressionRange {
  ruleId: string;
  startLine: number;
  /** End line (inclusive). Infinity if unclosed block. */
  endLine: number;
  /** The directive that opened this range. */
  directive: SuppressionDirective;
  /** Whether this suppression was used (matched at least one finding). */
  used: boolean;
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/**
 * Regex for suppression comments.
 * Matches: -- sqlever:disable SA001,SA002
 *          -- sqlever:disable add-column-not-null
 *          -- sqlever:enable SA001
 * Captures: [1] = "disable" | "enable", [2] = comma-separated rule IDs or names
 */
const SUPPRESSION_RE =
  /--\s*sqlever:(disable|enable)\s+([\w\-,\s]+)/;

/**
 * Parse all suppression directives from the raw SQL text.
 */
export function parseSuppressions(sql: string): SuppressionDirective[] {
  const lines = sql.split("\n");
  const directives: SuppressionDirective[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!;
    const match = SUPPRESSION_RE.exec(line);
    if (match) {
      const action = match[1] as "disable" | "enable";
      const rawIds = match[2]!;
      const ruleIds = rawIds
        .split(",")
        .map((id) => id.trim())
        .filter((id) => id.length > 0);

      directives.push({
        action,
        ruleIds,
        line: i + 1, // 1-indexed
      });
    }
  }

  return directives;
}

/**
 * Determine whether a suppression directive is a single-line (trailing) form.
 *
 * A single-line suppression is a `-- sqlever:disable` that appears on the same
 * line as SQL content (not on a line by itself). It suppresses findings on
 * that line only.
 */
function isSingleLineDirective(
  directive: SuppressionDirective,
  sqlLines: string[],
): boolean {
  if (directive.action !== "disable") return false;
  const line = sqlLines[directive.line - 1];
  if (!line) return false;

  // Check if there's SQL content before the suppression comment
  const commentStart = line.indexOf("--");
  if (commentStart <= 0) return false;

  const beforeComment = line.substring(0, commentStart).trim();
  return beforeComment.length > 0;
}

// ---------------------------------------------------------------------------
// Resolve ranges
// ---------------------------------------------------------------------------

/**
 * Resolve suppression directives into line ranges.
 *
 * Accepts both numeric rule IDs (SA001) and human-readable names
 * (add-column-not-null). Names are resolved to IDs via the nameToId map.
 *
 * Returns:
 * - An array of SuppressionRange objects
 * - An array of warning findings for issues like unknown rules, unclosed blocks
 */
export function resolveSuppressionRanges(
  directives: SuppressionDirective[],
  sqlLines: string[],
  totalLines: number,
  knownRuleIds: Set<string>,
  filePath: string,
  nameToId?: Map<string, string>,
): { ranges: SuppressionRange[]; warnings: Finding[] } {
  const ranges: SuppressionRange[] = [];
  const warnings: Finding[] = [];
  const resolvedNameToId = nameToId ?? new Map<string, string>();

  // Track open blocks per rule ID: ruleId -> opening directive
  const openBlocks = new Map<string, SuppressionDirective>();

  for (const directive of directives) {
    // Resolve names to IDs in the directive's ruleIds
    const resolvedIds = directive.ruleIds.map((idOrName) => {
      const resolved = resolvedNameToId.get(idOrName);
      return resolved ?? idOrName;
    });

    // Warn about unknown rule IDs/names
    for (let i = 0; i < directive.ruleIds.length; i++) {
      const resolved = resolvedIds[i]!;
      const original = directive.ruleIds[i]!;
      if (!knownRuleIds.has(resolved)) {
        warnings.push({
          ruleId: "suppression",
          severity: "warn",
          message: `Unknown rule ID "${original}" in suppression comment.`,
          location: { file: filePath, line: directive.line, column: 1 },
        });
      }
    }

    // Warn about "all" keyword
    if (resolvedIds.includes("all")) {
      warnings.push({
        ruleId: "suppression",
        severity: "warn",
        message: `"all" keyword is not supported in suppression comments. Suppress rules individually.`,
        location: { file: filePath, line: directive.line, column: 1 },
      });
      continue;
    }

    if (isSingleLineDirective(directive, sqlLines)) {
      // Single-line form: suppresses findings on THIS line only
      for (const ruleId of resolvedIds) {
        ranges.push({
          ruleId,
          startLine: directive.line,
          endLine: directive.line,
          directive,
          used: false,
        });
      }
    } else if (directive.action === "disable") {
      // Block form: open a suppression block
      for (const ruleId of resolvedIds) {
        openBlocks.set(ruleId, directive);
      }
    } else {
      // enable: close any matching open blocks
      for (const ruleId of resolvedIds) {
        const openDirective = openBlocks.get(ruleId);
        if (openDirective) {
          ranges.push({
            ruleId,
            startLine: openDirective.line,
            endLine: directive.line,
            directive: openDirective,
            used: false,
          });
          openBlocks.delete(ruleId);
        }
        // If no matching open block, the enable is a no-op (no warning needed)
      }
    }
  }

  // Close any unclosed blocks (extend to EOF)
  for (const [ruleId, directive] of openBlocks) {
    ranges.push({
      ruleId,
      startLine: directive.line,
      endLine: totalLines,
      directive,
      used: false,
    });
    warnings.push({
      ruleId: "suppression",
      severity: "warn",
      message: `Unclosed suppression block for "${ruleId}" — extends to end of file.`,
      location: { file: filePath, line: directive.line, column: 1 },
    });
  }

  return { ranges, warnings };
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

/**
 * Filter findings through suppression ranges and per-file skip lists.
 *
 * Returns:
 * - filtered: findings that are NOT suppressed
 * - suppressed: findings that WERE suppressed
 * - warnings: unused suppression warnings + any directive warnings
 */
export function filterFindings(
  findings: Finding[],
  ranges: SuppressionRange[],
  directiveWarnings: Finding[],
  fileSkipRules?: string[],
): { filtered: Finding[]; suppressed: Finding[]; warnings: Finding[] } {
  const filtered: Finding[] = [];
  const suppressed: Finding[] = [];
  const skipSet = new Set(fileSkipRules ?? []);

  for (const finding of findings) {
    // Check per-file skip list
    if (skipSet.has(finding.ruleId)) {
      suppressed.push(finding);
      continue;
    }

    // Check inline suppression ranges
    let isSuppressed = false;
    for (const range of ranges) {
      if (
        range.ruleId === finding.ruleId &&
        finding.location.line >= range.startLine &&
        finding.location.line <= range.endLine
      ) {
        isSuppressed = true;
        range.used = true;
        break;
      }
    }

    if (isSuppressed) {
      suppressed.push(finding);
    } else {
      filtered.push(finding);
    }
  }

  // Collect warnings
  const warnings: Finding[] = [...directiveWarnings];

  // Unused suppression warnings
  for (const range of ranges) {
    if (!range.used) {
      warnings.push({
        ruleId: "suppression",
        severity: "warn",
        message: `Unused suppression for "${range.ruleId}".`,
        location: {
          file: findings[0]?.location.file ?? "",
          line: range.directive.line,
          column: 1,
        },
      });
    }
  }

  return { filtered, suppressed, warnings };
}
