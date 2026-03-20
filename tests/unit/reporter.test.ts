import { describe, it, expect } from "bun:test";
import {
  formatText,
  formatJson,
  formatGithubAnnotations,
  formatGitlabCodeQuality,
  formatFindings,
  computeSummary,
  computeFingerprint,
  type Finding,
  type ReportMetadata,
} from "../../src/analysis/reporter";

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

const errorFinding: Finding = {
  ruleId: "SA004",
  severity: "error",
  message: "Adding a column with a volatile DEFAULT requires a full table rewrite",
  location: { file: "deploy/001-add-col.sql", line: 5, column: 1 },
  suggestion: "Add the column without DEFAULT, then backfill in batches",
};

const warnFinding: Finding = {
  ruleId: "SA007",
  severity: "warn",
  message: "CREATE INDEX without CONCURRENTLY will lock the table",
  location: { file: "deploy/002-add-index.sql", line: 12, column: 3 },
  suggestion: "Use CREATE INDEX CONCURRENTLY",
};

const infoFinding: Finding = {
  ruleId: "SA015",
  severity: "info",
  message: "Consider adding a comment to this table",
  location: { file: "deploy/003-create-table.sql", line: 1, column: 1 },
};

const mixedFindings: Finding[] = [errorFinding, warnFinding, infoFinding];

const defaultMetadata: ReportMetadata = {
  files_analyzed: 3,
  rules_checked: 21,
  duration_ms: 42,
};

// ---------------------------------------------------------------------------
// computeSummary
// ---------------------------------------------------------------------------

describe("computeSummary", () => {
  it("counts errors, warnings, and info findings", () => {
    const summary = computeSummary(mixedFindings);
    expect(summary).toEqual({ errors: 1, warnings: 1, info: 1 });
  });

  it("returns zeros for empty findings", () => {
    const summary = computeSummary([]);
    expect(summary).toEqual({ errors: 0, warnings: 0, info: 0 });
  });

  it("counts multiple errors correctly", () => {
    const summary = computeSummary([errorFinding, errorFinding, warnFinding]);
    expect(summary).toEqual({ errors: 2, warnings: 1, info: 0 });
  });
});

// ---------------------------------------------------------------------------
// formatText
// ---------------------------------------------------------------------------

describe("formatText", () => {
  it("includes file path header when provided", () => {
    const output = formatText([errorFinding], "deploy/001-add-col.sql", false);
    expect(output).toStartWith("deploy/001-add-col.sql\n");
  });

  it("omits file path header when not provided", () => {
    const output = formatText([errorFinding], undefined, false);
    expect(output).toStartWith("  error");
  });

  it("renders severity, ruleId, and message", () => {
    const output = formatText([errorFinding], undefined, false);
    expect(output).toContain("error SA004: Adding a column with a volatile DEFAULT");
  });

  it("renders location line", () => {
    const output = formatText([errorFinding], undefined, false);
    expect(output).toContain("at deploy/001-add-col.sql:5:1");
  });

  it("renders suggestion when present", () => {
    const output = formatText([errorFinding], undefined, false);
    expect(output).toContain("suggestion: Add the column without DEFAULT");
  });

  it("omits suggestion when absent", () => {
    const output = formatText([infoFinding], undefined, false);
    expect(output).not.toContain("suggestion:");
  });

  it("renders summary with counts", () => {
    const output = formatText(mixedFindings, undefined, false);
    expect(output).toContain("1 error");
    expect(output).toContain("1 warning");
    expect(output).toContain("1 info");
  });

  it("pluralizes summary counts", () => {
    const output = formatText(
      [errorFinding, errorFinding, warnFinding, warnFinding],
      undefined,
      false,
    );
    expect(output).toContain("2 errors");
    expect(output).toContain("2 warnings");
  });

  it("renders 'No issues found.' when empty", () => {
    const output = formatText([], undefined, false);
    expect(output).toContain("No issues found.");
  });

  it("includes ANSI codes when useColors is true", () => {
    const output = formatText([errorFinding], undefined, true);
    expect(output).toContain("\x1b[31m"); // red for error
    expect(output).toContain("\x1b[0m");  // reset
  });

  it("excludes ANSI codes when useColors is false", () => {
    const output = formatText([errorFinding], undefined, false);
    expect(output).not.toContain("\x1b[");
  });

  it("renders warn severity with correct label", () => {
    const output = formatText([warnFinding], undefined, false);
    expect(output).toContain("warn  SA007:");
  });

  it("renders info severity with correct label", () => {
    const output = formatText([infoFinding], undefined, false);
    expect(output).toContain("info  SA015:");
  });
});

// ---------------------------------------------------------------------------
// formatJson
// ---------------------------------------------------------------------------

describe("formatJson", () => {
  it("produces valid JSON", () => {
    const output = formatJson(mixedFindings, defaultMetadata);
    expect(() => JSON.parse(output)).not.toThrow();
  });

  it("includes version: 1", () => {
    const parsed = JSON.parse(formatJson(mixedFindings, defaultMetadata));
    expect(parsed.version).toBe(1);
  });

  it("includes metadata", () => {
    const parsed = JSON.parse(formatJson(mixedFindings, defaultMetadata));
    expect(parsed.metadata).toEqual({
      files_analyzed: 3,
      rules_checked: 21,
      duration_ms: 42,
    });
  });

  it("includes correct summary counts", () => {
    const parsed = JSON.parse(formatJson(mixedFindings, defaultMetadata));
    expect(parsed.summary).toEqual({ errors: 1, warnings: 1, info: 1 });
  });

  it("includes all findings with correct fields", () => {
    const parsed = JSON.parse(formatJson(mixedFindings, defaultMetadata));
    expect(parsed.findings).toHaveLength(3);
    expect(parsed.findings[0].ruleId).toBe("SA004");
    expect(parsed.findings[0].severity).toBe("error");
    expect(parsed.findings[0].message).toBe(
      "Adding a column with a volatile DEFAULT requires a full table rewrite",
    );
    expect(parsed.findings[0].location).toEqual({
      file: "deploy/001-add-col.sql",
      line: 5,
      column: 1,
    });
    expect(parsed.findings[0].suggestion).toBe(
      "Add the column without DEFAULT, then backfill in batches",
    );
  });

  it("omits suggestion field when not present", () => {
    const parsed = JSON.parse(formatJson([infoFinding], defaultMetadata));
    expect(parsed.findings[0]).not.toHaveProperty("suggestion");
  });

  it("handles empty findings", () => {
    const parsed = JSON.parse(formatJson([], defaultMetadata));
    expect(parsed.findings).toEqual([]);
    expect(parsed.summary).toEqual({ errors: 0, warnings: 0, info: 0 });
  });

  it("pretty-prints with 2-space indentation", () => {
    const output = formatJson([errorFinding], defaultMetadata);
    // The output should contain indented lines
    expect(output).toContain('  "version": 1');
  });
});

// ---------------------------------------------------------------------------
// formatGithubAnnotations
// ---------------------------------------------------------------------------

describe("formatGithubAnnotations", () => {
  it("formats error findings as ::error", () => {
    const output = formatGithubAnnotations([errorFinding]);
    expect(output).toContain(
      "::error file=deploy/001-add-col.sql,line=5,col=1::SA004:",
    );
  });

  it("formats warn findings as ::warning", () => {
    const output = formatGithubAnnotations([warnFinding]);
    expect(output).toContain(
      "::warning file=deploy/002-add-index.sql,line=12,col=3::SA007:",
    );
  });

  it("formats info findings as ::notice", () => {
    const output = formatGithubAnnotations([infoFinding]);
    expect(output).toContain(
      "::notice file=deploy/003-create-table.sql,line=1,col=1::SA015:",
    );
  });

  it("includes suggestion in message when present", () => {
    const output = formatGithubAnnotations([errorFinding]);
    expect(output).toContain(
      "Add the column without DEFAULT, then backfill in batches",
    );
  });

  it("does not include dash-dash separator when no suggestion", () => {
    const output = formatGithubAnnotations([infoFinding]);
    // Should not contain " — " since there's no suggestion
    expect(output).not.toContain(" — ");
  });

  it("outputs one line per finding", () => {
    const output = formatGithubAnnotations(mixedFindings);
    const lines = output.trim().split("\n");
    expect(lines).toHaveLength(3);
  });

  it("returns empty string for no findings", () => {
    const output = formatGithubAnnotations([]);
    expect(output).toBe("");
  });

  it("each line starts with :: annotation prefix", () => {
    const output = formatGithubAnnotations(mixedFindings);
    const lines = output.trim().split("\n");
    for (const line of lines) {
      expect(line).toMatch(/^::(error|warning|notice) /);
    }
  });
});

// ---------------------------------------------------------------------------
// formatGitlabCodeQuality
// ---------------------------------------------------------------------------

describe("formatGitlabCodeQuality", () => {
  it("produces valid JSON array", () => {
    const output = formatGitlabCodeQuality(mixedFindings);
    const parsed = JSON.parse(output);
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed).toHaveLength(3);
  });

  it("maps error severity to critical", () => {
    const parsed = JSON.parse(formatGitlabCodeQuality([errorFinding]));
    expect(parsed[0].severity).toBe("critical");
  });

  it("maps warn severity to major", () => {
    const parsed = JSON.parse(formatGitlabCodeQuality([warnFinding]));
    expect(parsed[0].severity).toBe("major");
  });

  it("maps info severity to minor", () => {
    const parsed = JSON.parse(formatGitlabCodeQuality([infoFinding]));
    expect(parsed[0].severity).toBe("minor");
  });

  it("sets check_name to ruleId", () => {
    const parsed = JSON.parse(formatGitlabCodeQuality([errorFinding]));
    expect(parsed[0].check_name).toBe("SA004");
  });

  it("sets location path and begin line", () => {
    const parsed = JSON.parse(formatGitlabCodeQuality([errorFinding]));
    expect(parsed[0].location).toEqual({
      path: "deploy/001-add-col.sql",
      lines: { begin: 5 },
    });
  });

  it("includes suggestion in description when present", () => {
    const parsed = JSON.parse(formatGitlabCodeQuality([errorFinding]));
    expect(parsed[0].description).toContain(errorFinding.message);
    expect(parsed[0].description).toContain(errorFinding.suggestion);
  });

  it("uses only message when no suggestion", () => {
    const parsed = JSON.parse(formatGitlabCodeQuality([infoFinding]));
    expect(parsed[0].description).toBe(infoFinding.message);
  });

  it("produces a hex SHA-1 fingerprint", () => {
    const parsed = JSON.parse(formatGitlabCodeQuality([errorFinding]));
    expect(parsed[0].fingerprint).toMatch(/^[a-f0-9]{40}$/);
  });

  it("returns empty JSON array for no findings", () => {
    const output = formatGitlabCodeQuality([]);
    expect(JSON.parse(output)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// computeFingerprint
// ---------------------------------------------------------------------------

describe("computeFingerprint", () => {
  it("returns a 40-character hex string", () => {
    const fp = computeFingerprint("SA004", "deploy/001.sql", 5);
    expect(fp).toMatch(/^[a-f0-9]{40}$/);
  });

  it("produces stable fingerprints for same inputs", () => {
    const fp1 = computeFingerprint("SA004", "deploy/001.sql", 5);
    const fp2 = computeFingerprint("SA004", "deploy/001.sql", 5);
    expect(fp1).toBe(fp2);
  });

  it("produces different fingerprints for different ruleIds", () => {
    const fp1 = computeFingerprint("SA004", "deploy/001.sql", 5);
    const fp2 = computeFingerprint("SA007", "deploy/001.sql", 5);
    expect(fp1).not.toBe(fp2);
  });

  it("produces different fingerprints for different files", () => {
    const fp1 = computeFingerprint("SA004", "deploy/001.sql", 5);
    const fp2 = computeFingerprint("SA004", "deploy/002.sql", 5);
    expect(fp1).not.toBe(fp2);
  });

  it("produces different fingerprints for different lines", () => {
    const fp1 = computeFingerprint("SA004", "deploy/001.sql", 5);
    const fp2 = computeFingerprint("SA004", "deploy/001.sql", 10);
    expect(fp1).not.toBe(fp2);
  });
});

// ---------------------------------------------------------------------------
// formatFindings dispatcher
// ---------------------------------------------------------------------------

describe("formatFindings", () => {
  it("dispatches to text formatter", () => {
    const output = formatFindings("text", [errorFinding], { useColors: false });
    expect(output).toContain("error SA004:");
    expect(output).toContain("at deploy/001-add-col.sql:5:1");
  });

  it("dispatches to json formatter", () => {
    const output = formatFindings("json", [errorFinding], {
      metadata: defaultMetadata,
    });
    const parsed = JSON.parse(output);
    expect(parsed.version).toBe(1);
    expect(parsed.findings).toHaveLength(1);
  });

  it("dispatches to github-annotations formatter", () => {
    const output = formatFindings("github-annotations", [errorFinding]);
    expect(output).toContain("::error file=");
  });

  it("dispatches to gitlab-codequality formatter", () => {
    const output = formatFindings("gitlab-codequality", [errorFinding]);
    const parsed = JSON.parse(output);
    expect(parsed[0].check_name).toBe("SA004");
  });

  it("provides default metadata for json when not specified", () => {
    const output = formatFindings("json", [errorFinding]);
    const parsed = JSON.parse(output);
    expect(parsed.metadata).toEqual({
      files_analyzed: 0,
      rules_checked: 0,
      duration_ms: 0,
    });
  });

  it("passes filePath option to text formatter", () => {
    const output = formatFindings("text", [errorFinding], {
      filePath: "my-file.sql",
      useColors: false,
    });
    expect(output).toStartWith("my-file.sql\n");
  });
});
