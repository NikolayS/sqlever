/**
 * Tests for src/ai/review.ts and src/commands/review.ts
 *
 * Covers: risk assessment, finding conversion, markdown formatting, text
 * formatting, JSON formatting, review engine with mock LLM, review
 * engine without LLM, argument parsing, CLI wiring, and edge cases.
 *
 * >=10 tests as required by issue #107.
 */

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { loadModule } from "libpg-query";
import { join } from "node:path";
import {
  mkdirSync,
  writeFileSync,
  rmSync,
  existsSync,
} from "node:fs";
import {
  assessRisk,
  toFindingEntries,
  runReview,
  formatReviewMarkdown,
  formatReviewText,
  formatReviewJson,
  formatReview,
  shortenPath,
  type FindingEntry,
  type LLMProvider,
  type LLMExplanation,
  type ReviewResult,
} from "../../src/ai/review";
import {
  parseReviewArgs,
  runReviewCommand,
} from "../../src/commands/review";
import type { Finding } from "../../src/analysis/types";

// Ensure WASM module is loaded before tests
beforeAll(async () => {
  await loadModule();
});

const TMP_DIR = join(import.meta.dir, "..", ".tmp-review-tests");

// Create temp directory structure for tests
beforeAll(() => {
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
  mkdirSync(TMP_DIR, { recursive: true });
  mkdirSync(join(TMP_DIR, "deploy"), { recursive: true });

  // Clean SQL file
  writeFileSync(
    join(TMP_DIR, "deploy", "clean.sql"),
    "CREATE TABLE t (id serial PRIMARY KEY);\n",
  );

  // SQL file that triggers SA004 (CREATE INDEX without CONCURRENTLY)
  writeFileSync(
    join(TMP_DIR, "deploy", "index_issue.sql"),
    "CREATE INDEX idx_t_id ON t (id);\n",
  );

  // SQL file that triggers SA010 (UPDATE without WHERE)
  writeFileSync(
    join(TMP_DIR, "deploy", "no_where.sql"),
    "UPDATE t SET x = 1;\n",
  );

  // SQL file with SA001 error (ADD COLUMN NOT NULL without DEFAULT)
  writeFileSync(
    join(TMP_DIR, "deploy", "error_migration.sql"),
    "ALTER TABLE users ADD COLUMN active boolean NOT NULL;\n",
  );

  // A sqitch.plan file
  writeFileSync(
    join(TMP_DIR, "sqitch.plan"),
    `%project=test
%uri=https://example.com

clean 2024-01-15T10:30:00Z dev <dev@example.com> # clean migration
index_issue 2024-01-15T10:31:00Z dev <dev@example.com> # index issue
no_where 2024-01-15T10:32:00Z dev <dev@example.com> # no where
error_migration 2024-01-15T10:33:00Z dev <dev@example.com> # error migration
`,
  );
});

afterAll(() => {
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
});

// Suppress stdout during test runs
function silenceStdout(): () => void {
  const original = process.stdout.write.bind(process.stdout);
  process.stdout.write = (() => true) as typeof process.stdout.write;
  return () => {
    process.stdout.write = original;
  };
}

// Capture stdout during test runs
function captureStdout(): { getOutput: () => string; restore: () => void } {
  let output = "";
  const original = process.stdout.write.bind(process.stdout);
  process.stdout.write = ((chunk: unknown) => {
    output += typeof chunk === "string" ? chunk : String(chunk);
    return true;
  }) as typeof process.stdout.write;
  return {
    getOutput: () => output,
    restore: () => {
      process.stdout.write = original;
    },
  };
}

// ---------------------------------------------------------------------------
// Mock data
// ---------------------------------------------------------------------------

const mockFindings: Finding[] = [
  {
    ruleId: "SA004",
    severity: "warn",
    message: 'CREATE INDEX on table "t" without CONCURRENTLY may lock the table.',
    location: { file: "/path/to/deploy/index_issue.sql", line: 1, column: 1 },
    suggestion: "Use CREATE INDEX CONCURRENTLY to avoid table locks.",
  },
  {
    ruleId: "SA001",
    severity: "error",
    message: 'Adding NOT NULL column "active" to table "users" without a DEFAULT will fail.',
    location: { file: "/path/to/deploy/error_migration.sql", line: 1, column: 1 },
    suggestion: "Add a DEFAULT value, or add the column as nullable first.",
  },
  {
    ruleId: "SA010",
    severity: "warn",
    message: "UPDATE without WHERE clause affects all rows.",
    location: { file: "/path/to/deploy/no_where.sql", line: 1, column: 1 },
  },
];

const mockFindingEntries: FindingEntry[] = [
  {
    ruleId: "SA004",
    severity: "warn",
    message: 'CREATE INDEX on table "t" without CONCURRENTLY may lock the table.',
    file: "/path/to/deploy/index_issue.sql",
    line: 1,
    suggestion: "Use CREATE INDEX CONCURRENTLY to avoid table locks.",
  },
  {
    ruleId: "SA001",
    severity: "error",
    message: 'Adding NOT NULL column "active" to table "users" without a DEFAULT will fail.',
    file: "/path/to/deploy/error_migration.sql",
    line: 1,
    suggestion: "Add a DEFAULT value, or add the column as nullable first.",
  },
  {
    ruleId: "SA010",
    severity: "warn",
    message: "UPDATE without WHERE clause affects all rows.",
    file: "/path/to/deploy/no_where.sql",
    line: 1,
  },
];

/** Mock LLM provider for testing. */
class MockLLMProvider implements LLMProvider {
  async explain(
    _sql: string,
    _findings: FindingEntry[],
  ): Promise<LLMExplanation> {
    return {
      summary:
        "This migration adds an index on the t table, updates all rows in t, and adds a NOT NULL column to users.",
      suggestedImprovements: [
        "Use CREATE INDEX CONCURRENTLY to avoid locking the table during index creation.",
        "Add a WHERE clause to the UPDATE statement to limit the scope of changes.",
        "Add a DEFAULT value when adding a NOT NULL column to avoid failures on populated tables.",
      ],
    };
  }
}

/** Mock LLM provider that throws an error. */
class FailingLLMProvider implements LLMProvider {
  async explain(): Promise<LLMExplanation> {
    throw new Error("LLM service unavailable");
  }
}

// ---------------------------------------------------------------------------
// Test 1: assessRisk
// ---------------------------------------------------------------------------

describe("assessRisk", () => {
  test("returns 'high' when there are error-level findings", () => {
    const result = assessRisk(mockFindingEntries);
    expect(result).toBe("high");
  });

  test("returns 'medium' when there are only warnings", () => {
    const warnOnly = mockFindingEntries.filter((f) => f.severity === "warn");
    const result = assessRisk(warnOnly);
    expect(result).toBe("medium");
  });

  test("returns 'low' when there are no findings", () => {
    const result = assessRisk([]);
    expect(result).toBe("low");
  });

  test("returns 'low' when there are only info findings", () => {
    const infoOnly: FindingEntry[] = [
      {
        ruleId: "SA999",
        severity: "info",
        message: "Informational message",
        file: "test.sql",
        line: 1,
      },
    ];
    const result = assessRisk(infoOnly);
    expect(result).toBe("low");
  });
});

// ---------------------------------------------------------------------------
// Test 2: toFindingEntries
// ---------------------------------------------------------------------------

describe("toFindingEntries", () => {
  test("converts raw Finding[] to FindingEntry[]", () => {
    const entries = toFindingEntries(mockFindings);
    expect(entries.length).toBe(3);
    expect(entries[0]!.ruleId).toBe("SA004");
    expect(entries[0]!.file).toBe("/path/to/deploy/index_issue.sql");
    expect(entries[0]!.line).toBe(1);
    expect(entries[0]!.suggestion).toBe(
      "Use CREATE INDEX CONCURRENTLY to avoid table locks.",
    );
    expect(entries[2]!.suggestion).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Test 3: formatReviewMarkdown
// ---------------------------------------------------------------------------

describe("formatReviewMarkdown", () => {
  test("produces markdown with risk summary, findings table, and suggestions", () => {
    const result: ReviewResult = {
      risk: "high",
      findings: mockFindingEntries,
      filesReviewed: ["file1.sql", "file2.sql", "file3.sql"],
      errorCount: 1,
      warnCount: 2,
      infoCount: 0,
    };

    const md = formatReviewMarkdown(result);

    // Header
    expect(md).toContain("## sqlever review");

    // Risk summary
    expect(md).toContain("HIGH");

    // Counts
    expect(md).toContain("1 error");
    expect(md).toContain("2 warnings");
    expect(md).toContain("3 files");

    // Findings table
    expect(md).toContain("| Severity | Rule | Message | File | Line |");
    expect(md).toContain("SA004");
    expect(md).toContain("SA001");
    expect(md).toContain("SA010");

    // Suggestions
    expect(md).toContain("### Suggested improvements");
    expect(md).toContain("Use CREATE INDEX CONCURRENTLY");

    // Footer
    expect(md).toContain("Generated by sqlever review");
  });

  test("includes LLM explanation when present", () => {
    const result: ReviewResult = {
      risk: "medium",
      findings: [mockFindingEntries[0]!],
      explanation: {
        summary: "This migration creates an index on the t table.",
        suggestedImprovements: ["Use CONCURRENTLY."],
      },
      filesReviewed: ["file1.sql"],
      errorCount: 0,
      warnCount: 1,
      infoCount: 0,
    };

    const md = formatReviewMarkdown(result);
    expect(md).toContain("### What this migration does");
    expect(md).toContain("This migration creates an index on the t table.");
    expect(md).toContain("### AI-suggested improvements");
    expect(md).toContain("Use CONCURRENTLY.");
  });

  test("shows 'No issues found' when there are no findings", () => {
    const result: ReviewResult = {
      risk: "low",
      findings: [],
      filesReviewed: ["file1.sql"],
      errorCount: 0,
      warnCount: 0,
      infoCount: 0,
    };

    const md = formatReviewMarkdown(result);
    expect(md).toContain("No issues found");
    expect(md).toContain("LOW");
  });
});

// ---------------------------------------------------------------------------
// Test 4: formatReviewText
// ---------------------------------------------------------------------------

describe("formatReviewText", () => {
  test("produces plain text with risk and findings", () => {
    const result: ReviewResult = {
      risk: "high",
      findings: mockFindingEntries,
      filesReviewed: ["file1.sql"],
      errorCount: 1,
      warnCount: 2,
      infoCount: 0,
    };

    const text = formatReviewText(result);
    expect(text).toContain("Risk: HIGH");
    expect(text).toContain("1 error(s)");
    expect(text).toContain("2 warning(s)");
    expect(text).toContain("SA004");
    expect(text).toContain("SA001");
  });
});

// ---------------------------------------------------------------------------
// Test 5: formatReviewJson
// ---------------------------------------------------------------------------

describe("formatReviewJson", () => {
  test("produces valid JSON with all review fields", () => {
    const result: ReviewResult = {
      risk: "medium",
      findings: [mockFindingEntries[0]!],
      filesReviewed: ["file1.sql"],
      errorCount: 0,
      warnCount: 1,
      infoCount: 0,
    };

    const jsonStr = formatReviewJson(result);
    const parsed = JSON.parse(jsonStr);
    expect(parsed.risk).toBe("medium");
    expect(parsed.findings.length).toBe(1);
    expect(parsed.findings[0].ruleId).toBe("SA004");
    expect(parsed.errorCount).toBe(0);
    expect(parsed.warnCount).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Test 6: runReview with mock LLM
// ---------------------------------------------------------------------------

describe("runReview with mock LLM", () => {
  test("integrates static findings with LLM explanation", async () => {
    const sqlContents = new Map<string, string>();
    sqlContents.set(
      "/path/to/deploy/index_issue.sql",
      "CREATE INDEX idx_t_id ON t (id);\n",
    );

    const result = await runReview(mockFindings, ["file1.sql", "file2.sql"], {
      format: "markdown",
      llm: new MockLLMProvider(),
      sqlContents,
    });

    expect(result.risk).toBe("high");
    expect(result.findings.length).toBe(3);
    expect(result.explanation).toBeDefined();
    expect(result.explanation!.summary).toContain("migration");
    expect(result.explanation!.suggestedImprovements.length).toBe(3);
    expect(result.errorCount).toBe(1);
    expect(result.warnCount).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Test 7: runReview without LLM (graceful degradation)
// ---------------------------------------------------------------------------

describe("runReview without LLM", () => {
  test("produces review without explanation when no LLM provided", async () => {
    const result = await runReview(mockFindings, ["file1.sql"], {
      format: "markdown",
    });

    expect(result.risk).toBe("high");
    expect(result.findings.length).toBe(3);
    expect(result.explanation).toBeUndefined();
  });

  test("handles LLM failure gracefully", async () => {
    const sqlContents = new Map<string, string>();
    sqlContents.set("file.sql", "CREATE TABLE t (id int);");

    const result = await runReview(mockFindings, ["file1.sql"], {
      format: "markdown",
      llm: new FailingLLMProvider(),
      sqlContents,
    });

    expect(result.risk).toBe("high");
    expect(result.findings.length).toBe(3);
    // LLM failure is non-fatal — explanation should be undefined
    expect(result.explanation).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Test 8: parseReviewArgs
// ---------------------------------------------------------------------------

describe("parseReviewArgs", () => {
  test("parses empty args with default format markdown", () => {
    const opts = parseReviewArgs([]);
    expect(opts.targets).toEqual([]);
    expect(opts.format).toBe("markdown");
    expect(opts.all).toBe(false);
    expect(opts.changed).toBe(false);
  });

  test("parses --format text", () => {
    const opts = parseReviewArgs(["--format", "text"]);
    expect(opts.format).toBe("text");
  });

  test("parses --format json", () => {
    const opts = parseReviewArgs(["--format", "json"]);
    expect(opts.format).toBe("json");
  });

  test("parses --format markdown", () => {
    const opts = parseReviewArgs(["--format", "markdown"]);
    expect(opts.format).toBe("markdown");
  });

  test("throws on invalid --format value", () => {
    expect(() => parseReviewArgs(["--format", "xml"])).toThrow(
      "Invalid --format",
    );
  });

  test("parses positional targets", () => {
    const opts = parseReviewArgs(["file1.sql", "dir/"]);
    expect(opts.targets).toEqual(["file1.sql", "dir/"]);
  });

  test("parses --all flag", () => {
    const opts = parseReviewArgs(["--all"]);
    expect(opts.all).toBe(true);
  });

  test("parses --changed flag", () => {
    const opts = parseReviewArgs(["--changed"]);
    expect(opts.changed).toBe(true);
  });

  test("parses --force-rule", () => {
    const opts = parseReviewArgs(["--force-rule", "SA004"]);
    expect(opts.forceRules).toEqual(["SA004"]);
  });

  test("parses combined flags and targets", () => {
    const opts = parseReviewArgs([
      "file.sql",
      "--format",
      "json",
      "--force-rule",
      "SA001",
      "--all",
    ]);
    expect(opts.targets).toEqual(["file.sql"]);
    expect(opts.format).toBe("json");
    expect(opts.forceRules).toEqual(["SA001"]);
    expect(opts.all).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Test 9: runReviewCommand with real analysis
// ---------------------------------------------------------------------------

describe("runReviewCommand", () => {
  test("reviews a clean SQL file with exit code 0 and low risk", async () => {
    const restore = silenceStdout();
    try {
      const result = await runReviewCommand({
        targets: [join(TMP_DIR, "deploy", "clean.sql")],
        format: "markdown",
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.exitCode).toBe(0);
      expect(result.review.risk).toBe("low");
    } finally {
      restore();
    }
  });

  test("reviews a file with warnings and assigns medium risk", async () => {
    const restore = silenceStdout();
    try {
      const result = await runReviewCommand({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "markdown",
        all: false,
        changed: false,
        forceRules: [],
      });
      // SA010 is a warning — risk should be medium, exit code 0
      expect(result.exitCode).toBe(0);
      expect(result.review.risk).toBe("medium");
      expect(result.review.warnCount).toBeGreaterThan(0);
    } finally {
      restore();
    }
  });

  test("reviews a file with errors and assigns high risk", async () => {
    const restore = silenceStdout();
    try {
      const result = await runReviewCommand({
        targets: [join(TMP_DIR, "deploy", "error_migration.sql")],
        format: "markdown",
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.exitCode).toBe(2);
      expect(result.review.risk).toBe("high");
      expect(result.review.errorCount).toBeGreaterThan(0);
    } finally {
      restore();
    }
  });

  test("outputs valid JSON when --format json", async () => {
    const cap = captureStdout();
    try {
      await runReviewCommand({
        targets: [join(TMP_DIR, "deploy", "clean.sql")],
        format: "json",
        all: false,
        changed: false,
        forceRules: [],
      });
      const parsed = JSON.parse(cap.getOutput());
      expect(parsed.risk).toBe("low");
      expect(Array.isArray(parsed.findings)).toBe(true);
      expect(parsed.filesReviewed).toBeDefined();
    } finally {
      cap.restore();
    }
  });

  test("outputs markdown with table headers when --format markdown", async () => {
    const cap = captureStdout();
    try {
      await runReviewCommand({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "markdown",
        all: false,
        changed: false,
        forceRules: [],
      });
      const output = cap.getOutput();
      expect(output).toContain("## sqlever review");
      expect(output).toContain("| Severity | Rule | Message | File | Line |");
    } finally {
      cap.restore();
    }
  });

  test("respects --force-rule to skip specific rules", async () => {
    const restore = silenceStdout();
    try {
      const result = await runReviewCommand({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "text",
        all: false,
        changed: false,
        forceRules: ["SA010"],
      });
      const sa010 = result.review.findings.filter((f) => f.ruleId === "SA010");
      expect(sa010.length).toBe(0);
    } finally {
      restore();
    }
  });

  test("reviews from sqitch.plan when no targets given", async () => {
    const restore = silenceStdout();
    try {
      const result = await runReviewCommand({
        targets: [],
        format: "markdown",
        all: false,
        changed: false,
        forceRules: [],
        topDir: TMP_DIR,
        planFile: join(TMP_DIR, "sqitch.plan"),
      });
      // Plan has 4 changes
      expect(result.review.filesReviewed.length).toBe(4);
    } finally {
      restore();
    }
  });
});

// ---------------------------------------------------------------------------
// Test 10: formatReview dispatcher
// ---------------------------------------------------------------------------

describe("formatReview dispatcher", () => {
  const result: ReviewResult = {
    risk: "low",
    findings: [],
    filesReviewed: ["file1.sql"],
    errorCount: 0,
    warnCount: 0,
    infoCount: 0,
  };

  test("dispatches to markdown formatter", () => {
    const output = formatReview(result, "markdown");
    expect(output).toContain("## sqlever review");
  });

  test("dispatches to text formatter", () => {
    const output = formatReview(result, "text");
    expect(output).toContain("Risk: LOW");
  });

  test("dispatches to json formatter", () => {
    const output = formatReview(result, "json");
    const parsed = JSON.parse(output);
    expect(parsed.risk).toBe("low");
  });
});

// ---------------------------------------------------------------------------
// Test 11: shortenPath helper
// ---------------------------------------------------------------------------

describe("shortenPath", () => {
  test("shortens long paths to last 2 segments", () => {
    expect(shortenPath("/home/user/project/deploy/migration.sql")).toBe(
      "deploy/migration.sql",
    );
  });

  test("preserves short paths", () => {
    expect(shortenPath("file.sql")).toBe("file.sql");
    expect(shortenPath("deploy/file.sql")).toBe("deploy/file.sql");
  });
});

// ---------------------------------------------------------------------------
// Test 12: CLI wiring
// ---------------------------------------------------------------------------

describe("CLI wiring", () => {
  const CLI_PATH = join(import.meta.dir, "..", "..", "src", "cli.ts");

  test("sqlever review file.sql runs successfully", async () => {
    const cleanFile = join(TMP_DIR, "deploy", "clean.sql");
    const proc = Bun.spawn(
      ["bun", "run", CLI_PATH, "review", cleanFile],
      { stdout: "pipe", stderr: "pipe" },
    );
    const exitCode = await proc.exited;
    expect(exitCode).toBe(0);
  });

  test("sqlever review --format markdown outputs markdown", async () => {
    const cleanFile = join(TMP_DIR, "deploy", "clean.sql");
    const proc = Bun.spawn(
      ["bun", "run", CLI_PATH, "review", "--format", "markdown", cleanFile],
      { stdout: "pipe", stderr: "pipe" },
    );
    const stdout = await new Response(proc.stdout).text();
    await proc.exited;
    expect(stdout).toContain("## sqlever review");
  });

  test("sqlever review --format json outputs valid JSON", async () => {
    const cleanFile = join(TMP_DIR, "deploy", "clean.sql");
    const proc = Bun.spawn(
      ["bun", "run", CLI_PATH, "review", "--format", "json", cleanFile],
      { stdout: "pipe", stderr: "pipe" },
    );
    const stdout = await new Response(proc.stdout).text();
    await proc.exited;
    const parsed = JSON.parse(stdout);
    expect(parsed.risk).toBe("low");
  });

  test("sqlever review exits 2 when errors found", async () => {
    const errorFile = join(TMP_DIR, "deploy", "error_migration.sql");
    const proc = Bun.spawn(
      ["bun", "run", CLI_PATH, "review", errorFile],
      { stdout: "pipe", stderr: "pipe" },
    );
    const exitCode = await proc.exited;
    expect(exitCode).toBe(2);
  });
});
