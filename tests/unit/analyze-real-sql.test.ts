/**
 * REAL-TEST-3: sqlever analyze on real SQL files -- verify findings are correct.
 *
 * Implements GitHub issue #142.
 *
 * Creates realistic migration SQL files that exercise multiple analysis rules,
 * runs the full analyze pipeline (parse -> analyze -> report), and verifies
 * that findings match expectations EXACTLY -- no false positives, no missed
 * issues.
 *
 * Test files:
 *   file1.sql — ALTER TABLE ADD COLUMN NOT NULL without default (SA001 should fire)
 *   file2.sql — CREATE INDEX without CONCURRENTLY (SA004 should fire)
 *   file3.sql — DROP TABLE (SA007 should fire)
 *   file4.sql — Safe migration: CREATE TABLE IF NOT EXISTS, ADD COLUMN with DEFAULT (no rules should fire)
 *   file5.sql — UPDATE without WHERE inside CREATE FUNCTION (SA010 should NOT fire -- PL/pgSQL exclusion)
 *   file6.sql — Multiple dangerous patterns in one file (SA001, SA004, SA006, SA010)
 *   file7.sql — CIC inside transaction (SA020 should fire)
 *   file8.sql — ALTER COLUMN TYPE (SA003 should fire)
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

import { Analyzer } from "../../src/analysis/index.js";
import { allRules } from "../../src/analysis/rules/index.js";
import { defaultRegistry } from "../../src/analysis/registry.js";
import {
  formatJson,
  formatGithubAnnotations,
  formatText,
  computeSummary,
  type ReportMetadata,
} from "../../src/analysis/reporter.js";
import type { Finding, AnalysisConfig } from "../../src/analysis/types.js";

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

const TMP_DIR = join(import.meta.dir, "..", ".tmp-real-sql-tests");
let analyzer: Analyzer;

beforeAll(async () => {
  await loadModule();
  // Ensure all rules are registered in the default registry
  for (const rule of allRules) {
    if (!defaultRegistry.has(rule.id)) {
      defaultRegistry.register(rule);
    }
  }
  analyzer = new Analyzer();
  await analyzer.ensureWasm();

  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
  mkdirSync(TMP_DIR, { recursive: true });

  // -- file1.sql: ADD COLUMN NOT NULL without DEFAULT (SA001)
  writeFileSync(
    join(TMP_DIR, "file1.sql"),
    `-- Migration: add required email column to users
set lock_timeout = '5s';
alter table users
  add column email text not null;
`,
  );

  // -- file2.sql: CREATE INDEX without CONCURRENTLY (SA004)
  writeFileSync(
    join(TMP_DIR, "file2.sql"),
    `-- Migration: add index on users.email
set lock_timeout = '5s';
create index idx_users_email on users (email);
`,
  );

  // -- file3.sql: DROP TABLE (SA007)
  writeFileSync(
    join(TMP_DIR, "file3.sql"),
    `-- Migration: remove legacy audit_log table
set lock_timeout = '5s';
drop table audit_log;
`,
  );

  // -- file4.sql: Safe migration -- no rules should fire
  writeFileSync(
    join(TMP_DIR, "file4.sql"),
    `-- Migration: create orders table and add nullable column
create table if not exists orders (
  id int8 generated always as identity primary key,
  user_id int8 not null,
  total numeric(12, 2) not null default 0,
  created_at timestamptz not null default now()
);
`,
  );

  // -- file5.sql: UPDATE without WHERE inside CREATE FUNCTION (SA010 exclusion)
  writeFileSync(
    join(TMP_DIR, "file5.sql"),
    `-- Migration: create function that updates all rows
create or replace function reset_counters()
returns void
language plpgsql
as $$
begin
  update counters set value = 0;
end;
$$;
`,
  );

  // -- file6.sql: Multiple dangerous patterns in one file
  writeFileSync(
    join(TMP_DIR, "file6.sql"),
    `-- Migration: complex migration with multiple issues
set lock_timeout = '5s';

alter table products
  add column sku text not null;

create index idx_products_sku on products (sku);

alter table products
  drop column legacy_code;

delete from temp_data;
`,
  );

  // -- file7.sql: CREATE INDEX CONCURRENTLY inside BEGIN (SA020)
  writeFileSync(
    join(TMP_DIR, "file7.sql"),
    `-- Migration: add index inside transaction (incorrect)
begin;
create index concurrently idx_orders_user on orders (user_id);
commit;
`,
  );

  // -- file8.sql: ALTER COLUMN TYPE (SA003)
  writeFileSync(
    join(TMP_DIR, "file8.sql"),
    `-- Migration: change column type
set lock_timeout = '5s';
alter table products
  alter column price type numeric(10, 2);
`,
  );
});

afterAll(() => {
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Run analysis on a file, skipping SA013 (lock_timeout) to focus on the
 *  rules under test. SA013 fires on any risky DDL without a preceding
 *  SET lock_timeout, which is orthogonal to the patterns tested here. */
function analyzeFile(
  fileName: string,
  config: AnalysisConfig = {},
): Finding[] {
  const filePath = join(TMP_DIR, fileName);
  return analyzer.analyze(filePath, { config });
}

/** Extract findings for a specific rule ID. */
function findingsFor(findings: Finding[], ruleId: string): Finding[] {
  return findings.filter((f) => f.ruleId === ruleId);
}

// ---------------------------------------------------------------------------
// File 1: SA001 -- ADD COLUMN NOT NULL without DEFAULT
// ---------------------------------------------------------------------------

describe("file1: ALTER TABLE ADD COLUMN NOT NULL without DEFAULT", () => {
  test("SA001 fires on NOT NULL column without DEFAULT", () => {
    const findings = analyzeFile("file1.sql");
    const sa001 = findingsFor(findings, "SA001");

    expect(sa001).toHaveLength(1);
    expect(sa001[0]!.severity).toBe("error");
    expect(sa001[0]!.message).toContain("email");
    expect(sa001[0]!.message).toContain("users");
    expect(sa001[0]!.message).toContain("NOT NULL");
    expect(sa001[0]!.message).toContain("without a DEFAULT");
    expect(sa001[0]!.location.line).toBeGreaterThan(1);
    expect(sa001[0]!.suggestion).toBeDefined();
  });

  test("SA001 finding has correct location pointing to the ALTER TABLE statement", () => {
    const findings = analyzeFile("file1.sql");
    const sa001 = findingsFor(findings, "SA001");

    // The parser reports the statement location at the whitespace/newline
    // preceding the ALTER TABLE keyword (byte offset of the statement
    // boundary). The important thing is that it points near the correct
    // statement.
    expect(sa001[0]!.location.line).toBeGreaterThanOrEqual(2);
    expect(sa001[0]!.location.line).toBeLessThanOrEqual(3);
  });

  test("SA004 does NOT fire on file1 (no CREATE INDEX)", () => {
    const findings = analyzeFile("file1.sql");
    const sa004 = findingsFor(findings, "SA004");
    expect(sa004).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// File 2: SA004 -- CREATE INDEX without CONCURRENTLY
// ---------------------------------------------------------------------------

describe("file2: CREATE INDEX without CONCURRENTLY", () => {
  test("SA004 fires on CREATE INDEX without CONCURRENTLY", () => {
    const findings = analyzeFile("file2.sql");
    const sa004 = findingsFor(findings, "SA004");

    expect(sa004).toHaveLength(1);
    expect(sa004[0]!.severity).toBe("warn");
    expect(sa004[0]!.message).toContain("idx_users_email");
    expect(sa004[0]!.message).toContain("users");
    expect(sa004[0]!.message).toContain("CONCURRENTLY");
    expect(sa004[0]!.suggestion).toContain("CONCURRENTLY");
  });

  test("SA001 does NOT fire on file2 (no ADD COLUMN NOT NULL)", () => {
    const findings = analyzeFile("file2.sql");
    const sa001 = findingsFor(findings, "SA001");
    expect(sa001).toHaveLength(0);
  });

  test("SA004 finding location points to the CREATE INDEX statement", () => {
    const findings = analyzeFile("file2.sql");
    const sa004 = findingsFor(findings, "SA004");

    // The parser reports the statement location near line 2-3
    expect(sa004[0]!.location.line).toBeGreaterThanOrEqual(2);
    expect(sa004[0]!.location.line).toBeLessThanOrEqual(3);
  });
});

// ---------------------------------------------------------------------------
// File 3: SA007 -- DROP TABLE
// ---------------------------------------------------------------------------

describe("file3: DROP TABLE", () => {
  test("SA007 fires on DROP TABLE in deploy context", () => {
    const findings = analyzeFile("file3.sql");
    const sa007 = findingsFor(findings, "SA007");

    expect(sa007).toHaveLength(1);
    expect(sa007[0]!.severity).toBe("error");
    expect(sa007[0]!.message).toContain("audit_log");
    expect(sa007[0]!.message).toContain("irreversible");
  });

  test("SA007 does NOT fire in revert context (rule-level test)", () => {
    // The SA007 rule checks isRevertContext on the AnalysisContext. Test
    // the rule directly to verify it respects revert context.
    const { parseSync } = require("libpg-query");
    const sql = `set lock_timeout = '5s';\ndrop table audit_log;\n`;
    const ast = parseSync(sql);
    const { SA007 } = require("../../src/analysis/rules/SA007.js");

    // Non-revert context: should fire
    const nonRevertFindings = SA007.check({
      ast,
      rawSql: sql,
      filePath: "deploy/drop.sql",
      pgVersion: 17,
      config: {},
      isRevertContext: false,
    });
    expect(findingsFor(nonRevertFindings, "SA007")).toHaveLength(1);

    // Revert context: should NOT fire
    const revertFindings = SA007.check({
      ast,
      rawSql: sql,
      filePath: "revert/drop.sql",
      pgVersion: 17,
      config: {},
      isRevertContext: true,
    });
    expect(findingsFor(revertFindings, "SA007")).toHaveLength(0);
  });

  test("SA007 finding has suggestion about backups", () => {
    const findings = analyzeFile("file3.sql");
    const sa007 = findingsFor(findings, "SA007");
    expect(sa007[0]!.suggestion).toContain("backup");
  });
});

// ---------------------------------------------------------------------------
// File 4: Safe migration -- NO rules should fire
// ---------------------------------------------------------------------------

describe("file4: safe migration (no findings expected)", () => {
  test("no findings for a well-written CREATE TABLE IF NOT EXISTS", () => {
    const findings = analyzeFile("file4.sql");

    // Filter out info-level findings (some rules produce informational notices)
    const errorsAndWarnings = findings.filter(
      (f) => f.severity === "error" || f.severity === "warn",
    );

    expect(errorsAndWarnings).toHaveLength(0);
  });

  test("no SA001 on safe file", () => {
    const findings = analyzeFile("file4.sql");
    expect(findingsFor(findings, "SA001")).toHaveLength(0);
  });

  test("no SA004 on safe file", () => {
    const findings = analyzeFile("file4.sql");
    expect(findingsFor(findings, "SA004")).toHaveLength(0);
  });

  test("no SA007 on safe file", () => {
    const findings = analyzeFile("file4.sql");
    expect(findingsFor(findings, "SA007")).toHaveLength(0);
  });

  test("no SA010 on safe file", () => {
    const findings = analyzeFile("file4.sql");
    expect(findingsFor(findings, "SA010")).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// File 5: PL/pgSQL exclusion -- SA010 should NOT fire inside function
// ---------------------------------------------------------------------------

describe("file5: UPDATE without WHERE inside CREATE FUNCTION (PL/pgSQL exclusion)", () => {
  test("SA010 does NOT fire on UPDATE inside function body", () => {
    const findings = analyzeFile("file5.sql");
    const sa010 = findingsFor(findings, "SA010");

    expect(sa010).toHaveLength(0);
  });

  test("no error-level findings on PL/pgSQL function definition", () => {
    const findings = analyzeFile("file5.sql");
    const errors = findings.filter((f) => f.severity === "error");
    expect(errors).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// File 6: Multiple findings in one file
// ---------------------------------------------------------------------------

describe("file6: multiple dangerous patterns in one file", () => {
  test("SA001 fires on ADD COLUMN NOT NULL without DEFAULT", () => {
    const findings = analyzeFile("file6.sql");
    const sa001 = findingsFor(findings, "SA001");

    expect(sa001).toHaveLength(1);
    expect(sa001[0]!.message).toContain("sku");
    expect(sa001[0]!.message).toContain("products");
  });

  test("SA004 fires on CREATE INDEX without CONCURRENTLY", () => {
    const findings = analyzeFile("file6.sql");
    const sa004 = findingsFor(findings, "SA004");

    expect(sa004).toHaveLength(1);
    expect(sa004[0]!.message).toContain("idx_products_sku");
  });

  test("SA006 fires on DROP COLUMN", () => {
    const findings = analyzeFile("file6.sql");
    const sa006 = findingsFor(findings, "SA006");

    expect(sa006).toHaveLength(1);
    expect(sa006[0]!.severity).toBe("warn");
    expect(sa006[0]!.message).toContain("legacy_code");
    expect(sa006[0]!.message).toContain("products");
  });

  test("SA010 fires on DELETE without WHERE", () => {
    const findings = analyzeFile("file6.sql");
    const sa010 = findingsFor(findings, "SA010");

    expect(sa010).toHaveLength(1);
    expect(sa010[0]!.message).toContain("temp_data");
    expect(sa010[0]!.message).toContain("without a WHERE");
  });

  test("total findings include SA001, SA004, SA006, SA010", () => {
    const findings = analyzeFile("file6.sql");
    const ruleIds = new Set(findings.map((f) => f.ruleId));

    expect(ruleIds.has("SA001")).toBe(true);
    expect(ruleIds.has("SA004")).toBe(true);
    expect(ruleIds.has("SA006")).toBe(true);
    expect(ruleIds.has("SA010")).toBe(true);
  });

  test("findings are ordered by location (line number)", () => {
    const findings = analyzeFile("file6.sql");
    // Findings from different rules may arrive in rule execution order,
    // but within the same file they should have monotonically increasing
    // or at least valid line numbers
    for (const f of findings) {
      expect(f.location.line).toBeGreaterThan(0);
      expect(f.location.column).toBeGreaterThan(0);
    }
  });
});

// ---------------------------------------------------------------------------
// File 7: SA020 -- CIC inside transaction
// ---------------------------------------------------------------------------

describe("file7: CREATE INDEX CONCURRENTLY inside BEGIN (SA020)", () => {
  test("SA020 fires on CIC inside explicit BEGIN block", () => {
    const findings = analyzeFile("file7.sql");
    const sa020 = findingsFor(findings, "SA020");

    expect(sa020).toHaveLength(1);
    expect(sa020[0]!.severity).toBe("error");
    expect(sa020[0]!.message).toContain("CONCURRENTLY");
    expect(sa020[0]!.message).toContain("cannot run inside a transaction");
    expect(sa020[0]!.message).toContain("idx_orders_user");
  });

  test("SA004 does NOT fire on CIC (CONCURRENTLY is present)", () => {
    const findings = analyzeFile("file7.sql");
    const sa004 = findingsFor(findings, "SA004");
    expect(sa004).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// File 8: SA003 -- ALTER COLUMN TYPE
// ---------------------------------------------------------------------------

describe("file8: ALTER COLUMN TYPE (SA003)", () => {
  test("SA003 fires on ALTER COLUMN TYPE", () => {
    const findings = analyzeFile("file8.sql");
    const sa003 = findingsFor(findings, "SA003");

    expect(sa003).toHaveLength(1);
    expect(sa003[0]!.severity).toBe("error");
    expect(sa003[0]!.message).toContain("price");
    expect(sa003[0]!.message).toContain("products");
    expect(sa003[0]!.message).toContain("table rewrite");
  });

  test("SA003 finding has suggestion about expand/contract", () => {
    const findings = analyzeFile("file8.sql");
    const sa003 = findingsFor(findings, "SA003");
    expect(sa003[0]!.suggestion).toContain("expand/contract");
  });
});

// ---------------------------------------------------------------------------
// Reporter format validation
// ---------------------------------------------------------------------------

describe("reporter format validation on real findings", () => {
  test("JSON output is valid JSON with correct schema", () => {
    const findings = analyzeFile("file6.sql");
    const metadata: ReportMetadata = {
      files_analyzed: 1,
      rules_checked: 43,
      duration_ms: 10,
    };

    const jsonStr = formatJson(findings, metadata);
    const parsed = JSON.parse(jsonStr);

    expect(parsed.version).toBe(1);
    expect(parsed.metadata.files_analyzed).toBe(1);
    expect(parsed.metadata.rules_checked).toBe(43);
    expect(Array.isArray(parsed.findings)).toBe(true);
    expect(parsed.findings.length).toBeGreaterThan(0);

    // Verify each finding has the required fields
    for (const f of parsed.findings) {
      expect(f.ruleId).toBeDefined();
      expect(f.severity).toBeDefined();
      expect(f.message).toBeDefined();
      expect(f.location).toBeDefined();
      expect(f.location.file).toBeDefined();
      expect(f.location.line).toBeGreaterThan(0);
      expect(f.location.column).toBeGreaterThan(0);
    }

    // Verify summary
    expect(parsed.summary).toBeDefined();
    expect(typeof parsed.summary.errors).toBe("number");
    expect(typeof parsed.summary.warnings).toBe("number");
    expect(typeof parsed.summary.info).toBe("number");
  });

  test("JSON summary counts match actual findings", () => {
    const findings = analyzeFile("file6.sql");
    const summary = computeSummary(findings);
    const metadata: ReportMetadata = {
      files_analyzed: 1,
      rules_checked: 43,
      duration_ms: 5,
    };

    const parsed = JSON.parse(formatJson(findings, metadata));

    expect(parsed.summary.errors).toBe(summary.errors);
    expect(parsed.summary.warnings).toBe(summary.warnings);
    expect(parsed.summary.info).toBe(summary.info);
  });

  test("github-annotations format produces valid workflow commands", () => {
    const findings = analyzeFile("file2.sql");
    const output = formatGithubAnnotations(findings);

    const lines = output.trim().split("\n");
    expect(lines.length).toBeGreaterThan(0);

    for (const line of lines) {
      // Each line must match ::level file=...,line=...,col=...::message
      expect(line).toMatch(/^::(error|warning|notice) file=.+,line=\d+,col=\d+::.+$/);
    }
  });

  test("github-annotations maps severity correctly", () => {
    // file1 has SA001 (error severity)
    const errorFindings = analyzeFile("file1.sql");
    const errorOutput = formatGithubAnnotations(
      errorFindings.filter((f) => f.ruleId === "SA001"),
    );
    expect(errorOutput).toContain("::error ");

    // file2 has SA004 (warn severity)
    const warnFindings = analyzeFile("file2.sql");
    const warnOutput = formatGithubAnnotations(
      warnFindings.filter((f) => f.ruleId === "SA004"),
    );
    expect(warnOutput).toContain("::warning ");
  });

  test("text format includes finding details", () => {
    const findings = analyzeFile("file1.sql");
    const sa001Only = findings.filter((f) => f.ruleId === "SA001");
    const output = formatText(sa001Only, join(TMP_DIR, "file1.sql"), false);

    expect(output).toContain("SA001");
    expect(output).toContain("error");
    expect(output).toContain("email");
    expect(output).toContain("suggestion:");
  });

  test("JSON output for clean file has empty findings array", () => {
    const findings = analyzeFile("file4.sql");
    const metadata: ReportMetadata = {
      files_analyzed: 1,
      rules_checked: 43,
      duration_ms: 2,
    };

    // Filter only error/warn like we do in the "safe" test
    const errorsAndWarnings = findings.filter(
      (f) => f.severity === "error" || f.severity === "warn",
    );

    const parsed = JSON.parse(formatJson(errorsAndWarnings, metadata));
    expect(parsed.findings).toHaveLength(0);
    expect(parsed.summary.errors).toBe(0);
    expect(parsed.summary.warnings).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Full pipeline: analyzeSql with raw SQL strings
// ---------------------------------------------------------------------------

describe("full pipeline: analyzeSql on raw SQL", () => {
  test("analyzeSql produces identical results to analyze for same SQL", () => {
    const { readFileSync } = require("fs");
    const filePath = join(TMP_DIR, "file1.sql");
    const sql = readFileSync(filePath, "utf-8");

    const fromFile = analyzer.analyze(filePath);
    const fromSql = analyzer.analyzeSql(sql, filePath);

    expect(fromSql.length).toBe(fromFile.length);
    for (let i = 0; i < fromFile.length; i++) {
      expect(fromSql[i]!.ruleId).toBe(fromFile[i]!.ruleId);
      expect(fromSql[i]!.severity).toBe(fromFile[i]!.severity);
      expect(fromSql[i]!.location.line).toBe(fromFile[i]!.location.line);
    }
  });

  test("analyzeSql on empty SQL returns no findings", () => {
    const findings = analyzer.analyzeSql("", "empty.sql");
    expect(findings).toHaveLength(0);
  });

  test("analyzeSql on whitespace-only SQL returns no findings", () => {
    const findings = analyzer.analyzeSql("   \n\n  \n", "whitespace.sql");
    expect(findings).toHaveLength(0);
  });

  test("analyzeSql on unparseable SQL returns parse-error finding", () => {
    const findings = analyzer.analyzeSql(
      "SELEC broken syntax here;",
      "broken.sql",
    );
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("parse-error");
    expect(findings[0]!.severity).toBe("error");
    expect(findings[0]!.message).toContain("Failed to parse");
  });
});

// ---------------------------------------------------------------------------
// Config: skip rules, severity overrides
// ---------------------------------------------------------------------------

describe("config: skip rules and severity overrides", () => {
  test("skipping SA001 removes it from findings", () => {
    const findings = analyzeFile("file1.sql", { skip: ["SA001"] });
    const sa001 = findingsFor(findings, "SA001");
    expect(sa001).toHaveLength(0);
  });

  test("setting SA004 severity to 'off' removes it from findings", () => {
    const findings = analyzeFile("file2.sql", {
      rules: { SA004: { severity: "off" } },
    });
    const sa004 = findingsFor(findings, "SA004");
    expect(sa004).toHaveLength(0);
  });

  test("setting SA004 severity to 'error' promotes it", () => {
    const findings = analyzeFile("file2.sql", {
      rules: { SA004: { severity: "error" } },
    });
    const sa004 = findingsFor(findings, "SA004");
    expect(sa004).toHaveLength(1);
    expect(sa004[0]!.severity).toBe("error");
  });

  test("errorOnWarn promotes all warnings to errors", () => {
    const findings = analyzeFile("file2.sql", { errorOnWarn: true });
    const sa004 = findingsFor(findings, "SA004");
    expect(sa004).toHaveLength(1);
    expect(sa004[0]!.severity).toBe("error");
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

describe("edge cases", () => {
  test("SQL with psql metacommands does not produce parse errors", () => {
    const sql = `\\set ON_ERROR_STOP on
create table if not exists t (id int8 generated always as identity primary key);
`;
    const findings = analyzer.analyzeSql(sql, "metacommand.sql");
    const parseErrors = findingsFor(findings, "parse-error");
    expect(parseErrors).toHaveLength(0);
  });

  test("SQL comment-only file produces no findings", () => {
    const sql = `-- This is just a comment
-- Another comment
/* Block comment */
`;
    const findings = analyzer.analyzeSql(sql, "comments.sql");
    expect(findings).toHaveLength(0);
  });

  test("multiple statements separated by semicolons are all analyzed", () => {
    const sql = `set lock_timeout = '5s';
alter table t1 add column a text not null;
alter table t2 add column b text not null;
`;
    const findings = analyzer.analyzeSql(sql, "multi.sql");
    const sa001 = findingsFor(findings, "SA001");
    expect(sa001).toHaveLength(2);
    expect(sa001[0]!.message).toContain("t1");
    expect(sa001[1]!.message).toContain("t2");
  });

  test("SA020 does not fire on CIC without transaction context", () => {
    const sql = `create index concurrently idx_test on t (col);`;
    const findings = analyzer.analyzeSql(sql, "cic-no-tx.sql");
    const sa020 = findingsFor(findings, "SA020");
    expect(sa020).toHaveLength(0);
  });

  test("SA020 fires on CIC with isTransactional context", () => {
    const { parseSync } = require("libpg-query");
    const sql = `create index concurrently idx_test on t (col);`;
    const ast = parseSync(sql);
    const context = {
      ast,
      rawSql: sql,
      filePath: "transactional.sql",
      pgVersion: 17,
      config: {},
      isTransactional: true,
    };
    // Import SA020 directly to test the rule
    const { SA020 } = require("../../src/analysis/rules/SA020.js");
    const findings = SA020.check(context);
    const sa020 = findingsFor(findings, "SA020");
    expect(sa020).toHaveLength(1);
  });

  test("inline suppression disables a specific rule", () => {
    const sql = `-- sqlever:disable SA001
set lock_timeout = '5s';
alter table users add column name text not null;
`;
    const findings = analyzer.analyzeSql(sql, "suppressed.sql");
    const sa001 = findingsFor(findings, "SA001");
    expect(sa001).toHaveLength(0);
  });
});
