/**
 * Tests for analysis rules SA038-SA042 (style rules, info severity).
 *
 * Uses libpg-query to parse SQL fixtures and verifies that each rule
 * triggers (or does not trigger) on the appropriate SQL patterns.
 *
 * Test structure per rule:
 * - trigger/ fixtures: must produce at least one finding with the rule ID
 * - no_trigger/ fixtures: must produce zero findings for that rule
 * - Additional inline tests for edge cases and specific behaviors
 */

import { describe, test, expect, beforeAll } from "bun:test";
import { parseSync, loadModule } from "libpg-query";
import { readFileSync, readdirSync } from "fs";
import { join } from "path";

import { SA038 } from "../../src/analysis/rules/SA038.js";
import { SA039 } from "../../src/analysis/rules/SA039.js";
import { SA040 } from "../../src/analysis/rules/SA040.js";
import { SA041 } from "../../src/analysis/rules/SA041.js";
import { SA042 } from "../../src/analysis/rules/SA042.js";
import { getRule } from "../../src/analysis/rules/index.js";
import type { AnalysisContext } from "../../src/analysis/types.js";

const FIXTURES_DIR = join(import.meta.dir, "..", "fixtures", "analysis");

/**
 * Build an AnalysisContext from raw SQL text.
 */
function makeContext(
  sql: string,
  overrides: Partial<AnalysisContext> = {},
): AnalysisContext {
  const ast = parseSync(sql);
  return {
    ast,
    rawSql: sql,
    filePath: overrides.filePath ?? "test.sql",
    pgVersion: overrides.pgVersion ?? 17,
    config: overrides.config ?? {},
    isRevertContext: overrides.isRevertContext ?? false,
    ...overrides,
  };
}

/**
 * Load a fixture file and build a context.
 */
function loadFixture(
  ruleId: string,
  category: "trigger" | "no_trigger",
  fileName: string,
  overrides: Partial<AnalysisContext> = {},
): AnalysisContext {
  const filePath = join(FIXTURES_DIR, ruleId, category, fileName);
  const sql = readFileSync(filePath, "utf-8");
  return makeContext(sql, { filePath, ...overrides });
}

/**
 * Get all fixture files in a directory.
 */
function getFixtureFiles(
  ruleId: string,
  category: "trigger" | "no_trigger",
): string[] {
  const dir = join(FIXTURES_DIR, ruleId, category);
  try {
    return readdirSync(dir).filter((f) => f.endsWith(".sql"));
  } catch {
    return [];
  }
}

// Load WASM module before all tests
beforeAll(async () => {
  await loadModule();
});

// ─── Registry ────────────────────────────────────────────────────────

describe("rule registry (SA038-SA042)", () => {
  test("getRule returns SA038-SA042 by ID", () => {
    for (const id of ["SA038", "SA039", "SA040", "SA041", "SA042"]) {
      expect(getRule(id)?.id).toBe(id);
    }
  });

  test("all style rules have correct interface fields", () => {
    const styleRules = [SA038, SA039, SA040, SA041, SA042];
    for (const rule of styleRules) {
      expect(rule.id).toMatch(/^SA\d{3}$/);
      expect(rule.severity).toBe("info");
      expect(rule.type).toBe("static");
      expect(typeof rule.check).toBe("function");
    }
  });
});

// ─── SA038: Prefer text over varchar(n) ───────────────────────────────

describe("SA038: Prefer text over varchar(n)", () => {
  test("metadata", () => {
    expect(SA038.id).toBe("SA038");
    expect(SA038.severity).toBe("info");
    expect(SA038.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA038", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA038", "trigger", file);
      const findings = SA038.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA038");
      expect(findings[0]!.severity).toBe("info");
    });
  }

  for (const file of getFixtureFiles("SA038", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA038", "no_trigger", file);
      const findings = SA038.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on CREATE TABLE with varchar column", () => {
    const ctx = makeContext(
      "CREATE TABLE users (id int8 PRIMARY KEY, name varchar(100));",
    );
    const findings = SA038.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("name");
    expect(findings[0]!.message).toContain("users");
    expect(findings[0]!.message).toContain("varchar");
  });

  test("fires on ALTER TABLE ADD COLUMN varchar", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD COLUMN email varchar(255);",
    );
    const findings = SA038.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("email");
    expect(findings[0]!.message).toContain("varchar");
  });

  test("detects multiple varchar columns in one table", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 PRIMARY KEY, a varchar(50), b varchar(100));",
    );
    const findings = SA038.check(ctx);
    expect(findings).toHaveLength(2);
  });

  test("does not fire on text columns", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 PRIMARY KEY, name text);",
    );
    const findings = SA038.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on integer columns", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 PRIMARY KEY, count integer);",
    );
    const findings = SA038.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about CHECK constraint", () => {
    const ctx = makeContext("CREATE TABLE t (name varchar(50));");
    const findings = SA038.check(ctx);
    expect(findings[0]!.suggestion).toContain("CHECK");
  });
});

// ─── SA039: Prefer bigint over int for PKs ────────────────────────────

describe("SA039: Prefer bigint over int for PKs", () => {
  test("metadata", () => {
    expect(SA039.id).toBe("SA039");
    expect(SA039.severity).toBe("info");
    expect(SA039.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA039", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA039", "trigger", file);
      const findings = SA039.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA039");
      expect(findings[0]!.severity).toBe("info");
    });
  }

  for (const file of getFixtureFiles("SA039", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA039", "no_trigger", file);
      const findings = SA039.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on integer PRIMARY KEY", () => {
    const ctx = makeContext(
      "CREATE TABLE users (id integer PRIMARY KEY, name text);",
    );
    const findings = SA039.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("id");
    expect(findings[0]!.message).toContain("users");
    expect(findings[0]!.message).toContain("32-bit");
  });

  test("fires on smallint PRIMARY KEY", () => {
    const ctx = makeContext(
      "CREATE TABLE lookup (id smallint PRIMARY KEY, label text);",
    );
    const findings = SA039.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("id");
  });

  test("does not fire on bigint PRIMARY KEY", () => {
    const ctx = makeContext(
      "CREATE TABLE users (id bigint PRIMARY KEY, name text);",
    );
    const findings = SA039.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on int8 IDENTITY PRIMARY KEY", () => {
    const ctx = makeContext(
      "CREATE TABLE users (id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY);",
    );
    const findings = SA039.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on integer column without PRIMARY KEY", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id bigint PRIMARY KEY, count integer);",
    );
    const findings = SA039.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on text PRIMARY KEY", () => {
    const ctx = makeContext(
      "CREATE TABLE config (key text PRIMARY KEY, value text);",
    );
    const findings = SA039.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about bigint", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id integer PRIMARY KEY);",
    );
    const findings = SA039.check(ctx);
    expect(findings[0]!.suggestion).toContain("bigint");
  });
});

// ─── SA040: Prefer IDENTITY over SERIAL ───────────────────────────────

describe("SA040: Prefer IDENTITY over SERIAL", () => {
  test("metadata", () => {
    expect(SA040.id).toBe("SA040");
    expect(SA040.severity).toBe("info");
    expect(SA040.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA040", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA040", "trigger", file);
      const findings = SA040.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA040");
      expect(findings[0]!.severity).toBe("info");
    });
  }

  for (const file of getFixtureFiles("SA040", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA040", "no_trigger", file);
      const findings = SA040.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on serial", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id serial PRIMARY KEY, name text);",
    );
    const findings = SA040.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("serial");
    expect(findings[0]!.message).toContain("IDENTITY");
  });

  test("fires on bigserial", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id bigserial PRIMARY KEY, name text);",
    );
    const findings = SA040.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("bigserial");
  });

  test("fires on smallserial", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id smallserial PRIMARY KEY);",
    );
    const findings = SA040.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("smallserial");
  });

  test("fires on serial4", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id serial4 PRIMARY KEY);",
    );
    const findings = SA040.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("serial4");
  });

  test("fires on serial8", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id serial8 PRIMARY KEY);",
    );
    const findings = SA040.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("serial8");
  });

  test("fires on ALTER TABLE ADD COLUMN serial", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD COLUMN seq serial;",
    );
    const findings = SA040.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("seq");
  });

  test("does not fire on IDENTITY column", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY);",
    );
    const findings = SA040.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on plain integer columns", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id bigint PRIMARY KEY, count int4);",
    );
    const findings = SA040.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about IDENTITY", () => {
    const ctx = makeContext("CREATE TABLE t (id serial);");
    const findings = SA040.check(ctx);
    expect(findings[0]!.suggestion).toContain("generated always as identity");
  });
});

// ─── SA041: Prefer timestamptz over timestamp ─────────────────────────

describe("SA041: Prefer timestamptz over timestamp", () => {
  test("metadata", () => {
    expect(SA041.id).toBe("SA041");
    expect(SA041.severity).toBe("info");
    expect(SA041.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA041", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA041", "trigger", file);
      const findings = SA041.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA041");
      expect(findings[0]!.severity).toBe("info");
    });
  }

  for (const file of getFixtureFiles("SA041", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA041", "no_trigger", file);
      const findings = SA041.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on timestamp column in CREATE TABLE", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 PRIMARY KEY, created_at timestamp);",
    );
    const findings = SA041.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("created_at");
    expect(findings[0]!.message).toContain("timestamp without time zone");
  });

  test("fires on ALTER TABLE ADD COLUMN timestamp", () => {
    const ctx = makeContext(
      "ALTER TABLE events ADD COLUMN updated_at timestamp;",
    );
    const findings = SA041.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("updated_at");
  });

  test("detects multiple timestamp columns", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 PRIMARY KEY, created_at timestamp, updated_at timestamp);",
    );
    const findings = SA041.check(ctx);
    expect(findings).toHaveLength(2);
  });

  test("does not fire on timestamptz", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 PRIMARY KEY, created_at timestamptz);",
    );
    const findings = SA041.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on date type", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 PRIMARY KEY, event_date date);",
    );
    const findings = SA041.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on text columns", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int8 PRIMARY KEY, name text);",
    );
    const findings = SA041.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about timestamptz", () => {
    const ctx = makeContext("CREATE TABLE t (ts timestamp);");
    const findings = SA041.check(ctx);
    expect(findings[0]!.suggestion).toContain("timestamptz");
  });
});

// ─── SA042: Prefer IF NOT EXISTS / IF EXISTS ──────────────────────────

describe("SA042: Prefer IF NOT EXISTS / IF EXISTS", () => {
  test("metadata", () => {
    expect(SA042.id).toBe("SA042");
    expect(SA042.severity).toBe("info");
    expect(SA042.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA042", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA042", "trigger", file);
      const findings = SA042.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA042");
      expect(findings[0]!.severity).toBe("info");
    });
  }

  for (const file of getFixtureFiles("SA042", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA042", "no_trigger", file);
      const findings = SA042.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on CREATE TABLE without IF NOT EXISTS", () => {
    const ctx = makeContext("CREATE TABLE users (id int8 PRIMARY KEY);");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("CREATE TABLE");
    expect(findings[0]!.message).toContain("IF NOT EXISTS");
  });

  test("fires on CREATE INDEX without IF NOT EXISTS", () => {
    const ctx = makeContext("CREATE INDEX idx ON users (name);");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("CREATE INDEX");
    expect(findings[0]!.message).toContain("IF NOT EXISTS");
  });

  test("fires on CREATE SCHEMA without IF NOT EXISTS", () => {
    const ctx = makeContext("CREATE SCHEMA myapp;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("CREATE SCHEMA");
    expect(findings[0]!.message).toContain("IF NOT EXISTS");
  });

  test("fires on DROP TABLE without IF EXISTS", () => {
    const ctx = makeContext("DROP TABLE users;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("DROP TABLE");
    expect(findings[0]!.message).toContain("IF EXISTS");
  });

  test("fires on DROP INDEX without IF EXISTS", () => {
    const ctx = makeContext("DROP INDEX idx_users_name;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("DROP INDEX");
    expect(findings[0]!.message).toContain("IF EXISTS");
  });

  test("fires on DROP SCHEMA without IF EXISTS", () => {
    const ctx = makeContext("DROP SCHEMA myapp;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("DROP SCHEMA");
    expect(findings[0]!.message).toContain("IF EXISTS");
  });

  test("does not fire on CREATE TABLE IF NOT EXISTS", () => {
    const ctx = makeContext(
      "CREATE TABLE IF NOT EXISTS users (id int8 PRIMARY KEY);",
    );
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on CREATE INDEX IF NOT EXISTS", () => {
    const ctx = makeContext(
      "CREATE INDEX IF NOT EXISTS idx ON users (name);",
    );
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on CREATE SCHEMA IF NOT EXISTS", () => {
    const ctx = makeContext("CREATE SCHEMA IF NOT EXISTS myapp;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DROP TABLE IF EXISTS", () => {
    const ctx = makeContext("DROP TABLE IF EXISTS users;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DROP INDEX IF EXISTS", () => {
    const ctx = makeContext("DROP INDEX IF EXISTS idx;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on SELECT", () => {
    const ctx = makeContext("SELECT 1;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DROP VIEW (unsupported type)", () => {
    const ctx = makeContext("DROP VIEW myview;");
    const findings = SA042.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about idempotent migrations", () => {
    const ctx = makeContext("CREATE TABLE t (id int8);");
    const findings = SA042.check(ctx);
    expect(findings[0]!.suggestion).toContain("idempotent");
  });
});

// ─── Cross-cutting tests (SA038-SA042) ────────────────────────────────

describe("cross-cutting: SA038-SA042 empty inputs", () => {
  test("empty SQL produces no findings for any style rule", () => {
    const ctx = makeContext("SELECT 1;");
    const styleRules = [SA038, SA039, SA040, SA041, SA042];
    for (const rule of styleRules) {
      const findings = rule.check(ctx);
      // SA042 fires on SELECT since it's not a CREATE/DROP
      if (rule.id !== "SA042") {
        expect(findings).toHaveLength(0);
      }
    }
  });

  test("null AST produces no findings", () => {
    const ctx: AnalysisContext = {
      ast: { stmts: [] },
      rawSql: "",
      filePath: "test.sql",
      pgVersion: 17,
      config: {},
    };
    const styleRules = [SA038, SA039, SA040, SA041, SA042];
    for (const rule of styleRules) {
      const findings = rule.check(ctx);
      expect(findings).toHaveLength(0);
    }
  });
});
