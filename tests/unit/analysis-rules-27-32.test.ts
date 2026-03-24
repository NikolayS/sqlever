/**
 * Tests for analysis rules SA027-SA032.
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

import { SA027 } from "../../src/analysis/rules/SA027.js";
import { SA028 } from "../../src/analysis/rules/SA028.js";
import { SA029 } from "../../src/analysis/rules/SA029.js";
import { SA030 } from "../../src/analysis/rules/SA030.js";
import { SA031 } from "../../src/analysis/rules/SA031.js";
import { SA032 } from "../../src/analysis/rules/SA032.js";
import { allRules, getRule } from "../../src/analysis/rules/index.js";
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

// ─── Registry update ──────────────────────────────────────────────────

describe("rule registry (SA027-SA032)", () => {
  test("allRules contains 33 rules (SA001-SA032 plus SA002b)", () => {
    expect(allRules).toHaveLength(33);
  });

  test("getRule returns SA027-SA032 by ID", () => {
    for (const id of ["SA027", "SA028", "SA029", "SA030", "SA031", "SA032"]) {
      expect(getRule(id)?.id).toBe(id);
    }
  });

  test("all new rules have correct interface fields", () => {
    const newRules = [SA027, SA028, SA029, SA030, SA031, SA032];
    for (const rule of newRules) {
      expect(rule.id).toMatch(/^SA\d{3}$/);
      expect(["error", "warn", "info"]).toContain(rule.severity);
      expect(["static", "connected", "hybrid"]).toContain(rule.type);
      expect(typeof rule.check).toBe("function");
    }
  });
});

// ─── SA027: ALTER TABLE ALTER COLUMN DROP NOT NULL ────────────────────

describe("SA027: ALTER TABLE ALTER COLUMN DROP NOT NULL", () => {
  test("metadata", () => {
    expect(SA027.id).toBe("SA027");
    expect(SA027.severity).toBe("warn");
    expect(SA027.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA027", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA027", "trigger", file);
      const findings = SA027.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA027");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA027", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA027", "no_trigger", file);
      const findings = SA027.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("finding includes table and column names", () => {
    const ctx = makeContext(
      "ALTER TABLE users ALTER COLUMN email DROP NOT NULL;",
    );
    const findings = SA027.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("email");
    expect(findings[0]!.message).toContain("users");
  });

  test("finding includes suggestion", () => {
    const ctx = makeContext(
      "ALTER TABLE users ALTER COLUMN email DROP NOT NULL;",
    );
    const findings = SA027.check(ctx);
    expect(findings[0]!.suggestion).toBeDefined();
    expect(findings[0]!.suggestion).toContain("NULL");
  });

  test("finding has valid location", () => {
    const ctx = makeContext(
      "ALTER TABLE users ALTER COLUMN email DROP NOT NULL;",
    );
    const findings = SA027.check(ctx);
    expect(findings[0]!.location.line).toBe(1);
    expect(findings[0]!.location.column).toBe(1);
  });

  test("handles multiple DROP NOT NULL in one statement", () => {
    const sql = `ALTER TABLE t ALTER COLUMN a DROP NOT NULL, ALTER COLUMN b DROP NOT NULL;`;
    const ctx = makeContext(sql);
    const findings = SA027.check(ctx);
    expect(findings).toHaveLength(2);
  });

  test("does not fire on SET NOT NULL", () => {
    const ctx = makeContext(
      "ALTER TABLE users ALTER COLUMN email SET NOT NULL;",
    );
    const findings = SA027.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("empty SQL produces no findings", () => {
    const ctx = makeContext("SELECT 1;");
    const findings = SA027.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA028: TRUNCATE CASCADE ─────────────────────────────────────────

describe("SA028: TRUNCATE CASCADE", () => {
  test("metadata", () => {
    expect(SA028.id).toBe("SA028");
    expect(SA028.severity).toBe("warn");
    expect(SA028.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA028", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA028", "trigger", file);
      const findings = SA028.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA028");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA028", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA028", "no_trigger", file);
      const findings = SA028.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("finding includes table name", () => {
    const ctx = makeContext("TRUNCATE orders CASCADE;");
    const findings = SA028.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("orders");
  });

  test("finding includes suggestion", () => {
    const ctx = makeContext("TRUNCATE orders CASCADE;");
    const findings = SA028.check(ctx);
    expect(findings[0]!.suggestion).toBeDefined();
    expect(findings[0]!.suggestion).toContain("CASCADE");
  });

  test("does not fire on TRUNCATE without CASCADE", () => {
    const ctx = makeContext("TRUNCATE orders;");
    const findings = SA028.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on TRUNCATE RESTRICT", () => {
    const ctx = makeContext("TRUNCATE orders RESTRICT;");
    const findings = SA028.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on TRUNCATE with schema-qualified table and CASCADE", () => {
    const ctx = makeContext("TRUNCATE public.orders CASCADE;");
    const findings = SA028.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("public.orders");
  });

  test("does not fire inside CREATE FUNCTION body", () => {
    const sql = `CREATE FUNCTION cleanup() RETURNS void AS $$
BEGIN
  TRUNCATE orders CASCADE;
END;
$$ LANGUAGE plpgsql;`;
    const ctx = makeContext(sql);
    const findings = SA028.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire inside DO block", () => {
    const sql = `DO $$
BEGIN
  TRUNCATE orders CASCADE;
END;
$$;`;
    const ctx = makeContext(sql);
    const findings = SA028.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("empty SQL produces no findings", () => {
    const ctx = makeContext("SELECT 1;");
    const findings = SA028.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA029: SERIAL/BIGSERIAL column ─────────────────────────────────

describe("SA029: CREATE TABLE with SERIAL/BIGSERIAL column", () => {
  test("metadata", () => {
    expect(SA029.id).toBe("SA029");
    expect(SA029.severity).toBe("info");
    expect(SA029.type).toBe("static");
    expect(SA029.defaultOff).toBe(true);
  });

  for (const file of getFixtureFiles("SA029", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA029", "trigger", file);
      const findings = SA029.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA029");
      expect(findings[0]!.severity).toBe("info");
    });
  }

  for (const file of getFixtureFiles("SA029", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA029", "no_trigger", file);
      const findings = SA029.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("detects serial column", () => {
    const ctx = makeContext("CREATE TABLE t (id serial PRIMARY KEY);");
    const findings = SA029.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("serial");
    expect(findings[0]!.message).toContain("id");
  });

  test("detects bigserial column", () => {
    const ctx = makeContext("CREATE TABLE t (id bigserial PRIMARY KEY);");
    const findings = SA029.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("bigserial");
  });

  test("detects smallserial column", () => {
    const ctx = makeContext("CREATE TABLE t (id smallserial PRIMARY KEY);");
    const findings = SA029.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("smallserial");
  });

  test("does not fire on IDENTITY column", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY);",
    );
    const findings = SA029.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on integer column", () => {
    const ctx = makeContext("CREATE TABLE t (id integer PRIMARY KEY);");
    const findings = SA029.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("finding includes table name", () => {
    const ctx = makeContext("CREATE TABLE users (id serial PRIMARY KEY);");
    const findings = SA029.check(ctx);
    expect(findings[0]!.message).toContain("users");
  });

  test("finding includes suggestion about IDENTITY", () => {
    const ctx = makeContext("CREATE TABLE t (id serial PRIMARY KEY);");
    const findings = SA029.check(ctx);
    expect(findings[0]!.suggestion).toBeDefined();
    expect(findings[0]!.suggestion).toContain("identity");
  });

  test("empty SQL produces no findings", () => {
    const ctx = makeContext("SELECT 1;");
    const findings = SA029.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA030: ADD UNIQUE constraint / CREATE UNIQUE INDEX ──────────────

describe("SA030: ADD UNIQUE constraint or CREATE UNIQUE INDEX", () => {
  test("metadata", () => {
    expect(SA030.id).toBe("SA030");
    expect(SA030.severity).toBe("warn");
    expect(SA030.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA030", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA030", "trigger", file);
      const findings = SA030.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA030");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA030", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA030", "no_trigger", file);
      const findings = SA030.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on ALTER TABLE ADD UNIQUE constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT uniq_email UNIQUE (email);",
    );
    const findings = SA030.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
    expect(findings[0]!.message).toContain("uniq_email");
  });

  test("fires on CREATE UNIQUE INDEX", () => {
    const ctx = makeContext(
      "CREATE UNIQUE INDEX idx_email ON users (email);",
    );
    const findings = SA030.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("idx_email");
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on CREATE UNIQUE INDEX CONCURRENTLY", () => {
    const ctx = makeContext(
      "CREATE UNIQUE INDEX CONCURRENTLY idx_email ON users (email);",
    );
    const findings = SA030.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("includes column names for CREATE UNIQUE INDEX", () => {
    const ctx = makeContext(
      "CREATE UNIQUE INDEX idx_multi ON users (email, tenant_id);",
    );
    const findings = SA030.check(ctx);
    expect(findings[0]!.message).toContain("email");
    expect(findings[0]!.message).toContain("tenant_id");
  });

  test("does not fire on non-unique index", () => {
    const ctx = makeContext("CREATE INDEX idx_email ON users (email);");
    const findings = SA030.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on CHECK constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age >= 0);",
    );
    const findings = SA030.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on inline UNIQUE in CREATE TABLE", () => {
    const ctx = makeContext(
      "CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE);",
    );
    const findings = SA030.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("finding includes suggestion about duplicates", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT u UNIQUE (email);",
    );
    const findings = SA030.check(ctx);
    expect(findings[0]!.suggestion).toBeDefined();
    expect(findings[0]!.suggestion).toContain("duplicates");
  });

  test("empty SQL produces no findings", () => {
    const ctx = makeContext("SELECT 1;");
    const findings = SA030.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA031: ALTER TYPE ADD VALUE inside transaction (PG < 12) ────────

describe("SA031: ALTER TYPE ADD VALUE inside transaction (PG < 12)", () => {
  test("metadata", () => {
    expect(SA031.id).toBe("SA031");
    expect(SA031.severity).toBe("error");
    expect(SA031.type).toBe("static");
  });

  // Trigger fixtures require pgVersion < 12
  for (const file of getFixtureFiles("SA031", "trigger")) {
    test(`triggers on ${file} (PG 11)`, () => {
      const ctx = loadFixture("SA031", "trigger", file, { pgVersion: 11 });
      const findings = SA031.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA031");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  for (const file of getFixtureFiles("SA031", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      // no_trigger fixtures with default pgVersion=17 should not fire
      const ctx = loadFixture("SA031", "no_trigger", file);
      const findings = SA031.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on ALTER TYPE ADD VALUE inside BEGIN on PG 11", () => {
    const sql = "BEGIN;\nALTER TYPE status ADD VALUE 'archived';\nCOMMIT;";
    const ctx = makeContext(sql, { pgVersion: 11 });
    const findings = SA031.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("archived");
    expect(findings[0]!.message).toContain("11");
  });

  test("does not fire on PG 12", () => {
    const sql = "BEGIN;\nALTER TYPE status ADD VALUE 'archived';\nCOMMIT;";
    const ctx = makeContext(sql, { pgVersion: 12 });
    const findings = SA031.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on PG 17 (default)", () => {
    const sql = "BEGIN;\nALTER TYPE status ADD VALUE 'archived';\nCOMMIT;";
    const ctx = makeContext(sql);
    const findings = SA031.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire without BEGIN", () => {
    const sql = "ALTER TYPE status ADD VALUE 'archived';";
    const ctx = makeContext(sql, { pgVersion: 11 });
    const findings = SA031.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires for multiple ADD VALUE statements on PG 11", () => {
    const sql =
      "BEGIN;\nALTER TYPE status ADD VALUE 'archived';\nALTER TYPE status ADD VALUE 'deleted';\nCOMMIT;";
    const ctx = makeContext(sql, { pgVersion: 11 });
    const findings = SA031.check(ctx);
    expect(findings).toHaveLength(2);
  });

  test("finding includes suggestion", () => {
    const sql = "BEGIN;\nALTER TYPE status ADD VALUE 'archived';\nCOMMIT;";
    const ctx = makeContext(sql, { pgVersion: 11 });
    const findings = SA031.check(ctx);
    expect(findings[0]!.suggestion).toBeDefined();
    expect(findings[0]!.suggestion).toContain("BEGIN");
  });

  test("empty SQL produces no findings", () => {
    const ctx = makeContext("SELECT 1;", { pgVersion: 11 });
    const findings = SA031.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA032: BEGIN without COMMIT or ROLLBACK ─────────────────────────

describe("SA032: BEGIN without COMMIT or ROLLBACK", () => {
  test("metadata", () => {
    expect(SA032.id).toBe("SA032");
    expect(SA032.severity).toBe("warn");
    expect(SA032.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA032", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA032", "trigger", file);
      const findings = SA032.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA032");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA032", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA032", "no_trigger", file);
      const findings = SA032.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on BEGIN without COMMIT", () => {
    const sql = "BEGIN;\nCREATE TABLE users (id bigint PRIMARY KEY);";
    const ctx = makeContext(sql);
    const findings = SA032.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("BEGIN");
    expect(findings[0]!.message).toContain("COMMIT");
  });

  test("does not fire on BEGIN with COMMIT", () => {
    const sql =
      "BEGIN;\nCREATE TABLE users (id bigint PRIMARY KEY);\nCOMMIT;";
    const ctx = makeContext(sql);
    const findings = SA032.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on BEGIN with ROLLBACK", () => {
    const sql =
      "BEGIN;\nCREATE TABLE users (id bigint PRIMARY KEY);\nROLLBACK;";
    const ctx = makeContext(sql);
    const findings = SA032.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire without BEGIN", () => {
    const sql = "CREATE TABLE users (id bigint PRIMARY KEY);";
    const ctx = makeContext(sql);
    const findings = SA032.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("finding has location pointing to BEGIN", () => {
    const sql = "BEGIN;\nCREATE TABLE t (id bigint);";
    const ctx = makeContext(sql);
    const findings = SA032.check(ctx);
    expect(findings[0]!.location.line).toBe(1);
  });

  test("finding includes suggestion about COMMIT", () => {
    const sql = "BEGIN;\nCREATE TABLE t (id bigint);";
    const ctx = makeContext(sql);
    const findings = SA032.check(ctx);
    expect(findings[0]!.suggestion).toBeDefined();
    expect(findings[0]!.suggestion).toContain("COMMIT");
  });

  test("empty SQL produces no findings", () => {
    const ctx = makeContext("SELECT 1;");
    const findings = SA032.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire when COMMIT is present (even without explicit BEGIN pair)", () => {
    const sql = "INSERT INTO t (id) VALUES (1);\nCOMMIT;";
    const ctx = makeContext(sql);
    const findings = SA032.check(ctx);
    expect(findings).toHaveLength(0);
  });
});
