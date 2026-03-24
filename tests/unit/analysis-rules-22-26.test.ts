/**
 * Tests for analysis rules SA022-SA026.
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

import { SA022 } from "../../src/analysis/rules/SA022.js";
import { SA023 } from "../../src/analysis/rules/SA023.js";
import { SA024 } from "../../src/analysis/rules/SA024.js";
import { SA025 } from "../../src/analysis/rules/SA025.js";
import { SA026 } from "../../src/analysis/rules/SA026.js";
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

describe("rule registry (SA022-SA026)", () => {
  test("allRules contains 27 rules (SA001-SA026 plus SA002b)", () => {
    expect(allRules).toHaveLength(27);
  });

  test("getRule returns SA022-SA026 by ID", () => {
    for (const id of ["SA022", "SA023", "SA024", "SA025", "SA026"]) {
      expect(getRule(id)?.id).toBe(id);
    }
  });

  test("all new rules have correct interface fields", () => {
    const newRules = [SA022, SA023, SA024, SA025, SA026];
    for (const rule of newRules) {
      expect(rule.id).toMatch(/^SA\d{3}$/);
      expect(["error", "warn", "info"]).toContain(rule.severity);
      expect(["static", "connected", "hybrid"]).toContain(rule.type);
      expect(typeof rule.check).toBe("function");
    }
  });
});

// ─── SA022: DROP SCHEMA ───────────────────────────────────────────────

describe("SA022: DROP SCHEMA in non-revert context", () => {
  test("metadata", () => {
    expect(SA022.id).toBe("SA022");
    expect(SA022.severity).toBe("error");
    expect(SA022.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA022", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA022", "trigger", file);
      const findings = SA022.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA022");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  test("does not trigger in revert context", () => {
    const ctx = loadFixture(
      "SA022",
      "no_trigger",
      "drop_schema_in_revert.sql",
      { isRevertContext: true },
    );
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on DROP TABLE", () => {
    const ctx = loadFixture("SA022", "no_trigger", "drop_table.sql");
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on CREATE SCHEMA", () => {
    const ctx = loadFixture("SA022", "no_trigger", "create_schema.sql");
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on ALTER TABLE", () => {
    const ctx = loadFixture("SA022", "no_trigger", "alter_table.sql");
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on SELECT", () => {
    const ctx = loadFixture("SA022", "no_trigger", "select_statement.sql");
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on DROP INDEX", () => {
    const ctx = loadFixture("SA022", "no_trigger", "drop_index.sql");
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on DROP SCHEMA", () => {
    const ctx = makeContext("DROP SCHEMA myschema;");
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("myschema");
  });

  test("fires on DROP SCHEMA IF EXISTS", () => {
    const ctx = makeContext("DROP SCHEMA IF EXISTS myschema;");
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on DROP SCHEMA CASCADE", () => {
    const ctx = makeContext("DROP SCHEMA myschema CASCADE;");
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("exempt in revert context", () => {
    const ctx = makeContext("DROP SCHEMA myschema;", { isRevertContext: true });
    const findings = SA022.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes irreversible in message", () => {
    const ctx = makeContext("DROP SCHEMA s;");
    const findings = SA022.check(ctx);
    expect(findings[0]!.message).toContain("irreversible");
  });
});

// ─── SA023: DROP DATABASE ─────────────────────────────────────────────

describe("SA023: DROP DATABASE", () => {
  test("metadata", () => {
    expect(SA023.id).toBe("SA023");
    expect(SA023.severity).toBe("error");
    expect(SA023.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA023", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA023", "trigger", file);
      const findings = SA023.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA023");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  for (const file of getFixtureFiles("SA023", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA023", "no_trigger", file);
      const findings = SA023.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on DROP DATABASE", () => {
    const ctx = makeContext("DROP DATABASE mydb;");
    const findings = SA023.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("mydb");
  });

  test("fires on DROP DATABASE IF EXISTS", () => {
    const ctx = makeContext("DROP DATABASE IF EXISTS mydb;");
    const findings = SA023.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires even in revert context", () => {
    const ctx = makeContext("DROP DATABASE mydb;", { isRevertContext: true });
    const findings = SA023.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("includes database name in message", () => {
    const ctx = makeContext("DROP DATABASE production;");
    const findings = SA023.check(ctx);
    expect(findings[0]!.message).toContain("production");
  });

  test("includes suggestion about never in migration", () => {
    const ctx = makeContext("DROP DATABASE mydb;");
    const findings = SA023.check(ctx);
    expect(findings[0]!.suggestion).toContain("never");
  });
});

// ─── SA024: DROP ... CASCADE ──────────────────────────────────────────

describe("SA024: DROP ... CASCADE", () => {
  test("metadata", () => {
    expect(SA024.id).toBe("SA024");
    expect(SA024.severity).toBe("error");
    expect(SA024.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA024", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA024", "trigger", file);
      const findings = SA024.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA024");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  for (const file of getFixtureFiles("SA024", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA024", "no_trigger", file);
      const findings = SA024.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on DROP TABLE CASCADE", () => {
    const ctx = makeContext("DROP TABLE users CASCADE;");
    const findings = SA024.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("CASCADE");
  });

  test("fires on DROP SCHEMA CASCADE", () => {
    const ctx = makeContext("DROP SCHEMA myschema CASCADE;");
    const findings = SA024.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on DROP VIEW CASCADE", () => {
    const ctx = makeContext("DROP VIEW user_summary CASCADE;");
    const findings = SA024.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on ALTER TABLE DROP COLUMN CASCADE", () => {
    const ctx = makeContext("ALTER TABLE users DROP COLUMN name CASCADE;");
    const findings = SA024.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("name");
  });

  test("does not fire on DROP TABLE without CASCADE", () => {
    const ctx = makeContext("DROP TABLE users;");
    const findings = SA024.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DROP TABLE IF EXISTS without CASCADE", () => {
    const ctx = makeContext("DROP TABLE IF EXISTS users;");
    const findings = SA024.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on ALTER TABLE DROP COLUMN without CASCADE", () => {
    const ctx = makeContext("ALTER TABLE users DROP COLUMN name;");
    const findings = SA024.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about removing CASCADE", () => {
    const ctx = makeContext("DROP TABLE users CASCADE;");
    const findings = SA024.check(ctx);
    expect(findings[0]!.suggestion).toContain("Remove CASCADE");
  });

  test("fires on DROP INDEX CASCADE", () => {
    const ctx = makeContext("DROP INDEX idx_users_email CASCADE;");
    const findings = SA024.check(ctx);
    expect(findings).toHaveLength(1);
  });
});

// ─── SA025: Nested BEGIN / START TRANSACTION ──────────────────────────

describe("SA025: Nested BEGIN / START TRANSACTION", () => {
  test("metadata", () => {
    expect(SA025.id).toBe("SA025");
    expect(SA025.severity).toBe("warn");
    expect(SA025.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA025", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA025", "trigger", file);
      const findings = SA025.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA025");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA025", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA025", "no_trigger", file);
      const findings = SA025.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on BEGIN", () => {
    const ctx = makeContext("BEGIN; SELECT 1; COMMIT;");
    const findings = SA025.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("BEGIN");
  });

  test("fires on START TRANSACTION", () => {
    const ctx = makeContext("START TRANSACTION; SELECT 1; COMMIT;");
    const findings = SA025.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("START TRANSACTION");
  });

  test("does not fire on COMMIT alone", () => {
    const ctx = makeContext("COMMIT;");
    const findings = SA025.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on ROLLBACK", () => {
    const ctx = makeContext("ROLLBACK;");
    const findings = SA025.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on SAVEPOINT", () => {
    const ctx = makeContext("SAVEPOINT sp1;");
    const findings = SA025.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on plain DDL", () => {
    const ctx = makeContext("ALTER TABLE users ADD COLUMN bio text;");
    const findings = SA025.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about SAVEPOINT", () => {
    const ctx = makeContext("BEGIN;");
    const findings = SA025.check(ctx);
    expect(findings[0]!.suggestion).toContain("SAVEPOINT");
  });
});

// ─── SA026: Missing SET statement_timeout ─────────────────────────────

describe("SA026: Missing SET statement_timeout before long-running DML", () => {
  test("metadata", () => {
    expect(SA026.id).toBe("SA026");
    expect(SA026.severity).toBe("warn");
    expect(SA026.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA026", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA026", "trigger", file);
      const findings = SA026.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA026");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA026", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA026", "no_trigger", file);
      const findings = SA026.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on UPDATE without SET statement_timeout", () => {
    const ctx = makeContext("UPDATE users SET name = 'x' WHERE id = 1;");
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("statement_timeout");
  });

  test("fires on DELETE without SET statement_timeout", () => {
    const ctx = makeContext("DELETE FROM users WHERE active = false;");
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on INSERT ... SELECT without SET statement_timeout", () => {
    const sql = "INSERT INTO archive (id) SELECT id FROM users WHERE deleted = true;";
    const ctx = makeContext(sql);
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("INSERT ... SELECT");
  });

  test("does not fire when SET statement_timeout precedes UPDATE", () => {
    const sql = `SET statement_timeout = '30s';
UPDATE users SET name = 'x' WHERE id = 1;`;
    const ctx = makeContext(sql);
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire when SET LOCAL statement_timeout precedes DELETE", () => {
    const sql = `SET LOCAL statement_timeout = '30s';
DELETE FROM users WHERE active = false;`;
    const ctx = makeContext(sql);
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on INSERT ... VALUES (no table scan)", () => {
    const ctx = makeContext("INSERT INTO users (name) VALUES ('alice');");
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on SELECT", () => {
    const ctx = makeContext("SELECT * FROM users;");
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on CREATE TABLE", () => {
    const ctx = makeContext("CREATE TABLE t (id int);");
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on ALTER TABLE (covered by SA013)", () => {
    const ctx = makeContext("ALTER TABLE users ADD COLUMN bio text;");
    const findings = SA026.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on multiple DML after timeout set once", () => {
    const sql = `SET statement_timeout = '30s';
UPDATE users SET a = 1;
UPDATE users SET b = 2;`;
    const ctx = makeContext(sql);
    const findings = SA026.check(ctx);
    // statement_timeout was set before both
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about SET statement_timeout", () => {
    const ctx = makeContext("UPDATE t SET c = 1;");
    const findings = SA026.check(ctx);
    expect(findings[0]!.suggestion).toContain("statement_timeout");
  });

  test("includes table name in message", () => {
    const ctx = makeContext("DELETE FROM orders WHERE id > 100;");
    const findings = SA026.check(ctx);
    expect(findings[0]!.message).toContain("orders");
  });
});

// ─── Cross-cutting tests (SA022-SA026) ────────────────────────────────

describe("cross-cutting: SA022-SA026 empty inputs", () => {
  test("empty SQL produces no findings for any new rule", () => {
    const ctx = makeContext("SELECT 1;");
    const newRules = [SA022, SA023, SA024, SA025, SA026];
    for (const rule of newRules) {
      const findings = rule.check(ctx);
      expect(findings).toHaveLength(0);
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
    const newRules = [SA022, SA023, SA024, SA025, SA026];
    for (const rule of newRules) {
      const findings = rule.check(ctx);
      expect(findings).toHaveLength(0);
    }
  });
});
