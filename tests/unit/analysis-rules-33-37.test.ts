/**
 * Tests for analysis rules SA033-SA037.
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

import { SA033 } from "../../src/analysis/rules/SA033.js";
import { SA034 } from "../../src/analysis/rules/SA034.js";
import { SA035 } from "../../src/analysis/rules/SA035.js";
import { SA036 } from "../../src/analysis/rules/SA036.js";
import { SA037 } from "../../src/analysis/rules/SA037.js";
import { allRules, getRule } from "../../src/analysis/rules/index.js";
import type { AnalysisContext, DatabaseClient } from "../../src/analysis/types.js";

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

/** Stub database client for connected rule tests. */
const stubDb: DatabaseClient = {
  async query() {
    return { rows: [] };
  },
};

// Load WASM module before all tests
beforeAll(async () => {
  await loadModule();
});

// ─── Registry update ──────────────────────────────────────────────────

describe("rule registry (SA033-SA037)", () => {
  test("allRules contains 38 rules", () => {
    expect(allRules).toHaveLength(38);
  });

  test("getRule returns SA033-SA037 by ID", () => {
    for (const id of ["SA033", "SA034", "SA035", "SA036", "SA037"]) {
      expect(getRule(id)?.id).toBe(id);
    }
  });

  test("all new rules have correct interface fields", () => {
    const newRules = [SA033, SA034, SA035, SA036, SA037];
    for (const rule of newRules) {
      expect(rule.id).toMatch(/^SA\d{3}$/);
      expect(["error", "warn", "info"]).toContain(rule.severity);
      expect(["static", "connected", "hybrid"]).toContain(rule.type);
      expect(typeof rule.check).toBe("function");
    }
  });
});

// ─── SA033: Missing index on FK referencing column (connected) ────────

describe("SA033: Missing FK index (connected)", () => {
  test("metadata", () => {
    expect(SA033.id).toBe("SA033");
    expect(SA033.severity).toBe("info");
    expect(SA033.type).toBe("connected");
  });

  test("does not fire without db connection", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id);",
    );
    const findings = SA033.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on ADD FOREIGN KEY when db is present", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id);",
      { db: stubDb },
    );
    const findings = SA033.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.ruleId).toBe("SA033");
    expect(findings[0]!.severity).toBe("info");
    expect(findings[0]!.message).toContain("user_id");
    expect(findings[0]!.message).toContain("orders");
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on composite FK when db is present", () => {
    const ctx = makeContext(
      "ALTER TABLE order_items ADD CONSTRAINT fk_order_items FOREIGN KEY (order_id, product_id) REFERENCES orders(id, product_id);",
      { db: stubDb },
    );
    const findings = SA033.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.message).toContain("order_id, product_id");
  });

  test("fires on FK with NOT VALID when db is present", () => {
    const ctx = makeContext(
      "ALTER TABLE invoices ADD CONSTRAINT fk_invoices_customer FOREIGN KEY (customer_id) REFERENCES customers(id) NOT VALID;",
      { db: stubDb },
    );
    const findings = SA033.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.message).toContain("customer_id");
  });

  test("includes CREATE INDEX suggestion", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id);",
      { db: stubDb },
    );
    const findings = SA033.check(ctx);
    expect(findings[0]!.suggestion).toContain("CREATE INDEX CONCURRENTLY");
    expect(findings[0]!.suggestion).toContain("user_id");
  });

  test("handles schema-qualified table", () => {
    const ctx = makeContext(
      "ALTER TABLE app.payments ADD CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES app.orders(id);",
      { db: stubDb },
    );
    const findings = SA033.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.message).toContain("app.payments");
  });

  // Trigger fixtures (all with db)
  for (const file of getFixtureFiles("SA033", "trigger")) {
    test(`triggers on ${file} (with db)`, () => {
      const ctx = loadFixture("SA033", "trigger", file, { db: stubDb });
      const findings = SA033.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA033");
    });
  }

  // No-trigger fixtures (all without db -- connected rule skips)
  for (const file of getFixtureFiles("SA033", "no_trigger")) {
    test(`does not trigger on ${file} (no db)`, () => {
      const ctx = loadFixture("SA033", "no_trigger", file);
      const findings = SA033.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  // No-trigger fixtures with db (non-FK statements)
  for (const file of getFixtureFiles("SA033", "no_trigger")) {
    test(`does not trigger on ${file} (with db)`, () => {
      const ctx = loadFixture("SA033", "no_trigger", file, { db: stubDb });
      const findings = SA033.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("does not fire on CHECK constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age >= 0);",
      { db: stubDb },
    );
    const findings = SA033.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on UNIQUE constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email);",
      { db: stubDb },
    );
    const findings = SA033.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA034: CREATE INDEX CONCURRENTLY without indisvalid check ────────

describe("SA034: CIC without indisvalid check (info)", () => {
  test("metadata", () => {
    expect(SA034.id).toBe("SA034");
    expect(SA034.severity).toBe("info");
    expect(SA034.type).toBe("static");
  });

  test("fires on CREATE INDEX CONCURRENTLY", () => {
    const ctx = makeContext(
      "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);",
    );
    const findings = SA034.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA034");
    expect(findings[0]!.severity).toBe("info");
    expect(findings[0]!.message).toContain("idx_users_email");
    expect(findings[0]!.message).toContain("indisvalid");
  });

  test("does not fire on regular CREATE INDEX", () => {
    const ctx = makeContext("CREATE INDEX idx_users_email ON users (email);");
    const findings = SA034.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on CREATE UNIQUE INDEX CONCURRENTLY", () => {
    const ctx = makeContext(
      "CREATE UNIQUE INDEX CONCURRENTLY idx_users_email ON users (email);",
    );
    const findings = SA034.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("INVALID");
  });

  test("includes pg_index query in suggestion", () => {
    const ctx = makeContext(
      "CREATE INDEX CONCURRENTLY idx_test ON t (col);",
    );
    const findings = SA034.check(ctx);
    expect(findings[0]!.suggestion).toContain("pg_index");
    expect(findings[0]!.suggestion).toContain("indisvalid");
  });

  test("reports correct table name", () => {
    const ctx = makeContext(
      "CREATE INDEX CONCURRENTLY idx_orders_date ON orders (created_at);",
    );
    const findings = SA034.check(ctx);
    expect(findings[0]!.message).toContain("orders");
  });

  // Trigger fixtures
  for (const file of getFixtureFiles("SA034", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA034", "trigger", file);
      const findings = SA034.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA034");
    });
  }

  // No-trigger fixtures
  for (const file of getFixtureFiles("SA034", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA034", "no_trigger", file);
      const findings = SA034.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

});

// ─── SA035: DROP PRIMARY KEY breaks replica identity ──────────────────

describe("SA035: DROP PK breaks replica identity (warn)", () => {
  test("metadata", () => {
    expect(SA035.id).toBe("SA035");
    expect(SA035.severity).toBe("warn");
    expect(SA035.type).toBe("static");
  });

  test("fires on DROP CONSTRAINT with pkey suffix", () => {
    const ctx = makeContext(
      "ALTER TABLE users DROP CONSTRAINT users_pkey;",
    );
    const findings = SA035.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA035");
    expect(findings[0]!.severity).toBe("warn");
    expect(findings[0]!.message).toContain("users_pkey");
    expect(findings[0]!.message).toContain("logical replication");
  });

  test("fires on DROP CONSTRAINT with pk_ prefix", () => {
    const ctx = makeContext(
      "ALTER TABLE orders DROP CONSTRAINT pk_orders;",
    );
    const findings = SA035.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("pk_orders");
  });

  test("fires on DROP CONSTRAINT with _pk suffix", () => {
    const ctx = makeContext(
      "ALTER TABLE invoices DROP CONSTRAINT invoices_pk;",
    );
    const findings = SA035.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on DROP CONSTRAINT with primary in name", () => {
    const ctx = makeContext(
      "ALTER TABLE products DROP CONSTRAINT products_primary;",
    );
    const findings = SA035.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("does not fire on DROP FK constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE orders DROP CONSTRAINT fk_orders_user;",
    );
    const findings = SA035.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DROP CHECK constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users DROP CONSTRAINT chk_age;",
    );
    const findings = SA035.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes REPLICA IDENTITY suggestion", () => {
    const ctx = makeContext(
      "ALTER TABLE users DROP CONSTRAINT users_pkey;",
    );
    const findings = SA035.check(ctx);
    expect(findings[0]!.suggestion).toContain("REPLICA IDENTITY");
  });

  test("fires on CASCADE drop", () => {
    const ctx = makeContext(
      "ALTER TABLE accounts DROP CONSTRAINT accounts_pkey CASCADE;",
    );
    const findings = SA035.check(ctx);
    expect(findings).toHaveLength(1);
  });

  // Trigger fixtures
  for (const file of getFixtureFiles("SA035", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA035", "trigger", file);
      const findings = SA035.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA035");
    });
  }

  // No-trigger fixtures
  for (const file of getFixtureFiles("SA035", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA035", "no_trigger", file);
      const findings = SA035.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }
});

// ─── SA036: Large UPDATE/INSERT without batching (connected) ──────────

describe("SA036: Large UPDATE/INSERT without batching (connected)", () => {
  test("metadata", () => {
    expect(SA036.id).toBe("SA036");
    expect(SA036.severity).toBe("warn");
    expect(SA036.type).toBe("connected");
  });

  test("does not fire without db connection", () => {
    const ctx = makeContext("UPDATE users SET status = 'inactive';");
    const findings = SA036.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on UPDATE when db is present", () => {
    const ctx = makeContext(
      "UPDATE users SET status = 'inactive' WHERE last_login < '2020-01-01';",
      { db: stubDb },
    );
    const findings = SA036.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.ruleId).toBe("SA036");
    expect(findings[0]!.severity).toBe("warn");
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on INSERT ... SELECT when db is present", () => {
    const ctx = makeContext(
      "INSERT INTO archive_users (id, email) SELECT id, email FROM users WHERE deleted_at IS NOT NULL;",
      { db: stubDb },
    );
    const findings = SA036.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.message).toContain("INSERT");
    expect(findings[0]!.message).toContain("archive_users");
  });

  test("does not fire on INSERT ... VALUES", () => {
    const ctx = makeContext(
      "INSERT INTO users (email) VALUES ('test@example.com');",
      { db: stubDb },
    );
    const findings = SA036.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DELETE (handled by SA011)", () => {
    const ctx = makeContext(
      "DELETE FROM users WHERE id = 1;",
      { db: stubDb },
    );
    const findings = SA036.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("excludes DML inside CREATE FUNCTION", () => {
    const sql = `
      CREATE FUNCTION archive_users() RETURNS void AS $$
      BEGIN
        UPDATE users SET archived = true;
      END;
      $$ LANGUAGE plpgsql;
    `;
    const ctx = makeContext(sql, { db: stubDb });
    const findings = SA036.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("excludes DML inside DO block", () => {
    const sql = `
      DO $$
      BEGIN
        UPDATE users SET archived = true;
      END;
      $$;
    `;
    const ctx = makeContext(sql, { db: stubDb });
    const findings = SA036.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes threshold in message", () => {
    const ctx = makeContext("UPDATE users SET status = 'x';", {
      db: stubDb,
      config: { maxAffectedRows: 50_000 },
    });
    const findings = SA036.check(ctx);
    expect(findings[0]!.message).toContain("50000");
  });

  test("includes batching suggestion", () => {
    const ctx = makeContext("UPDATE users SET status = 'x';", {
      db: stubDb,
    });
    const findings = SA036.check(ctx);
    expect(findings[0]!.suggestion).toContain("sqlever batch");
  });

  test("handles schema-qualified table", () => {
    const ctx = makeContext(
      "UPDATE app.events SET processed = true WHERE processed = false;",
      { db: stubDb },
    );
    const findings = SA036.check(ctx);
    expect(findings[0]!.message).toContain("app.events");
  });

  // No-trigger fixtures (all without db)
  for (const file of getFixtureFiles("SA036", "no_trigger")) {
    test(`does not trigger on ${file} (no db)`, () => {
      const ctx = loadFixture("SA036", "no_trigger", file);
      const findings = SA036.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("does not fire on SELECT", () => {
    const ctx = makeContext("SELECT * FROM users;", { db: stubDb });
    const findings = SA036.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA037: Integer PK capacity warning (static) ──────────────────────

describe("SA037: Integer PK capacity (info)", () => {
  test("metadata", () => {
    expect(SA037.id).toBe("SA037");
    expect(SA037.severity).toBe("info");
    expect(SA037.type).toBe("static");
  });

  test("fires on int4 primary key", () => {
    const ctx = makeContext(
      "CREATE TABLE users (id int4 primary key, email text);",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA037");
    expect(findings[0]!.severity).toBe("info");
    expect(findings[0]!.message).toContain("int4");
    expect(findings[0]!.message).toContain("2.1 billion");
  });

  test("fires on integer primary key", () => {
    const ctx = makeContext(
      "CREATE TABLE orders (id integer primary key, total numeric(10,2));",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(1);
    // libpg-query normalizes "integer" to "int4" in the AST
    expect(findings[0]!.message).toContain("int4");
  });

  test("fires on int primary key", () => {
    const ctx = makeContext(
      "CREATE TABLE products (id int primary key, name text);",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(1);
    // libpg-query normalizes "int" to "int4" in the AST
    expect(findings[0]!.message).toContain("int4");
  });

  test("fires on serial primary key", () => {
    const ctx = makeContext(
      "CREATE TABLE events (id serial primary key, event_type text);",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("serial");
  });

  test("does not fire on bigint primary key", () => {
    const ctx = makeContext(
      "CREATE TABLE users (id int8 generated always as identity primary key, email text);",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on bigserial primary key", () => {
    const ctx = makeContext(
      "CREATE TABLE orders (id bigserial primary key, total numeric(10,2));",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on int4 non-PK column", () => {
    const ctx = makeContext(
      "CREATE TABLE orders (id int8 primary key, quantity int4 not null);",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on uuid primary key", () => {
    const ctx = makeContext(
      "CREATE TABLE sessions (id uuid primary key default gen_random_uuid(), user_id int8);",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on text primary key", () => {
    const ctx = makeContext(
      "CREATE TABLE config (key text primary key, value text);",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes bigint suggestion", () => {
    const ctx = makeContext(
      "CREATE TABLE t (id int4 primary key);",
    );
    const findings = SA037.check(ctx);
    expect(findings[0]!.suggestion).toContain("int8");
    expect(findings[0]!.suggestion).toContain("bigint");
  });

  test("handles schema-qualified table", () => {
    const ctx = makeContext(
      "CREATE TABLE app.metrics (id int4 primary key, value float8);",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("app.metrics");
  });

  test("does not fire on ALTER TABLE", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD COLUMN age int4;",
    );
    const findings = SA037.check(ctx);
    expect(findings).toHaveLength(0);
  });

  // Trigger fixtures
  for (const file of getFixtureFiles("SA037", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA037", "trigger", file);
      const findings = SA037.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA037");
    });
  }

  // No-trigger fixtures
  for (const file of getFixtureFiles("SA037", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA037", "no_trigger", file);
      const findings = SA037.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }
});
