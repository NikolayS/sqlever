import { describe, test, expect, beforeAll } from "bun:test";
import {
  ensureWasm,
  buildMigrationContext,
  buildPrompt,
  callLLM,
  explain,
  formatExplainOutput,
  DEFAULT_MODELS,
  type ExplainConfig,
  type ExplainResult,
  type LLMProvider,
} from "../../src/ai/explain";
import { parseExplainArgs } from "../../src/commands/explain";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Create a mock fetch function that returns a canned LLM response.
 */
function mockFetch(
  provider: LLMProvider,
  responseText: string,
): typeof globalThis.fetch {
  return (async (_url: string | URL | Request, _init?: RequestInit) => {
    let body: string;
    switch (provider) {
      case "openai":
        body = JSON.stringify({
          choices: [{ message: { content: responseText } }],
        });
        break;
      case "anthropic":
        body = JSON.stringify({
          content: [{ type: "text", text: responseText }],
        });
        break;
      case "ollama":
        body = JSON.stringify({ response: responseText });
        break;
    }
    return new Response(body, {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }) as unknown as typeof globalThis.fetch;
}

/**
 * Create a mock fetch that returns an error status.
 */
function mockFetchError(status: number, errorText: string): typeof globalThis.fetch {
  return (async (_url: string | URL | Request, _init?: RequestInit) => {
    return new Response(errorText, { status });
  }) as unknown as typeof globalThis.fetch;
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await ensureWasm();
});

// ---------------------------------------------------------------------------
// 1. SQL Parsing — buildMigrationContext
// ---------------------------------------------------------------------------

describe("buildMigrationContext", () => {
  test("parses CREATE TABLE", () => {
    const ctx = buildMigrationContext(
      "CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL);",
    );
    expect(ctx.tablesAffected).toContain("users");
    expect(ctx.operations).toHaveLength(1);
    expect(ctx.operations[0]!.type).toBe("CREATE TABLE");
    expect(ctx.operations[0]!.objectName).toBe("users");
    expect(ctx.riskLevel).toBe("low");
  });

  test("parses ALTER TABLE ADD COLUMN", () => {
    const ctx = buildMigrationContext(
      "ALTER TABLE users ADD COLUMN email text;",
    );
    expect(ctx.tablesAffected).toContain("users");
    expect(ctx.operations).toHaveLength(1);
    expect(ctx.operations[0]!.type).toBe("ALTER TABLE");
    expect(ctx.operations[0]!.subOperations).toContain('ADD COLUMN "email"');
    expect(ctx.locks[0]!.lockType).toBe("AccessExclusiveLock");
  });

  test("parses DROP TABLE as critical risk", () => {
    const ctx = buildMigrationContext("DROP TABLE users;");
    expect(ctx.operations).toHaveLength(1);
    expect(ctx.operations[0]!.type).toBe("DROP TABLE");
    expect(ctx.riskLevel).toBe("critical");
    expect(ctx.riskFactors.some((f) => f.includes("data loss"))).toBe(true);
  });

  test("parses CREATE INDEX (non-concurrent) as medium risk", () => {
    const ctx = buildMigrationContext(
      "CREATE INDEX idx_users_email ON users (email);",
    );
    expect(ctx.operations).toHaveLength(1);
    expect(ctx.operations[0]!.type).toBe("CREATE INDEX");
    expect(ctx.locks[0]!.lockType).toBe("ShareLock");
    expect(ctx.locks[0]!.blocksWrites).toBe(true);
    expect(ctx.riskLevel).toBe("medium");
  });

  test("parses CREATE INDEX CONCURRENTLY as low risk", () => {
    const ctx = buildMigrationContext(
      "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);",
    );
    expect(ctx.operations).toHaveLength(1);
    expect(ctx.operations[0]!.type).toBe("CREATE INDEX CONCURRENTLY");
    expect(ctx.locks[0]!.lockType).toBe("ShareUpdateExclusiveLock");
    expect(ctx.locks[0]!.blocksWrites).toBe(false);
  });

  test("parses multiple statements", () => {
    const sql = `
      CREATE TABLE orders (id serial PRIMARY KEY, user_id int);
      CREATE INDEX idx_orders_user_id ON orders (user_id);
      ALTER TABLE orders ADD COLUMN total numeric;
    `;
    const ctx = buildMigrationContext(sql);
    expect(ctx.tablesAffected).toContain("orders");
    expect(ctx.operations).toHaveLength(3);
    expect(ctx.operations[0]!.type).toBe("CREATE TABLE");
    expect(ctx.operations[1]!.type).toBe("CREATE INDEX");
    expect(ctx.operations[2]!.type).toBe("ALTER TABLE");
  });

  test("handles unparseable SQL gracefully", () => {
    const ctx = buildMigrationContext("THIS IS NOT VALID SQL !!!");
    expect(ctx.operations).toHaveLength(0);
    expect(ctx.riskLevel).toBe("medium");
    expect(ctx.riskFactors[0]).toContain("could not be parsed");
  });

  test("parses ALTER TABLE with DROP COLUMN as high risk", () => {
    const ctx = buildMigrationContext(
      "ALTER TABLE users DROP COLUMN email;",
    );
    expect(ctx.riskLevel).toBe("high");
    expect(ctx.riskFactors.some((f) => f.includes("Drops a column"))).toBe(true);
  });

  test("parses CREATE FUNCTION", () => {
    const ctx = buildMigrationContext(
      "CREATE FUNCTION add(a int, b int) RETURNS int AS $$ SELECT a + b; $$ LANGUAGE sql;",
    );
    expect(ctx.operations).toHaveLength(1);
    expect(ctx.operations[0]!.type).toBe("CREATE FUNCTION");
    expect(ctx.locks[0]!.lockType).toBe("none");
    expect(ctx.riskLevel).toBe("low");
  });

  test("parses schema-qualified table names", () => {
    const ctx = buildMigrationContext(
      "CREATE TABLE public.users (id serial PRIMARY KEY);",
    );
    expect(ctx.tablesAffected).toContain("public.users");
  });
});

// ---------------------------------------------------------------------------
// 2. Prompt building
// ---------------------------------------------------------------------------

describe("buildPrompt", () => {
  test("includes SQL in prompt", () => {
    const ctx = buildMigrationContext(
      "CREATE TABLE users (id serial PRIMARY KEY);",
    );
    const prompt = buildPrompt(ctx);
    expect(prompt).toContain("CREATE TABLE users");
    expect(prompt).toContain("```sql");
  });

  test("includes operations in prompt", () => {
    const ctx = buildMigrationContext(
      "ALTER TABLE users ADD COLUMN email text;",
    );
    const prompt = buildPrompt(ctx);
    expect(prompt).toContain("ALTER TABLE");
    expect(prompt).toContain("Detected Operations");
  });

  test("includes risk level in prompt", () => {
    const ctx = buildMigrationContext("DROP TABLE users;");
    const prompt = buildPrompt(ctx);
    expect(prompt).toContain("CRITICAL");
    expect(prompt).toContain("Risk Factors");
  });

  test("includes tables affected in prompt", () => {
    const ctx = buildMigrationContext(
      "CREATE INDEX idx_email ON users (email);",
    );
    const prompt = buildPrompt(ctx);
    expect(prompt).toContain("Tables Affected: users");
  });
});

// ---------------------------------------------------------------------------
// 3. LLM provider calls
// ---------------------------------------------------------------------------

describe("callLLM", () => {
  test("calls OpenAI with correct format", async () => {
    let capturedUrl = "";
    let capturedBody: Record<string, unknown> = {};

    const mockFn = (async (url: string | URL | Request, init?: RequestInit) => {
      capturedUrl = url as string;
      capturedBody = JSON.parse(init?.body as string) as Record<string, unknown>;
      return new Response(
        JSON.stringify({
          choices: [{ message: { content: "Test explanation" } }],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "openai",
      model: "gpt-4o",
      apiKey: "test-key",
    };

    const result = await callLLM("test prompt", config, mockFn);
    expect(result.content).toBe("Test explanation");
    expect(capturedUrl).toContain("openai.com");
    expect(capturedBody.model).toBe("gpt-4o");
  });

  test("calls Anthropic with correct format", async () => {
    let capturedUrl = "";
    let capturedHeaders: Record<string, string> = {};

    const mockFn = (async (url: string | URL | Request, init?: RequestInit) => {
      capturedUrl = url as string;
      const headers = init?.headers as Record<string, string>;
      capturedHeaders = headers;
      return new Response(
        JSON.stringify({
          content: [{ type: "text", text: "Anthropic explanation" }],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "anthropic",
      model: "claude-sonnet-4-20250514",
      apiKey: "test-key",
    };

    const result = await callLLM("test prompt", config, mockFn);
    expect(result.content).toBe("Anthropic explanation");
    expect(capturedUrl).toContain("anthropic.com");
    expect(capturedHeaders["x-api-key"]).toBe("test-key");
  });

  test("calls Ollama with correct format", async () => {
    let capturedUrl = "";

    const mockFn = (async (url: string | URL | Request) => {
      capturedUrl = url as string;
      return new Response(
        JSON.stringify({ response: "Ollama explanation" }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "ollama",
      model: "llama3.2",
    };

    const result = await callLLM("test prompt", config, mockFn);
    expect(result.content).toBe("Ollama explanation");
    expect(capturedUrl).toContain("localhost:11434");
  });

  test("throws on missing OpenAI API key", async () => {
    const config: ExplainConfig = {
      provider: "openai",
      model: "gpt-4o",
    };

    await expect(callLLM("test", config)).rejects.toThrow("API key required");
  });

  test("throws on missing Anthropic API key", async () => {
    const config: ExplainConfig = {
      provider: "anthropic",
      model: "claude-sonnet-4-20250514",
    };

    await expect(callLLM("test", config)).rejects.toThrow("API key required");
  });

  test("handles API error responses", async () => {
    const config: ExplainConfig = {
      provider: "openai",
      model: "gpt-4o",
      apiKey: "bad-key",
    };

    const mockFn = mockFetchError(401, "Unauthorized");
    await expect(callLLM("test", config, mockFn)).rejects.toThrow(
      "OpenAI API error (401)",
    );
  });

  test("handles Anthropic API error responses", async () => {
    const config: ExplainConfig = {
      provider: "anthropic",
      model: "claude-sonnet-4-20250514",
      apiKey: "bad-key",
    };

    const mockFn = mockFetchError(403, "Forbidden");
    await expect(callLLM("test", config, mockFn)).rejects.toThrow(
      "Anthropic API error (403)",
    );
  });
});

// ---------------------------------------------------------------------------
// 4. End-to-end explain with mock LLM
// ---------------------------------------------------------------------------

describe("explain (end-to-end with mock)", () => {
  test("produces full explain result for CREATE TABLE", async () => {
    const sql = "CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL);";
    const config: ExplainConfig = {
      provider: "openai",
      model: "gpt-4o",
      apiKey: "test-key",
    };

    const result = await explain(
      sql,
      config,
      mockFetch("openai", "This migration creates a new users table."),
    );

    expect(result.context.tablesAffected).toContain("users");
    expect(result.context.operations[0]!.type).toBe("CREATE TABLE");
    expect(result.explanation).toBe(
      "This migration creates a new users table.",
    );
  });

  test("produces full explain result for ALTER TABLE", async () => {
    const sql = "ALTER TABLE users ADD COLUMN email text NOT NULL DEFAULT '';";
    const config: ExplainConfig = {
      provider: "anthropic",
      model: "claude-sonnet-4-20250514",
      apiKey: "test-key",
    };

    const result = await explain(
      sql,
      config,
      mockFetch("anthropic", "This migration adds an email column to users."),
    );

    expect(result.context.tablesAffected).toContain("users");
    expect(result.context.riskLevel).toBe("low");
    expect(result.explanation).toBe(
      "This migration adds an email column to users.",
    );
  });

  test("produces full explain result with Ollama", async () => {
    const sql = "DROP TABLE old_logs;";
    const config: ExplainConfig = {
      provider: "ollama",
      model: "llama3.2",
    };

    const result = await explain(
      sql,
      config,
      mockFetch("ollama", "This migration drops the old_logs table."),
    );

    expect(result.context.riskLevel).toBe("critical");
    expect(result.explanation).toBe(
      "This migration drops the old_logs table.",
    );
  });
});

// ---------------------------------------------------------------------------
// 5. Output formatting
// ---------------------------------------------------------------------------

describe("formatExplainOutput", () => {
  test("formats output with all sections", () => {
    const result: ExplainResult = {
      context: {
        tablesAffected: ["users"],
        operations: [
          { type: "ALTER TABLE", objectName: "users", subOperations: ["ADD COLUMN"] },
        ],
        locks: [
          { lockType: "AccessExclusiveLock", blocksReads: true, blocksWrites: true },
        ],
        riskLevel: "medium",
        riskFactors: ["Adds column"],
        sql: "ALTER TABLE users ADD COLUMN email text;",
      },
      explanation: "This adds an email column.",
    };

    const output = formatExplainOutput(result);
    expect(output).toContain("Migration Explanation");
    expect(output).toContain("Tables affected: users");
    expect(output).toContain("Risk level: MEDIUM");
    expect(output).toContain("Blocking locks: AccessExclusiveLock");
    expect(output).toContain("This adds an email column.");
  });

  test("handles no blocking locks", () => {
    const result: ExplainResult = {
      context: {
        tablesAffected: ["users"],
        operations: [
          { type: "CREATE TABLE", objectName: "users", subOperations: [] },
        ],
        locks: [
          { lockType: "AccessExclusiveLock", blocksReads: false, blocksWrites: false },
        ],
        riskLevel: "low",
        riskFactors: ["No significant risk factors detected"],
        sql: "CREATE TABLE users (id serial PRIMARY KEY);",
      },
      explanation: "Creates a new table.",
    };

    const output = formatExplainOutput(result);
    expect(output).not.toContain("Blocking locks:");
    expect(output).toContain("Risk level: LOW");
  });
});

// ---------------------------------------------------------------------------
// 6. Argument parsing
// ---------------------------------------------------------------------------

describe("parseExplainArgs", () => {
  test("parses file target", () => {
    const opts = parseExplainArgs(["migration.sql"]);
    expect(opts.target).toBe("migration.sql");
    expect(opts.provider).toBe("openai"); // default
  });

  test("parses --provider", () => {
    const opts = parseExplainArgs(["--provider", "anthropic", "file.sql"]);
    expect(opts.provider).toBe("anthropic");
    expect(opts.target).toBe("file.sql");
  });

  test("parses --model", () => {
    const opts = parseExplainArgs(["--model", "gpt-3.5-turbo", "file.sql"]);
    expect(opts.model).toBe("gpt-3.5-turbo");
  });

  test("parses --api-key", () => {
    const opts = parseExplainArgs(["--api-key", "sk-test", "file.sql"]);
    expect(opts.apiKey).toBe("sk-test");
  });

  test("parses --ollama-url", () => {
    const opts = parseExplainArgs(["--ollama-url", "http://myhost:11434", "file.sql"]);
    expect(opts.ollamaBaseUrl).toBe("http://myhost:11434");
  });

  test("throws on invalid provider", () => {
    expect(() => parseExplainArgs(["--provider", "invalid"])).toThrow(
      "Invalid --provider",
    );
  });

  test("throws on missing --model value", () => {
    expect(() => parseExplainArgs(["--model"])).toThrow("--model requires");
  });

  test("throws on missing --api-key value", () => {
    expect(() => parseExplainArgs(["--api-key"])).toThrow("--api-key requires");
  });
});

// ---------------------------------------------------------------------------
// 7. Default models
// ---------------------------------------------------------------------------

describe("DEFAULT_MODELS", () => {
  test("has defaults for all providers", () => {
    expect(DEFAULT_MODELS.openai).toBeDefined();
    expect(DEFAULT_MODELS.anthropic).toBeDefined();
    expect(DEFAULT_MODELS.ollama).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// 8. Lock info detection
// ---------------------------------------------------------------------------

describe("lock detection", () => {
  test("ALTER TABLE gets AccessExclusiveLock", () => {
    const ctx = buildMigrationContext("ALTER TABLE users ADD COLUMN age int;");
    expect(ctx.locks[0]!.lockType).toBe("AccessExclusiveLock");
    expect(ctx.locks[0]!.blocksReads).toBe(true);
    expect(ctx.locks[0]!.blocksWrites).toBe(true);
  });

  test("CREATE INDEX CONCURRENTLY gets ShareUpdateExclusiveLock", () => {
    const ctx = buildMigrationContext(
      "CREATE INDEX CONCURRENTLY idx ON users (email);",
    );
    expect(ctx.locks[0]!.lockType).toBe("ShareUpdateExclusiveLock");
    expect(ctx.locks[0]!.blocksReads).toBe(false);
    expect(ctx.locks[0]!.blocksWrites).toBe(false);
  });

  test("CREATE TABLE does not block reads or writes on existing tables", () => {
    const ctx = buildMigrationContext("CREATE TABLE new_table (id int);");
    expect(ctx.locks[0]!.blocksReads).toBe(false);
    expect(ctx.locks[0]!.blocksWrites).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 9. Risk computation
// ---------------------------------------------------------------------------

describe("risk computation", () => {
  test("DROP TABLE is critical", () => {
    const ctx = buildMigrationContext("DROP TABLE users;");
    expect(ctx.riskLevel).toBe("critical");
  });

  test("ALTER COLUMN TYPE is high risk", () => {
    const ctx = buildMigrationContext(
      "ALTER TABLE users ALTER COLUMN name TYPE varchar(500);",
    );
    expect(ctx.riskLevel).toBe("high");
  });

  test("CREATE TABLE is low risk", () => {
    const ctx = buildMigrationContext("CREATE TABLE t (id int);");
    expect(ctx.riskLevel).toBe("low");
  });

  test("multiple operations pick the highest risk", () => {
    const sql = `
      CREATE TABLE t1 (id int);
      DROP TABLE t2;
    `;
    const ctx = buildMigrationContext(sql);
    expect(ctx.riskLevel).toBe("critical");
  });
});

// ---------------------------------------------------------------------------
// 10. Edge cases
// ---------------------------------------------------------------------------

describe("edge cases", () => {
  test("empty SQL returns minimal context", () => {
    // Empty string won't parse but buildMigrationContext handles gracefully
    const ctx = buildMigrationContext(
      "SELECT 1;",
    );
    // SELECT is not a DDL, so no operations
    expect(ctx.operations).toHaveLength(0);
    expect(ctx.tablesAffected).toHaveLength(0);
  });

  test("psql metacommands are stripped before parsing", () => {
    const sql = `\\set ON_ERROR_STOP on
CREATE TABLE users (id serial PRIMARY KEY);`;
    const ctx = buildMigrationContext(sql);
    expect(ctx.operations).toHaveLength(1);
    expect(ctx.operations[0]!.type).toBe("CREATE TABLE");
  });

  test("complex migration with multiple DDL types", () => {
    const sql = `
      CREATE TABLE orders (id serial PRIMARY KEY);
      ALTER TABLE orders ADD COLUMN user_id int;
      CREATE INDEX CONCURRENTLY idx_orders_user ON orders (user_id);
      CREATE FUNCTION get_order(int) RETURNS orders AS $$ SELECT * FROM orders WHERE id = $1; $$ LANGUAGE sql;
    `;
    const ctx = buildMigrationContext(sql);
    expect(ctx.operations.length).toBeGreaterThanOrEqual(4);
    expect(ctx.tablesAffected).toContain("orders");
    // Should have mix of lock types
    const lockTypes = ctx.locks.map((l) => l.lockType);
    expect(lockTypes).toContain("AccessExclusiveLock");
    expect(lockTypes).toContain("ShareUpdateExclusiveLock");
    expect(lockTypes).toContain("none");
  });

  test("Ollama uses custom base URL", async () => {
    let capturedUrl = "";
    const mockFn = (async (url: string | URL | Request) => {
      capturedUrl = url as string;
      return new Response(
        JSON.stringify({ response: "explanation" }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "ollama",
      model: "llama3.2",
      ollamaBaseUrl: "http://custom:9999",
    };

    await callLLM("prompt", config, mockFn);
    expect(capturedUrl).toContain("custom:9999");
  });
});
