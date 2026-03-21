// src/ai/explain.ts — AI-powered migration explanation engine
//
// Parses migration SQL via libpg-query, builds structured context
// (tables affected, DDL types, lock types, estimated risk), and sends
// to an LLM for plain-English explanation.
//
// Implements SPEC Section 5.7 and GitHub issue #106.
// Design principle DD10: No hidden network calls — LLM is only
// contacted when `sqlever explain` is explicitly invoked.

import { parseSync, loadModule } from "libpg-query";
import { preprocessSql } from "../analysis/preprocessor";
import type { ParseResult, StmtEntry } from "../analysis/types";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Supported LLM providers. */
export type LLMProvider = "openai" | "anthropic" | "ollama";

/** Configuration for the explain engine. */
export interface ExplainConfig {
  /** LLM provider. */
  provider: LLMProvider;
  /** Model name (provider-specific). */
  model: string;
  /** API key (not needed for Ollama). */
  apiKey?: string;
  /** Ollama base URL (default: http://localhost:11434). */
  ollamaBaseUrl?: string;
}

/** A DDL operation extracted from a migration. */
export interface DDLOperation {
  /** The DDL type (e.g., "CREATE TABLE", "ALTER TABLE", "CREATE INDEX"). */
  type: string;
  /** The object name affected. */
  objectName: string;
  /** Sub-operations (e.g., ADD COLUMN, DROP CONSTRAINT). */
  subOperations: string[];
}

/** Lock information for a DDL operation. */
export interface LockInfo {
  /** Lock type (e.g., "AccessExclusiveLock", "ShareLock"). */
  lockType: string;
  /** Whether this lock blocks reads. */
  blocksReads: boolean;
  /** Whether this lock blocks writes. */
  blocksWrites: boolean;
}

/** Risk level of a migration. */
export type RiskLevel = "low" | "medium" | "high" | "critical";

/** Structured context built from parsing the migration SQL. */
export interface MigrationContext {
  /** Tables affected by the migration. */
  tablesAffected: string[];
  /** DDL operations. */
  operations: DDLOperation[];
  /** Lock information per operation. */
  locks: LockInfo[];
  /** Estimated risk level. */
  riskLevel: RiskLevel;
  /** Risk factors found. */
  riskFactors: string[];
  /** The original SQL. */
  sql: string;
}

/** Result of the explain command. */
export interface ExplainResult {
  /** The structured migration context. */
  context: MigrationContext;
  /** Plain-English explanation from the LLM. */
  explanation: string;
}

/** Response shape returned by LLM API calls. */
export interface LLMResponse {
  content: string;
}

// ---------------------------------------------------------------------------
// WASM loading
// ---------------------------------------------------------------------------

let wasmLoaded = false;

export async function ensureWasm(): Promise<void> {
  if (!wasmLoaded) {
    await loadModule();
    wasmLoaded = true;
  }
}

// ---------------------------------------------------------------------------
// SQL parsing and context extraction
// ---------------------------------------------------------------------------

/**
 * Lock type mapping for DDL operations.
 * Based on PostgreSQL documentation for lock types acquired by various DDL.
 */
const LOCK_MAP: Record<string, LockInfo> = {
  "CREATE TABLE": {
    lockType: "AccessExclusiveLock",
    blocksReads: false,
    blocksWrites: false,
  },
  "DROP TABLE": {
    lockType: "AccessExclusiveLock",
    blocksReads: true,
    blocksWrites: true,
  },
  "ALTER TABLE": {
    lockType: "AccessExclusiveLock",
    blocksReads: true,
    blocksWrites: true,
  },
  "CREATE INDEX": {
    lockType: "ShareLock",
    blocksReads: false,
    blocksWrites: true,
  },
  "CREATE INDEX CONCURRENTLY": {
    lockType: "ShareUpdateExclusiveLock",
    blocksReads: false,
    blocksWrites: false,
  },
  "DROP INDEX": {
    lockType: "AccessExclusiveLock",
    blocksReads: true,
    blocksWrites: true,
  },
  "CREATE FUNCTION": {
    lockType: "none",
    blocksReads: false,
    blocksWrites: false,
  },
  "CREATE VIEW": {
    lockType: "none",
    blocksReads: false,
    blocksWrites: false,
  },
  "CREATE TRIGGER": {
    lockType: "ShareRowExclusiveLock",
    blocksReads: false,
    blocksWrites: true,
  },
  "DROP TRIGGER": {
    lockType: "AccessExclusiveLock",
    blocksReads: true,
    blocksWrites: true,
  },
  "CREATE SEQUENCE": {
    lockType: "none",
    blocksReads: false,
    blocksWrites: false,
  },
  "CREATE TYPE": {
    lockType: "none",
    blocksReads: false,
    blocksWrites: false,
  },
  "CREATE SCHEMA": {
    lockType: "none",
    blocksReads: false,
    blocksWrites: false,
  },
  "ADD CONSTRAINT": {
    lockType: "AccessExclusiveLock",
    blocksReads: true,
    blocksWrites: true,
  },
  "VALIDATE CONSTRAINT": {
    lockType: "ShareUpdateExclusiveLock",
    blocksReads: false,
    blocksWrites: false,
  },
};

/** Risk factors and their associated risk level escalation. */
const RISK_ESCALATION: Array<{
  pattern: string;
  level: RiskLevel;
  factor: string;
}> = [
  {
    pattern: "DROP TABLE",
    level: "critical",
    factor: "Drops a table — data loss if no backup",
  },
  {
    pattern: "DROP COLUMN",
    level: "high",
    factor: "Drops a column — data loss",
  },
  {
    pattern: "ALTER COLUMN TYPE",
    level: "high",
    factor: "Changes column type — may cause full table rewrite with AccessExclusiveLock",
  },
  {
    pattern: "NOT NULL",
    level: "medium",
    factor: "Adds NOT NULL constraint — requires full table scan to validate",
  },
  {
    pattern: "CREATE INDEX",
    level: "medium",
    factor: "Creates index — blocks writes unless CONCURRENTLY",
  },
  {
    pattern: "CONCURRENTLY",
    level: "low",
    factor: "Uses CONCURRENTLY — non-blocking but slower",
  },
  {
    pattern: "ADD COLUMN",
    level: "low",
    factor: "Adds column — fast in PG 11+ with volatile defaults",
  },
];

/**
 * Extract the relation (table) name from a libpg-query RangeVar node.
 */
function extractRelationName(relation: Record<string, unknown>): string {
  const schema = relation.schemaname as string | undefined;
  const name = relation.relname as string | undefined;
  if (!name) return "unknown";
  return schema ? `${schema}.${name}` : name;
}

/**
 * Extract sub-operations from ALTER TABLE commands.
 */
function extractAlterSubOps(cmds: Array<Record<string, unknown>>): string[] {
  const subOps: string[] = [];
  for (const cmdEntry of cmds) {
    const cmd = cmdEntry.AlterTableCmd as Record<string, unknown> | undefined;
    if (!cmd) continue;

    const subtype = cmd.subtype as string;
    const colName = cmd.name as string | undefined;

    // For ADD COLUMN, the name is in def.ColumnDef.colname, not cmd.name
    const defColName = (cmd.def as Record<string, unknown> | undefined)
      ?.ColumnDef as Record<string, unknown> | undefined;
    const effectiveName = colName ?? (defColName?.colname as string | undefined);

    switch (subtype) {
      case "AT_AddColumn":
        subOps.push(`ADD COLUMN${effectiveName ? ` "${effectiveName}"` : ""}`);
        break;
      case "AT_DropColumn":
        subOps.push(`DROP COLUMN${effectiveName ? ` "${effectiveName}"` : ""}`);
        break;
      case "AT_AlterColumnType":
        subOps.push(`ALTER COLUMN TYPE${colName ? ` "${colName}"` : ""}`);
        break;
      case "AT_SetNotNull":
        subOps.push(`SET NOT NULL${colName ? ` "${colName}"` : ""}`);
        break;
      case "AT_DropNotNull":
        subOps.push(`DROP NOT NULL${colName ? ` "${colName}"` : ""}`);
        break;
      case "AT_AddConstraint":
        subOps.push("ADD CONSTRAINT");
        break;
      case "AT_DropConstraint":
        subOps.push("DROP CONSTRAINT");
        break;
      case "AT_SetDefault":
        subOps.push(`SET DEFAULT${colName ? ` "${colName}"` : ""}`);
        break;
      case "AT_DropDefault":
        subOps.push(`DROP DEFAULT${colName ? ` "${colName}"` : ""}`);
        break;
      case "AT_AddIndex":
        subOps.push("ADD INDEX");
        break;
      case "AT_ValidateConstraint":
        subOps.push("VALIDATE CONSTRAINT");
        break;
      case "AT_ColumnDefault":
        subOps.push(`COLUMN DEFAULT${colName ? ` "${colName}"` : ""}`);
        break;
      default:
        subOps.push(subtype ?? "UNKNOWN");
    }
  }
  return subOps;
}

/**
 * Parse a single statement entry and extract DDL operations.
 */
function parseStmtEntry(entry: StmtEntry): DDLOperation | null {
  const stmt = entry.stmt;

  // CREATE TABLE
  if (stmt.CreateStmt) {
    const createStmt = stmt.CreateStmt as Record<string, unknown>;
    const relation = createStmt.relation as Record<string, unknown>;
    return {
      type: "CREATE TABLE",
      objectName: extractRelationName(relation),
      subOperations: [],
    };
  }

  // ALTER TABLE
  if (stmt.AlterTableStmt) {
    const alterStmt = stmt.AlterTableStmt as Record<string, unknown>;
    const relation = alterStmt.relation as Record<string, unknown>;
    const cmds = (alterStmt.cmds ?? []) as Array<Record<string, unknown>>;
    return {
      type: "ALTER TABLE",
      objectName: extractRelationName(relation),
      subOperations: extractAlterSubOps(cmds),
    };
  }

  // CREATE INDEX
  if (stmt.IndexStmt) {
    const indexStmt = stmt.IndexStmt as Record<string, unknown>;
    const idxName = (indexStmt.idxname as string) ?? "unnamed";
    const relation = indexStmt.relation as Record<string, unknown>;
    const concurrent = indexStmt.concurrent as boolean | undefined;
    const tableName = extractRelationName(relation);
    return {
      type: concurrent ? "CREATE INDEX CONCURRENTLY" : "CREATE INDEX",
      objectName: `${idxName} ON ${tableName}`,
      subOperations: [],
    };
  }

  // DROP TABLE
  if (stmt.DropStmt) {
    const dropStmt = stmt.DropStmt as Record<string, unknown>;
    const removeType = dropStmt.removeType as string;
    if (removeType === "OBJECT_TABLE") {
      const objects = (dropStmt.objects ?? []) as Array<unknown>;
      const names = objects.map((obj) => {
        if (Array.isArray(obj)) {
          return obj.map((n: Record<string, unknown>) => (n.String as Record<string, unknown>)?.sval ?? "").join(".");
        }
        return "unknown";
      });
      return {
        type: "DROP TABLE",
        objectName: names.join(", "),
        subOperations: [],
      };
    }
    if (removeType === "OBJECT_INDEX") {
      const objects = (dropStmt.objects ?? []) as Array<unknown>;
      const names = objects.map((obj) => {
        if (Array.isArray(obj)) {
          return obj.map((n: Record<string, unknown>) => (n.String as Record<string, unknown>)?.sval ?? "").join(".");
        }
        return "unknown";
      });
      return {
        type: "DROP INDEX",
        objectName: names.join(", "),
        subOperations: [],
      };
    }
    return null;
  }

  // CREATE FUNCTION / PROCEDURE
  if (stmt.CreateFunctionStmt) {
    const funcStmt = stmt.CreateFunctionStmt as Record<string, unknown>;
    const funcname = (funcStmt.funcname ?? []) as Array<Record<string, unknown>>;
    const name = funcname.map((n) => (n.String as Record<string, unknown>)?.sval ?? "").join(".");
    return {
      type: "CREATE FUNCTION",
      objectName: name,
      subOperations: [],
    };
  }

  // CREATE VIEW
  if (stmt.ViewStmt) {
    const viewStmt = stmt.ViewStmt as Record<string, unknown>;
    const view = viewStmt.view as Record<string, unknown>;
    return {
      type: "CREATE VIEW",
      objectName: extractRelationName(view),
      subOperations: [],
    };
  }

  // CREATE TRIGGER
  if (stmt.CreateTrigStmt) {
    const trigStmt = stmt.CreateTrigStmt as Record<string, unknown>;
    const trigName = (trigStmt.trigname as string) ?? "unnamed";
    const relation = trigStmt.relation as Record<string, unknown>;
    const tableName = extractRelationName(relation);
    return {
      type: "CREATE TRIGGER",
      objectName: `${trigName} ON ${tableName}`,
      subOperations: [],
    };
  }

  // CREATE SEQUENCE
  if (stmt.CreateSeqStmt) {
    const seqStmt = stmt.CreateSeqStmt as Record<string, unknown>;
    const seq = seqStmt.sequence as Record<string, unknown>;
    return {
      type: "CREATE SEQUENCE",
      objectName: extractRelationName(seq),
      subOperations: [],
    };
  }

  // CREATE SCHEMA
  if (stmt.CreateSchemaStmt) {
    const schemaStmt = stmt.CreateSchemaStmt as Record<string, unknown>;
    const schemaName = (schemaStmt.schemaname as string) ?? "unnamed";
    return {
      type: "CREATE SCHEMA",
      objectName: schemaName,
      subOperations: [],
    };
  }

  // CREATE TYPE
  if (stmt.CreateEnumStmt || stmt.CompositeTypeStmt) {
    const typeStmt = (stmt.CreateEnumStmt ?? stmt.CompositeTypeStmt) as Record<string, unknown>;
    const typeName = (typeStmt.typeName ?? []) as Array<Record<string, unknown>>;
    const name = typeName.map((n) => (n.String as Record<string, unknown>)?.sval ?? "").join(".");
    return {
      type: "CREATE TYPE",
      objectName: name,
      subOperations: [],
    };
  }

  return null;
}

/**
 * Extract tables affected from operations.
 */
function extractTablesAffected(operations: DDLOperation[]): string[] {
  const tables = new Set<string>();
  for (const op of operations) {
    // Extract the base table name (strip index/trigger "X ON Y" syntax)
    if (op.objectName.includes(" ON ")) {
      const parts = op.objectName.split(" ON ");
      tables.add(parts[1]!.trim());
    } else if (
      op.type === "CREATE TABLE" ||
      op.type === "ALTER TABLE" ||
      op.type === "DROP TABLE"
    ) {
      tables.add(op.objectName);
    }
  }
  return Array.from(tables);
}

/**
 * Compute risk level from operations.
 */
function computeRisk(operations: DDLOperation[]): {
  level: RiskLevel;
  factors: string[];
} {
  let level: RiskLevel = "low";
  const factors: string[] = [];
  const riskOrder: Record<RiskLevel, number> = {
    low: 0,
    medium: 1,
    high: 2,
    critical: 3,
  };

  for (const op of operations) {
    for (const escalation of RISK_ESCALATION) {
      const matchesType = op.type.includes(escalation.pattern);
      const matchesSubOp = op.subOperations.some((sub) =>
        sub.includes(escalation.pattern),
      );

      if (matchesType || matchesSubOp) {
        if (!factors.includes(escalation.factor)) {
          factors.push(escalation.factor);
        }
        if (riskOrder[escalation.level]! > riskOrder[level]!) {
          level = escalation.level;
        }
      }
    }
  }

  // If no factors found, add a default
  if (factors.length === 0) {
    factors.push("No significant risk factors detected");
  }

  return { level, factors };
}

/**
 * Get lock information for an operation.
 */
function getLockInfo(operation: DDLOperation): LockInfo {
  // Check for ALTER TABLE sub-operations that have specific lock types
  if (operation.type === "ALTER TABLE") {
    for (const subOp of operation.subOperations) {
      if (subOp === "VALIDATE CONSTRAINT") {
        return (
          LOCK_MAP["VALIDATE CONSTRAINT"] ?? {
            lockType: "AccessExclusiveLock",
            blocksReads: true,
            blocksWrites: true,
          }
        );
      }
    }
  }

  return (
    LOCK_MAP[operation.type] ?? {
      lockType: "unknown",
      blocksReads: false,
      blocksWrites: false,
    }
  );
}

/**
 * Parse migration SQL and build structured context.
 *
 * This is the pure analysis step — no LLM calls.
 */
export function buildMigrationContext(sql: string): MigrationContext {
  const { cleanedSql } = preprocessSql(sql);

  let ast: ParseResult;
  try {
    ast = parseSync(cleanedSql) as ParseResult;
  } catch {
    // Return minimal context if parsing fails
    return {
      tablesAffected: [],
      operations: [],
      locks: [],
      riskLevel: "medium",
      riskFactors: ["SQL could not be parsed — manual review recommended"],
      sql,
    };
  }

  const operations: DDLOperation[] = [];

  for (const entry of ast.stmts) {
    const op = parseStmtEntry(entry);
    if (op) {
      operations.push(op);
    }
  }

  const tablesAffected = extractTablesAffected(operations);
  const locks = operations.map(getLockInfo);
  const { level: riskLevel, factors: riskFactors } = computeRisk(operations);

  return {
    tablesAffected,
    operations,
    locks,
    riskLevel,
    riskFactors,
    sql,
  };
}

// ---------------------------------------------------------------------------
// LLM prompt building
// ---------------------------------------------------------------------------

/**
 * Build a prompt for the LLM from the migration context.
 */
export function buildPrompt(context: MigrationContext): string {
  const parts: string[] = [];

  parts.push(
    "You are a PostgreSQL migration expert. Analyze the following migration and provide:",
  );
  parts.push("1. A plain-English summary of what this migration does");
  parts.push(
    "2. A risk assessment covering potential issues, lock contention, and downtime impact",
  );
  parts.push("");
  parts.push("## Migration SQL");
  parts.push("```sql");
  parts.push(context.sql.trim());
  parts.push("```");
  parts.push("");

  if (context.operations.length > 0) {
    parts.push("## Detected Operations");
    for (let i = 0; i < context.operations.length; i++) {
      const op = context.operations[i]!;
      const lock = context.locks[i];
      parts.push(
        `- ${op.type} ${op.objectName}${op.subOperations.length > 0 ? ` (${op.subOperations.join(", ")})` : ""}${lock ? ` [Lock: ${lock.lockType}]` : ""}`,
      );
    }
    parts.push("");
  }

  if (context.tablesAffected.length > 0) {
    parts.push(`## Tables Affected: ${context.tablesAffected.join(", ")}`);
    parts.push("");
  }

  parts.push(`## Risk Level: ${context.riskLevel.toUpperCase()}`);
  parts.push("### Risk Factors");
  for (const factor of context.riskFactors) {
    parts.push(`- ${factor}`);
  }
  parts.push("");
  parts.push(
    "Provide a concise explanation followed by a risk assessment. " +
      "Focus on operational impact: will this block reads or writes? " +
      "Could it cause downtime? Are there safer alternatives?",
  );

  return parts.join("\n");
}

// ---------------------------------------------------------------------------
// LLM API calls (fetch-based, no SDK dependencies)
// ---------------------------------------------------------------------------

/** Default models per provider. */
export const DEFAULT_MODELS: Record<LLMProvider, string> = {
  openai: "gpt-4o",
  anthropic: "claude-sonnet-4-20250514",
  ollama: "llama3.2",
};

/** Default Ollama base URL. */
const DEFAULT_OLLAMA_URL = "http://localhost:11434";

/**
 * Call the OpenAI API.
 */
async function callOpenAI(
  prompt: string,
  config: ExplainConfig,
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
): Promise<LLMResponse> {
  if (!config.apiKey) {
    throw new Error(
      "OpenAI API key required. Set --api-key or OPENAI_API_KEY environment variable.",
    );
  }

  const response = await fetchFn("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${config.apiKey}`,
    },
    body: JSON.stringify({
      model: config.model,
      messages: [{ role: "user", content: prompt }],
      temperature: 0.3,
      max_tokens: 2000,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OpenAI API error (${response.status}): ${errorText}`);
  }

  const data = (await response.json()) as {
    choices: Array<{ message: { content: string } }>;
  };
  const content = data.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error("OpenAI returned an empty response");
  }

  return { content };
}

/**
 * Call the Anthropic API.
 */
async function callAnthropic(
  prompt: string,
  config: ExplainConfig,
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
): Promise<LLMResponse> {
  if (!config.apiKey) {
    throw new Error(
      "Anthropic API key required. Set --api-key or ANTHROPIC_API_KEY environment variable.",
    );
  }

  const response = await fetchFn("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": config.apiKey,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: config.model,
      max_tokens: 2000,
      messages: [{ role: "user", content: prompt }],
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Anthropic API error (${response.status}): ${errorText}`);
  }

  const data = (await response.json()) as {
    content: Array<{ type: string; text: string }>;
  };
  const textBlock = data.content?.find((b) => b.type === "text");
  if (!textBlock?.text) {
    throw new Error("Anthropic returned an empty response");
  }

  return { content: textBlock.text };
}

/**
 * Call the Ollama API (local).
 */
async function callOllama(
  prompt: string,
  config: ExplainConfig,
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
): Promise<LLMResponse> {
  const baseUrl = config.ollamaBaseUrl ?? DEFAULT_OLLAMA_URL;
  const response = await fetchFn(`${baseUrl}/api/generate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: config.model,
      prompt,
      stream: false,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Ollama API error (${response.status}): ${errorText}`);
  }

  const data = (await response.json()) as { response: string };
  if (!data.response) {
    throw new Error("Ollama returned an empty response");
  }

  return { content: data.response };
}

/**
 * Call the configured LLM provider.
 */
export async function callLLM(
  prompt: string,
  config: ExplainConfig,
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
): Promise<LLMResponse> {
  switch (config.provider) {
    case "openai":
      return callOpenAI(prompt, config, fetchFn);
    case "anthropic":
      return callAnthropic(prompt, config, fetchFn);
    case "ollama":
      return callOllama(prompt, config, fetchFn);
    default:
      throw new Error(
        `Unknown LLM provider: ${config.provider as string}. Supported: openai, anthropic, ollama.`,
      );
  }
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

/**
 * Format the explain result for terminal output.
 */
export function formatExplainOutput(result: ExplainResult): string {
  const parts: string[] = [];

  // Header
  parts.push("=== Migration Explanation ===");
  parts.push("");

  // Context summary
  if (result.context.tablesAffected.length > 0) {
    parts.push(
      `Tables affected: ${result.context.tablesAffected.join(", ")}`,
    );
  }

  if (result.context.operations.length > 0) {
    parts.push(
      `Operations: ${result.context.operations.map((op) => op.type).join(", ")}`,
    );
  }

  const blockingLocks = result.context.locks.filter(
    (l) => l.blocksReads || l.blocksWrites,
  );
  if (blockingLocks.length > 0) {
    const lockTypes = [...new Set(blockingLocks.map((l) => l.lockType))];
    parts.push(`Blocking locks: ${lockTypes.join(", ")}`);
  }

  parts.push(`Risk level: ${result.context.riskLevel.toUpperCase()}`);
  parts.push("");

  // Risk factors
  parts.push("--- Risk Factors ---");
  for (const factor of result.context.riskFactors) {
    parts.push(`  - ${factor}`);
  }
  parts.push("");

  // LLM explanation
  parts.push("--- Explanation ---");
  parts.push(result.explanation);
  parts.push("");

  return parts.join("\n");
}

// ---------------------------------------------------------------------------
// Main explain function
// ---------------------------------------------------------------------------

/**
 * Run the explain engine: parse SQL, build context, call LLM.
 *
 * @param sql The migration SQL to explain.
 * @param config LLM configuration.
 * @param fetchFn Optional fetch function override (for testing).
 */
export async function explain(
  sql: string,
  config: ExplainConfig,
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
): Promise<ExplainResult> {
  // 1. Build structured context
  const context = buildMigrationContext(sql);

  // 2. Build prompt
  const prompt = buildPrompt(context);

  // 3. Call LLM
  const response = await callLLM(prompt, config, fetchFn);

  return {
    context,
    explanation: response.content,
  };
}
