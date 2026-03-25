// src/analysis/types.ts — Core types for the sqlever analysis engine
//
// Defines the Rule interface, AnalysisContext, Finding, and Severity types
// per SPEC section 5.1. These types are the contract between the analyzer
// entry point, the rule registry, and individual rule implementations.

// ---------------------------------------------------------------------------
// libpg-query AST node types
// ---------------------------------------------------------------------------

/**
 * Recursive AST node type for libpg-query output.
 *
 * Uses `Record<string, unknown>` at the type level. Rules access
 * statement-specific fields via the `node()` helper which returns
 * a permissive record type for deep property access.
 */
export type PgNode = Record<string, unknown>;

/** Shared empty object returned by node() for nullish inputs. */
const EMPTY: Record<string, unknown> = Object.freeze({}) as Record<string, unknown>;

/**
 * Narrow an unknown AST value to a record for property access.
 *
 * Usage in rules: `const alterStmt = node(stmt.AlterTableStmt)`
 * then access `alterStmt.objtype`, `alterStmt.cmds`, etc.
 *
 * Returns a frozen EMPTY sentinel for nullish inputs, avoiding
 * a fresh allocation per call.
 */
export function node(value: unknown): Record<string, unknown> {
  return (value ?? EMPTY) as Record<string, unknown>;
}

/**
 * Narrow an unknown AST value to an array of records.
 *
 * Usage in rules: `for (const cmd of nodes(alterStmt.cmds))`
 *
 * Accepts a generic parameter to avoid double-casts in callers:
 *   `nodes<StringNode>(items)` instead of `nodes(items) as unknown as StringNode[]`
 */
export function nodes<T = Record<string, unknown>>(value: unknown): T[] {
  return (value ?? []) as T[];
}

/**
 * A DefElem node from the libpg-query AST (used for options/params lists).
 */
export interface DefElem {
  DefElem?: {
    defname?: string;
    arg?: PgNode;
    defaction?: number;
    location?: number;
  };
}

/**
 * A String node from the libpg-query AST (name list elements, etc.).
 */
export interface StringNode {
  String?: {
    sval?: string;
  };
}

/**
 * A TypeName node from the libpg-query AST.
 */
export interface TypeName {
  names?: StringNode[];
  typmods?: PgNode[];
  typemod?: number;
  arrayBounds?: PgNode[];
  location?: number;
}

// ---------------------------------------------------------------------------
// Severity
// ---------------------------------------------------------------------------

/** Severity level for analysis findings. */
export type Severity = "error" | "warn" | "info";

// ---------------------------------------------------------------------------
// Finding
// ---------------------------------------------------------------------------

/** Source location for a finding. */
export interface FindingLocation {
  file: string;
  line: number;
  column: number;
  endLine?: number;
  endColumn?: number;
}

/** Alias for FindingLocation — used by rule implementations. */
export type Location = FindingLocation;

/** A single finding produced by a rule. */
export interface Finding {
  ruleId: string;
  /** Human-readable rule name, attached by the analyzer after rule execution. */
  ruleName?: string;
  severity: Severity;
  message: string;
  location: FindingLocation;
  suggestion?: string;
}

// ---------------------------------------------------------------------------
// AnalysisContext
// ---------------------------------------------------------------------------

/** Parsed SQL AST from libpg-query. */
export interface ParseResult {
  stmts: StmtEntry[];
}

/** A single statement entry from the parser. */
export interface StmtEntry {
  stmt: PgNode;
  stmt_location?: number;
  stmt_len?: number;
}

/** Configuration for the analysis engine. */
export interface AnalysisConfig {
  /** Rules to skip globally. */
  skip?: string[];
  /** Treat warnings as errors. */
  errorOnWarn?: boolean;
  /** Max affected rows threshold for batch-related rules. */
  maxAffectedRows?: number;
  /** Minimum PG version migrations must support. */
  pgVersion?: number;
  /** Per-rule configuration. */
  rules?: Record<string, RuleConfig>;
  /** Per-file overrides keyed by file path. */
  overrides?: Record<string, FileOverride>;
}

/** Per-rule configuration. */
export interface RuleConfig {
  /** Override max_affected_rows for this rule. */
  maxAffectedRows?: number;
  /** Severity override. */
  severity?: Severity | "off";
}

/** Per-file override. */
export interface FileOverride {
  /** Rules to skip for this file. */
  skip?: string[];
}

/** Minimal database client interface for connected/hybrid rules. */
export interface DatabaseClient {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

/** Context passed to every rule's check() method. */
export interface AnalysisContext {
  /** Parsed AST from libpg-query. */
  ast: ParseResult;
  /** Original SQL text (after preprocessor strips metacommands). */
  rawSql: string;
  /** Path to the file being analyzed. */
  filePath: string;
  /** Minimum PG version to target. */
  pgVersion: number;
  /** Analysis configuration. */
  config: AnalysisConfig;
  /** Database client, present only for connected/hybrid rules with active connection. */
  db?: DatabaseClient;
  /** Whether this file is in a revert context (e.g. under revert/ in a sqitch project). */
  isRevertContext?: boolean;
  /** Whether the script runs inside a transaction block (e.g. deploy mode without auto-commit). */
  isTransactional?: boolean;
}

// ---------------------------------------------------------------------------
// Rule
// ---------------------------------------------------------------------------

/** Rule type classification. */
export type RuleType = "static" | "connected" | "hybrid";

/**
 * Rule interface — the contract every analysis rule must implement.
 *
 * Rules receive an AnalysisContext and return an array of findings.
 * Suppression filtering happens in the analyzer entry point AFTER
 * rules return findings — rules do not see or reason about suppressions.
 */
export interface Rule {
  /** Unique rule identifier, e.g., "SA001". */
  id: string;
  /** Human-readable kebab-case name, e.g., "add-column-not-null". */
  name: string;
  /** Default severity level. */
  severity: Severity;
  /** Whether this rule is static, connected, or hybrid. */
  type: RuleType;
  /** If true, the rule is off by default and must be explicitly enabled in config. */
  defaultOff?: boolean;
  /** Run the rule against the given context, returning any findings. */
  check(context: AnalysisContext): Finding[];
}

// ---------------------------------------------------------------------------
// AnalyzeOptions
// ---------------------------------------------------------------------------

/** Options passed to the Analyzer.analyze() entry point. */
export interface AnalyzeOptions {
  /** Analysis configuration (from sqlever.toml). */
  config?: AnalysisConfig;
  /** Database client for connected/hybrid rules. */
  db?: DatabaseClient;
  /** Minimum PG version to target. Default: 14. */
  pgVersion?: number;
  /** Whether to treat the file as a revert script (affects SA007 etc.). */
  isRevert?: boolean;
  /** Whether the script runs inside a transaction block (deploy mode without auto-commit). */
  isTransactional?: boolean;
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/**
 * Precomputed line-start offsets for offsetToLocation. Cached for the
 * last-seen rawSql string so that repeated calls within the same file
 * (typical during analysis) use an O(log N) binary search instead of
 * an O(N) linear scan per call.
 */
let cachedLineStartsSql: string | undefined;
let cachedLineStarts: number[] = [];

function getLineStarts(rawSql: string): number[] {
  if (rawSql === cachedLineStartsSql) return cachedLineStarts;
  const starts = [0]; // line 1 starts at offset 0
  for (let i = 0; i < rawSql.length; i++) {
    if (rawSql[i] === "\n") {
      starts.push(i + 1);
    }
  }
  cachedLineStartsSql = rawSql;
  cachedLineStarts = starts;
  return starts;
}

/**
 * Convert a byte offset in the source SQL to a 1-based line and column.
 *
 * Uses a precomputed line-start index with binary search for O(log N)
 * per call instead of O(N).
 */
export function offsetToLocation(
  rawSql: string,
  byteOffset: number,
  filePath: string,
): Location {
  const starts = getLineStarts(rawSql);
  const offset = Math.min(byteOffset, rawSql.length);

  // Binary search for the line containing this offset
  let lo = 0;
  let hi = starts.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >>> 1;
    if (starts[mid]! <= offset) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }

  const line = lo + 1; // 1-based
  const col = offset - starts[lo]! + 1; // 1-based
  return { file: filePath, line, column: col };
}

/**
 * Extract the type name string from a libpg-query TypeName node.
 * Returns the last name part (e.g. "varchar", "int4", "text").
 */
export function extractTypeName(typeName: TypeName | PgNode | undefined): string | null {
  if (!typeName?.names) return null;
  const names = typeName.names as StringNode[];
  const last = names[names.length - 1];
  return last?.String?.sval ?? null;
}

/**
 * Extract type modifiers (e.g. length for varchar, precision/scale for numeric).
 */
export function extractTypeMods(typeName: TypeName | PgNode | undefined): number[] {
  if (!typeName?.typmods) return [];
  return (typeName.typmods as PgNode[])
    .map((m) => node(node(m).A_Const).ival)
    .map((ival) => node(ival).ival)
    .filter((v): v is number => typeof v === "number");
}

/**
 * Get the fully-qualified type name for display purposes.
 * Skips "pg_catalog" schema prefix.
 */
export function displayTypeName(typeName: TypeName | PgNode | undefined): string {
  if (!typeName?.names) return "unknown";
  const names = (typeName.names as StringNode[])
    .map((n) => n?.String?.sval)
    .filter((s): s is string => !!s)
    .filter((s) => s !== "pg_catalog");
  const base = names.join(".");
  const mods = extractTypeMods(typeName);
  if (mods.length > 0) {
    return `${base}(${mods.join(",")})`;
  }
  return base;
}
