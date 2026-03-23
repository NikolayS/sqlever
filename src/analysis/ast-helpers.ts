// src/analysis/ast-helpers.ts — Shared AST helpers for analysis rules
//
// Centralizes repeated patterns: volatile function detection,
// ALTER TABLE command iteration, and DropStmt name extraction.

import type { StringNode } from "./types.js";
import { node, nodes } from "./types.js";

// ---------------------------------------------------------------------------
// Volatile functions
// ---------------------------------------------------------------------------

/**
 * Known volatile functions that cause table rewrites on all PG versions.
 * Checked case-insensitively via lowercase lookup.
 */
export const VOLATILE_FUNCTIONS: ReadonlySet<string> = new Set([
  "random",
  "gen_random_uuid",
  "clock_timestamp",
  "txid_current",
  "timeofday",
  "uuid_generate_v1",
  "uuid_generate_v1mc",
  "uuid_generate_v4",
  "statement_timestamp",
  "setseed",
  "nextval",
  "currval",
  "lastval",
]);

/**
 * Recursively check if an AST expression node contains a volatile function call.
 * Returns the volatile function name (lowercase) if found, or null.
 */
export function containsVolatileFunction(n: unknown): string | null {
  if (!n || typeof n !== "object") return null;
  const nd = node(n);

  // Direct function call
  if (nd.FuncCall) {
    const funcNode = node(nd.FuncCall);
    const funcNames = nodes(funcNode.funcname);
    for (const fn of funcNames) {
      const name = node(fn).String as Record<string, unknown> | undefined;
      const sval = name ? String(node(name).sval ?? "").toLowerCase() : "";
      if (sval && VOLATILE_FUNCTIONS.has(sval)) {
        return sval;
      }
    }
    // Check function arguments recursively
    for (const arg of nodes(funcNode.args)) {
      const result = containsVolatileFunction(arg);
      if (result) return result;
    }
    return null;
  }

  // TypeCast wrapping a volatile function (e.g. random()::int)
  if (nd.TypeCast) {
    return containsVolatileFunction(node(nd.TypeCast).arg);
  }

  // Check nested expressions
  if (nd.A_Expr) {
    const expr = node(nd.A_Expr);
    const left = containsVolatileFunction(expr.lexpr);
    if (left) return left;
    return containsVolatileFunction(expr.rexpr);
  }

  // CoalesceExpr
  if (nd.CoalesceExpr) {
    for (const arg of nodes(node(nd.CoalesceExpr).args)) {
      const result = containsVolatileFunction(arg);
      if (result) return result;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// ALTER TABLE iteration
// ---------------------------------------------------------------------------

/** Callback signature for forEachAlterTableCmd. */
export interface AlterTableCmdContext {
  /** The AlterTableCmd node (already unwrapped via node()). */
  cmd: Record<string, unknown>;
  /** The AlterTableStmt node (already unwrapped via node()). */
  alterStmt: Record<string, unknown>;
  /** The parent StmtEntry for location extraction. */
  stmtLocation: number;
}

/**
 * Iterate over ALTER TABLE ... commands in a parsed AST.
 *
 * Handles the boilerplate of: checking for AlterTableStmt, filtering
 * OBJECT_TABLE, iterating cmds, and guarding the AlterTableCmd null check.
 */
export function forEachAlterTableCmd(
  ast: { stmts?: { stmt: Record<string, unknown>; stmt_location?: number }[] },
  callback: (ctx: AlterTableCmdContext) => void,
): void {
  if (!ast?.stmts) return;

  for (const stmtEntry of ast.stmts) {
    const stmt = stmtEntry.stmt;
    if (!stmt?.AlterTableStmt) continue;

    const alterStmt = node(stmt.AlterTableStmt);
    if (alterStmt.objtype !== "OBJECT_TABLE") continue;

    for (const cmdEntry of nodes(alterStmt.cmds)) {
      if (!cmdEntry.AlterTableCmd) continue;
      const cmd = node(cmdEntry.AlterTableCmd);

      callback({
        cmd,
        alterStmt,
        stmtLocation: stmtEntry.stmt_location ?? 0,
      });
    }
  }
}

// ---------------------------------------------------------------------------
// DropStmt name extraction
// ---------------------------------------------------------------------------

/**
 * Extract qualified object names from a DropStmt's objects list.
 *
 * DropStmt stores names as List nodes containing String nodes.
 * Returns an array of dot-joined qualified names (e.g. ["public.my_index"]).
 */
export function extractDropObjectNames(
  dropStmt: Record<string, unknown>,
): string[] {
  const names: string[] = [];
  for (const obj of nodes(dropStmt.objects)) {
    const list = node(obj).List;
    if (list) {
      const parts = nodes<StringNode>(node(list).items)
        .map((item) => item?.String?.sval)
        .filter(Boolean);
      names.push(parts.join("."));
    }
  }
  return names;
}
