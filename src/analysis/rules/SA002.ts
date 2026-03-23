/**
 * SA002: ADD COLUMN DEFAULT volatile on any PG version
 *
 * Severity: error
 * Type: static
 *
 * Detects ALTER TABLE ... ADD COLUMN with a volatile default expression.
 * Volatile defaults (e.g. random(), gen_random_uuid(), clock_timestamp(),
 * txid_current()) cause a full table rewrite on ALL PostgreSQL versions,
 * including PG 11+. The PG 11 optimization only applies to immutable/stable
 * defaults.
 *
 * Note: now() is STABLE (returns transaction start time), not volatile —
 * DEFAULT now() does NOT cause a rewrite on PG 11+.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";

/**
 * Known volatile functions that cause table rewrites on all PG versions.
 * These are checked case-insensitively.
 */
const VOLATILE_FUNCTIONS: ReadonlySet<string> = new Set([
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
 */
function containsVolatileFunction(astNode: unknown): string | null {
  if (!astNode || typeof astNode !== "object") return null;
  const exprNode = node(astNode);

  // Direct function call
  if (exprNode.FuncCall) {
    const funcNode = node(exprNode.FuncCall);
    const funcNames = nodes(funcNode.funcname);
    for (const fn of funcNames) {
      const name = (node(fn).String as Record<string, unknown> | undefined);
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
  if (exprNode.TypeCast) {
    return containsVolatileFunction(node(exprNode.TypeCast).arg);
  }

  // Check nested expressions
  if (exprNode.A_Expr) {
    const expr = node(exprNode.A_Expr);
    const left = containsVolatileFunction(expr.lexpr);
    if (left) return left;
    return containsVolatileFunction(expr.rexpr);
  }

  // CoalesceExpr
  if (exprNode.CoalesceExpr) {
    for (const arg of nodes(node(exprNode.CoalesceExpr).args)) {
      const result = containsVolatileFunction(arg);
      if (result) return result;
    }
  }

  return null;
}

export const SA002: Rule = {
  id: "SA002",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.AlterTableStmt) continue;

      const alterStmt = node(stmt.AlterTableStmt);
      if (alterStmt.objtype !== "OBJECT_TABLE") continue;

      for (const cmdEntry of nodes(alterStmt.cmds)) {
        const cmd = node(cmdEntry.AlterTableCmd);
        if (!cmdEntry.AlterTableCmd || cmd.subtype !== "AT_AddColumn") continue;

        const colDef = node(cmd.def).ColumnDef;
        if (!colDef) continue;
        const colDefNode = node(colDef);

        for (const c of nodes(colDefNode.constraints)) {
          const constraint = node(c.Constraint);
          if (!c.Constraint || constraint.contype !== "CONSTR_DEFAULT") continue;

          const rawExpr = constraint.raw_expr;
          if (!rawExpr) continue;

          const volatileFunc = containsVolatileFunction(rawExpr);
          if (volatileFunc) {
            const location = offsetToLocation(
              rawSql,
              stmtEntry.stmt_location ?? 0,
              filePath,
            );
            const tableName = node(alterStmt.relation).relname ?? "unknown";
            const colName = colDefNode.colname ?? "unknown";

            findings.push({
              ruleId: "SA002",
              severity: "error",
              message: `Adding column "${colName}" to table "${tableName}" with volatile default ${volatileFunc}() causes a full table rewrite on all PostgreSQL versions.`,
              location,
              suggestion:
                "Add the column without a default, then backfill in batches using UPDATE.",
            });
          }
        }
      }
    }

    return findings;
  },
};

export default SA002;
