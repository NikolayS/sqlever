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
import { containsVolatileFunction, forEachAlterTableCmd } from "../ast-helpers.js";

export const SA002: Rule = {
  id: "SA002",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_AddColumn") return;

      const colDef = node(cmd.def).ColumnDef;
      if (!colDef) return;
      const colDefNode = node(colDef);

      for (const c of nodes(colDefNode.constraints)) {
        if (!c.Constraint) continue;
        const constraint = node(c.Constraint);
        if (constraint.contype !== "CONSTR_DEFAULT") continue;

        const rawExpr = constraint.raw_expr;
        if (!rawExpr) continue;

        const volatileFunc = containsVolatileFunction(rawExpr);
        if (volatileFunc) {
          const location = offsetToLocation(rawSql, stmtLocation, filePath);
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
    });

    return findings;
  },
};

export default SA002;
