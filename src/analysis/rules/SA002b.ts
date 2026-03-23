/**
 * SA002b: ADD COLUMN DEFAULT non-volatile on PG < 11
 *
 * Severity: warn
 * Type: static
 *
 * Detects ALTER TABLE ... ADD COLUMN with a non-volatile default expression
 * when targeting PostgreSQL < 11. On PG < 11, ANY default on ADD COLUMN
 * causes a full table rewrite. On PG 11+, non-volatile (immutable/stable)
 * defaults are metadata-only operations.
 *
 * This rule only fires when pgVersion < 11.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { containsVolatileFunction, forEachAlterTableCmd } from "../ast-helpers.js";

export const SA002b: Rule = {
  id: "SA002b",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath, pgVersion } = context;

    // Only fires when targeting PG < 11
    if (pgVersion >= 11) return findings;

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

        // Skip volatile defaults -- those are handled by SA002
        if (rawExpr && containsVolatileFunction(rawExpr)) continue;

        // This is a non-volatile default on PG < 11
        const location = offsetToLocation(rawSql, stmtLocation, filePath);
        const tableName = node(alterStmt.relation).relname ?? "unknown";
        const colName = colDefNode.colname ?? "unknown";

        findings.push({
          ruleId: "SA002b",
          severity: "warn",
          message: `Adding column "${colName}" to table "${tableName}" with a default causes a full table rewrite on PostgreSQL < 11 (target version: ${pgVersion}).`,
          location,
          suggestion:
            "Upgrade to PostgreSQL 11+ where non-volatile defaults are metadata-only, or add the column without a default and backfill.",
        });
      }
    });

    return findings;
  },
};

export default SA002b;
