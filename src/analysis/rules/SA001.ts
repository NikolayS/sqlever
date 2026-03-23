/**
 * SA001: ADD COLUMN NOT NULL without DEFAULT
 *
 * Severity: error
 * Type: static
 *
 * Detects ALTER TABLE ... ADD COLUMN with a NOT NULL constraint but no DEFAULT.
 * This fails outright on populated tables because existing rows would have NULL
 * for the new column, violating the NOT NULL constraint.
 *
 * Does NOT fire when a DEFAULT is present -- that case is covered by SA002/SA002b.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

export const SA001: Rule = {
  id: "SA001",
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

      const constraints = nodes(colDefNode.constraints);
      let hasNotNull = false;
      let hasDefault = false;

      for (const c of constraints) {
        if (!c.Constraint) continue;
        const constraint = node(c.Constraint);
        if (constraint.contype === "CONSTR_NOTNULL") hasNotNull = true;
        if (constraint.contype === "CONSTR_DEFAULT") hasDefault = true;
      }

      if (hasNotNull && !hasDefault) {
        const location = offsetToLocation(rawSql, stmtLocation, filePath);
        const tableName = node(alterStmt.relation).relname ?? "unknown";
        const colName = colDefNode.colname ?? "unknown";

        findings.push({
          ruleId: "SA001",
          severity: "error",
          message: `Adding NOT NULL column "${colName}" to table "${tableName}" without a DEFAULT will fail on populated tables.`,
          location,
          suggestion:
            "Add a DEFAULT value, or add the column as nullable first, backfill, then set NOT NULL.",
        });
      }
    });

    return findings;
  },
};

export default SA001;
