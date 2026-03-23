/**
 * SA016: ADD CONSTRAINT CHECK without NOT VALID
 *
 * Severity: error
 * Type: static
 *
 * Detects ALTER TABLE ... ADD CONSTRAINT ... CHECK without NOT VALID.
 * Without NOT VALID, the constraint performs a full table scan under
 * ShareLock (PG < 16) / ShareUpdateExclusiveLock (PG 16+). The ShareLock
 * blocks INSERT/UPDATE/DELETE for the duration of the validation scan.
 *
 * Safe pattern: ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT
 * in a separate statement.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";

export const SA016: Rule = {
  id: "SA016",
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
        if (!cmdEntry.AlterTableCmd || cmd.subtype !== "AT_AddConstraint") continue;

        const constraint = node(node(cmd.def).Constraint);
        if (!node(cmd.def).Constraint || constraint.contype !== "CONSTR_CHECK") continue;

        // Check for NOT VALID: skip_validation = true means NOT VALID was used
        const hasNotValid = constraint.skip_validation === true;

        if (!hasNotValid) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );

          const tableName = node(alterStmt.relation).relname ?? "unknown";
          const constraintName = constraint.conname ?? "unnamed";

          findings.push({
            ruleId: "SA016",
            severity: "error",
            message: `Adding CHECK constraint "${constraintName}" on table "${tableName}" without NOT VALID performs a full table scan under a heavy lock.`,
            location,
            suggestion:
              "Use ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT in a separate statement (takes a weaker lock and does not block writes).",
          });
        }
      }
    }

    return findings;
  },
};

export default SA016;
