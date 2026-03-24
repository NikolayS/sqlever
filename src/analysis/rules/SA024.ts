/**
 * SA024: DROP ... CASCADE
 *
 * Severity: error
 * Type: static
 *
 * Detects any DROP or ALTER TABLE statement that uses CASCADE behavior.
 * CASCADE silently destroys dependent objects (views, foreign keys,
 * functions, etc.) and can cause unexpected, wide-reaching data loss.
 *
 * Covers:
 * - DropStmt with behavior DROP_CASCADE (DROP TABLE/INDEX/SCHEMA/etc. CASCADE)
 * - AlterTableCmd with behavior DROP_CASCADE (ALTER TABLE DROP COLUMN ... CASCADE)
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";

export const SA024: Rule = {
  id: "SA024",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      // DropStmt with CASCADE
      if (stmt?.DropStmt) {
        const dropStmt = node(stmt.DropStmt);
        if (dropStmt.behavior === "DROP_CASCADE") {
          const removeType = String(dropStmt.removeType ?? "OBJECT_UNKNOWN")
            .replace("OBJECT_", "")
            .replace(/_/g, " ");

          findings.push({
            ruleId: "SA024",
            severity: "error",
            message: `DROP ${removeType} with CASCADE silently destroys dependent objects.`,
            location,
            suggestion:
              "Remove CASCADE and explicitly drop dependent objects to avoid unexpected data loss.",
          });
        }
        continue;
      }

      // AlterTableStmt with CASCADE on individual commands
      if (stmt?.AlterTableStmt) {
        const alterStmt = node(stmt.AlterTableStmt);
        for (const cmdEntry of nodes(alterStmt.cmds)) {
          if (!cmdEntry.AlterTableCmd) continue;
          const cmd = node(cmdEntry.AlterTableCmd);

          if (cmd.behavior === "DROP_CASCADE") {
            const colName = String(cmd.name ?? "unknown");
            findings.push({
              ruleId: "SA024",
              severity: "error",
              message: `ALTER TABLE DROP COLUMN ${colName} with CASCADE silently destroys dependent objects.`,
              location,
              suggestion:
                "Remove CASCADE and explicitly drop dependent objects to avoid unexpected data loss.",
            });
          }
        }
      }
    }

    return findings;
  },
};

export default SA024;
