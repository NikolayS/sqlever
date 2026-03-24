/**
 * SA030: ADD UNIQUE constraint or CREATE UNIQUE INDEX on existing table
 *
 * Severity: warn
 * Type: static
 *
 * Detects ALTER TABLE ... ADD CONSTRAINT ... UNIQUE and CREATE UNIQUE INDEX
 * statements. Adding a unique constraint or index to an existing table may
 * fail if duplicate values exist. The operation also takes a lock on the
 * table while scanning for duplicates.
 *
 * Does not fire on CREATE TABLE (inline UNIQUE constraints on new tables
 * are safe since there is no existing data).
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

export const SA030: Rule = {
  id: "SA030",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    // Check ALTER TABLE ... ADD CONSTRAINT ... UNIQUE
    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_AddConstraint") return;

      const constraint = node(node(cmd.def).Constraint);
      if (constraint.contype !== "CONSTR_UNIQUE") return;

      const location = offsetToLocation(rawSql, stmtLocation, filePath);
      const tableName = node(alterStmt.relation).relname ?? "unknown";
      const conName = constraint.conname
        ? ` "${constraint.conname}"`
        : "";

      findings.push({
        ruleId: "SA030",
        severity: "warn",
        message: `Adding UNIQUE constraint${conName} on table "${tableName}" may fail if duplicate values exist.`,
        location,
        suggestion:
          "Check for duplicates before adding the constraint. Consider using CREATE UNIQUE INDEX CONCURRENTLY to avoid blocking writes.",
      });
    });

    // Check CREATE UNIQUE INDEX
    if (ast?.stmts) {
      for (const stmtEntry of ast.stmts) {
        const stmt = stmtEntry.stmt;
        if (!stmt?.IndexStmt) continue;

        const indexStmt = node(stmt.IndexStmt);
        if (!indexStmt.unique) continue;

        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );
        const tableName = node(indexStmt.relation).relname ?? "unknown";
        const indexName = indexStmt.idxname ?? "unknown";

        // Extract column names
        const colNames: string[] = [];
        for (const param of nodes(indexStmt.indexParams)) {
          const elem = node(node(param).IndexElem);
          if (elem.name) {
            colNames.push(String(elem.name));
          }
        }

        const colStr = colNames.length > 0 ? ` (${colNames.join(", ")})` : "";

        findings.push({
          ruleId: "SA030",
          severity: "warn",
          message: `CREATE UNIQUE INDEX "${indexName}" on "${tableName}"${colStr} may fail if duplicate values exist.`,
          location,
          suggestion:
            "Check for duplicates before creating the index. Use CREATE UNIQUE INDEX CONCURRENTLY to avoid blocking writes.",
        });
      }
    }

    return findings;
  },
};

export default SA030;
