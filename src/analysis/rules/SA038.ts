/**
 * SA038: Prefer text over varchar(n)
 *
 * Severity: info
 * Type: static
 *
 * Detects column definitions using varchar(n). In Postgres, text and varchar(n)
 * have identical performance characteristics, but varchar(n) imposes an
 * arbitrary length limit that often needs to be changed later via a
 * constraint-rewriting ALTER TABLE. Prefer text with a CHECK constraint
 * if a length limit is truly needed.
 */

import type { Rule, Finding, AnalysisContext, StringNode } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

/** Serial-family type names are not varchar -- skip them. */
function isVarchar(typeName: Record<string, unknown>): boolean {
  const names = nodes<StringNode>(typeName.names);
  const last = names[names.length - 1];
  return last?.String?.sval === "varchar";
}

function getColName(colDef: Record<string, unknown>): string {
  return (colDef.colname as string) ?? "unknown";
}

export const SA038: Rule = {
  id: "SA038",
  name: "prefer-text-over-varchar",
  severity: "info",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // CREATE TABLE ... (col varchar(n), ...)
      if (stmt?.CreateStmt) {
        const createStmt = node(stmt.CreateStmt);
        const tableName = node(createStmt.relation).relname ?? "unknown";
        const elts = nodes(createStmt.tableElts);

        for (const elt of elts) {
          if (!elt.ColumnDef) continue;
          const colDef = node(elt.ColumnDef);
          const typeName = node(colDef.typeName);

          if (isVarchar(typeName)) {
            const location = offsetToLocation(
              rawSql,
              (colDef.location as number) ?? stmtEntry.stmt_location ?? 0,
              filePath,
            );
            findings.push({
              ruleId: "SA038",
              severity: "info",
              message: `Column "${getColName(colDef)}" on table "${tableName}" uses varchar(n). Prefer text.`,
              location,
              suggestion:
                "Use text instead of varchar(n). If a length limit is needed, use a CHECK constraint.",
            });
          }
        }
      }
    }

    // ALTER TABLE ... ADD COLUMN ... varchar(n)
    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_AddColumn") return;

      const colDef = node(node(cmd.def).ColumnDef);
      if (!colDef.typeName) return;
      const typeName = node(colDef.typeName);

      if (isVarchar(typeName)) {
        const tableName = node(alterStmt.relation).relname ?? "unknown";
        const location = offsetToLocation(rawSql, stmtLocation, filePath);
        findings.push({
          ruleId: "SA038",
          severity: "info",
          message: `Column "${getColName(colDef)}" on table "${tableName}" uses varchar(n). Prefer text.`,
          location,
          suggestion:
            "Use text instead of varchar(n). If a length limit is needed, use a CHECK constraint.",
        });
      }
    });

    return findings;
  },
};

export default SA038;
