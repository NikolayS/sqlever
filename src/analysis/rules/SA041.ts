/**
 * SA041: Prefer timestamptz over timestamp
 *
 * Severity: info
 * Type: static
 *
 * Detects column definitions using timestamp (without time zone). The
 * timestamp type stores values without timezone information, which can
 * cause subtle bugs when servers or clients use different timezones.
 * Prefer timestamptz (timestamp with time zone) for correct behavior.
 *
 * In the libpg-query AST, "timestamp" parses to pg_catalog.timestamp
 * while "timestamptz" parses without the pg_catalog prefix.
 */

import type { Rule, Finding, AnalysisContext, StringNode } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

function isTimestampWithoutTz(typeName: Record<string, unknown>): boolean {
  const names = nodes<StringNode>(typeName.names);
  const last = names[names.length - 1];
  const sval = last?.String?.sval;
  // In the AST, "timestamp" -> pg_catalog.timestamp
  // "timestamptz" -> just timestamptz (no pg_catalog prefix)
  // Both have the sval "timestamp" for the last element but differ in prefix.
  // Actually: "timestamp" -> names: [pg_catalog, timestamp]
  //           "timestamptz" -> names: [timestamptz] (single element)
  // So we detect: last name is "timestamp" AND there is a pg_catalog prefix
  if (sval === "timestamp") {
    const first = names[0];
    // If there's a pg_catalog prefix, this is "timestamp without time zone"
    // If there's no prefix, it could still be "timestamp" spelled out
    // In practice, libpg-query always adds pg_catalog for "timestamp"
    return first?.String?.sval === "pg_catalog" || names.length === 1;
  }
  return false;
}

export const SA041: Rule = {
  id: "SA041",
  severity: "info",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      if (stmt?.CreateStmt) {
        const createStmt = node(stmt.CreateStmt);
        const tableName = node(createStmt.relation).relname ?? "unknown";
        const elts = nodes(createStmt.tableElts);

        for (const elt of elts) {
          if (!elt.ColumnDef) continue;
          const colDef = node(elt.ColumnDef);
          const typeName = node(colDef.typeName);

          if (isTimestampWithoutTz(typeName)) {
            const colName = (colDef.colname as string) ?? "unknown";
            const location = offsetToLocation(
              rawSql,
              (colDef.location as number) ?? stmtEntry.stmt_location ?? 0,
              filePath,
            );
            findings.push({
              ruleId: "SA041",
              severity: "info",
              message: `Column "${colName}" on table "${tableName}" uses timestamp without time zone. Prefer timestamptz.`,
              location,
              suggestion:
                "Use timestamptz (timestamp with time zone) to avoid timezone-related bugs.",
            });
          }
        }
      }
    }

    // ALTER TABLE ... ADD COLUMN ... timestamp
    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_AddColumn") return;

      const colDef = node(node(cmd.def).ColumnDef);
      if (!colDef.typeName) return;
      const typeName = node(colDef.typeName);

      if (isTimestampWithoutTz(typeName)) {
        const tableName = node(alterStmt.relation).relname ?? "unknown";
        const colName = (colDef.colname as string) ?? "unknown";
        const location = offsetToLocation(rawSql, stmtLocation, filePath);
        findings.push({
          ruleId: "SA041",
          severity: "info",
          message: `Column "${colName}" on table "${tableName}" uses timestamp without time zone. Prefer timestamptz.`,
          location,
          suggestion:
            "Use timestamptz (timestamp with time zone) to avoid timezone-related bugs.",
        });
      }
    });

    return findings;
  },
};

export default SA041;
