/**
 * SA040: Prefer IDENTITY over SERIAL
 *
 * Severity: info
 * Type: static
 *
 * Detects serial, bigserial, smallserial, serial4, serial8 type names in
 * column definitions. The SERIAL pseudo-types create implicit sequences
 * with ownership semantics that can cause surprises. The SQL-standard
 * GENERATED ALWAYS AS IDENTITY is preferred for modern Postgres.
 */

import type { Rule, Finding, AnalysisContext, StringNode } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

const SERIAL_TYPES = new Set([
  "serial",
  "bigserial",
  "smallserial",
  "serial4",
  "serial8",
  "serial2",
]);

function isSerialType(typeName: Record<string, unknown>): string | null {
  const names = nodes<StringNode>(typeName.names);
  const last = names[names.length - 1];
  const sval = last?.String?.sval;
  return sval !== undefined && SERIAL_TYPES.has(sval) ? sval : null;
}

export const SA040: Rule = {
  id: "SA040",
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
          const serialType = isSerialType(typeName);

          if (serialType) {
            const colName = (colDef.colname as string) ?? "unknown";
            const location = offsetToLocation(
              rawSql,
              (colDef.location as number) ?? stmtEntry.stmt_location ?? 0,
              filePath,
            );
            findings.push({
              ruleId: "SA040",
              severity: "info",
              message: `Column "${colName}" on table "${tableName}" uses ${serialType}. Prefer IDENTITY.`,
              location,
              suggestion:
                "Use int8 generated always as identity (or int4 generated always as identity) instead of serial types.",
            });
          }
        }
      }
    }

    // ALTER TABLE ... ADD COLUMN ... serial
    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_AddColumn") return;

      const colDef = node(node(cmd.def).ColumnDef);
      if (!colDef.typeName) return;
      const typeName = node(colDef.typeName);
      const serialType = isSerialType(typeName);

      if (serialType) {
        const tableName = node(alterStmt.relation).relname ?? "unknown";
        const colName = (colDef.colname as string) ?? "unknown";
        const location = offsetToLocation(rawSql, stmtLocation, filePath);
        findings.push({
          ruleId: "SA040",
          severity: "info",
          message: `Column "${colName}" on table "${tableName}" uses ${serialType}. Prefer IDENTITY.`,
          location,
          suggestion:
            "Use int8 generated always as identity (or int4 generated always as identity) instead of serial types.",
        });
      }
    });

    return findings;
  },
};

export default SA040;
