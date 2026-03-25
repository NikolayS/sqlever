/**
 * SA039: Prefer bigint over int for primary keys
 *
 * Severity: info
 * Type: static
 *
 * Detects CREATE TABLE or ALTER TABLE ADD COLUMN where a primary key column
 * uses int4/integer/int instead of int8/bigint. Using a 32-bit integer for
 * primary keys risks capacity exhaustion on high-volume tables.
 */

import type { Rule, Finding, AnalysisContext, StringNode } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

/** int4/integer type names in the AST. */
const INT4_TYPES = new Set(["int4", "int2"]);

function isSmallIntType(typeName: Record<string, unknown>): boolean {
  const names = nodes<StringNode>(typeName.names);
  const last = names[names.length - 1];
  const sval = last?.String?.sval;
  return sval !== undefined && INT4_TYPES.has(sval);
}

function hasPrimaryKey(constraints: Record<string, unknown>[]): boolean {
  return constraints.some((c) => {
    const conNode = node(c.Constraint ?? c);
    return conNode.contype === "CONSTR_PRIMARY";
  });
}

export const SA039: Rule = {
  id: "SA039",
  name: "prefer-bigint-pk",
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
          const constraints = nodes(colDef.constraints);

          if (isSmallIntType(typeName) && hasPrimaryKey(constraints)) {
            const colName = (colDef.colname as string) ?? "unknown";
            const location = offsetToLocation(
              rawSql,
              (colDef.location as number) ?? stmtEntry.stmt_location ?? 0,
              filePath,
            );
            findings.push({
              ruleId: "SA039",
              severity: "info",
              message: `Primary key column "${colName}" on table "${tableName}" uses a 32-bit integer type. Prefer bigint.`,
              location,
              suggestion:
                "Use bigint (int8) for primary key columns to avoid future capacity issues.",
            });
          }
        }
      }
    }

    // ALTER TABLE ... ADD COLUMN ... integer PRIMARY KEY
    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_AddColumn") return;

      const colDef = node(node(cmd.def).ColumnDef);
      if (!colDef.typeName) return;
      const typeName = node(colDef.typeName);
      const constraints = nodes(colDef.constraints);

      if (isSmallIntType(typeName) && hasPrimaryKey(constraints)) {
        const tableName = node(alterStmt.relation).relname ?? "unknown";
        const colName = (colDef.colname as string) ?? "unknown";
        const location = offsetToLocation(rawSql, stmtLocation, filePath);
        findings.push({
          ruleId: "SA039",
          severity: "info",
          message: `Primary key column "${colName}" on table "${tableName}" uses a 32-bit integer type. Prefer bigint.`,
          location,
          suggestion:
            "Use bigint (int8) for primary key columns to avoid future capacity issues.",
        });
      }
    });

    return findings;
  },
};

export default SA039;
