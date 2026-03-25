/**
 * SA029: CREATE TABLE with SERIAL/BIGSERIAL/SMALLSERIAL column
 *
 * Severity: info
 * Type: static
 * Default: off
 *
 * Detects CREATE TABLE statements that use serial, bigserial, or smallserial
 * column types. The IDENTITY column syntax (e.g. int8 generated always as
 * identity) is the modern replacement, offering better control over sequence
 * ownership and standards compliance.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import type { StringNode } from "../types.js";

const SERIAL_TYPES: ReadonlySet<string> = new Set([
  "serial",
  "bigserial",
  "smallserial",
  "serial4",
  "serial8",
  "serial2",
]);

export const SA029: Rule = {
  id: "SA029",
  name: "create-table-serial",
  severity: "info",
  type: "static",
  defaultOff: true,

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.CreateStmt) continue;

      const createStmt = node(stmt.CreateStmt);
      const tableName = node(createStmt.relation).relname ?? "unknown";

      for (const elt of nodes(createStmt.tableElts)) {
        const colDef = node(elt).ColumnDef;
        if (!colDef) continue;

        const col = node(colDef);
        const colName = col.colname ?? "unknown";
        const typeName = node(col.typeName);
        const typeNames = nodes<StringNode>(typeName.names);

        for (const tn of typeNames) {
          const sval = tn?.String?.sval?.toLowerCase();
          if (sval && SERIAL_TYPES.has(sval)) {
            const location = offsetToLocation(
              rawSql,
              stmtEntry.stmt_location ?? 0,
              filePath,
            );

            findings.push({
              ruleId: "SA029",
              severity: "info",
              message: `Column "${colName}" in table "${tableName}" uses ${sval}. Prefer IDENTITY columns instead.`,
              location,
              suggestion:
                "Use 'bigint generated always as identity' (or 'int generated always as identity') for better sequence control and standards compliance.",
            });
          }
        }
      }
    }

    return findings;
  },
};

export default SA029;
