/**
 * SA037: Integer primary key capacity warning
 *
 * Severity: info
 * Type: static
 *
 * Detects CREATE TABLE statements where the primary key column uses int4
 * (integer / int / serial) rather than int8 (bigint / bigserial). The int4
 * type has a maximum of ~2.1 billion values, which large or fast-growing
 * tables can exhaust. Using bigint (int8) from the start avoids a costly
 * ALTER COLUMN TYPE migration later.
 *
 * Static check: only inspects CREATE TABLE definitions. No database
 * connection is needed since the column type is visible in the DDL.
 */

import type {
  Rule,
  Finding,
  AnalysisContext,
  StringNode,
} from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";

/** int4-family type names from pg_catalog. */
const INT4_TYPES = new Set(["int4", "integer", "int", "serial"]);

export const SA037: Rule = {
  id: "SA037",
  name: "int-pk-capacity",
  severity: "info",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.CreateStmt) continue;

      const createStmt = node(stmt.CreateStmt);
      const tableName = node(createStmt.relation).relname ?? "unknown";
      const schemaName = node(createStmt.relation).schemaname as
        | string
        | undefined;
      const qualifiedName = schemaName
        ? `${schemaName}.${tableName}`
        : tableName;

      for (const elt of nodes(createStmt.tableElts)) {
        if (!elt.ColumnDef) continue;
        const colDef = node(elt.ColumnDef);
        const colName = colDef.colname as string | undefined;
        if (!colName) continue;

        // Check if this column has a PK constraint
        let isPk = false;
        for (const c of nodes(colDef.constraints)) {
          if (!c.Constraint) continue;
          const constraint = node(c.Constraint);
          if (constraint.contype === "CONSTR_PRIMARY") {
            isPk = true;
            break;
          }
        }

        if (!isPk) continue;

        // Check the column type
        const typeName = node(colDef.typeName);
        const typeNames = nodes<StringNode>(typeName.names);
        const lastTypePart = typeNames[typeNames.length - 1];
        const typeStr = lastTypePart?.String?.sval?.toLowerCase();

        if (!typeStr || !INT4_TYPES.has(typeStr)) continue;

        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        findings.push({
          ruleId: "SA037",
          severity: "info",
          message: `Primary key column "${colName}" on table "${qualifiedName}" uses ${typeStr}, which is limited to ~2.1 billion values. Consider using bigint (int8) to avoid future capacity issues.`,
          location,
          suggestion:
            "Use int8 (bigint) or bigserial for primary keys: " +
            `${colName} int8 generated always as identity primary key`,
        });
      }
    }

    return findings;
  },
};

export default SA037;
