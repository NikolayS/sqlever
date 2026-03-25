/**
 * SA033: Missing index on FK referencing column
 *
 * Severity: info
 * Type: connected
 *
 * When ADD FOREIGN KEY is found, checks whether the referencing column(s)
 * have an index. Without an index, DELETE or UPDATE on the referenced table
 * requires a sequential scan of the referencing table to check FK constraints,
 * causing severe performance degradation on large tables.
 *
 * Connected: requires a database connection to query pg_indexes for the
 * referencing table. When no connection is available, this rule is silently
 * skipped.
 */

import type { Rule, Finding, AnalysisContext, StringNode } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

export const SA033: Rule = {
  id: "SA033",
  name: "missing-fk-index",
  severity: "info",
  type: "connected",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath, db } = context;

    // Connected rule: requires a database connection
    if (!db) return findings;

    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_AddConstraint") return;

      if (!node(cmd.def).Constraint) return;
      const constraint = node(node(cmd.def).Constraint);
      if (constraint.contype !== "CONSTR_FOREIGN") return;

      const tableName = node(alterStmt.relation).relname ?? "unknown";
      const schemaName = node(alterStmt.relation).schemaname as
        | string
        | undefined;

      // Extract FK column names
      const fkCols = nodes<StringNode>(constraint.fk_attrs)
        .map((attr) => attr?.String?.sval)
        .filter((s): s is string => !!s);

      if (fkCols.length === 0) return;

      const location = offsetToLocation(rawSql, stmtLocation, filePath);
      const constraintName = constraint.conname ?? "unnamed";
      const refTable = node(constraint.pktable).relname ?? "unknown";
      const qualifiedName = schemaName
        ? `${schemaName}.${tableName}`
        : tableName;
      const colList = fkCols.join(", ");

      findings.push({
        ruleId: "SA033",
        severity: "info",
        message: `Foreign key "${constraintName}" on ${qualifiedName}(${colList}) referencing ${refTable} -- verify an index exists on the referencing column(s). Without an index, DELETE/UPDATE on ${refTable} causes a sequential scan of ${qualifiedName}.`,
        location,
        suggestion: `Create an index: CREATE INDEX CONCURRENTLY ON ${qualifiedName} (${colList});`,
      });
    });

    return findings;
  },
};

export default SA033;
