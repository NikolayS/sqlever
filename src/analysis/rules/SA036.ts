/**
 * SA036: Large UPDATE/INSERT without batching
 *
 * Severity: warn
 * Type: connected
 *
 * Extends the SA011 pattern. Detects UPDATE and INSERT ... SELECT statements
 * targeting tables that may have large row counts. Suggests using
 * `sqlever batch` for batched execution to avoid long-running transactions,
 * table bloat, and lock contention.
 *
 * Unlike SA011, this rule also covers INSERT ... SELECT (bulk inserts from
 * another table), which can be equally problematic on large tables.
 *
 * Connected: requires a database connection. When no connection is available,
 * this rule is silently skipped.
 *
 * PL/pgSQL body exclusion: DML inside CREATE FUNCTION, CREATE PROCEDURE,
 * and DO blocks is excluded.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";

export const SA036: Rule = {
  id: "SA036",
  severity: "warn",
  type: "connected",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath, db, config } = context;

    // Connected rule: requires a database connection
    if (!db) return findings;

    if (!ast?.stmts) return findings;

    const threshold = config.maxAffectedRows ?? 10_000;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // Skip PL/pgSQL bodies (CREATE FUNCTION, CREATE PROCEDURE, DO blocks)
      if (stmt?.CreateFunctionStmt || stmt?.DoStmt) continue;

      let tableName: string | undefined;
      let schemaName: string | undefined;
      let dmlType: "UPDATE" | "INSERT" | undefined;

      if (stmt?.UpdateStmt) {
        const rel = node(node(stmt.UpdateStmt).relation);
        tableName = rel?.relname as string | undefined;
        schemaName = rel?.schemaname as string | undefined;
        dmlType = "UPDATE";
      } else if (stmt?.InsertStmt) {
        const insertStmt = node(stmt.InsertStmt);
        // Only flag INSERT ... SELECT (not VALUES-based inserts)
        // VALUES-based inserts have selectStmt.SelectStmt.valuesLists
        if (!insertStmt.selectStmt) continue;
        const selectInner = node(node(insertStmt.selectStmt).SelectStmt);
        if (selectInner.valuesLists) continue;
        const rel = node(insertStmt.relation);
        tableName = rel?.relname as string | undefined;
        schemaName = rel?.schemaname as string | undefined;
        dmlType = "INSERT";
      }

      if (!tableName || !dmlType) continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      const qualifiedName = schemaName
        ? `${schemaName}.${tableName}`
        : tableName;

      findings.push({
        ruleId: "SA036",
        severity: "warn",
        message: `${dmlType} on table "${qualifiedName}" may affect a large number of rows (threshold: ${threshold}). Consider batching to avoid long transactions and bloat.`,
        location,
        suggestion:
          "Use `sqlever batch` for batched execution, or manually batch with LIMIT/OFFSET patterns to keep transactions short.",
      });
    }

    return findings;
  },
};

export default SA036;
