/**
 * SA026: Missing SET statement_timeout before long-running DML
 *
 * Severity: warn
 * Type: static
 *
 * Detects UPDATE, DELETE, or INSERT ... SELECT statements that are not
 * preceded by a SET statement_timeout in the same file. Long-running DML
 * on existing tables can hold row locks for extended periods, cause
 * replication lag, and exhaust connection pool slots.
 *
 * If a SET statement_timeout (or SET LOCAL statement_timeout) appears before
 * the DML statement in the same file, this rule does not fire.
 *
 * Pure INSERT ... VALUES (not INSERT ... SELECT) is excluded because it
 * inserts new rows without scanning existing data.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";

/**
 * Check if a statement is a SET statement_timeout.
 */
function isStatementTimeoutSet(stmt: Record<string, unknown>): boolean {
  if (!stmt?.VariableSetStmt) return false;
  const setStmt = node(stmt.VariableSetStmt);
  return (
    setStmt.kind === "VAR_SET_VALUE" && setStmt.name === "statement_timeout"
  );
}

/**
 * Check if an INSERT statement uses a SELECT source (INSERT ... SELECT)
 * rather than VALUES.
 */
function isInsertSelect(stmt: Record<string, unknown>): boolean {
  if (!stmt?.InsertStmt) return false;
  const insertStmt = node(stmt.InsertStmt);
  const selectStmt = insertStmt.selectStmt
    ? node(insertStmt.selectStmt)
    : null;
  if (!selectStmt?.SelectStmt) return false;
  const sel = node(selectStmt.SelectStmt);
  // INSERT ... VALUES has valuesLists; INSERT ... SELECT has fromClause or a subquery
  return !sel.valuesLists;
}

/**
 * Check if a statement is DML that may be long-running on existing tables.
 */
function isLongRunningDML(
  stmt: Record<string, unknown>,
): { risky: boolean; description: string } {
  // UPDATE
  if (stmt?.UpdateStmt) {
    const tableName = node(node(stmt.UpdateStmt).relation).relname ?? "unknown";
    return { risky: true, description: `UPDATE on "${tableName}"` };
  }

  // DELETE
  if (stmt?.DeleteStmt) {
    const tableName = node(node(stmt.DeleteStmt).relation).relname ?? "unknown";
    return { risky: true, description: `DELETE on "${tableName}"` };
  }

  // INSERT ... SELECT (but not INSERT ... VALUES)
  if (isInsertSelect(stmt)) {
    const tableName = node(node(stmt.InsertStmt).relation).relname ?? "unknown";
    return {
      risky: true,
      description: `INSERT ... SELECT into "${tableName}"`,
    };
  }

  return { risky: false, description: "" };
}

export const SA026: Rule = {
  id: "SA026",
  name: "missing-statement-timeout",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    // Scan all statements to find SET statement_timeout positions
    let statementTimeoutSeen = false;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // Track SET statement_timeout
      if (isStatementTimeoutSet(stmt)) {
        statementTimeoutSeen = true;
        continue;
      }

      // Check for long-running DML
      const { risky, description } = isLongRunningDML(stmt);
      if (risky && !statementTimeoutSeen) {
        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        findings.push({
          ruleId: "SA026",
          severity: "warn",
          message: `${description} without a preceding SET statement_timeout. Long-running DML can hold locks and cause replication lag.`,
          location,
          suggestion:
            "Add SET statement_timeout = '30s'; (or appropriate duration) before long-running DML to prevent unbounded execution.",
        });
      }
    }

    return findings;
  },
};

export default SA026;
