/**
 * SA028: TRUNCATE ... CASCADE
 *
 * Severity: warn
 * Type: static
 *
 * Detects TRUNCATE statements with CASCADE behavior. TRUNCATE CASCADE
 * automatically truncates all tables that have foreign-key references to
 * the named table(s), potentially causing unexpected data loss across
 * multiple tables.
 *
 * PL/pgSQL body exclusion: TRUNCATE inside CREATE FUNCTION, CREATE PROCEDURE,
 * and DO blocks is excluded -- these define function bodies, not direct
 * migration operations.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";

export const SA028: Rule = {
  id: "SA028",
  name: "truncate-cascade",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // Skip PL/pgSQL bodies (CREATE FUNCTION, CREATE PROCEDURE, DO blocks)
      if (stmt?.CreateFunctionStmt || stmt?.DoStmt) continue;

      if (!stmt?.TruncateStmt) continue;

      const truncateStmt = node(stmt.TruncateStmt);

      // Only fire on CASCADE behavior
      if (truncateStmt.behavior !== "DROP_CASCADE") continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      // Extract table names
      const tableNames: string[] = [];
      for (const rel of nodes(truncateStmt.relations)) {
        const rv = node(node(rel).RangeVar);
        if (rv?.relname) {
          const schema = rv.schemaname ? `${rv.schemaname}.` : "";
          tableNames.push(`${schema}${rv.relname}`);
        }
      }

      const nameStr =
        tableNames.length > 0 ? tableNames.join(", ") : "unknown";

      findings.push({
        ruleId: "SA028",
        severity: "warn",
        message: `TRUNCATE CASCADE on ${nameStr} will cascade to all referencing tables, causing potential data loss.`,
        location,
        suggestion:
          "Use TRUNCATE without CASCADE and handle dependent tables explicitly, or verify that cascading data removal is intentional.",
      });
    }

    return findings;
  },
};

export default SA028;
