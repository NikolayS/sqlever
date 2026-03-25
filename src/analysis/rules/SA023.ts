/**
 * SA023: DROP DATABASE
 *
 * Severity: error
 * Type: static
 *
 * Detects DROP DATABASE statements. Dropping a database is irreversible
 * data loss and should never appear in a migration script. Unlike DROP TABLE
 * or DROP SCHEMA, there is no revert-context exemption -- DROP DATABASE
 * in a migration is always an error.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";

export const SA023: Rule = {
  id: "SA023",
  name: "drop-database",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.DropdbStmt) continue;

      const dropDb = node(stmt.DropdbStmt);
      const dbName = String(dropDb.dbname ?? "unknown");

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      findings.push({
        ruleId: "SA023",
        severity: "error",
        message: `DROP DATABASE ${dbName} causes irreversible destruction of the entire database.`,
        location,
        suggestion:
          "Remove this statement. DROP DATABASE should never appear in a migration script.",
      });
    }

    return findings;
  },
};

export default SA023;
