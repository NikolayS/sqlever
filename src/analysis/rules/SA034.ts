/**
 * SA034: CREATE INDEX CONCURRENTLY without indisvalid check
 *
 * Severity: info
 * Type: static
 *
 * CREATE INDEX CONCURRENTLY can silently produce an INVALID index if it
 * encounters a deadlock, uniqueness violation, or other error during the
 * second pass. The statement does not raise an error in this case -- it
 * completes successfully but leaves pg_index.indisvalid = false.
 *
 * This rule fires on every CIC statement as a reminder to verify
 * pg_index.indisvalid after the migration completes.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";

export const SA034: Rule = {
  id: "SA034",
  severity: "info",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.IndexStmt) continue;

      const indexStmt = node(stmt.IndexStmt);

      // Only care about CREATE INDEX CONCURRENTLY
      if (!indexStmt.concurrent) continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      const idxName = (indexStmt.idxname as string) ?? "unnamed";
      const tableName = node(indexStmt.relation).relname ?? "unknown";

      findings.push({
        ruleId: "SA034",
        severity: "info",
        message: `CREATE INDEX CONCURRENTLY "${idxName}" on table "${tableName}" can silently produce an INVALID index. Verify pg_index.indisvalid after completion.`,
        location,
        suggestion:
          "After the migration, run: SELECT indexrelid::regclass, indisvalid FROM pg_index WHERE indexrelid = '\"" +
          idxName +
          "\"'::regclass; If indisvalid is false, DROP and recreate the index.",
      });
    }

    return findings;
  },
};

export default SA034;
