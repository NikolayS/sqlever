/**
 * SA032: BEGIN without COMMIT or ROLLBACK
 *
 * Severity: warn
 * Type: static
 *
 * Detects scripts that contain a BEGIN statement but no corresponding
 * COMMIT or ROLLBACK. This leaves the transaction open, which means the
 * changes will not be committed (psql auto-rollbacks on disconnect).
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";

export const SA032: Rule = {
  id: "SA032",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    // Scan for transaction statements
    let beginLocation: number | null = null;
    let hasCommitOrRollback = false;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.TransactionStmt) continue;

      const txStmt = node(stmt.TransactionStmt);

      if (txStmt.kind === "TRANS_STMT_BEGIN") {
        // Track the first BEGIN we find
        if (beginLocation === null) {
          beginLocation = stmtEntry.stmt_location ?? 0;
        }
      } else if (
        txStmt.kind === "TRANS_STMT_COMMIT" ||
        txStmt.kind === "TRANS_STMT_ROLLBACK"
      ) {
        hasCommitOrRollback = true;
      }
    }

    if (beginLocation !== null && !hasCommitOrRollback) {
      const location = offsetToLocation(rawSql, beginLocation, filePath);

      findings.push({
        ruleId: "SA032",
        severity: "warn",
        message:
          "Script contains BEGIN but no COMMIT or ROLLBACK. The transaction will remain open.",
        location,
        suggestion:
          "Add a COMMIT statement at the end of the transaction block, or a ROLLBACK if the changes should not persist.",
      });
    }

    return findings;
  },
};

export default SA032;
