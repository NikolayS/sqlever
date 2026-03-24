/**
 * SA025: Nested BEGIN / START TRANSACTION inside migration
 *
 * Severity: warn
 * Type: static
 *
 * Detects BEGIN or START TRANSACTION statements in migration scripts.
 * Migration runners (sqitch, sqlever) typically wrap each migration in a
 * transaction already. A nested BEGIN/START TRANSACTION inside the script
 * is likely a bug -- it either has no effect (Postgres does not support
 * nested transactions via BEGIN) or indicates the author expects standalone
 * execution semantics.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";

export const SA025: Rule = {
  id: "SA025",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.TransactionStmt) continue;

      const txStmt = node(stmt.TransactionStmt);

      // Only flag BEGIN and START TRANSACTION, not COMMIT/ROLLBACK/etc.
      if (
        txStmt.kind !== "TRANS_STMT_BEGIN" &&
        txStmt.kind !== "TRANS_STMT_START"
      ) {
        continue;
      }

      const keyword =
        txStmt.kind === "TRANS_STMT_BEGIN" ? "BEGIN" : "START TRANSACTION";

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      findings.push({
        ruleId: "SA025",
        severity: "warn",
        message: `${keyword} inside a migration script is likely a bug. Migrations are already wrapped in a transaction by the runner.`,
        location,
        suggestion:
          "Remove the explicit transaction control. If you need savepoints, use SAVEPOINT instead.",
      });
    }

    return findings;
  },
};

export default SA025;
