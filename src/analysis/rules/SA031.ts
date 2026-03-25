/**
 * SA031: ALTER TYPE ADD VALUE inside a transaction (PG < 12)
 *
 * Severity: error
 * Type: static
 *
 * Detects ALTER TYPE ... ADD VALUE (AlterEnumStmt) when the script also
 * contains a BEGIN (TransactionStmt with TRANS_STMT_BEGIN). On Postgres
 * versions before 12, ALTER TYPE ... ADD VALUE cannot run inside a
 * transaction block -- it fails at runtime.
 *
 * This rule is version-gated: it only fires when pgVersion < 12.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";

export const SA031: Rule = {
  id: "SA031",
  name: "alter-type-add-value-in-transaction",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath, pgVersion } = context;

    // Only relevant for PG < 12
    if (pgVersion >= 12) return findings;

    if (!ast?.stmts) return findings;

    // Check if script contains a BEGIN statement
    let hasBegin = false;
    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (stmt?.TransactionStmt) {
        const txStmt = node(stmt.TransactionStmt);
        if (txStmt.kind === "TRANS_STMT_BEGIN") {
          hasBegin = true;
          break;
        }
      }
    }

    if (!hasBegin) return findings;

    // Find all ALTER TYPE ... ADD VALUE statements
    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.AlterEnumStmt) continue;

      const enumStmt = node(stmt.AlterEnumStmt);
      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      const newVal = enumStmt.newVal ?? "unknown";

      findings.push({
        ruleId: "SA031",
        severity: "error",
        message: `ALTER TYPE ... ADD VALUE '${newVal}' cannot run inside a transaction on Postgres < 12 (target: ${pgVersion}).`,
        location,
        suggestion:
          "Remove the BEGIN/COMMIT wrapper, or upgrade the minimum PG version to 12+.",
      });
    }

    return findings;
  },
};

export default SA031;
