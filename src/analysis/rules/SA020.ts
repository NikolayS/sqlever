/**
 * SA020: CONCURRENTLY operations in transactional context
 *
 * Severity: error
 * Type: static
 *
 * Detects CREATE INDEX CONCURRENTLY, DROP INDEX CONCURRENTLY, or
 * REINDEX CONCURRENTLY usage. These operations cannot run inside a
 * transaction block and will fail at runtime if attempted.
 *
 * In project mode, the analyzer would check the plan file for an
 * auto-commit marker. In standalone mode, this rule warns on
 * any CONCURRENTLY usage with guidance to ensure it runs outside a
 * transaction block.
 *
 * Also recognizes the -- sqlever:auto-commit (or legacy
 * -- sqlever:no-transaction) script comment (sqlever-only convention)
 * to suppress the warning.
 */

import type { Rule, Finding, AnalysisContext, DefElem } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import { extractDropObjectNames } from "../ast-helpers.js";

/**
 * Check if the SQL contains a -- sqlever:auto-commit or
 * -- sqlever:no-transaction (legacy) directive comment.
 */
function hasAutoCommitDirective(rawSql: string): boolean {
  return /--\s*sqlever:(auto-commit|no-transaction)/i.test(rawSql);
}

export const SA020: Rule = {
  id: "SA020",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    // If the file has a -- sqlever:auto-commit (or legacy no-transaction) directive, skip
    if (hasAutoCommitDirective(rawSql)) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // CREATE INDEX CONCURRENTLY
      if (stmt?.IndexStmt) {
        const indexStmt = node(stmt.IndexStmt);
        if (indexStmt.concurrent) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );
          const idxName = indexStmt.idxname ?? "unnamed";

          findings.push({
            ruleId: "SA020",
            severity: "error",
            message: `CREATE INDEX CONCURRENTLY "${idxName}" cannot run inside a transaction block.`,
            location,
            suggestion:
              "Mark this migration as auto-commit, or add a -- sqlever:auto-commit comment. In sqitch, use an auto-commit (non-transactional) change.",
          });
        }
      }

      // DROP INDEX CONCURRENTLY
      if (stmt?.DropStmt) {
        const dropStmt = node(stmt.DropStmt);
        if (
          dropStmt.removeType === "OBJECT_INDEX" &&
          dropStmt.concurrent
        ) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );

          const indexNames = extractDropObjectNames(dropStmt);
          const nameStr =
            indexNames.length > 0 ? indexNames.join(", ") : "unnamed";

          findings.push({
            ruleId: "SA020",
            severity: "error",
            message: `DROP INDEX CONCURRENTLY ${nameStr} cannot run inside a transaction block.`,
            location,
            suggestion:
              "Mark this migration as auto-commit, or add a -- sqlever:auto-commit comment. In sqitch, use an auto-commit (non-transactional) change.",
          });
        }
      }

      // REINDEX CONCURRENTLY
      if (stmt?.ReindexStmt) {
        const reindexStmt = node(stmt.ReindexStmt);
        const params = nodes<DefElem>(reindexStmt.params);
        const hasConcurrently = params.some(
          (p) => p?.DefElem?.defname === "concurrently",
        );

        if (hasConcurrently) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );

          const target =
            node(reindexStmt.relation).relname ?? reindexStmt.name ?? "unknown";

          findings.push({
            ruleId: "SA020",
            severity: "error",
            message: `REINDEX CONCURRENTLY on "${target}" cannot run inside a transaction block.`,
            location,
            suggestion:
              "Mark this migration as auto-commit, or add a -- sqlever:auto-commit comment. In sqitch, use an auto-commit (non-transactional) change.",
          });
        }
      }
    }

    return findings;
  },
};

export default SA020;
