/**
 * SA005: DROP INDEX without CONCURRENTLY
 *
 * Severity: warn
 * Type: static
 *
 * Detects DROP INDEX statements that do not use the CONCURRENTLY option.
 * Without CONCURRENTLY, DROP INDEX takes an AccessExclusiveLock on the table.
 */

import type { Rule, Finding, AnalysisContext, StringNode } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";

export const SA005: Rule = {
  id: "SA005",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.DropStmt) continue;

      const dropStmt = node(stmt.DropStmt);

      // Only care about DROP INDEX
      if (dropStmt.removeType !== "OBJECT_INDEX") continue;

      // Skip if CONCURRENTLY is already used
      if (dropStmt.concurrent) continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      // Extract index name(s) from the objects list
      const indexNames: string[] = [];
      for (const obj of nodes(dropStmt.objects)) {
        const list = node(obj).List;
        if (list) {
          const names = nodes(node(list).items)
            .map((item) => (item as unknown as StringNode)?.String?.sval)
            .filter(Boolean);
          indexNames.push(names.join("."));
        }
      }

      const nameStr = indexNames.length > 0 ? indexNames.join(", ") : "unnamed";

      findings.push({
        ruleId: "SA005",
        severity: "warn",
        message: `DROP INDEX ${nameStr} without CONCURRENTLY takes an AccessExclusiveLock.`,
        location,
        suggestion:
          "Use DROP INDEX CONCURRENTLY to avoid blocking reads and writes. Note: CONCURRENTLY cannot run inside a transaction block.",
      });
    }

    return findings;
  },
};

export default SA005;
