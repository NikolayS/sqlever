/**
 * SA022: DROP SCHEMA in non-revert context
 *
 * Severity: error
 * Type: static
 *
 * Detects DROP SCHEMA statements outside of revert scripts. Dropping a schema
 * destroys all objects within it and is irreversible. In sqitch project context,
 * files under revert/ are exempt since DROP SCHEMA is expected in revert scripts.
 * In standalone mode, always fires.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import type { StringNode } from "../types.js";

/**
 * Extract schema names from a DROP SCHEMA statement's objects list.
 *
 * Unlike DROP TABLE (which uses List nodes), DROP SCHEMA stores
 * names as direct String nodes.
 */
function extractSchemaNames(
  dropStmt: Record<string, unknown>,
): string[] {
  const names: string[] = [];
  for (const obj of nodes<StringNode>(dropStmt.objects)) {
    const sval = obj?.String?.sval;
    if (sval) {
      names.push(sval);
    }
  }
  return names;
}

export const SA022: Rule = {
  id: "SA022",
  name: "drop-schema",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath, isRevertContext } = context;

    // In revert context, DROP SCHEMA is expected and exempt
    if (isRevertContext) return findings;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.DropStmt) continue;

      const dropStmt = node(stmt.DropStmt);

      // Only care about DROP SCHEMA
      if (dropStmt.removeType !== "OBJECT_SCHEMA") continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      const schemaNames = extractSchemaNames(dropStmt);
      const nameStr =
        schemaNames.length > 0 ? schemaNames.join(", ") : "unknown";

      findings.push({
        ruleId: "SA022",
        severity: "error",
        message: `DROP SCHEMA ${nameStr} destroys all objects in the schema and is irreversible.`,
        location,
        suggestion:
          "Ensure a backup exists. In a sqitch project, DROP SCHEMA is expected only in revert/ scripts.",
      });
    }

    return findings;
  },
};

export default SA022;
