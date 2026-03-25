/**
 * SA014: VACUUM FULL or CLUSTER
 *
 * Severity: warn
 * Type: static
 *
 * Detects VACUUM FULL and CLUSTER statements. Both operations take an
 * AccessExclusiveLock on the table and perform a full table rewrite,
 * making them unsuitable for use in production migrations.
 *
 * Regular VACUUM (without FULL) is fine and does not trigger this rule.
 */

import type { Rule, Finding, AnalysisContext, DefElem } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";

export const SA014: Rule = {
  id: "SA014",
  name: "vacuum-full-or-cluster",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // Check VACUUM FULL
      if (stmt?.VacuumStmt) {
        const vacuumStmt = node(stmt.VacuumStmt);
        const options = nodes<DefElem>(vacuumStmt.options);
        const hasFull = options.some(
          (opt) => opt?.DefElem?.defname === "full",
        );

        if (hasFull) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );

          // Extract table names from rels
          const tableNames: string[] = [];
          for (const rel of nodes(vacuumStmt.rels)) {
            const rv = node(node(rel).VacuumRelation).relation;
            if (rv) {
              const rvNode = node(rv);
              const schema = rvNode.schemaname ? `${rvNode.schemaname}.` : "";
              tableNames.push(`${schema}${rvNode.relname}`);
            }
          }
          const nameStr =
            tableNames.length > 0 ? tableNames.join(", ") : "all tables";

          findings.push({
            ruleId: "SA014",
            severity: "warn",
            message: `VACUUM FULL on ${nameStr} takes an AccessExclusiveLock and rewrites the entire table.`,
            location,
            suggestion:
              "Avoid VACUUM FULL in migrations. Use regular VACUUM or pg_repack for online table compaction.",
          });
        }
      }

      // Check CLUSTER
      if (stmt?.ClusterStmt) {
        const clusterStmt = node(stmt.ClusterStmt);
        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        const tableName = node(clusterStmt.relation).relname ?? "unknown";

        findings.push({
          ruleId: "SA014",
          severity: "warn",
          message: `CLUSTER on "${tableName}" takes an AccessExclusiveLock and rewrites the entire table.`,
          location,
          suggestion:
            "Avoid CLUSTER in migrations. Use pg_repack for online table reorganization.",
        });
      }
    }

    return findings;
  },
};

export default SA014;
