/**
 * SA006: DROP COLUMN
 *
 * Severity: warn
 * Type: static
 *
 * Detects ALTER TABLE ... DROP COLUMN statements. Dropping a column is
 * irreversible data loss. While PostgreSQL marks the column as dropped
 * (metadata-only, no rewrite), the data is gone and cannot be recovered
 * without a backup.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

export const SA006: Rule = {
  id: "SA006",
  name: "drop-column",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_DropColumn") return;

      const location = offsetToLocation(rawSql, stmtLocation, filePath);
      const tableName = node(alterStmt.relation).relname ?? "unknown";
      const colName = cmd.name ?? "unknown";

      findings.push({
        ruleId: "SA006",
        severity: "warn",
        message: `Dropping column "${colName}" from table "${tableName}" causes irreversible data loss.`,
        location,
        suggestion:
          "Ensure a backup exists and that no application code depends on this column. Consider a deprecation period before dropping.",
      });
    });

    return findings;
  },
};

export default SA006;
