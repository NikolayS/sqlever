/**
 * SA027: ALTER TABLE ALTER COLUMN DROP NOT NULL
 *
 * Severity: warn
 * Type: static
 *
 * Detects ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL statements.
 * Removing a NOT NULL constraint is a semantic change that may break
 * application code relying on non-null guarantees.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

export const SA027: Rule = {
  id: "SA027",
  name: "drop-not-null",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_DropNotNull") return;

      const location = offsetToLocation(rawSql, stmtLocation, filePath);
      const tableName = node(alterStmt.relation).relname ?? "unknown";
      const colName = cmd.name ?? "unknown";

      findings.push({
        ruleId: "SA027",
        severity: "warn",
        message: `Dropping NOT NULL constraint on "${colName}" in table "${tableName}" may break application assumptions.`,
        location,
        suggestion:
          "Verify that all application code handles NULL values for this column before removing the constraint.",
      });
    });

    return findings;
  },
};

export default SA027;
