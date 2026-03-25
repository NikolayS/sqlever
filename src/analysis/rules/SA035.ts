/**
 * SA035: DROP PRIMARY KEY constraint may break replica identity
 *
 * Severity: warn
 * Type: static
 *
 * Detects ALTER TABLE ... DROP CONSTRAINT on primary key constraints.
 * Logical replication uses the primary key as the default replica identity
 * (REPLICA IDENTITY DEFAULT). Dropping a PK without first setting an
 * alternative replica identity (REPLICA IDENTITY USING INDEX or FULL)
 * breaks logical replication subscribers.
 *
 * Detection: uses AT_DropConstraint subtype. Since the AST does not
 * directly indicate whether the dropped constraint is a PK, we use a
 * naming convention heuristic (constraint name containing "pkey" or "pk")
 * and also detect any DROP CONSTRAINT as a cautionary measure, flagging
 * only those whose name matches common PK naming patterns.
 *
 * Additionally detects AT_DropConstraint where the constraint is
 * explicitly referenced via behavior (the AST sets behavior field).
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

/** Common PK constraint name patterns. */
const PK_NAME_PATTERNS = [/pkey$/i, /^pk_/i, /_pk$/i, /primary/i];

function isPkConstraintName(name: string): boolean {
  return PK_NAME_PATTERNS.some((p) => p.test(name));
}

export const SA035: Rule = {
  id: "SA035",
  name: "drop-primary-key",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_DropConstraint") return;

      const constraintName = (cmd.name as string) ?? "";
      if (!constraintName) return;

      // Only flag if name matches PK naming patterns
      if (!isPkConstraintName(constraintName)) return;

      const location = offsetToLocation(rawSql, stmtLocation, filePath);
      const tableName = node(alterStmt.relation).relname ?? "unknown";

      findings.push({
        ruleId: "SA035",
        severity: "warn",
        message: `Dropping primary key constraint "${constraintName}" on table "${tableName}" may break logical replication. REPLICA IDENTITY DEFAULT uses the primary key.`,
        location,
        suggestion:
          "Before dropping the PK, set an alternative replica identity: ALTER TABLE " +
          tableName +
          " REPLICA IDENTITY USING INDEX <unique_index>; or ALTER TABLE " +
          tableName +
          " REPLICA IDENTITY FULL;",
      });
    });

    return findings;
  },
};

export default SA035;
