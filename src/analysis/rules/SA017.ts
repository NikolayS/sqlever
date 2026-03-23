/**
 * SA017: ALTER COLUMN SET NOT NULL on existing column
 *
 * Severity: warn (static portion)
 * Type: hybrid
 *
 * Static: Fires on any ALTER TABLE ... ALTER COLUMN ... SET NOT NULL.
 * On PG < 12, this requires a full table scan under AccessExclusiveLock.
 * On PG 12+, it is metadata-only IF a valid CHECK (col IS NOT NULL)
 * constraint exists.
 *
 * Connected: When a database connection is available, checks the catalog
 * for an existing valid CHECK (col IS NOT NULL) constraint and suppresses
 * the finding if found.
 *
 * Recommended three-step pattern:
 *   1. ALTER TABLE t ADD CONSTRAINT chk CHECK (col IS NOT NULL) NOT VALID;
 *   2. ALTER TABLE t VALIDATE CONSTRAINT chk;
 *   3. ALTER TABLE t ALTER COLUMN col SET NOT NULL;
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node } from "../types.js";
import { forEachAlterTableCmd } from "../ast-helpers.js";

export const SA017: Rule = {
  id: "SA017",
  severity: "warn",
  type: "hybrid",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    forEachAlterTableCmd(ast, ({ cmd, alterStmt, stmtLocation }) => {
      if (cmd.subtype !== "AT_SetNotNull") return;

      const colName = cmd.name ?? "unknown";
      const tableName = node(alterStmt.relation).relname ?? "unknown";

      // Connected check: if DB is available, check for existing
      // CHECK (col IS NOT NULL) constraint and suppress if found
      // This is left as a stub for the analysis engine integration

      const location = offsetToLocation(rawSql, stmtLocation, filePath);

      findings.push({
        ruleId: "SA017",
        severity: "warn",
        message: `SET NOT NULL on column "${colName}" of table "${tableName}" requires a full table scan on PG < 12, or a valid CHECK (${colName} IS NOT NULL) constraint on PG 12+.`,
        location,
        suggestion:
          "Use the three-step pattern: (1) ADD CONSTRAINT ... CHECK (col IS NOT NULL) NOT VALID, (2) VALIDATE CONSTRAINT, (3) SET NOT NULL.",
      });
    });

    return findings;
  },
};

export default SA017;
