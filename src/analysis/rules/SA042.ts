/**
 * SA042: Prefer IF NOT EXISTS / IF EXISTS on CREATE/DROP
 *
 * Severity: info
 * Type: static
 *
 * Detects CREATE TABLE/INDEX/SCHEMA without IF NOT EXISTS and DROP
 * TABLE/INDEX/SCHEMA without IF EXISTS. Idempotent migrations are safer
 * for re-runnable scripts and reduce failures in deploy pipelines.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, node, nodes } from "../types.js";
import type { StringNode } from "../types.js";

export const SA042: Rule = {
  id: "SA042",
  severity: "info",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      const stmtLoc = stmtEntry.stmt_location ?? 0;

      // CREATE TABLE without IF NOT EXISTS
      if (stmt?.CreateStmt) {
        const createStmt = node(stmt.CreateStmt);
        if (!createStmt.if_not_exists) {
          const tableName = node(createStmt.relation).relname ?? "unknown";
          findings.push({
            ruleId: "SA042",
            severity: "info",
            message: `CREATE TABLE "${tableName}" without IF NOT EXISTS.`,
            location: offsetToLocation(rawSql, stmtLoc, filePath),
            suggestion:
              "Use CREATE TABLE IF NOT EXISTS for idempotent migrations.",
          });
        }
      }

      // CREATE INDEX without IF NOT EXISTS
      if (stmt?.IndexStmt) {
        const indexStmt = node(stmt.IndexStmt);
        if (!indexStmt.if_not_exists) {
          const idxName = (indexStmt.idxname as string) ?? "unknown";
          findings.push({
            ruleId: "SA042",
            severity: "info",
            message: `CREATE INDEX "${idxName}" without IF NOT EXISTS.`,
            location: offsetToLocation(rawSql, stmtLoc, filePath),
            suggestion:
              "Use CREATE INDEX IF NOT EXISTS for idempotent migrations.",
          });
        }
      }

      // CREATE SCHEMA without IF NOT EXISTS
      if (stmt?.CreateSchemaStmt) {
        const schemaStmt = node(stmt.CreateSchemaStmt);
        if (!schemaStmt.if_not_exists) {
          const schemaName = (schemaStmt.schemaname as string) ?? "unknown";
          findings.push({
            ruleId: "SA042",
            severity: "info",
            message: `CREATE SCHEMA "${schemaName}" without IF NOT EXISTS.`,
            location: offsetToLocation(rawSql, stmtLoc, filePath),
            suggestion:
              "Use CREATE SCHEMA IF NOT EXISTS for idempotent migrations.",
          });
        }
      }

      // DROP TABLE/INDEX/SCHEMA without IF EXISTS
      if (stmt?.DropStmt) {
        const dropStmt = node(stmt.DropStmt);
        const removeType = dropStmt.removeType as string;

        // Only check TABLE, INDEX, SCHEMA
        const supportedTypes = new Set([
          "OBJECT_TABLE",
          "OBJECT_INDEX",
          "OBJECT_SCHEMA",
        ]);
        if (!supportedTypes.has(removeType)) continue;

        if (!dropStmt.missing_ok) {
          const typeLabel = removeType
            .replace("OBJECT_", "")
            .toLowerCase()
            .replace(/^\w/, (c: string) => c.toUpperCase());

          // Extract the object name(s)
          let objName = "unknown";
          const objects = nodes(dropStmt.objects);
          if (objects.length > 0) {
            const first = objects[0];
            // SCHEMA uses String nodes directly, TABLE/INDEX use List nodes
            if (first?.List) {
              const items = nodes<StringNode>(node(first.List).items);
              objName = items
                .map((item) => item?.String?.sval)
                .filter(Boolean)
                .join(".");
            } else if (first?.String) {
              objName =
                (first as unknown as StringNode).String?.sval ?? "unknown";
            }
          }

          findings.push({
            ruleId: "SA042",
            severity: "info",
            message: `DROP ${typeLabel.toUpperCase()} "${objName}" without IF EXISTS.`,
            location: offsetToLocation(rawSql, stmtLoc, filePath),
            suggestion: `Use DROP ${typeLabel.toUpperCase()} IF EXISTS for idempotent migrations.`,
          });
        }
      }
    }

    return findings;
  },
};

export default SA042;
