// tests/unit/schema-diff.test.ts
// RED tests for 5.12 offline schema diff
// These tests define the expected behavior and WILL FAIL until implemented.

import { describe, it, expect } from "bun:test";

// These imports will fail until the module exists — that's expected (RED)
// import { schemaDiff } from "../../src/commands/schema-diff";

describe("schema diff — offline two-file comparison", () => {
  it("detects new table", () => {
    // Given before has no users table, after has it
    const before = ``;
    const after = `CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL);`;
    // When we diff
    // const result = schemaDiff(before, after);
    // Then output contains CREATE TABLE
    // expect(result.sql).toContain("CREATE TABLE users");
    expect(true).toBe(false); // RED: not implemented
  });

  it("detects dropped table (requires --confirm-destructive)", () => {
    expect(true).toBe(false); // RED
  });

  it("detects new column", () => {
    expect(true).toBe(false); // RED
  });

  it("detects column type change", () => {
    expect(true).toBe(false); // RED
  });

  it("detects new index", () => {
    expect(true).toBe(false); // RED
  });

  it("detects new foreign key", () => {
    expect(true).toBe(false); // RED
  });

  it("detects column nullability change", () => {
    expect(true).toBe(false); // RED
  });

  it("handles column rename hint --rename table.old:new", () => {
    expect(true).toBe(false); // RED
  });

  it("topologically orders output (FKs after tables)", () => {
    expect(true).toBe(false); // RED
  });

  it("applies --safe by default (CREATE INDEX becomes CONCURRENTLY)", () => {
    expect(true).toBe(false); // RED
  });

  it("parses pg_dump --schema-only output", () => {
    expect(true).toBe(false); // RED
  });
});

describe("schema diff — enum handling", () => {
  it("generates ADD VALUE for new enum values", () => {
    expect(true).toBe(false); // RED
  });

  it("requires --allow-enum-removal for dropped enum values", () => {
    expect(true).toBe(false); // RED
  });
});

describe("schema diff CLI", () => {
  it("sqlever schema diff <before.sql> <after.sql> exits 0 with diff output", () => {
    expect(true).toBe(false); // RED
  });

  it("sqlever schema diff --out writes to file", () => {
    expect(true).toBe(false); // RED
  });

  it("sqlever schema diff --unsafe skips safe rewrites", () => {
    expect(true).toBe(false); // RED
  });
});
