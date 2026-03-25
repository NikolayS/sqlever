// tests/unit/plan-roundtrip.test.ts — Round-trip tests for plan parser/writer
//
// Validates that parse(serialize(parse(content))) produces structurally
// identical Plan objects, and that append operations produce parseable output.
//
// Issue: NikolayS/sqlever#36

import { describe, expect, it, beforeEach, afterEach } from "bun:test";
import { readFileSync, mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { parsePlan } from "../../src/plan/parser";
import {
  serializePlan,
  appendChange,
} from "../../src/plan/writer";
import { computeChangeId } from "../../src/plan/types";
import type { Plan, Change } from "../../src/plan/types";

const FIXTURES_DIR = join(import.meta.dir, "..", "fixtures");

// ---------------------------------------------------------------------------
// Structural equivalence helpers
// ---------------------------------------------------------------------------

/**
 * Assert that two Plan objects are structurally equivalent:
 * same project, same pragmas, same changes (in order) with matching
 * IDs/names/deps/notes/parents, and same tags.
 */
function expectPlansEqual(a: Plan, b: Plan): void {
  // Project
  expect(a.project.name).toBe(b.project.name);
  expect(a.project.uri).toBe(b.project.uri);

  // Pragmas
  expect(a.pragmas.size).toBe(b.pragmas.size);
  for (const [key, value] of a.pragmas) {
    expect(b.pragmas.get(key)).toBe(value);
  }

  // Changes
  expect(a.changes.length).toBe(b.changes.length);
  for (let i = 0; i < a.changes.length; i++) {
    const ca = a.changes[i]!;
    const cb = b.changes[i]!;
    expect(ca.change_id).toBe(cb.change_id);
    expect(ca.name).toBe(cb.name);
    expect(ca.project).toBe(cb.project);
    expect(ca.note).toBe(cb.note);
    expect(ca.planner_name).toBe(cb.planner_name);
    expect(ca.planner_email).toBe(cb.planner_email);
    expect(ca.planned_at).toBe(cb.planned_at);
    expect(ca.requires).toEqual(cb.requires);
    expect(ca.conflicts).toEqual(cb.conflicts);
    expect(ca.parent).toBe(cb.parent);
  }

  // Tags
  expect(a.tags.length).toBe(b.tags.length);
  for (let i = 0; i < a.tags.length; i++) {
    const ta = a.tags[i]!;
    const tb = b.tags[i]!;
    expect(ta.tag_id).toBe(tb.tag_id);
    expect(ta.name).toBe(tb.name);
    expect(ta.project).toBe(tb.project);
    expect(ta.change_id).toBe(tb.change_id);
    expect(ta.note).toBe(tb.note);
    expect(ta.planner_name).toBe(tb.planner_name);
    expect(ta.planner_email).toBe(tb.planner_email);
    expect(ta.planned_at).toBe(tb.planned_at);
  }
}

// ---------------------------------------------------------------------------
// Helpers: minimal plan builders
// ---------------------------------------------------------------------------

function simplePlan(): string {
  return [
    "%syntax-version=1.0.0",
    "%project=testproject",
    "",
    "create_table 2024-06-01T09:00:00Z Alice <alice@example.com> # Create main table",
  ].join("\n") + "\n";
}

function planWithTags(): string {
  return [
    "%syntax-version=1.0.0",
    "%project=tagged",
    "%uri=https://example.com/tagged",
    "",
    "init_schema 2024-01-01T00:00:00Z Dev <dev@example.com> # Bootstrap schema",
    "add_users 2024-01-02T00:00:00Z Dev <dev@example.com> # Users table",
    "@v1.0 2024-01-02T00:01:00Z Dev <dev@example.com> # First release",
    "add_posts 2024-01-03T00:00:00Z Dev <dev@example.com> # Posts table",
    "@v2.0 2024-01-03T00:01:00Z Dev <dev@example.com> # Second release",
  ].join("\n") + "\n";
}

function planWithDeps(): string {
  return [
    "%syntax-version=1.0.0",
    "%project=depproject",
    "",
    "create_schema 2024-01-01T00:00:00Z Dev <dev@example.com> # schema",
    "add_users [create_schema] 2024-01-02T00:00:00Z Dev <dev@example.com> # users",
    "add_posts [create_schema add_users] 2024-01-03T00:00:00Z Dev <dev@example.com> # posts",
    "add_comments [add_posts !legacy_comments] 2024-01-04T00:00:00Z Dev <dev@example.com> # comments",
  ].join("\n") + "\n";
}

function planWithRework(): string {
  return [
    "%syntax-version=1.0.0",
    "%project=reworkproject",
    "%uri=https://example.com/rework",
    "",
    "add_users 2024-01-01T00:00:00Z Dev <dev@example.com> # Initial users",
    "@v1.0 2024-01-01T00:01:00Z Dev <dev@example.com> # Version 1",
    "add_posts 2024-01-02T00:00:00Z Dev <dev@example.com> # Posts table",
    "add_users [add_users@v1.0] 2024-02-01T00:00:00Z Dev <dev@example.com> # Reworked users with email",
    "@v2.0 2024-02-01T00:01:00Z Dev <dev@example.com> # Version 2",
  ].join("\n") + "\n";
}

function planWithUnicodeNotes(): string {
  return [
    "%syntax-version=1.0.0",
    "%project=unicode_proj",
    "",
    "add_users 2024-01-01T00:00:00Z Dev <dev@example.com> # Create users table",
    "add_emojis 2024-01-02T00:00:00Z Dev <dev@example.com> # Support CJK and accented chars",
  ].join("\n") + "\n";
}

// ---------------------------------------------------------------------------
// Round-trip tests
// ---------------------------------------------------------------------------

describe("plan round-trip: parse -> serialize -> parse", () => {
  it("simple plan (1 change)", () => {
    const original = parsePlan(simplePlan());
    const serialized = serializePlan(original);
    const reparsed = parsePlan(serialized);
    expectPlansEqual(original, reparsed);
  });

  it("plan with tags", () => {
    const original = parsePlan(planWithTags());
    const serialized = serializePlan(original);
    const reparsed = parsePlan(serialized);
    expectPlansEqual(original, reparsed);
  });

  it("plan with dependencies", () => {
    const original = parsePlan(planWithDeps());
    const serialized = serializePlan(original);
    const reparsed = parsePlan(serialized);
    expectPlansEqual(original, reparsed);
  });

  it("plan with reworked changes", () => {
    const original = parsePlan(planWithRework());
    const serialized = serializePlan(original);
    const reparsed = parsePlan(serialized);
    expectPlansEqual(original, reparsed);
  });

  it("plan with unicode notes", () => {
    const original = parsePlan(planWithUnicodeNotes());
    const serialized = serializePlan(original);
    const reparsed = parsePlan(serialized);
    expectPlansEqual(original, reparsed);
  });

  it("customer-zero plan (255 changes)", () => {
    const content = readFileSync(
      join(FIXTURES_DIR, "customer-zero.plan"),
      "utf-8",
    );
    const original = parsePlan(content);
    const serialized = serializePlan(original);
    const reparsed = parsePlan(serialized);
    expectPlansEqual(original, reparsed);
  });

  it("double round-trip produces identical results", () => {
    const content = readFileSync(
      join(FIXTURES_DIR, "customer-zero.plan"),
      "utf-8",
    );
    const plan1 = parsePlan(content);
    const ser1 = serializePlan(plan1);
    const plan2 = parsePlan(ser1);
    const ser2 = serializePlan(plan2);
    const plan3 = parsePlan(ser2);

    expectPlansEqual(plan1, plan2);
    expectPlansEqual(plan2, plan3);
    // After first normalization, serialized output should be byte-identical
    expect(ser1).toBe(ser2);
  });
});

// ---------------------------------------------------------------------------
// Change ID verification — customer-zero
// ---------------------------------------------------------------------------

describe("customer-zero change ID verification", () => {
  const content = readFileSync(
    join(FIXTURES_DIR, "customer-zero.plan"),
    "utf-8",
  );
  const plan = parsePlan(content);

  it("all 255 change IDs are non-empty 40-char hex strings", () => {
    expect(plan.changes.length).toBe(255);
    for (const change of plan.changes) {
      expect(change.change_id).toMatch(/^[0-9a-f]{40}$/);
    }
  });

  it("each change after the first has a parent", () => {
    for (let i = 1; i < plan.changes.length; i++) {
      const change = plan.changes[i]!;
      expect(change.parent).toBeDefined();
      expect(change.parent).not.toBe("");
    }
  });

  it("parent chain is continuous (change N's parent = change N-1's ID)", () => {
    expect(plan.changes[0]!.parent).toBeUndefined();
    for (let i = 1; i < plan.changes.length; i++) {
      expect(plan.changes[i]!.parent).toBe(plan.changes[i - 1]!.change_id);
    }
  });

  it("no duplicate change IDs", () => {
    const ids = plan.changes.map((c) => c.change_id);
    const unique = new Set(ids);
    expect(unique.size).toBe(ids.length);
  });

  it("change IDs are deterministic across parses", () => {
    const plan2 = parsePlan(content);
    for (let i = 0; i < plan.changes.length; i++) {
      expect(plan.changes[i]!.change_id).toBe(plan2.changes[i]!.change_id);
    }
  });

  it("change IDs survive round-trip through writer", () => {
    const serialized = serializePlan(plan);
    const reparsed = parsePlan(serialized);
    for (let i = 0; i < plan.changes.length; i++) {
      expect(plan.changes[i]!.change_id).toBe(
        reparsed.changes[i]!.change_id,
      );
    }
  });
});

// ---------------------------------------------------------------------------
// Append round-trip
// ---------------------------------------------------------------------------

describe("append round-trip: serializePlan + appendChange + parse", () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "sqlever-roundtrip-test-"));
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("appended change is present with correct ID after re-parse", async () => {
    // Build a base plan with one change
    const basePlan = parsePlan(simplePlan());
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, serializePlan(basePlan), "utf-8");

    // Construct a new change that will be appended
    const parentId = basePlan.changes[basePlan.changes.length - 1]!.change_id;
    const newChangeInput = {
      project: basePlan.project.name,
      change: "add_indexes",
      parent: parentId,
      planner_name: "Bob",
      planner_email: "bob@example.com",
      planned_at: "2024-06-02T12:00:00Z",
      requires: [] as string[],
      conflicts: [] as string[],
      note: "Add performance indexes",
    };
    const expectedId = computeChangeId(newChangeInput);

    const newChange: Change = {
      change_id: expectedId,
      name: "add_indexes",
      project: basePlan.project.name,
      note: "Add performance indexes",
      planner_name: "Bob",
      planner_email: "bob@example.com",
      planned_at: "2024-06-02T12:00:00Z",
      requires: [],
      conflicts: [],
      parent: parentId,
    };

    await appendChange(planPath, newChange);

    // Re-parse the file after append
    const resultContent = readFileSync(planPath, "utf-8");
    const resultPlan = parsePlan(resultContent);

    // Should have 2 changes now
    expect(resultPlan.changes.length).toBe(2);

    // The appended change should be present
    const appended = resultPlan.changes[1]!;
    expect(appended.name).toBe("add_indexes");
    expect(appended.note).toBe("Add performance indexes");
    expect(appended.planner_name).toBe("Bob");
    expect(appended.planner_email).toBe("bob@example.com");
    expect(appended.planned_at).toBe("2024-06-02T12:00:00Z");

    // Its computed ID should match what we expect
    expect(appended.change_id).toBe(expectedId);

    // Parent chain should be intact
    expect(appended.parent).toBe(basePlan.changes[0]!.change_id);
  });

  it("multiple appends preserve correct parent chain", async () => {
    const basePlan = parsePlan(simplePlan());
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, serializePlan(basePlan), "utf-8");

    let lastId = basePlan.changes[basePlan.changes.length - 1]!.change_id;

    // Append 3 changes in sequence
    for (let i = 0; i < 3; i++) {
      const changeInput = {
        project: basePlan.project.name,
        change: `step_${i}`,
        parent: lastId,
        planner_name: "Bot",
        planner_email: "bot@example.com",
        planned_at: `2024-07-0${i + 1}T00:00:00Z`,
        requires: [] as string[],
        conflicts: [] as string[],
        note: `Step ${i}`,
      };
      const changeId = computeChangeId(changeInput);

      await appendChange(planPath, {
        change_id: changeId,
        name: `step_${i}`,
        project: basePlan.project.name,
        note: `Step ${i}`,
        planner_name: "Bot",
        planner_email: "bot@example.com",
        planned_at: `2024-07-0${i + 1}T00:00:00Z`,
        requires: [],
        conflicts: [],
        parent: lastId,
      });

      lastId = changeId;
    }

    // Re-parse and verify
    const resultContent = readFileSync(planPath, "utf-8");
    const resultPlan = parsePlan(resultContent);

    expect(resultPlan.changes.length).toBe(4); // 1 original + 3 appended

    // Verify parent chain is continuous
    expect(resultPlan.changes[0]!.parent).toBeUndefined();
    for (let i = 1; i < resultPlan.changes.length; i++) {
      expect(resultPlan.changes[i]!.parent).toBe(
        resultPlan.changes[i - 1]!.change_id,
      );
    }

    // Verify names
    expect(resultPlan.changes[1]!.name).toBe("step_0");
    expect(resultPlan.changes[2]!.name).toBe("step_1");
    expect(resultPlan.changes[3]!.name).toBe("step_2");
  });
});
