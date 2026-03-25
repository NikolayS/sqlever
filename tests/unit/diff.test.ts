import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtemp, writeFile, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  parseDiffArgs,
  computePendingDiff,
  computeRangeDiff,
  findTagChangeIndex,
  formatDiffText,
  type DiffResult,
} from "../../src/commands/diff";
import type { Plan, Change, Tag } from "../../src/plan/types";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a minimal Change for test plans. */
function makeChange(
  name: string,
  changeId: string,
  overrides: Partial<Change> = {},
): Change {
  return {
    change_id: changeId,
    name,
    project: "testproj",
    note: "",
    planner_name: "Test",
    planner_email: "test@test.com",
    planned_at: "2024-01-01T00:00:00Z",
    requires: [],
    conflicts: [],
    ...overrides,
  };
}

/** Build a minimal Tag for test plans. */
function makeTag(
  name: string,
  tagId: string,
  changeId: string,
): Tag {
  return {
    tag_id: tagId,
    name,
    project: "testproj",
    change_id: changeId,
    note: "",
    planner_name: "Test",
    planner_email: "test@test.com",
    planned_at: "2024-01-01T00:00:00Z",
  };
}

/** Build a minimal Plan with the given changes and tags. */
function makePlan(
  changes: Change[],
  tags: Tag[] = [],
): Plan {
  return {
    project: { name: "testproj" },
    pragmas: new Map([
      ["syntax-version", "1.0.0"],
      ["project", "testproj"],
    ]),
    changes,
    tags,
  };
}

// ---------------------------------------------------------------------------
// Tests: parseDiffArgs
// ---------------------------------------------------------------------------

describe("parseDiffArgs", () => {
  test("returns empty options when no args given", () => {
    const opts = parseDiffArgs([]);
    expect(opts.fromTag).toBeUndefined();
    expect(opts.toTag).toBeUndefined();
    expect(opts.format).toBeUndefined();
  });

  test("parses --from and --to tags", () => {
    const opts = parseDiffArgs(["--from", "v1.0", "--to", "v2.0"]);
    expect(opts.fromTag).toBe("v1.0");
    expect(opts.toTag).toBe("v2.0");
  });

  test("strips @ prefix from tag names", () => {
    const opts = parseDiffArgs(["--from", "@v1.0", "--to", "@v2.0"]);
    expect(opts.fromTag).toBe("v1.0");
    expect(opts.toTag).toBe("v2.0");
  });

  test("parses --format json", () => {
    const opts = parseDiffArgs(["--format", "json"]);
    expect(opts.format).toBe("json");
  });

  test("parses --format text", () => {
    const opts = parseDiffArgs(["--format", "text"]);
    expect(opts.format).toBe("text");
  });

  test("throws on missing --from value", () => {
    expect(() => parseDiffArgs(["--from"])).toThrow("--from requires a tag name");
  });

  test("throws on missing --to value", () => {
    expect(() => parseDiffArgs(["--to"])).toThrow("--to requires a tag name");
  });

  test("throws on invalid --format value", () => {
    expect(() => parseDiffArgs(["--format", "xml"])).toThrow("Invalid --format");
  });

  test("skips unknown flags", () => {
    const opts = parseDiffArgs(["--unknown", "--from", "v1"]);
    expect(opts.fromTag).toBe("v1");
  });
});

// ---------------------------------------------------------------------------
// Tests: findTagChangeIndex
// ---------------------------------------------------------------------------

describe("findTagChangeIndex", () => {
  test("returns the index of the change a tag is attached to", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
      makeChange("c", "id_c"),
    ];
    const tags = [makeTag("v1.0", "tag_v1", "id_b")];
    const plan = makePlan(changes, tags);

    expect(findTagChangeIndex(plan, "v1.0")).toBe(1);
  });

  test("returns -1 for unknown tag", () => {
    const plan = makePlan([makeChange("a", "id_a")]);
    expect(findTagChangeIndex(plan, "nonexistent")).toBe(-1);
  });
});

// ---------------------------------------------------------------------------
// Tests: computePendingDiff
// ---------------------------------------------------------------------------

describe("computePendingDiff", () => {
  test("returns all changes as pending when nothing deployed", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
      makeChange("c", "id_c"),
    ];
    const plan = makePlan(changes);
    const deployed = new Set<string>();

    const result = computePendingDiff(plan, deployed);

    expect(result.project).toBe("testproj");
    expect(result.from_tag).toBeNull();
    expect(result.to_tag).toBeNull();
    expect(result.changes).toHaveLength(3);
    expect(result.changes.every((c) => c.status === "pending")).toBe(true);
    expect(result.changes.map((c) => c.name)).toEqual(["a", "b", "c"]);
  });

  test("returns only pending changes (filters out deployed)", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
      makeChange("c", "id_c"),
    ];
    const plan = makePlan(changes);
    const deployed = new Set(["id_a", "id_c"]);

    const result = computePendingDiff(plan, deployed);

    expect(result.changes).toHaveLength(1);
    expect(result.changes[0]!.name).toBe("b");
    expect(result.changes[0]!.status).toBe("pending");
  });

  test("returns empty array when all changes are deployed", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
    ];
    const plan = makePlan(changes);
    const deployed = new Set(["id_a", "id_b"]);

    const result = computePendingDiff(plan, deployed);

    expect(result.changes).toHaveLength(0);
  });

  test("preserves dependencies and notes in output", () => {
    const changes = [
      makeChange("a", "id_a", {
        requires: ["setup"],
        conflicts: ["old_a"],
        note: "Add table A",
      }),
    ];
    const plan = makePlan(changes);

    const result = computePendingDiff(plan, new Set());

    expect(result.changes[0]!.requires).toEqual(["setup"]);
    expect(result.changes[0]!.conflicts).toEqual(["old_a"]);
    expect(result.changes[0]!.note).toBe("Add table A");
  });

  test("returns empty for empty plan", () => {
    const plan = makePlan([]);
    const result = computePendingDiff(plan, new Set());
    expect(result.changes).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Tests: computeRangeDiff
// ---------------------------------------------------------------------------

describe("computeRangeDiff", () => {
  test("returns changes between two tags", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
      makeChange("c", "id_c"),
      makeChange("d", "id_d"),
      makeChange("e", "id_e"),
    ];
    const tags = [
      makeTag("v1.0", "tag_v1", "id_b"),
      makeTag("v2.0", "tag_v2", "id_d"),
    ];
    const plan = makePlan(changes, tags);

    const result = computeRangeDiff(plan, "v1.0", "v2.0", new Set());

    expect(result.from_tag).toBe("v1.0");
    expect(result.to_tag).toBe("v2.0");
    expect(result.changes.map((c) => c.name)).toEqual(["c", "d"]);
  });

  test("annotates deployed/pending status in range", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
      makeChange("c", "id_c"),
      makeChange("d", "id_d"),
    ];
    const tags = [
      makeTag("v1.0", "tag_v1", "id_a"),
      makeTag("v2.0", "tag_v2", "id_d"),
    ];
    const plan = makePlan(changes, tags);

    const result = computeRangeDiff(plan, "v1.0", "v2.0", new Set(["id_b"]));

    expect(result.changes).toHaveLength(3);
    expect(result.changes[0]!.name).toBe("b");
    expect(result.changes[0]!.status).toBe("deployed");
    expect(result.changes[1]!.name).toBe("c");
    expect(result.changes[1]!.status).toBe("pending");
    expect(result.changes[2]!.name).toBe("d");
    expect(result.changes[2]!.status).toBe("pending");
  });

  test("throws when --from tag is not found", () => {
    const plan = makePlan([makeChange("a", "id_a")]);
    expect(() => computeRangeDiff(plan, "nonexistent", "v2.0", new Set())).toThrow(
      'Tag "nonexistent" not found in plan',
    );
  });

  test("throws when --to tag is not found", () => {
    const changes = [makeChange("a", "id_a")];
    const tags = [makeTag("v1.0", "tag_v1", "id_a")];
    const plan = makePlan(changes, tags);

    expect(() => computeRangeDiff(plan, "v1.0", "nonexistent", new Set())).toThrow(
      'Tag "nonexistent" not found in plan',
    );
  });

  test("throws when --from appears after --to in plan", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
    ];
    const tags = [
      makeTag("v1.0", "tag_v1", "id_a"),
      makeTag("v2.0", "tag_v2", "id_b"),
    ];
    const plan = makePlan(changes, tags);

    expect(() => computeRangeDiff(plan, "v2.0", "v1.0", new Set())).toThrow(
      'Tag "v2.0" appears after "v1.0"',
    );
  });

  test("returns empty array when tags reference same change", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
    ];
    // Two tags on the same change
    const tags = [
      makeTag("v1.0", "tag_v1", "id_b"),
      makeTag("v1.1", "tag_v11", "id_b"),
    ];
    const plan = makePlan(changes, tags);

    const result = computeRangeDiff(plan, "v1.0", "v1.1", new Set());
    expect(result.changes).toHaveLength(0);
  });

  test("returns adjacent tag range correctly", () => {
    const changes = [
      makeChange("a", "id_a"),
      makeChange("b", "id_b"),
    ];
    const tags = [
      makeTag("v1.0", "tag_v1", "id_a"),
      makeTag("v2.0", "tag_v2", "id_b"),
    ];
    const plan = makePlan(changes, tags);

    const result = computeRangeDiff(plan, "v1.0", "v2.0", new Set());
    expect(result.changes).toHaveLength(1);
    expect(result.changes[0]!.name).toBe("b");
  });
});

// ---------------------------------------------------------------------------
// Tests: formatDiffText
// ---------------------------------------------------------------------------

describe("formatDiffText", () => {
  test("shows project name", () => {
    const result: DiffResult = {
      project: "myproj",
      from_tag: null,
      to_tag: null,
      changes: [],
    };
    const text = formatDiffText(result);
    expect(text).toContain("# Project: myproj");
  });

  test("shows 'Showing pending changes' for pending mode", () => {
    const result: DiffResult = {
      project: "myproj",
      from_tag: null,
      to_tag: null,
      changes: [],
    };
    const text = formatDiffText(result);
    expect(text).toContain("# Showing pending changes");
  });

  test("shows range header for tag range mode", () => {
    const result: DiffResult = {
      project: "myproj",
      from_tag: "v1.0",
      to_tag: "v2.0",
      changes: [],
    };
    const text = formatDiffText(result);
    expect(text).toContain("# Range: @v1.0 .. @v2.0");
  });

  test("shows 'No changes found.' for empty result", () => {
    const result: DiffResult = {
      project: "myproj",
      from_tag: null,
      to_tag: null,
      changes: [],
    };
    const text = formatDiffText(result);
    expect(text).toContain("No changes found.");
  });

  test("uses + marker for pending changes", () => {
    const result: DiffResult = {
      project: "myproj",
      from_tag: null,
      to_tag: null,
      changes: [
        { name: "add_users", status: "pending", requires: [], conflicts: [], note: "" },
      ],
    };
    const text = formatDiffText(result);
    expect(text).toContain("  + add_users [pending]");
  });

  test("uses space marker for deployed changes", () => {
    const result: DiffResult = {
      project: "myproj",
      from_tag: "v1", to_tag: "v2",
      changes: [
        { name: "add_users", status: "deployed", requires: [], conflicts: [], note: "" },
      ],
    };
    const text = formatDiffText(result);
    expect(text).toContain("    add_users [deployed]");
  });

  test("shows requires, conflicts, and note", () => {
    const result: DiffResult = {
      project: "myproj",
      from_tag: null,
      to_tag: null,
      changes: [
        {
          name: "add_orders",
          status: "pending",
          requires: ["add_users", "add_products"],
          conflicts: ["old_schema"],
          note: "Order tracking tables",
        },
      ],
    };
    const text = formatDiffText(result);
    expect(text).toContain("requires: add_users, add_products");
    expect(text).toContain("conflicts: old_schema");
    expect(text).toContain("note: Order tracking tables");
  });

  test("shows summary counts", () => {
    const result: DiffResult = {
      project: "myproj",
      from_tag: "v1",
      to_tag: "v2",
      changes: [
        { name: "a", status: "deployed", requires: [], conflicts: [], note: "" },
        { name: "b", status: "pending", requires: [], conflicts: [], note: "" },
        { name: "c", status: "pending", requires: [], conflicts: [], note: "" },
      ],
    };
    const text = formatDiffText(result);
    expect(text).toContain("Total: 3 change(s)");
    expect(text).toContain("2 pending");
    expect(text).toContain("1 deployed");
  });
});

// ---------------------------------------------------------------------------
// Tests: CLI integration (subprocess)
// ---------------------------------------------------------------------------

describe("diff CLI integration", () => {
  const CWD = import.meta.dir + "/../..";
  let tempDir: string;

  beforeEach(async () => {
    resetConfig();
    tempDir = await mkdtemp(join(tmpdir(), "sqlever-diff-test-"));
    await mkdir(join(tempDir, "deploy"), { recursive: true });
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  async function run(
    ...args: string[]
  ): Promise<{ stdout: string; stderr: string; exitCode: number }> {
    const proc = Bun.spawn(
      ["bun", "run", "src/cli.ts", ...args],
      {
        cwd: CWD,
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    return { stdout, stderr, exitCode };
  }

  test("exits with error when no plan file exists", async () => {
    const { stderr, exitCode } = await run(
      "diff",
      "--top-dir",
      tempDir,
    );
    expect(exitCode).not.toBe(0);
    expect(stderr).toContain("plan file not found");
  });

  test("shows pending changes in plan-only mode (no DB)", async () => {
    await writeFile(join(tempDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    await writeFile(
      join(tempDir, "sqitch.plan"),
      `%syntax-version=1.0.0
%project=testproj

init_schema 2024-01-01T00:00:00Z Dev <dev@test.com> # first
add_users 2024-01-02T00:00:00Z Dev <dev@test.com> # second
`,
    );

    const { stdout, exitCode } = await run("diff", "--top-dir", tempDir);

    expect(exitCode).toBe(0);
    expect(stdout).toContain("# Project: testproj");
    expect(stdout).toContain("# Showing pending changes");
    expect(stdout).toContain("init_schema");
    expect(stdout).toContain("add_users");
    expect(stdout).toContain("[pending]");
  });

  test("--format json outputs valid JSON in diff", async () => {
    await writeFile(join(tempDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    await writeFile(
      join(tempDir, "sqitch.plan"),
      `%syntax-version=1.0.0
%project=jsontest

change_one 2024-01-01T00:00:00Z Dev <dev@test.com> # a note
`,
    );

    const { stdout, exitCode } = await run(
      "diff",
      "--top-dir",
      tempDir,
      "--format",
      "json",
    );

    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.project).toBe("jsontest");
    expect(data.from_tag).toBeNull();
    expect(data.to_tag).toBeNull();
    expect(data.changes).toHaveLength(1);
    expect(data.changes[0].name).toBe("change_one");
    expect(data.changes[0].status).toBe("pending");
    expect(data.changes[0].note).toBe("a note");
  });

  test("--from without --to produces an error", async () => {
    await writeFile(join(tempDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    await writeFile(
      join(tempDir, "sqitch.plan"),
      `%syntax-version=1.0.0
%project=testproj

init_schema 2024-01-01T00:00:00Z Dev <dev@test.com> # first
@v1.0 2024-01-01T00:00:00Z Dev <dev@test.com> # tag
`,
    );

    const { stderr, exitCode } = await run(
      "diff",
      "--top-dir",
      tempDir,
      "--from",
      "v1.0",
    );

    expect(exitCode).not.toBe(0);
    expect(stderr).toContain("--from requires --to");
  });

  test("range diff between tags shows changes", async () => {
    await writeFile(join(tempDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    await writeFile(
      join(tempDir, "sqitch.plan"),
      `%syntax-version=1.0.0
%project=testproj

init_schema 2024-01-01T00:00:00Z Dev <dev@test.com> # first
@v1.0 2024-01-01T00:00:01Z Dev <dev@test.com> # tag v1
add_users 2024-01-02T00:00:00Z Dev <dev@test.com> # second
add_orders 2024-01-03T00:00:00Z Dev <dev@test.com> # third
@v2.0 2024-01-03T00:00:01Z Dev <dev@test.com> # tag v2
extra_stuff 2024-01-04T00:00:00Z Dev <dev@test.com> # fourth
`,
    );

    const { stdout, exitCode } = await run(
      "diff",
      "--top-dir",
      tempDir,
      "--from",
      "v1.0",
      "--to",
      "v2.0",
    );

    expect(exitCode).toBe(0);
    expect(stdout).toContain("# Range: @v1.0 .. @v2.0");
    expect(stdout).toContain("add_users");
    expect(stdout).toContain("add_orders");
    // extra_stuff is outside the range
    expect(stdout).not.toContain("extra_stuff");
    // init_schema is before the range
    expect(stdout).not.toContain("init_schema");
  });
});
