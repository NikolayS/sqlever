// tests/unit/git-correlated-includes.test.ts
//
// Tests for git-correlated \i / \ir resolution in database migrations.
//
// Verifies the core guarantee: when a migration references a shared file
// via \i or \ir, sqlever resolves the version from the git commit where
// the migration was planned -- not HEAD. This prevents silent breakage
// when shared files are modified after a migration is deployed.
//
// Issue #166: Test git-correlated \i / \ir resolution for database migrations

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { execSync } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  resolveIncludes,
  resolveDeployIncludes,
  findCommitByTimestamp,
  getFileAtCommit,
  getFileContent,
} from "../../src/includes/snapshot";

// ---------------------------------------------------------------------------
// Helpers -- temporary git repos
// ---------------------------------------------------------------------------

function makeTempDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-gitcorr-"));
}

function initGitRepo(): string {
  const dir = makeTempDir();
  execSync("git init -b main", { cwd: dir, stdio: "ignore" });
  execSync('git config user.email "test@sqlever.dev"', {
    cwd: dir,
    stdio: "ignore",
  });
  execSync('git config user.name "Test User"', {
    cwd: dir,
    stdio: "ignore",
  });
  writeFileSync(join(dir, ".gitkeep"), "");
  execSync("git add .gitkeep", { cwd: dir, stdio: "ignore" });
  execSync('git commit -m "initial"', { cwd: dir, stdio: "ignore" });
  return dir;
}

/**
 * Create a file, add, and commit it. Returns the commit hash.
 * Accepts an optional date to set GIT_COMMITTER_DATE and GIT_AUTHOR_DATE
 * for timestamp-based commit lookup tests.
 */
function commitFile(
  repoRoot: string,
  filePath: string,
  content: string,
  message?: string,
  date?: string,
): string {
  const absolutePath = join(repoRoot, filePath);
  const dir = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
  mkdirSync(dir, { recursive: true });
  writeFileSync(absolutePath, content);
  execSync(`git add "${filePath}"`, { cwd: repoRoot, stdio: "ignore" });

  const env = date
    ? { ...process.env, GIT_COMMITTER_DATE: date, GIT_AUTHOR_DATE: date }
    : undefined;
  execSync(`git commit -m "${message ?? `add ${filePath}`}"`, {
    cwd: repoRoot,
    stdio: "ignore",
    env,
  });
  return execSync("git rev-parse HEAD", { cwd: repoRoot }).toString().trim();
}

function cleanupDir(dir: string): void {
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    // Best effort
  }
}

// ---------------------------------------------------------------------------
// Tests: issue #166 scenario -- view loses a column
// ---------------------------------------------------------------------------

describe("issue #166 scenario: view loses a column", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot mode preserves the original shared file (with phone column)", () => {
    // Step 1: Create the shared view definition with phone column
    const hashWithPhone = commitFile(
      repoRoot,
      "shared/user_summary.sql",
      [
        "create or replace view user_summary as",
        "select",
        "    u.id,",
        "    u.name,",
        "    u.email,",
        "    u.phone,",
        "    count(o.id) as order_count",
        "from users as u",
        "left join orders as o on o.user_id = u.id",
        "group by u.id, u.name, u.email, u.phone;",
      ].join("\n"),
    );

    // Step 2: Create the migration that includes the shared file
    commitFile(
      repoRoot,
      "deploy/add_user_summary.sql",
      "begin;\n\\i shared/user_summary.sql\ncommit;\n",
    );

    // Step 3: Later, someone removes phone from the view
    commitFile(
      repoRoot,
      "shared/user_summary.sql",
      [
        "create or replace view user_summary as",
        "select",
        "    u.id,",
        "    u.name,",
        "    u.email,",
        "    count(o.id) as order_count",
        "from users as u",
        "left join orders as o on o.user_id = u.id",
        "group by u.id, u.name, u.email;",
      ].join("\n"),
    );

    // Snapshot mode: resolve at the commit where phone existed
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/add_user_summary.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      hashWithPhone,
      false, // snapshot enabled
    );

    expect(snapshotResult).toBeDefined();
    expect(snapshotResult!.content).toContain("u.phone");
    expect(snapshotResult!.content).toContain("begin;");
    expect(snapshotResult!.content).toContain("commit;");
  });

  it("--no-snapshot mode uses current version (phone column missing)", () => {
    // Step 1: Create the shared view definition with phone column
    commitFile(
      repoRoot,
      "shared/user_summary.sql",
      [
        "create or replace view user_summary as",
        "select",
        "    u.id,",
        "    u.name,",
        "    u.email,",
        "    u.phone,",
        "    count(o.id) as order_count",
        "from users as u",
        "left join orders as o on o.user_id = u.id",
        "group by u.id, u.name, u.email, u.phone;",
      ].join("\n"),
    );

    // Step 2: Create the migration
    commitFile(
      repoRoot,
      "deploy/add_user_summary.sql",
      "begin;\n\\i shared/user_summary.sql\ncommit;\n",
    );

    // Step 3: Remove phone from the view
    commitFile(
      repoRoot,
      "shared/user_summary.sql",
      [
        "create or replace view user_summary as",
        "select",
        "    u.id,",
        "    u.name,",
        "    u.email,",
        "    count(o.id) as order_count",
        "from users as u",
        "left join orders as o on o.user_id = u.id",
        "group by u.id, u.name, u.email;",
      ].join("\n"),
    );

    // --no-snapshot: should get the current version (no phone)
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/add_user_summary.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      undefined,
      true, // noSnapshot
    );

    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).not.toContain("u.phone");
    expect(noSnapshotResult!.content).toContain("u.email");
  });

  it("snapshot vs no-snapshot: same migration, different results", () => {
    // Create initial version with phone
    const hashV1 = commitFile(
      repoRoot,
      "shared/user_summary.sql",
      "create or replace view user_summary as\nselect u.id, u.phone from users as u;\n",
    );

    commitFile(
      repoRoot,
      "deploy/add_view.sql",
      "begin;\n\\i shared/user_summary.sql\ncommit;\n",
    );

    // Remove phone
    commitFile(
      repoRoot,
      "shared/user_summary.sql",
      "create or replace view user_summary as\nselect u.id from users as u;\n",
    );

    // Snapshot: phone present
    const snapshot = resolveDeployIncludes(
      join(repoRoot, "deploy/add_view.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      hashV1,
      false,
    );

    // No-snapshot: phone absent
    const noSnapshot = resolveDeployIncludes(
      join(repoRoot, "deploy/add_view.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      undefined,
      true,
    );

    expect(snapshot).toBeDefined();
    expect(noSnapshot).toBeDefined();

    // The two modes produce different content for the same migration
    expect(snapshot!.content).toContain("u.phone");
    expect(noSnapshot!.content).not.toContain("u.phone");

    // But both produce valid SQL structure
    expect(snapshot!.content).toContain("begin;");
    expect(snapshot!.content).toContain("commit;");
    expect(noSnapshot!.content).toContain("begin;");
    expect(noSnapshot!.content).toContain("commit;");
  });
});

// ---------------------------------------------------------------------------
// Tests: \ir resolution with git-correlated snapshots
// ---------------------------------------------------------------------------

describe("\\ir directive with git-correlated snapshots", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("\\ir ../common/setup.sql resolves from the correct commit", () => {
    // Commit v1 of the common setup file
    const hashV1 = commitFile(
      repoRoot,
      "common/setup.sql",
      "create schema if not exists app_v1;\n",
    );

    // Create migration using \ir relative path
    commitFile(
      repoRoot,
      "deploy/init.sql",
      "begin;\n\\ir ../common/setup.sql\ncommit;\n",
    );

    // Update common/setup.sql to v2
    commitFile(
      repoRoot,
      "common/setup.sql",
      "create schema if not exists app_v2;\n",
    );

    // Resolve at the v1 commit
    const result = resolveIncludes("deploy/init.sql", {
      commitHash: hashV1,
      repoRoot,
    });

    expect(result.content).toContain("app_v1");
    expect(result.content).not.toContain("app_v2");
    expect(result.includedFiles).toEqual(["common/setup.sql"]);
  });

  it("\\ir resolves nested relative includes from historical commit", () => {
    // Create a chain: deploy/init.sql -> \ir helpers/funcs.sql -> \ir ../types/core.sql
    const hashV1 = commitFile(
      repoRoot,
      "types/core.sql",
      "create type mood_v1 as enum ('happy');\n",
    );

    commitFile(
      repoRoot,
      "deploy/helpers/funcs.sql",
      "\\ir ../../types/core.sql\ncreate function get_mood_v1() returns mood_v1 as $$ select 'happy'::mood_v1 $$ language sql;\n",
    );

    commitFile(
      repoRoot,
      "deploy/init.sql",
      "\\ir helpers/funcs.sql\n",
    );

    // Update types to v2
    commitFile(
      repoRoot,
      "types/core.sql",
      "create type mood_v2 as enum ('happy', 'sad');\n",
    );

    // Resolve at v1
    const result = resolveIncludes("deploy/init.sql", {
      commitHash: hashV1,
      repoRoot,
    });

    expect(result.content).toContain("mood_v1");
    expect(result.content).not.toContain("mood_v2");
    expect(result.includedFiles).toContain("deploy/helpers/funcs.sql");
    expect(result.includedFiles).toContain("types/core.sql");
  });
});

// ---------------------------------------------------------------------------
// Tests: changes to included files after migration deployment
// ---------------------------------------------------------------------------

describe("changes to included files after migration was deployed", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("included file modified after migration: snapshot uses original", () => {
    // Original: shared helper with create table
    const hashOriginal = commitFile(
      repoRoot,
      "shared/tables.sql",
      "create table orders (id int8 generated always as identity, amount numeric(10,2));\n",
    );

    commitFile(
      repoRoot,
      "deploy/add_orders.sql",
      "begin;\n\\i shared/tables.sql\ncommit;\n",
    );

    // Later: someone adds a column to the shared file
    commitFile(
      repoRoot,
      "shared/tables.sql",
      "create table orders (id int8 generated always as identity, amount numeric(10,2), currency text default 'USD');\n",
    );

    // Snapshot resolves the original (no currency column)
    const result = resolveIncludes("deploy/add_orders.sql", {
      commitHash: hashOriginal,
      repoRoot,
    });

    expect(result.content).toContain("amount numeric(10,2)");
    expect(result.content).not.toContain("currency");
  });

  it("included file deleted after migration: snapshot still resolves", () => {
    // Create and commit a shared file
    const hashWithFile = commitFile(
      repoRoot,
      "shared/deprecated.sql",
      "create function old_helper() returns void as $$ $$ language sql;\n",
    );

    commitFile(
      repoRoot,
      "deploy/use_old.sql",
      "\\i shared/deprecated.sql\n",
    );

    // Delete the file in a later commit
    execSync("git rm shared/deprecated.sql", {
      cwd: repoRoot,
      stdio: "ignore",
    });
    execSync('git commit -m "remove deprecated file"', {
      cwd: repoRoot,
      stdio: "ignore",
    });

    // Snapshot at the original commit still finds the file
    const result = resolveIncludes("deploy/use_old.sql", {
      commitHash: hashWithFile,
      repoRoot,
    });

    expect(result.content).toContain("old_helper");
  });

  it("included file renamed after migration: snapshot resolves old path", () => {
    // Create file at original path
    const hashOriginal = commitFile(
      repoRoot,
      "shared/utils.sql",
      "create function util_fn() returns int as $$ select 42 $$ language sql;\n",
    );

    commitFile(
      repoRoot,
      "deploy/use_utils.sql",
      "\\i shared/utils.sql\n",
    );

    // Rename the file
    execSync("git mv shared/utils.sql shared/utilities.sql", {
      cwd: repoRoot,
      stdio: "ignore",
    });
    execSync('git commit -m "rename utils to utilities"', {
      cwd: repoRoot,
      stdio: "ignore",
    });

    // Snapshot at original commit still finds old path
    const result = resolveIncludes("deploy/use_utils.sql", {
      commitHash: hashOriginal,
      repoRoot,
    });

    expect(result.content).toContain("util_fn");
  });

  it("multiple migrations reference same file at different versions", () => {
    // v1 of shared file
    const hashV1 = commitFile(
      repoRoot,
      "shared/config.sql",
      "create table config (key text primary key, val text);\n",
    );

    commitFile(
      repoRoot,
      "deploy/001_config.sql",
      "\\i shared/config.sql\n",
    );

    // v2: add a column
    const hashV2 = commitFile(
      repoRoot,
      "shared/config.sql",
      "create table config (key text primary key, val text, description text);\n",
    );

    commitFile(
      repoRoot,
      "deploy/002_config_desc.sql",
      "\\i shared/config.sql\n",
    );

    // Migration 001 at hashV1: no description column
    const result1 = resolveIncludes("deploy/001_config.sql", {
      commitHash: hashV1,
      repoRoot,
    });
    expect(result1.content).not.toContain("description");
    expect(result1.content).toContain("val text");

    // Migration 002 at hashV2: has description column
    const result2 = resolveIncludes("deploy/002_config_desc.sql", {
      commitHash: hashV2,
      repoRoot,
    });
    expect(result2.content).toContain("description text");
  });
});

// ---------------------------------------------------------------------------
// Tests: timestamp-based commit resolution
// ---------------------------------------------------------------------------

describe("timestamp-based commit resolution", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("findCommitByTimestamp returns commit at or before the timestamp", () => {
    // Create commits at specific dates
    commitFile(
      repoRoot,
      "shared/v1.sql",
      "-- v1\n",
      "v1 commit",
      "2025-06-01T12:00:00Z",
    );

    const hashV2 = commitFile(
      repoRoot,
      "shared/v2.sql",
      "-- v2\n",
      "v2 commit",
      "2025-06-15T12:00:00Z",
    );

    commitFile(
      repoRoot,
      "shared/v3.sql",
      "-- v3\n",
      "v3 commit",
      "2025-07-01T12:00:00Z",
    );

    // Query with timestamp between v2 and v3: should return v2's commit
    const commit = findCommitByTimestamp("2025-06-20T00:00:00Z", repoRoot);
    expect(commit).toBe(hashV2);
  });

  it("findCommitByTimestamp returns undefined for timestamp before all commits", () => {
    commitFile(
      repoRoot,
      "file.sql",
      "-- content\n",
      "first real commit",
      "2025-06-01T12:00:00Z",
    );

    // The initial .gitkeep commit has no controlled date, so we use a
    // date before the epoch to ensure no commits match.
    const veryOld = findCommitByTimestamp("1970-01-01T00:00:00Z", repoRoot);
    expect(veryOld).toBeUndefined();
  });

  it("resolveDeployIncludes uses planned_at to find the right commit", () => {
    // Commit shared file v1 at a specific date
    commitFile(
      repoRoot,
      "shared/setup.sql",
      "-- setup v1: original\n",
      "add setup v1",
      "2025-06-01T12:00:00Z",
    );

    commitFile(
      repoRoot,
      "deploy/init.sql",
      "\\i shared/setup.sql\n",
      "add migration",
      "2025-06-02T12:00:00Z",
    );

    // Update shared file at a later date
    commitFile(
      repoRoot,
      "shared/setup.sql",
      "-- setup v2: updated\n",
      "update setup",
      "2025-07-01T12:00:00Z",
    );

    // planned_at = 2025-06-05 (between v1 and v2 of shared file)
    // Should resolve to the commit at or before that date, which has v1
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/init.sql"),
      "2025-06-05T00:00:00Z",
      repoRoot,
      undefined,
      false, // snapshot mode
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("setup v1: original");
    expect(result!.content).not.toContain("setup v2: updated");
  });

  it("planned_at after all commits gets the latest version", () => {
    commitFile(
      repoRoot,
      "shared/latest.sql",
      "-- latest version\n",
      "add latest",
      "2025-06-01T12:00:00Z",
    );

    commitFile(
      repoRoot,
      "deploy/use_latest.sql",
      "\\i shared/latest.sql\n",
      "add migration",
      "2025-06-02T12:00:00Z",
    );

    // planned_at far in the future
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/use_latest.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      false,
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("latest version");
  });
});

// ---------------------------------------------------------------------------
// Tests: edge cases for git-correlated resolution
// ---------------------------------------------------------------------------

describe("git-correlated edge cases", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("missing include file at historical commit throws descriptive error", () => {
    // Commit the deploy script referencing a file that does not exist
    // at any commit
    commitFile(
      repoRoot,
      "deploy/broken.sql",
      "\\i shared/nonexistent.sql\n",
    );

    const hashBroken = execSync("git rev-parse HEAD", { cwd: repoRoot })
      .toString()
      .trim();

    expect(() =>
      resolveIncludes("deploy/broken.sql", {
        commitHash: hashBroken,
        repoRoot,
      }),
    ).toThrow(/not found.*nonexistent\.sql/i);
  });

  it("circular include through three files throws error", () => {
    commitFile(repoRoot, "a.sql", "\\i b.sql\n");
    commitFile(repoRoot, "b.sql", "\\i c.sql\n");
    commitFile(repoRoot, "c.sql", "\\i a.sql\n");

    expect(() =>
      resolveIncludes("a.sql", { repoRoot }),
    ).toThrow(/[Cc]ircular include/);
  });

  it("self-referencing file throws circular include error", () => {
    commitFile(repoRoot, "loop.sql", "\\i loop.sql\n");

    expect(() =>
      resolveIncludes("loop.sql", { repoRoot }),
    ).toThrow(/[Cc]ircular include/);
  });

  it("deeply nested includes resolve from the same historical commit", () => {
    // Create a chain: a -> b -> c -> d, all at v1
    const hashV1 = commitFile(repoRoot, "d.sql", "-- d_v1\n");
    commitFile(repoRoot, "c.sql", "\\i d.sql\n-- c_v1\n");
    commitFile(repoRoot, "b.sql", "\\i c.sql\n-- b_v1\n");
    commitFile(repoRoot, "a.sql", "\\i b.sql\n-- a_v1\n");

    // Update d to v2
    commitFile(repoRoot, "d.sql", "-- d_v2\n");

    // Resolve at v1: all files should be v1
    const result = resolveIncludes("a.sql", {
      commitHash: hashV1,
      repoRoot,
    });

    expect(result.content).toContain("d_v1");
    expect(result.content).toContain("c_v1");
    expect(result.content).toContain("b_v1");
    expect(result.content).toContain("a_v1");
    expect(result.content).not.toContain("d_v2");
  });

  it("include file that exists at HEAD but not at historical commit falls back to HEAD", () => {
    // Commit the deploy script first
    const hashOld = commitFile(
      repoRoot,
      "deploy/early.sql",
      "\\i shared/later.sql\n",
    );

    // The included file is added in a later commit
    commitFile(
      repoRoot,
      "shared/later.sql",
      "-- added later\n",
    );

    // Resolving at hashOld: shared/later.sql did not exist at that commit,
    // but the fallback chain tries HEAD, where it does exist
    const result = resolveIncludes("deploy/early.sql", {
      commitHash: hashOld,
      repoRoot,
    });

    expect(result.content).toContain("added later");
  });

  it("include with path containing spaces resolves correctly", () => {
    commitFile(
      repoRoot,
      "shared dir/my file.sql",
      "-- file with spaces in path\n",
    );
    commitFile(
      repoRoot,
      "deploy/init.sql",
      "\\i 'shared dir/my file.sql'\n",
    );

    const result = resolveIncludes("deploy/init.sql", { repoRoot });

    expect(result.content).toContain("file with spaces in path");
  });

  it("empty included file does not break resolution", () => {
    commitFile(repoRoot, "shared/empty.sql", "");
    commitFile(repoRoot, "deploy/init.sql", "begin;\n\\i shared/empty.sql\ncommit;\n");

    const result = resolveIncludes("deploy/init.sql", { repoRoot });

    expect(result.content).toContain("begin;");
    expect(result.content).toContain("commit;");
    expect(result.includedFiles).toEqual(["shared/empty.sql"]);
  });

  it("max depth is enforced for deeply nested includes", () => {
    // Create a chain longer than maxDepth
    commitFile(repoRoot, "level5.sql", "-- bottom\n");
    commitFile(repoRoot, "level4.sql", "\\i level5.sql\n");
    commitFile(repoRoot, "level3.sql", "\\i level4.sql\n");
    commitFile(repoRoot, "level2.sql", "\\i level3.sql\n");
    commitFile(repoRoot, "level1.sql", "\\i level2.sql\n");
    commitFile(repoRoot, "level0.sql", "\\i level1.sql\n");

    // maxDepth=4 means 5 levels of nesting (0..4) are OK, but 6 is not
    expect(() =>
      resolveIncludes("level0.sql", { repoRoot, maxDepth: 4 }),
    ).toThrow(/depth exceeded/i);

    // maxDepth=5 should allow it
    const result = resolveIncludes("level0.sql", {
      repoRoot,
      maxDepth: 5,
    });
    expect(result.content).toContain("-- bottom");
  });
});

// ---------------------------------------------------------------------------
// Tests: mixed \i and \ir in the same script
// ---------------------------------------------------------------------------

describe("mixed \\i and \\ir directives with git correlation", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("script with both \\i and \\ir resolves each from the correct path", () => {
    const hashV1 = commitFile(
      repoRoot,
      "shared/types.sql",
      "create type role_v1 as enum ('admin');\n",
    );
    commitFile(
      repoRoot,
      "deploy/helpers.sql",
      "create function helper_v1() returns void as $$ $$ language sql;\n",
    );
    commitFile(
      repoRoot,
      "deploy/init.sql",
      [
        "begin;",
        "\\i shared/types.sql",
        "\\ir helpers.sql",
        "commit;",
      ].join("\n"),
    );

    // Update both files
    commitFile(
      repoRoot,
      "shared/types.sql",
      "create type role_v2 as enum ('admin', 'user');\n",
    );
    commitFile(
      repoRoot,
      "deploy/helpers.sql",
      "create function helper_v2() returns void as $$ $$ language sql;\n",
    );

    // Resolve at v1 commit
    const result = resolveIncludes("deploy/init.sql", {
      commitHash: hashV1,
      repoRoot,
    });

    expect(result.content).toContain("role_v1");
    expect(result.content).not.toContain("role_v2");
    // helpers.sql was committed after hashV1, so it falls back
    // The result will contain whichever version is available
    expect(result.includedFiles).toContain("shared/types.sql");
    expect(result.includedFiles).toContain("deploy/helpers.sql");
  });
});

// ---------------------------------------------------------------------------
// Tests: getFileAtCommit across different git states
// ---------------------------------------------------------------------------

describe("getFileAtCommit across different git states", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("retrieves correct version across multiple modifications", () => {
    const hash1 = commitFile(repoRoot, "file.sql", "-- version 1\n");
    const hash2 = commitFile(repoRoot, "file.sql", "-- version 2\n");
    const hash3 = commitFile(repoRoot, "file.sql", "-- version 3\n");

    expect(getFileAtCommit(hash1, "file.sql", repoRoot)).toBe("-- version 1\n");
    expect(getFileAtCommit(hash2, "file.sql", repoRoot)).toBe("-- version 2\n");
    expect(getFileAtCommit(hash3, "file.sql", repoRoot)).toBe("-- version 3\n");
  });

  it("returns undefined for file not present at a given commit", () => {
    const hashBefore = execSync("git rev-parse HEAD", { cwd: repoRoot })
      .toString()
      .trim();

    commitFile(repoRoot, "new_file.sql", "-- new content\n");

    // File did not exist at hashBefore
    expect(getFileAtCommit(hashBefore, "new_file.sql", repoRoot)).toBeUndefined();
  });

  it("handles file on a branch merged back to main", () => {
    // Create a branch, modify a file, merge back
    commitFile(repoRoot, "shared.sql", "-- main v1\n");
    const hashMainV1 = execSync("git rev-parse HEAD", { cwd: repoRoot })
      .toString()
      .trim();

    execSync("git checkout -b feature", { cwd: repoRoot, stdio: "ignore" });
    const hashFeature = commitFile(
      repoRoot,
      "shared.sql",
      "-- feature branch version\n",
    );

    execSync("git checkout main", { cwd: repoRoot, stdio: "ignore" });
    execSync("git merge feature --no-edit", {
      cwd: repoRoot,
      stdio: "ignore",
    });

    // At hashMainV1: original version
    expect(getFileAtCommit(hashMainV1, "shared.sql", repoRoot)).toBe(
      "-- main v1\n",
    );

    // At hashFeature: feature version
    expect(getFileAtCommit(hashFeature, "shared.sql", repoRoot)).toBe(
      "-- feature branch version\n",
    );
  });
});

// ---------------------------------------------------------------------------
// Tests: getFileContent fallback chain with git correlation
// ---------------------------------------------------------------------------

describe("getFileContent fallback chain", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("prefers specific commit over HEAD", () => {
    const hash1 = commitFile(repoRoot, "data.sql", "-- at commit 1\n");
    commitFile(repoRoot, "data.sql", "-- at HEAD\n");

    const content = getFileContent("data.sql", hash1, repoRoot);
    expect(content).toBe("-- at commit 1\n");
  });

  it("falls back to HEAD when file not at specific commit", () => {
    const hashBefore = execSync("git rev-parse HEAD", { cwd: repoRoot })
      .toString()
      .trim();

    commitFile(repoRoot, "new.sql", "-- at HEAD\n");

    // new.sql did not exist at hashBefore, but does at HEAD
    const content = getFileContent("new.sql", hashBefore, repoRoot);
    expect(content).toBe("-- at HEAD\n");
  });

  it("falls back to working tree when file not in git at all", () => {
    writeFileSync(join(repoRoot, "untracked.sql"), "-- untracked content\n");

    const content = getFileContent("untracked.sql", undefined, repoRoot);
    expect(content).toBe("-- untracked content\n");
  });

  it("returns undefined when file not found anywhere", () => {
    const content = getFileContent("ghost.sql", undefined, repoRoot);
    expect(content).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Tests: scriptContent parameter (pre-read optimization)
// ---------------------------------------------------------------------------

describe("resolveDeployIncludes with pre-read scriptContent", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("uses scriptContent to detect directives without reading from disk", () => {
    // The scriptContent parameter is an optimization: it lets the caller
    // pass already-read content so resolveDeployIncludes does not need to
    // readFileSync just to scan for directives. The actual include
    // resolution still reads from git via getFileContent.
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- resolved functions\n",
    );
    commitFile(
      repoRoot,
      "deploy/init.sql",
      "\\i shared/funcs.sql\n",
    );

    // Pass the same content as scriptContent -- should still resolve
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/init.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      false,
      "\\i shared/funcs.sql\n",
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("resolved functions");
  });

  it("returns undefined when pre-read content has no includes", () => {
    commitFile(repoRoot, "deploy/init.sql", "\\i something.sql\n");

    // Pre-read content with no includes overrides directive scanning
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/init.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      false,
      "create table t (id int);\n", // no includes
    );

    expect(result).toBeUndefined();
  });
});
