// tests/unit/snapshot-includes-demo.test.ts — Demonstration that snapshot
// includes correctly resolve \i files from the git commit when a migration
// was planned, NOT from the current HEAD.
//
// Issue #163: Demo — snapshot includes, git-correlated \i resolution proven.
//
// This is the core value proposition of snapshot includes: when a shared
// file changes after a migration was written, replaying that migration
// should use the original version of the shared file (snapshot mode),
// not the current one.
//
// The test creates a realistic scenario:
//   - Commit 1: shared/get_version.sql returns "v1.0"
//   - Commit 1: create_schema migration includes shared/get_version.sql
//   - Commit 2: shared/get_version.sql updated to return "v2.0 -- BREAKING CHANGE"
//   - Commit 2: add_users migration also includes shared/get_version.sql
//
// Then verifies:
//   1. Snapshot mode resolves the v1 file for commit-1 migrations
//   2. Snapshot mode resolves the v2 file for commit-2 migrations
//   3. --no-snapshot resolves the current (v2) file for ALL migrations
//   4. This holds for nested includes, \ir directives, and multi-migration plans

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { execSync } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  resolveDeployIncludes,
  resolveIncludes,
  findCommitByTimestamp,
  getFileAtCommit,
} from "../../src/includes/snapshot";

// ---------------------------------------------------------------------------
// Helpers — temporary git repos
// ---------------------------------------------------------------------------

function makeTempDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-demo-"));
}

function initGitRepo(): string {
  const dir = makeTempDir();
  execSync("git init", { cwd: dir, stdio: "ignore" });
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
 * Commit multiple files atomically (single commit). Returns the commit hash.
 */
function commitFiles(
  repoRoot: string,
  files: Array<{ path: string; content: string }>,
  message: string,
  dateISO?: string,
): string {
  for (const file of files) {
    const absolutePath = join(repoRoot, file.path);
    const dir = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
    mkdirSync(dir, { recursive: true });
    writeFileSync(absolutePath, file.content);
    execSync(`git add "${file.path}"`, { cwd: repoRoot, stdio: "ignore" });
  }

  const env = dateISO
    ? { GIT_COMMITTER_DATE: dateISO }
    : undefined;
  const dateFlag = dateISO ? `--date="${dateISO}"` : "";
  const cmd = `git commit -m "${message}" ${dateFlag}`;

  execSync(cmd, {
    cwd: repoRoot,
    stdio: "ignore",
    env: env ? { ...process.env, ...env } : undefined,
  });
  return execSync("git rev-parse HEAD", { cwd: repoRoot })
    .toString()
    .trim();
}

function cleanupDir(dir: string): void {
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    // Best effort
  }
}

// ---------------------------------------------------------------------------
// Demo scenario: two migrations sharing a common included file
// ---------------------------------------------------------------------------

describe("snapshot includes demo -- git-correlated \\i resolution", () => {
  let repoRoot: string;

  // Commit hashes captured during setup
  let commit1Hash: string; // create_schema + shared/get_version.sql v1
  let commit2Hash: string; // add_users + shared/get_version.sql v2

  // Timestamps for planned_at simulation
  const PLANNED_AT_1 = "2025-06-01T12:00:00Z";
  const PLANNED_AT_2 = "2025-06-02T12:00:00Z";

  beforeEach(() => {
    repoRoot = initGitRepo();

    // -- Commit 1: shared/get_version.sql returns v1.0 --
    // Also create the create_schema migration that uses \i to include it.
    commit1Hash = commitFiles(
      repoRoot,
      [
        {
          path: "shared/get_version.sql",
          content: [
            "create or replace function get_version()",
            "returns text",
            "language sql",
            "as $$",
            "  select 'v1.0'::text;",
            "$$;",
            "",
          ].join("\n"),
        },
        {
          path: "deploy/create_schema.sql",
          content: [
            "-- Deploy create_schema",
            "create schema if not exists myapp;",
            "\\i shared/get_version.sql",
            "",
          ].join("\n"),
        },
      ],
      "add create_schema migration with get_version v1.0",
      PLANNED_AT_1,
    );

    // -- Commit 2: update shared/get_version.sql to v2.0, add add_users --
    commit2Hash = commitFiles(
      repoRoot,
      [
        {
          path: "shared/get_version.sql",
          content: [
            "create or replace function get_version()",
            "returns text",
            "language sql",
            "as $$",
            "  select 'v2.0 -- BREAKING CHANGE'::text;",
            "$$;",
            "",
          ].join("\n"),
        },
        {
          path: "deploy/add_users.sql",
          content: [
            "-- Deploy add_users",
            "create table myapp.users (id int8 generated always as identity);",
            "\\i shared/get_version.sql",
            "",
          ].join("\n"),
        },
      ],
      "add add_users migration with get_version v2.0",
      PLANNED_AT_2,
    );
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  // -------------------------------------------------------------------------
  // Test 1: git show verifies the file content at each commit
  // -------------------------------------------------------------------------

  it("git history contains correct versions of shared/get_version.sql", () => {
    const v1Content = getFileAtCommit(
      commit1Hash,
      "shared/get_version.sql",
      repoRoot,
    );
    expect(v1Content).toBeDefined();
    expect(v1Content).toContain("v1.0");
    expect(v1Content).not.toContain("v2.0");
    expect(v1Content).not.toContain("BREAKING CHANGE");

    const v2Content = getFileAtCommit(
      commit2Hash,
      "shared/get_version.sql",
      repoRoot,
    );
    expect(v2Content).toBeDefined();
    expect(v2Content).toContain("v2.0 -- BREAKING CHANGE");
    expect(v2Content).not.toContain("'v1.0'");
  });

  // -------------------------------------------------------------------------
  // Test 2: findCommitByTimestamp maps planned_at to the right commit
  // -------------------------------------------------------------------------

  it("findCommitByTimestamp resolves planned_at to the correct commit", () => {
    // A timestamp just after commit 1 should find commit 1
    const resolved1 = findCommitByTimestamp(
      "2025-06-01T12:00:01Z",
      repoRoot,
    );
    expect(resolved1).toBe(commit1Hash);

    // A timestamp just after commit 2 should find commit 2
    const resolved2 = findCommitByTimestamp(
      "2025-06-02T12:00:01Z",
      repoRoot,
    );
    expect(resolved2).toBe(commit2Hash);
  });

  // -------------------------------------------------------------------------
  // Test 3a: THE KEY DEMONSTRATION -- snapshot mode
  //
  // Deploy only create_schema (commit 1). The shared file has since changed
  // to v2.0 at HEAD. With snapshot mode, the included file should resolve
  // from commit 1, returning "v1.0" -- the historically correct version.
  // -------------------------------------------------------------------------

  it("snapshot: create_schema resolves shared file from commit 1 (v1.0)", () => {
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/create_schema.sql"),
      PLANNED_AT_1,
      repoRoot,
      commit1Hash,
      false, // noSnapshot = false (snapshot mode)
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("v1.0");
    expect(result!.content).not.toContain("v2.0");
    expect(result!.content).not.toContain("BREAKING CHANGE");
    expect(result!.includedFiles).toEqual(["shared/get_version.sql"]);
  });

  // -------------------------------------------------------------------------
  // Test 3b: THE KEY DEMONSTRATION -- no-snapshot mode
  //
  // Same migration (create_schema), but with --no-snapshot. The included
  // file resolves from HEAD, returning "v2.0 -- BREAKING CHANGE" --
  // which is NOT what the migration author intended.
  // -------------------------------------------------------------------------

  it("no-snapshot: create_schema resolves shared file from HEAD (v2.0)", () => {
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/create_schema.sql"),
      PLANNED_AT_1,
      repoRoot,
      commit1Hash,
      true, // noSnapshot = true
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("v2.0 -- BREAKING CHANGE");
    expect(result!.content).not.toContain("'v1.0'");
  });

  // -------------------------------------------------------------------------
  // Test 4: Both migrations deployed -- each gets the correct version
  // -------------------------------------------------------------------------

  it("snapshot: each migration resolves the shared file from its own commit", () => {
    // create_schema was planned at commit 1 -- should get v1.0
    const result1 = resolveDeployIncludes(
      join(repoRoot, "deploy/create_schema.sql"),
      PLANNED_AT_1,
      repoRoot,
      commit1Hash,
      false,
    );
    expect(result1).toBeDefined();
    expect(result1!.content).toContain("v1.0");
    expect(result1!.content).not.toContain("v2.0");

    // add_users was planned at commit 2 -- should get v2.0
    const result2 = resolveDeployIncludes(
      join(repoRoot, "deploy/add_users.sql"),
      PLANNED_AT_2,
      repoRoot,
      commit2Hash,
      false,
    );
    expect(result2).toBeDefined();
    expect(result2!.content).toContain("v2.0 -- BREAKING CHANGE");
  });

  // -------------------------------------------------------------------------
  // Test 5: no-snapshot mode -- both migrations see HEAD (v2.0)
  // -------------------------------------------------------------------------

  it("no-snapshot: both migrations resolve the shared file from HEAD", () => {
    const result1 = resolveDeployIncludes(
      join(repoRoot, "deploy/create_schema.sql"),
      PLANNED_AT_1,
      repoRoot,
      undefined,
      true,
    );
    expect(result1).toBeDefined();
    expect(result1!.content).toContain("v2.0 -- BREAKING CHANGE");

    const result2 = resolveDeployIncludes(
      join(repoRoot, "deploy/add_users.sql"),
      PLANNED_AT_2,
      repoRoot,
      undefined,
      true,
    );
    expect(result2).toBeDefined();
    expect(result2!.content).toContain("v2.0 -- BREAKING CHANGE");
  });

  // -------------------------------------------------------------------------
  // Test 6: resolveIncludes at the low level confirms the mechanism
  // -------------------------------------------------------------------------

  it("resolveIncludes with commit hash directly inlines the correct version", () => {
    // Resolve create_schema at commit 1
    const result = resolveIncludes("deploy/create_schema.sql", {
      commitHash: commit1Hash,
      repoRoot,
    });

    // The \i directive should be replaced with the v1.0 function body
    expect(result.content).toContain("v1.0");
    expect(result.content).not.toContain("v2.0");
    // The original migration SQL should be preserved
    expect(result.content).toContain("create schema if not exists myapp");
    // Diagnostic comments should bracket the included content
    expect(result.content).toContain(
      "-- [snapshot] begin include: shared/get_version.sql",
    );
    expect(result.content).toContain(
      "-- [snapshot] end include: shared/get_version.sql",
    );
  });

  // -------------------------------------------------------------------------
  // Test 7: timestamp-based commit resolution (no explicit hash)
  // -------------------------------------------------------------------------

  it("snapshot: planned_at timestamp resolves to the correct commit automatically", () => {
    // Use the high-level API without an explicit commit hash.
    // The planned_at timestamp should find the right commit via git log.
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/create_schema.sql"),
      // Use a timestamp just after PLANNED_AT_1 but before PLANNED_AT_2
      "2025-06-01T12:00:01Z",
      repoRoot,
      undefined, // no explicit commit hash
      false,     // snapshot mode
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("v1.0");
    expect(result!.content).not.toContain("v2.0");
  });
});

// ---------------------------------------------------------------------------
// Demo scenario: nested includes with version evolution
// ---------------------------------------------------------------------------

describe("snapshot includes demo -- nested includes across versions", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot resolves nested includes from the correct historical commit", () => {
    // Commit 1: base types v1, functions v1 (which includes types), deploy script
    const commit1 = commitFiles(
      repoRoot,
      [
        {
          path: "shared/types.sql",
          content: "create type app_role as enum ('user');\n",
        },
        {
          path: "shared/functions.sql",
          content: [
            "\\i shared/types.sql",
            "create function default_role() returns app_role",
            "language sql as $$ select 'user'::app_role; $$;",
            "",
          ].join("\n"),
        },
        {
          path: "deploy/001-init.sql",
          content: "\\i shared/functions.sql\n",
        },
      ],
      "v1: types and functions",
      "2025-03-01T10:00:00Z",
    );

    // Commit 2: types v2 (added 'admin'), functions v2 (default now 'admin')
    commitFiles(
      repoRoot,
      [
        {
          path: "shared/types.sql",
          content: "create type app_role as enum ('user', 'admin');\n",
        },
        {
          path: "shared/functions.sql",
          content: [
            "\\i shared/types.sql",
            "create function default_role() returns app_role",
            "language sql as $$ select 'admin'::app_role; $$;",
            "",
          ].join("\n"),
        },
        {
          path: "deploy/002-roles.sql",
          content: "\\i shared/functions.sql\n",
        },
      ],
      "v2: add admin role",
      "2025-03-02T10:00:00Z",
    );

    // Snapshot at commit 1: types should have only 'user', function returns 'user'
    const result1 = resolveDeployIncludes(
      join(repoRoot, "deploy/001-init.sql"),
      "2025-03-01T10:00:00Z",
      repoRoot,
      commit1,
      false,
    );
    expect(result1).toBeDefined();
    expect(result1!.content).toContain("('user')");
    expect(result1!.content).toContain("'user'::app_role");
    expect(result1!.content).not.toContain("'admin'");
    // Both shared files should be listed as included
    expect(result1!.includedFiles).toContain("shared/functions.sql");
    expect(result1!.includedFiles).toContain("shared/types.sql");

    // No-snapshot: should see the v2 versions (with admin)
    const resultNoSnapshot = resolveDeployIncludes(
      join(repoRoot, "deploy/001-init.sql"),
      "2025-03-01T10:00:00Z",
      repoRoot,
      undefined,
      true,
    );
    expect(resultNoSnapshot).toBeDefined();
    expect(resultNoSnapshot!.content).toContain("'admin'");
  });
});

// ---------------------------------------------------------------------------
// Demo scenario: \ir (relative) includes with version evolution
// ---------------------------------------------------------------------------

describe("snapshot includes demo -- \\ir relative includes", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot resolves \\ir includes from the correct historical commit", () => {
    // Commit 1: helper v1 next to the deploy script, included via \ir
    const commit1 = commitFiles(
      repoRoot,
      [
        {
          path: "deploy/helpers/setup.sql",
          content: "create extension if not exists pgcrypto; -- v1\n",
        },
        {
          path: "deploy/001-init.sql",
          content: [
            "-- Deploy 001",
            "\\ir helpers/setup.sql",
            "create table accounts (id uuid default gen_random_uuid());",
            "",
          ].join("\n"),
        },
      ],
      "v1: init with pgcrypto",
      "2025-04-01T10:00:00Z",
    );

    // Commit 2: update helper to also create uuid-ossp
    commitFiles(
      repoRoot,
      [
        {
          path: "deploy/helpers/setup.sql",
          content: [
            "create extension if not exists pgcrypto; -- v2",
            'create extension if not exists "uuid-ossp"; -- added in v2',
            "",
          ].join("\n"),
        },
      ],
      "v2: add uuid-ossp extension",
      "2025-04-02T10:00:00Z",
    );

    // Snapshot at commit 1: only pgcrypto, no uuid-ossp
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001-init.sql"),
      "2025-04-01T10:00:00Z",
      repoRoot,
      commit1,
      false,
    );
    expect(result).toBeDefined();
    expect(result!.content).toContain("pgcrypto; -- v1");
    expect(result!.content).not.toContain("uuid-ossp");
    expect(result!.includedFiles).toEqual(["deploy/helpers/setup.sql"]);

    // No-snapshot: gets uuid-ossp too
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/001-init.sql"),
      "2025-04-01T10:00:00Z",
      repoRoot,
      undefined,
      true,
    );
    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).toContain("uuid-ossp");
    expect(noSnapshotResult!.content).toContain("pgcrypto; -- v2");
  });
});

// ---------------------------------------------------------------------------
// Demo scenario: file added AFTER migration was planned
// ---------------------------------------------------------------------------

describe("snapshot includes demo -- file added after migration", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot falls back gracefully when included file did not exist at planned commit", () => {
    // Commit 1: deploy script that includes a file that does not yet exist
    const commit1 = commitFiles(
      repoRoot,
      [
        {
          path: "deploy/001-init.sql",
          content: "\\i shared/late_addition.sql\n",
        },
      ],
      "add migration referencing not-yet-created file",
      "2025-05-01T10:00:00Z",
    );

    // Commit 2: the included file is created
    commitFiles(
      repoRoot,
      [
        {
          path: "shared/late_addition.sql",
          content: "-- this file was added later\nselect 1;\n",
        },
      ],
      "add shared file",
      "2025-05-02T10:00:00Z",
    );

    // At commit 1, the file did not exist. The fallback chain in
    // getFileContent tries commit -> HEAD -> working tree. Since the
    // file exists at HEAD, it falls back to HEAD.
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001-init.sql"),
      "2025-05-01T10:00:00Z",
      repoRoot,
      commit1,
      false,
    );
    expect(result).toBeDefined();
    expect(result!.content).toContain("this file was added later");
  });
});

// ---------------------------------------------------------------------------
// Demo scenario: three-commit evolution proving isolation
// ---------------------------------------------------------------------------

describe("snapshot includes demo -- three-version evolution", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("each of three migrations sees the shared file as it was when planned", () => {
    // Version 1
    const c1 = commitFiles(
      repoRoot,
      [
        {
          path: "shared/config.sql",
          content: "select set_config('app.version', '1.0', false);\n",
        },
        {
          path: "deploy/m1.sql",
          content: "\\i shared/config.sql\n",
        },
      ],
      "v1",
      "2025-07-01T10:00:00Z",
    );

    // Version 2
    const c2 = commitFiles(
      repoRoot,
      [
        {
          path: "shared/config.sql",
          content: "select set_config('app.version', '2.0', false);\n",
        },
        {
          path: "deploy/m2.sql",
          content: "\\i shared/config.sql\n",
        },
      ],
      "v2",
      "2025-07-02T10:00:00Z",
    );

    // Version 3
    const c3 = commitFiles(
      repoRoot,
      [
        {
          path: "shared/config.sql",
          content: "select set_config('app.version', '3.0', false);\n",
        },
        {
          path: "deploy/m3.sql",
          content: "\\i shared/config.sql\n",
        },
      ],
      "v3",
      "2025-07-03T10:00:00Z",
    );

    // Each migration should resolve the config file from its own commit
    const r1 = resolveDeployIncludes(
      join(repoRoot, "deploy/m1.sql"),
      "2025-07-01T10:00:00Z",
      repoRoot,
      c1,
      false,
    );
    expect(r1).toBeDefined();
    expect(r1!.content).toContain("'1.0'");
    expect(r1!.content).not.toContain("'2.0'");
    expect(r1!.content).not.toContain("'3.0'");

    const r2 = resolveDeployIncludes(
      join(repoRoot, "deploy/m2.sql"),
      "2025-07-02T10:00:00Z",
      repoRoot,
      c2,
      false,
    );
    expect(r2).toBeDefined();
    expect(r2!.content).toContain("'2.0'");
    expect(r2!.content).not.toContain("'1.0'");
    expect(r2!.content).not.toContain("'3.0'");

    const r3 = resolveDeployIncludes(
      join(repoRoot, "deploy/m3.sql"),
      "2025-07-03T10:00:00Z",
      repoRoot,
      c3,
      false,
    );
    expect(r3).toBeDefined();
    expect(r3!.content).toContain("'3.0'");
    expect(r3!.content).not.toContain("'1.0'");
    expect(r3!.content).not.toContain("'2.0'");

    // With --no-snapshot, all three see v3 (HEAD)
    for (const migration of ["m1", "m2", "m3"]) {
      const rNoSnap = resolveDeployIncludes(
        join(repoRoot, `deploy/${migration}.sql`),
        "2025-07-01T10:00:00Z",
        repoRoot,
        undefined,
        true,
      );
      expect(rNoSnap).toBeDefined();
      expect(rNoSnap!.content).toContain("'3.0'");
    }
  });
});
