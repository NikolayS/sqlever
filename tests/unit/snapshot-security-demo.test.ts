// tests/unit/snapshot-security-demo.test.ts -- Security demo for snapshot includes
//
// Proves that git-correlated \i resolution prevents a real class of security
// vulnerabilities. Without snapshot includes, replaying historical migrations
// on a fresh database silently picks up modified shared files -- potentially
// weakening row-level security, authentication functions, role grants, or
// check constraints.
//
// Issue: NikolayS/sqlever#165

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { execSync } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  resolveIncludes,
  resolveDeployIncludes,
  getHeadCommit,
} from "../../src/includes/snapshot";

// ---------------------------------------------------------------------------
// Helpers -- temporary git repos
// ---------------------------------------------------------------------------

function makeTempDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-sec-demo-"));
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

function commitFile(
  repoRoot: string,
  filePath: string,
  content: string,
  message?: string,
): string {
  const absolutePath = join(repoRoot, filePath);
  const dir = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
  mkdirSync(dir, { recursive: true });
  writeFileSync(absolutePath, content);
  execSync(`git add "${filePath}"`, { cwd: repoRoot, stdio: "ignore" });
  execSync(`git commit -m "${message ?? `add ${filePath}`}"`, {
    cwd: repoRoot,
    stdio: "ignore",
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
// Scenario 1: Row-level security policy bypass
//
// A shared row_level_security.sql starts with strict per-user isolation.
// A later migration relaxes it for admin access. Without snapshot includes,
// deploying migration 1 on a fresh database uses the relaxed policy --
// exposing all users' data from the moment the table is created.
// ---------------------------------------------------------------------------

describe("security demo: RLS policy bypass via modified shared file", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot includes preserve the strict RLS policy for migration 1", () => {
    // Step 1: commit the strict RLS policy (original, secure version)
    const strictPolicy = [
      "-- Row-level security: users can ONLY see their own data",
      "create policy user_isolation on orders",
      "    for all",
      "    using (user_id = current_setting('app.current_user_id')::bigint);",
    ].join("\n");
    const hashV1 = commitFile(
      repoRoot,
      "shared/row_level_security.sql",
      strictPolicy + "\n",
      "add strict RLS policy",
    );

    // Step 2: commit migration 1 that includes the strict policy
    const migration1 = [
      "create table orders (",
      "    id bigint generated always as identity primary key,",
      "    user_id bigint not null,",
      "    total numeric(10,2) not null,",
      "    created_at timestamptz not null default now()",
      ");",
      "alter table orders enable row level security;",
      "\\i shared/row_level_security.sql",
    ].join("\n");
    commitFile(
      repoRoot,
      "deploy/create_orders.sql",
      migration1 + "\n",
      "add create_orders migration",
    );

    // Step 3: later, the RLS policy is relaxed for admin access
    const relaxedPolicy = [
      "-- Row-level security: RELAXED for admin access",
      "create policy user_isolation on orders",
      "    for all",
      "    using (",
      "        user_id = current_setting('app.current_user_id')::bigint",
      "        or current_setting('app.is_admin', true)::boolean",
      "    );",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/row_level_security.sql",
      relaxedPolicy + "\n",
      "relax RLS for admin dashboard",
    );

    // WITH snapshot (sqlever default): migration 1 gets the STRICT policy
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_orders.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      hashV1,
      false, // noSnapshot = false (default)
    );

    expect(snapshotResult).toBeDefined();
    // The strict policy has only the user_id check
    expect(snapshotResult!.content).toContain("user_isolation");
    expect(snapshotResult!.content).toContain(
      "user_id = current_setting('app.current_user_id')::bigint",
    );
    // The admin bypass MUST NOT be present -- this is the security guarantee
    expect(snapshotResult!.content).not.toContain("app.is_admin");
    expect(snapshotResult!.content).not.toContain("RELAXED");

    // WITHOUT snapshot (--no-snapshot): migration 1 gets the RELAXED policy
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_orders.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      true, // noSnapshot = true
    );

    expect(noSnapshotResult).toBeDefined();
    // The relaxed policy includes the admin bypass -- SECURITY VULNERABILITY
    expect(noSnapshotResult!.content).toContain("app.is_admin");
    expect(noSnapshotResult!.content).toContain("RELAXED");
  });

  it("both migrations get the correct policy version for their time", () => {
    // Strict v1
    const strictPolicy = [
      "create policy user_isolation on orders",
      "    for all",
      "    using (user_id = current_setting('app.current_user_id')::bigint);",
    ].join("\n");
    const hashV1 = commitFile(
      repoRoot,
      "shared/row_level_security.sql",
      strictPolicy + "\n",
    );

    // Migration 1 at v1
    commitFile(
      repoRoot,
      "deploy/create_orders.sql",
      "create table orders (id bigint);\n\\i shared/row_level_security.sql\n",
    );

    // Relaxed v2
    const relaxedPolicy = [
      "create policy user_isolation on orders",
      "    for all",
      "    using (",
      "        user_id = current_setting('app.current_user_id')::bigint",
      "        or current_setting('app.is_admin', true)::boolean",
      "    );",
    ].join("\n");
    const hashV2 = commitFile(
      repoRoot,
      "shared/row_level_security.sql",
      relaxedPolicy + "\n",
    );

    // Migration 2 at v2
    commitFile(
      repoRoot,
      "deploy/add_admin_dashboard.sql",
      "create table admin_settings (id bigint);\n" +
        "drop policy if exists user_isolation on orders;\n" +
        "\\i shared/row_level_security.sql\n",
    );

    // Migration 1 @ hashV1: strict, no admin bypass
    const m1Result = resolveIncludes("deploy/create_orders.sql", {
      commitHash: hashV1,
      repoRoot,
    });
    expect(m1Result.content).not.toContain("app.is_admin");

    // Migration 2 @ hashV2: relaxed, with admin bypass
    const m2Result = resolveIncludes("deploy/add_admin_dashboard.sql", {
      commitHash: hashV2,
      repoRoot,
    });
    expect(m2Result.content).toContain("app.is_admin");
  });
});

// ---------------------------------------------------------------------------
// Scenario 2: Authentication function tampering
//
// A shared authenticate.sql defines a password-hashing function. A later
// change weakens it (e.g., switches from bcrypt to plain text comparison).
// Without snapshot includes, all earlier migrations that relied on the
// secure version silently use the weakened one on fresh deploys.
// ---------------------------------------------------------------------------

describe("security demo: authentication function tampering", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot includes preserve the secure auth function", () => {
    // v1: secure authentication with pgcrypto
    const secureAuth = [
      "create or replace function authenticate(p_email text, p_password text)",
      "returns boolean as $$",
      "begin",
      "    return exists (",
      "        select 1 from users",
      "        where email = p_email",
      "        and password_hash = crypt(p_password, password_hash)",
      "    );",
      "end;",
      "$$ language plpgsql security definer;",
    ].join("\n");
    const hashSecure = commitFile(
      repoRoot,
      "shared/authenticate.sql",
      secureAuth + "\n",
      "add secure auth function",
    );

    // Migration that sets up the auth system
    commitFile(
      repoRoot,
      "deploy/setup_auth.sql",
      "create extension if not exists pgcrypto;\n\\i shared/authenticate.sql\n",
      "add auth setup migration",
    );

    // v2: weakened authentication -- plain text comparison (insecure)
    const weakAuth = [
      "create or replace function authenticate(p_email text, p_password text)",
      "returns boolean as $$",
      "begin",
      "    -- TEMPORARY: skip hashing for dev convenience",
      "    return exists (",
      "        select 1 from users",
      "        where email = p_email",
      "        and password_hash = p_password",
      "    );",
      "end;",
      "$$ language plpgsql security definer;",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/authenticate.sql",
      weakAuth + "\n",
      "weaken auth for dev",
    );

    // Snapshot: migration gets the SECURE version with crypt()
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/setup_auth.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      hashSecure,
      false,
    );

    expect(snapshotResult).toBeDefined();
    expect(snapshotResult!.content).toContain("crypt(p_password, password_hash)");
    expect(snapshotResult!.content).not.toContain(
      "password_hash = p_password",
    );

    // No-snapshot: migration gets the WEAKENED version -- passwords compared in plain text
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/setup_auth.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      true,
    );

    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).toContain(
      "password_hash = p_password",
    );
    expect(noSnapshotResult!.content).not.toContain("crypt(");
  });
});

// ---------------------------------------------------------------------------
// Scenario 3: Role grants escalation
//
// A shared grants.sql initially gives a role SELECT-only access. A later
// migration expands it to include INSERT, UPDATE, DELETE. Without snapshot
// includes, the earliest migration already grants full DML access on fresh
// deploys -- violating least privilege.
// ---------------------------------------------------------------------------

describe("security demo: role privilege escalation via modified grants", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot includes enforce least-privilege grants for early migrations", () => {
    // v1: read-only access
    const readOnlyGrants = [
      "-- Grants for app_readonly role: SELECT only",
      "grant select on all tables in schema public to app_readonly;",
      "alter default privileges in schema public",
      "    grant select on tables to app_readonly;",
    ].join("\n");
    const hashReadOnly = commitFile(
      repoRoot,
      "shared/grants.sql",
      readOnlyGrants + "\n",
      "add read-only grants",
    );

    // Migration 1: sets up the readonly role
    commitFile(
      repoRoot,
      "deploy/create_roles.sql",
      "create role app_readonly;\n\\i shared/grants.sql\n",
      "add role creation migration",
    );

    // v2: full DML access (too permissive for the original migration)
    const fullGrants = [
      "-- Grants for app_readonly role: FULL DML (expanded for new features)",
      "grant select, insert, update, delete on all tables in schema public to app_readonly;",
      "alter default privileges in schema public",
      "    grant select, insert, update, delete on tables to app_readonly;",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/grants.sql",
      fullGrants + "\n",
      "expand grants for new features",
    );

    // Snapshot: migration 1 gets SELECT-only grants
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_roles.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      hashReadOnly,
      false,
    );

    expect(snapshotResult).toBeDefined();
    expect(snapshotResult!.content).toContain("grant select on all tables");
    expect(snapshotResult!.content).not.toContain("insert");
    expect(snapshotResult!.content).not.toContain("update");
    expect(snapshotResult!.content).not.toContain("delete");

    // No-snapshot: migration 1 grants full DML -- privilege escalation
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_roles.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      true,
    );

    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).toContain("insert");
    expect(noSnapshotResult!.content).toContain("update");
    expect(noSnapshotResult!.content).toContain("delete");
  });
});

// ---------------------------------------------------------------------------
// Scenario 4: Check constraint weakening
//
// A shared constraints.sql enforces strict data validation. A later migration
// relaxes it. Without snapshot includes, the relaxed constraints apply from
// the very first migration on fresh deploys, allowing invalid data.
// ---------------------------------------------------------------------------

describe("security demo: check constraint weakening", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot includes preserve strict validation constraints", () => {
    // v1: strict constraints -- amount must be positive, status limited
    const strictConstraints = [
      "alter table transactions",
      "    add constraint chk_amount_positive",
      "    check (amount > 0);",
      "",
      "alter table transactions",
      "    add constraint chk_status_valid",
      "    check (status in ('pending', 'completed', 'failed'));",
    ].join("\n");
    const hashStrict = commitFile(
      repoRoot,
      "shared/constraints.sql",
      strictConstraints + "\n",
      "add strict validation constraints",
    );

    // Migration 1: create transactions table with strict constraints
    commitFile(
      repoRoot,
      "deploy/create_transactions.sql",
      [
        "create table transactions (",
        "    id bigint generated always as identity primary key,",
        "    amount numeric(12,2) not null,",
        "    status text not null",
        ");",
        "\\i shared/constraints.sql",
      ].join("\n") + "\n",
      "add transactions table",
    );

    // v2: relaxed constraints -- amount can be zero or negative (refunds),
    // new statuses allowed
    const relaxedConstraints = [
      "alter table transactions",
      "    add constraint chk_amount_positive",
      "    check (amount >= -10000);",
      "",
      "alter table transactions",
      "    add constraint chk_status_valid",
      "    check (status in ('pending', 'completed', 'failed', 'reversed', 'voided', 'any'));",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/constraints.sql",
      relaxedConstraints + "\n",
      "relax constraints for refund feature",
    );

    // Snapshot: migration 1 gets strict constraints (amount > 0)
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_transactions.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      hashStrict,
      false,
    );

    expect(snapshotResult).toBeDefined();
    expect(snapshotResult!.content).toContain("check (amount > 0)");
    expect(snapshotResult!.content).not.toContain("-10000");
    expect(snapshotResult!.content).not.toContain("reversed");
    expect(snapshotResult!.content).not.toContain("voided");

    // No-snapshot: migration 1 gets relaxed constraints -- data integrity weakened
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_transactions.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      true,
    );

    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).toContain("-10000");
    expect(noSnapshotResult!.content).toContain("reversed");
  });
});

// ---------------------------------------------------------------------------
// Scenario 5: Audit trigger removal
//
// A shared audit_triggers.sql installs triggers that log all modifications
// for compliance. A later migration removes one trigger. Without snapshot
// includes, the earliest migration (which should have full auditing) silently
// runs without the removed trigger on fresh deploys -- compliance violation.
// ---------------------------------------------------------------------------

describe("security demo: audit trigger removal breaks compliance", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot includes preserve audit triggers for compliance", () => {
    // v1: full audit coverage -- insert, update, delete
    const fullAudit = [
      "create trigger audit_insert",
      "    after insert on sensitive_data",
      "    for each row execute function audit.log_change();",
      "",
      "create trigger audit_update",
      "    after update on sensitive_data",
      "    for each row execute function audit.log_change();",
      "",
      "create trigger audit_delete",
      "    after delete on sensitive_data",
      "    for each row execute function audit.log_change();",
    ].join("\n");
    const hashFull = commitFile(
      repoRoot,
      "shared/audit_triggers.sql",
      fullAudit + "\n",
      "add full audit triggers",
    );

    // Migration 1: create sensitive table with audit
    commitFile(
      repoRoot,
      "deploy/create_sensitive_data.sql",
      [
        "create table sensitive_data (",
        "    id bigint generated always as identity primary key,",
        "    ssn text not null,",
        "    medical_record text",
        ");",
        "\\i shared/audit_triggers.sql",
      ].join("\n") + "\n",
      "add sensitive data table",
    );

    // v2: delete trigger removed for "performance" -- compliance gap
    const partialAudit = [
      "create trigger audit_insert",
      "    after insert on sensitive_data",
      "    for each row execute function audit.log_change();",
      "",
      "create trigger audit_update",
      "    after update on sensitive_data",
      "    for each row execute function audit.log_change();",
      "",
      "-- audit_delete removed for performance optimization",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/audit_triggers.sql",
      partialAudit + "\n",
      "remove delete trigger for performance",
    );

    // Snapshot: migration gets ALL THREE triggers -- full compliance
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_sensitive_data.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      hashFull,
      false,
    );

    expect(snapshotResult).toBeDefined();
    expect(snapshotResult!.content).toContain("audit_insert");
    expect(snapshotResult!.content).toContain("audit_update");
    expect(snapshotResult!.content).toContain("audit_delete");

    // No-snapshot: migration MISSES the delete trigger -- compliance violation
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_sensitive_data.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      true,
    );

    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).toContain("audit_insert");
    expect(noSnapshotResult!.content).toContain("audit_update");
    // The delete trigger is gone -- silent compliance failure
    expect(noSnapshotResult!.content).not.toContain(
      "create trigger audit_delete",
    );
  });
});

// ---------------------------------------------------------------------------
// Scenario 6: Nested include chain -- security policy depends on auth helper
//
// shared/rls_policies.sql includes shared/auth_helpers.sql. Both files are
// modified in a later commit. Without snapshot includes, the nested chain
// silently uses the wrong versions on fresh deploys.
// ---------------------------------------------------------------------------

describe("security demo: nested include chain with multiple tampered files", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot includes preserve the entire nested chain at the correct version", () => {
    // v1: auth helper checks for valid JWT
    const authHelperV1 = [
      "create or replace function is_authenticated()",
      "returns boolean as $$",
      "begin",
      "    return current_setting('request.jwt.claims', true)::jsonb",
      "        ->> 'sub' is not null;",
      "end;",
      "$$ language plpgsql stable security definer;",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/auth_helpers.sql",
      authHelperV1 + "\n",
      "add auth helper v1",
    );

    // v1: RLS policy depends on auth helper
    const rlsV1 = [
      "\\i shared/auth_helpers.sql",
      "",
      "create policy require_auth on protected_data",
      "    for all",
      "    using (is_authenticated());",
    ].join("\n");
    const hashV1 = commitFile(
      repoRoot,
      "shared/rls_policies.sql",
      rlsV1 + "\n",
      "add RLS policy v1",
    );

    // Migration 1
    commitFile(
      repoRoot,
      "deploy/create_protected_data.sql",
      [
        "create table protected_data (",
        "    id bigint generated always as identity primary key,",
        "    payload jsonb not null",
        ");",
        "alter table protected_data enable row level security;",
        "\\i shared/rls_policies.sql",
      ].join("\n") + "\n",
      "add protected data table",
    );

    // v2: auth helper weakened -- always returns true in dev
    const authHelperV2 = [
      "create or replace function is_authenticated()",
      "returns boolean as $$",
      "begin",
      "    -- Always authenticated in development",
      "    return true;",
      "end;",
      "$$ language plpgsql stable security definer;",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/auth_helpers.sql",
      authHelperV2 + "\n",
      "weaken auth for dev mode",
    );

    // v2: RLS policy also weakened
    const rlsV2 = [
      "\\i shared/auth_helpers.sql",
      "",
      "create policy require_auth on protected_data",
      "    for all",
      "    using (true);",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/rls_policies.sql",
      rlsV2 + "\n",
      "relax RLS for dev mode",
    );

    // Snapshot: migration 1 gets the SECURE nested chain
    const snapshotResult = resolveIncludes(
      "deploy/create_protected_data.sql",
      { commitHash: hashV1, repoRoot },
    );

    // Auth helper checks JWT claims
    expect(snapshotResult.content).toContain("request.jwt.claims");
    expect(snapshotResult.content).toContain("is_authenticated()");
    // Must NOT have the weakened "return true" or "using (true)"
    expect(snapshotResult.content).not.toContain("return true;");
    expect(snapshotResult.content).not.toContain("using (true)");

    // No-snapshot: the entire nested chain uses HEAD versions
    const head = getHeadCommit(repoRoot);
    const noSnapshotResult = resolveIncludes(
      "deploy/create_protected_data.sql",
      { commitHash: head, repoRoot },
    );

    // Auth helper always returns true -- no real authentication
    expect(noSnapshotResult.content).toContain("return true;");
    // RLS policy uses (true) -- no real access control
    expect(noSnapshotResult.content).toContain("using (true)");
    // JWT check is gone
    expect(noSnapshotResult.content).not.toContain("request.jwt.claims");
  });
});

// ---------------------------------------------------------------------------
// Scenario 7: Multiple migrations reference the same shared file at
// different points in time
//
// Three migrations each include shared/permissions.sql. The file changes
// between each migration. Without snapshot includes, all three migrations
// use the latest version -- only the third migration should.
// ---------------------------------------------------------------------------

describe("security demo: three migrations, three versions of shared file", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("each migration gets exactly the permissions file from its commit", () => {
    // Version 1: minimal permissions
    const v1 = "grant usage on schema api to web_anon;\n";
    const hash1 = commitFile(repoRoot, "shared/permissions.sql", v1);
    commitFile(
      repoRoot,
      "deploy/m1_public_api.sql",
      "create schema api;\n\\i shared/permissions.sql\n",
    );

    // Version 2: add authenticated role
    const v2 = [
      "grant usage on schema api to web_anon;",
      "grant usage on schema api to authenticated;",
      "grant select on all tables in schema api to authenticated;",
    ].join("\n") + "\n";
    const hash2 = commitFile(repoRoot, "shared/permissions.sql", v2);
    commitFile(
      repoRoot,
      "deploy/m2_auth_api.sql",
      "create role authenticated;\n\\i shared/permissions.sql\n",
    );

    // Version 3: add admin with full access
    const v3 = [
      "grant usage on schema api to web_anon;",
      "grant usage on schema api to authenticated;",
      "grant select on all tables in schema api to authenticated;",
      "grant all on schema api to admin_role;",
      "grant all on all tables in schema api to admin_role;",
    ].join("\n") + "\n";
    const hash3 = commitFile(repoRoot, "shared/permissions.sql", v3);
    commitFile(
      repoRoot,
      "deploy/m3_admin.sql",
      "create role admin_role;\n\\i shared/permissions.sql\n",
    );

    // Migration 1 @ hash1: only web_anon grant
    const r1 = resolveIncludes("deploy/m1_public_api.sql", {
      commitHash: hash1,
      repoRoot,
    });
    expect(r1.content).toContain("web_anon");
    expect(r1.content).not.toContain("authenticated");
    expect(r1.content).not.toContain("admin_role");

    // Migration 2 @ hash2: web_anon + authenticated
    const r2 = resolveIncludes("deploy/m2_auth_api.sql", {
      commitHash: hash2,
      repoRoot,
    });
    expect(r2.content).toContain("web_anon");
    expect(r2.content).toContain("authenticated");
    expect(r2.content).not.toContain("admin_role");

    // Migration 3 @ hash3: all three roles
    const r3 = resolveIncludes("deploy/m3_admin.sql", {
      commitHash: hash3,
      repoRoot,
    });
    expect(r3.content).toContain("web_anon");
    expect(r3.content).toContain("authenticated");
    expect(r3.content).toContain("admin_role");

    // Without snapshot: ALL three migrations would get v3 (admin grants).
    // Migration 1 would grant admin_role permissions before admin_role
    // even exists, and migration 2 would grant admin access before
    // the admin feature is introduced.
    const head = getHeadCommit(repoRoot);
    const r1NoSnap = resolveIncludes("deploy/m1_public_api.sql", {
      commitHash: head,
      repoRoot,
    });
    // This is the vulnerability: migration 1 now includes admin_role grants
    expect(r1NoSnap.content).toContain("admin_role");
  });
});

// ---------------------------------------------------------------------------
// Scenario 8: Security definer function with privilege escalation
//
// A shared function defined with SECURITY DEFINER runs as the function
// owner (typically a superuser). If the function body is later modified
// to include additional operations, those operations inherit the elevated
// privileges on fresh deploys of old migrations.
// ---------------------------------------------------------------------------

describe("security demo: SECURITY DEFINER privilege escalation", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot includes prevent unintended superuser-context operations", () => {
    // v1: safe function -- only reads from public schema
    const safeFn = [
      "create or replace function api.get_user_profile(p_user_id bigint)",
      "returns jsonb as $$",
      "    select to_jsonb(u) from public.users as u",
      "    where u.id = p_user_id;",
      "$$ language sql stable security definer;",
    ].join("\n");
    const hashSafe = commitFile(
      repoRoot,
      "shared/api_functions.sql",
      safeFn + "\n",
      "add safe API function",
    );

    commitFile(
      repoRoot,
      "deploy/create_api.sql",
      "create schema if not exists api;\n\\i shared/api_functions.sql\n",
    );

    // v2: function now also creates roles and modifies system catalogs
    // (runs as superuser due to SECURITY DEFINER)
    const unsafeFn = [
      "create or replace function api.get_user_profile(p_user_id bigint)",
      "returns jsonb as $$",
      "declare",
      "    result jsonb;",
      "begin",
      "    select to_jsonb(u) into result from public.users as u",
      "    where u.id = p_user_id;",
      "    -- Added: sync user role (runs as superuser!)",
      "    execute format(",
      "        'create role if not exists user_%s',",
      "        p_user_id",
      "    );",
      "    return result;",
      "end;",
      "$$ language plpgsql volatile security definer;",
    ].join("\n");
    commitFile(
      repoRoot,
      "shared/api_functions.sql",
      unsafeFn + "\n",
      "add role sync to API function",
    );

    // Snapshot: original safe function -- no role creation
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_api.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      hashSafe,
      false,
    );

    expect(snapshotResult).toBeDefined();
    expect(snapshotResult!.content).toContain("to_jsonb(u)");
    expect(snapshotResult!.content).not.toContain("create role");
    expect(snapshotResult!.content).toContain("sql stable security definer");

    // No-snapshot: function now creates roles via superuser context
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/create_api.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      true,
    );

    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).toContain("create role");
    expect(noSnapshotResult!.content).toContain("plpgsql volatile security definer");
  });
});
