/**
 * End-to-end tests for the compiled sqlever binary.
 *
 * These tests build the binary via `bun run build`, then exercise it as an
 * external process -- verifying --help, --version, analyze on clean and
 * problematic SQL, JSON output, error handling, binary size, and startup time.
 *
 * Requires: bun 1.1+, no database needed.
 */

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { join } from "node:path";
import {
  existsSync,
  mkdirSync,
  writeFileSync,
  rmSync,
  statSync,
} from "node:fs";

const ROOT = join(import.meta.dir, "..", "..");
const BINARY = join(ROOT, "dist", "sqlever");
const TMP_DIR = join(import.meta.dir, "..", ".tmp-e2e-compiled");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Run the compiled binary with the given arguments. */
async function run(
  args: string[],
  options?: { timeout?: number; cwd?: string },
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const proc = Bun.spawn([BINARY, ...args], {
    stdout: "pipe",
    stderr: "pipe",
    cwd: options?.cwd ?? ROOT,
  });

  const timeout = options?.timeout ?? 30_000;
  const timer = setTimeout(() => proc.kill(), timeout);

  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);

  clearTimeout(timer);
  return { stdout, stderr, exitCode };
}

// ---------------------------------------------------------------------------
// Setup: build binary and create test fixtures
// ---------------------------------------------------------------------------

beforeAll(async () => {
  // Build the binary
  const buildProc = Bun.spawn(["bun", "run", "build"], {
    stdout: "pipe",
    stderr: "pipe",
    cwd: ROOT,
  });
  const buildExit = await buildProc.exited;
  if (buildExit !== 0) {
    const stderr = await new Response(buildProc.stderr).text();
    throw new Error(`Build failed with exit code ${buildExit}: ${stderr}`);
  }

  // Verify binary exists
  if (!existsSync(BINARY)) {
    throw new Error(`Binary not found at ${BINARY} after build`);
  }

  // Create temp fixtures
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
  mkdirSync(TMP_DIR, { recursive: true });

  // Clean SQL -- no analysis findings expected
  writeFileSync(
    join(TMP_DIR, "clean.sql"),
    "create table if not exists t (id int8 generated always as identity primary key);\n",
  );

  // SQL with issues -- CREATE INDEX without CONCURRENTLY triggers SA004
  writeFileSync(
    join(TMP_DIR, "has_issues.sql"),
    "create index idx_t_id on t (id);\n",
  );

  // Broken SQL -- parse error
  writeFileSync(join(TMP_DIR, "broken.sql"), "create tabl oops;\n");

  // UPDATE without WHERE -- triggers SA010
  writeFileSync(join(TMP_DIR, "no_where.sql"), "update t set x = 1;\n");
}, 120_000);

afterAll(() => {
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
});

// ---------------------------------------------------------------------------
// Binary existence and size
// ---------------------------------------------------------------------------

describe("binary artifact", () => {
  test("dist/sqlever exists after build", () => {
    expect(existsSync(BINARY)).toBe(true);
  });

  test("binary size is under 100 MiB", () => {
    const stat = statSync(BINARY);
    const sizeInMiB = stat.size / (1024 * 1024);
    expect(sizeInMiB).toBeLessThan(100);
  });
});

// ---------------------------------------------------------------------------
// --version
// ---------------------------------------------------------------------------

describe("--version", () => {
  test("prints semver version and exits 0", async () => {
    const { stdout, exitCode } = await run(["--version"]);
    expect(exitCode).toBe(0);
    expect(stdout.trim()).toMatch(/^\d+\.\d+\.\d+$/);
  });

  test("startup time is under 500ms", async () => {
    // Run a few times to get a representative measurement
    const times: number[] = [];
    for (let i = 0; i < 3; i++) {
      const start = performance.now();
      await run(["--version"]);
      times.push(performance.now() - start);
    }
    times.sort((a, b) => a - b);
    const median = times[Math.floor(times.length / 2)]!;
    // Generous threshold -- compiled binary should be fast, but CI may be slow
    expect(median).toBeLessThan(500);
  });
});

// ---------------------------------------------------------------------------
// --help
// ---------------------------------------------------------------------------

describe("--help", () => {
  test("prints usage information and exits 0", async () => {
    const { stdout, exitCode } = await run(["--help"]);
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Usage:");
    expect(stdout).toContain("Commands:");
    expect(stdout).toContain("deploy");
    expect(stdout).toContain("revert");
    expect(stdout).toContain("analyze");
  });

  test("lists global options", async () => {
    const { stdout } = await run(["--help"]);
    expect(stdout).toContain("--help");
    expect(stdout).toContain("--version");
    expect(stdout).toContain("--format");
    expect(stdout).toContain("--db-uri");
  });
});

// ---------------------------------------------------------------------------
// analyze -- clean file
// ---------------------------------------------------------------------------

describe("analyze clean file", () => {
  test("exits 0 on clean SQL", async () => {
    const { exitCode } = await run([
      "analyze",
      join(TMP_DIR, "clean.sql"),
    ]);
    expect(exitCode).toBe(0);
  });

  test("text output reports no issues", async () => {
    const { stdout, exitCode } = await run([
      "analyze",
      join(TMP_DIR, "clean.sql"),
    ]);
    expect(exitCode).toBe(0);
    expect(stdout).toContain("No issues found");
  });
});

// ---------------------------------------------------------------------------
// analyze -- file with findings
// ---------------------------------------------------------------------------

describe("analyze file with issues", () => {
  test("reports SA004 for CREATE INDEX without CONCURRENTLY", async () => {
    const { stdout, exitCode } = await run([
      "analyze",
      join(TMP_DIR, "has_issues.sql"),
    ]);
    expect(stdout).toContain("SA004");
    // SA004 is a warning, so exit code is 0 (warnings only, no --strict)
    expect(exitCode).toBe(0);
  });

  test("reports SA010 for UPDATE without WHERE", async () => {
    const { stdout } = await run([
      "analyze",
      join(TMP_DIR, "no_where.sql"),
    ]);
    expect(stdout).toContain("SA010");
  });
});

// ---------------------------------------------------------------------------
// analyze --format json
// ---------------------------------------------------------------------------

describe("analyze --format json", () => {
  test("outputs valid JSON for clean file", async () => {
    const { stdout, exitCode } = await run([
      "analyze",
      "--format",
      "json",
      join(TMP_DIR, "clean.sql"),
    ]);
    expect(exitCode).toBe(0);
    const parsed = JSON.parse(stdout);
    expect(parsed.version).toBe(1);
    expect(parsed.metadata.files_analyzed).toBe(1);
    expect(Array.isArray(parsed.findings)).toBe(true);
    expect(parsed.findings.length).toBe(0);
    expect(parsed.summary).toBeDefined();
  });

  test("outputs valid JSON with findings", async () => {
    const { stdout } = await run([
      "analyze",
      "--format",
      "json",
      join(TMP_DIR, "has_issues.sql"),
    ]);
    const parsed = JSON.parse(stdout);
    expect(parsed.version).toBe(1);
    expect(Array.isArray(parsed.findings)).toBe(true);
    const sa004 = parsed.findings.filter(
      (f: { ruleId: string }) => f.ruleId === "SA004",
    );
    expect(sa004.length).toBeGreaterThan(0);
  });

  test("JSON output includes summary counts", async () => {
    const { stdout } = await run([
      "analyze",
      "--format",
      "json",
      join(TMP_DIR, "no_where.sql"),
    ]);
    const parsed = JSON.parse(stdout);
    expect(typeof parsed.summary.warnings).toBe("number");
    expect(parsed.summary.warnings).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe("error handling", () => {
  test("nonexistent file produces error and non-zero exit", async () => {
    const { exitCode, stderr } = await run([
      "analyze",
      join(TMP_DIR, "does_not_exist.sql"),
    ]);
    expect(exitCode).not.toBe(0);
    expect(stderr.length).toBeGreaterThan(0);
  });

  test("broken SQL produces parse-error finding and exit 2", async () => {
    const { stdout, exitCode } = await run([
      "analyze",
      "--format",
      "json",
      join(TMP_DIR, "broken.sql"),
    ]);
    expect(exitCode).toBe(2);
    const parsed = JSON.parse(stdout);
    const parseErrors = parsed.findings.filter(
      (f: { ruleId: string }) => f.ruleId === "parse-error",
    );
    expect(parseErrors.length).toBe(1);
  });

  test("unknown command produces error and exit 1", async () => {
    const { exitCode, stderr } = await run(["notacommand"]);
    expect(exitCode).toBe(1);
    expect(stderr).toContain("unknown command");
  });

  test("no arguments prints help and exits 0", async () => {
    const { stdout, exitCode } = await run([]);
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Usage:");
  });
});

// ---------------------------------------------------------------------------
// init and add -- project scaffolding via compiled binary
// ---------------------------------------------------------------------------

describe("init and add", () => {
  // init creates files in cwd, using the argument as the project name
  const PROJECT_DIR = join(TMP_DIR, "myproject");

  test("init creates project files in the target directory", async () => {
    mkdirSync(PROJECT_DIR, { recursive: true });
    const { exitCode } = await run(["init", "testproject"], {
      cwd: PROJECT_DIR,
    });
    expect(exitCode).toBe(0);
    expect(existsSync(join(PROJECT_DIR, "sqitch.plan"))).toBe(true);
    expect(existsSync(join(PROJECT_DIR, "deploy"))).toBe(true);
    expect(existsSync(join(PROJECT_DIR, "revert"))).toBe(true);
    expect(existsSync(join(PROJECT_DIR, "verify"))).toBe(true);
  });

  test("add creates migration scripts", async () => {
    const { exitCode } = await run(
      ["add", "create_users", "-n", "add users table"],
      { cwd: PROJECT_DIR },
    );
    expect(exitCode).toBe(0);
    expect(existsSync(join(PROJECT_DIR, "deploy", "create_users.sql"))).toBe(
      true,
    );
    expect(existsSync(join(PROJECT_DIR, "revert", "create_users.sql"))).toBe(
      true,
    );
    expect(existsSync(join(PROJECT_DIR, "verify", "create_users.sql"))).toBe(
      true,
    );
  });

  test("analyze runs on scaffolded deploy script", async () => {
    const { exitCode } = await run([
      "analyze",
      join(PROJECT_DIR, "deploy", "create_users.sql"),
    ]);
    // Scaffolded deploy scripts trigger SA025 (BEGIN inside migration),
    // but that is a warning so exit code remains 0
    expect(exitCode).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// WASM parser verification (libpg-query works in compiled binary)
// ---------------------------------------------------------------------------

describe("WASM parser in compiled binary", () => {
  test("parses complex SQL from edge-cases fixture", async () => {
    const fixture = join(ROOT, "tests", "fixtures", "edge-cases.sql");
    if (!existsSync(fixture)) {
      // Skip if fixture does not exist
      return;
    }
    const { exitCode } = await run([
      "analyze",
      "--format",
      "json",
      fixture,
    ]);
    // edge-cases.sql may have findings, but should not crash
    expect(exitCode === 0 || exitCode === 2).toBe(true);
  });
});
