// tests/integration/helpers.ts — shared utilities for integration tests
//
// Provides:
//   setupTestDb()    — create a fresh, isolated test database
//   teardownTestDb() — drop the test database
//   runSqlever(args) — spawn the sqlever binary/script and capture output
//   queryDb(sql)     — run a SQL query against the test database
//
// All functions use the PG connection defined by TEST_PG_URI (default:
// postgresql://postgres:test@localhost:5417/postgres).

import Client from "pg/lib/client";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Base URI pointing to the *admin* database (postgres). */
const PG_HOST = process.env.TEST_PG_HOST ?? "localhost";
const PG_PORT = Number(process.env.TEST_PG_PORT ?? "5417");
const PG_USER = process.env.TEST_PG_USER ?? "postgres";
const PG_PASS = process.env.TEST_PG_PASS ?? "test";

/** Build a postgresql:// URI for a given database name. */
export function pgUri(database: string): string {
  return `postgresql://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${database}`;
}

/** Admin URI used for CREATE/DROP DATABASE. */
const ADMIN_URI = pgUri("postgres");

// ---------------------------------------------------------------------------
// Database lifecycle
// ---------------------------------------------------------------------------

/**
 * Create a fresh test database with a unique name.
 *
 * Returns the database name. The caller is responsible for calling
 * `teardownTestDb(name)` when the test is done (typically in afterEach).
 */
export async function setupTestDb(
  suffix?: string,
): Promise<string> {
  const id = suffix ?? randomSuffix();
  const dbName = `sqlever_test_${id}`;

  const client = new Client({ connectionString: ADMIN_URI });
  await client.connect();
  try {
    // DROP in case a previous run left it behind
    await client.query(`DROP DATABASE IF EXISTS ${dbName}`);
    await client.query(`CREATE DATABASE ${dbName}`);
  } finally {
    await client.end();
  }

  return dbName;
}

/**
 * Drop the test database.
 *
 * Forces disconnection of any remaining backends first so the DROP succeeds
 * even if a test leaked connections.
 */
export async function teardownTestDb(dbName: string): Promise<void> {
  const client = new Client({ connectionString: ADMIN_URI });
  await client.connect();
  try {
    // Terminate lingering connections
    await client.query(
      `SELECT pg_terminate_backend(pid)
       FROM pg_stat_activity
       WHERE datname = $1 AND pid <> pg_backend_pid()`,
      [dbName],
    );
    await client.query(`DROP DATABASE IF EXISTS ${dbName}`);
  } finally {
    await client.end();
  }
}

// ---------------------------------------------------------------------------
// Query helper
// ---------------------------------------------------------------------------

/**
 * Execute a SQL query against a test database and return the result rows.
 *
 * Opens and closes a connection per call — acceptable for tests.
 */
export async function queryDb<T = Record<string, unknown>>(
  dbName: string,
  sql: string,
  params?: unknown[],
): Promise<T[]> {
  const client = new Client({ connectionString: pgUri(dbName) });
  await client.connect();
  try {
    const result = await client.query(sql, params);
    return result.rows as T[];
  } finally {
    await client.end();
  }
}

// ---------------------------------------------------------------------------
// sqlever runner
// ---------------------------------------------------------------------------

export interface RunResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

/**
 * Spawn the sqlever CLI with the given arguments and return its output.
 *
 * Runs `bun run src/cli.ts <...args>` from the project root so that the
 * test does not need a compiled binary. The `cwd` argument allows the
 * subprocess to run in a specific directory (e.g., a temp project dir).
 *
 * @param args - CLI arguments (e.g., ["init", "myproject"])
 * @param options.cwd - Working directory for the subprocess
 * @param options.env - Additional environment variables
 */
export async function runSqlever(
  args: string[],
  options: { cwd?: string; env?: Record<string, string> } = {},
): Promise<RunResult> {
  const projectRoot = new URL("../../", import.meta.url).pathname;
  const cliEntry = `${projectRoot}src/cli.ts`;

  const proc = Bun.spawn(["bun", "run", cliEntry, ...args], {
    cwd: options.cwd ?? projectRoot,
    env: {
      ...process.env,
      // Ensure consistent planner identity in tests
      SQLEVER_USER_NAME: "Test Runner",
      SQLEVER_USER_EMAIL: "test@example.com",
      ...options.env,
    },
    stdout: "pipe",
    stderr: "pipe",
  });

  const [stdout, stderr] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);

  const exitCode = await proc.exited;

  return { stdout, stderr, exitCode };
}

// ---------------------------------------------------------------------------
// PG availability check
// ---------------------------------------------------------------------------

/**
 * Check whether the test PostgreSQL instance is reachable.
 *
 * Uses a raw TCP socket probe instead of the `pg` client library to avoid
 * false positives when `pg/lib/client` is mocked by unit tests (e.g.,
 * client.test.ts uses `mock.module("pg/lib/client", ...)` which would make
 * the pg Client `connect()` silently succeed even without a real PG).
 */
export async function checkPgAvailable(): Promise<boolean> {
  const { createConnection } = await import("node:net");
  return new Promise<boolean>((resolve) => {
    const socket = createConnection(
      { host: PG_HOST, port: PG_PORT, timeout: 2_000 },
      () => {
        // TCP connection succeeded — PG port is listening
        socket.destroy();
        resolve(true);
      },
    );
    socket.on("error", () => resolve(false));
    socket.on("timeout", () => {
      socket.destroy();
      resolve(false);
    });
  });
}

/**
 * Eagerly-evaluated PG availability flag.
 *
 * Usage:
 *   import { hasPg } from "./helpers";
 *   describe.skipIf(!hasPg)("my PG tests", () => { ... });
 */
export const hasPg: boolean = await checkPgAvailable();

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

function randomSuffix(): string {
  return Math.random().toString(36).slice(2, 10);
}
