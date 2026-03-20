// src/lock-guard.ts — Lock timeout guard for deploy scripts
//
// See SPEC.md Section 5.9: Automatically set lock_timeout before DDL
// execution to prevent runaway lock waits. If the migration script
// already sets lock_timeout, the auto-prepend is skipped.
//
// Also provides retry-with-backoff for CI pipelines where transient
// lock conflicts should be retried automatically (--lock-retries N).

// ---------------------------------------------------------------------------
// Detection: does the script already set lock_timeout?
// ---------------------------------------------------------------------------

/**
 * Check whether a SQL script already contains a `SET lock_timeout`
 * statement at the top level. Case-insensitive search.
 *
 * When this returns true, the auto-prepend is skipped for that script
 * (per SPEC 5.9: "Per-migration override").
 *
 * Matches patterns like:
 *   SET lock_timeout = '5s';
 *   SET lock_timeout = 5000;
 *   set Lock_Timeout = '10s';
 *   SET lock_timeout TO '5s';
 */
export function shouldSetLockTimeout(scriptContent: string): boolean {
  // If the script already sets lock_timeout, we should NOT auto-set it.
  // So this function returns true when we SHOULD set it (i.e., the script
  // does NOT already contain SET lock_timeout).
  const pattern = /\bSET\s+lock_timeout\b/i;
  return !pattern.test(scriptContent);
}

// ---------------------------------------------------------------------------
// Build the SET lock_timeout prefix
// ---------------------------------------------------------------------------

/**
 * Build the SQL statement that sets lock_timeout for a psql session.
 *
 * @param timeoutMs — timeout in milliseconds (e.g., 5000 for 5s)
 * @returns SQL SET statement with trailing newline
 */
export function buildLockTimeoutPrefix(timeoutMs: number): string {
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new Error(
      `Invalid lock timeout: ${timeoutMs}. Must be a non-negative finite number.`,
    );
  }
  return `SET lock_timeout = '${timeoutMs}ms';\n`;
}

// ---------------------------------------------------------------------------
// Lock timeout error detection
// ---------------------------------------------------------------------------

/**
 * Check whether an error message indicates a lock timeout.
 *
 * PostgreSQL raises error code 55P03 (lock_not_available) when
 * lock_timeout fires. The psql stderr typically contains:
 *   ERROR:  canceling statement due to lock timeout
 */
export function isLockTimeoutError(errorMessage: string): boolean {
  // Match the PostgreSQL lock timeout error message
  const patterns = [
    /canceling statement due to lock timeout/i,
    /could not obtain lock/i,
    /lock_not_available/i,
  ];
  return patterns.some((p) => p.test(errorMessage));
}

// ---------------------------------------------------------------------------
// Retry with exponential backoff
// ---------------------------------------------------------------------------

/** Options for the retry-with-backoff function. */
export interface RetryOptions {
  /** Maximum number of retries (0 = no retry, just run once). */
  maxRetries: number;

  /** Initial delay in milliseconds before the first retry. Default: 1000. */
  initialDelayMs?: number;

  /** Maximum delay in milliseconds (cap for exponential growth). Default: 30000. */
  maxDelayMs?: number;

  /**
   * Optional predicate to decide whether a given error is retryable.
   * If not provided, all errors are retried.
   */
  shouldRetry?: (error: unknown) => boolean;

  /**
   * Optional callback invoked before each retry attempt.
   * Useful for logging retry information.
   */
  onRetry?: (attempt: number, error: unknown, delayMs: number) => void;
}

/**
 * Retry a function with exponential backoff.
 *
 * Designed for CI pipelines where transient lock conflicts should be
 * retried automatically (SPEC 5.9: --lock-retries N).
 *
 * Backoff schedule: initialDelayMs * 2^(attempt-1), capped at maxDelayMs.
 * Example with defaults: 1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
 *
 * @param fn — the async function to execute
 * @param options — retry configuration
 * @returns the result of fn on success
 * @throws the last error after all retries are exhausted
 */
export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  options: RetryOptions,
): Promise<T> {
  const {
    maxRetries,
    initialDelayMs = 1000,
    maxDelayMs = 30_000,
    shouldRetry,
    onRetry,
  } = options;

  let lastError: unknown;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error: unknown) {
      lastError = error;

      // If we've used all retries, throw
      if (attempt >= maxRetries) {
        throw error;
      }

      // If the error is not retryable, throw immediately
      if (shouldRetry && !shouldRetry(error)) {
        throw error;
      }

      // Calculate delay with exponential backoff
      const delay = Math.min(initialDelayMs * Math.pow(2, attempt), maxDelayMs);

      // Notify before sleeping
      if (onRetry) {
        onRetry(attempt + 1, error, delay);
      }

      // Sleep
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  // This should be unreachable, but TypeScript needs it
  throw lastError;
}

// ---------------------------------------------------------------------------
// Build psql -c argument for SET lock_timeout
// ---------------------------------------------------------------------------

/**
 * Build the psql `-c` argument value for setting lock_timeout.
 *
 * This is used by PsqlRunner to prepend the SET command to the psql
 * invocation via `-c "SET lock_timeout = '<ms>ms'"` before `-f script.sql`.
 *
 * @param timeoutMs — timeout in milliseconds
 * @returns the SQL command string (without the -c flag itself)
 */
export function buildLockTimeoutCommand(timeoutMs: number): string {
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new Error(
      `Invalid lock timeout: ${timeoutMs}. Must be a non-negative finite number.`,
    );
  }
  return `SET lock_timeout = '${timeoutMs}ms'`;
}
