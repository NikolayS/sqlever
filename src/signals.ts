// Signal handling — graceful shutdown on SIGINT/SIGTERM
// See SPEC.md DD13: advisory lock release on all exit paths

type CleanupFn = () => void | Promise<void>;

export class ShutdownManager {
  private shuttingDown = false;
  private cleanupCallbacks: CleanupFn[] = [];
  private signalReceived: "SIGINT" | "SIGTERM" | null = null;
  private quiet = false;

  /**
   * Register SIGINT and SIGTERM handlers on the process.
   * Call this once at startup, before any database operations.
   *
   * @param options.quiet — suppress "Shutting down..." message
   * @param options.process_ — process object (for testing)
   */
  register(options?: {
    quiet?: boolean;
    process_?: NodeJS.Process;
  }): void {
    const proc = options?.process_ ?? process;
    this.quiet = options?.quiet ?? false;

    const handler = (signal: "SIGINT" | "SIGTERM") => {
      if (this.shuttingDown) {
        // Second signal — force exit immediately
        proc.exit(signal === "SIGINT" ? 130 : 143);
        return; // unreachable, but satisfies control flow
      }

      this.shuttingDown = true;
      this.signalReceived = signal;

      if (!this.quiet) {
        // Write directly to stderr to avoid buffering issues during shutdown
        proc.stderr.write("Shutting down...\n");
      }

      const exitCode = signal === "SIGINT" ? 130 : 143;

      this.runCleanup()
        .catch(() => {
          // Cleanup errors should not prevent exit.
          // The safety net (PG disconnects on process exit) handles
          // anything we miss here.
        })
        .finally(() => {
          proc.exit(exitCode);
        });
    };

    proc.on("SIGINT", () => handler("SIGINT"));
    proc.on("SIGTERM", () => handler("SIGTERM"));
  }

  /**
   * Register a cleanup callback to run during graceful shutdown.
   * Callbacks run in registration order. Each callback should be
   * short-lived and async-safe (no user interaction, no long waits).
   *
   * Typical uses:
   *   - Roll back in-flight transaction
   *   - Release advisory lock (pg_advisory_unlock)
   *   - Close database connection
   */
  onShutdown(fn: CleanupFn): void {
    this.cleanupCallbacks.push(fn);
  }

  /**
   * Returns true once the first signal has been received and shutdown
   * is in progress. Long-running operations should check this to
   * bail out early.
   */
  isShuttingDown(): boolean {
    return this.shuttingDown;
  }

  /**
   * Run all registered cleanup callbacks sequentially.
   * Errors in individual callbacks are caught so that subsequent
   * callbacks still run.
   */
  private async runCleanup(): Promise<void> {
    for (const fn of this.cleanupCallbacks) {
      try {
        await fn();
      } catch {
        // Swallow — best-effort cleanup. PG disconnect is the safety net.
      }
    }
  }
}

/**
 * Singleton instance for the application. Import and use directly:
 *
 *   import { shutdownManager } from "./signals";
 *   shutdownManager.register();
 *   shutdownManager.onShutdown(async () => { await db.end(); });
 */
export const shutdownManager = new ShutdownManager();
