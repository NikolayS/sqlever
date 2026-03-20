import { describe, expect, it, beforeEach } from "bun:test";
import { ShutdownManager } from "../../src/signals";
import { EventEmitter } from "events";

/**
 * Create a mock process object that supports signal listeners and
 * tracks exit() calls and stderr writes, without affecting the real
 * process.
 */
function createMockProcess() {
  const emitter = new EventEmitter();
  const exits: number[] = [];
  const stderrWrites: string[] = [];

  const mock = Object.assign(emitter, {
    exit: (code: number) => {
      exits.push(code);
    },
    stderr: {
      write: (msg: string) => {
        stderrWrites.push(msg);
        return true;
      },
    },
  }) as unknown as NodeJS.Process;

  return { mock, exits, stderrWrites };
}

describe("ShutdownManager", () => {
  let manager: ShutdownManager;

  beforeEach(() => {
    manager = new ShutdownManager();
  });

  it("isShuttingDown() returns false initially", () => {
    expect(manager.isShuttingDown()).toBe(false);
  });

  it("registers SIGINT and SIGTERM handlers", () => {
    const { mock } = createMockProcess();
    manager.register({ process_: mock });

    expect(mock.listenerCount("SIGINT")).toBe(1);
    expect(mock.listenerCount("SIGTERM")).toBe(1);
  });

  it("sets shutting-down flag on first SIGINT", async () => {
    const { mock } = createMockProcess();
    manager.register({ process_: mock });

    mock.emit("SIGINT");

    // Allow microtask (cleanup promise) to settle
    await new Promise((r) => setTimeout(r, 10));

    expect(manager.isShuttingDown()).toBe(true);
  });

  it("exits with code 130 on SIGINT", async () => {
    const { mock, exits } = createMockProcess();
    manager.register({ process_: mock });

    mock.emit("SIGINT");
    await new Promise((r) => setTimeout(r, 10));

    expect(exits).toContain(130);
  });

  it("exits with code 143 on SIGTERM", async () => {
    const { mock, exits } = createMockProcess();
    manager.register({ process_: mock });

    mock.emit("SIGTERM");
    await new Promise((r) => setTimeout(r, 10));

    expect(exits).toContain(143);
  });

  it("prints 'Shutting down...' on first signal", async () => {
    const { mock, stderrWrites } = createMockProcess();
    manager.register({ process_: mock });

    mock.emit("SIGINT");
    await new Promise((r) => setTimeout(r, 10));

    expect(stderrWrites).toContain("Shutting down...\n");
  });

  it("suppresses message in quiet mode", async () => {
    const { mock, stderrWrites } = createMockProcess();
    manager.register({ quiet: true, process_: mock });

    mock.emit("SIGINT");
    await new Promise((r) => setTimeout(r, 10));

    expect(stderrWrites).toHaveLength(0);
  });

  it("runs cleanup callbacks on shutdown", async () => {
    const { mock } = createMockProcess();
    const called: string[] = [];

    manager.register({ process_: mock });
    manager.onShutdown(() => {
      called.push("first");
    });
    manager.onShutdown(async () => {
      called.push("second");
    });

    mock.emit("SIGTERM");
    await new Promise((r) => setTimeout(r, 50));

    expect(called).toEqual(["first", "second"]);
  });

  it("continues cleanup when a callback throws", async () => {
    const { mock, exits } = createMockProcess();
    const called: string[] = [];

    manager.register({ process_: mock });
    manager.onShutdown(() => {
      called.push("before-error");
    });
    manager.onShutdown(() => {
      throw new Error("cleanup failed");
    });
    manager.onShutdown(() => {
      called.push("after-error");
    });

    mock.emit("SIGINT");
    await new Promise((r) => setTimeout(r, 50));

    expect(called).toEqual(["before-error", "after-error"]);
    expect(exits).toContain(130);
  });

  it("force-exits on second signal (double Ctrl+C)", async () => {
    const { mock, exits } = createMockProcess();

    // Register a slow cleanup so the first signal is still processing
    manager.register({ process_: mock });
    manager.onShutdown(
      () => new Promise((r) => setTimeout(r, 500)),
    );

    // First signal — starts cleanup
    mock.emit("SIGINT");
    // Give just enough time for the flag to be set but cleanup is still running
    await new Promise((r) => setTimeout(r, 10));

    expect(manager.isShuttingDown()).toBe(true);
    // First signal hasn't exited yet (cleanup is still running)
    expect(exits).toHaveLength(0);

    // Second signal — force exit
    mock.emit("SIGINT");

    // The second signal triggers synchronous exit(130)
    expect(exits).toEqual([130]);
  });

  it("force-exits with 143 on double SIGTERM", async () => {
    const { mock, exits } = createMockProcess();

    manager.register({ process_: mock });
    manager.onShutdown(
      () => new Promise((r) => setTimeout(r, 500)),
    );

    mock.emit("SIGTERM");
    await new Promise((r) => setTimeout(r, 10));

    mock.emit("SIGTERM");

    expect(exits).toEqual([143]);
  });

  it("can register callbacks before register()", () => {
    const { mock } = createMockProcess();
    const called: string[] = [];

    // Register callback before calling register()
    manager.onShutdown(() => {
      called.push("early");
    });

    manager.register({ process_: mock });

    mock.emit("SIGINT");
    // Use a longer timeout to allow async cleanup
    return new Promise<void>((resolve) => {
      setTimeout(() => {
        expect(called).toEqual(["early"]);
        resolve();
      }, 50);
    });
  });
});
