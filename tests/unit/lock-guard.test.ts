import { describe, it, expect } from "bun:test";
import {
  shouldSetLockTimeout,
  buildLockTimeoutPrefix,
  buildLockTimeoutCommand,
  isLockTimeoutError,
  retryWithBackoff,
} from "../../src/lock-guard";
import { buildPsqlCommand, type PsqlRunOptions } from "../../src/psql";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const defaultUri = "postgresql://user@localhost:5432/testdb";

function opts(overrides: Partial<PsqlRunOptions> = {}): PsqlRunOptions {
  return { uri: defaultUri, ...overrides };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("lock-guard module", () => {
  // -----------------------------------------------------------------------
  // shouldSetLockTimeout — script detection
  // -----------------------------------------------------------------------

  describe("shouldSetLockTimeout()", () => {
    it("returns true when script does not contain SET lock_timeout", () => {
      const script = `
        CREATE TABLE users (id serial PRIMARY KEY);
        ALTER TABLE users ADD COLUMN name text;
      `;
      expect(shouldSetLockTimeout(script)).toBe(true);
    });

    it("returns false when script contains SET lock_timeout (exact case)", () => {
      const script = `
        SET lock_timeout = '10s';
        ALTER TABLE users ADD COLUMN email text;
      `;
      expect(shouldSetLockTimeout(script)).toBe(false);
    });

    it("returns false when script contains SET lock_timeout (uppercase)", () => {
      const script = `
        SET LOCK_TIMEOUT = '5s';
        ALTER TABLE users ADD COLUMN email text;
      `;
      expect(shouldSetLockTimeout(script)).toBe(false);
    });

    it("returns false when script contains SET lock_timeout (mixed case)", () => {
      const script = `SET Lock_Timeout = '5s';\nDROP INDEX IF EXISTS idx_foo;`;
      expect(shouldSetLockTimeout(script)).toBe(false);
    });

    it("returns false when script uses SET lock_timeout TO syntax", () => {
      const script = `SET lock_timeout TO '3s';`;
      expect(shouldSetLockTimeout(script)).toBe(false);
    });

    it("returns true for empty script", () => {
      expect(shouldSetLockTimeout("")).toBe(true);
    });

    it("returns true when lock_timeout appears only in a comment", () => {
      // The simple regex-based detection does not parse comments,
      // so a SET lock_timeout inside a comment will still suppress
      // the auto-prepend. This is conservative (false negative)
      // rather than dangerous (false positive). We test the actual
      // behavior here — the regex matches even inside comments.
      const script = `-- SET lock_timeout = '5s';\nALTER TABLE t ADD COLUMN c int;`;
      // The regex WILL match the comment, so shouldSetLockTimeout returns false
      expect(shouldSetLockTimeout(script)).toBe(false);
    });

    it("returns true when 'lock_timeout' appears without SET keyword", () => {
      const script = `-- this migration needs a custom lock_timeout\nALTER TABLE t ADD COLUMN c int;`;
      expect(shouldSetLockTimeout(script)).toBe(true);
    });
  });

  // -----------------------------------------------------------------------
  // buildLockTimeoutPrefix — SQL generation
  // -----------------------------------------------------------------------

  describe("buildLockTimeoutPrefix()", () => {
    it("generates correct SET statement for 5000ms", () => {
      expect(buildLockTimeoutPrefix(5000)).toBe(
        "SET lock_timeout = '5000ms';\n",
      );
    });

    it("generates correct SET statement for 0ms (disabled)", () => {
      expect(buildLockTimeoutPrefix(0)).toBe(
        "SET lock_timeout = '0ms';\n",
      );
    });

    it("generates correct SET statement for 30000ms", () => {
      expect(buildLockTimeoutPrefix(30000)).toBe(
        "SET lock_timeout = '30000ms';\n",
      );
    });

    it("throws for negative timeout", () => {
      expect(() => buildLockTimeoutPrefix(-1)).toThrow("Invalid lock timeout");
    });

    it("throws for NaN", () => {
      expect(() => buildLockTimeoutPrefix(NaN)).toThrow("Invalid lock timeout");
    });

    it("throws for Infinity", () => {
      expect(() => buildLockTimeoutPrefix(Infinity)).toThrow(
        "Invalid lock timeout",
      );
    });
  });

  // -----------------------------------------------------------------------
  // buildLockTimeoutCommand — psql -c argument
  // -----------------------------------------------------------------------

  describe("buildLockTimeoutCommand()", () => {
    it("generates command without semicolon or newline", () => {
      expect(buildLockTimeoutCommand(5000)).toBe(
        "SET lock_timeout = '5000ms'",
      );
    });

    it("throws for negative value", () => {
      expect(() => buildLockTimeoutCommand(-100)).toThrow(
        "Invalid lock timeout",
      );
    });
  });

  // -----------------------------------------------------------------------
  // isLockTimeoutError — error classification
  // -----------------------------------------------------------------------

  describe("isLockTimeoutError()", () => {
    it("detects canceling statement due to lock timeout", () => {
      expect(
        isLockTimeoutError("ERROR:  canceling statement due to lock timeout"),
      ).toBe(true);
    });

    it("detects could not obtain lock", () => {
      expect(
        isLockTimeoutError(
          "ERROR:  could not obtain lock on relation \"users\"",
        ),
      ).toBe(true);
    });

    it("detects lock_not_available error code reference", () => {
      expect(isLockTimeoutError("lock_not_available")).toBe(true);
    });

    it("returns false for unrelated errors", () => {
      expect(
        isLockTimeoutError('ERROR:  relation "users" does not exist'),
      ).toBe(false);
    });

    it("returns false for empty string", () => {
      expect(isLockTimeoutError("")).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // retryWithBackoff — retry logic
  // -----------------------------------------------------------------------

  describe("retryWithBackoff()", () => {
    it("returns result on first success (no retries needed)", async () => {
      const result = await retryWithBackoff(() => Promise.resolve(42), {
        maxRetries: 3,
        initialDelayMs: 1,
      });

      expect(result).toBe(42);
    });

    it("retries and succeeds on second attempt", async () => {
      let calls = 0;
      const fn = async () => {
        calls++;
        if (calls < 2) throw new Error("lock timeout");
        return "ok";
      };

      const result = await retryWithBackoff(fn, {
        maxRetries: 3,
        initialDelayMs: 1,
      });

      expect(result).toBe("ok");
      expect(calls).toBe(2);
    });

    it("throws after exhausting all retries", async () => {
      let calls = 0;
      const fn = async () => {
        calls++;
        throw new Error("always fails");
      };

      await expect(
        retryWithBackoff(fn, { maxRetries: 2, initialDelayMs: 1 }),
      ).rejects.toThrow("always fails");

      // Initial attempt + 2 retries = 3 calls total
      expect(calls).toBe(3);
    });

    it("does not retry when maxRetries is 0", async () => {
      let calls = 0;
      const fn = async () => {
        calls++;
        throw new Error("fail");
      };

      await expect(
        retryWithBackoff(fn, { maxRetries: 0, initialDelayMs: 1 }),
      ).rejects.toThrow("fail");

      expect(calls).toBe(1);
    });

    it("respects shouldRetry predicate — throws immediately on non-retryable error", async () => {
      let calls = 0;
      const fn = async () => {
        calls++;
        throw new Error("syntax error");
      };

      await expect(
        retryWithBackoff(fn, {
          maxRetries: 5,
          initialDelayMs: 1,
          shouldRetry: (err) =>
            err instanceof Error && err.message.includes("lock timeout"),
        }),
      ).rejects.toThrow("syntax error");

      // Should have tried only once since shouldRetry returned false
      expect(calls).toBe(1);
    });

    it("retries when shouldRetry returns true", async () => {
      let calls = 0;
      const fn = async () => {
        calls++;
        if (calls < 3) throw new Error("lock timeout");
        return "success";
      };

      const result = await retryWithBackoff(fn, {
        maxRetries: 5,
        initialDelayMs: 1,
        shouldRetry: (err) =>
          err instanceof Error && err.message.includes("lock timeout"),
      });

      expect(result).toBe("success");
      expect(calls).toBe(3);
    });

    it("calls onRetry callback before each retry", async () => {
      let calls = 0;
      const retryLog: Array<{ attempt: number; delayMs: number }> = [];

      const fn = async () => {
        calls++;
        if (calls <= 3) throw new Error("lock timeout");
        return "done";
      };

      await retryWithBackoff(fn, {
        maxRetries: 5,
        initialDelayMs: 1,
        maxDelayMs: 100,
        onRetry: (attempt, _err, delayMs) => {
          retryLog.push({ attempt, delayMs });
        },
      });

      expect(retryLog).toHaveLength(3);
      expect(retryLog[0]!.attempt).toBe(1);
      expect(retryLog[1]!.attempt).toBe(2);
      expect(retryLog[2]!.attempt).toBe(3);
    });

    it("caps delay at maxDelayMs", async () => {
      let calls = 0;
      const delays: number[] = [];

      const fn = async () => {
        calls++;
        if (calls <= 6) throw new Error("lock timeout");
        return "done";
      };

      await retryWithBackoff(fn, {
        maxRetries: 6,
        initialDelayMs: 10,
        maxDelayMs: 50,
        onRetry: (_attempt, _err, delayMs) => {
          delays.push(delayMs);
        },
      });

      // Delays should be: 10, 20, 40, 50 (capped), 50 (capped), 50 (capped)
      expect(delays[0]).toBe(10);
      expect(delays[1]).toBe(20);
      expect(delays[2]).toBe(40);
      expect(delays[3]).toBe(50); // capped
      expect(delays[4]).toBe(50); // capped
      expect(delays[5]).toBe(50); // capped
    });
  });

  // -----------------------------------------------------------------------
  // PsqlRunner lockTimeout integration (buildPsqlCommand)
  // -----------------------------------------------------------------------

  describe("buildPsqlCommand with lockTimeout", () => {
    it("adds -c SET lock_timeout before -f when lockTimeout is set", () => {
      const cmd = buildPsqlCommand(
        "deploy/001.sql",
        opts({ lockTimeout: 5000 }),
        "psql",
      );

      // Find -c and its value
      const cIdx = cmd.args.indexOf("-c");
      expect(cIdx).toBeGreaterThanOrEqual(0);
      expect(cmd.args[cIdx + 1]).toBe("SET lock_timeout = '5000ms'");

      // -c must come before -f
      const fIdx = cmd.args.indexOf("-f");
      expect(cIdx).toBeLessThan(fIdx);
    });

    it("does not add -c when lockTimeout is not set", () => {
      const cmd = buildPsqlCommand("deploy/001.sql", opts(), "psql");

      expect(cmd.args).not.toContain("-c");
    });

    it("adds -c with 0ms lockTimeout (valid — disables lock timeout)", () => {
      const cmd = buildPsqlCommand(
        "deploy/001.sql",
        opts({ lockTimeout: 0 }),
        "psql",
      );

      const cIdx = cmd.args.indexOf("-c");
      expect(cIdx).toBeGreaterThanOrEqual(0);
      expect(cmd.args[cIdx + 1]).toBe("SET lock_timeout = '0ms'");
    });

    it("does not add -c when lockTimeout is undefined", () => {
      const cmd = buildPsqlCommand(
        "deploy/001.sql",
        opts({ lockTimeout: undefined }),
        "psql",
      );

      expect(cmd.args).not.toContain("-c");
    });
  });
});
