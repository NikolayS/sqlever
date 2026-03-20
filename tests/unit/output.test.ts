import { describe, it, expect, beforeEach, afterEach, spyOn } from "bun:test";
import {
  setConfig,
  getConfig,
  resetConfig,
  info,
  error,
  verbose,
  json,
  table,
  maskUri,
} from "../../src/output";

// ---------------------------------------------------------------------------
// Helpers — capture stdout / stderr writes
// ---------------------------------------------------------------------------

function captureWrites() {
  let stdout = "";
  let stderr = "";

  const stdoutSpy = spyOn(process.stdout, "write").mockImplementation(
    (chunk: string | Uint8Array) => {
      stdout += String(chunk);
      return true;
    },
  );

  const stderrSpy = spyOn(process.stderr, "write").mockImplementation(
    (chunk: string | Uint8Array) => {
      stderr += String(chunk);
      return true;
    },
  );

  return {
    get stdout() {
      return stdout;
    },
    get stderr() {
      return stderr;
    },
    restore() {
      stdoutSpy.mockRestore();
      stderrSpy.mockRestore();
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("output module", () => {
  beforeEach(() => resetConfig());

  describe("setConfig / getConfig / resetConfig", () => {
    it("returns default config after reset", () => {
      expect(getConfig()).toEqual({
        format: "text",
        quiet: false,
        verbose: false,
      });
    });

    it("merges partial config", () => {
      setConfig({ quiet: true });
      expect(getConfig()).toEqual({
        format: "text",
        quiet: true,
        verbose: false,
      });
    });

    it("overrides full config", () => {
      setConfig({ format: "json", quiet: true, verbose: true });
      expect(getConfig()).toEqual({
        format: "json",
        quiet: true,
        verbose: true,
      });
    });
  });

  // -----------------------------------------------------------------------
  // info()
  // -----------------------------------------------------------------------

  describe("info()", () => {
    it("writes to stdout", () => {
      const cap = captureWrites();
      try {
        info("hello");
        expect(cap.stdout).toBe("hello\n");
        expect(cap.stderr).toBe("");
      } finally {
        cap.restore();
      }
    });

    it("is suppressed in quiet mode", () => {
      setConfig({ quiet: true });
      const cap = captureWrites();
      try {
        info("should not appear");
        expect(cap.stdout).toBe("");
      } finally {
        cap.restore();
      }
    });
  });

  // -----------------------------------------------------------------------
  // error()
  // -----------------------------------------------------------------------

  describe("error()", () => {
    it("writes to stderr", () => {
      const cap = captureWrites();
      try {
        error("oops");
        expect(cap.stderr).toBe("oops\n");
        expect(cap.stdout).toBe("");
      } finally {
        cap.restore();
      }
    });

    it("is shown even in quiet mode", () => {
      setConfig({ quiet: true });
      const cap = captureWrites();
      try {
        error("still visible");
        expect(cap.stderr).toBe("still visible\n");
      } finally {
        cap.restore();
      }
    });
  });

  // -----------------------------------------------------------------------
  // verbose()
  // -----------------------------------------------------------------------

  describe("verbose()", () => {
    it("is suppressed by default", () => {
      const cap = captureWrites();
      try {
        verbose("debug info");
        expect(cap.stderr).toBe("");
      } finally {
        cap.restore();
      }
    });

    it("writes to stderr when verbose is enabled", () => {
      setConfig({ verbose: true });
      const cap = captureWrites();
      try {
        verbose("debug info");
        expect(cap.stderr).toBe("debug info\n");
      } finally {
        cap.restore();
      }
    });
  });

  // -----------------------------------------------------------------------
  // json()
  // -----------------------------------------------------------------------

  describe("json()", () => {
    it("outputs pretty-printed JSON to stdout", () => {
      const cap = captureWrites();
      try {
        json({ key: "value", n: 42 });
        const parsed = JSON.parse(cap.stdout);
        expect(parsed).toEqual({ key: "value", n: 42 });
      } finally {
        cap.restore();
      }
    });

    it("handles arrays", () => {
      const cap = captureWrites();
      try {
        json([1, 2, 3]);
        expect(JSON.parse(cap.stdout)).toEqual([1, 2, 3]);
      } finally {
        cap.restore();
      }
    });

    it("handles null", () => {
      const cap = captureWrites();
      try {
        json(null);
        expect(cap.stdout).toBe("null\n");
      } finally {
        cap.restore();
      }
    });
  });

  // -----------------------------------------------------------------------
  // table()
  // -----------------------------------------------------------------------

  describe("table()", () => {
    it("prints aligned columns with object rows", () => {
      const cap = captureWrites();
      try {
        table(
          [
            { name: "Alice", age: 30 },
            { name: "Bob", age: 7 },
          ],
          ["name", "age"],
        );
        const lines = cap.stdout.split("\n");
        expect(lines[0]).toBe("name   age");
        expect(lines[1]).toBe("-----  ---");
        expect(lines[2]).toBe("Alice  30 ");
        expect(lines[3]).toBe("Bob    7  ");
      } finally {
        cap.restore();
      }
    });

    it("prints aligned columns with array rows", () => {
      const cap = captureWrites();
      try {
        table(
          [
            ["Alice", "30"],
            ["Bob", "7"],
          ],
          ["name", "age"],
        );
        const lines = cap.stdout.split("\n");
        expect(lines[0]).toBe("name   age");
        expect(lines[1]).toBe("-----  ---");
        expect(lines[2]).toBe("Alice  30 ");
        expect(lines[3]).toBe("Bob    7  ");
      } finally {
        cap.restore();
      }
    });

    it("handles wide data that exceeds header width", () => {
      const cap = captureWrites();
      try {
        table(
          [{ id: "1", description: "A very long description here" }],
          ["id", "description"],
        );
        const lines = cap.stdout.split("\n");
        // "description" is 11 chars, data is 28 chars — data wins.
        expect(lines[0]).toBe("id  description                 ");
        expect(lines[2]).toBe("1   A very long description here");
      } finally {
        cap.restore();
      }
    });

    it("is suppressed in quiet mode", () => {
      setConfig({ quiet: true });
      const cap = captureWrites();
      try {
        table([{ a: 1 }], ["a"]);
        expect(cap.stdout).toBe("");
      } finally {
        cap.restore();
      }
    });

    it("outputs JSON in json format mode", () => {
      setConfig({ format: "json" });
      const cap = captureWrites();
      try {
        table(
          [
            { name: "Alice", age: 30 },
            { name: "Bob", age: 7 },
          ],
          ["name", "age"],
        );
        const parsed = JSON.parse(cap.stdout);
        expect(parsed).toEqual([
          { name: "Alice", age: 30 },
          { name: "Bob", age: 7 },
        ]);
      } finally {
        cap.restore();
      }
    });

    it("converts array rows to objects in json format mode", () => {
      setConfig({ format: "json" });
      const cap = captureWrites();
      try {
        table(
          [
            ["Alice", "30"],
            ["Bob", "7"],
          ],
          ["name", "age"],
        );
        const parsed = JSON.parse(cap.stdout);
        expect(parsed).toEqual([
          { name: "Alice", age: "30" },
          { name: "Bob", age: "7" },
        ]);
      } finally {
        cap.restore();
      }
    });

    it("handles empty rows", () => {
      const cap = captureWrites();
      try {
        table([], ["name", "age"]);
        const lines = cap.stdout.split("\n");
        expect(lines[0]).toBe("name  age");
        expect(lines[1]).toBe("----  ---");
        expect(lines.length).toBe(3); // header + separator + trailing newline
      } finally {
        cap.restore();
      }
    });
  });

  // -----------------------------------------------------------------------
  // maskUri()
  // -----------------------------------------------------------------------

  describe("maskUri()", () => {
    it("masks password in postgresql:// URI", () => {
      expect(maskUri("postgresql://user:secret@host/db")).toBe(
        "postgresql://user:***@host/db",
      );
    });

    it("masks password in postgres:// URI", () => {
      expect(maskUri("postgres://admin:p4$$w0rd@db.example.com:5432/mydb")).toBe(
        "postgres://admin:***@db.example.com:5432/mydb",
      );
    });

    it("masks password in db:pg:// URI", () => {
      expect(maskUri("db:pg://user:secret@host/db")).toBe(
        "db:pg://user:***@host/db",
      );
    });

    it("preserves URI without password", () => {
      expect(maskUri("postgresql://host/db")).toBe("postgresql://host/db");
    });

    it("preserves URI with user but no password", () => {
      expect(maskUri("postgresql://user@host/db")).toBe(
        "postgresql://user@host/db",
      );
    });

    it("handles password with special characters", () => {
      expect(maskUri("postgresql://user:p@ss:word!@host/db")).toBe(
        "postgresql://user:***@host/db",
      );
    });

    it("handles non-URI strings (passthrough)", () => {
      expect(maskUri("/var/run/postgres")).toBe("/var/run/postgres");
    });

    it("handles empty string", () => {
      expect(maskUri("")).toBe("");
    });

    it("masks password in URI with port", () => {
      expect(maskUri("postgresql://user:secret@host:5432/db")).toBe(
        "postgresql://user:***@host:5432/db",
      );
    });

    it("masks password in URI with query params", () => {
      expect(
        maskUri("postgresql://user:secret@host/db?sslmode=require"),
      ).toBe("postgresql://user:***@host/db?sslmode=require");
    });
  });
});
