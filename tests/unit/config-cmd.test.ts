import { describe, it, expect, beforeEach, afterEach, spyOn } from "bun:test";
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { parseConfigOptions, loadProjectConf, writeProjectConf, runConfig } from "../../src/commands/config";
import { parseArgs } from "../../src/cli";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a temporary directory with an optional sqitch.conf file. */
function makeTmpDir(confContent?: string): string {
  const dir = mkdtempSync(join(tmpdir(), "sqlever-config-test-"));
  if (confContent !== undefined) {
    writeFileSync(join(dir, "sqitch.conf"), confContent, "utf-8");
  }
  return dir;
}

// ---------------------------------------------------------------------------
// Tests: parseConfigOptions
// ---------------------------------------------------------------------------

describe("parseConfigOptions", () => {
  it("returns defaults when no args are provided", () => {
    const args = parseArgs(["config"]);
    const opts = parseConfigOptions(args);
    expect(opts.key).toBeUndefined();
    expect(opts.value).toBeUndefined();
    expect(opts.unset).toBe(false);
    expect(opts.list).toBe(false);
  });

  it("parses key argument", () => {
    const args = parseArgs(["config", "core.engine"]);
    const opts = parseConfigOptions(args);
    expect(opts.key).toBe("core.engine");
    expect(opts.value).toBeUndefined();
  });

  it("parses key and value arguments", () => {
    const args = parseArgs(["config", "core.engine", "pg"]);
    const opts = parseConfigOptions(args);
    expect(opts.key).toBe("core.engine");
    expect(opts.value).toBe("pg");
  });

  it("parses --unset flag", () => {
    const args = parseArgs(["config", "--unset", "core.engine"]);
    const opts = parseConfigOptions(args);
    expect(opts.unset).toBe(true);
    expect(opts.key).toBe("core.engine");
  });

  it("parses --list flag", () => {
    const args = parseArgs(["config", "--list"]);
    const opts = parseConfigOptions(args);
    expect(opts.list).toBe(true);
  });

  it("parses -l flag", () => {
    const args = parseArgs(["config", "-l"]);
    const opts = parseConfigOptions(args);
    expect(opts.list).toBe(true);
  });

  it("uses --top-dir from global args", () => {
    const args = parseArgs(["--top-dir", "/my/project", "config", "core.engine"]);
    const opts = parseConfigOptions(args);
    expect(opts.topDir).toBe("/my/project");
  });
});

// ---------------------------------------------------------------------------
// Tests: loadProjectConf
// ---------------------------------------------------------------------------

describe("loadProjectConf", () => {
  it("returns empty conf when sqitch.conf does not exist", () => {
    const dir = makeTmpDir();
    const { conf, path } = loadProjectConf(dir);
    expect(conf.entries).toEqual([]);
    expect(path).toContain("sqitch.conf");
    rmSync(dir, { recursive: true });
  });

  it("parses existing sqitch.conf", () => {
    const dir = makeTmpDir("[core]\n\tengine = pg\n");
    const { conf } = loadProjectConf(dir);
    expect(conf.entries.length).toBe(1);
    expect(conf.entries[0]!.key).toBe("core.engine");
    expect(conf.entries[0]!.value).toBe("pg");
    rmSync(dir, { recursive: true });
  });
});

// ---------------------------------------------------------------------------
// Tests: writeProjectConf
// ---------------------------------------------------------------------------

describe("writeProjectConf", () => {
  it("writes serialized conf to disk", () => {
    const dir = makeTmpDir();
    const confPath = join(dir, "sqitch.conf");
    const conf = {
      entries: [{ key: "core.engine", value: "pg" as string | true }],
      rawLines: [],
    };
    writeProjectConf(conf, confPath);
    const content = readFileSync(confPath, "utf-8");
    expect(content).toContain("[core]");
    expect(content).toContain("engine = pg");
    rmSync(dir, { recursive: true });
  });
});

// ---------------------------------------------------------------------------
// Tests: runConfig (integration with filesystem)
// ---------------------------------------------------------------------------

describe("runConfig", () => {
  let stdoutChunks: string[];
  let stderrChunks: string[];
  let stdoutSpy: ReturnType<typeof spyOn>;
  let stderrSpy: ReturnType<typeof spyOn>;

  beforeEach(() => {
    resetConfig();
    stdoutChunks = [];
    stderrChunks = [];
    stdoutSpy = spyOn(process.stdout, "write").mockImplementation((chunk: string | Uint8Array) => {
      stdoutChunks.push(String(chunk));
      return true;
    });
    stderrSpy = spyOn(process.stderr, "write").mockImplementation((chunk: string | Uint8Array) => {
      stderrChunks.push(String(chunk));
      return true;
    });
  });

  afterEach(() => {
    stdoutSpy.mockRestore();
    stderrSpy.mockRestore();
  });

  it("gets a config value", () => {
    const dir = makeTmpDir("[core]\n\tengine = pg\n");
    const args = parseArgs(["--top-dir", dir, "config", "core.engine"]);
    const code = runConfig(args);
    expect(code).toBe(0);
    expect(stdoutChunks.join("")).toContain("pg");
    rmSync(dir, { recursive: true });
  });

  it("returns 1 for missing key", () => {
    const dir = makeTmpDir("[core]\n\tengine = pg\n");
    const args = parseArgs(["--top-dir", dir, "config", "core.nonexistent"]);
    const code = runConfig(args);
    expect(code).toBe(1);
    rmSync(dir, { recursive: true });
  });

  it("sets a config value", () => {
    const dir = makeTmpDir("[core]\n\tengine = pg\n");
    const args = parseArgs(["--top-dir", dir, "config", "core.engine", "sqlite"]);
    const code = runConfig(args);
    expect(code).toBe(0);

    // Verify value was written
    const content = readFileSync(join(dir, "sqitch.conf"), "utf-8");
    expect(content).toContain("sqlite");
    rmSync(dir, { recursive: true });
  });

  it("sets a new key in existing conf", () => {
    const dir = makeTmpDir("[core]\n\tengine = pg\n");
    const args = parseArgs(["--top-dir", dir, "config", "core.plan_file", "my.plan"]);
    const code = runConfig(args);
    expect(code).toBe(0);

    const content = readFileSync(join(dir, "sqitch.conf"), "utf-8");
    expect(content).toContain("plan_file = my.plan");
    rmSync(dir, { recursive: true });
  });

  it("unsets a config value", () => {
    const dir = makeTmpDir("[core]\n\tengine = pg\n\tplan_file = sqitch.plan\n");
    const args = parseArgs(["--top-dir", dir, "config", "--unset", "core.engine"]);
    const code = runConfig(args);
    expect(code).toBe(0);

    const content = readFileSync(join(dir, "sqitch.conf"), "utf-8");
    expect(content).not.toContain("engine");
    expect(content).toContain("plan_file");
    rmSync(dir, { recursive: true });
  });

  it("lists all config entries", () => {
    const dir = makeTmpDir("[core]\n\tengine = pg\n\tplan_file = sqitch.plan\n");
    const args = parseArgs(["--top-dir", dir, "config", "--list"]);
    const code = runConfig(args);
    expect(code).toBe(0);
    const output = stdoutChunks.join("");
    expect(output).toContain("core.engine=pg");
    expect(output).toContain("core.plan_file=sqitch.plan");
    rmSync(dir, { recursive: true });
  });

  it("lists entries from empty conf", () => {
    const dir = makeTmpDir();
    const args = parseArgs(["--top-dir", dir, "config", "--list"]);
    const code = runConfig(args);
    expect(code).toBe(0);
    expect(stdoutChunks.join("")).toBe("");
    rmSync(dir, { recursive: true });
  });

  it("returns 1 with usage when no key is given", () => {
    const dir = makeTmpDir();
    const args = parseArgs(["--top-dir", dir, "config"]);
    const code = runConfig(args);
    expect(code).toBe(1);
    expect(stderrChunks.join("")).toContain("Usage");
    rmSync(dir, { recursive: true });
  });

  it("creates sqitch.conf when setting value in new project", () => {
    const dir = makeTmpDir(); // no sqitch.conf
    const args = parseArgs(["--top-dir", dir, "config", "core.engine", "pg"]);
    const code = runConfig(args);
    expect(code).toBe(0);

    const content = readFileSync(join(dir, "sqitch.conf"), "utf-8");
    expect(content).toContain("engine = pg");
    rmSync(dir, { recursive: true });
  });

  it("handles subsection keys correctly", () => {
    const dir = makeTmpDir(
      '[core]\n\tengine = pg\n\n[engine "pg"]\n\ttarget = db:pg:mydb\n',
    );
    const args = parseArgs(["--top-dir", dir, "config", "engine.pg.target"]);
    const code = runConfig(args);
    expect(code).toBe(0);
    expect(stdoutChunks.join("")).toContain("db:pg:mydb");
    rmSync(dir, { recursive: true });
  });
});
