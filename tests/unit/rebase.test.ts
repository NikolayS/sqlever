import { describe, it, expect } from "bun:test";
import { parseRebaseOptions } from "../../src/commands/rebase";

// ---------------------------------------------------------------------------
// Tests: parseRebaseOptions (pure unit tests, no I/O)
// ---------------------------------------------------------------------------

describe("parseRebaseOptions", () => {
  it("returns defaults when no args are provided", () => {
    const opts = parseRebaseOptions([]);
    expect(opts.ontoChange).toBeUndefined();
    expect(opts.toChange).toBeUndefined();
    expect(opts.noPrompt).toBe(false);
  });

  it("parses --onto flag", () => {
    const opts = parseRebaseOptions(["--onto", "my_change"]);
    expect(opts.ontoChange).toBe("my_change");
    expect(opts.toChange).toBeUndefined();
    expect(opts.noPrompt).toBe(false);
  });

  it("parses --to flag", () => {
    const opts = parseRebaseOptions(["--to", "target_change"]);
    expect(opts.toChange).toBe("target_change");
    expect(opts.ontoChange).toBeUndefined();
  });

  it("parses --onto and --to together", () => {
    const opts = parseRebaseOptions([
      "--onto", "base_change",
      "--to", "target_change",
    ]);
    expect(opts.ontoChange).toBe("base_change");
    expect(opts.toChange).toBe("target_change");
  });

  it("parses -y flag", () => {
    const opts = parseRebaseOptions(["-y"]);
    expect(opts.noPrompt).toBe(true);
  });

  it("parses --no-prompt flag", () => {
    const opts = parseRebaseOptions(["--no-prompt"]);
    expect(opts.noPrompt).toBe(true);
  });

  it("parses combined flags", () => {
    const opts = parseRebaseOptions([
      "--onto", "base",
      "--to", "target",
      "-y",
    ]);
    expect(opts.ontoChange).toBe("base");
    expect(opts.toChange).toBe("target");
    expect(opts.noPrompt).toBe(true);
  });

  it("throws on --onto without value", () => {
    expect(() => parseRebaseOptions(["--onto"])).toThrow("Missing value for --onto");
  });

  it("throws on --onto with flag-like value", () => {
    expect(() => parseRebaseOptions(["--onto", "--to"])).toThrow(
      "Missing value for --onto",
    );
  });

  it("throws on --to without value", () => {
    expect(() => parseRebaseOptions(["--to"])).toThrow("Missing value for --to");
  });

  it("throws on --to with flag-like value", () => {
    expect(() => parseRebaseOptions(["--to", "--onto"])).toThrow(
      "Missing value for --to",
    );
  });

  it("ignores unknown flags gracefully", () => {
    const opts = parseRebaseOptions(["--unknown", "--onto", "change1"]);
    expect(opts.ontoChange).toBe("change1");
  });
});
