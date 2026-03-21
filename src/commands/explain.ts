// src/commands/explain.ts — sqlever explain command
//
// Usage:
//   sqlever explain <file.sql>          — explain a migration file
//   sqlever explain --provider openai   — use OpenAI (default)
//   sqlever explain --provider anthropic — use Anthropic
//   sqlever explain --provider ollama   — use local Ollama
//   sqlever explain --model gpt-4o     — specify model
//   sqlever explain --api-key sk-...   — API key (or env var)
//
// Implements SPEC Section 5.7 and GitHub issue #106.

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  ensureWasm,
  explain,
  formatExplainOutput,
  DEFAULT_MODELS,
  type LLMProvider,
  type ExplainConfig,
} from "../ai/explain";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ExplainOptions {
  /** Path to the migration SQL file. */
  target?: string;
  /** LLM provider. */
  provider: LLMProvider;
  /** Model name. */
  model?: string;
  /** API key. */
  apiKey?: string;
  /** Ollama base URL. */
  ollamaBaseUrl?: string;
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

/**
 * Parse explain-specific arguments from the rest array.
 */
export function parseExplainArgs(rest: string[]): ExplainOptions {
  const opts: ExplainOptions = {
    provider: "openai",
  };

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "--provider") {
      const val = rest[i + 1];
      if (val === "openai" || val === "anthropic" || val === "ollama") {
        opts.provider = val;
      } else {
        throw new Error(
          `Invalid --provider value '${val ?? ""}'. Expected openai, anthropic, or ollama.`,
        );
      }
      i += 2;
      continue;
    }

    if (arg === "--model") {
      const val = rest[i + 1];
      if (!val) {
        throw new Error("--model requires a value");
      }
      opts.model = val;
      i += 2;
      continue;
    }

    if (arg === "--api-key") {
      const val = rest[i + 1];
      if (!val) {
        throw new Error("--api-key requires a value");
      }
      opts.apiKey = val;
      i += 2;
      continue;
    }

    if (arg === "--ollama-url") {
      const val = rest[i + 1];
      if (!val) {
        throw new Error("--ollama-url requires a value");
      }
      opts.ollamaBaseUrl = val;
      i += 2;
      continue;
    }

    // Positional argument — file target
    if (!opts.target) {
      opts.target = arg;
    }
    i++;
  }

  return opts;
}

/**
 * Resolve the API key from args, environment variables, or error.
 */
function resolveApiKey(opts: ExplainOptions): string | undefined {
  if (opts.apiKey) return opts.apiKey;

  switch (opts.provider) {
    case "openai":
      return process.env.OPENAI_API_KEY;
    case "anthropic":
      return process.env.ANTHROPIC_API_KEY;
    case "ollama":
      return undefined; // No key needed
  }
}

// ---------------------------------------------------------------------------
// Main command
// ---------------------------------------------------------------------------

/**
 * Run the explain command.
 */
export async function runExplain(opts: ExplainOptions): Promise<number> {
  // Validate target
  if (!opts.target) {
    throw new Error(
      "Usage: sqlever explain <file.sql> [--provider openai|anthropic|ollama] [--model <name>] [--api-key <key>]",
    );
  }

  const filePath = resolve(opts.target);
  if (!existsSync(filePath)) {
    throw new Error(`File not found: ${opts.target}`);
  }

  // Read SQL
  const sql = readFileSync(filePath, "utf-8");
  if (sql.trim().length === 0) {
    throw new Error(`File is empty: ${opts.target}`);
  }

  // Resolve API key
  const apiKey = resolveApiKey(opts);

  // Build config
  const config: ExplainConfig = {
    provider: opts.provider,
    model: opts.model ?? DEFAULT_MODELS[opts.provider],
    apiKey,
    ollamaBaseUrl: opts.ollamaBaseUrl,
  };

  // Ensure WASM loaded for SQL parsing
  await ensureWasm();

  // Run explain
  const result = await explain(sql, config);

  // Output
  const output = formatExplainOutput(result);
  process.stdout.write(output);

  return 0;
}
