// src/commands/config.ts — sqlever config command
//
// Read and write sqitch.conf values.
//
// Usage:
//   sqlever config <key>           # get a value
//   sqlever config <key> <value>   # set a value
//   sqlever config --unset <key>   # remove a key
//   sqlever config --list          # list all key-value pairs
//
// This mirrors `sqitch config` / `git config` behavior.
// Reads/writes the project-level sqitch.conf file.

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { join, resolve } from "node:path";
import type { ParsedArgs } from "../cli";
import {
  parseSqitchConf,
  confGetString,
  confSet,
  confUnset,
  serializeSqitchConf,
  type SqitchConf,
} from "../config/sqitch-conf";
import { info, error as logError } from "../output";

// ---------------------------------------------------------------------------
// Config-specific argument parsing
// ---------------------------------------------------------------------------

export interface ConfigOptions {
  /** The config key to get or set. */
  key?: string;
  /** The value to set (undefined means "get"). */
  value?: string;
  /** Remove the key (--unset). */
  unset: boolean;
  /** List all config entries (--list / -l). */
  list: boolean;
  /** Project root directory. */
  topDir: string;
}

/**
 * Parse config-specific options from the CLI's rest args.
 *
 * Usage:
 *   sqlever config [--list] [--unset] [<key>] [<value>]
 */
export function parseConfigOptions(args: ParsedArgs): ConfigOptions {
  const opts: ConfigOptions = {
    unset: false,
    list: false,
    topDir: args.topDir ?? ".",
  };

  const rest = args.rest;
  const positional: string[] = [];

  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--unset") {
      opts.unset = true;
      i++;
      continue;
    }
    if (token === "--list" || token === "-l") {
      opts.list = true;
      i++;
      continue;
    }

    positional.push(token);
    i++;
  }

  if (positional.length >= 1) {
    opts.key = positional[0];
  }
  if (positional.length >= 2) {
    opts.value = positional[1];
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Core config logic
// ---------------------------------------------------------------------------

/**
 * Load the project-level sqitch.conf file.
 *
 * Returns the parsed conf and the file path.
 * If the file does not exist, returns an empty conf.
 */
export function loadProjectConf(topDir: string): { conf: SqitchConf; path: string } {
  const confPath = join(resolve(topDir), "sqitch.conf");
  if (!existsSync(confPath)) {
    return {
      conf: { entries: [], rawLines: [] },
      path: confPath,
    };
  }

  const text = readFileSync(confPath, "utf-8");
  return {
    conf: parseSqitchConf(text),
    path: confPath,
  };
}

/**
 * Write a SqitchConf back to disk.
 */
export function writeProjectConf(conf: SqitchConf, path: string): void {
  const text = serializeSqitchConf(conf);
  writeFileSync(path, text, "utf-8");
}

// ---------------------------------------------------------------------------
// Main config command
// ---------------------------------------------------------------------------

/**
 * Execute the `config` command.
 *
 * Modes:
 *   --list              List all config entries
 *   <key>               Get the value of a key
 *   <key> <value>       Set a key to a value
 *   --unset <key>       Remove a key
 */
export function runConfig(args: ParsedArgs): number {
  const opts = parseConfigOptions(args);

  // --list mode
  if (opts.list) {
    const { conf } = loadProjectConf(opts.topDir);
    for (const entry of conf.entries) {
      const val = entry.value === true ? "true" : entry.value;
      process.stdout.write(`${entry.key}=${val}\n`);
    }
    return 0;
  }

  // No key specified
  if (!opts.key) {
    logError("Usage: sqlever config [--list] [--unset] <key> [<value>]");
    return 1;
  }

  // --unset mode
  if (opts.unset) {
    const { conf, path } = loadProjectConf(opts.topDir);
    confUnset(conf, opts.key);
    writeProjectConf(conf, path);
    info(`Unset '${opts.key}'.`);
    return 0;
  }

  // Set mode: key + value
  if (opts.value !== undefined) {
    const { conf, path } = loadProjectConf(opts.topDir);
    confSet(conf, opts.key, opts.value);
    writeProjectConf(conf, path);
    return 0;
  }

  // Get mode: key only
  const { conf } = loadProjectConf(opts.topDir);
  const val = confGetString(conf, opts.key);
  if (val === undefined) {
    // Key not found -- exit 1 (matches git config behavior)
    return 1;
  }
  process.stdout.write(val + "\n");
  return 0;
}
