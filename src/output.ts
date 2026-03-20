// src/output.ts — Shared output helpers for sqlever CLI
//
// All CLI output flows through this module so that --quiet, --verbose,
// and --format flags are respected consistently.

export type OutputFormat = "text" | "json";

export interface OutputConfig {
  format: OutputFormat;
  quiet: boolean;
  verbose: boolean;
}

let config: OutputConfig = {
  format: "text",
  quiet: false,
  verbose: false,
};

/** Replace the global output config. */
export function setConfig(next: Partial<OutputConfig>): void {
  config = { ...config, ...next };
}

/** Return a copy of the current config (useful for tests). */
export function getConfig(): OutputConfig {
  return { ...config };
}

/** Reset config to defaults (useful for tests). */
export function resetConfig(): void {
  config = { format: "text", quiet: false, verbose: false };
}

// ---------------------------------------------------------------------------
// Core output functions
// ---------------------------------------------------------------------------

/** Print an informational message to stdout. Suppressed by --quiet. */
export function info(msg: string): void {
  if (config.quiet) return;
  process.stdout.write(msg + "\n");
}

/** Print an error message to stderr. Always shown. */
export function error(msg: string): void {
  process.stderr.write(msg + "\n");
}

/** Print a verbose/debug message to stderr. Only shown with --verbose. */
export function verbose(msg: string): void {
  if (!config.verbose) return;
  process.stderr.write(msg + "\n");
}

/** Print structured data as JSON to stdout. Intended for --format json. */
export function json(data: unknown): void {
  process.stdout.write(JSON.stringify(data, null, 2) + "\n");
}

// ---------------------------------------------------------------------------
// Table output
// ---------------------------------------------------------------------------

/**
 * Print an aligned text table to stdout.
 *
 * @param rows  - Array of objects (each row) or array of arrays.
 * @param headers - Column headers. When rows are objects, headers also
 *                  serve as the keys to extract from each row.
 *
 * Respects --quiet (suppressed) and --format json (emits JSON instead).
 */
export function table(
  rows: Record<string, unknown>[] | string[][],
  headers: string[],
): void {
  // In JSON mode, emit structured data instead of a text table.
  if (config.format === "json") {
    const structured = rows.map((row) => {
      if (Array.isArray(row)) {
        const obj: Record<string, unknown> = {};
        headers.forEach((h, i) => {
          obj[h] = row[i] ?? null;
        });
        return obj;
      }
      return row;
    });
    json(structured);
    return;
  }

  if (config.quiet) return;

  // Normalize rows to string arrays.
  const stringRows: string[][] = rows.map((row) => {
    if (Array.isArray(row)) return row.map(String);
    return headers.map((h) => String(row[h] ?? ""));
  });

  // Compute column widths.
  const widths = headers.map((h) => h.length);
  for (const row of stringRows) {
    for (let i = 0; i < headers.length; i++) {
      const cell = row[i] ?? "";
      if (cell.length > widths[i]) {
        widths[i] = cell.length;
      }
    }
  }

  const pad = (s: string, w: number) => s + " ".repeat(Math.max(0, w - s.length));

  // Header line.
  const headerLine = headers.map((h, i) => pad(h, widths[i])).join("  ");
  process.stdout.write(headerLine + "\n");

  // Separator line.
  const separator = widths.map((w) => "-".repeat(w)).join("  ");
  process.stdout.write(separator + "\n");

  // Data lines.
  for (const row of stringRows) {
    const line = headers.map((_, i) => pad(row[i] ?? "", widths[i])).join("  ");
    process.stdout.write(line + "\n");
  }
}

// ---------------------------------------------------------------------------
// URI masking
// ---------------------------------------------------------------------------

/**
 * Mask passwords in database connection URIs.
 *
 * Handles schemes like:
 *   postgresql://user:secret@host/db  ->  postgresql://user:***@host/db
 *   db:pg://user:secret@host/db       ->  db:pg://user:***@host/db
 *   postgres://host/db                ->  postgres://host/db  (no password)
 *
 * The regex matches:
 *   (scheme://)(user):(password)(@host...)
 * and replaces the password portion with ***.
 */
export function maskUri(uri: string): string {
  // Match scheme (including compound schemes like "db:pg"), then "://",
  // then "user:password@" where password is everything between the first
  // colon after :// and the LAST @ sign before the host (to handle
  // passwords containing @).
  return uri.replace(
    /^([a-zA-Z][a-zA-Z0-9+.:~-]*:\/\/)([^:@/]+):(.+)@(?=[^@]*$)/,
    "$1$2:***@",
  );
}
