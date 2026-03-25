// src/commands/deploy.ts — sqlever deploy command
//
// The core command: executes pending migration scripts against a PostgreSQL
// database, tracking state in sqitch.* registry tables.
//
// See SPEC.md Section 7 (Data flow — deploy), DD12, DD13, DD14.

import { readFileSync, existsSync } from "node:fs";
import { join, resolve } from "node:path";
import type { ParsedArgs } from "../cli";
import { loadConfig, type MergedConfig } from "../config/index";
import { DatabaseClient } from "../db/client";
import { Registry, type RecordDeployInput } from "../db/registry";
import { topologicalSort, filterPending, filterToTarget, validateDependencies } from "../plan/sort";
import { computeScriptHashFromBytes } from "../plan/types";
import type { Change, Plan, Tag } from "../plan/types";
import { PsqlRunner, type PsqlRunResult } from "../psql";
import { shouldSetLockTimeout } from "../lock-guard";
import { resolveDeployIncludes } from "../includes/snapshot";
import { shutdownManager } from "../signals";
import { info, error as logError, verbose, getConfig } from "../output";
import { sqitchToStandard } from "../db/uri";
import { DeployProgress, shouldUseTUI } from "../tui/deploy";
import { parseDblabOptions, runDblabDeploy } from "./deploy-dblab";
import { isExpandChange, isContractChange } from "../expand-contract/phase-filter";
import { ExpandContractTracker } from "../expand-contract/tracker";
import { loadPlan } from "./shared";
import { runPreDeployAnalysis, type PreDeployAnalysisResult } from "./deploy-analyze";

// ---------------------------------------------------------------------------
// Exit codes (SPEC R6)
// ---------------------------------------------------------------------------

export const EXIT_DEPLOY_FAILED = 1;
export const EXIT_ANALYSIS_BLOCKED = 2;
export const EXIT_CONCURRENT_DEPLOY = 4;
export const EXIT_LOCK_TIMEOUT = 5;
export const EXIT_DB_UNREACHABLE = 10;

// ---------------------------------------------------------------------------
// Advisory lock
// ---------------------------------------------------------------------------

/**
 * Namespace constant for the two-argument advisory lock form.
 * Stable across PG versions (application-defined, not hashtext).
 * ASCII bytes of "sqlv" = 0x73716C76.
 */
export const ADVISORY_LOCK_NAMESPACE = 0x7371_6C76;

/**
 * Compute a stable 32-bit integer hash of a project name for use as
 * the second argument to pg_advisory_lock(namespace, key).
 *
 * Uses a simple DJB2-style hash. The result is always positive 32-bit
 * so it fits in PostgreSQL's int4 argument.
 */
export function projectLockKey(projectName: string): number {
  let hash = 5381;
  for (let i = 0; i < projectName.length; i++) {
    // hash * 33 + charCode, keep within 32-bit signed range
    hash = ((hash << 5) + hash + projectName.charCodeAt(i)) | 0;
  }
  // Ensure positive value for pg_advisory_lock int4 argument
  return hash & 0x7fff_ffff;
}

// ---------------------------------------------------------------------------
// Deploy options
// ---------------------------------------------------------------------------

/** Expand/contract deploy phase. */
export type DeployPhase = "expand" | "contract";

export interface DeployOptions {
  /** Deploy up to and including this change name. */
  to?: string;
  /** Transaction scope: change (default), all, or tag. */
  mode: "change" | "all" | "tag";
  /** Print what would be deployed, make no changes. */
  dryRun: boolean;
  /** Run verify scripts after each change. */
  verify: boolean;
  /** Key-value pairs passed as psql -v variables. */
  variables: Record<string, string>;
  /** Database connection URI. */
  dbUri?: string;
  /** Named target from config. */
  target?: string;
  /** Path to the psql binary. */
  dbClient?: string;
  /** Lock timeout in milliseconds for deploy scripts. */
  lockTimeout?: number;
  /** Project directory. */
  projectDir: string;
  /** Committer name (from git config or env). */
  committerName: string;
  /** Committer email (from git config or env). */
  committerEmail: string;
  /** Disable TUI dashboard even when stdout is a TTY. */
  noTui: boolean;
  /** Skip snapshot include resolution; use HEAD/current files (Sqitch-compatible). */
  noSnapshot: boolean;
  /** Expand/contract phase filter: deploy only expand or contract migrations. */
  phase?: DeployPhase;
  /** Skip pre-deploy static analysis (--no-analyze). */
  noAnalyze: boolean;
  /** Bypass all analysis errors (--force). */
  force: boolean;
  /** Bypass specific analysis rules (--force-rule SA003). */
  forceRules: string[];
}

/**
 * Parse deploy-specific options from CLI args.
 */
export function parseDeployOptions(args: ParsedArgs): DeployOptions {
  let to: string | undefined;
  let mode: "change" | "all" | "tag" = "change";
  let dryRun = false;
  let verify: boolean | undefined;
  let dbClient: string | undefined;
  let lockTimeout: number | undefined;
  let noTui = false;
  let noSnapshot = false;
  let noAnalyze = false;
  let force = false;
  const forceRules: string[] = [];
  let phase: DeployPhase | undefined;
  const variables: Record<string, string> = {};

  const rest = args.rest;
  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--phase") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--phase requires a value (expand or contract)");
      }
      if (val !== "expand" && val !== "contract") {
        throw new Error(`Unknown phase: ${val}. Must be one of: expand, contract`);
      }
      phase = val;
      i++;
      continue;
    }
    if (token === "--to") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--to requires a change name");
      }
      to = val;
      i++;
      continue;
    }
    if (token === "--mode") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--mode requires a value (change, all, or tag)");
      }
      if (val !== "change" && val !== "all" && val !== "tag") {
        throw new Error(`Unknown mode: ${val}. Must be one of: change, all, tag`);
      }
      if (val !== "change") {
        throw new Error(`--mode ${val} is not yet implemented. Only --mode change is supported.`);
      }
      mode = val;
      i++;
      continue;
    }
    if (token === "--dry-run") {
      dryRun = true;
      i++;
      continue;
    }
    if (token === "--verify") {
      verify = true;
      i++;
      continue;
    }
    if (token === "--no-verify") {
      verify = false;
      i++;
      continue;
    }
    if (token === "--set") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--set requires a key=value argument");
      }
      const eqIdx = val.indexOf("=");
      if (eqIdx !== -1) {
        variables[val.slice(0, eqIdx)] = val.slice(eqIdx + 1);
      }
      i++;
      continue;
    }
    if (token === "--db-client" || token === "--client") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--db-client requires a path to the psql binary");
      }
      dbClient = val;
      i++;
      continue;
    }
    if (token === "--lock-timeout") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--lock-timeout requires a value in milliseconds");
      }
      lockTimeout = parseInt(val, 10);
      i++;
      continue;
    }
    if (token === "--no-tui") {
      noTui = true;
      i++;
      continue;
    }
    if (token === "--no-snapshot") {
      noSnapshot = true;
      i++;
      continue;
    }
    if (token === "--no-analyze") {
      noAnalyze = true;
      i++;
      continue;
    }
    if (token === "--force") {
      force = true;
      i++;
      continue;
    }
    if (token === "--force-rule") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--force-rule requires a rule ID argument");
      }
      forceRules.push(val);
      i++;
      continue;
    }
    // Positional: treat as target name
    if (!args.target && !args.dbUri) {
      // Could be a target name or URI
      args.target = token;
    }
    i++;
  }

  // Load merged config to get defaults
  const projectDir = args.topDir ?? ".";
  const config = loadConfig(projectDir);

  // Resolve verify: CLI flag > config
  if (verify === undefined) {
    verify = config.deploy.verify;
  }

  // Resolve mode: CLI flag already set above, but check config if still default
  // (mode was already set from CLI --mode, config was handled during loadConfig)

  // Resolve DB URI from: --db-uri flag > --target flag lookup > config engine target
  let dbUri = args.dbUri;
  if (!dbUri) {
    const targetName = args.target;
    if (targetName) {
      // Look up target in config
      const targetConfig = config.targets[targetName];
      if (targetConfig?.uri) {
        dbUri = targetConfig.uri;
      } else {
        // Maybe it's a URI directly
        if (targetName.includes("://")) {
          dbUri = targetName;
        }
      }
    }
    if (!dbUri) {
      // Fall back to engine target
      const engineName = config.core.engine ?? "pg";
      const engineConfig = config.engines[engineName];
      if (engineConfig?.target) {
        const targetRef = engineConfig.target;
        // Could be a target name or a URI
        if (targetRef.includes("://")) {
          dbUri = targetRef;
        } else {
          const t = config.targets[targetRef];
          if (t?.uri) dbUri = t.uri;
        }
      }
    }
  }

  // Resolve psql client
  if (!dbClient) {
    const engineName = config.core.engine ?? "pg";
    const engineConfig = config.engines[engineName];
    dbClient = engineConfig?.client;
  }

  // Committer info: env > git config fallback
  const committerName = process.env.SQITCH_FULLNAME
    ?? process.env.USER
    ?? "sqlever";
  const committerEmail = process.env.SQITCH_EMAIL
    ?? process.env.EMAIL
    ?? "sqlever@localhost";

  return {
    to,
    mode,
    dryRun,
    verify,
    variables,
    dbUri,
    target: args.target,
    dbClient,
    lockTimeout,
    projectDir,
    committerName,
    committerEmail,
    noTui,
    noSnapshot,
    noAnalyze,
    force,
    forceRules,
    phase,
  };
}

// ---------------------------------------------------------------------------
// Script helpers
// ---------------------------------------------------------------------------

/**
 * Check if a deploy script is marked as auto-commit (each statement
 * commits via its own implicit transaction, vs --single-transaction
 * which wraps everything in an explicit BEGIN/COMMIT).
 *
 * Looks for `-- sqlever:auto-commit` (preferred) or the legacy
 * `-- sqlever:no-transaction` directive on the first line.
 * Both are accepted for backward compatibility.
 */
export function isAutoCommit(scriptContent: string): boolean {
  const firstLine = scriptContent.split("\n")[0] ?? "";
  return /--\s*sqlever:(auto-commit|no-transaction)/i.test(firstLine);
}

/**
 * Resolve the path to a deploy/verify script.
 */
function scriptPath(
  topDir: string,
  dir: string,
  changeName: string,
): string {
  return join(resolve(topDir), dir, `${changeName}.sql`);
}

// ---------------------------------------------------------------------------
// Deploy result
// ---------------------------------------------------------------------------

export interface DeployResult {
  /** Total changes deployed. */
  deployed: number;
  /** Total changes skipped (already deployed). */
  skipped: number;
  /** The change that failed, if any. */
  failedChange?: string;
  /** Error message if deploy failed. */
  error?: string;
  /** Whether this was a dry-run. */
  dryRun: boolean;
  /** Analysis findings from pre-deploy check (included for JSON output). */
  analysis?: PreDeployAnalysisResult;
}

// ---------------------------------------------------------------------------
// Core deploy logic (testable, receives dependencies)
// ---------------------------------------------------------------------------

export interface DeployDeps {
  db: DatabaseClient;
  registry: Registry;
  psqlRunner: PsqlRunner;
  config: MergedConfig;
  shutdownMgr: typeof shutdownManager;
}

/**
 * Execute the deploy workflow.
 *
 * This is the pure logic, separated from I/O setup so it can be unit-tested
 * with mocked dependencies.
 */
export async function executeDeploy(
  options: DeployOptions,
  deps: DeployDeps,
  /** Pre-parsed plan — avoids re-reading and re-parsing the plan file when the caller already has it. */
  preloadedPlan?: Plan,
): Promise<DeployResult> {
  const { db, registry, psqlRunner, config, shutdownMgr } = deps;
  const topDir = resolve(options.projectDir);
  const deployDir = config.core.deploy_dir;
  const verifyDir = config.core.verify_dir;
  // 1. Parse plan file (use preloaded plan when available)
  let plan: Plan;
  if (preloadedPlan) {
    plan = preloadedPlan;
  } else {
    try {
      plan = loadPlan(topDir, config);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { deployed: 0, skipped: 0, dryRun: options.dryRun, error: msg };
    }
  }
  const projectName = plan.project.name;

  // 2. Resolve DB URI
  const dbUri = options.dbUri;
  if (!dbUri) {
    return { deployed: 0, skipped: 0, dryRun: options.dryRun, error: "No database URI specified. Use --db-uri or configure a target." };
  }

  // Convert to standard URI for psql
  const standardUri = sqitchToStandard(dbUri);

  // 2a. Compute plan-level pending changes for dry-run (no DB needed)
  let allChanges = plan.changes;
  if (options.to) {
    allChanges = filterToTarget(allChanges, options.to);
  }

  // Build script name lookup: change_id -> script filename (handles reworks)
  const dryRunScriptNameMap = buildScriptNameMap(plan);

  // 2b. Pre-deploy static analysis (R4)
  let analysisResult: PreDeployAnalysisResult | undefined;
  if (!options.noAnalyze) {
    // Collect deploy script paths for pending changes
    const scriptPaths: string[] = [];
    for (const change of allChanges) {
      const sName = dryRunScriptNameMap.get(change.change_id) ?? change.name;
      const deployPath = scriptPath(topDir, deployDir, sName);
      if (existsSync(deployPath)) {
        scriptPaths.push(deployPath);
      }
    }

    if (scriptPaths.length > 0) {
      analysisResult = await runPreDeployAnalysis(scriptPaths, {
        forceRules: options.forceRules,
        force: options.force,
      });

      // Display findings (text mode)
      const outputCfg = getConfig();
      if (outputCfg.format !== "json" && analysisResult.output) {
        process.stdout.write(analysisResult.output);
      }

      // Block deploy on error-severity findings (unless --force)
      if (analysisResult.blocked) {
        logError("Deploy blocked by static analysis errors. Use --force to override or --no-analyze to skip.");
        return {
          deployed: 0,
          skipped: 0,
          dryRun: options.dryRun,
          error: "Static analysis errors found",
          analysis: analysisResult,
        };
      }
    }
  }

  // Dry-run: show what would be deployed without touching the database.
  // Per spec, --dry-run makes zero DB changes.
  if (options.dryRun) {
    const sortedChanges = topologicalSort(allChanges);
    info(`Dry-run: ${sortedChanges.length} change(s) would be deployed:`);
    for (const change of sortedChanges) {
      const sName = dryRunScriptNameMap.get(change.change_id) ?? change.name;
      const deployPath = scriptPath(topDir, deployDir, sName);
      const autoCommit = existsSync(deployPath) && isAutoCommit(readFileSync(deployPath, "utf-8"));
      const marker = autoCommit ? " [auto-commit]" : "";
      info(`  + ${change.name}${marker}`);
    }
    return { deployed: 0, skipped: 0, dryRun: true, analysis: analysisResult };
  }

  // 3. Connect to database
  await db.connect();

  // 4. Acquire advisory lock
  const lockKey = projectLockKey(projectName);
  let lockAcquired = false;

  try {
    const lockResult = await db.query<{ pg_try_advisory_lock: boolean }>(
      "SELECT pg_try_advisory_lock($1, $2)",
      [ADVISORY_LOCK_NAMESPACE, lockKey],
    );
    lockAcquired = lockResult.rows[0]?.pg_try_advisory_lock === true;

    if (!lockAcquired) {
      logError("Another deploy is already running for this project (advisory lock held).");
      return {
        deployed: 0,
        skipped: 0,
        dryRun: options.dryRun,
        error: "Concurrent deploy detected",
      };
    }

    verbose(`Advisory lock acquired: namespace=${ADVISORY_LOCK_NAMESPACE}, key=${lockKey}`);

    // Register cleanup for signal handling
    shutdownMgr.onShutdown(async () => {
      try {
        await db.query("SELECT pg_advisory_unlock($1, $2)", [ADVISORY_LOCK_NAMESPACE, lockKey]);
        verbose("Advisory lock released (shutdown)");
      } catch {
        // Best effort — PG releases on disconnect anyway
      }
      try {
        await db.disconnect();
      } catch {
        // Best effort
      }
    });

    // 5. Create registry schema if needed
    await registry.createRegistry();

    // 6. Register project
    await registry.getProject({
      project: projectName,
      uri: plan.project.uri ?? null,
      creator_name: options.committerName,
      creator_email: options.committerEmail,
    });

    // 7. Read deployed changes
    const deployedChanges = await registry.getDeployedChanges(projectName);
    const deployedIds = new Set(deployedChanges.map((c) => c.change_id));
    const deployedNames = deployedChanges.map((c) => c.change);

    // 8. Compute pending changes (re-filter with DB state)
    let pendingChanges = filterPending(allChanges, Array.from(deployedIds));

    // 8a. Phase filtering: --phase expand deploys only expand migrations,
    //     --phase contract deploys only contract migrations (after backfill check)
    if (options.phase) {
      const tracker = new ExpandContractTracker(db);

      if (options.phase === "expand") {
        // Filter to only expand changes (naming convention: *_expand)
        pendingChanges = pendingChanges.filter((c) => isExpandChange(c.name));
        if (pendingChanges.length === 0) {
          info("No pending expand migrations to deploy.");
          return { deployed: 0, skipped: 0, dryRun: options.dryRun };
        }
      } else if (options.phase === "contract") {
        // Contract phase: filter to only contract changes
        const contractPending = pendingChanges.filter((c) => isContractChange(c.name));
        if (contractPending.length === 0) {
          info("No pending contract migrations to deploy.");
          return { deployed: 0, skipped: 0, dryRun: options.dryRun };
        }

        // Verify that the expand phase has been deployed for each contract change.
        // For a contract change named "foo_contract", the expand change is "foo_expand".
        for (const cc of contractPending) {
          const baseName = cc.name.replace(/_contract$/, "");
          const expandName = `${baseName}_expand`;

          // Check if the expand change is deployed
          if (!deployedNames.includes(expandName)) {
            return {
              deployed: 0,
              skipped: 0,
              dryRun: options.dryRun,
              failedChange: cc.name,
              error: `Cannot deploy contract change "${cc.name}": expand change "${expandName}" has not been deployed yet. Run 'sqlever deploy --phase expand' first.`,
            };
          }

          // Verify backfill completion via the tracker.
          // Look up the operation state; if it exists and is in "expanded" phase,
          // the backfill has been verified. If in "expanding", the expand deploy
          // succeeded but the tracker needs to be transitioned.
          await tracker.ensureSchema();
          const operation = await tracker.getOperationByName(projectName, baseName);
          if (operation) {
            if (operation.phase === "expanding") {
              // Auto-transition from expanding -> expanded (expand deploy is done)
              await tracker.transitionPhase(operation.id, "expanded");
              verbose(`Auto-transitioned "${baseName}" from expanding to expanded.`);
            }
            if (operation.phase === "expanded") {
              // Transition to contracting (which verifies backfill)
              try {
                await tracker.transitionPhase(operation.id, "contracting", {
                  table_schema: operation.table_schema,
                  table_name: operation.table_name,
                  new_column: baseName.replace(/^.*_/, ""),
                });
              } catch (err) {
                const msg = err instanceof Error ? err.message : String(err);
                if (msg.includes("backfill")) {
                  return {
                    deployed: 0,
                    skipped: 0,
                    dryRun: options.dryRun,
                    failedChange: cc.name,
                    error: `Cannot deploy contract change "${cc.name}": ${msg}`,
                  };
                }
                // If backfill check fails for other reasons (e.g., table not tracked),
                // log a warning but allow the contract migration's own SQL to handle verification
                verbose(`Tracker backfill check skipped for "${baseName}": ${msg}`);
              }
            }
            // If already "contracting" or "completed", proceed
          }
          // If no tracker record exists, the contract migration's own SQL
          // (which includes a backfill verification check) will handle it
        }

        pendingChanges = contractPending;
      }
    }

    if (pendingChanges.length === 0) {
      info("Nothing to deploy. Database is up to date.");
      return { deployed: 0, skipped: allChanges.length, dryRun: options.dryRun };
    }

    // Validate dependencies (pending changes may depend on already-deployed)
    validateDependencies(pendingChanges, deployedNames);

    // Topological sort
    const sortedChanges = topologicalSort(pendingChanges);

    // Build tag lookup: change_id -> tag name strings (for registry events)
    const changeTagMap = buildChangeTagMap(plan);

    // Build tag object lookup: change_id -> Tag[] (for recordTag calls)
    const changeTagObjectMap = buildChangeTagObjectMap(plan);

    // Build script name lookup: change_id -> script filename (handles reworks)
    const scriptNameMap = buildScriptNameMap(plan);

    // Build dependency name -> change_id map once (avoids O(C*N) rebuild per change)
    const dependencyMap = buildDependencyMap(allChanges);

    // 9. Set up TUI progress dashboard
    const outputCfg = getConfig();
    const useTUI = shouldUseTUI({ noTui: options.noTui, quiet: outputCfg.quiet });
    const progress = new DeployProgress({ isTTY: useTUI });
    const deployStartTime = Date.now();
    progress.start(sortedChanges.length);

    // 10. Execute each pending change
    let deployedCount = 0;
    let failedCount = 0;

    for (const change of sortedChanges) {
      // Check for shutdown request
      if (shutdownMgr.isShuttingDown()) {
        progress.finish({
          totalDeployed: deployedCount,
          totalFailed: failedCount,
          totalSkipped: 0,
          elapsedMs: Date.now() - deployStartTime,
        });
        return {
          deployed: deployedCount,
          skipped: 0,
          dryRun: false,
          error: "Deploy interrupted by signal",
        };
      }

      // Use the script name map to resolve reworked changes to their
      // versioned script (e.g. add_users@v1.0.sql for the original).
      const changeScriptName = scriptNameMap.get(change.change_id) ?? change.name;
      const deployScript = scriptPath(topDir, deployDir, changeScriptName);
      if (!existsSync(deployScript)) {
        progress.updateChange(change.name, "failed");
        failedCount++;
        progress.finish({
          totalDeployed: deployedCount,
          totalFailed: failedCount,
          totalSkipped: 0,
          elapsedMs: Date.now() - deployStartTime,
        });
        return {
          deployed: deployedCount,
          skipped: 0,
          dryRun: false,
          failedChange: change.name,
          error: `Deploy script not found: ${deployScript}`,
        };
      }

      // Read deploy script once — reuse for auto-commit check, hash, and include resolution
      const scriptBytes = readFileSync(deployScript);
      const scriptContent = scriptBytes.toString("utf-8");
      const autoCommit = isAutoCommit(scriptContent);
      const scriptHash = computeScriptHashFromBytes(scriptBytes);

      // Resolve lock_timeout for this script
      let effectiveLockTimeout: number | undefined = options.lockTimeout;
      if (effectiveLockTimeout != null && !shouldSetLockTimeout(scriptContent)) {
        // Script already sets lock_timeout — skip auto-set
        effectiveLockTimeout = undefined;
      }

      // Determine transaction mode for psql
      // Sqitch does NOT pass --single-transaction by default.
      const useSingleTransaction = !autoCommit && (options.mode === "all" || options.mode === "tag");

      // Mark change as running in TUI
      progress.updateChange(change.name, "running");
      const changeStartTime = Date.now();

      if (!useTUI) {
        info(`Deploying change: ${change.name}`);
      }

      // Resolve snapshot includes (if any) before executing — pass pre-read content
      const resolved = resolveDeployIncludes(
        deployScript,
        change.planned_at,
        topDir,
        undefined, // commitHash — let resolveDeployIncludes look it up from planned_at
        options.noSnapshot,
        scriptContent,
      );

      // Execute via psql — use assembled content when includes were resolved,
      // otherwise pass the original script file (preserving psql's own \i handling
      // when --no-snapshot is set or there are no includes).
      let psqlResult: PsqlRunResult;
      if (resolved && !options.noSnapshot) {
        psqlResult = await psqlRunner.runContent(resolved.content, {
          uri: standardUri,
          singleTransaction: useSingleTransaction,
          variables: options.variables,
          dbClient: options.dbClient,
          workingDir: topDir,
          lockTimeout: effectiveLockTimeout,
        });
      } else {
        psqlResult = await psqlRunner.run(deployScript, {
          uri: standardUri,
          singleTransaction: useSingleTransaction,
          variables: options.variables,
          dbClient: options.dbClient,
          workingDir: topDir,
          lockTimeout: effectiveLockTimeout,
        });
      }

      const changeDuration = Date.now() - changeStartTime;

      if (psqlResult.exitCode !== 0) {
        // Deploy script failed
        const errMsg = psqlResult.error?.message ?? psqlResult.stderr;
        progress.updateChange(change.name, "failed", changeDuration);
        failedCount++;
        logError(`Deploy failed on change "${change.name}": ${errMsg}`);

        // Record fail event
        try {
          await registry.recordFail({
            change_id: change.change_id,
            script_hash: scriptHash,
            change: change.name,
            project: projectName,
            note: change.note,
            committer_name: options.committerName,
            committer_email: options.committerEmail,
            planned_at: new Date(change.planned_at),
            planner_name: change.planner_name,
            planner_email: change.planner_email,
            requires: change.requires,
            conflicts: change.conflicts,
            tags: changeTagMap.get(change.change_id) ?? [],
            dependencies: buildDependencies(change, dependencyMap),
          });
        } catch {
          // Best effort — don't mask the original error
        }

        progress.finish({
          totalDeployed: deployedCount,
          totalFailed: failedCount,
          totalSkipped: 0,
          elapsedMs: Date.now() - deployStartTime,
        });

        return {
          deployed: deployedCount,
          skipped: 0,
          dryRun: false,
          failedChange: change.name,
          error: errMsg,
        };
      }

      // Mark change as done in TUI
      progress.updateChange(change.name, "done", changeDuration);

      // Record successful deploy in tracking tables
      const recordInput: RecordDeployInput = {
        change_id: change.change_id,
        script_hash: scriptHash,
        change: change.name,
        project: projectName,
        note: change.note,
        committer_name: options.committerName,
        committer_email: options.committerEmail,
        planned_at: new Date(change.planned_at),
        planner_name: change.planner_name,
        planner_email: change.planner_email,
        requires: change.requires,
        conflicts: change.conflicts,
        tags: changeTagMap.get(change.change_id) ?? [],
        dependencies: buildDependencies(change, dependencyMap),
      };

      // Record tracking update in its own transaction (psql runs in a
      // separate process, so the tracking connection always needs its own
      // transaction regardless of the script's transaction mode).
      await db.transaction(async () => {
        await registry.recordDeploy(recordInput);
      });

      // Record any tags attached to this change (O(1) lookup instead of linear scan)
      const changeTags = changeTagObjectMap.get(change.change_id) ?? [];
      for (const tag of changeTags) {
        await registry.recordTag({
          tag_id: tag.tag_id,
          tag: `@${tag.name}`,
          project: projectName,
          change_id: change.change_id,
          note: tag.note,
          committer_name: options.committerName,
          committer_email: options.committerEmail,
          planned_at: new Date(tag.planned_at),
          planner_name: tag.planner_name,
          planner_email: tag.planner_email,
        });
      }

      deployedCount++;

      // Run verify if enabled
      if (options.verify) {
        const verifyScript = scriptPath(topDir, verifyDir, changeScriptName);
        if (existsSync(verifyScript)) {
          verbose(`Verifying change: ${change.name}`);
          const verifyResult = await psqlRunner.run(verifyScript, {
            uri: standardUri,
            variables: options.variables,
            dbClient: options.dbClient,
            workingDir: topDir,
          });
          if (verifyResult.exitCode !== 0) {
            const errMsg = verifyResult.error?.message ?? verifyResult.stderr;
            logError(`Verify failed for change "${change.name}": ${errMsg}`);
            progress.finish({
              totalDeployed: deployedCount,
              totalFailed: 1,
              totalSkipped: 0,
              elapsedMs: Date.now() - deployStartTime,
            });
            return {
              deployed: deployedCount,
              skipped: 0,
              dryRun: false,
              failedChange: change.name,
              error: `Verify failed: ${errMsg}`,
            };
          }
        }
      }
    }

    // 10a. Update expand/contract tracker state after successful deploys
    if (options.phase && deployedCount > 0) {
      const tracker = new ExpandContractTracker(db);
      try {
        await tracker.ensureSchema();

        if (options.phase === "expand") {
          // For each deployed expand change, create or update tracker state
          for (const change of sortedChanges) {
            if (isExpandChange(change.name)) {
              const baseName = change.name.replace(/_expand$/, "");
              const existing = await tracker.getOperationByName(projectName, baseName);
              if (!existing) {
                // Create new operation in "expanding" phase, then transition to "expanded"
                const op = await tracker.createOperation({
                  change_name: baseName,
                  project: projectName,
                  table_schema: "public",
                  table_name: baseName,
                  started_by: options.committerEmail,
                });
                await tracker.transitionPhase(op.id, "expanded");
                verbose(`Tracker: "${baseName}" -> expanded`);
              }
            }
          }
        } else if (options.phase === "contract") {
          // For each deployed contract change, transition tracker to "completed"
          for (const change of sortedChanges) {
            if (isContractChange(change.name)) {
              const baseName = change.name.replace(/_contract$/, "");
              const existing = await tracker.getOperationByName(projectName, baseName);
              if (existing && existing.phase === "contracting") {
                await tracker.transitionPhase(existing.id, "completed");
                verbose(`Tracker: "${baseName}" -> completed`);
              }
            }
          }
        }
      } catch (err) {
        // Tracker updates are best-effort — the deploy itself succeeded
        verbose(`Tracker state update failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    // 11. Print summary
    progress.finish({
      totalDeployed: deployedCount,
      totalFailed: failedCount,
      totalSkipped: 0,
      elapsedMs: Date.now() - deployStartTime,
    });
    if (!useTUI) {
      info(`Deployed ${deployedCount} change(s) successfully.`);
    }

    return { deployed: deployedCount, skipped: 0, dryRun: false, analysis: analysisResult };
  } finally {
    // Always release advisory lock
    if (lockAcquired) {
      try {
        await db.query("SELECT pg_advisory_unlock($1, $2)", [ADVISORY_LOCK_NAMESPACE, lockKey]);
        verbose("Advisory lock released");
      } catch {
        // Best effort — PG releases on disconnect
      }
    }

    await db.disconnect();
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Build the change -> tag name mapping from the plan.
 * Returns tags formatted as "@tagname" for the events table.
 */
/**
 * Build a mapping from change_id to the script filename (without .sql).
 *
 * For reworked changes the earlier version's script lives at
 * `<name>@<tag>.sql` (created by `sqlever rework`). The latest version
 * keeps `<name>.sql`. This function inspects the plan for duplicate
 * change names and resolves the correct script name for each version.
 */
export function buildScriptNameMap(plan: Plan): Map<string, string> {
  const map = new Map<string, string>();

  // Group change indices by name to detect reworks
  const nameIndices = new Map<string, number[]>();
  for (let i = 0; i < plan.changes.length; i++) {
    const c = plan.changes[i]!;
    const indices = nameIndices.get(c.name) ?? [];
    indices.push(i);
    nameIndices.set(c.name, indices);
  }

  // Build a change_id -> tag name lookup for tags that follow a change
  // Collect all tags indexed by the change_id they are attached to
  const changeIdToTag = new Map<string, string>();
  for (const tag of plan.tags) {
    // First tag wins — we only need the first tag after each change
    if (!changeIdToTag.has(tag.change_id)) {
      changeIdToTag.set(tag.change_id, tag.name);
    }
  }

  for (const [name, indices] of nameIndices) {
    if (indices.length === 1) {
      // No rework — use plain name
      map.set(plan.changes[indices[0]!]!.change_id, name);
    } else {
      // Multiple occurrences — earlier versions use name@tag
      for (let j = 0; j < indices.length; j++) {
        const changeIdx = indices[j]!;
        const change = plan.changes[changeIdx]!;

        if (j < indices.length - 1) {
          // Earlier version: find the tag between this occurrence and
          // the next. Walk from this change forward to find the first
          // tagged change.
          let tagName: string | undefined;
          const nextIdx = indices[j + 1]!;
          for (let k = changeIdx; k < nextIdx; k++) {
            tagName = changeIdToTag.get(plan.changes[k]!.change_id);
            if (tagName) break;
          }
          map.set(change.change_id, tagName ? `${name}@${tagName}` : name);
        } else {
          // Latest version — uses the plain name
          map.set(change.change_id, name);
        }
      }
    }
  }

  return map;
}

function buildChangeTagMap(plan: Plan): Map<string, string[]> {
  const map = new Map<string, string[]>();
  for (const tag of plan.tags) {
    const existing = map.get(tag.change_id) ?? [];
    existing.push(`@${tag.name}`);
    map.set(tag.change_id, existing);
  }
  return map;
}

/**
 * Build a mapping from change_id to full Tag objects (for recordTag calls).
 * O(1) lookup per change replaces O(T) linear scan via plan.tags.filter.
 */
function buildChangeTagObjectMap(plan: Plan): Map<string, Tag[]> {
  const map = new Map<string, Tag[]>();
  for (const tag of plan.tags) {
    const existing = map.get(tag.change_id) ?? [];
    existing.push(tag);
    map.set(tag.change_id, existing);
  }
  return map;
}

/**
 * Build a name -> change_id lookup map (first occurrence wins for reworked changes).
 * Constructed once before the deploy loop and passed to buildDependencies.
 */
export function buildDependencyMap(allChanges: Change[]): Map<string, string> {
  const map = new Map<string, string>();
  for (const c of allChanges) {
    if (!map.has(c.name)) {
      map.set(c.name, c.change_id);
    }
  }
  return map;
}

/**
 * Build dependency records for a change, resolving dependency IDs from
 * a pre-built name-to-ID map.
 */
function buildDependencies(
  change: Change,
  dependencyMap: Map<string, string>,
): RecordDeployInput["dependencies"] {
  const deps: RecordDeployInput["dependencies"] = [];

  for (const req of change.requires) {
    // Resolve name@tag to base name for lookup
    const baseName = req.indexOf("@") === -1 ? req : req.slice(0, req.indexOf("@"));
    deps.push({
      type: "require",
      dependency: req,
      dependency_id: dependencyMap.get(baseName) ?? null,
    });
  }

  for (const conflict of change.conflicts) {
    const baseName = conflict.indexOf("@") === -1 ? conflict : conflict.slice(0, conflict.indexOf("@"));
    deps.push({
      type: "conflict",
      dependency: conflict,
      dependency_id: dependencyMap.get(baseName) ?? null,
    });
  }

  return deps;
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

/**
 * Run the deploy command from CLI args.
 *
 * Returns an exit code instead of calling process.exit() directly,
 * so that callers (and finally blocks) can run cleanup before exiting.
 */
export async function runDeploy(args: ParsedArgs): Promise<number> {
  // Check for DBLab flags — if present, delegate to the DBLab workflow
  const dblabOpts = parseDblabOptions(args);
  if (dblabOpts) {
    return runDblabDeploy(dblabOpts);
  }

  const options = parseDeployOptions(args);

  if (!options.dbUri) {
    logError("No database URI specified. Use --db-uri or configure a target in sqitch.conf.");
    return EXIT_DEPLOY_FAILED;
  }

  // Set up signal handling
  shutdownManager.register({ quiet: false });

  // Parse plan file once — reuse for DB session settings and executeDeploy
  const projectDir = resolve(options.projectDir);
  const config = loadConfig(options.projectDir);
  let plan: Plan | undefined;
  let projectName = "unknown";
  try {
    plan = loadPlan(projectDir, config);
    projectName = plan.project.name;
  } catch {
    // Plan file may not exist yet; continue with default project name
  }

  const db = new DatabaseClient(options.dbUri, {
    command: "deploy",
    project: projectName,
    statementTimeout: 0,
    idleInTransactionSessionTimeout: 600_000,
  });
  const registry = new Registry(db);
  const psqlRunner = new PsqlRunner(options.dbClient);

  const result = await executeDeploy(options, {
    db,
    registry,
    psqlRunner,
    config,
    shutdownMgr: shutdownManager,
  }, plan);

  if (result.error && !result.dryRun) {
    if (result.error === "Static analysis errors found") {
      return EXIT_ANALYSIS_BLOCKED;
    }
    if (result.error === "Concurrent deploy detected") {
      return EXIT_CONCURRENT_DEPLOY;
    }
    return EXIT_DEPLOY_FAILED;
  }

  return 0;
}
