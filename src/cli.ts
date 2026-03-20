#!/usr/bin/env bun
// sqevo — Sqitch-compatible PostgreSQL migration tool

const [, , cmd, ...args] = process.argv;

const commands: Record<string, () => void> = {
  add: () => console.error("sqevo add: not yet implemented"),
  deploy: () => console.error("sqevo deploy: not yet implemented"),
  revert: () => console.error("sqevo revert: not yet implemented"),
  verify: () => console.error("sqevo verify: not yet implemented"),
  status: () => console.error("sqevo status: not yet implemented"),
  log: () => console.error("sqevo log: not yet implemented"),
};

if (!cmd || cmd === "--help" || cmd === "-h") {
  console.log(`sqevo — Sqitch-compatible PostgreSQL migration tool

Usage:
  sqevo <command> [options]

Commands:
  add       Add a new migration
  deploy    Deploy migrations
  revert    Revert migrations
  verify    Verify deployed migrations
  status    Show deployment status
  log       Show deployment log

Not yet implemented — contributions welcome.
https://github.com/NikolayS/stitch
`);
  process.exit(0);
}

const handler = commands[cmd];
if (!handler) {
  console.error(`sqevo: unknown command '${cmd}'`);
  process.exit(1);
}

handler();
