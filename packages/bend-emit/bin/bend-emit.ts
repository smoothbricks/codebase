#!/usr/bin/env bun
import { run } from '../src/cli.js';

try {
  await run(process.argv.slice(2));
} catch (err) {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
}
