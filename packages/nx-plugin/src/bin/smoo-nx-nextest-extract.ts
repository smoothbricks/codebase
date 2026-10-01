#!/usr/bin/env node
import { ensureNextestArchiveExtracted, extractWithNextest } from '../nextest-extraction.js';

// Prints the directory holding the archive's extraction on stdout, the only
// thing a runner's `$(...)` captures. Everything said to a person goes to stderr.
const args = process.argv.slice(2);
const archive = args[0];
if (archive === undefined || args.length !== 1 || archive.startsWith('-')) {
  process.stderr.write('usage: smoo-nx-nextest-extract <archive.tar.zst>\n');
  process.exit(2);
}
const say = (line: string) => process.stderr.write(`smoo-nx-nextest-extract: ${line}\n`);
const seconds = (ms: number) => `${(ms / 1000).toFixed(1)}s`;
try {
  const extraction = await ensureNextestArchiveExtracted(archive, extractWithNextest, (event) => {
    switch (event.kind) {
      case 'waiting':
        say(`${event.holder} is extracting ${archive}; waiting for it`);
        break;
      case 'stale-lock':
        say(`${event.holder} stopped refreshing its extraction lock ${seconds(event.ageMs)} ago; extracting instead`);
        break;
    }
  });
  const identity = `${archive} (sha256 ${extraction.key.slice(0, 12)})`;
  switch (extraction.kind) {
    case 'reused':
      say(`running ${identity} from ${extraction.directory}`);
      break;
    case 'extracted':
      say(`extracted ${identity} to ${extraction.directory} in ${seconds(extraction.elapsedMs)}`);
      break;
    case 'awaited':
      say(
        `running ${identity} from ${extraction.directory} after ${seconds(extraction.elapsedMs)} waiting on ${extraction.holder}`,
      );
      break;
  }
  process.stdout.write(`${extraction.directory}\n`);
} catch (error) {
  say(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
