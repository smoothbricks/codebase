import { constants, fstatSync, readSync, writeSync } from 'node:fs';
import {
  CAPTURE_FD,
  CAPTURE_LEAVES,
  CAPTURE_READ_ABSENT,
  isMutationReportTool,
  type MutationReportTool,
} from './capture.js';
import { loadOpenBeneath, type OpenBeneath } from './open-beneath.js';

/**
 * The capture reader a `mutationReport` run's executor starts once the command's subtree is retired
 * (`mutation-report.ts`). Started on Bun as `bun --config=/dev/null capture-reader.js <tool> <limit>`, it inherits the
 * run's capture directory on {@link CAPTURE_FD}, held by the executor since it made it, and reads the tool's fixed
 * report leaf beneath that descriptor (`open-beneath.ts`): each directory on the way opened `O_DIRECTORY | O_NOFOLLOW`
 * relative to the one before, the leaf `O_NOFOLLOW | O_NONBLOCK`. No path is resolved from a root or a working
 * directory, so a parent swapped under its name, even swapped back, cannot turn the read to another file: what is read
 * is an entry of the directory the executor holds.
 *
 * The leaf must be a regular file with one link of at most `<limit>` bytes, refused by its size before a byte is read,
 * that neither grows nor shrinks while read. Its bytes go to stdout and the reader exits 0; a run that wrote no report
 * exits {@link CAPTURE_READ_ABSENT}; anything else exits 1 with its reason as the one line on stderr. It takes no other
 * input.
 */

class Refused extends Error {}
class Absent extends Error {}

/** `name` opened beneath `directory` with `flags`. */
function beneath(open: OpenBeneath, directory: number, name: string, flags: number, kind: string): number {
  const opened = open(directory, name, flags);
  switch (opened.kind) {
    case 'opened':
      return opened.fd;
    case 'absent':
      throw new Absent();
    case 'not-it':
      throw new Refused(`${name} is a link or not ${kind}`);
    case 'failed':
      throw new Refused(`${name} could not be opened (${opened.cause})`);
  }
}

/** The report's bytes, read beneath the capture directory on {@link CAPTURE_FD}. */
function read(open: OpenBeneath, tool: MutationReportTool, limit: number): Buffer {
  if (!fstatSync(CAPTURE_FD).isDirectory()) throw new Refused(`descriptor ${CAPTURE_FD} is not the capture directory`);
  const [leaf, ...outward] = [...CAPTURE_LEAVES[tool]].reverse();
  let directory = CAPTURE_FD;
  for (const name of outward.reverse()) {
    directory = beneath(
      open,
      directory,
      name,
      constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
      'a directory',
    );
  }
  const fd = beneath(
    open,
    directory,
    leaf ?? '',
    constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    'a file',
  );
  const info = fstatSync(fd);
  if (!info.isFile()) throw new Refused('the report is not a regular file');
  if (info.nlink !== 1) throw new Refused(`the report has ${info.nlink} links`);
  if (info.size > limit)
    throw new Refused(`the report holds ${info.size} bytes, over Nx's ${limit}-byte artifact bound`);
  const bytes = Buffer.alloc(info.size);
  let at = 0;
  while (at < bytes.length) {
    const got = readSync(fd, bytes, at, bytes.length - at, at);
    if (got === 0) throw new Refused(`the report shrank while read (${at} of ${bytes.length} bytes)`);
    at += got;
  }
  if (readSync(fd, Buffer.alloc(1), 0, 1, at) !== 0) throw new Refused(`the report grew while read (past ${at} bytes)`);
  return bytes;
}

const [tool, limitText] = process.argv.slice(2);
const limit = Number(limitText);
if (process.versions.bun === undefined || !isMutationReportTool(tool) || !Number.isSafeInteger(limit) || limit < 0) {
  writeSync(
    2,
    `capture-reader runs on Bun as \`capture-reader.js <stryker|cargo-mutants> <limit>\` with the capture on fd ${CAPTURE_FD}\n`,
  );
  process.exit(1);
}
try {
  const bytes = read(await loadOpenBeneath(), tool, limit);
  for (let at = 0; at < bytes.length; ) at += writeSync(1, bytes, at);
} catch (error) {
  if (error instanceof Absent) process.exit(CAPTURE_READ_ABSENT);
  writeSync(2, `${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
}
