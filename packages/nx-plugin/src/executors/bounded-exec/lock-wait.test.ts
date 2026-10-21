import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import {
  describeHolders,
  lockCandidates,
  lockWaits,
  type OpenFile,
  type PathOpener,
  parseLsofFields,
  parseLsofTable,
} from './lock-wait.js';

// The rows of the 2026-10-21 cargo case: `cargo --frozen test` blocked on the package cache while another cargo held
// it open. The cwd and txt rows are never lock candidates; the numbered REG row is the only one.
const INCIDENT_HEADER = 'COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME';
const INCIDENT_CWD = 'cargo 94769 dev cwd DIR 1,330 2624 2 /private/var/folders/cowshed/502/mnt/repo/shed';
const INCIDENT_TXT = 'cargo 94769 dev txt REG 1,20 6268080 1152921500312225049 /usr/lib/libobjc.A.dylib';
const INCIDENT_CACHE = 'cargo 94769 dev 5u REG 1,20 0 123 /Users/dev/.cargo/.package-cache';
const INCIDENT_TABLE = [INCIDENT_HEADER, INCIDENT_CWD, INCIDENT_TXT, INCIDENT_CACHE].join('\n');
const CACHE = '/Users/dev/.cargo/.package-cache';
const CWD = '/private/var/folders/cowshed/502/mnt/repo/shed';

const incidentRows: OpenFile[] = [
  { pid: 94769, fd: 'cwd', type: 'DIR', path: CWD },
  { pid: 94769, fd: 'txt', type: 'REG', path: '/usr/lib/libobjc.A.dylib' },
  { pid: 94769, fd: '5u', type: 'REG', path: CACHE },
];

describe('parseLsofTable', () => {
  it('reads the incident rows and skips the header', () => {
    expect(parseLsofTable(INCIDENT_TABLE)).toEqual(incidentRows);
  });

  it('skips any row it cannot parse, and keeps spaces inside NAME', () => {
    const text = [
      INCIDENT_HEADER,
      'garbage',
      '',
      'cargo 94769 dev 5u',
      'cargo 1 dev 3u REG 1,2 0 9 /Users/dev/My Cache/a file',
    ].join('\n');
    expect(parseLsofTable(text)).toEqual([{ pid: 1, fd: '3u', type: 'REG', path: '/Users/dev/My Cache/a file' }]);
  });
});

describe('lockCandidates', () => {
  it('keeps only the numbered REG row of the incident', () => {
    expect(lockCandidates(incidentRows)).toEqual([CACHE]);
  });

  it('keeps numbered regular files and nothing else, sorted and distinct', () => {
    const row = (fd: string, type: string, path: string): OpenFile => ({ pid: 7, fd, type, path });
    expect(
      lockCandidates([
        row('255r', 'REG', '/b'),
        row('txt', 'REG', '/unnumbered-txt'),
        row('mem', 'REG', '/unnumbered-mem'),
        row('5u', 'DIR', '/a-dir'),
        row('3r', 'CHR', '/dev/null'),
        row('10w', 'REG', '/a'),
        row('11w', 'REG', '/a'),
      ]),
    ).toEqual(['/a', '/b']);
  });
});

describe('lockWaits', () => {
  it('names the outside process that holds the incident cache open', () => {
    const openers: PathOpener[] = [{ pid: 51234, command: 'cargo', path: CACHE }];
    expect(lockWaits(incidentRows, new Set([94769]), openers)).toEqual([
      { path: CACHE, waiters: [94769], holders: [{ pid: 51234, command: 'cargo' }] },
    ]);
  });

  it('names no wait when only processes of the run hold the file', () => {
    const openers: PathOpener[] = [{ pid: 94769, command: 'cargo', path: CACHE }];
    expect(lockWaits(incidentRows, new Set([94769]), openers)).toEqual([]);
  });

  it('names no wait for a file the run holds only through an unnumbered descriptor', () => {
    const openers: PathOpener[] = [{ pid: 51234, command: 'cargo', path: '/usr/lib/libobjc.A.dylib' }];
    expect(lockWaits(incidentRows, new Set([94769]), openers)).toEqual([]);
  });

  it('deduplicates holders by pid and lists every waiter of the path', () => {
    const tree: OpenFile[] = [...incidentRows, { pid: 94770, fd: '4u', type: 'REG', path: CACHE }];
    const openers: PathOpener[] = [
      { pid: 51234, command: 'cargo', path: CACHE },
      { pid: 51234, command: 'cargo', path: CACHE },
      { pid: 60000, command: 'rustc', path: CACHE },
    ];
    expect(lockWaits(tree, new Set([94769, 94770]), openers)).toEqual([
      {
        path: CACHE,
        waiters: [94769, 94770],
        holders: [
          { pid: 51234, command: 'cargo' },
          { pid: 60000, command: 'rustc' },
        ],
      },
    ]);
  });
});

describe('parseLsofFields', () => {
  it('reads the holders of a path from lsof -F pcn output, file records included', () => {
    const text = 'p63368\ncperl\nf3\nn/private/tmp/held\np63372\ncperl\nf3\nn/private/tmp/held\n';
    expect(parseLsofFields(text)).toEqual([
      { pid: 63368, command: 'perl', path: '/private/tmp/held' },
      { pid: 63372, command: 'perl', path: '/private/tmp/held' },
    ]);
    expect(parseLsofFields('')).toEqual([]);
  });

  it('refuses any output that is not that shape', () => {
    for (const text of [
      'n/x\n',
      'p12\nn/x\n',
      'pnope\ncsh\nn/x\n',
      'p12\ncsh\nq1\n',
      'p12\ncsh\nc sh\nn/x\n',
      'p0\ncsh\nn/x\n',
    ]) {
      expect(parseLsofFields(text), JSON.stringify(text)).toBeNull();
    }
  });
});

describe('describeHolders', () => {
  it('names each holder by pid and command', () => {
    expect(
      describeHolders([
        { pid: 51234, command: 'cargo' },
        { pid: 9, command: 'rustc' },
      ]),
    ).toBe('pid 51234 (cargo), pid 9 (rustc)');
  });
});

// Generators shaped like `lsof -nP -p` rows: COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME.
const paths = fc.stringMatching(/^\/[A-Za-z0-9._ -]{0,30}[A-Za-z0-9._-]$/);
const fds = fc.stringMatching(/^(\d{1,3}[rwu]|cwd|txt|mem|rtd)$/);
const types = fc.constantFrom('REG', 'DIR', 'CHR', 'IPv4', 'PIPE');

function renderTable(rows: readonly OpenFile[]): string {
  return rows.map((row) => `cmd ${row.pid} dev ${row.fd} ${row.type} 1,20 0 1 ${row.path}`).join('\n');
}

describe('lsof reading properties', () => {
  it('reads back every row that renders as lsof prints it', () => {
    const rows = fc.array(fc.record({ pid: fc.integer({ min: 1, max: 99_999 }), fd: fds, type: types, path: paths }), {
      maxLength: 20,
    });
    fc.assert(fc.property(rows, (generated) => expect(parseLsofTable(renderTable(generated))).toEqual(generated)));
  });

  it('reads back every process and path that renders as lsof -F prints it', () => {
    const processes = fc.array(
      fc.record({
        pid: fc.integer({ min: 1, max: 99_999 }),
        command: fc.stringMatching(/^[a-z][a-z0-9_.-]{0,15}$/),
        paths: fc.array(paths, { minLength: 1, maxLength: 4 }),
      }),
      { maxLength: 8 },
    );
    fc.assert(
      fc.property(processes, (generated) => {
        const text = generated
          .map(({ pid, command, paths: names }) =>
            [`p${pid}`, `c${command}`, ...names.flatMap((name) => ['f3', `n${name}`])].join('\n'),
          )
          .join('\n');
        const expected = generated.flatMap(({ pid, command, paths: names }) =>
          names.map((path) => ({ pid, command, path })),
        );
        expect(parseLsofFields(`${text}\n`)).toEqual(expected);
      }),
    );
  });

  it('names a wait for exactly the paths an outside process holds, and never a process of the run', () => {
    const samplePaths = ['/a/x', '/a/y', '/b z'];
    const row = fc.record({
      pid: fc.integer({ min: 1, max: 12 }),
      fd: fds,
      type: types,
      path: fc.constantFrom(...samplePaths),
    });
    const opener = fc.record({
      pid: fc.integer({ min: 1, max: 12 }),
      command: fc.constantFrom('cargo', 'rustc', 'perl'),
      path: fc.constantFrom(...samplePaths),
    });
    const treePids = fc.uniqueArray(fc.integer({ min: 1, max: 12 }), { maxLength: 12 });
    fc.assert(
      fc.property(
        fc.array(row, { maxLength: 12 }),
        treePids,
        fc.array(opener, { maxLength: 12 }),
        (tree, pidList, openers) => {
          const pids = new Set(pidList);
          const waits = lockWaits(tree, pids, openers);
          const names = waits.map((wait) => wait.path);
          expect(names).toEqual([...names].sort());
          expect(new Set(names).size).toBe(names.length);
          for (const wait of waits) {
            expect(wait.holders.length).toBeGreaterThan(0);
            expect(wait.holders.some((holder) => pids.has(holder.pid))).toBe(false);
            expect(new Set(wait.holders.map((holder) => holder.pid)).size).toBe(wait.holders.length);
            expect(wait.waiters.length).toBeGreaterThan(0);
            expect(wait.waiters.every((pid) => tree.some((file) => file.pid === pid))).toBe(true);
          }
          for (const path of lockCandidates(tree)) {
            const outside = openers.some((opener) => opener.path === path && !pids.has(opener.pid));
            expect(names.includes(path)).toBe(outside);
          }
        },
      ),
    );
  });
});
