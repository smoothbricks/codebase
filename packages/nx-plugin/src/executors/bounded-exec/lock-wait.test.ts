import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { describeLockWait, lockCandidates, lockWaits, type OpenFile, parseLsofTable } from './lock-wait.js';

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
  it('names no wait in the 2026-10-21 tree, whose one process held the cache and whose holder ran outside it', () => {
    expect(lockWaits(incidentRows)).toEqual([]);
  });

  it('names a lockable file two processes of the tree hold, with both', () => {
    const tree: OpenFile[] = [...incidentRows, { pid: 94770, fd: '4u', type: 'REG', path: CACHE }];
    expect(lockWaits(tree)).toEqual([{ path: CACHE, holders: [94769, 94770] }]);
  });

  it('names no wait for a file two processes hold only through an unnumbered descriptor', () => {
    const tree: OpenFile[] = [
      ...incidentRows,
      { pid: 94770, fd: 'txt', type: 'REG', path: '/usr/lib/libobjc.A.dylib' },
    ];
    expect(lockWaits(tree)).toEqual([]);
  });

  it('counts a process once however many descriptors it holds the file on', () => {
    const tree: OpenFile[] = [...incidentRows, { pid: 94769, fd: '6u', type: 'REG', path: CACHE }];
    expect(lockWaits(tree)).toEqual([]);
  });
});

describe('describeLockWait', () => {
  it('names the file and each process of the tree that holds it', () => {
    expect(describeLockWait({ path: CACHE, holders: [94769, 94770] })).toBe(`${CACHE} open by pid 94769, 94770`);
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

  it('names a wait for exactly the lockable paths two or more processes of the tree hold', () => {
    const samplePaths = ['/a/x', '/a/y', '/b z'];
    const row = fc.record({
      pid: fc.integer({ min: 1, max: 12 }),
      fd: fds,
      type: types,
      path: fc.constantFrom(...samplePaths),
    });
    fc.assert(
      fc.property(fc.array(row, { maxLength: 12 }), (tree) => {
        const waits = lockWaits(tree);
        const names = waits.map((wait) => wait.path);
        expect(names).toEqual([...names].sort());
        expect(new Set(names).size).toBe(names.length);
        for (const wait of waits) {
          expect(wait.holders).toEqual([...new Set(wait.holders)].sort((a, b) => a - b));
          expect(wait.holders.every((pid) => tree.some((file) => file.pid === pid && file.path === wait.path))).toBe(
            true,
          );
        }
        for (const path of lockCandidates(tree)) {
          const holders = new Set(
            tree.filter((file) => file.path === path && file.type === 'REG' && /^\d/.test(file.fd)).map((f) => f.pid),
          );
          expect(names.includes(path)).toBe(holders.size >= 2);
        }
      }),
    );
  });
});
