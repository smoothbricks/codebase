import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  findAddedPathSpelling,
  findForbiddenSpellings,
  findLineSpellings,
  formatSpellingFinding,
  LINE_RULES,
  type LineRuleId,
  type SpellingRuleId,
} from './forbidden-spelling-policy.js';
import { diffScope, upstreamBase } from './test-walk-policy.js';

// Every forbidden spelling is assembled at runtime, so this file carries none and needs no exemption from the policy
// it tests.
const word = (...parts: string[]): string => parts.join('');
const cache = word('ca', 'che');
const Cache = word('Ca', 'che');
const CACHE = cache.toUpperCase();
const skipFlag = word('--skip-nx-', cache);
// A variable named like a program reads as one in `${…}` command position, so the program names live in one record.
const program = {
  ps: word('p', 's'),
  lsof: word('ls', 'of'),
  pgrep: word('pg', 'rep'),
  pkill: word('pk', 'ill'),
  killall: word('kill', 'all'),
  ls: word('l', 's'),
  find: word('fi', 'nd'),
};
const proc = word('/pr', 'oc');
const ledger = word('led', 'ger');
const SK: LineRuleId = 'NO-SKIP-NX-CACHE';
const HC: LineRuleId = 'NO-HOST-CENSUS';
const everyLine = [{ start: 1, end: Number.POSITIVE_INFINITY }];

describe('forbidden-spelling-policy', () => {
  it('refuses each planted spelling, passes each control, and does not read itself', () => {
    const lineCases: readonly (readonly [string, readonly LineRuleId[]])[] = [
      // Cache bypass: the flag in any spelling, the environment switch set true; an off value or a name is no bypass.
      [`nx run-many -t test ${skipFlag}`, [SK]],
      [`['nx', 'test', '--skipNx${Cache}']`, [SK]],
      [`nx test --disable-nx-${cache}`, [SK]],
      [`NX_SKIP_NX_${CACHE}=true nx test`, [SK]],
      [`export NX_DISABLE_NX_${CACHE}=true`, [SK]],
      [`process.env.NX_SKIP_NX_${CACHE} = 'true';`, [SK]],
      [`env: { NX_SKIP_NX_${CACHE}: 'true' },`, [SK]],
      [`  NX_DISABLE_NX_${CACHE}: true`, [SK]],
      [`"NX_SKIP_NX_${CACHE}": "true",`, [SK]],
      [`nx test ${skipFlag}=false`, []],
      [`NX_SKIP_NX_${CACHE}=false nx test`, []],
      [`env: { NX_SKIP_NX_${CACHE}: 'false' },`, []],
      [`export NX_DISABLE_NX_${CACHE}=1`, []],
      [`if (process.env.NX_SKIP_NX_${CACHE} === 'true') return;`, []],
      [`// never pass ${skipFlag}`, []],
      [`const skip = nxArgs.skipNx${Cache};`, []],
      [`nx test --skip-remote-${cache}`, []],
      [`nx test ${skipFlag}-reuse`, []],
      // Host census: a selecting option and no scoping one, a signal by name, a listing of every pid.
      [`${program.ps} -axo pid=,ppid=,uid=,time=,command=`, [HC]],
      [`${program.ps} aux | grep node`, [HC]],
      [`${program.ps} -ef`, [HC]],
      [`${program.ps} -E`, [HC]],
      [`${program.pkill} -f node`, [HC]],
      [`  ${program.killall} node`, [HC]],
      [`${program.lsof} -nP -d cwd`, [HC]],
      [`${program.lsof} +D /tmp`, [HC]],
      [`${program.pgrep} -f vitest`, [HC]],
      [`  { ${program.pgrep} -x "$name" || true; } 2>/dev/null`, [HC]],
      [`const r = await probe('${program.ps}', ['-axo', 'pid=,ppid=']);`, [HC]],
      [`spawnSync('${program.lsof}', ['-nP', '-d', 'cwd'])`, [HC]],
      [`Command::new("${program.ps}").args(["-ax", "-o", "pid"])`, [HC]],
      [`let _ = read_dir("${proc}")?;`, [HC]],
      [`readdirSync('${proc}')`, [HC]],
      [`run: ${program.ps} -ef`, [HC]],
      [`  - ${program.ps} -ax`, [HC]],
      [`sudo -n ${program.ps} -ax`, [HC]],
      [`env FOO=1 ${program.ps} -ax`, [HC]],
      [`/usr/local/bin/${program.lsof} -nP -d cwd`, [HC]],
      [`/usr/local/bin/${program.ps} -ax`, [HC]],
      // An unquoted shell command needs no selecting option: `lsof` and `pgrep` select every process by default.
      [`${program.lsof}`, [HC]],
      [`${program.lsof} | head`, [HC]],
      [`${program.pgrep} node`, [HC]],
      [`${program.pgrep} vitest`, [HC]],
      [`  run: ${program.ps} -ax`, [HC]],
      [`  run: ${program.lsof}`, [HC]],
      [`command: ["${program.ps}", "-ax"]`, [HC]],
      [`${program.ls} ${proc}`, [HC]],
      [`${program.find} ${proc} -maxdepth 1`, [HC]],
      [`os.scandir("${proc}")`, [HC]],
      // Scoped to held pids, arguments out of sight, or no program in command position.
      [`${program.ps} -p 12 -o command=`, []],
      [`${program.ps} -o pid= -p 12`, []],
      [`${program.pgrep} -P 12`, []],
      [`${program.lsof} -nP -a -p 12`, []],
      [`${program.lsof} -p 12 -d cwd`, []],
      [`return probe('${program.lsof}', args, deadline);`, []],
      [`const ${program.ps} = list.length;`, []],
      [`${program.ps}.push(x);`, []],
      [`'SIGKILL (external or a ${program.pkill})'`, []],
      [`// ${program.pkill} -f x`, []],
      [`Command::new("${program.pgrep}").args(["-P", &pid.to_string()])`, []],
      [`  const names = sleepers.map((${program.ps}) => ${program.ps}.name);`, []],
      [`\`${program.lsof} +f -- \${path}\` names them`, []],
      [`echo \`${program.ps} -ax\``, [HC]],
      [`readdirSync('${proc}/123/task')`, []],
      [`  - run: ${program.pgrep} -P 12`, []],
      [`  run: ${program.ps} -p 12 -o command=`, []],
      [`${program.ls} ${proc}/123`, []],
      [`echo "see ${program.lsof}"`, []],
      [`throw new Error('${program.lsof} failed');`, []],
      [`  - ${program.pgrep} --parent 12`, []],
    ];
    expect(
      lineCases.map(([line]): readonly [string, readonly SpellingRuleId[]] => [
        line,
        findLineSpellings(line, everyLine, LINE_RULES).map((found) => found.id),
      ]),
    ).toEqual([...lineCases]);

    // A pure deletion adds no line: the kept line its range names is not judged, the same line added is.
    const kept = `a\n${program.ps} -ax\nkeep\n`;
    expect([
      findLineSpellings(kept, [{ start: 2, end: 2, deleted: true }], LINE_RULES).map((found) => found.id),
      findLineSpellings(kept, [{ start: 2, end: 2 }], LINE_RULES).map((found) => found.id),
    ]).toEqual([[], [HC]]);

    const pathCases: readonly (readonly [string, boolean])[] = [
      [`tooling/checks/foo.${ledger}.ts`, true],
      [`packages/billing/src/${ledger}.ts`, false],
      [`scripts/${word('known-', 'broken')}.tsv`, true],
      ['tooling/checks/host-paths.ts', false],
    ];
    expect(pathCases.map(([path]) => [path, findAddedPathSpelling(path) !== undefined])).toEqual(
      pathCases.map(([path, refused]) => [path, refused]),
    );

    for (const own of ['forbidden-spelling-policy.ts', 'forbidden-spelling-policy.test.ts'])
      expect([own, findLineSpellings(readFileSync(join(import.meta.dir, own), 'utf8'), everyLine, LINE_RULES)]).toEqual(
        [own, []],
      );
  });

  it('judges the lines and files a change adds in a real repository, and nothing it keeps', () => {
    const root = mkdtempSync(join(realpathSync(tmpdir()), 'forbidden-spelling-policy-'));
    const git = (...args: string[]): string =>
      execFileSync('git', ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', ...args], {
        cwd: root,
        encoding: 'utf8',
        env: {
          ...process.env,
          GIT_CONFIG_GLOBAL: '/dev/null',
          GIT_CONFIG_NOSYSTEM: '1',
          GIT_CEILING_DIRECTORIES: dirname(root),
        },
      });
    const write = (file: string, source: string): void => {
      mkdirSync(dirname(join(root, file)), { recursive: true });
      writeFileSync(join(root, file), source);
    };
    const inheritedBase = process.env.NX_BASE;
    delete process.env.NX_BASE;
    try {
      // The base holds a census in a file nobody touches, one on a line the change keeps, and one before a deleted line.
      write('scripts/old.sh', `${program.ps} aux\n`);
      write('scripts/keep.sh', `${program.ps} -ax\nkeep\n`);
      write('scripts/run.sh', `#!/bin/sh\n${program.pgrep} -f old\necho two\n`);
      write('doomed.ts', 'export {};\n');
      git('init', '--quiet', '-b', 'main');
      git('add', '.');
      git('commit', '--quiet', '-m', 'base');
      const base = upstreamBase(root);
      if (!base.ok) throw new Error(base.reason);

      // The change: a modified line, a deletion after a kept site, a planted file, a staged ledger, a plain `ledger.ts`, a
      // binary and a deleted file.
      write('scripts/keep.sh', `${program.ps} -ax\n`);
      write('scripts/run.sh', `#!/bin/sh\n${program.pgrep} -f old\necho two\nnx test ${skipFlag}\n`);
      write('tools/new.ts', `spawnSync('${program.lsof}', ['-nP']);\n`);
      // The staged ledger has the deleted file's bytes, which rename detection would report as a move of it.
      write(`tooling/checks/foo.${ledger}.ts`, 'export {};\n');
      git('add', `tooling/checks/foo.${ledger}.ts`);
      write(`packages/billing/src/${ledger}.ts`, 'export {};\n');
      write('blob.bin', `\0${program.pkill} -f x\n`);
      rmSync(join(root, 'doomed.ts'));
      const scope = diffScope(root, base.value);
      if (!scope.ok) throw new Error(scope.reason);
      const scan = findForbiddenSpellings(root, scope.value);
      if (scan.kind !== 'scanned') throw new Error(`${scan.file}: ${scan.detail}`);
      const findings = scan.findings;
      expect(findings.map((found) => [found.file, found.id, found.line, found.column])).toEqual([
        ['scripts/run.sh', SK, 4, 9],
        [`tooling/checks/foo.${ledger}.ts`, 'NO-WAIVER-LEDGER', 1, 1],
        ['tools/new.ts', HC, 1, 12],
      ]);
      expect(findings.map((found) => formatSpellingFinding(found.file, found).split('\n').slice(0, 2))[0]).toEqual([
        `error[${SK}]: a cache bypass manufactures a pass: nx test ${skipFlag}`,
        '  --> scripts/run.sh:4:9',
      ]);
      // A path git names but the tree no longer holds is inactive; one that cannot be inspected stops the whole scan.
      const scans = ['gone.ts', 'scripts/old.sh/inner.ts'].map((file) =>
        findForbiddenSpellings(root, {
          files: [file],
          changes: new Map([[file, everyLine]]),
          added: new Set<string>(),
        }),
      );
      expect(
        scans.map((one) =>
          one.kind === 'scanned'
            ? [one.kind, one.findings.length]
            : [one.kind, one.file, one.detail.includes('ENOTDIR')],
        ),
      ).toEqual([
        ['scanned', 0],
        ['unavailable', 'scripts/old.sh/inner.ts', true],
      ]);
    } finally {
      if (inheritedBase === undefined) delete process.env.NX_BASE;
      else process.env.NX_BASE = inheritedBase;
      rmSync(root, { recursive: true, force: true });
    }
  });
});
