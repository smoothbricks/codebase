import { describe, expect, it } from 'bun:test';

import { createRunProgress, withProgressReport } from './progress.js';

describe('run progress', () => {
  it('observes Go package execution and test completions, not build output or heartbeats', () => {
    const progress = createRunProgress();
    expect(
      progress.scan('stdout', '{"Action":"build-output","ImportPath":"example/first","Output":"linking\\n"}\n'),
    ).toBe(false);
    expect(progress.inFlight()).toEqual([]);
    expect(progress.scan('stdout', '{"Action":"start","Package":"example/first"}\n')).toBe(false);
    expect(progress.inFlight()).toEqual(['example/first']);
    expect(progress.scan('stdout', '{"Action":"run","Package":"example/first","Test":"TestReady"}\n')).toBe(false);
    expect(progress.inFlight()).toEqual(['example/first TestReady']);
    expect(progress.scan('stdout', '{"Action":"output","Package":"example/first","Output":"...\\n"}\n')).toBe(false);
    expect(progress.scan('stdout', '{"Action":"pass","Package":"example/first","Test":"TestReady"}\n')).toBe(true);
    expect(progress.scan('stdout', '{"Action":"pass","Package":"example/first"}\n')).toBe(true);
    expect(progress.inFlight()).toEqual([]);
    expect(
      progress.scan('stdout', '{"Action":"build-output","ImportPath":"example/second","Output":"compiling\\n"}\n'),
    ).toBe(false);
    expect(progress.scan('stdout', '{"Action":"start","Package":"example/second"}\n')).toBe(false);
    expect(progress.scan('stdout', '{"Action":"run","Package":"example/second","Test":"TestHang"}\n')).toBe(false);
    expect(progress.inFlight()).toEqual(['example/second TestHang']);
  });

  it('tracks Go package startup and parallel packages until their terminal reports', () => {
    const progress = createRunProgress('go test ./...');
    progress.scan('stdout', '{"Action":"start","Package":"first"}\n{"Action":"start","Package":"second"}\n');
    expect(progress.inFlight()).toEqual(['first', 'second']);
    progress.scan('stdout', '{"Action":"pass","Package":"first"}\n');
    expect(progress.inFlight()).toEqual(['second']);
    progress.scan('stdout', '{"Action":"fail","Package":"second"}\n');
    expect(progress.inFlight()).toEqual([]);
  });

  it('buffers split Go events and does not mistake test output for lifecycle events', () => {
    const progress = createRunProgress('go test ./...');
    expect(progress.scan('stdout', '{"Action":"sta')).toBe(false);
    expect(progress.scan('stdout', 'rt","Package":"example"}\n')).toBe(false);
    expect(progress.scan('stdout', '{"Action":"run","Package":"example","Test":"TestHang"}\n')).toBe(false);
    expect(progress.scan('stdout', '{"Action":"pause","Package":"example","Test":"TestHang"}\n')).toBe(false);
    expect(progress.inFlight()).toEqual(['example TestHang']);
    expect(progress.scan('stdout', '{"Action":"cont","Package":"example","Test":"TestHang"}\n')).toBe(false);
    expect(progress.scan('stdout', '{"Action":"output","Package":"example","Output":"test fake ... ok\\n"}\n')).toBe(
      false,
    );
    expect(progress.scan('stderr', '...\n')).toBe(false);
    expect(progress.scan('stderr', '{invalid JSON}\n')).toBe(false);
    expect(progress.scan('stdout', '{"Action":"skip","Package":"example","Test":"TestHang"}\n')).toBe(true);
    expect(progress.inFlight()).toEqual(['example']);
    expect(progress.scan('stdout', '{"Action":"pass","Package":"example"}\n')).toBe(true);
  });

  it('counts a nextest status line with its run counter as a finished test, and a SLOW line as none', () => {
    const progress = createRunProgress();
    expect(progress.scan('stderr', '        SLOW [>  1.000s] (───) nt tests::hangs\n')).toBe(false);
    expect(progress.scan('stderr', '        SLOW [>  1.000s] (───) nt tests::slow3\n')).toBe(false);
    expect(progress.inFlight()).toEqual(['nt tests::hangs', 'nt tests::slow3']);
    expect(progress.scan('stderr', '        PASS [   2.512s] (2/4) nt tests::slow3\n')).toBe(true);
    expect(progress.inFlight()).toEqual(['nt tests::hangs']);
    for (const finished of [
      '        FAIL [   0.443s] (3/4) nt tests::fails',
      '     SIGSEGV [   0.010s] ( 4/12) nt tests::crash',
      '   TRY 2 FAIL [   0.010s] (5/12) nt tests::flaky',
      '   LEAK-FAIL [   0.010s] (6/12) nt tests::leaky',
    ]) {
      expect(createRunProgress().scan('stderr', `${finished}\n`)).toBe(true);
    }
  });

  it('counts libtest result lines and names a test libtest says is still running', () => {
    const progress = createRunProgress();
    expect(progress.scan('stdout', 'running 3 tests\n')).toBe(false);
    expect(progress.scan('stdout', 'test tests::slow has been running for over 60 seconds\n')).toBe(false);
    expect(progress.inFlight()).toEqual(['tests::slow']);
    expect(progress.scan('stdout', 'test tests::quick ... ok\n')).toBe(true);
    expect(progress.scan('stdout', 'test tests::slow ... FAILED\n')).toBe(true);
    expect(progress.inFlight()).toEqual([]);
  });

  it('counts captured Vitest file and test completions, not reporter noise', () => {
    // Vitest 4.1.11 default reporter captures from a worker suite and a deliberately failing fixture.
    // Plain rows have only ANSI colour removed; the streamed case below retains the captured colour.
    for (const [line, finished] of [
      [' ✓ src/runner/do-runner-integration.test.ts (54 tests) 3362ms', true],
      [' ✓ src/runner/do-runner-signal-identity.test.ts (1 test) 694ms', true],
      ['       ✓ keeps a future same-label timer scheduled during successful alarm execution  575ms', true],
      [' ↓ src/runner/do-runner-ingress-cell.test.ts (1 test | 1 skipped)', true],
      // The same terminal form with the optional duration from Vitest's getDurationPrefix.
      [' ↓ src/runner/do-runner-ingress-cell.test.ts (1 test | 1 skipped) 1ms', true],
      [' ❯ reporter.test.js (3 tests | 1 failed | 1 skipped) 4ms', true],
      ['   ✓ reports a passing test 1ms', true],
      ['   × reports a failed test 3ms', true],
      ['stdout | src/runner/do-runner-integration.test.ts', false],
      [' Test Files  24 passed | 1 skipped (25)', false],
      ['      Tests  236 passed | 1 skipped (237)', false],
      [' ❯ src/pending.test.ts (2 tests | 1 failed)', false],
      [' ✓ still waiting...', false],
      [' FAIL  reporter.test.js > reports a failed test', false],
      [' ❯ reporter.test.js:2:47', false],
    ] as const) {
      for (const stream of ['stdout', 'stderr'] as const) {
        expect(createRunProgress('bun vitest run').scan(stream, `${line}\n`)).toBe(finished);
      }
    }
  });

  it('buffers ANSI-coloured Vitest results until their line finishes', () => {
    const progress = createRunProgress('bun vitest run');
    expect(progress.scan('stdout', ' \u001b[32m✓\u001b[39m src/runner/do-runner-integration.test.ts ')).toBe(false);
    expect(progress.scan('stderr', 'still waiting...\n')).toBe(false);
    expect(
      progress.scan(
        'stdout',
        '\u001b[2m(\u001b[22m\u001b[2m54 tests\u001b[22m\u001b[2m)\u001b[22m\u001b[33m 3362\u001b[2mms\u001b[22m\u001b[39m',
      ),
    ).toBe(false);
    expect(progress.scan('stdout', '\n')).toBe(true);
    expect(progress.scan('stdout', '\n')).toBe(false);
    expect(progress.inFlight()).toEqual([]);
  });

  it("counts bun's result lines and each new dot, however the chunks split them", () => {
    const progress = createRunProgress();
    expect(progress.scan('stderr', '(pass) suite > one [0.02ms]\n')).toBe(true);
    expect(progress.scan('stderr', '.')).toBe(true);
    expect(progress.scan('stderr', '.')).toBe(true);
    // The newline that ends the dots line reports nothing new.
    expect(progress.scan('stderr', '\n')).toBe(false);
    expect(progress.scan('stderr', '..\n.')).toBe(true);
    expect(progress.scan('stderr', '\n\n4 pass\n0 fail\n')).toBe(false);
  });

  it('keeps each stream its own partial line', () => {
    const progress = createRunProgress();
    expect(progress.scan('stderr', '        PASS [   0.001s] (1/2) nt ')).toBe(false);
    expect(progress.scan('stdout', 'unrelated\n')).toBe(false);
    expect(progress.scan('stderr', 'tests::split\n')).toBe(true);
  });

  it('finds no progress in a heartbeat, a warning, or ANSI-coloured noise', () => {
    const progress = createRunProgress();
    expect(progress.scan('stdout', 'still waiting...\n')).toBe(false);
    expect(progress.scan('stderr', 'warning: slow host\n')).toBe(false);
    expect(progress.scan('stderr', '\u001b[33m        SLOW\u001b[0m [>  30.000s] (───) nt tests::hangs\n')).toBe(false);
    expect(progress.inFlight()).toEqual(['nt tests::hangs']);
    expect(progress.scan('stderr', '\u001b[32m        PASS\u001b[0m [  31.000s] (1/1) nt tests::hangs\n')).toBe(true);
    expect(progress.inFlight()).toEqual([]);
  });
});

describe('progress report', () => {
  it('asks Go for its package execution events without changing its test selection', () => {
    expect(withProgressReport('go test -count=1 ./hts/... ./host/...')).toBe(
      'go test -json -count=1 ./hts/... ./host/...',
    );
    expect(withProgressReport('go test -json -count=1 ./...')).toBe('go test -json -count=1 ./...');
    expect(withProgressReport('go test -json=false ./first && go test -count=1 ./second')).toBe(
      'go test -json ./first && go test -json -count=1 ./second',
    );
  });

  it('asks a lone bun test for dots, keeping the runtime the target named', () => {
    expect(withProgressReport('bun test --timeout=0 src')).toBe('bun test --dots --timeout=0 src');
    expect(withProgressReport('../x/.runtime/bun test a')).toBe('../x/.runtime/bun test --dots a');
  });

  it('leaves a command with no bun, two runners, or dots already chosen unchanged', () => {
    for (const command of [
      'cargo nextest run -E all()',
      'bun test a && bun test b',
      'bun test --dots src',
      'bun test --reporter=dots src',
      'node script.js',
    ]) {
      expect(withProgressReport(command)).toBe(command);
    }
  });
});
