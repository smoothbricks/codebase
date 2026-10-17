import { describe, expect, it } from 'bun:test';

import { createRunProgress, withProgressReport } from './progress.js';

describe('run progress', () => {
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
