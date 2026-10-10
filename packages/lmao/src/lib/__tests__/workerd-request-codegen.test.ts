/**
 * Regression: workerd allows `new Function` at startup and refuses it inside a
 * request, so a module-init probe alone picked the compiled materializer and the
 * first request's class build threw EvalError. The fixture reproduces that policy
 * in a fresh process (materializer selection happens once, at module init) and
 * every class is first built after codegen is refused.
 *
 * workerd is identified by navigator.userAgent or, in isolates without navigator
 * (compatibility date before 2022-03-21, or no_global_navigator), by the
 * WebSocketPair global. With neither identity the fixture must fail on the compiled
 * path, which proves its refusal reaches the class builds.
 */
import { describe, expect, it } from 'bun:test';
import { fileURLToPath } from 'node:url';

const fixturePath = fileURLToPath(new URL('./fixtures/workerd-request-codegen.fixture.ts', import.meta.url));

function runFixture(identity: 'navigator' | 'websocketpair' | 'none') {
  const run = Bun.spawnSync({ cmd: [process.execPath, fixturePath, identity], stdout: 'pipe', stderr: 'pipe' });
  return {
    exitCode: run.exitCode,
    stdout: new TextDecoder().decode(run.stdout),
    // Bun colors uncaught errors when the runner forces color (nx does), which splits the message.
    stderr: Bun.stripANSI(new TextDecoder().decode(run.stderr)),
  };
}

const requestObservation = {
  codegenRefused: true,
  mode: 'closure',
  columnBuffer: { count: 1.5, phase: 1 },
  columnWriter: { count: 2.5, phase: 1 },
  span: {
    value: 7,
    tagPhase: 0,
    resultStatus: 200,
    resultPhase: 1,
    logMessage: 'handled',
    logStatus: 202,
  },
  wasmSpan: { route: '/orders', status: 404, phase: 1 },
};

describe('workerd request-context codegen', () => {
  for (const identity of ['navigator', 'websocketpair'] as const) {
    it(`builds every class via closures when ${identity} identifies workerd`, () => {
      const run = runFixture(identity);
      if (run.exitCode !== 0) throw new Error(run.stderr);
      expect(JSON.parse(run.stdout)).toEqual(requestObservation);
    });
  }

  it('fails with EvalError on the compiled path when nothing identifies workerd', () => {
    const run = runFixture('none');
    expect(run.exitCode).not.toBe(0);
    expect(run.stderr).toContain('EvalError: Code generation from strings disallowed for this context');
  });
});
