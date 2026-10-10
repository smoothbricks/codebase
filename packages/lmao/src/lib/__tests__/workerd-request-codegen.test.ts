/**
 * Regression: workerd allows `new Function` at startup and refuses it inside a
 * request, so a module-init probe alone picked the compiled materializer and the
 * first request's class build threw EvalError. The fixture reproduces that policy
 * in a fresh process (materializer selection happens once, at module init) and
 * every class is first built after codegen is refused.
 */
import { describe, expect, it } from 'bun:test';
import { fileURLToPath } from 'node:url';

describe('workerd request-context codegen', () => {
  it('builds column buffers, writers, span loggers and result writers via closures', () => {
    const run = Bun.spawnSync({
      cmd: [process.execPath, fileURLToPath(new URL('./fixtures/workerd-request-codegen.fixture.ts', import.meta.url))],
      stdout: 'pipe',
      stderr: 'pipe',
    });
    if (run.exitCode !== 0) throw new Error(new TextDecoder().decode(run.stderr));

    expect(JSON.parse(new TextDecoder().decode(run.stdout))).toEqual({
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
    });
  });
});
