/**
 * workerd allows `new Function` at startup and refuses it inside a request, where
 * ColumnBuffer and ColumnWriter classes are first built. The materializer is chosen
 * once, at module init, so each scenario runs the fixture in a fresh process; see
 * fixtures/workerdRequestCodegen.fixture.ts.
 *
 * Either workerd signal must select the closure materializer. With neither, the
 * module-init probe succeeds, selects 'compiled', and the first class build fails,
 * which proves the fixture's refusal reaches the compiled path.
 *
 * Spec: specs/lmao/01b6_buffer_codegen_extension.md#smoo/lmao!n/buffer-codegen.materializer-selection
 */
import { describe, expect, it } from 'bun:test';
import { fileURLToPath } from 'node:url';

const fixturePath = fileURLToPath(new URL('./fixtures/workerdRequestCodegen.fixture.ts', import.meta.url));

function runFixture(identity: 'navigator' | 'websocketpair' | 'none') {
  const run = Bun.spawnSync({ cmd: [process.execPath, fixturePath, identity], stdout: 'pipe', stderr: 'pipe' });
  return {
    exitCode: run.exitCode,
    stdout: new TextDecoder().decode(run.stdout),
    // Bun colors uncaught errors when the runner forces color (nx does), which splits the message.
    stderr: Bun.stripANSI(new TextDecoder().decode(run.stderr)),
  };
}

describe('workerd request-context codegen', () => {
  for (const identity of ['navigator', 'websocketpair'] as const) {
    it(`builds ColumnBuffer and ColumnWriter classes via closures when ${identity} identifies workerd`, () => {
      const run = runFixture(identity);
      if (run.exitCode !== 0) throw new Error(run.stderr);
      expect(JSON.parse(run.stdout)).toEqual({
        codegenSupported: false,
        mode: 'closure',
        status: [0, 1],
        count: [1, 2],
        label: ['a', 'b'],
        done: [1],
      });
    });
  }

  it('fails with EvalError on the compiled path when nothing identifies workerd', () => {
    const run = runFixture('none');
    expect(run.exitCode).not.toBe(0);
    expect(run.stderr).toContain('EvalError: Code generation from strings disallowed for this context');
  });
});
