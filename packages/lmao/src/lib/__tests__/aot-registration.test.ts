/**
 * Registration inversion of the SpanBuffer AOT ABI slot (span-buffer/aot/v1).
 *
 * The slot is realm-global module-load state, so each scenario runs in its own
 * subprocess: a host that registers first is adopted, an empty realm gets
 * lmao's default, and a non-conforming occupant is a setup-time TypeError.
 */
import { describe, expect, it } from 'bun:test';
import { fileURLToPath } from 'node:url';

function runFixture(name: string): unknown {
  const probe = Bun.spawnSync({
    cmd: [process.execPath, fileURLToPath(new URL(`./fixtures/${name}`, import.meta.url))],
    stdout: 'pipe',
    stderr: 'pipe',
  });
  if (probe.exitCode !== 0) throw new Error(new TextDecoder().decode(probe.stderr));
  return JSON.parse(new TextDecoder().decode(probe.stdout));
}

describe('span-buffer AOT registration', () => {
  it('adopts a conforming host runtime registered before v1 evaluates', () => {
    expect(runFixture('aot-host-first.fixture.ts')).toEqual({ adopted: true });
  });

  it('installs the frozen lmao default when the slot is empty, and a late registrar throws in its own stack', () => {
    expect(runFixture('aot-default.fixture.ts')).toEqual({
      conforming: true,
      frozen: true,
      lateRegistrationThrew: true,
    });
  });

  it('refuses a non-conforming occupant with the registration-conflict TypeError', () => {
    expect(runFixture('aot-conflict.fixture.ts')).toEqual({
      threw: true,
      typeError: true,
      message: 'TypeError: Conflicting LMAO SpanBuffer AOT runtime registrations',
    });
  });
});
