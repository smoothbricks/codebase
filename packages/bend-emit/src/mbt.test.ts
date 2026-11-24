import { expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { run as runCli } from './cli.js';
import { checkBendFile } from './emit.js';
import {
  type CannotDrive,
  type ExcludedTransition,
  exploreMachine,
  type Machine,
  type MachineBinding,
  walkMachine,
} from './mbt.js';

type GateState = { readonly $: 'Closed' } | { readonly $: 'Open' } | { readonly $: 'Locked' };
type GateInput =
  | { readonly $: 'OpenGate' }
  | { readonly $: 'CloseGate' }
  | { readonly $: 'LockGate' }
  | { readonly $: 'Ping' };

const conformancePath = join(import.meta.dir, '../test/conformance.bend');

function withTempDir<T>(fn: (dir: string) => Promise<T> | T): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), 'bend-emit-mbt-test-'));
  return Promise.resolve(fn(dir)).finally(() => rmSync(dir, { recursive: true, force: true }));
}

function isGateMachine(v: unknown): v is Machine<GateState, GateInput> {
  if (typeof v !== 'object' || v === null) return false;
  return (
    'name' in v &&
    typeof v.name === 'string' &&
    'init' in v &&
    typeof v.init === 'function' &&
    'step' in v &&
    typeof v.step === 'function' &&
    'inputs' in v &&
    typeof v.inputs === 'function' &&
    'showState' in v &&
    typeof v.showState === 'function' &&
    'showInput' in v &&
    typeof v.showInput === 'function'
  );
}

async function loadEmittedGateMachine(dir: string): Promise<Machine<GateState, GateInput>> {
  const book = await checkBendFile(conformancePath);
  const em = book.emit({
    target: { kind: 'ts-module', modelName: 'conformance.bend', flags: [] },
    machines: [
      {
        name: 'gate',
        init: 'gate_init',
        step: 'gate_step',
        input: 'GateInput',
        handled: 'gate_handled',
      },
    ],
  });
  if (em.kind !== 'module') throw new Error('expected ts-module');
  const outPath = join(dir, 'gate-model.ts');
  writeFileSync(outPath, em.source);
  const mod: unknown = await import(`${pathToFileURL(outPath).href}?t=${Date.now()}-${Math.random()}`);
  if (typeof mod !== 'object' || mod === null || !('machine_gate' in mod) || !isGateMachine(mod.machine_gate)) {
    throw new Error('emitted module did not export machine_gate');
  }
  return mod.machine_gate;
}

type GateImpl = { readonly mode: 'closed' | 'open' | 'locked'; readonly pings: number };

function makeGateBinding(overrides?: {
  apply?: (impl: GateImpl, input: GateInput) => GateImpl | CannotDrive;
  excluded?: readonly ExcludedTransition[];
}): MachineBinding<GateState, GateInput, GateImpl> {
  const defaultApply = (impl: GateImpl, input: GateInput): GateImpl => {
    if (impl.mode === 'closed') {
      if (input.$ === 'OpenGate') return { ...impl, mode: 'open' };
      if (input.$ === 'LockGate') return { ...impl, mode: 'locked' };
      return impl;
    }
    if (impl.mode === 'open') {
      if (input.$ === 'CloseGate') return { ...impl, mode: 'closed' };
      if (input.$ === 'Ping') return { ...impl, pings: impl.pings + 1 };
      return impl;
    }
    if (input.$ === 'CloseGate') return { ...impl, mode: 'closed' };
    return impl;
  };
  return {
    create: () => ({ mode: 'closed', pings: 0 }),
    apply: overrides?.apply ?? defaultApply,
    project: (impl): GateState => {
      if (impl.mode === 'closed') return { $: 'Closed' };
      if (impl.mode === 'open') return { $: 'Open' };
      return { $: 'Locked' };
    },
    ...(overrides?.excluded !== undefined ? { excluded: overrides.excluded } : {}),
  };
}

test('gate machine emits inputs_gate and machine_gate in ts-module, loads in Bun, and refuses bad specs or --cap overflow', async () => {
  await withTempDir(async (dir) => {
    const outPath = join(dir, 'cli-gate.ts');
    const manifestPath = join(dir, 'manifest.json');
    await runCli([
      conformancePath,
      dir,
      '--ts-module',
      outPath,
      '--machine',
      'gate:init=gate_init,step=gate_step,input=GateInput,handled=gate_handled',
      '--manifest',
      manifestPath,
    ]);
    const source = readFileSync(outPath, 'utf8');
    expect(source).toContain('export function inputs_gate()');
    expect(source).toContain('export const machine_gate =');

    const mod: unknown = await import(`${pathToFileURL(outPath).href}?cli=${Date.now()}`);
    expect(typeof mod === 'object' && mod !== null && 'machine_gate' in mod && isGateMachine(mod.machine_gate)).toBe(
      true,
    );
    if (typeof mod !== 'object' || mod === null || !('machine_gate' in mod) || !isGateMachine(mod.machine_gate)) {
      throw new Error('missing machine_gate');
    }
    const gate = mod.machine_gate;
    expect(gate.name).toBe('gate');
    expect(gate.inputs().map((i) => gate.showInput(i))).toEqual(['OpenGate', 'CloseGate', 'LockGate', 'Ping']);

    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    expect(manifest.machines).toEqual([
      {
        name: 'gate',
        init: 'gate_init',
        step: 'gate_step',
        stateType: 'GateState',
        inputType: 'GateInput',
        handled: 'gate_handled',
        inputCount: 4,
      },
    ]);

    const book = await checkBendFile(conformancePath);
    // Refuses when enumerated input count exceeds cap
    expect(() =>
      book.emit({
        target: { kind: 'ts-module', modelName: 'conformance.bend', flags: [] },
        machines: [{ name: 'gate', init: 'gate_init', step: 'gate_step', input: 'GateInput' }],
        cap: 2,
      }),
    ).toThrow('bend-emit refuses:');

    // Refuses when input type disagrees with step signature
    expect(() =>
      book.emit({
        target: { kind: 'ts-module', modelName: 'conformance.bend', flags: [] },
        machines: [{ name: 'gate', init: 'gate_init', step: 'gate_step', input: 'Packet' }],
      }),
    ).toThrow('bend-emit refuses:');

    // Refuses on targets other than ts-module
    expect(() =>
      book.emit({
        target: { kind: 'rust-module', modelName: 'conformance.bend', flags: [] },
        machines: [{ name: 'gate', init: 'gate_init', step: 'gate_step', input: 'GateInput' }],
      }),
    ).toThrow('bend-emit refuses:');
  });
});

test('correct gate binding passes transition-coverage at 100% and simple-paths', async () => {
  await withTempDir(async (dir) => {
    const machine = await loadEmittedGateMachine(dir);
    const binding = makeGateBinding();

    const cov = walkMachine(machine, binding, { generator: 'transition-coverage', maxStates: 16 });
    expect(cov.ok).toBe(true);
    expect(cov.failures).toEqual([]);
    expect(cov.transitions).toEqual({ covered: 12, total: 12, uncovered: [] });
    // 7 unhandled self-loops; Open/Ping is declared handled via gate_handled
    expect(cov.unhandled).toEqual({ checked: 7, total: 7, failed: 0 });
    expect(cov.excluded).toEqual([]);
    expect(cov.budget).toEqual({
      maxStates: 16,
      exploredStates: 3,
      exhausted: false,
      unexploredFrontier: 0,
    });
    expect(cov.paths.length).toBe(12);

    const simple = walkMachine(machine, binding, { generator: 'simple-paths', maxDepth: 3, maxStates: 16 });
    expect(simple.ok).toBe(true);
    expect(simple.failures).toEqual([]);
    expect(simple.paths.length).toBeGreaterThan(0);
    // Simple paths never repeat a state key along a path
    for (const p of simple.paths) {
      const statesAlongPath = [...p.steps.map((s) => s.state), p.targetKey];
      expect(new Set(statesAlongPath).size).toBe(statesAlongPath.length);
    }
  });
});

test('planted cannot-drive fails coverage and passes once excluded', async () => {
  await withTempDir(async (dir) => {
    const machine = await loadEmittedGateMachine(dir);
    const base = makeGateBinding();
    const plantedApply = (impl: GateImpl, input: GateInput): GateImpl | CannotDrive => {
      if (impl.mode === 'closed' && input.$ === 'LockGate') return 'cannot-drive';
      return base.apply(impl, input);
    };

    const red = walkMachine(machine, makeGateBinding({ apply: plantedApply }), {
      generator: 'transition-coverage',
      maxStates: 16,
    });
    expect(red.ok).toBe(false);
    expect(red.transitions.covered).toBeLessThan(red.transitions.total);
    expect(red.transitions.uncovered).toContainEqual({ state: 'Closed', input: 'LockGate' });
    expect(
      red.failures.some((f) => f.kind === 'uncovered-transition' && f.state === 'Closed' && f.input === 'LockGate'),
    ).toBe(true);
    expect(red.failures.some((f) => f.kind === 'coverage-incomplete')).toBe(true);

    // Excluding Closed/LockGate and the Locked transitions unreachable without it restores 100% of (total - excluded)
    const excluded: ExcludedTransition[] = [
      { state: 'Closed', input: 'LockGate', why: 'lock hardware not present in this harness' },
      { state: 'Locked', input: 'OpenGate', why: 'Locked unreachable without LockGate' },
      { state: 'Locked', input: 'CloseGate', why: 'Locked unreachable without LockGate' },
      { state: 'Locked', input: 'LockGate', why: 'Locked unreachable without LockGate' },
      { state: 'Locked', input: 'Ping', why: 'Locked unreachable without LockGate' },
    ];
    const green = walkMachine(machine, makeGateBinding({ apply: plantedApply, excluded }), {
      generator: 'transition-coverage',
      maxStates: 16,
    });
    expect(green.ok).toBe(true);
    expect(green.failures).toEqual([]);
    expect(green.transitions).toEqual({ covered: 7, total: 12, uncovered: [] });
    expect(green.excluded).toEqual(excluded);
  });
});

test('excluded row naming no model transition fails closed', async () => {
  await withTempDir(async (dir) => {
    const machine = await loadEmittedGateMachine(dir);
    const result = walkMachine(
      machine,
      makeGateBinding({
        excluded: [{ state: 'Ghost', input: 'OpenGate', why: 'stale exclusion' }],
      }),
      { generator: 'transition-coverage', maxStates: 16 },
    );
    expect(result.ok).toBe(false);
    expect(result.failures).toContainEqual({
      kind: 'invalid-exclusion',
      state: 'Ghost',
      input: 'OpenGate',
      why: 'stale exclusion',
      reason: 'unknown-transition',
    });
  });
});

test('unhandled input that moves the implementation projection fails closed', async () => {
  await withTempDir(async (dir) => {
    const machine = await loadEmittedGateMachine(dir);
    const base = makeGateBinding();
    const buggyApply = (impl: GateImpl, input: GateInput): GateImpl | CannotDrive => {
      // Bug: Open + LockGate is unhandled in the model (must stay Open), but impl moves to Locked!
      if (impl.mode === 'open' && input.$ === 'LockGate') return { ...impl, mode: 'locked' };
      return base.apply(impl, input);
    };

    const result = walkMachine(machine, makeGateBinding({ apply: buggyApply }), {
      generator: 'transition-coverage',
      maxStates: 16,
    });
    expect(result.ok).toBe(false);
    expect(result.unhandled.failed).toBeGreaterThanOrEqual(1);
    expect(result.failures).toContainEqual(
      expect.objectContaining({
        kind: 'unhandled-changed-state',
        state: 'Open',
        input: 'LockGate',
        actualKey: 'Locked',
      }),
    );
  });
});

test('state budget exhaustion is reported and fails closed on any Machine<S, I>', () => {
  // General non-Bend Machine<number, 'inc' | 'dec'> proves mbt.ts does not depend on emitted files.
  const counter: Machine<number, 'inc' | 'dec'> = {
    name: 'counter',
    init: () => 0,
    step: (s, i) => (i === 'inc' ? s + 1 : Math.max(0, s - 1)),
    inputs: () => ['inc', 'dec'],
    showState: (s) => String(s),
    showInput: (i) => i,
  };

  const graph = exploreMachine(counter, 2);
  expect(graph.budget).toEqual({
    maxStates: 2,
    exploredStates: 2,
    exhausted: true,
    unexploredFrontier: 1,
  });

  const binding: MachineBinding<number, 'inc' | 'dec', number> = {
    create: () => 0,
    apply: (impl, input) => (input === 'inc' ? impl + 1 : Math.max(0, impl - 1)),
    project: (impl) => impl,
  };
  const result = walkMachine(counter, binding, { generator: 'transition-coverage', maxStates: 2 });
  expect(result.ok).toBe(false);
  expect(result.budget).toEqual({
    maxStates: 2,
    exploredStates: 2,
    exhausted: true,
    unexploredFrontier: 1,
  });
  expect(result.failures).toContainEqual({
    kind: 'state-budget-exhausted',
    maxStates: 2,
    exploredStates: 2,
    unexploredFrontier: 1,
  });
});
