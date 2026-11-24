export type CannotDrive = 'cannot-drive';

export interface Machine<S, I> {
  readonly name: string;
  init(): S;
  step(state: S, input: I): S;
  inputs(): readonly I[];
  showState(state: S): string;
  showInput(input: I): string;
  /** Optional model predicate declaring self-loops that are genuinely handled transitions (not unhandled). */
  readonly isHandledSelfLoop?: (state: S, input: I) => boolean;
}

export interface ExcludedTransition {
  readonly state: string;
  readonly input: string;
  readonly why: string;
}

export interface MachineBinding<S, I, Impl> {
  create(): Impl;
  apply(impl: Impl, input: I): Impl | CannotDrive;
  project(impl: Impl): S;
  readonly excluded?: readonly ExcludedTransition[];
}

export interface TransitionKey {
  readonly state: string;
  readonly input: string;
}

export interface ModelTransition<S, I> {
  readonly from: S;
  readonly fromKey: string;
  readonly input: I;
  readonly inputKey: string;
  readonly to: S;
  readonly toKey: string;
  /** True iff toKey === fromKey and not declared handled by machine.isHandledSelfLoop. */
  readonly unhandled: boolean;
}

export interface PathStep<S, I> {
  readonly fromKey: string;
  readonly input: I;
  readonly inputKey: string;
  readonly expected: S;
  readonly expectedKey: string;
  readonly unhandled: boolean;
}

export interface MachinePath<S, I> {
  readonly initKey: string;
  readonly steps: readonly PathStep<S, I>[];
  readonly targetKey: string;
}

export interface StateBudget {
  readonly maxStates: number;
  readonly exploredStates: number;
  readonly exhausted: boolean;
  readonly unexploredFrontier: number;
}

export interface ModelGraph<S, I> {
  readonly init: S;
  readonly initKey: string;
  readonly states: ReadonlyMap<string, S>;
  readonly transitions: readonly ModelTransition<S, I>[];
  readonly budget: StateBudget;
  readonly duplicateInputKeys: readonly string[];
}

export interface DrivenPath {
  readonly steps: readonly TransitionKey[];
  readonly targetKey: string;
  readonly ok: boolean;
}

export type MbtFailure =
  | { readonly kind: 'init-mismatch'; readonly expectedKey: string; readonly actualKey: string }
  | {
      readonly kind: 'state-mismatch';
      readonly path: readonly TransitionKey[];
      readonly stepIndex: number;
      readonly state: string;
      readonly input: string;
      readonly expectedKey: string;
      readonly actualKey: string;
    }
  | {
      readonly kind: 'unhandled-changed-state';
      readonly path: readonly TransitionKey[];
      readonly stepIndex: number;
      readonly state: string;
      readonly input: string;
      readonly actualKey: string;
    }
  | {
      readonly kind: 'uncovered-transition';
      readonly state: string;
      readonly input: string;
      readonly reason: 'cannot-drive' | 'not-reached';
      readonly path?: readonly TransitionKey[];
    }
  | {
      readonly kind: 'invalid-exclusion';
      readonly state: string;
      readonly input: string;
      readonly why: string;
      readonly reason: 'unknown-transition' | 'duplicate-exclusion' | 'empty-why';
    }
  | {
      readonly kind: 'state-budget-exhausted';
      readonly maxStates: number;
      readonly exploredStates: number;
      readonly unexploredFrontier: number;
    }
  | { readonly kind: 'path-budget-exhausted'; readonly maxPaths: number; readonly generatedPaths: number }
  | {
      readonly kind: 'coverage-incomplete';
      readonly covered: number;
      readonly required: number;
      readonly total: number;
      readonly excluded: number;
    }
  | { readonly kind: 'duplicate-input-key'; readonly inputKey: string };

export interface MbtResult {
  readonly ok: boolean;
  readonly generator: 'transition-coverage' | 'simple-paths';
  readonly paths: readonly DrivenPath[];
  readonly transitions: {
    readonly covered: number;
    readonly total: number;
    readonly uncovered: readonly TransitionKey[];
  };
  readonly excluded: readonly ExcludedTransition[];
  readonly unhandled: {
    readonly checked: number;
    readonly total: number;
    readonly failed: number;
  };
  readonly budget: StateBudget & {
    readonly maxPaths?: number;
    readonly pathsExhausted?: boolean;
  };
  readonly failures: readonly MbtFailure[];
}

export type WalkOptions =
  | {
      readonly generator: 'transition-coverage';
      readonly maxStates: number;
    }
  | {
      readonly generator: 'simple-paths';
      readonly maxDepth: number;
      readonly maxStates: number;
      readonly maxPaths?: number;
    };

const DEFAULT_MAX_PATHS = 10_000;

export function transitionEdgeKey(stateKey: string, inputKey: string): string {
  return JSON.stringify([stateKey, inputKey]);
}

function toPathStep<S, I>(t: ModelTransition<S, I>): PathStep<S, I> {
  return {
    fromKey: t.fromKey,
    input: t.input,
    inputKey: t.inputKey,
    expected: t.to,
    expectedKey: t.toKey,
    unhandled: t.unhandled,
  };
}

/** Pure BFS reachability up to maxStates. General over any Machine<S, I>. */
export function exploreMachine<S, I>(machine: Machine<S, I>, maxStates: number): ModelGraph<S, I> {
  if (!Number.isInteger(maxStates) || maxStates < 1) {
    throw new RangeError(`maxStates must be a positive integer, got ${String(maxStates)}`);
  }

  const seenInputKeys = new Set<string>();
  const duplicateInputKeys: string[] = [];
  const uniqueInputs: { readonly input: I; readonly key: string }[] = [];
  for (const input of machine.inputs()) {
    const key = machine.showInput(input);
    if (seenInputKeys.has(key)) {
      duplicateInputKeys.push(key);
      continue;
    }
    seenInputKeys.add(key);
    uniqueInputs.push({ input, key });
  }

  const init = machine.init();
  const initKey = machine.showState(init);
  const states = new Map<string, S>([[initKey, init]]);
  const queue: string[] = [initKey];
  const transitions: ModelTransition<S, I>[] = [];
  const frontier = new Set<string>();

  for (let head = 0; head < queue.length; head++) {
    const fromKey = queue[head];
    if (fromKey === undefined) continue;
    const from = states.get(fromKey);
    if (from === undefined) continue;

    for (const { input, key: inputKey } of uniqueInputs) {
      const to = machine.step(from, input);
      const toKey = machine.showState(to);
      const isSelfLoop = toKey === fromKey;
      const handledSelfLoop = isSelfLoop && (machine.isHandledSelfLoop?.(from, input) ?? false);
      const unhandled = isSelfLoop && !handledSelfLoop;

      transitions.push({ from, fromKey, input, inputKey, to, toKey, unhandled });

      if (!states.has(toKey)) {
        if (states.size < maxStates) {
          states.set(toKey, to);
          queue.push(toKey);
        } else {
          frontier.add(toKey);
        }
      }
    }
  }

  return {
    init,
    initKey,
    states,
    transitions,
    budget: {
      maxStates,
      exploredStates: states.size,
      exhausted: frontier.size > 0,
      unexploredFrontier: frontier.size,
    },
    duplicateInputKeys,
  };
}

/**
 * Generator A: for every non-excluded reachable (state, input) transition, the shortest path from init
 * to its source state plus that transition. Linear in reachable transitions.
 */
export function transitionCoveragePaths<S, I>(
  graph: ModelGraph<S, I>,
  excludedKeys?: ReadonlySet<string>,
): readonly MachinePath<S, I>[] {
  const outgoing = new Map<string, ModelTransition<S, I>[]>();
  for (const t of graph.transitions) {
    if (t.toKey === t.fromKey || !graph.states.has(t.toKey)) continue;
    const list = outgoing.get(t.fromKey);
    if (list !== undefined) list.push(t);
    else outgoing.set(t.fromKey, [t]);
  }

  const shortestTo = new Map<string, readonly PathStep<S, I>[]>([[graph.initKey, []]]);

  const runBfs = (allowExcluded: boolean): void => {
    const bfsQueue = [...shortestTo.keys()];
    for (let head = 0; head < bfsQueue.length; head++) {
      const stateKey = bfsQueue[head];
      if (stateKey === undefined) continue;
      const prefix = shortestTo.get(stateKey);
      if (prefix === undefined) continue;
      for (const edge of outgoing.get(stateKey) ?? []) {
        if (!allowExcluded && excludedKeys?.has(transitionEdgeKey(edge.fromKey, edge.inputKey))) continue;
        if (shortestTo.has(edge.toKey)) continue;
        shortestTo.set(edge.toKey, [...prefix, toPathStep(edge)]);
        bfsQueue.push(edge.toKey);
      }
    }
  };

  // Prefer prefixes through non-excluded transitions so excluding one shortcut does not mask other states.
  runBfs(false);
  if (shortestTo.size < graph.states.size) runBfs(true);

  const paths: MachinePath<S, I>[] = [];
  for (const t of graph.transitions) {
    if (excludedKeys?.has(transitionEdgeKey(t.fromKey, t.inputKey))) continue;
    const prefix = shortestTo.get(t.fromKey);
    if (prefix === undefined) continue;
    paths.push({
      initKey: graph.initKey,
      steps: [...prefix, toPathStep(t)],
      targetKey: t.toKey,
    });
  }

  if (paths.length === 0) {
    paths.push({ initKey: graph.initKey, steps: [], targetKey: graph.initKey });
  }
  return paths;
}

/**
 * Generator B: all simple paths (no repeated state key) starting from init up to maxDepth transitions,
 * bounded by maxPaths.
 */
export function simplePaths<S, I>(
  graph: ModelGraph<S, I>,
  options: { readonly maxDepth: number; readonly maxPaths?: number },
): { readonly paths: readonly MachinePath<S, I>[]; readonly exhausted: boolean; readonly maxPaths: number } {
  const { maxDepth } = options;
  const maxPaths = options.maxPaths ?? DEFAULT_MAX_PATHS;
  if (!Number.isInteger(maxDepth) || maxDepth < 0) {
    throw new RangeError(`maxDepth must be a non-negative integer, got ${String(maxDepth)}`);
  }
  if (!Number.isInteger(maxPaths) || maxPaths < 1) {
    throw new RangeError(`maxPaths must be a positive integer, got ${String(maxPaths)}`);
  }

  const outgoing = new Map<string, ModelTransition<S, I>[]>();
  for (const t of graph.transitions) {
    if (t.toKey === t.fromKey || !graph.states.has(t.toKey)) continue;
    const list = outgoing.get(t.fromKey);
    if (list !== undefined) list.push(t);
    else outgoing.set(t.fromKey, [t]);
  }

  const paths: MachinePath<S, I>[] = [];
  let exhausted = false;

  const visit = (stateKey: string, steps: readonly PathStep<S, I>[], visited: ReadonlySet<string>): void => {
    if (steps.length >= maxDepth) return;
    for (const edge of outgoing.get(stateKey) ?? []) {
      if (visited.has(edge.toKey)) continue;
      if (paths.length >= maxPaths) {
        exhausted = true;
        return;
      }
      const nextSteps = [...steps, toPathStep(edge)];
      paths.push({ initKey: graph.initKey, steps: nextSteps, targetKey: edge.toKey });
      const nextVisited = new Set(visited);
      nextVisited.add(edge.toKey);
      visit(edge.toKey, nextSteps, nextVisited);
      if (exhausted) return;
    }
  };

  visit(graph.initKey, [], new Set([graph.initKey]));

  if (paths.length === 0) {
    paths.push({ initKey: graph.initKey, steps: [], targetKey: graph.initKey });
  }
  return { paths, exhausted, maxPaths };
}

/**
 * Walk a machine against a synchronous binding, verifying projection equality after every step,
 * unhandled self-loop invariance, exclusion validity, explicit state/path budgets, and (for
 * transition-coverage) 100% coverage over non-excluded transitions.
 */
export function walkMachine<S, I, Impl>(
  machine: Machine<S, I>,
  binding: MachineBinding<S, I, Impl>,
  options: WalkOptions,
): MbtResult {
  const graph = exploreMachine(machine, options.maxStates);
  const failures: MbtFailure[] = [];

  for (const inputKey of graph.duplicateInputKeys) {
    failures.push({ kind: 'duplicate-input-key', inputKey });
  }
  if (graph.budget.exhausted) {
    failures.push({
      kind: 'state-budget-exhausted',
      maxStates: graph.budget.maxStates,
      exploredStates: graph.budget.exploredStates,
      unexploredFrontier: graph.budget.unexploredFrontier,
    });
  }

  const transitionMap = new Map<string, ModelTransition<S, I>>();
  for (const t of graph.transitions) {
    transitionMap.set(transitionEdgeKey(t.fromKey, t.inputKey), t);
  }

  const excludedKeys = new Set<string>();
  const validExcluded: ExcludedTransition[] = [];
  for (const ex of binding.excluded ?? []) {
    if (ex.why.trim().length === 0) {
      failures.push({ kind: 'invalid-exclusion', state: ex.state, input: ex.input, why: ex.why, reason: 'empty-why' });
      continue;
    }
    const edgeKey = transitionEdgeKey(ex.state, ex.input);
    if (!transitionMap.has(edgeKey)) {
      failures.push({
        kind: 'invalid-exclusion',
        state: ex.state,
        input: ex.input,
        why: ex.why,
        reason: 'unknown-transition',
      });
      continue;
    }
    if (excludedKeys.has(edgeKey)) {
      failures.push({
        kind: 'invalid-exclusion',
        state: ex.state,
        input: ex.input,
        why: ex.why,
        reason: 'duplicate-exclusion',
      });
      continue;
    }
    excludedKeys.add(edgeKey);
    validExcluded.push(ex);
  }

  let pathsToDrive: readonly MachinePath<S, I>[];
  let maxPathsBudget: number | undefined;
  let pathsExhausted: boolean | undefined;

  if (options.generator === 'transition-coverage') {
    pathsToDrive = transitionCoveragePaths(graph, excludedKeys);
  } else {
    const sp = simplePaths(graph, {
      maxDepth: options.maxDepth,
      ...(options.maxPaths !== undefined ? { maxPaths: options.maxPaths } : {}),
    });
    pathsToDrive = sp.paths;
    maxPathsBudget = sp.maxPaths;
    pathsExhausted = sp.exhausted;
    if (sp.exhausted) {
      failures.push({
        kind: 'path-budget-exhausted',
        maxPaths: sp.maxPaths,
        generatedPaths: sp.paths.length,
      });
    }
  }

  const coveredKeys = new Set<string>();
  const unhandledCheckedKeys = new Set<string>();
  const unhandledFailedKeys = new Set<string>();
  const cannotDrivePaths = new Map<string, readonly TransitionKey[]>();
  const drivenPaths: DrivenPath[] = [];

  for (const p of pathsToDrive) {
    const stepKeys: TransitionKey[] = p.steps.map((s) => ({ state: s.fromKey, input: s.inputKey }));
    let impl = binding.create();
    const initActual = machine.showState(binding.project(impl));
    if (initActual !== p.initKey) {
      failures.push({ kind: 'init-mismatch', expectedKey: p.initKey, actualKey: initActual });
      drivenPaths.push({ steps: stepKeys, targetKey: p.targetKey, ok: false });
      continue;
    }

    let pathOk = true;
    for (let stepIndex = 0; stepIndex < p.steps.length; stepIndex++) {
      const step = p.steps[stepIndex];
      if (step === undefined) continue;
      const edgeKey = transitionEdgeKey(step.fromKey, step.inputKey);
      const isExcluded = excludedKeys.has(edgeKey);
      const nextImpl = binding.apply(impl, step.input);

      if (nextImpl === 'cannot-drive') {
        if (!isExcluded) {
          pathOk = false;
          if (!cannotDrivePaths.has(edgeKey)) {
            cannotDrivePaths.set(edgeKey, stepKeys.slice(0, stepIndex + 1));
          }
        }
        break;
      }

      const actualKey = machine.showState(binding.project(nextImpl));
      if (step.unhandled) {
        if (actualKey !== step.fromKey) {
          pathOk = false;
          unhandledFailedKeys.add(edgeKey);
          failures.push({
            kind: 'unhandled-changed-state',
            path: stepKeys.slice(0, stepIndex + 1),
            stepIndex,
            state: step.fromKey,
            input: step.inputKey,
            actualKey,
          });
          break;
        }
        unhandledCheckedKeys.add(edgeKey);
        if (!isExcluded) coveredKeys.add(edgeKey);
      } else {
        if (actualKey !== step.expectedKey) {
          pathOk = false;
          failures.push({
            kind: 'state-mismatch',
            path: stepKeys.slice(0, stepIndex + 1),
            stepIndex,
            state: step.fromKey,
            input: step.inputKey,
            expectedKey: step.expectedKey,
            actualKey,
          });
          break;
        }
        if (!isExcluded) coveredKeys.add(edgeKey);
      }

      impl = nextImpl;
    }

    drivenPaths.push({ steps: stepKeys, targetKey: p.targetKey, ok: pathOk });
  }

  const total = graph.transitions.length;
  const uncovered: TransitionKey[] = [];
  for (const t of graph.transitions) {
    const edgeKey = transitionEdgeKey(t.fromKey, t.inputKey);
    if (excludedKeys.has(edgeKey)) continue;
    if (!coveredKeys.has(edgeKey)) {
      uncovered.push({ state: t.fromKey, input: t.inputKey });
      if (options.generator === 'transition-coverage') {
        const cdPath = cannotDrivePaths.get(edgeKey);
        failures.push({
          kind: 'uncovered-transition',
          state: t.fromKey,
          input: t.inputKey,
          reason: cdPath !== undefined ? 'cannot-drive' : 'not-reached',
          ...(cdPath !== undefined ? { path: cdPath } : {}),
        });
      }
    }
  }

  const required = total - excludedKeys.size;
  const covered = coveredKeys.size;
  if (options.generator === 'transition-coverage' && covered < required) {
    failures.push({
      kind: 'coverage-incomplete',
      covered,
      required,
      total,
      excluded: excludedKeys.size,
    });
  }

  let unhandledTotal = 0;
  for (const t of graph.transitions) {
    if (t.unhandled) unhandledTotal++;
  }

  return {
    ok: failures.length === 0,
    generator: options.generator,
    paths: drivenPaths,
    transitions: {
      covered,
      total,
      uncovered,
    },
    excluded: validExcluded,
    unhandled: {
      checked: unhandledCheckedKeys.size,
      total: unhandledTotal,
      failed: unhandledFailedKeys.size,
    },
    budget: {
      ...graph.budget,
      ...(maxPathsBudget !== undefined ? { maxPaths: maxPathsBudget } : {}),
      ...(pathsExhausted !== undefined ? { pathsExhausted } : {}),
    },
    failures,
  };
}
