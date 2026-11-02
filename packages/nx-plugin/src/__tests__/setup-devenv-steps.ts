import { readFileSync } from 'node:fs';

/** The managed setup-devenv action, as the YAML parser reads it. */
function setupDevenvAction(): unknown {
  return Bun.YAML.parse(
    readFileSync(new URL('../../managed/templates/github/actions/setup-devenv/action.yml', import.meta.url), 'utf8'),
  );
}

/** The managed setup-devenv action's steps in the order a runner takes them, each as its fields. */
export function setupDevenvSteps(): Map<string, unknown>[] {
  const action = setupDevenvAction();
  if (typeof action !== 'object' || action === null || !('runs' in action)) return [];
  const runs = action.runs;
  if (typeof runs !== 'object' || runs === null || !('steps' in runs) || !Array.isArray(runs.steps)) return [];
  return runs.steps.map(
    (step: unknown) => new Map(typeof step === 'object' && step !== null ? Object.entries(step) : []),
  );
}

/** The names of the inputs the managed setup-devenv action declares. */
export function setupDevenvInputNames(): string[] {
  const action = setupDevenvAction();
  if (typeof action !== 'object' || action === null || !('inputs' in action)) return [];
  const inputs = action.inputs;
  return typeof inputs === 'object' && inputs !== null ? Object.keys(inputs) : [];
}
