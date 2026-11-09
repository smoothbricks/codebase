import { readFileSync } from 'node:fs';

/** A managed action, as the YAML parser reads it. */
function managedAction(name: string): unknown {
  return Bun.YAML.parse(
    readFileSync(new URL(`../../managed/templates/github/actions/${name}/action.yml`, import.meta.url), 'utf8'),
  );
}

/** A managed action's steps in the order a runner takes them, each as its fields. */
function actionSteps(name: string): Map<string, unknown>[] {
  const action = managedAction(name);
  if (typeof action !== 'object' || action === null || !('runs' in action)) return [];
  const runs = action.runs;
  if (typeof runs !== 'object' || runs === null || !('steps' in runs) || !Array.isArray(runs.steps)) return [];
  return runs.steps.map(
    (step: unknown) => new Map(typeof step === 'object' && step !== null ? Object.entries(step) : []),
  );
}

/** The managed setup-devenv action's steps in the order a runner takes them, each as its fields. */
export function setupDevenvSteps(): Map<string, unknown>[] {
  return actionSteps('setup-devenv');
}

/** The managed save-nix-devenv action's steps, the job-end half of setup-devenv. */
export function saveNixDevenvSteps(): Map<string, unknown>[] {
  return actionSteps('save-nix-devenv');
}

/** The names of the inputs the managed setup-devenv action declares. */
export function setupDevenvInputNames(): string[] {
  const action = managedAction('setup-devenv');
  if (typeof action !== 'object' || action === null || !('inputs' in action)) return [];
  const inputs = action.inputs;
  return typeof inputs === 'object' && inputs !== null ? Object.keys(inputs) : [];
}
