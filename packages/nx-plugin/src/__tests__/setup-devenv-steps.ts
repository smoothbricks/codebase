import { readFileSync } from 'node:fs';

/** The managed setup-devenv action's steps in the order a runner takes them, each as its fields. */
export function setupDevenvSteps(): Map<string, unknown>[] {
  const action: unknown = Bun.YAML.parse(
    readFileSync(new URL('../../managed/templates/github/actions/setup-devenv/action.yml', import.meta.url), 'utf8'),
  );
  if (typeof action !== 'object' || action === null || !('runs' in action)) return [];
  const runs = action.runs;
  if (typeof runs !== 'object' || runs === null || !('steps' in runs) || !Array.isArray(runs.steps)) return [];
  return runs.steps.map(
    (step: unknown) => new Map(typeof step === 'object' && step !== null ? Object.entries(step) : []),
  );
}
