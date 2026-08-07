/** Shared `runs-on:` emission for generated GitHub workflow YAML. */

export type WorkflowRunsOn = string | string[] | undefined;

export function isNixosRunner(runsOn: WorkflowRunsOn): boolean {
  const labels = runsOn === undefined ? [] : typeof runsOn === 'string' ? [runsOn] : runsOn;
  return labels.some((label) => label === 'nixos' || label.startsWith('nixos-'));
}

/**
 * Trusted jobs use configured nixos self-hosted labels; fork PRs stay on
 * ubuntu-latest (no access to the private runner fleet).
 */
export function githubUsesNixosRunnerExpr(): string {
  return "(github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository)";
}

/**
 * Full `    runs-on: …` line(s), indented for a job under `jobs.<id>:`.
 * Nixos labels emit the multiline prettier-stable fork-gate expression.
 */
export function renderRunsOnLine(runsOn: WorkflowRunsOn): string {
  if (!isNixosRunner(runsOn)) {
    let value: string;
    if (runsOn === undefined) {
      value = 'ubuntu-latest';
    } else if (typeof runsOn === 'string') {
      value = runsOn.length > 0 ? runsOn : 'ubuntu-latest';
    } else if (runsOn.length === 0) {
      value = 'ubuntu-latest';
    } else if (runsOn.length === 1) {
      value = runsOn[0] ?? 'ubuntu-latest';
    } else {
      value = `[${runsOn.map((label) => `'${label}'`).join(', ')}]`;
    }
    return `    runs-on: ${value}`;
  }

  const labels = typeof runsOn === 'string' ? [runsOn] : runsOn;
  // Multiline matches prettier so checked-in workflows stay byte-identical.
  const nixosJson = JSON.stringify(labels);
  return `    runs-on:
      \${{ ${githubUsesNixosRunnerExpr()} &&
      fromJSON('${nixosJson}') || 'ubuntu-latest' }}`;
}
