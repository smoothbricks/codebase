/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions cover literal GitHub Actions expressions. */

import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  type CiWorkflowDefinitionOptions,
  CiWorkflowStepKind,
  cargoCredentialStepLines,
  defineCiWorkflow,
  renderCiWorkflowYaml,
} from '../ci-workflow.js';

const nixosRunsOn = ['nixos-latest-x64', 'self-hosted'] as const;

function options(overrides: Partial<CiWorkflowDefinitionOptions> = {}): CiWorkflowDefinitionOptions {
  return {
    deploy: false,
    browserTests: false,
    e2eDeployment: false,
    pushBranches: ['main'],
    ...overrides,
  };
}

describe('CI workflow definition', () => {
  it('renders the checked-in local CI workflow copy', async () => {
    const rendered = renderCiWorkflowYaml(options({ runsOn: [...nixosRunsOn] }));
    const packageRoot = join(import.meta.dir, '..', '..', '..');

    await expect(readFile(join(packageRoot, '..', '..', '.github/workflows/ci.yml'), 'utf8')).resolves.toBe(rendered);
  });

  it('deploys immediately after build and renumbers following steps', () => {
    const definition = options({ deploy: true, browserTests: true });
    const steps = defineCiWorkflow(definition);
    const rendered = renderCiWorkflowYaml(definition);

    expect(steps.map((step) => [step.kind, step.number])).toEqual([
      [CiWorkflowStepKind.Checkout, 2],
      [CiWorkflowStepKind.SetupDevenv, 3],
      [CiWorkflowStepKind.SetNxShas, 4],
      [CiWorkflowStepKind.RestoreNxCache, 5],
      [CiWorkflowStepKind.Build, 6],
      [CiWorkflowStepKind.Deploy, 7],
      [CiWorkflowStepKind.Lint, 8],
      [CiWorkflowStepKind.UnitTests, 9],
      [CiWorkflowStepKind.BrowserTests, 10],
      [CiWorkflowStepKind.ManagedFilesCheck, 11],
      [CiWorkflowStepKind.ManagedFilesDispatch, 12],
      [CiWorkflowStepKind.SaveNxCache, 13],
      [CiWorkflowStepKind.UploadTraceDbs, 14],
      [CiWorkflowStepKind.SaveNixDevenv, 15],
    ]);
    expect(rendered.match(/- name: 🚀 Deploy Stage/g)).toHaveLength(1);
    expect(rendered).toContain('id: deploy');
    expect(rendered).toContain('smoo github-ci nx-deploy --mode run-many --name "Deploy Stage" --step 7');
    expect(rendered).toContain('smoo github-ci nx-smart --target test-browser --name "Browser Tests" --step 10');
    expect(rendered).toContain('group: ${{ github.workflow }}-${{ github.ref }}');
    expect(rendered).toContain('cancel-in-progress: true');
    expect(rendered).toContain('github.event.pull_request.head.repo.full_name == github.repository');
    expect(rendered).toContain("github.ref == 'refs/heads/private'");
    expect(rendered).toContain("# Step 13\n      # Nx's database cache needs artifact files");
  });

  it('adds only generic Cloudflare credentials for Wrangler-backed deploys', () => {
    const rendered = renderCiWorkflowYaml(options({ deploy: true, deployProvider: 'cloudflare' }));

    expect(rendered).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
    expect(rendered).toContain('CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}');
    expect(rendered.match(/^\s+[A-Z][A-Z0-9_]+: \${{ secrets\.[A-Z][A-Z0-9_]+ }}$/gm)).toEqual([
      '          CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}',
      '          CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}',
    ]);
  });

  it('clones declared sibling sources before setup and renumbers following steps', () => {
    const definition = options({
      sourceCheckouts: [
        {
          path: '../smoothbricks',
          repository: 'https://git.example.net/codebase/smoothbricks.git',
          ref: 'abc123',
          tokenEnv: 'SOURCE_READ_TOKEN',
        },
        {
          path: '../_fork/minigraf',
          repository: 'https://git.example.net/codebase/minigraf.git',
          ref: 'def456',
          tokenEnv: 'SOURCE_READ_TOKEN',
        },
      ],
    });
    const steps = defineCiWorkflow(definition);

    expect(steps.slice(0, 4).map((step) => [step.kind, step.number])).toEqual([
      [CiWorkflowStepKind.Checkout, 2],
      [CiWorkflowStepKind.SourceCheckouts, 3],
      [CiWorkflowStepKind.SetupDevenv, 4],
      [CiWorkflowStepKind.SetNxShas, 5],
    ]);
    const rendered = renderCiWorkflowYaml(definition);
    expect(rendered).toContain('- name: 📦 Check out sibling sources');
    expect(rendered).toContain('github.event.pull_request.head.repo.full_name == github.repository');
    expect(rendered.match(/SOURCE_READ_TOKEN: \$\{\{ secrets\.SOURCE_READ_TOKEN \}\}/g)).toHaveLength(1);
    expect(rendered).toContain(
      'git clone --filter=blob:none https://git.example.net/codebase/smoothbricks.git "$root/../smoothbricks"',
    );
    expect(rendered).toContain('git -C "$root/../smoothbricks" checkout --detach abc123');
    expect(rendered).toContain(
      'git clone --filter=blob:none https://git.example.net/codebase/minigraf.git "$root/../_fork/minigraf"',
    );
    expect(rendered).toContain('git -C "$root/../_fork/minigraf" checkout --detach def456');
    // Credential hygiene: no token in any URL or argv; authorization is
    // per-command env config whose key is scoped to the exact origin, so
    // nothing leaks to other hosts or into the sibling's .git/config.
    expect(rendered).not.toContain('x-access-token:${SOURCE_READ_TOKEN}@');
    expect(rendered).not.toMatch(/https:\/\/[^ ]*SOURCE_READ_TOKEN/);
    expect(rendered).toContain("key='http.https://git.example.net/.extraheader'");
    expect(rendered).toContain(
      'val="AUTHORIZATION: basic $(printf \'x-access-token:%s\' "$SOURCE_READ_TOKEN" | base64 | tr -d \'\\n\')"',
    );
    const configPrefixes = rendered.match(/GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="\$key" GIT_CONFIG_VALUE_0="\$val" \\/g);
    expect(configPrefixes).toHaveLength(4);
    expect(rendered).toContain(
      'GIT_CONFIG_VALUE_0="$val" \\\n          git -C "$root/../smoothbricks" checkout --detach abc123',
    );
    expect(rendered).toContain('# Step 4. Composite action internals');
  });

  it('clones public siblings unauthenticated and omits the env block without tokens', () => {
    const rendered = renderCiWorkflowYaml(
      options({
        sourceCheckouts: [{ path: '../public', repository: 'https://git.example.net/codebase/public.git' }],
      }),
    );

    expect(rendered).toContain(
      'git clone --filter=blob:none https://git.example.net/codebase/public.git "$root/../public"',
    );
    expect(rendered).not.toMatch(/^\s+[A-Z][A-Z0-9_]+: \$\{\{ secrets\./m);
    expect(rendered).not.toContain('GIT_CONFIG');
  });

  it('refuses malformed source checkout declarations at render time', () => {
    expect(() =>
      renderCiWorkflowYaml(
        options({ sourceCheckouts: [{ path: '/absolute', repository: 'https://git.example.net/x.git' }] }),
      ),
    ).toThrow('path relative to the workspace root');
    expect(() =>
      renderCiWorkflowYaml(options({ sourceCheckouts: [{ path: '../x', repository: 'git@example.net:x.git' }] })),
    ).toThrow('https repository URL');
    expect(() =>
      renderCiWorkflowYaml(
        options({ sourceCheckouts: [{ path: '../x', repository: 'https://token@git.example.net/x.git' }] }),
      ),
    ).toThrow('credential-free');
    expect(() =>
      renderCiWorkflowYaml(
        options({
          sourceCheckouts: [{ path: '../x', repository: 'https://git.example.net/x.git', tokenEnv: 'bad-name' }],
        }),
      ),
    ).toThrow('secret env name');
  });

  it('restricts Cargo credential answers to HTTPS origins and get operations without persisting secrets', () => {
    const directory = mkdtempSync(join(tmpdir(), 'cargo-credentials-'));
    try {
      const githubEnv = join(directory, 'env');
      writeFileSync(githubEnv, '');
      const lines = cargoCredentialStepLines(
        { kind: CiWorkflowStepKind.CargoCredentials, name: 'Credentials', number: 3 },
        { gitOrigins: [{ origin: 'https://[::1]:8443', tokenEnv: 'SOURCE_READ_TOKEN' }] },
      );
      const script = lines
        .slice(lines.indexOf('        run: |') + 1)
        .map((line) => line.slice(10))
        .join('\n');
      const environment = {
        ...process.env,
        RUNNER_TEMP: directory,
        GITHUB_ENV: githubEnv,
        SOURCE_READ_TOKEN: 'fixture-secret',
      };
      const prepared = spawnSync('sh', ['-eu', '-c', script], { env: environment, encoding: 'utf8' });
      expect(prepared.status).toBe(0);
      expect(prepared.stdout + prepared.stderr + readFileSync(githubEnv, 'utf8')).not.toContain('fixture-secret');
      const helper = join(directory, 'cargo-git-credential.sh');
      expect(readFileSync(helper, 'utf8')).not.toContain('fixture-secret');
      for (const { operation, protocol, host, expected } of [
        {
          operation: 'get',
          protocol: 'https',
          host: '[::1]:8443',
          expected: 'username=x-access-token\npassword=fixture-secret\n',
        },
        { operation: 'get', protocol: 'http', host: '[::1]:8443', expected: '' },
        { operation: 'get', protocol: 'https', host: '[::1]:8444', expected: '' },
        { operation: 'get', protocol: 'https', host: 'other.example.net', expected: '' },
        { operation: 'store', protocol: 'https', host: '[::1]:8443', expected: '' },
        { operation: 'erase', protocol: 'https', host: '[::1]:8443', expected: '' },
      ]) {
        const result = spawnSync('sh', [helper, operation], {
          env: environment,
          encoding: 'utf8',
          input: `protocol=${protocol}\nhost=${host}\npath=owner/repository.git\n\n`,
        });
        expect(result.status).toBe(0);
        expect(result.stdout).toBe(expected);
        expect(result.stderr).toBe('');
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('refuses missing registry secrets before setup and skips private Cargo jobs for fork PRs', () => {
    const definition = options({ cargoCredentials: { registryTokenEnvs: ['CARGO_REGISTRIES_EXAMPLE_TOKEN'] } });
    const steps = defineCiWorkflow(definition);
    expect(steps.findIndex((step) => step.kind === CiWorkflowStepKind.CargoCredentials)).toBeLessThan(
      steps.findIndex((step) => step.kind === CiWorkflowStepKind.SetupDevenv),
    );
    expect(Bun.YAML.parse(renderCiWorkflowYaml(definition))).toMatchObject({
      jobs: {
        main: {
          if: "${{ github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository }}",
        },
      },
    });
    const lines = cargoCredentialStepLines(
      { kind: CiWorkflowStepKind.CargoCredentials, name: 'Credentials', number: 3 },
      definition.cargoCredentials ?? {},
    );
    const script = lines
      .slice(lines.indexOf('        run: |') + 1)
      .map((line) => line.slice(10))
      .join('\n');
    const missing = spawnSync('sh', ['-eu', '-c', script], {
      env: { CARGO_REGISTRIES_EXAMPLE_TOKEN: '' },
      encoding: 'utf8',
    });
    expect(missing.status).not.toBe(0);
    expect(missing.stderr).toContain('CARGO_REGISTRIES_EXAMPLE_TOKEN');
    const present = spawnSync('sh', ['-eu', '-c', script], {
      env: { CARGO_REGISTRIES_EXAMPLE_TOKEN: 'fixture-secret' },
      encoding: 'utf8',
    });
    expect(present.status).toBe(0);
    expect(present.stdout + present.stderr).toBe('');
  });

  it('does not mention cargo credentials when the root did not opt in', () => {
    const rendered = renderCiWorkflowYaml(options());
    expect(rendered).not.toContain('Prepare Cargo credentials');
    expect(rendered).not.toContain('CARGO_NET_GIT_FETCH_WITH_CLI');
    expect(rendered).not.toContain('credential.helper');
  });

  it('refuses malformed cargo credential declarations at render time', () => {
    expect(() =>
      renderCiWorkflowYaml(
        options({
          cargoCredentials: {
            gitOrigins: [
              { origin: 'https://git.example.net', tokenEnv: 'FIRST_TOKEN' },
              { origin: 'https://git.example.net:443', tokenEnv: 'SECOND_TOKEN' },
            ],
          },
        }),
      ),
    ).toThrow('one token per origin');
    expect(() => renderCiWorkflowYaml(options({ cargoCredentials: {} }))).toThrow(
      'at least one gitOrigins or registryTokenEnvs',
    );
    expect(() => renderCiWorkflowYaml(options({ cargoCredentials: { registryTokenEnvs: ['bad-name'] } }))).toThrow(
      'upper-case secret env names',
    );
    expect(() =>
      renderCiWorkflowYaml(
        options({ cargoCredentials: { gitOrigins: [{ origin: 'http://git.example.net', tokenEnv: 'T' }] } }),
      ),
    ).toThrow('credential-free https origin');
    expect(() =>
      renderCiWorkflowYaml(
        options({ cargoCredentials: { gitOrigins: [{ origin: 'https://token@git.example.net', tokenEnv: 'T' }] } }),
      ),
    ).toThrow('credential-free https origin');
    expect(() =>
      renderCiWorkflowYaml(
        options({ cargoCredentials: { gitOrigins: [{ origin: 'https://git.example.net/repo', tokenEnv: 'T' }] } }),
      ),
    ).toThrow('without a path');
    expect(() =>
      renderCiWorkflowYaml(
        options({ cargoCredentials: { gitOrigins: [{ origin: 'https://git.example.net', tokenEnv: 'bad' }] } }),
      ),
    ).toThrow('upper-case secret env name');
  });

  it('renders deployment E2E as a dependent job with an independent stage input', () => {
    const rendered = renderCiWorkflowYaml(options({ deploy: true, e2eDeployment: true, runsOn: [...nixosRunsOn] }));

    expect(rendered).toContain('deployment-stage: ${{ steps.deploy.outputs.stage }}');
    expect(rendered).toContain('  e2e-deployment:\n    name: E2E Tests (Deployed Stage)\n    needs: main');
    expect(rendered).not.toContain('\n\n\n  e2e-deployment:');
    expect(rendered).toContain(
      "if: ${{ needs.main.result == 'success' && needs.main.outputs.deployment-stage != '' }}",
    );
    expect(rendered).toContain('timeout-minutes: 15');
    expect(rendered).toContain(
      "if: ${{ needs.main.result == 'success' && needs.main.outputs.deployment-stage != '' }}\n    env:\n      GH_TOKEN: ${{ github.token }}\n    steps:",
    );
    expect(rendered).toContain('# prettier-ignore\n        run: smoo github-ci nx-smart --target e2e-deployment');
    expect(rendered).toContain(
      'smoo github-ci nx-smart --target e2e-deployment --mode run-many --stage "${{ needs.main.outputs.deployment-stage }}" --stream-output --name "E2E Tests (Deployed Stage)" --step 4',
    );
    expect(rendered.match(/name: E2E Tests \(Deployed Stage\)/g)).toHaveLength(2);
  });

  it('omits optional browser and deployment-E2E lanes when disabled', () => {
    const rendered = renderCiWorkflowYaml(options({ deploy: true }));

    expect(rendered).not.toContain('--target test-browser');
    expect(rendered).not.toContain('  e2e-deployment:');
    expect(rendered).not.toContain('deployment-stage:');
  });

  it('uses the same architecture-scoped key to restore and save the Nx cache', async () => {
    const rendered = renderCiWorkflowYaml(options());
    const packageRoot = join(import.meta.dir, '..', '..', '..');
    const restoreAction = await readFile(join(packageRoot, '..', '..', '.github/actions/cache-nx/action.yml'), 'utf8');
    const restoreKey = restoreAction.match(/^\s*key: (.+)$/m)?.[1];
    const saveKey = rendered.match(/^\s*key: (.+)$/m)?.[1];

    expect(restoreKey).toBe('${{ runner.os }}-${{ runner.arch }}-nx-db-v1-${{ github.sha }}');
    expect(saveKey).toBe(restoreKey);
  });

  it('keeps the Actions cache transport off host runners, which cache on the shared bind', () => {
    const rendered = renderCiWorkflowYaml(options());
    // Both halves of the transport must be gated, or a host runner either
    // overwrites its live cache with an older archive or uploads a copy of it.
    for (const step of ['🧠 Restore Nx cache', '💾 Save Nx cache']) {
      const body = rendered.slice(rendered.indexOf(`- name: ${step}`));
      expect(body.slice(0, body.indexOf('uses:'))).toContain("steps.setup.outputs.host-runner != 'true'");
    }
  });

  it('nixos config gates both jobs away from private runners for fork PRs', () => {
    const rendered = renderCiWorkflowYaml(options({ deploy: true, e2eDeployment: true, runsOn: [...nixosRunsOn] }));
    const runnerExpression =
      "runs-on:\n      ${{ (github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository) &&\n      fromJSON('[\"nixos-latest-x64\",\"self-hosted\"]') || 'ubuntu-latest' }}";

    expect(rendered.match(new RegExp(runnerExpression.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'))).toHaveLength(2);
    expect(rendered).toContain('uses: ./.github/actions/setup-devenv');
    expect(rendered).not.toContain('github-actions-bootstrap.sh');
  });

  it('gives trusted jobs the private registry read token before setup and skips fork PRs', () => {
    const rendered = renderCiWorkflowYaml(
      options({
        privateNpm: {
          scope: '@priv.test',
          readTokenEnv: 'PRIV_NPM_READ_TOKEN',
          publishTokenEnv: 'PRIV_NPM_PUBLISH_TOKEN',
        },
      }),
    );

    expect(rendered).toContain(
      "if: ${{ github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository }}",
    );
    expect(rendered).toContain('PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}');
    // Registry URL lives in .npmrc, not a job-level GitHub variable.
    expect(rendered).not.toContain('PRIV_NPM_REGISTRY');
    expect(rendered).not.toContain('vars.PRIV_NPM_REGISTRY');
    // Publisher credential is a publish-job secret; CI install must not see it.
    expect(rendered).not.toContain('PRIV_NPM_PUBLISH_TOKEN');
    expect(rendered.indexOf('PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}')).toBeLessThan(
      rendered.indexOf('uses: ./.github/actions/setup-devenv'),
    );
  });

  it('does not inject a read token or skip fork PRs when only a publish token is declared', () => {
    const rendered = renderCiWorkflowYaml(
      options({
        privateNpm: {
          scope: '@priv.test',
          publishTokenEnv: 'PRIV_NPM_PUBLISH_TOKEN',
        },
      }),
    );

    expect(rendered).not.toContain('PRIV_NPM_READ_TOKEN');
    expect(rendered).not.toContain('PRIV_NPM_PUBLISH_TOKEN');
    expect(rendered).not.toContain('github.event.pull_request.head.repo.full_name');
  });

  it('does not mention private registry credentials when the root did not opt in', () => {
    const rendered = renderCiWorkflowYaml(options());

    expect(rendered).not.toContain('PRIV_NPM_REGISTRY');
    expect(rendered).not.toContain('PRIV_NPM_READ_TOKEN');
    expect(rendered).not.toContain('PRIV_NPM_PUBLISH_TOKEN');
  });
});
