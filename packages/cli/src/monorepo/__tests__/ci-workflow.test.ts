/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions cover literal GitHub Actions expressions. */

import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { format } from 'prettier';
import type { PackageCargoGitOrigin } from '../../lib/json.js';
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
    expect(rendered).toContain("cancel-in-progress: ${{ github.ref != 'refs/heads/main' }}");
    expect(rendered).toContain('github.event.pull_request.head.repo.full_name == github.repository');
    expect(rendered).toContain("github.ref == 'refs/heads/main'");
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

  it('mirrors the runner proxy into git config so Cargo git fetches take the same route', () => {
    const directory = mkdtempSync(join(tmpdir(), 'cargo-gitcfg-'));
    try {
      const scriptOf = (): string => {
        const lines = cargoCredentialStepLines(
          { kind: CiWorkflowStepKind.CargoCredentials, name: 'Credentials', number: 3 },
          { gitOrigins: [{ origin: 'https://git.example.net', tokenEnv: 'SOURCE_READ_TOKEN' }] },
        );
        return lines
          .slice(lines.indexOf('        run: |') + 1)
          .map((line) => line.slice(10))
          .join('\n');
      };
      const script = scriptOf();
      const baseEnv = {
        PATH: process.env.PATH ?? '/usr/bin:/bin',
        RUNNER_TEMP: directory,
        SOURCE_READ_TOKEN: 'fixture-secret',
      };
      const runScript = (env: Record<string, string>, tag: string): { status: number | null; githubEnv: string } => {
        const githubEnv = join(directory, `env-${tag}`);
        writeFileSync(githubEnv, '');
        const result = spawnSync('sh', ['-eu', '-c', script], {
          env: { ...baseEnv, GITHUB_ENV: githubEnv, ...env },
          encoding: 'utf8',
        });
        return { status: result.status, githubEnv: readFileSync(githubEnv, 'utf8') };
      };
      // Proxied runner: both schemes land in git config after the helper entries.
      const proxied = runScript({ HTTPS_PROXY: 'http://proxy.example.net:8080' }, 'proxied');
      expect(proxied.status).toBe(0);
      expect(proxied.githubEnv).toContain('GIT_CONFIG_KEY_2=https.proxy');
      expect(proxied.githubEnv).toContain('GIT_CONFIG_VALUE_2=http://proxy.example.net:8080');
      expect(proxied.githubEnv).toContain('GIT_CONFIG_KEY_3=http.proxy');
      expect(proxied.githubEnv).toContain('GIT_CONFIG_COUNT=4');
      // Direct network: no proxy entries, count covers only the helper.
      const direct = runScript({}, 'direct');
      expect(direct.status).toBe(0);
      expect(direct.githubEnv).toContain('GIT_CONFIG_COUNT=2');
      expect(direct.githubEnv).not.toContain('proxy');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('rewrites a mirrored origin with insteadOf and answers the mirror host', () => {
    const directory = mkdtempSync(join(tmpdir(), 'cargo-mirror-'));
    try {
      const lines = cargoCredentialStepLines(
        { kind: CiWorkflowStepKind.CargoCredentials, name: 'Credentials', number: 3 },
        {
          gitOrigins: [
            {
              origin: 'https://git.example.net',
              tokenEnv: 'SOURCE_READ_TOKEN',
              internalMirror: 'http://10.89.0.1:3000',
            },
          ],
        },
      );
      const script = lines
        .slice(lines.indexOf('        run: |') + 1)
        .map((line) => line.slice(10))
        .join('\n');
      const githubEnv = join(directory, 'env');
      writeFileSync(githubEnv, '');
      const environment = {
        PATH: process.env.PATH ?? '/usr/bin:/bin',
        RUNNER_TEMP: directory,
        GITHUB_ENV: githubEnv,
        SOURCE_READ_TOKEN: 'fixture-secret',
      };
      const prepared = spawnSync('sh', ['-eu', '-c', script], { env: environment, encoding: 'utf8' });
      expect(prepared.status).toBe(0);
      const written = readFileSync(githubEnv, 'utf8');
      expect(written).toContain('GIT_CONFIG_KEY_2=url.http://10.89.0.1:3000/.insteadOf');
      expect(written).toContain('GIT_CONFIG_VALUE_2=https://git.example.net/');
      expect(written).toContain('GIT_CONFIG_COUNT=3');
      const helper = join(directory, 'cargo-git-credential.sh');
      const ask = (protocol: string, host: string): string => {
        const result = spawnSync('sh', [helper, 'get'], {
          env: environment,
          encoding: 'utf8',
          input: `protocol=${protocol}\nhost=${host}\n\n`,
        });
        expect(result.status).toBe(0);
        return result.stdout;
      };
      // Declared origin and its rewritten mirror both answer; anything else stays silent.
      expect(ask('https', 'git.example.net')).toBe('username=x-access-token\npassword=fixture-secret\n');
      expect(ask('http', '10.89.0.1:3000')).toBe('username=x-access-token\npassword=fixture-secret\n');
      expect(ask('http', 'git.example.net')).toBe('');
      expect(ask('https', 'other.example.net')).toBe('');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('rewrites every declared SSH spelling of a mirrored forge onto the same mirror', () => {
    const directory = mkdtempSync(join(tmpdir(), 'cargo-ssh-'));
    try {
      const lines = cargoCredentialStepLines(
        { kind: CiWorkflowStepKind.CargoCredentials, name: 'Credentials', number: 3 },
        {
          gitOrigins: [
            {
              origin: 'https://git.example.net',
              tokenEnv: 'SOURCE_READ_TOKEN',
              internalMirror: 'http://10.89.0.1:3000',
              // Both spellings a lockfile can carry: git matches insteadOf
              // prefixes textually and infers neither from the other.
              sshOrigins: ['ssh://forgejo@forge.example.net:2223/', 'ssh://forge.example.net:2223'],
            },
          ],
        },
      );
      const script = lines
        .slice(lines.indexOf('        run: |') + 1)
        .map((line) => line.slice(10))
        .join('\n');
      const githubEnv = join(directory, 'env');
      writeFileSync(githubEnv, '');
      const environment = {
        PATH: process.env.PATH ?? '/usr/bin:/bin',
        RUNNER_TEMP: directory,
        GITHUB_ENV: githubEnv,
        SOURCE_READ_TOKEN: 'fixture-secret',
      };
      const prepared = spawnSync('sh', ['-eu', '-c', script], { env: environment, encoding: 'utf8' });
      expect(prepared.status).toBe(0);
      const written = readFileSync(githubEnv, 'utf8');
      // One insteadOf pair per spelling, all pointing at the one mirror, and a
      // missing trailing slash is normalized: `ssh://host:2223` would rewrite
      // `ssh://host:2223-other/` too.
      expect(written).toContain('GIT_CONFIG_KEY_2=url.http://10.89.0.1:3000/.insteadOf');
      expect(written).toContain('GIT_CONFIG_VALUE_2=https://git.example.net/');
      expect(written).toContain('GIT_CONFIG_KEY_3=url.http://10.89.0.1:3000/.insteadOf');
      expect(written).toContain('GIT_CONFIG_VALUE_3=ssh://forgejo@forge.example.net:2223/');
      expect(written).toContain('GIT_CONFIG_KEY_4=url.http://10.89.0.1:3000/.insteadOf');
      expect(written).toContain('GIT_CONFIG_VALUE_4=ssh://forge.example.net:2223/');
      expect(written).toContain('GIT_CONFIG_COUNT=5');
      const helper = join(directory, 'cargo-git-credential.sh');
      const ask = (protocol: string, host: string): string => {
        const result = spawnSync('sh', [helper, 'get'], {
          env: environment,
          encoding: 'utf8',
          input: `protocol=${protocol}\nhost=${host}\n\n`,
        });
        expect(result.status).toBe(0);
        return result.stdout;
      };
      // The rewrite happens before transport, so git only ever asks for the
      // mirror. The SSH spelling stays credential-free: an SSH origin the
      // runner cannot reach must fail loudly, not collect a token.
      expect(ask('http', '10.89.0.1:3000')).toBe('username=x-access-token\npassword=fixture-secret\n');
      expect(ask('ssh', 'forge.example.net:2223')).toBe('');
      expect(ask('ssh', 'forgejo@forge.example.net:2223')).toBe('');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('refuses SSH spellings no mirror rewrites, and spellings git cannot match as a prefix', () => {
    const origin = { origin: 'https://git.example.net', tokenEnv: 'SOURCE_READ_TOKEN' };
    const render = (entry: PackageCargoGitOrigin): string[] =>
      cargoCredentialStepLines(
        { kind: CiWorkflowStepKind.CargoCredentials, name: 'Credentials', number: 3 },
        { gitOrigins: [entry] },
      );

    // An sshOrigins entry only means anything as a rewrite target.
    expect(() => render({ ...origin, sshOrigins: ['ssh://forge.example.net:2223/'] })).toThrow('internalMirror');
    const mirrored = { ...origin, internalMirror: 'http://10.89.0.1:3000' };
    for (const sshOrigins of [
      [],
      // scp syntax is not a URL prefix git can rewrite from a Cargo pin.
      ['forgejo@forge.example.net:axe/minigraf.git'],
      // A path would rewrite one repository, not the forge.
      ['ssh://forge.example.net:2223/axe/minigraf.git'],
      // A password in a declared origin is a secret in package.json.
      ['ssh://forgejo:hunter2@forge.example.net:2223/'],
      ['https://git.example.net/'],
      // Two identical spellings make the same key ambiguous.
      ['ssh://forge.example.net:2223/', 'ssh://forge.example.net:2223'],
    ]) {
      expect(() => render({ ...mirrored, sshOrigins })).toThrow('sshOrigins');
    }
    // One spelling, two mirrors: git keeps whichever identical key it read
    // last, so the declaration is refused instead of resolved.
    expect(() =>
      cargoCredentialStepLines(
        { kind: CiWorkflowStepKind.CargoCredentials, name: 'Credentials', number: 3 },
        {
          gitOrigins: [
            { ...mirrored, sshOrigins: ['ssh://forge.example.net:2223/'] },
            {
              origin: 'https://git.other.net',
              tokenEnv: 'OTHER_READ_TOKEN',
              internalMirror: 'http://10.89.0.2:3000',
              sshOrigins: ['ssh://forge.example.net:2223/'],
            },
          ],
        },
      ),
    ).toThrow('sshOrigins');
  });

  it('refuses malformed internal mirrors at render time', () => {
    for (const internalMirror of [
      'http://git.example.net/private/repo.git',
      'https://token@git.example.net',
      'ftp://git.example.net',
      'https://git.example.net',
    ]) {
      expect(() =>
        cargoCredentialStepLines(
          { kind: CiWorkflowStepKind.CargoCredentials, name: 'Credentials', number: 3 },
          { gitOrigins: [{ origin: 'https://git.example.net', tokenEnv: 'SOURCE_READ_TOKEN', internalMirror }] },
        ),
      ).toThrow('internalMirror');
    }
  });

  it('gives every Nx job the declared remote cache at the address its runners reach', () => {
    const definition = options({
      deploy: true,
      e2eDeployment: true,
      productionOnPush: true,
      remoteCache: {
        server: 'https://nx-cache.example.net',
        internalServer: 'http://10.89.0.1:8765',
        tokenSecret: 'NX_REMOTE_CACHE_TOKEN',
      },
    });
    const rendered = renderCiWorkflowYaml(definition);
    const cacheEnv = {
      NX_SELF_HOSTED_REMOTE_CACHE_SERVER: 'http://10.89.0.1:8765',
      NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN: '${{ secrets.NX_REMOTE_CACHE_TOKEN }}',
    };

    expect(Bun.YAML.parse(rendered)).toMatchObject({
      jobs: {
        main: {
          env: cacheEnv,
          // A fork PR receives no secrets, and Nx fails every task a cache
          // server refuses, so the job cannot run there at all.
          if: "${{ github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository }}",
        },
        'e2e-deployment': { env: cacheEnv },
        'deploy-production': { env: cacheEnv },
      },
    });
    // The public origin belongs to shells outside the runner network; a job
    // that took it would leave the internal address unused.
    expect(rendered).not.toContain('https://nx-cache.example.net');
  });

  it('takes the public server without an internal runner, and emits nothing without the declaration', () => {
    const publicOnly = renderCiWorkflowYaml(
      options({ remoteCache: { server: 'https://nx-cache.example.net', tokenSecret: 'NX_REMOTE_CACHE_TOKEN' } }),
    );

    expect(Bun.YAML.parse(publicOnly)).toMatchObject({
      jobs: { main: { env: { NX_SELF_HOSTED_REMOTE_CACHE_SERVER: 'https://nx-cache.example.net' } } },
    });
    expect(renderCiWorkflowYaml(options())).not.toContain('NX_SELF_HOSTED');
    // Without a cache token the job needs no secrets, so fork PRs still run.
    expect(renderCiWorkflowYaml(options())).not.toContain('head.repo.full_name');
  });

  it('refuses a remote cache declaration Nx could not use, at render time', () => {
    for (const server of [
      'https://nx-cache.example.net/',
      'https://nx-cache.example.net/cache',
      'https://token@nx-cache.example.net',
      'ftp://nx-cache.example.net',
      'nx-cache.example.net:8765',
    ]) {
      expect(() =>
        renderCiWorkflowYaml(options({ remoteCache: { server, tokenSecret: 'NX_REMOTE_CACHE_TOKEN' } })),
      ).toThrow('smoo.remoteCache server');
    }
    const server = 'https://nx-cache.example.net';
    expect(() =>
      renderCiWorkflowYaml(
        options({ remoteCache: { server, internalServer: 'http://10.89.0.1:8765/', tokenSecret: 'TOKEN' } }),
      ),
    ).toThrow('smoo.remoteCache internalServer');
    expect(() =>
      renderCiWorkflowYaml(options({ remoteCache: { server, internalServer: server, tokenSecret: 'TOKEN' } })),
    ).toThrow('repeats server');
    expect(() =>
      renderCiWorkflowYaml(options({ remoteCache: { server, tokenSecret: 'nx_remote_cache_token' } })),
    ).toThrow('tokenSecret');
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

describe('renderCiWorkflowYaml with deploy configuration', () => {
  const rendered = renderCiWorkflowYaml(
    options({
      deploy: true,
      deployProvider: 'cloudflare',
      e2eDeployment: true,
      pushBranches: ['trunk'],
      environments: { staging: 'staging', production: 'production' },
      deploySecrets: { E2E_CONTROL_TOKEN: 'E2E_CONTROL_TOKEN', GITHUB_CLIENT_SECRET: 'EXAMPLE_GITHUB_CLIENT_SECRET' },
      e2eSecrets: { GIT_CRYPT_KEY_B64: 'GIT_CRYPT_KEY_B64' },
      productionOnPush: true,
    }),
  );

  it('puts the staging environment on the validate and e2e jobs', () => {
    expect(rendered).toContain('  main:\n    name: Validate\n');
    expect((rendered.match(/ {4}environment: staging\n/g) ?? []).length).toBe(2);
  });

  it('exposes extra deploy secrets on the deploy steps under their env names', () => {
    expect(rendered).toContain('          E2E_CONTROL_TOKEN: ${{ secrets.E2E_CONTROL_TOKEN }}');
    expect(rendered).toContain('          GITHUB_CLIENT_SECRET: ${{ secrets.EXAMPLE_GITHUB_CLIENT_SECRET }}');
  });

  it('exposes e2e secrets only on the e2e step', () => {
    const e2eJob = rendered.slice(rendered.indexOf('  e2e-deployment:'), rendered.indexOf('  deploy-production:'));
    expect(e2eJob).toContain('          GIT_CRYPT_KEY_B64: ${{ secrets.GIT_CRYPT_KEY_B64 }}');
    expect(e2eJob).not.toContain('EXAMPLE_GITHUB_CLIENT_SECRET');
    const mainJob = rendered.slice(0, rendered.indexOf('  e2e-deployment:'));
    expect(mainJob).not.toContain('GIT_CRYPT_KEY_B64');
  });

  it('uses the configured push branch for the staging deploy condition', () => {
    expect(rendered).toContain("(github.event_name == 'push' && github.ref == 'refs/heads/trunk')");
  });

  it('adds a production-on-push job gated on validate and the e2e job', () => {
    expect(rendered).toContain(
      '  deploy-production:\n    name: Deploy Production\n    needs: [main, e2e-deployment]\n',
    );
    expect(rendered).toContain('    environment: production\n');
    expect(rendered).toContain(
      "    if: ${{ !cancelled() && github.event_name == 'push' && github.ref == 'refs/heads/trunk' && needs.main.result == 'success' && (needs.e2e-deployment.result == 'success' || needs.e2e-deployment.result == 'skipped') }}",
    );
    expect(rendered).toContain(
      'run: smoo github-ci nx-deploy --stage production --mode run-many --select-tag production-push-deploy-target --name "Deploy Production" --step 4',
    );
  });

  it('gates production on validate alone when there is no e2e job', () => {
    const withoutE2e = renderCiWorkflowYaml(
      options({ deploy: true, deployProvider: 'cloudflare', pushBranches: ['trunk'], productionOnPush: true }),
    );
    const productionJob = withoutE2e.slice(withoutE2e.indexOf('  deploy-production:'));

    expect(productionJob).toContain('    needs: [main]\n');
    expect(productionJob).toContain(
      "    if: ${{ !cancelled() && github.event_name == 'push' && github.ref == 'refs/heads/trunk' && needs.main.result == 'success' }}",
    );
    expect(productionJob).not.toContain('needs.e2e-deployment');
  });

  it('renders a production job the repository Prettier config keeps byte for byte', async () => {
    const withoutE2e = renderCiWorkflowYaml(
      options({ deploy: true, deployProvider: 'cloudflare', pushBranches: ['trunk'], productionOnPush: true }),
    );
    for (const workflow of [rendered, withoutE2e]) {
      const productionJob = workflow.slice(workflow.indexOf('  deploy-production:'));
      expect(productionJob).toContain('    # prettier-ignore\n    if: ${{ !cancelled() ');
      expect(productionJob).toContain(
        '        # prettier-ignore\n        run: smoo github-ci nx-deploy --stage production ',
      );
      // A consuming repo's commit hook formats staged YAML with Prettier; a rewrapped line reads as drift forever.
      await expect(
        format(workflow, { parser: 'yaml', printWidth: 120, proseWrap: 'always', singleQuote: true }),
      ).resolves.toBe(workflow);
    }
  });

  it('keeps a protected staging environment off CI runs that do not deploy', () => {
    const validateOnly = renderCiWorkflowYaml(options({ environments: { staging: 'staging' } }));

    expect(validateOnly).not.toContain('environment:');
  });

  it('omits the production job, environments and secret blocks when not configured', () => {
    const plain = renderCiWorkflowYaml(options({ deploy: true, deployProvider: 'cloudflare', e2eDeployment: true }));
    expect(plain).not.toContain('deploy-production');
    expect(plain).not.toContain('environment:');
    expect(plain).not.toContain('GIT_CRYPT_KEY_B64');
    expect(plain).toContain("github.ref == 'refs/heads/main'");
  });

  it('serializes pushes to the staging push branch instead of canceling a running deploy', () => {
    expect(rendered).toContain("cancel-in-progress: ${{ github.ref != 'refs/heads/trunk' }}");
    expect(rendered).not.toContain('cancel-in-progress: true');
  });
  it('quotes YAML-significant names instead of rejecting valid ones', () => {
    const quoted = renderCiWorkflowYaml(
      options({
        deploy: true,
        deployProvider: 'cloudflare',
        e2eDeployment: true,
        pushBranches: ["o'brien", 'trunk'],
        environments: { staging: 'review env', production: 'production' },
        productionOnPush: true,
      }),
    );

    // Expression literal: the branch quote doubles; the YAML list quotes the item.
    expect(quoted).toContain("github.ref == 'refs/heads/o''brien'");
    expect(quoted).toContain("github.ref != 'refs/heads/o''brien'");
    expect(quoted).toContain('- "o\'brien"');
    expect(quoted).toContain('- trunk');
    // Significant environment names quote; plain ones stay bare.
    expect(quoted).toContain('environment: "review env"');
    expect(quoted).toContain('environment: production');
  });

  it('preserves scalar-looking branch and environment names as strings', () => {
    const workflow = renderCiWorkflowYaml(
      options({
        deploy: true,
        e2eDeployment: true,
        productionOnPush: true,
        pushBranches: ['123', 'null', 'false'],
        environments: { staging: 'true', production: '123' },
      }),
    );
    expect(Bun.YAML.parse(workflow)).toMatchObject({
      on: { push: { branches: ['123', 'null', 'false'] } },
      jobs: {
        main: { environment: 'true' },
        'e2e-deployment': { environment: 'true' },
        'deploy-production': { environment: '123' },
      },
    });
  });

  it('runs cargo and sibling-source preflight in both follow-up jobs before setup, with shifting anchors', () => {
    const configured = options({
      deploy: true,
      deployProvider: 'cloudflare',
      e2eDeployment: true,
      pushBranches: ['trunk'],
      productionOnPush: true,
      cargoCredentials: { registryTokenEnvs: ['CARGO_REGISTRIES_EXAMPLE_TOKEN'] },
      sourceCheckouts: [{ path: '../sibling', repository: 'https://git.example.net/sibling.git' }],
    });
    const full = renderCiWorkflowYaml(configured);
    const e2eJob = full.slice(full.indexOf('  e2e-deployment:'), full.indexOf('  deploy-production:'));
    const productionJob = full.slice(full.indexOf('  deploy-production:'));

    for (const job of [e2eJob, productionJob]) {
      expect(job).toContain('CARGO_REGISTRIES_EXAMPLE_TOKEN: ${{ secrets.CARGO_REGISTRIES_EXAMPLE_TOKEN }}');
      const cargoAt = job.indexOf('- name: Prepare Cargo credentials');
      const sourcesAt = job.indexOf('- name: 📦 Check out sibling sources');
      const setupAt = job.indexOf('- name: 🧱 Setup Nix/devenv');
      expect(cargoAt).toBeGreaterThanOrEqual(0);
      expect(sourcesAt).toBeGreaterThan(cargoAt);
      expect(setupAt).toBeGreaterThan(sourcesAt);
      // Checkout 2, cargo 3, sources 4, setup 5: the middle step and its anchor move to 6.
      expect(job).toContain('# Step 6');
      expect(job).toContain('--step 6');
      expect(job).not.toContain('--step 4');
      expect(job).toContain('# Step 7');
    }
    expect(e2eJob).toContain(
      'run: smoo github-ci nx-smart --target e2e-deployment --mode run-many --stage "${{ needs.main.outputs.deployment-stage }}" --stream-output --name "E2E Tests (Deployed Stage)" --step 6',
    );
    expect(productionJob).toContain(
      'run: smoo github-ci nx-deploy --stage production --mode run-many --select-tag production-push-deploy-target --name "Deploy Production" --step 6',
    );
  });

  it('keeps the base follow-up anchors when no preflight is configured', () => {
    const e2eJob = rendered.slice(rendered.indexOf('  e2e-deployment:'), rendered.indexOf('  deploy-production:'));
    expect(e2eJob).toContain('--step 4');
    expect(e2eJob).not.toContain('Prepare Cargo credentials');
    expect(e2eJob).not.toContain('Check out sibling sources');
    expect(e2eJob).not.toContain('CARGO_REGISTRIES_EXAMPLE_TOKEN');
  });
});
