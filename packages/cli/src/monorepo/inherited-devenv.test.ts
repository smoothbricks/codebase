import { describe, expect, it } from 'bun:test';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';

/**
 * The producer is a managed raw script direnv runs as plain Bun source, so it is
 * loaded here the same way: a `bun -e` child that imports it with no transform
 * preloads (a static import would load it through them), whose env carries only
 * PATH/HOME. Every routing input travels in the request.
 */
const PRODUCER = join(managedAssetsRoot, 'raw/tooling/direnv/inherited-devenv.ts');
const SCRIPT = `
const producer = await import(Bun.argv[1]);
const { env, texts } = JSON.parse(Bun.argv[2]);
process.stdout.write(JSON.stringify({
  evaluator: producer.privateEnvironment(env),
  embedded: texts.map((text) => producer.embedsRouting(text, env)),
}));
`;

interface Outcome {
  readonly evaluator: Readonly<Record<string, string>>;
  readonly embedded: readonly boolean[];
}

function isOutcome(value: unknown): value is Outcome {
  return (
    typeof value === 'object' &&
    value !== null &&
    'evaluator' in value &&
    typeof value.evaluator === 'object' &&
    value.evaluator !== null &&
    Object.values(value.evaluator).every((entry: unknown) => typeof entry === 'string') &&
    'embedded' in value &&
    Array.isArray(value.embedded) &&
    value.embedded.every((entry: unknown) => typeof entry === 'boolean')
  );
}

async function produce(env: Record<string, string>, texts: readonly string[] = []): Promise<Outcome> {
  const proc = Bun.spawn({
    cmd: ['bun', '-e', SCRIPT, PRODUCER, JSON.stringify({ env, texts })],
    env: { PATH: process.env.PATH, HOME: process.env.HOME },
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  expect({ exitCode, stderr }).toEqual({ exitCode: 0, stderr: '' });
  const outcome: unknown = JSON.parse(stdout);
  if (!isOutcome(outcome)) throw new Error(`malformed producer outcome: ${stdout}`);
  return outcome;
}

const token = 'workspace-token-sentinel_-0123456789abcdefghijklmnopqrstuv';
const gateway = `http://cowshed:${token}@127.0.0.1:49104`;
const bundle = '/workspace/.cowshed/ca-bundle.pem';
// What cowshed hands a workspace child, plus what no evaluator may receive.
const workspaceEnv: Record<string, string> = {
  PATH: '/nix/store/devenv/bin',
  HOME: '/workspace/.cowshed/home',
  XDG_CACHE_HOME: '/workspace/.cowshed/cache',
  NIX_CACHE_HOME: '/private/cowshed/caches/nix/cache',
  HTTP_PROXY: gateway,
  HTTPS_PROXY: gateway,
  http_proxy: gateway,
  https_proxy: gateway,
  NO_PROXY: '127.0.0.1,localhost,::1',
  no_proxy: '127.0.0.1,localhost,::1',
  NIX_SSL_CERT_FILE: bundle,
  SSL_CERT_FILE: bundle,
  GIT_SSL_CAINFO: bundle,
  NODE_EXTRA_CA_CERTS: '/workspace/.cowshed/ca.pem',
  NIX_CONFIG: `access-tokens = github.com=caller-access-token\nimpure-env = SECRET=1\nssl-cert-file = /etc/nix/stale.crt # host default\nssl-cert-file = ${bundle}`,
  COWSHED_WORKSPACE_TOKEN: token,
  PRIVATE_CREDENTIAL: 'origin-only-secret-sentinel',
  NODE_USE_ENV_PROXY: '1',
};

describe('inherited devenv evaluator routing', () => {
  it('evaluates through the live gateway proxy and CA, with no other credential', async () => {
    const { evaluator } = await produce(workspaceEnv);
    expect(evaluator).toEqual({
      PATH: '/nix/store/devenv/bin',
      HOME: '/workspace/.cowshed/home',
      XDG_CACHE_HOME: '/workspace/.cowshed/cache',
      NIX_CACHE_HOME: '/private/cowshed/caches/nix/cache',
      HTTP_PROXY: gateway,
      HTTPS_PROXY: gateway,
      http_proxy: gateway,
      https_proxy: gateway,
      NO_PROXY: '127.0.0.1,localhost,::1',
      no_proxy: '127.0.0.1,localhost,::1',
      NIX_SSL_CERT_FILE: bundle,
      SSL_CERT_FILE: bundle,
      GIT_SSL_CAINFO: bundle,
      NODE_EXTRA_CA_CERTS: '/workspace/.cowshed/ca.pem',
      // nix.conf's own ssl-cert-file outranks NIX_SSL_CERT_FILE; only this line pins the bundle.
      NIX_CONFIG: `ssl-cert-file = ${bundle}`,
    });
  });

  it('passes no NIX_CONFIG when the caller pins no CA file', async () => {
    const { evaluator } = await produce({ PATH: '/bin', NIX_CONFIG: 'access-tokens = github.com=caller-access-token' });
    expect(evaluator).toEqual({ PATH: '/bin' });
  });

  it('finds the proxy credential in a would-be artifact however it is spelled', async () => {
    const encoded = 'http://user:p%40ss@proxy.example.net:8080';
    const { embedded } = await produce({ ...workspaceEnv, ALL_PROXY: encoded }, [
      `export CAPTURED=${gateway}`,
      `export CAPTURED=cowshed:${token}`,
      `export CAPTURED=${token}`,
      'export CAPTURED=p@ss',
      'export CAPTURED=p%40ss',
      // Neither the exclusions nor the fixed username label are credentials.
      'export NO_PROXY=127.0.0.1,localhost,::1 DIR=/Users/me/.cowshed/run',
    ]);
    expect(embedded).toEqual([true, true, true, true, true, false]);
  });

  it('finds a credential carried as the username alone', async () => {
    const { embedded } = await produce({ HTTPS_PROXY: 'http://token-as-user-sentinel@proxy.example.net:3128' }, [
      'export CAPTURED=token-as-user-sentinel',
    ]);
    expect(embedded).toEqual([true]);
  });
});
