import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  assertPrivatePackage,
  npmPublishedVersionExists,
  type PrivateNpmRegistry,
  privateNpmUserconfigContent,
  requirePrivateNpmRegistry,
  resolvePrivateNpmRegistry,
  selectPublishDestination,
  selectRegistryForPackage,
  withPrivateNpmUserconfig,
} from '../private-npm.js';
import {
  FIXTURE_OWNER,
  FIXTURE_PUBLISH_TOKEN,
  FIXTURE_PUBLISH_TOKEN_ENV,
  FIXTURE_READ_TOKEN,
  FIXTURE_READ_TOKEN_ENV,
  FIXTURE_SCOPE,
  type FixtureNpmRegistry,
  type PrivateNpmFixture,
  withPrivateNpmFixture,
} from './helpers/private-registry.js';

const PRIVATE_PACKAGE = `${FIXTURE_SCOPE}/probe`;
const PRIVATE_VERSION = '0.1.0';
const DECLARED_REGISTRY = `https://forge.example.test/api/packages/${FIXTURE_OWNER}/npm/`;

describe('private npm registry configuration', () => {
  it('resolves the declared scope, endpoint and path-scoped auth key', async () => {
    await withConfiguredRoot({ registry: DECLARED_REGISTRY }, async (root) => {
      const resolved = resolvePrivateNpmRegistry(root);

      if (!resolved.ok) {
        throw new Error(
          `expected ${DECLARED_REGISTRY} to resolve, got ${resolved.error.kind}: ${resolved.error.message}`,
        );
      }
      expect(resolved.value.scope).toBe(FIXTURE_SCOPE);
      // The owner path is part of the identity: dropping it addresses the wrong
      // Forgejo namespace, or nothing at all.
      expect(resolved.value.registry).toBe(DECLARED_REGISTRY);
      expect(resolved.value.authKey).toBe(`//forge.example.test/api/packages/${FIXTURE_OWNER}/npm/:_authToken`);
      expect(resolved.value.readTokenEnv).toBe(FIXTURE_READ_TOKEN_ENV);
      expect(resolved.value.publishTokenEnv).toBe(FIXTURE_PUBLISH_TOKEN_ENV);
    });
  });

  it('preserves a server base path ahead of the owner path', async () => {
    // Forgejo is commonly mounted under a prefix; losing it addresses a path
    // the server does not serve.
    const registry = `https://forge.example.test/git/api/packages/${FIXTURE_OWNER}/npm/`;
    await withConfiguredRoot({ registry }, async (root) => {
      const resolved = resolvePrivateNpmRegistry(root);

      if (!resolved.ok) {
        throw new Error(`expected ${registry} to resolve, got ${resolved.error.kind}: ${resolved.error.message}`);
      }
      expect(resolved.value.registry).toBe(registry);
      expect(resolved.value.authKey).toBe(`//forge.example.test/git/api/packages/${FIXTURE_OWNER}/npm/:_authToken`);
    });
  });

  it('appends the trailing slash the npm auth key requires', async () => {
    const registry = `https://forge.example.test/api/packages/${FIXTURE_OWNER}/npm`;
    await withConfiguredRoot({ registry }, async (root) => {
      const resolved = resolvePrivateNpmRegistry(root);

      if (!resolved.ok) {
        throw new Error(`expected ${registry} to resolve, got ${resolved.error.kind}: ${resolved.error.message}`);
      }
      expect(resolved.value.registry).toBe(`${registry}/`);
      expect(resolved.value.authKey).toBe(`//forge.example.test/api/packages/${FIXTURE_OWNER}/npm/:_authToken`);
    });
  });

  it('reports missing configuration for a root without a declared private scope', async () => {
    await withConfiguredRoot({ registry: DECLARED_REGISTRY, declare: false }, async (root) => {
      const resolved = resolvePrivateNpmRegistry(root);

      expect(resolved.ok).toBe(false);
      if (resolved.ok) {
        return;
      }
      expect(resolved.error.kind).toBe('MissingConfiguration');
    });
  });

  it('names the missing .npmrc entry without disclosing any credential', async () => {
    await withConfiguredRoot({ registry: null }, async (root) => {
      const resolved = resolvePrivateNpmRegistry(root);

      expect(resolved.ok).toBe(false);
      if (resolved.ok) {
        return;
      }
      expect(resolved.error.kind).toBe('MissingRegistry');
      expect(resolved.error.message).toContain('.npmrc');
      expect(resolved.error.message).toContain(FIXTURE_SCOPE);
      expect(resolved.error.message).not.toContain(FIXTURE_READ_TOKEN);
      expect(resolved.error.message).not.toContain(FIXTURE_PUBLISH_TOKEN);
    });
  });

  it.each([
    ['plaintext transport', `http://forge.example.test/api/packages/${FIXTURE_OWNER}/npm/`],
    ['embedded credentials', `https://user:s3cr3t@forge.example.test/api/packages/${FIXTURE_OWNER}/npm/`],
    ['query string', `https://forge.example.test/api/packages/${FIXTURE_OWNER}/npm/?token=s3cr3t`],
    ['fragment', `https://forge.example.test/api/packages/${FIXTURE_OWNER}/npm/#s3cr3t`],
    ['missing owner npm path', 'https://forge.example.test/'],
    ['unparseable url', 'forge.example.test/api/packages/owner/npm/'],
  ])('refuses a registry URL with %s', async (_case, registry) => {
    await withConfiguredRoot({ registry }, async (root) => {
      const resolved = resolvePrivateNpmRegistry(root);

      expect(resolved.ok).toBe(false);
      if (resolved.ok) {
        return;
      }
      expect(resolved.error.kind).toBe('InvalidRegistry');
      // A rejected URL may be echoed; a credential embedded in one may not be.
      expect(resolved.error.message).not.toContain('s3cr3t');
    });
  });
});

/**
 * Every status and publish call routes through scope selection first. A
 * private-scoped name that selected no private registry would be probed — and
 * published — on npmjs, and a public dependency that selected the private one
 * would send a credential to the wrong host.
 */
describe('registry selection by package scope', () => {
  it('routes the declared scope privately and leaves every other scope public', async () => {
    await withConfiguredRoot({ registry: DECLARED_REGISTRY }, async (root) => {
      expect(selectRegistryForPackage(root, `${FIXTURE_SCOPE}/probe`)?.registry).toBe(DECLARED_REGISTRY);
      expect(selectRegistryForPackage(root, '@smoothbricks/columine')).toBeNull();
      expect(selectRegistryForPackage(root, 'lodash')).toBeNull();
      // A near-miss name must not inherit the scope's routing.
      expect(selectRegistryForPackage(root, `${FIXTURE_SCOPE}-other/pkg`)).toBeNull();
    });
  });
});

/**
 * The publish destination is the last decision before bytes leave: a package
 * carrying npm:private must never reach the public registry because its
 * destination failed to resolve. The refusal has to happen before any network
 * I/O, and its diagnostic has to say which configuration is missing.
 */
describe('publish destination selection', () => {
  it('sends an in-scope package to the resolved private registry', async () => {
    await withConfiguredRoot({ registry: DECLARED_REGISTRY }, async (root) => {
      const destination = selectPublishDestination(root, {
        name: `${FIXTURE_SCOPE}/probe`,
        tags: ['npm:private'],
      });

      expect(destination.kind).toBe('private');
      if (destination.kind !== 'private') {
        return;
      }
      expect(destination.registry.registry).toBe(DECLARED_REGISTRY);
    });
  });

  it('sends a public package to the public registry even with no private configuration', async () => {
    await withConfiguredRoot({ registry: DECLARED_REGISTRY, declare: false }, async (root) => {
      expect(selectPublishDestination(root, { name: '@smoothbricks/columine', tags: ['npm:public'] })).toEqual({
        kind: 'public',
      });
    });
  });

  it('refuses an npm:private package whose destination cannot resolve, without contacting a registry', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      const pkg = { name: `${FIXTURE_SCOPE}/probe`, tags: ['npm:private'] };

      // Declared configuration, no scoped .npmrc entry: the diagnostic must
      // name the file an operator has to configure.
      await withConfiguredRoot({ registry: null }, async (root) => {
        expect(() => selectPublishDestination(root, pkg)).toThrow(/npmrc/);
      });

      // No declared configuration at all: still a refusal, naming the package
      // and the missing declaration rather than falling back to npmjs.
      await withConfiguredRoot({ registry: DECLARED_REGISTRY, declare: false }, async (root) => {
        expect(() => selectPublishDestination(root, pkg)).toThrow(/privateNpm/);
        expect(() => selectPublishDestination(root, pkg)).toThrow(/probe/);
      });

      // Configured, but the package is outside the declared scope.
      await withConfiguredRoot({ registry: DECLARED_REGISTRY }, async (root) => {
        expect(() => selectPublishDestination(root, { name: '@other/thing', tags: ['npm:private'] })).toThrow(
          new RegExp(FIXTURE_SCOPE.replace('.', '\\.')),
        );
      });

      expect(fixture.privateRegistry.requests).toEqual([]);
      expect(fixture.publicRegistry.requests).toEqual([]);
    });
  });

  it('keeps credentials out of destination refusals', async () => {
    await withConfiguredRoot({ registry: null }, async (root) => {
      let message = '';
      try {
        selectPublishDestination(root, { name: `${FIXTURE_SCOPE}/probe`, tags: ['npm:private'] });
      } catch (error) {
        message = error instanceof Error ? error.message : String(error);
      }
      expect(message).not.toBe('');
      expect(message).not.toContain(FIXTURE_READ_TOKEN);
      expect(message).not.toContain(FIXTURE_PUBLISH_TOKEN);
    });
  });
});

describe('private package scope enforcement', () => {
  const registry: PrivateNpmRegistry = {
    scope: FIXTURE_SCOPE,
    registry: DECLARED_REGISTRY,
    authKey: `//forge.example.test/api/packages/${FIXTURE_OWNER}/npm/:_authToken`,
    readTokenEnv: FIXTURE_READ_TOKEN_ENV,
    publishTokenEnv: FIXTURE_PUBLISH_TOKEN_ENV,
  };

  it('accepts a package inside the declared scope', () => {
    expect(() => assertPrivatePackage({ name: PRIVATE_PACKAGE, json: {} }, registry)).not.toThrow();
  });

  it('refuses a package outside the declared scope', () => {
    // Packing an out-of-scope name would route it to the private owner path
    // under a name that owner does not own.
    expect(() => assertPrivatePackage({ name: '@other/thing', json: {} }, registry)).toThrow(/@other\/thing/);
    expect(() => assertPrivatePackage({ name: '@other/thing', json: {} }, registry)).toThrow(
      new RegExp(FIXTURE_SCOPE.replace('.', '\\.')),
    );
  });

  it('refuses a package pinning an explicit registry other than the resolved one', () => {
    // Packing such a package would ship a manifest that sends consumers, and a
    // publish, to a registry the private configuration never named.
    expect(() =>
      assertPrivatePackage(
        { name: PRIVATE_PACKAGE, json: { publishConfig: { registry: 'https://registry.npmjs.org/' } } },
        registry,
      ),
    ).toThrow(new RegExp(PRIVATE_PACKAGE.replace('.', '\\.').replace('/', '\\/')));
  });

  it('accepts a package pinning exactly the resolved registry', () => {
    expect(() =>
      assertPrivatePackage(
        { name: PRIVATE_PACKAGE, json: { publishConfig: { registry: DECLARED_REGISTRY } } },
        registry,
      ),
    ).not.toThrow();
  });

  it('keeps credentials out of scope diagnostics', () => {
    let message = '';
    try {
      assertPrivatePackage({ name: '@other/thing', json: {} }, registry);
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).not.toBe('');
    expect(message).not.toContain(FIXTURE_READ_TOKEN);
    expect(message).not.toContain(FIXTURE_PUBLISH_TOKEN);
  });
});

describe('private operations without declared configuration', () => {
  it('refuses before any registry is contacted', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await withConfiguredRoot({ registry: DECLARED_REGISTRY, declare: false }, async (root) => {
        expect(() => requirePrivateNpmRegistry(root)).toThrow();
      });

      expect(fixture.privateRegistry.requests).toEqual([]);
      expect(fixture.publicRegistry.requests).toEqual([]);
    });
  });

  it('refuses when the declared endpoint variable is unset', async () => {
    await withConfiguredRoot({ registry: null }, async (root) => {
      expect(() => requirePrivateNpmRegistry(root)).toThrow(/npmrc/);
    });
  });
});

describe('private npm userconfig', () => {
  it('references the token environment variable instead of writing credential bytes', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      const resolved = loopbackRegistry(fixture.privateRegistry);

      const read = privateNpmUserconfigContent(resolved, { mode: 'read' });
      const publish = privateNpmUserconfigContent(resolved, { mode: 'publish' });

      for (const content of [read, publish]) {
        expect(content).toContain(`${FIXTURE_SCOPE}:registry=${fixture.privateRegistry.registry}`);
        expect(content).toContain(fixture.privateRegistry.authKey);
        expect(content).not.toContain(FIXTURE_READ_TOKEN);
        expect(content).not.toContain(FIXTURE_PUBLISH_TOKEN);
      }
      expect(read).toContain(`\${${FIXTURE_READ_TOKEN_ENV}}`);
      expect(read).not.toContain(FIXTURE_PUBLISH_TOKEN_ENV);
      expect(publish).toContain(`\${${FIXTURE_PUBLISH_TOKEN_ENV}}`);
    });
  });

  it('refuses to materialize a userconfig when the credential is unset, without contacting a registry', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      const resolved = loopbackRegistry(fixture.privateRegistry);

      await expect(withPrivateNpmUserconfig(resolved, 'read', async () => 'unreachable')).rejects.toThrow(
        new RegExp(FIXTURE_READ_TOKEN_ENV),
      );
      await expect(withPrivateNpmUserconfig(resolved, 'publish', async () => 'unreachable')).rejects.toThrow(
        new RegExp(FIXTURE_PUBLISH_TOKEN_ENV),
      );

      expect(fixture.privateRegistry.requests).toEqual([]);
      expect(fixture.publicRegistry.requests).toEqual([]);
    });
  });

  it('refuses publish mode when no publish token env is declared, without contacting a registry', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      const resolved = loopbackRegistry(fixture.privateRegistry);
      delete resolved.publishTokenEnv;

      await expect(withPrivateNpmUserconfig(resolved, 'publish', async () => 'unreachable')).rejects.toThrow(
        /publish token/,
      );

      expect(fixture.privateRegistry.requests).toEqual([]);
      expect(fixture.publicRegistry.requests).toEqual([]);
    });
  });

  it('refuses to render a publish userconfig when no publish token env is declared', () => {
    const readOnly: PrivateNpmRegistry = {
      scope: FIXTURE_SCOPE,
      registry: DECLARED_REGISTRY,
      authKey: `//forge.example.test/api/packages/${FIXTURE_OWNER}/npm/:_authToken`,
      readTokenEnv: FIXTURE_READ_TOKEN_ENV,
    };

    // Rendering and materializing must agree: a publish userconfig carrying
    // the read credential would 401 mid-publication, or spend a read-only
    // credential on a write.
    expect(() => privateNpmUserconfigContent(readOnly, { mode: 'publish' })).toThrow(/publish token/);
    expect(privateNpmUserconfigContent(readOnly, { mode: 'read' })).toContain(`\${${FIXTURE_READ_TOKEN_ENV}}`);
  });

  it('exposes the userconfig only for the operation, owner-readable, and removes it after', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      const resolved = loopbackRegistry(fixture.privateRegistry);

      const path = await withTokenEnv(() =>
        withPrivateNpmUserconfig(resolved, 'publish', async (userconfig) => {
          expect((await stat(userconfig)).mode & 0o777).toBe(0o600);
          expect(await readFile(userconfig, 'utf8')).toBe(privateNpmUserconfigContent(resolved, { mode: 'publish' }));
          return userconfig;
        }),
      );

      // A leftover userconfig is a durable pointer to a credentialed endpoint.
      await expect(stat(path)).rejects.toThrow();
    });
  });

  it('authenticates a real npm read against the configured owner path', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await fixture.privateRegistry.publishPackage({ name: PRIVATE_PACKAGE, version: PRIVATE_VERSION });
      const resolved = loopbackRegistry(fixture.privateRegistry);

      const result = await withTokenEnv(() =>
        withPrivateNpmUserconfig(resolved, 'read', async (userconfig) => {
          const client = await fixture.createNpmClient({
            registry: fixture.privateRegistry,
            userconfig,
            tokenEnv: FIXTURE_READ_TOKEN_ENV,
            token: FIXTURE_READ_TOKEN,
          });
          return client.view(`${PRIVATE_PACKAGE}@${PRIVATE_VERSION}`);
        }),
      );

      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain(PRIVATE_VERSION);
      const requests = fixture.privateRegistry.requestsFor(PRIVATE_PACKAGE);
      expect(requests.length).toBeGreaterThanOrEqual(1);
      expect(new Set(requests.map((request) => request.authorization))).toEqual(
        new Set([`Bearer ${FIXTURE_READ_TOKEN}`]),
      );
    });
  });

  it('refuses an unauthenticated read rather than treating it as absent', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await fixture.privateRegistry.publishPackage({ name: PRIVATE_PACKAGE, version: PRIVATE_VERSION });
      const client = await fixture.createNpmClient({ registry: fixture.privateRegistry, anonymous: true });
      fixture.privateRegistry.failWith(401);

      const result = await client.view(`${PRIVATE_PACKAGE}@${PRIVATE_VERSION}`);

      expect(result.exitCode).not.toBe(0);
      const combined = `${result.stdout}\n${result.stderr}`;
      expect(combined).toContain('E401');
      // Genuine absence is E404 alone; an auth failure must stay distinguishable.
      expect(combined).not.toContain('E404');
    });
  });
});

describe('private npm published-version status', () => {
  it('reports a published version through the resolved private registry', async () => {
    await withStatusFixture(async ({ fixture, root, userconfig }) => {
      await fixture.privateRegistry.publishPackage({ name: PRIVATE_PACKAGE, version: PRIVATE_VERSION });

      await expect(
        npmPublishedVersionExists(root, PRIVATE_PACKAGE, PRIVATE_VERSION, {
          registry: fixture.privateRegistry.registry,
          userconfig,
        }),
      ).resolves.toBe(true);
    });
  });

  it('reads through a repository .npmrc whose own auth line names an unset env', async () => {
    // The shape that cost a day: a repository commits
    // `//host/path:_authToken=${NPM_PUBLISH_TOKEN}` at the workspace root.
    // npm ranks that project file ABOVE the userconfig this CLI writes, so with
    // the publish env unset it sent the unexpanded value and the registry
    // answered 401 — with a perfectly good read credential in hand.
    await withStatusFixture(async ({ fixture, root, userconfig }) => {
      await fixture.privateRegistry.publishPackage({ name: PRIVATE_PACKAGE, version: PRIVATE_VERSION });
      const resolved = loopbackRegistry(fixture.privateRegistry);
      await writeFile(
        join(root, '.npmrc'),
        `${resolved.scope}:registry=${resolved.registry}\n${resolved.authKey}=\${NOT_SET_ANYWHERE}\n`,
      );

      // Control: with the userconfig alone the project file wins, so the token
      // npm sends is the unexpanded reference — the registry sees no usable
      // credential. Asserted on the wire, because a fixture that answers
      // anonymous reads would let the server's verdict hide it.
      await npmPublishedVersionExists(root, PRIVATE_PACKAGE, PRIVATE_VERSION, {
        registry: fixture.privateRegistry.registry,
        userconfig,
      });
      const overridden = fixture.privateRegistry.requestsFor('probe').at(-1);
      expect(overridden?.authorization ?? '').not.toContain(FIXTURE_READ_TOKEN);

      await expect(
        npmPublishedVersionExists(root, PRIVATE_PACKAGE, PRIVATE_VERSION, {
          registry: fixture.privateRegistry.registry,
          userconfig,
          credential: { authKey: resolved.authKey, tokenEnv: FIXTURE_READ_TOKEN_ENV },
        }),
      ).resolves.toBe(true);
      const authenticated = fixture.privateRegistry.requestsFor('probe').at(-1);
      expect(authenticated?.authorization ?? '').toContain(FIXTURE_READ_TOKEN);
    });
  });

  it('reports a genuine not-found as absent', async () => {
    await withStatusFixture(async ({ fixture, root, userconfig }) => {
      fixture.privateRegistry.failWith(404);

      await expect(
        npmPublishedVersionExists(root, PRIVATE_PACKAGE, PRIVATE_VERSION, {
          registry: fixture.privateRegistry.registry,
          userconfig,
        }),
      ).resolves.toBe(false);
    });
  });

  it.each([
    ['unauthorized', 401],
    ['forbidden', 403],
    ['unavailable', 503],
  ])('blocks on a %s registry response instead of reporting the version unpublished', async (_case, status) => {
    await withStatusFixture(async ({ fixture, root, userconfig }) => {
      fixture.privateRegistry.failWith(status);

      await expect(
        npmPublishedVersionExists(root, PRIVATE_PACKAGE, PRIVATE_VERSION, {
          registry: fixture.privateRegistry.registry,
          userconfig,
        }),
      ).rejects.toThrow();

      // One attempt per query: npm retries 5xx by default, and a retry storm
      // inside a publish gate turns an outage into a hang.
      expect(fixture.privateRegistry.requestsFor(PRIVATE_PACKAGE)).toHaveLength(1);
    });
  });

  it('never contacts a public registry for a private package status', async () => {
    await withStatusFixture(async ({ fixture, root, userconfig }) => {
      fixture.privateRegistry.failWith(403);

      await expect(
        npmPublishedVersionExists(root, PRIVATE_PACKAGE, PRIVATE_VERSION, {
          registry: fixture.privateRegistry.registry,
          userconfig,
        }),
      ).rejects.toThrow();

      expect(fixture.publicRegistry.requests).toEqual([]);
    });
  });
});

function loopbackRegistry(registry: FixtureNpmRegistry): PrivateNpmRegistry {
  return {
    scope: FIXTURE_SCOPE,
    registry: registry.registry,
    authKey: registry.authKey,
    readTokenEnv: FIXTURE_READ_TOKEN_ENV,
    publishTokenEnv: FIXTURE_PUBLISH_TOKEN_ENV,
  };
}

interface StatusFixture {
  fixture: PrivateNpmFixture;
  root: string;
  userconfig: string;
}

/**
 * Status queries run with the generated userconfig on disk and the token only
 * in the environment, exactly as the private CLI path invokes npm.
 */
async function withStatusFixture(fn: (context: StatusFixture) => Promise<void>): Promise<void> {
  await withPrivateNpmFixture(async (fixture) => {
    const root = join(fixture.root, 'status-root');
    await mkdir(root, { recursive: true });
    const userconfig = join(root, 'userconfig');
    await writeFile(
      userconfig,
      privateNpmUserconfigContent(loopbackRegistry(fixture.privateRegistry), { mode: 'read' }),
      { mode: 0o600 },
    );
    const previous = process.env[FIXTURE_READ_TOKEN_ENV];
    process.env[FIXTURE_READ_TOKEN_ENV] = FIXTURE_READ_TOKEN;
    try {
      await fn({ fixture, root, userconfig });
    } finally {
      restoreEnv(FIXTURE_READ_TOKEN_ENV, previous);
    }
  });
}

async function withConfiguredRoot(
  options: { registry: string | null; declare?: boolean },
  fn: (root: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-private-npm-config-'));
  const previousRead = process.env[FIXTURE_READ_TOKEN_ENV];
  const previousPublish = process.env[FIXTURE_PUBLISH_TOKEN_ENV];
  try {
    await writeFile(
      join(root, 'package.json'),
      `${JSON.stringify(
        {
          name: '@priv.test/source',
          version: '0.0.0',
          private: true,
          workspaces: ['packages/*'],
          ...(options.declare === false
            ? {}
            : {
                smoo: {
                  privateNpm: {
                    scope: FIXTURE_SCOPE,
                    readTokenEnv: FIXTURE_READ_TOKEN_ENV,
                    publishTokenEnv: FIXTURE_PUBLISH_TOKEN_ENV,
                  },
                },
              }),
        },
        null,
        2,
      )}\n`,
    );
    if (options.registry !== null) {
      await writeFile(join(root, '.npmrc'), `${FIXTURE_SCOPE}:registry=${options.registry}\n`);
    }
    // Credentials present throughout: a diagnostic must never echo them even
    // when they are available to echo.
    process.env[FIXTURE_READ_TOKEN_ENV] = FIXTURE_READ_TOKEN;
    process.env[FIXTURE_PUBLISH_TOKEN_ENV] = FIXTURE_PUBLISH_TOKEN;
    await fn(root);
  } finally {
    restoreEnv(FIXTURE_READ_TOKEN_ENV, previousRead);
    restoreEnv(FIXTURE_PUBLISH_TOKEN_ENV, previousPublish);
    await rm(root, { recursive: true, force: true });
  }
}

function restoreEnv(name: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[name];
  } else {
    process.env[name] = value;
  }
}

/** Both credentials present in the environment, as a configured operator shell has them. */
async function withTokenEnv<T>(fn: () => Promise<T>): Promise<T> {
  const previousRead = process.env[FIXTURE_READ_TOKEN_ENV];
  const previousPublish = process.env[FIXTURE_PUBLISH_TOKEN_ENV];
  process.env[FIXTURE_READ_TOKEN_ENV] = FIXTURE_READ_TOKEN;
  process.env[FIXTURE_PUBLISH_TOKEN_ENV] = FIXTURE_PUBLISH_TOKEN;
  try {
    return await fn();
  } finally {
    restoreEnv(FIXTURE_READ_TOKEN_ENV, previousRead);
    restoreEnv(FIXTURE_PUBLISH_TOKEN_ENV, previousPublish);
  }
}
