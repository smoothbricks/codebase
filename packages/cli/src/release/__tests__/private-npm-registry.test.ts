import { describe, expect, it } from 'bun:test';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { releaseBootstrapNpmPackages } from '../index.js';
import { withPackWorkspace } from './helpers/pack-workspace.js';
import {
  FIXTURE_OWNER,
  FIXTURE_READ_TOKEN,
  FIXTURE_READ_TOKEN_ENV,
  FIXTURE_REGISTRY_ENV,
  FIXTURE_SCOPE,
  type PrivateNpmFixture,
  withPrivateNpmFixture,
} from './helpers/private-registry.js';

const PRIVATE_PACKAGE = `${FIXTURE_SCOPE}/probe`;
const PRIVATE_VERSION = '0.1.0';
const PUBLIC_PACKAGE = 'fixture-public-dep';
const PUBLIC_VERSION = '2.0.0';
const OWNER_PATH_PREFIX = `/api/packages/${FIXTURE_OWNER}/npm/`;

/**
 * Bun install routing is a deployment contract with no unit-testable seam: the
 * only place the scope URL, the token reference and the owner path are all
 * resolved is inside the installer. These tests run the pinned Bun against a
 * loopback registry, so a Bun upgrade that stopped expanding `$VAR` — or a
 * configuration change that leaked the credential to a public origin — fails
 * here instead of in a private-install outage.
 */
describe('private npm scope routing through bun install', () => {
  it('expands the declared registry and read-token variables into owner-path requests', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await seedPackages(fixture);
      const consumer = await fixture.createConsumer({
        dependencies: { [PRIVATE_PACKAGE]: PRIVATE_VERSION },
        registry: '$FIXTURE_PUBLIC_REGISTRY',
        scopes: { [FIXTURE_SCOPE]: { url: `$${FIXTURE_REGISTRY_ENV}`, token: `$${FIXTURE_READ_TOKEN_ENV}` } },
      });

      const result = await consumer.install(fixtureEnv(fixture));

      expect(result.exitCode).toBe(0);
      expect(await consumer.installedVersion(PRIVATE_PACKAGE)).toBe(PRIVATE_VERSION);
      const requests = fixture.privateRegistry.requests;
      expect(requests.length).toBeGreaterThanOrEqual(2);
      expect(requests.map((request) => request.path.startsWith(OWNER_PATH_PREFIX))).not.toContain(false);
      expect(requests.map((request) => request.authorization)).not.toContain(null);
      expect(new Set(requests.map((request) => request.authorization))).toEqual(
        new Set([`Bearer ${FIXTURE_READ_TOKEN}`]),
      );
      // Both the metadata and the tarball must come from the owner path.
      expect(requests.some((request) => request.path.endsWith('.tgz'))).toBe(true);
    });
  });

  it('leaves public dependencies on the public registry without attaching the private credential', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await seedPackages(fixture);
      const consumer = await fixture.createConsumer({
        dependencies: { [PRIVATE_PACKAGE]: PRIVATE_VERSION, [PUBLIC_PACKAGE]: PUBLIC_VERSION },
        registry: '$FIXTURE_PUBLIC_REGISTRY',
        scopes: { [FIXTURE_SCOPE]: { url: `$${FIXTURE_REGISTRY_ENV}`, token: `$${FIXTURE_READ_TOKEN_ENV}` } },
      });

      const result = await consumer.install(fixtureEnv(fixture));

      expect(result.exitCode).toBe(0);
      expect(await consumer.installedVersion(PUBLIC_PACKAGE)).toBe(PUBLIC_VERSION);
      const publicRequests = fixture.publicRegistry.requests;
      expect(publicRequests.length).toBeGreaterThanOrEqual(2);
      expect(publicRequests.map((request) => request.authorization)).toEqual(publicRequests.map(() => null));
      expect(fixture.publicRegistry.requestsFor(FIXTURE_SCOPE)).toEqual([]);
      expect(fixture.privateRegistry.requestsFor(PUBLIC_PACKAGE)).toEqual([]);
    });
  });

  it('refuses the install when the registry variable is unset instead of falling back to the public registry', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await seedPackages(fixture);
      const consumer = await fixture.createConsumer({
        dependencies: { [PRIVATE_PACKAGE]: PRIVATE_VERSION },
        registry: '$FIXTURE_PUBLIC_REGISTRY',
        scopes: { [FIXTURE_SCOPE]: { url: `$${FIXTURE_REGISTRY_ENV}`, token: `$${FIXTURE_READ_TOKEN_ENV}` } },
      });

      const { [FIXTURE_REGISTRY_ENV]: _registry, ...envWithoutRegistry } = fixtureEnv(fixture);
      const result = await consumer.install(envWithoutRegistry);

      expect(result.exitCode).not.toBe(0);
      expect(await consumer.installedVersion(PRIVATE_PACKAGE)).toBeNull();
      // No request at all: an unresolved endpoint must not become a public lookup.
      expect(fixture.privateRegistry.requests).toEqual([]);
      expect(fixture.publicRegistry.requestsFor(FIXTURE_SCOPE)).toEqual([]);
    });
  });

  it('blocks the install on a rejected read credential without a public fallback', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await seedPackages(fixture);
      const consumer = await fixture.createConsumer({
        dependencies: { [PRIVATE_PACKAGE]: PRIVATE_VERSION },
        registry: '$FIXTURE_PUBLIC_REGISTRY',
        scopes: { [FIXTURE_SCOPE]: { url: `$${FIXTURE_REGISTRY_ENV}`, token: `$${FIXTURE_READ_TOKEN_ENV}` } },
      });
      fixture.privateRegistry.failWith(401);

      const { [FIXTURE_READ_TOKEN_ENV]: _token, ...envWithoutToken } = fixtureEnv(fixture);
      const result = await consumer.install(envWithoutToken);

      expect(result.exitCode).not.toBe(0);
      expect(await consumer.installedVersion(PRIVATE_PACKAGE)).toBeNull();
      const attempted = fixture.privateRegistry.requests;
      expect(attempted.length).toBeGreaterThanOrEqual(1);
      expect(attempted.map((request) => request.path.startsWith(OWNER_PATH_PREFIX))).not.toContain(false);
      // An unset token must not be substituted by a real credential from anywhere.
      expect(attempted.map((request) => request.authorization?.includes(FIXTURE_READ_TOKEN) ?? false)).not.toContain(
        true,
      );
      expect(fixture.publicRegistry.requestsFor(FIXTURE_SCOPE)).toEqual([]);
    });
  });

  it('keeps the read token out of the lockfile, node_modules and the install cache', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await seedPackages(fixture);
      const consumer = await fixture.createConsumer({
        dependencies: { [PRIVATE_PACKAGE]: PRIVATE_VERSION },
        registry: '$FIXTURE_PUBLIC_REGISTRY',
        scopes: { [FIXTURE_SCOPE]: { url: `$${FIXTURE_REGISTRY_ENV}`, token: `$${FIXTURE_READ_TOKEN_ENV}` } },
      });

      expect((await consumer.install(fixtureEnv(fixture))).exitCode).toBe(0);

      expect(await consumer.findSecretLeaks(FIXTURE_READ_TOKEN)).toEqual([]);
      // The endpoint itself is not a secret and is expected in resolution data;
      // only the credential must be absent.
      expect(await consumer.findSecretLeaks(fixture.privateRegistry.registry)).not.toEqual([]);
    });
  });

  /**
   * Runs the bunfig a repository actually commits, with only the environment
   * values redirected at the loopback registry. Skipped unless a path is
   * supplied, because the config lives in the consuming repository rather than here:
   *
   *   SMOO_PRIVATE_NPM_BUNFIG=/path/to/consumer/bunfig.toml \
   *     bun test src/release/__tests__/private-npm-registry.test.ts
   */
  it.skipIf(!process.env.SMOO_PRIVATE_NPM_BUNFIG)(
    'routes the committed repository bunfig scope through the declared variables',
    async () => {
      const bunfigPath = process.env.SMOO_PRIVATE_NPM_BUNFIG ?? '';
      const bunfig = await readFile(bunfigPath, 'utf8');
      expect(bunfig).toContain(FIXTURE_SCOPE);
      await withPrivateNpmFixture(async (fixture) => {
        await seedPackages(fixture);
        const consumer = await fixture.createConsumer({
          dependencies: { [PRIVATE_PACKAGE]: PRIVATE_VERSION },
          bunfig,
        });

        const result = await consumer.install(fixtureEnv(fixture));

        expect(result.exitCode).toBe(0);
        expect(await consumer.installedVersion(PRIVATE_PACKAGE)).toBe(PRIVATE_VERSION);
        expect(
          fixture.privateRegistry.requests.map((request) => request.path.startsWith(OWNER_PATH_PREFIX)),
        ).not.toContain(false);
        expect(new Set(fixture.privateRegistry.requests.map((request) => request.authorization))).toEqual(
          new Set([`Bearer ${FIXTURE_READ_TOKEN}`]),
        );
      });
    },
  );
});

/**
 * Bootstrap and trust-publisher are npmjs-specific: they run `npm login`,
 * publish a public placeholder and configure npm trusted publishing. Once
 * release classification widened to include npm:private, any flow still reading
 * that widened list would create a PUBLIC npmjs package named after a private
 * one. These paths must keep reading the public-only list.
 */
describe('public npm bootstrap boundary', () => {
  it('never offers a private package to the npmjs placeholder bootstrap', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await withPackWorkspace(
        async (root) => {
          // The existence probe is the observable, and it must never be a real
          // npmjs request. Both client configurations are written because the
          // probe is an npm/bun implementation detail: npm reads .npmrc, bun
          // reads bunfig.toml, and either must land on the loopback stand-in.
          await writeFile(join(root, '.npmrc'), `registry=${fixture.publicRegistry.registry}\n`);
          await writeFile(
            join(root, 'bunfig.toml'),
            `[install]\nregistry = ${JSON.stringify(fixture.publicRegistry.registry)}\n`,
          );

          await releaseBootstrapNpmPackages(root, { dryRun: true, skipLogin: true });

          const queried = fixture.publicRegistry.requestsFor('public-face');
          expect(queried.length).toBeGreaterThanOrEqual(1);
          expect(fixture.publicRegistry.requestsFor('alpha')).toEqual([]);
          expect(fixture.publicRegistry.requestsFor('beta')).toEqual([]);
          expect(fixture.privateRegistry.requests).toEqual([]);
        },
        { publicFace: true },
      );
    });
  });
});

async function seedPackages(fixture: PrivateNpmFixture): Promise<void> {
  await fixture.privateRegistry.publishPackage({ name: PRIVATE_PACKAGE, version: PRIVATE_VERSION });
  await fixture.publicRegistry.publishPackage({ name: PUBLIC_PACKAGE, version: PUBLIC_VERSION });
}

function fixtureEnv(fixture: PrivateNpmFixture): Record<string, string> {
  return {
    [FIXTURE_REGISTRY_ENV]: fixture.privateRegistry.registry,
    [FIXTURE_READ_TOKEN_ENV]: FIXTURE_READ_TOKEN,
    // A default registry that is also loopback makes any accidental public
    // fallback observable instead of silently contacting npmjs.
    FIXTURE_PUBLIC_REGISTRY: fixture.publicRegistry.registry,
  };
}
