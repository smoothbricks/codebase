import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { run } from '../lib/run.js';

const HOST_CACHE_ROOT = '/var/cache/ci';
const SYSTEM_CHROME_PATHS = [
  '/usr/bin/google-chrome',
  '/usr/bin/chromium-browser',
  '/usr/bin/chromium',
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
] as const;

export interface ChromiumSetupDependencies {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly exists: (path: string) => boolean;
  readonly run: (command: string, args: string[], cwd: string, env?: Record<string, string>) => Promise<void>;
}

export type ChromiumSetupResult =
  | { readonly mode: 'persistent-cache'; readonly browserCachePath: string }
  | { readonly mode: 'system'; readonly executablePath: string }
  | { readonly mode: 'developer-cache'; readonly browserCachePath?: string };

const defaultDependencies: ChromiumSetupDependencies = {
  env: process.env,
  exists: existsSync,
  run,
};

/**
 * Ensure Chromium is available without polluting ephemeral GitHub runners.
 * Persistent host runners and developer machines may install into their
 * respective caches; ephemeral GitHub runners must use their image's browser.
 */
export async function ensureChromium(
  cwd = process.cwd(),
  dependencies: ChromiumSetupDependencies = defaultDependencies,
): Promise<ChromiumSetupResult> {
  const { env, exists, run: runCommand } = dependencies;
  const githubActions = env.GITHUB_ACTIONS === 'true';

  if (githubActions && exists(HOST_CACHE_ROOT)) {
    // Host setup provisions the XDG cache directory, not arbitrary siblings
    // under /var/cache/ci. Honor an explicit Playwright cache before that default.
    const browserCachePath =
      env.PLAYWRIGHT_BROWSERS_PATH ?? join(env.XDG_CACHE_HOME ?? `${HOST_CACHE_ROOT}/xdg`, 'ms-playwright');
    await runCommand('playwright', ['install', 'chromium', '--only-shell'], cwd, {
      PLAYWRIGHT_BROWSERS_PATH: browserCachePath,
    });
    console.log(`Chromium ready in persistent cache: ${browserCachePath}`);
    return { mode: 'persistent-cache', browserCachePath };
  }

  if (githubActions) {
    const candidates = [env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH, ...SYSTEM_CHROME_PATHS].filter(
      (path): path is string => typeof path === 'string' && path.length > 0,
    );
    const executablePath = candidates.find(exists);
    if (!executablePath) {
      throw new Error(
        `GitHub-hosted runner has no preinstalled Chromium. Refusing to download; searched: ${candidates.join(', ')}`,
      );
    }
    console.log(`Using preinstalled Chromium: ${executablePath}`);
    return { mode: 'system', executablePath };
  }

  const browserCachePath = env.PLAYWRIGHT_BROWSERS_PATH;
  await runCommand(
    'playwright',
    ['install', 'chromium', '--only-shell'],
    cwd,
    browserCachePath ? { PLAYWRIGHT_BROWSERS_PATH: browserCachePath } : undefined,
  );
  console.log(browserCachePath ? `Chromium ready in configured cache: ${browserCachePath}` : 'Chromium ready.');
  return browserCachePath ? { mode: 'developer-cache', browserCachePath } : { mode: 'developer-cache' };
}

/** Launch after setup so Playwright reads the selected cache before its first import. */
export async function runWithChromium(
  command: string,
  args: string[],
  cwd = process.cwd(),
  dependencies: ChromiumSetupDependencies = defaultDependencies,
): Promise<void> {
  const browser = await ensureChromium(cwd, dependencies);
  const environment: Record<string, string> | undefined =
    browser.mode === 'system'
      ? { PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH: browser.executablePath }
      : browser.browserCachePath
        ? { PLAYWRIGHT_BROWSERS_PATH: browser.browserCachePath }
        : undefined;
  await dependencies.run(command, args, cwd, environment);
}
