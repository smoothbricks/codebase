import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';
import { navigationRequestId } from '@smoothbricks/statebus-navigation-core';
import { type Browser, type BrowserContext, chromium, type Page } from 'playwright';
import type {} from './fixture.js';

let server: ReturnType<typeof Bun.serve>;
let browser: Browser;
let context: BrowserContext;
let page: Page;
let base: string;
beforeAll(async () => {
  const build = await Bun.build({
    entrypoints: [new URL('./fixture.ts', import.meta.url).pathname],
    target: 'browser',
  });
  if (!build.success) throw new AggregateError(build.logs, 'Browser fixture failed to build.');
  const script = await build.outputs[0].text();
  server = Bun.serve({
    port: 0,
    hostname: '127.0.0.1',
    fetch(request) {
      if (new URL(request.url).pathname === '/fixture.js')
        return new Response(script, { headers: { 'content-type': 'text/javascript' } });
      return new Response(
        '<!doctype html><html><body><div id="app"></div><script type="module" src="/fixture.js"></script></body></html>',
        { headers: { 'content-type': 'text/html' } },
      );
    },
  });
  base = `http://127.0.0.1:${server.port}`;
  const executablePath =
    process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH ??
    ['/usr/bin/chromium', '/usr/bin/google-chrome', '/usr/bin/chromium-browser'].find(existsSync);
  browser = await chromium.launch({ headless: true, executablePath, args: ['--no-sandbox'] });
});
beforeEach(async () => {
  context = await browser.newContext();
  page = await context.newPage();
  await page.goto(base);
  await page.waitForSelector('#billing');
});
afterEach(async () => {
  await context?.close();
});
afterAll(async () => {
  await browser?.close();
  await server?.stop(true);
});

async function pathIs(expected: string): Promise<void> {
  await page.waitForFunction((value) => document.querySelector('#path')?.textContent === value, expected);
  expect(await page.locator('#path').textContent()).toBe(expected);
}

describe('actual browser history with StateBus and React StrictMode', () => {
  it('renders an intent-driven deeplink, replace, native Back/Forward and hash changes', async () => {
    await page.click('#billing');
    await pathIs('/settings/billing?tab=invoices#recent');
    await page.evaluate(() => window.navigationTest.navigate({ kind: 'replace', to: '?tab=plans#annual' }));
    await pathIs('/settings/billing?tab=plans#annual');
    await page.goBack();
    await pathIs('/');
    await page.goForward();
    await pathIs('/settings/billing?tab=plans#annual');
    const before = await page.evaluate(() => window.navigationTest.rawCount());
    await page.evaluate(() => {
      location.hash = 'usage';
    });
    await pathIs('/settings/billing?tab=plans#usage');
    expect(await page.evaluate(() => window.navigationTest.rawCount())).toBe(before + 1);
  });
  it('blocks bus intent until confirmation but does not pretend to veto native history', async () => {
    await page.click('#guard');
    await page.click('#billing');
    await page.waitForFunction(() => window.navigationTest.state().operation.kind === 'blocked');
    await pathIs('/');
    await page.click('#confirm');
    await pathIs('/settings/billing?tab=invoices#recent');
    await page.goBack();
    await pathIs('/');
    expect(await page.evaluate(() => window.navigationTest.state().guard)).toBe('Unsaved changes');
  });
  it('rejects unsafe/cross-origin URLs and WebIDL-wrapping go values without changing history', async () => {
    for (const to of ['javascript:window.UNSAFE=true', 'https://other.example/private']) {
      await page.evaluate((value) => window.navigationTest.navigate({ kind: 'push', to: value }), to);
      await page.waitForFunction(() => window.navigationTest.state().operation.kind === 'failed');
      await pathIs('/');
    }
    await page.evaluate(() => window.navigationTest.navigate({ kind: 'go', delta: 2 ** 32 }));
    await page.waitForFunction(() => window.navigationTest.state().operation.kind === 'failed');
    await pathIs('/');
  });
  it('acknowledges unchanged navigate and prevents aborted/disposed adapter writes', async () => {
    await page.evaluate(() => window.navigationTest.navigate({ kind: 'navigate', to: '/' }));
    await page.waitForFunction(() => window.navigationTest.state().operation.kind === 'idle');
    const request = {
      requestId: navigationRequestId('cancelled'),
      intent: { kind: 'push', to: '/not-written' },
    } as const;
    expect(await page.evaluate((input) => window.navigationTest.rawExecute(input, true), request)).toMatchObject({
      kind: 'failed',
      error: { code: 'cancelled' },
    });
    await page.evaluate(() => window.navigationTest.disposeRaw());
    expect(await page.evaluate((input) => window.navigationTest.rawExecute(input, false), request)).toMatchObject({
      kind: 'failed',
      error: { code: 'disposed' },
    });
    expect(new URL(page.url()).pathname).toBe('/');
  });
  it('opens a real new tab without giving it an opener', async () => {
    const opened = context.waitForEvent('page');
    await page.click('#external');
    const tab = await opened;
    await tab.waitForLoadState();
    expect(new URL(tab.url()).pathname).toBe('/opened');
    expect(await tab.evaluate(() => window.opener === null)).toBe(true);
    expect(new URL(page.url()).pathname).toBe('/');
  });
  it('executes explicit external assign and replace through the real Location primitive', async () => {
    for (const mode of ['assign', 'replace'] as const) {
      await page.evaluate(
        (value) => window.navigationTest.navigate({ kind: 'external', mode: value, href: `/external-${value}` }),
        mode,
      );
      await page.waitForURL(`**/external-${mode}`);
      await page.waitForSelector('#billing');
      expect(new URL(page.url()).pathname).toBe(`/external-${mode}`);
    }
  });
  it('installs a real beforeunload guard only while dirty and removes it on disposal', async () => {
    await page.click('#guard'); // Native dialogs require user activation.
    const dialog = page.waitForEvent('dialog');
    await page.evaluate(() => {
      location.href = '/leave';
    });
    const prompt = await dialog;
    expect(prompt.type()).toBe('beforeunload');
    await prompt.dismiss();
    expect(new URL(page.url()).pathname).toBe('/');
    const cdp = await context.newCDPSession(page);
    const remote = await cdp.send('Runtime.evaluate', { expression: 'window' });
    const id = remote.result.objectId;
    if (!id) throw new Error('Window object was not available to the browser test.');
    const before = await cdp.send('DOMDebugger.getEventListeners', { objectId: id });
    expect(before.listeners.some((listener) => listener.type === 'beforeunload')).toBe(true);
    await page.evaluate(() => window.navigationTest.dispose());
    const after = await cdp.send('DOMDebugger.getEventListeners', { objectId: id });
    expect(
      after.listeners.filter((listener) => ['beforeunload', 'popstate', 'hashchange'].includes(listener.type)),
    ).toEqual([]);
    await page.goto(`${base}/leave`);
    expect(new URL(page.url()).pathname).toBe('/leave');
    await cdp.detach();
  });
});
