/**
 * Temporary HTTP server for the GitHub App manifest flow callback.
 * Serves the auto-submit form page and receives the redirect with the temporary code.
 */

import { createServer, type Server } from 'node:http';

interface CallbackServerOptions {
  /** HTML page to serve on GET / (the auto-submitting manifest form) */
  manifestPage: string;
  /** Timeout in milliseconds before rejecting (default: 5 minutes) */
  timeoutMs?: number;
}

interface CallbackServerResult {
  /** Port the server is listening on */
  port: number;
  /** Returns a promise that resolves with the callback code */
  waitForCode: () => Promise<string>;
  /** Manually close the server */
  close: () => void;
  /** Update the manifest page HTML (useful when port is needed to build the manifest) */
  setManifestPage: (html: string) => void;
}

const DEFAULT_TIMEOUT_MS = 300_000; // 5 minutes

/**
 * Start a temporary HTTP server to handle the GitHub App manifest flow.
 *
 * - GET / serves the manifest HTML page (auto-submits to GitHub)
 * - GET /callback?code=XYZ receives the redirect after app creation
 *
 * The server auto-closes after receiving the callback code.
 */
export function startCallbackServer(options: CallbackServerOptions): Promise<CallbackServerResult> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  return new Promise((resolveServer) => {
    let resolveCode: (code: string) => void;
    let rejectCode: (error: Error) => void;
    let settled = false;
    let currentManifestPage = options.manifestPage;

    const codePromise = new Promise<string>((resolve, reject) => {
      resolveCode = resolve;
      rejectCode = reject;
    });

    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let server: Server;

    const cleanup = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      try {
        server.close();
      } catch {
        // Already closed
      }
    };

    server = createServer((req, res) => {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const pathname = url.pathname;

      if (pathname === '/' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(currentManifestPage);
        return;
      }

      if (pathname === '/callback') {
        const code = url.searchParams.get('code');
        if (code) {
          res.writeHead(200, { 'Content-Type': 'text/html' });
          res.end(
            '<html><body><h1>GitHub App created!</h1><p>You can close this tab and return to the terminal.</p></body></html>',
          );

          if (!settled) {
            settled = true;
            resolveCode?.(code);
            // Auto-close after a brief delay to ensure response is sent
            setTimeout(() => cleanup(), 100);
          }
          return;
        }
      }

      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
    });

    server.listen(0, () => {
      const addr = server.address();
      const port = typeof addr === 'object' && addr !== null ? addr.port : 0;

      resolveServer({
        port,
        waitForCode: () => {
          // Start timeout when waitForCode is called
          if (!settled && !timeoutId) {
            timeoutId = setTimeout(() => {
              if (!settled) {
                settled = true;
                rejectCode?.(new Error('Timeout waiting for GitHub App creation callback'));
                cleanup();
              }
            }, timeoutMs);
          }
          return codePromise;
        },
        close: cleanup,
        setManifestPage: (html: string) => {
          currentManifestPage = html;
        },
      });
    });
  });
}
