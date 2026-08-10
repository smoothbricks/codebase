import { describe, expect, it } from 'bun:test';
import { CloudflareApiError, CloudflareRestClient } from './cloudflare.js';

type CloudflareFetcher = NonNullable<ConstructorParameters<typeof CloudflareRestClient>[2]>;

function jsonFetcher(body: unknown, status = 200): CloudflareFetcher {
  return async () =>
    new Response(JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/json' },
    });
}

describe('CloudflareRestClient', () => {
  it('accepts nullable diagnostics in successful API envelopes', async () => {
    const client = new CloudflareRestClient(
      'account-id',
      'api-token',
      jsonFetcher({
        result: [
          {
            id: 'domain-id',
            hostname: 'app.pr45.example.com',
            service: 'app-pr45',
          },
        ],
        success: true,
        errors: null,
        messages: null,
        result_info: {
          page: 1,
          per_page: 1000,
          count: 1,
          total_count: 1,
        },
      }),
    );

    await expect(client.listWorkerDomains()).resolves.toEqual([
      {
        id: 'domain-id',
        hostname: 'app.pr45.example.com',
        service: 'app-pr45',
      },
    ]);
  });

  it('preserves structured Cloudflare API failures', async () => {
    const client = new CloudflareRestClient(
      'account-id',
      'api-token',
      jsonFetcher(
        {
          result: null,
          success: false,
          errors: [{ code: 10000, message: 'Authentication error' }],
          messages: [],
        },
        403,
      ),
    );

    try {
      await client.listWorkerDomains();
      throw new Error('expected listWorkerDomains to fail');
    } catch (error) {
      expect(error).toBeInstanceOf(CloudflareApiError);
      expect(error).toMatchObject({
        message:
          'Cloudflare API /accounts/account-id/workers/domains?per_page=1000&page=1 failed: Authentication error',
        status: 403,
        codes: [10000],
      });
    }
  });
});
