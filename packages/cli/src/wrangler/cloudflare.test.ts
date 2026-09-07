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

function rawFetcher(body: string | null, status = 200): CloudflareFetcher {
  return async () => new Response(body, { status, headers: { 'Content-Type': 'application/json' } });
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

describe('CloudflareRestClient responses without an envelope', () => {
  it('treats an empty 2xx body as success on a delete', async () => {
    // Workers custom-domain delete answers a successful call without the usual envelope.
    const client = new CloudflareRestClient('account-1', 'token', rawFetcher(null, 200));
    await expect(client.deleteWorkerDomain('domain-1')).resolves.toBeUndefined();
  });

  it('treats a bare null 2xx body as success on a delete', async () => {
    const client = new CloudflareRestClient('account-1', 'token', rawFetcher('null', 200));
    await expect(client.deleteWorkerDomain('domain-1')).resolves.toBeUndefined();
  });

  it('still rejects a non-envelope body on a failed request', async () => {
    const client = new CloudflareRestClient('account-1', 'token', rawFetcher('{"unexpected":true}', 500));
    await expect(client.deleteWorkerDomain('domain-1')).rejects.toThrow(/malformed response/);
  });

  it('still rejects an unparseable body', async () => {
    const client = new CloudflareRestClient('account-1', 'token', rawFetcher('<html>', 200));
    await expect(client.deleteWorkerDomain('domain-1')).rejects.toThrow(/malformed response/);
  });

  it('still rejects a non-envelope object on a successful status', async () => {
    const client = new CloudflareRestClient('account-1', 'token', rawFetcher('{"unexpected":true}', 200));
    await expect(client.deleteWorkerDomain('domain-1')).rejects.toThrow(/malformed response/);
  });
});
