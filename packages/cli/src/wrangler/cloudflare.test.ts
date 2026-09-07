import { describe, expect, it } from 'bun:test';
import { CloudflareApiError, CloudflareRestClient, type D1DatabaseRecord } from './cloudflare.js';

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

describe('CloudflareRestClient D1', () => {
  it('lists databases from the paginated envelope', async () => {
    const client = new CloudflareRestClient(
      'account-1',
      'token',
      jsonFetcher({
        success: true,
        result: [{ uuid: 'db-1', name: 'site-staging-db', version: 'production' }],
        result_info: { count: 1, page: 1, per_page: 1000, total_count: 1 },
      }),
    );
    // Hoisted: TypeScript's excess-property check rejects the extra field in an inline
    // literal, and typia's createIs keeps it at runtime.
    const expected: D1DatabaseRecord & { version: string } = {
      uuid: 'db-1',
      name: 'site-staging-db',
      version: 'production',
    };
    await expect(client.listD1Databases()).resolves.toEqual([expected]);
  });

  it('creates a database and returns its record', async () => {
    const client = new CloudflareRestClient(
      'account-1',
      'token',
      jsonFetcher({ success: true, result: { uuid: 'db-2', name: 'site-pr7-db' } }),
    );
    await expect(client.createD1Database('site-pr7-db')).resolves.toEqual({ uuid: 'db-2', name: 'site-pr7-db' });
  });

  it('deletes a database by uuid', async () => {
    const calls: string[] = [];
    const client = new CloudflareRestClient('account-1', 'token', async (input, init) => {
      calls.push(`${init?.method ?? 'GET'} ${input}`);
      return new Response(JSON.stringify({ success: true, result: null }), { status: 200 });
    });
    await client.deleteD1Database('db-2');
    expect(calls).toEqual(['DELETE https://api.cloudflare.com/client/v4/accounts/account-1/d1/database/db-2']);
  });
});
