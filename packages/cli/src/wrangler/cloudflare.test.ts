import { describe, expect, it } from 'bun:test';
import { CloudflareApiError, CloudflareRestClient, type D1DatabaseRecord, type R2Bucket } from './cloudflare.js';

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

/** Answers each call with the next queued body, repeating the last one, and records every request. */
function pageFetcher(bodies: unknown[]): { fetcher: CloudflareFetcher; calls: string[] } {
  const calls: string[] = [];
  let index = 0;
  return {
    calls,
    fetcher: async (input, init) => {
      calls.push(`${init?.method ?? 'GET'} ${input}`);
      const body = bodies[Math.min(index, bodies.length - 1)];
      index += 1;
      return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } });
    },
  };
}

function buckets(count: number, from = 0): R2Bucket[] {
  return Array.from({ length: count }, (_, index) => ({ name: `bucket-${from + index}` }));
}

function databases(count: number, from = 0): D1DatabaseRecord[] {
  return Array.from({ length: count }, (_, index) => ({ uuid: `db-${from + index}`, name: `site-${from + index}-db` }));
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

const V4 = 'https://api.cloudflare.com/client/v4';

describe('CloudflareRestClient page-numbered listings', () => {
  it('reads every page when Cloudflare omits total_pages', async () => {
    // KV, D1 and others answer with count/page/per_page/total_count only. Treating a missing
    // total_pages as "one page" hid every database past the first page.
    const { fetcher, calls } = pageFetcher([
      { success: true, result: databases(1000), result_info: { count: 1000, page: 1, per_page: 1000 } },
      { success: true, result: databases(1, 1000), result_info: { count: 1, page: 2, per_page: 1000 } },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    const records = await client.listD1Databases();

    expect(records).toHaveLength(1001);
    expect(records[1000]).toEqual({ uuid: 'db-1000', name: 'site-1000-db' });
    expect(calls).toEqual([
      `GET ${V4}/accounts/account-1/d1/database?per_page=1000&page=1`,
      `GET ${V4}/accounts/account-1/d1/database?per_page=1000&page=2`,
    ]);
  });

  it('keeps reading a short page while total_count promises more', async () => {
    const { fetcher, calls } = pageFetcher([
      { success: true, result: [{ id: 'kv-1', title: 'site-pr7-cache' }], result_info: { count: 1, total_count: 2 } },
      {
        success: true,
        result: [{ id: 'kv-2', title: 'site-pr7-sessions' }],
        result_info: { count: 1, total_count: 2 },
      },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listKvNamespaces()).resolves.toEqual([
      { id: 'kv-1', title: 'site-pr7-cache' },
      { id: 'kv-2', title: 'site-pr7-sessions' },
    ]);
    expect(calls).toHaveLength(2);
  });

  it('stops at a short page when Cloudflare reports no counts at all', async () => {
    const { fetcher, calls } = pageFetcher([{ success: true, result: [{ id: 'kv-1', title: 'site-pr7-cache' }] }]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listKvNamespaces()).resolves.toHaveLength(1);
    expect(calls).toHaveLength(1);
  });

  it('honours total_pages where Cloudflare sends it', async () => {
    const record = { id: 'dns-1', name: 'pr7.example.test', type: 'CNAME', content: 'worker.example.test' };
    const { fetcher, calls } = pageFetcher([
      { success: true, result: [record], result_info: { page: 1, per_page: 1000, total_pages: 2, total_count: 2 } },
      { success: true, result: [{ ...record, id: 'dns-2' }], result_info: { page: 2, per_page: 1000, total_pages: 2 } },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listDnsRecords('zone-1')).resolves.toHaveLength(2);
    expect(calls).toEqual([
      `GET ${V4}/zones/zone-1/dns_records?per_page=1000&page=1`,
      `GET ${V4}/zones/zone-1/dns_records?per_page=1000&page=2`,
    ]);
  });

  it('requests zones at the page size that endpoint accepts', async () => {
    // `GET /zones` rejects per_page above 50, so the shared 1000 would fail the whole listing.
    const { fetcher, calls } = pageFetcher([{ success: true, result: [{ id: 'zone-1', name: 'example.test' }] }]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listZones()).resolves.toHaveLength(1);
    expect(calls).toEqual([`GET ${V4}/zones?per_page=50&page=1`]);
  });

  it('refuses a listing that never reports its end', async () => {
    const { fetcher, calls } = pageFetcher([
      {
        success: true,
        result: Array.from({ length: 50 }, (_, i) => ({ id: `zone-${i}`, name: `z${i}.example.test` })),
      },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listZones()).rejects.toThrow(/past 1000 pages without reporting an end/);
    expect(calls).toHaveLength(1000);
  });
});

describe('CloudflareRestClient cursor listings', () => {
  it('follows the R2 bucket cursor instead of page numbers', async () => {
    const { fetcher, calls } = pageFetcher([
      { success: true, result: { buckets: buckets(1000) }, result_info: { cursor: 'cursor-2', per_page: 1000 } },
      { success: true, result: { buckets: buckets(2, 1000) }, result_info: { per_page: 1000 } },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    const found = await client.listR2Buckets();

    expect(found).toHaveLength(1002);
    expect(found[1001]).toEqual({ name: 'bucket-1001' });
    expect(calls).toEqual([
      `GET ${V4}/accounts/account-1/r2/buckets?per_page=1000`,
      `GET ${V4}/accounts/account-1/r2/buckets?per_page=1000&cursor=cursor-2`,
    ]);
  });

  it('refuses a cursor that repeats itself', async () => {
    const { fetcher } = pageFetcher([
      { success: true, result: { buckets: buckets(1000) }, result_info: { cursor: 'stuck', per_page: 1000 } },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listR2Buckets()).rejects.toThrow(/repeated one pagination cursor/);
  });

  it('refuses a listing that claims truncation without a cursor', async () => {
    // More objects exist and nothing says where to resume: a partial key list would delete the
    // bucket's visible objects and then fail on the bucket itself.
    const { fetcher } = pageFetcher([
      { success: true, result: { objects: [{ key: 'a/1.json' }] }, result_info: { is_truncated: true } },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listR2Objects('site-pr7-uploads')).rejects.toThrow(/truncated without a cursor/);
  });

  it('trusts is_truncated on a short object page and stops when it clears', async () => {
    const { fetcher, calls } = pageFetcher([
      {
        success: true,
        result: { objects: [{ key: 'a/1.json' }] },
        result_info: { cursor: 'cursor-2', is_truncated: true },
      },
      {
        success: true,
        result: { objects: [{ key: 'b/2.json' }] },
        result_info: { cursor: 'cursor-3', is_truncated: false },
      },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listR2Objects('site-pr7-uploads')).resolves.toEqual(['a/1.json', 'b/2.json']);
    expect(calls).toEqual([
      `GET ${V4}/accounts/account-1/r2/buckets/site-pr7-uploads/objects?per_page=1000`,
      `GET ${V4}/accounts/account-1/r2/buckets/site-pr7-uploads/objects?per_page=1000&cursor=cursor-2`,
    ]);
  });
});

describe('CloudflareRestClient unpaginated listings', () => {
  it('asks the Workers script and route endpoints once, without page parameters', async () => {
    const { fetcher, calls } = pageFetcher([
      { success: true, result: [{ id: 'site-pr7' }] },
      { success: true, result: [{ id: 'route-1', pattern: 'pr7.example.test/*', script: 'site-pr7' }] },
    ]);
    const client = new CloudflareRestClient('account-1', 'token', fetcher);

    await expect(client.listWorkerScripts()).resolves.toEqual([{ id: 'site-pr7' }]);
    await expect(client.listWorkerRoutes('zone-1')).resolves.toHaveLength(1);
    expect(calls).toEqual([`GET ${V4}/accounts/account-1/workers/scripts`, `GET ${V4}/zones/zone-1/workers/routes`]);
  });
});

describe('CloudflareRestClient reads that carry nothing', () => {
  it('refuses an empty 2xx body on a listing instead of reporting an empty account', async () => {
    const client = new CloudflareRestClient('account-1', 'token', rawFetcher(null, 200));
    await expect(client.listD1Databases()).rejects.toThrow(/empty response body/);
  });

  it('refuses a bare null 2xx body on a listing', async () => {
    const client = new CloudflareRestClient('account-1', 'token', rawFetcher('null', 200));
    await expect(client.listKvNamespaces()).rejects.toThrow(/empty response body/);
  });

  it('refuses an empty 2xx body on a create instead of inventing a record', async () => {
    const client = new CloudflareRestClient('account-1', 'token', rawFetcher('', 200));
    await expect(client.createD1Database('site-pr7-db')).rejects.toThrow(/empty response body/);
    await expect(client.createKvNamespace('site-pr7-cache')).rejects.toThrow(/empty response body/);
  });

  it('keeps the API token out of a failure message', async () => {
    const client = new CloudflareRestClient(
      'account-1',
      'super-secret-token',
      jsonFetcher({ success: false, errors: [{ code: 10000, message: 'Authentication error' }] }, 403),
    );

    const error = await client.listD1Databases().catch((thrown: unknown) => thrown);

    expect(error).toBeInstanceOf(CloudflareApiError);
    expect(String(error)).not.toContain('super-secret-token');
    expect(String(error)).toContain('/accounts/account-1/d1/database');
  });
});
