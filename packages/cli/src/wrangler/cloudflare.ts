import typia from 'typia';
import type { LiveKvNamespace } from './stage.js';

export interface R2Bucket {
  name: string;
}

export interface WorkerScript {
  id: string;
}

/** One secret bound to a Worker. Cloudflare answers names and types only; a value is never readable. */
export interface WorkerSecret {
  name: string;
}

export interface WorkerRoute {
  id: string;
  pattern: string;
  script?: string;
}

export interface WorkerDomain {
  id: string;
  hostname: string;
  service?: string;
}

export interface CloudflareZone {
  id: string;
  name: string;
}

export interface DnsRecord {
  id: string;
  name: string;
  type: string;
  content: string;
  proxied?: boolean;
}

export interface D1DatabaseRecord {
  uuid: string;
  name: string;
}

export interface CloudflareClient {
  listKvNamespaces(): Promise<LiveKvNamespace[]>;
  createKvNamespace(title: string): Promise<LiveKvNamespace>;
  deleteKvNamespace(id: string): Promise<void>;
  listR2Buckets(): Promise<R2Bucket[]>;
  createR2Bucket(name: string): Promise<void>;
  listR2Objects(bucket: string): Promise<string[]>;
  deleteR2Object(bucket: string, key: string): Promise<void>;
  deleteR2Bucket(name: string): Promise<void>;
  listWorkerScripts(): Promise<WorkerScript[]>;
  /** Names only; the Worker's stored values are write-only from outside. */
  listWorkerSecrets(workerName: string): Promise<string[]>;
  deleteWorkerScript(name: string): Promise<void>;
  listWorkerDomains(): Promise<WorkerDomain[]>;
  createWorkerDomain(hostname: string, workerName: string, zoneId: string): Promise<void>;
  deleteWorkerDomain(id: string): Promise<void>;
  listZones(): Promise<CloudflareZone[]>;
  listWorkerRoutes(zoneId: string): Promise<WorkerRoute[]>;
  createWorkerRoute(zoneId: string, pattern: string, workerName: string): Promise<void>;
  deleteWorkerRoute(zoneId: string, routeId: string): Promise<void>;
  listDnsRecords(zoneId: string): Promise<DnsRecord[]>;
  createDnsRecord(zoneId: string, name: string, content: string): Promise<void>;
  deleteDnsRecord(zoneId: string, recordId: string): Promise<void>;
  listD1Databases(): Promise<D1DatabaseRecord[]>;
  createD1Database(name: string): Promise<D1DatabaseRecord>;
  deleteD1Database(uuid: string): Promise<void>;
}

/**
 * The end-of-listing signals a `result_info` can carry. Cloudflare also echoes `page`, `per_page`
 * and `count`, which no caller here needs: page-numbered endpoints are bounded by `total_pages`
 * where they send it and by `total_count` where they do not, and R2 paginates by cursor instead.
 */
interface CloudflareResultInfo {
  total_count?: number;
  total_pages?: number;
  cursor?: string;
  is_truncated?: boolean;
}

interface CloudflareEnvelope {
  success: boolean;
  result?: unknown;
  errors?: Array<{ code?: number; message?: string }> | null;
  messages?: Array<{ code?: number; message?: string }> | null;
  result_info?: CloudflareResultInfo;
}

const parseCloudflareEnvelope = typia.json.createIsParse<CloudflareEnvelope>();
/** A body with nothing to read: empty, or the bare `null` some endpoints answer with. */
const EMPTY_BODY = /^\s*(?:null)?\s*$/;
/** Cloudflare rejects a larger page outright; KV, R2, DNS and D1 all document 1000 as legal. */
const PAGE_SIZE = 1000;
/** `GET /zones` caps `per_page` at 50, so the shared page size would be a 400. */
const ZONE_PAGE_SIZE = 50;
/** A listing that never reports its end is a broken listing; refuse rather than page forever. */
const MAX_LIST_PAGES = 1000;
const isKvNamespaces = typia.createIs<LiveKvNamespace[]>();
const isR2Buckets = typia.createIs<R2Bucket[]>();
const isWorkerScripts = typia.createIs<WorkerScript[]>();
const isWorkerSecrets = typia.createIs<WorkerSecret[]>();
const isWorkerDomains = typia.createIs<WorkerDomain[]>();
const isCloudflareZones = typia.createIs<CloudflareZone[]>();
const isWorkerRoutes = typia.createIs<WorkerRoute[]>();
const isDnsRecords = typia.createIs<DnsRecord[]>();
const isD1Databases = typia.createIs<D1DatabaseRecord[]>();
const isCreatedD1Database = typia.createIs<D1DatabaseRecord>();
const isR2Objects = typia.createIs<Array<{ key: string }>>();
const isR2BucketPage = typia.createIs<{ buckets: R2Bucket[] }>();
const isR2ObjectPage = typia.createIs<{ objects: Array<{ key: string }> }>();
const isCreatedKvNamespace = typia.createIs<LiveKvNamespace>();

/** What one page's `result_info` says about the rest of a page-numbered listing. */
type PageVerdict = 'more' | 'done' | 'contradiction';

/**
 * Where the listing stands after `page`. `total_pages` is authoritative where Cloudflare sends it
 * (DNS records, Workers domains); KV namespaces and D1 databases report only `total_count`, and
 * R2-style endpoints report neither — so fall back to that count and then to a short page, which
 * only the final page can be. Trusting a missing `total_pages` as "one page" silently truncated
 * every account past its first page.
 *
 * A page with no rows at all while the metadata still promises more is a contradiction, not an
 * ending: the listing was torn, by a concurrent change or by the API itself. Say so instead of
 * returning the rows read so far, which a caller planning deletions would read as the whole truth.
 */
function pageVerdict(
  page: number,
  rows: number,
  collected: number,
  perPage: number,
  info: CloudflareResultInfo | undefined,
): PageVerdict {
  if (info?.total_pages !== undefined) {
    if (page >= info.total_pages) return 'done';
    return rows === 0 ? 'contradiction' : 'more';
  }
  if (info?.total_count !== undefined && collected < info.total_count) {
    return rows === 0 ? 'contradiction' : 'more';
  }
  if (rows === 0) return 'done';
  return rows === perPage ? 'more' : 'done';
}

export class CloudflareApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly codes: number[],
  ) {
    super(message);
  }
}

type CloudflareFetcher = (input: string, init?: RequestInit) => Promise<Response>;

export class CloudflareRestClient implements CloudflareClient {
  private readonly accountPath: string;

  constructor(
    accountId: string,
    private readonly apiToken: string,
    private readonly fetcher: CloudflareFetcher = fetch,
  ) {
    if (!accountId || !apiToken) {
      throw new Error('CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN are required.');
    }
    this.accountPath = `/accounts/${encodeURIComponent(accountId)}`;
  }

  listKvNamespaces(): Promise<LiveKvNamespace[]> {
    return this.listPages(`${this.accountPath}/storage/kv/namespaces`, isKvNamespaces, PAGE_SIZE);
  }

  async createKvNamespace(title: string): Promise<LiveKvNamespace> {
    const { result } = await this.request(`${this.accountPath}/storage/kv/namespaces`, {
      method: 'POST',
      body: JSON.stringify({ title }),
    });
    if (!isCreatedKvNamespace(result)) {
      throw new Error(`Cloudflare returned an invalid KV namespace after creating ${title}.`);
    }
    return result;
  }

  deleteKvNamespace(id: string): Promise<void> {
    return this.mutate(`${this.accountPath}/storage/kv/namespaces/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  listR2Buckets(): Promise<R2Bucket[]> {
    // R2 paginates by cursor: a `page` parameter is ignored, so page numbers would re-read the
    // first 1000 buckets forever or stop there. Either a bare array or a `buckets` page arrives.
    return this.listCursor(`${this.accountPath}/r2/buckets`, (result) =>
      isR2Buckets(result) ? result : isR2BucketPage(result) ? result.buckets : undefined,
    );
  }

  createR2Bucket(name: string): Promise<void> {
    return this.mutate(`${this.accountPath}/r2/buckets`, { method: 'POST', body: JSON.stringify({ name }) });
  }

  async listR2Objects(bucket: string): Promise<string[]> {
    const objects = await this.listCursor(
      `${this.accountPath}/r2/buckets/${encodeURIComponent(bucket)}/objects`,
      (result) => (isR2Objects(result) ? result : isR2ObjectPage(result) ? result.objects : undefined),
    );
    return objects.map((object) => object.key);
  }

  deleteR2Object(bucket: string, key: string): Promise<void> {
    // The endpoint requires literal slashes in an object key and percent-encoding everywhere else.
    const objectPath = key.split('/').map(encodeURIComponent).join('/');
    return this.mutate(`${this.accountPath}/r2/buckets/${encodeURIComponent(bucket)}/objects/${objectPath}`, {
      method: 'DELETE',
    });
  }

  deleteR2Bucket(name: string): Promise<void> {
    return this.mutate(`${this.accountPath}/r2/buckets/${encodeURIComponent(name)}`, { method: 'DELETE' });
  }

  listWorkerScripts(): Promise<WorkerScript[]> {
    // `workers/scripts` takes no pagination parameters: the account's scripts arrive in one answer.
    return this.listOnce(`${this.accountPath}/workers/scripts`, isWorkerScripts);
  }

  async listWorkerSecrets(workerName: string): Promise<string[]> {
    // Unpaginated, like the script listing itself. A Worker that does not exist answers 404 rather
    // than an empty list, so the caller must establish that the Worker is there before asking.
    const secrets = await this.listOnce(
      `${this.accountPath}/workers/scripts/${encodeURIComponent(workerName)}/secrets`,
      isWorkerSecrets,
    );
    return secrets.map((secret) => secret.name);
  }

  deleteWorkerScript(name: string): Promise<void> {
    return this.mutate(`${this.accountPath}/workers/scripts/${encodeURIComponent(name)}`, { method: 'DELETE' });
  }

  listWorkerDomains(): Promise<WorkerDomain[]> {
    return this.listPages(`${this.accountPath}/workers/domains`, isWorkerDomains, PAGE_SIZE);
  }

  createWorkerDomain(hostname: string, workerName: string, zoneId: string): Promise<void> {
    return this.mutate(`${this.accountPath}/workers/domains`, {
      method: 'PUT',
      body: JSON.stringify({ hostname, service: workerName, zone_id: zoneId }),
    });
  }

  deleteWorkerDomain(id: string): Promise<void> {
    return this.mutate(`${this.accountPath}/workers/domains/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  listZones(): Promise<CloudflareZone[]> {
    return this.listPages('/zones', isCloudflareZones, ZONE_PAGE_SIZE);
  }

  listWorkerRoutes(zoneId: string): Promise<WorkerRoute[]> {
    // A zone's route table is unpaginated as well.
    return this.listOnce(`/zones/${encodeURIComponent(zoneId)}/workers/routes`, isWorkerRoutes);
  }

  createWorkerRoute(zoneId: string, pattern: string, workerName: string): Promise<void> {
    return this.mutate(`/zones/${encodeURIComponent(zoneId)}/workers/routes`, {
      method: 'POST',
      body: JSON.stringify({ pattern, script: workerName }),
    });
  }

  deleteWorkerRoute(zoneId: string, routeId: string): Promise<void> {
    return this.mutate(`/zones/${encodeURIComponent(zoneId)}/workers/routes/${encodeURIComponent(routeId)}`, {
      method: 'DELETE',
    });
  }

  listDnsRecords(zoneId: string): Promise<DnsRecord[]> {
    return this.listPages(`/zones/${encodeURIComponent(zoneId)}/dns_records`, isDnsRecords, PAGE_SIZE);
  }

  createDnsRecord(zoneId: string, name: string, content: string): Promise<void> {
    return this.mutate(`/zones/${encodeURIComponent(zoneId)}/dns_records`, {
      method: 'POST',
      body: JSON.stringify({ type: 'CNAME', name, content, proxied: true }),
    });
  }

  deleteDnsRecord(zoneId: string, recordId: string): Promise<void> {
    return this.mutate(`/zones/${encodeURIComponent(zoneId)}/dns_records/${encodeURIComponent(recordId)}`, {
      method: 'DELETE',
    });
  }

  listD1Databases(): Promise<D1DatabaseRecord[]> {
    return this.listPages(`${this.accountPath}/d1/database`, isD1Databases, PAGE_SIZE);
  }

  async createD1Database(name: string): Promise<D1DatabaseRecord> {
    const { result } = await this.request(`${this.accountPath}/d1/database`, {
      method: 'POST',
      body: JSON.stringify({ name }),
    });
    if (!isCreatedD1Database(result)) {
      throw new Error(`Cloudflare returned an invalid D1 database after creating ${name}.`);
    }
    return result;
  }

  deleteD1Database(uuid: string): Promise<void> {
    return this.mutate(`${this.accountPath}/d1/database/${encodeURIComponent(uuid)}`, { method: 'DELETE' });
  }

  /** An endpoint that answers its whole collection at once. */
  private async listOnce<T>(path: string, isItems: (value: unknown) => value is T[]): Promise<T[]> {
    const envelope = await this.request(path);
    if (!isItems(envelope.result)) {
      throw new Error(`Cloudflare returned an invalid listing for ${path}.`);
    }
    return envelope.result;
  }

  private async listPages<T>(path: string, isItems: (value: unknown) => value is T[], perPage: number): Promise<T[]> {
    const items: T[] = [];
    for (let page = 1; page <= MAX_LIST_PAGES; page += 1) {
      const envelope = await this.request(`${path}?per_page=${perPage}&page=${page}`);
      if (!isItems(envelope.result)) {
        throw new Error(`Cloudflare returned an invalid paginated result for ${path}.`);
      }
      const rows = envelope.result;
      items.push(...rows);
      const verdict = pageVerdict(page, rows.length, items.length, perPage, envelope.result_info);
      if (verdict === 'contradiction') {
        throw new Error(`Cloudflare answered page ${page} of ${path} with no rows while reporting more to come.`);
      }
      if (verdict === 'done') return items;
    }
    throw new Error(`Cloudflare paged ${path} past ${MAX_LIST_PAGES} pages without reporting an end.`);
  }

  private async listCursor<T>(path: string, readRows: (result: unknown) => T[] | undefined): Promise<T[]> {
    const items: T[] = [];
    let cursor: string | undefined;
    for (let page = 1; page <= MAX_LIST_PAGES; page += 1) {
      const query = new URLSearchParams({ per_page: String(PAGE_SIZE) });
      if (cursor !== undefined) query.set('cursor', cursor);
      const envelope = await this.request(`${path}?${query.toString()}`);
      const rows = readRows(envelope.result);
      if (!rows) {
        throw new Error(`Cloudflare returned an invalid listing for ${path}.`);
      }
      items.push(...rows);
      const info = envelope.result_info;
      const next = info?.cursor;
      if (info?.is_truncated === true && !next) {
        // The listing says more exists and gives nothing to continue with: report the hole rather
        // than hand back a silently partial listing.
        throw new Error(`Cloudflare reported ${path} as truncated without a cursor to continue.`);
      }
      // `is_truncated` is authoritative both ways where the endpoint sends it (objects), and an
      // empty page there still precedes more keys. Where it does not (buckets), only the last page
      // can be short.
      const more = info?.is_truncated ?? (rows.length === PAGE_SIZE && next !== undefined);
      if (!more || !next) return items;
      if (next === cursor) {
        throw new Error(`Cloudflare repeated one pagination cursor for ${path}, so the listing never ends.`);
      }
      cursor = next;
    }
    throw new Error(`Cloudflare paged ${path} past ${MAX_LIST_PAGES} pages without reporting an end.`);
  }

  /** A read: the payload *is* the envelope, so a 2xx carrying nothing is a failed read. */
  private async request(path: string, init?: RequestInit): Promise<CloudflareEnvelope> {
    const { response, text } = await this.send(path, init);
    if (response.ok && EMPTY_BODY.test(text)) {
      throw new CloudflareApiError(`Cloudflare returned an empty response body for ${path}.`, response.status, []);
    }
    return readEnvelope(path, response, text);
  }

  /**
   * A mutation with nothing to read back. A few endpoints (Workers custom-domain delete among them)
   * answer a successful call with no envelope at all: an empty body or a bare null. On a 2xx that
   * carries nothing to read, the call succeeded.
   */
  private async mutate(path: string, init: RequestInit): Promise<void> {
    const { response, text } = await this.send(path, init);
    if (response.ok && EMPTY_BODY.test(text)) return;
    readEnvelope(path, response, text);
  }

  private async send(path: string, init: RequestInit = {}): Promise<{ response: Response; text: string }> {
    const response = await this.fetcher(`https://api.cloudflare.com/client/v4${path}`, {
      ...init,
      headers: {
        Authorization: `Bearer ${this.apiToken}`,
        'Content-Type': 'application/json',
        ...init.headers,
      },
    });
    return { response, text: await response.text() };
  }
}

/** The envelope, or a failure named by endpoint and Cloudflare's own codes — never by credential. */
function readEnvelope(path: string, response: Response, text: string): CloudflareEnvelope {
  let body: CloudflareEnvelope | null;
  try {
    body = parseCloudflareEnvelope(text);
  } catch {
    // Not JSON at all; the same failure as JSON of the wrong shape.
    body = null;
  }
  if (!body) {
    throw new CloudflareApiError(`Cloudflare returned a malformed response for ${path}.`, response.status, []);
  }
  if (!response.ok || !body.success) {
    const errors = body.errors ?? [];
    const message =
      errors
        .map((error) => error.message)
        .filter(Boolean)
        .join('; ') || `HTTP ${response.status}`;
    throw new CloudflareApiError(
      `Cloudflare API ${path} failed: ${message}`,
      response.status,
      errors.flatMap((error) => error.code ?? []),
    );
  }
  return body;
}
