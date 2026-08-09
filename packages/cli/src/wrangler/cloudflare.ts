import typia from 'typia';
import type { LiveKvNamespace } from './environment.js';

export interface R2Bucket {
  name: string;
}

export interface WorkerScript {
  id: string;
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
}

interface CloudflareEnvelope {
  success: boolean;
  result?: unknown;
  errors?: Array<{ code?: number; message?: string }>;
  result_info?: {
    page?: number;
    total_pages?: number;
    cursor?: string;
    is_truncated?: boolean;
  };
}

const isCloudflareEnvelope = typia.createIs<CloudflareEnvelope>();
const isKvNamespaces = typia.createIs<LiveKvNamespace[]>();
const isR2Buckets = typia.createIs<R2Bucket[]>();
const isWorkerScripts = typia.createIs<WorkerScript[]>();
const isWorkerDomains = typia.createIs<WorkerDomain[]>();
const isCloudflareZones = typia.createIs<CloudflareZone[]>();
const isWorkerRoutes = typia.createIs<WorkerRoute[]>();
const isDnsRecords = typia.createIs<DnsRecord[]>();
const isR2Objects = typia.createIs<Array<{ key: string }>>();
const isR2BucketPage = typia.createIs<{ buckets: R2Bucket[] }>();
const isR2ObjectPage = typia.createIs<{ objects: Array<{ key: string }> }>();
const isCreatedKvNamespace = typia.createIs<LiveKvNamespace>();

export class CloudflareApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly codes: number[],
  ) {
    super(message);
  }
}

export class CloudflareRestClient implements CloudflareClient {
  private readonly accountPath: string;

  constructor(
    accountId: string,
    private readonly apiToken: string,
    private readonly fetcher: typeof fetch = fetch,
  ) {
    if (!accountId || !apiToken) {
      throw new Error('CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN are required.');
    }
    this.accountPath = `/accounts/${encodeURIComponent(accountId)}`;
  }

  listKvNamespaces(): Promise<LiveKvNamespace[]> {
    return this.listPageItems(`${this.accountPath}/storage/kv/namespaces`, isKvNamespaces);
  }

  async createKvNamespace(title: string): Promise<LiveKvNamespace> {
    const result = await this.result(`${this.accountPath}/storage/kv/namespaces`, {
      method: 'POST',
      body: JSON.stringify({ title }),
    });
    if (!isCreatedKvNamespace(result)) {
      throw new Error(`Cloudflare returned an invalid KV namespace after creating ${title}.`);
    }
    return result;
  }

  async deleteKvNamespace(id: string): Promise<void> {
    await this.result(`${this.accountPath}/storage/kv/namespaces/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  async listR2Buckets(): Promise<R2Bucket[]> {
    const buckets: R2Bucket[] = [];
    let page = 1;
    for (;;) {
      const envelope = await this.request(`${this.accountPath}/r2/buckets?per_page=1000&page=${page}`);
      let rows: R2Bucket[];
      if (isR2Buckets(envelope.result)) rows = envelope.result;
      else if (isR2BucketPage(envelope.result)) rows = envelope.result.buckets;
      else throw new Error('Cloudflare returned an invalid R2 bucket listing.');
      buckets.push(...rows);
      const totalPages = envelope.result_info?.total_pages ?? page;
      if (page >= totalPages) break;
      page += 1;
    }
    return buckets;
  }

  async createR2Bucket(name: string): Promise<void> {
    await this.result(`${this.accountPath}/r2/buckets`, { method: 'POST', body: JSON.stringify({ name }) });
  }

  async listR2Objects(bucket: string): Promise<string[]> {
    const keys: string[] = [];
    let cursor: string | undefined;
    do {
      const query = new URLSearchParams({ per_page: '1000' });
      if (cursor) query.set('cursor', cursor);
      const envelope = await this.request(
        `${this.accountPath}/r2/buckets/${encodeURIComponent(bucket)}/objects?${query.toString()}`,
      );
      let rows: Array<{ key: string }>;
      if (isR2Objects(envelope.result)) rows = envelope.result;
      else if (isR2ObjectPage(envelope.result)) rows = envelope.result.objects;
      else throw new Error(`Cloudflare returned an invalid object listing for R2 bucket ${bucket}.`);
      keys.push(...rows.map((row) => row.key));
      cursor = envelope.result_info?.is_truncated === true ? envelope.result_info.cursor : undefined;
    } while (cursor);
    return keys;
  }

  async deleteR2Object(bucket: string, key: string): Promise<void> {
    const objectPath = key.split('/').map(encodeURIComponent).join('/');
    await this.result(`${this.accountPath}/r2/buckets/${encodeURIComponent(bucket)}/objects/${objectPath}`, {
      method: 'DELETE',
    });
  }

  async deleteR2Bucket(name: string): Promise<void> {
    await this.result(`${this.accountPath}/r2/buckets/${encodeURIComponent(name)}`, { method: 'DELETE' });
  }

  listWorkerScripts(): Promise<WorkerScript[]> {
    return this.listPageItems(`${this.accountPath}/workers/scripts`, isWorkerScripts);
  }

  async deleteWorkerScript(name: string): Promise<void> {
    await this.result(`${this.accountPath}/workers/scripts/${encodeURIComponent(name)}`, { method: 'DELETE' });
  }

  listWorkerDomains(): Promise<WorkerDomain[]> {
    return this.listPageItems(`${this.accountPath}/workers/domains`, isWorkerDomains);
  }

  async createWorkerDomain(hostname: string, workerName: string, zoneId: string): Promise<void> {
    await this.result(`${this.accountPath}/workers/domains`, {
      method: 'PUT',
      body: JSON.stringify({ hostname, service: workerName, zone_id: zoneId }),
    });
  }

  async deleteWorkerDomain(id: string): Promise<void> {
    await this.result(`${this.accountPath}/workers/domains/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  listZones(): Promise<CloudflareZone[]> {
    return this.listPageItems('/zones', isCloudflareZones);
  }

  listWorkerRoutes(zoneId: string): Promise<WorkerRoute[]> {
    return this.listPageItems(`/zones/${encodeURIComponent(zoneId)}/workers/routes`, isWorkerRoutes);
  }

  async createWorkerRoute(zoneId: string, pattern: string, workerName: string): Promise<void> {
    await this.result(`/zones/${encodeURIComponent(zoneId)}/workers/routes`, {
      method: 'POST',
      body: JSON.stringify({ pattern, script: workerName }),
    });
  }

  async deleteWorkerRoute(zoneId: string, routeId: string): Promise<void> {
    await this.result(`/zones/${encodeURIComponent(zoneId)}/workers/routes/${encodeURIComponent(routeId)}`, {
      method: 'DELETE',
    });
  }

  listDnsRecords(zoneId: string): Promise<DnsRecord[]> {
    return this.listPageItems(`/zones/${encodeURIComponent(zoneId)}/dns_records`, isDnsRecords);
  }

  async createDnsRecord(zoneId: string, name: string, content: string): Promise<void> {
    await this.result(`/zones/${encodeURIComponent(zoneId)}/dns_records`, {
      method: 'POST',
      body: JSON.stringify({ type: 'CNAME', name, content, proxied: true }),
    });
  }

  async deleteDnsRecord(zoneId: string, recordId: string): Promise<void> {
    await this.result(`/zones/${encodeURIComponent(zoneId)}/dns_records/${encodeURIComponent(recordId)}`, {
      method: 'DELETE',
    });
  }

  private async listPageItems<T>(path: string, isItems: (value: unknown) => value is T[]): Promise<T[]> {
    const items: T[] = [];
    let page = 1;
    for (;;) {
      const separator = path.includes('?') ? '&' : '?';
      const envelope = await this.request(`${path}${separator}per_page=1000&page=${page}`);
      if (!isItems(envelope.result)) {
        throw new Error(`Cloudflare returned an invalid paginated result for ${path}.`);
      }
      items.push(...envelope.result);
      const totalPages = envelope.result_info?.total_pages ?? page;
      if (page >= totalPages) break;
      page += 1;
    }
    return items;
  }

  private async result(path: string, init?: RequestInit): Promise<unknown> {
    return (await this.request(path, init)).result;
  }

  private async request(path: string, init: RequestInit = {}): Promise<CloudflareEnvelope> {
    const response = await this.fetcher(`https://api.cloudflare.com/client/v4${path}`, {
      ...init,
      headers: {
        Authorization: `Bearer ${this.apiToken}`,
        'Content-Type': 'application/json',
        ...init.headers,
      },
    });
    const body: unknown = await response.json();
    if (!isCloudflareEnvelope(body)) {
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
}
