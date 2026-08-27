import { lstatSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import {
  classifyMemoryBackendError,
  HindsightMemoryBackend,
  MEMORY_BACKEND_CODECS,
  MemoryBackendError,
  Mem0MemoryBackend,
  OpenVikingMemoryBackend,
  type MemoryBackendProvider,
  type MemoryBackendTransport,
  type MemoryBackendTransportInput,
  type MemoryBackendProviderId,
} from './memory-backend-spi.js';
import { KmMemoryProviderConfigSchema, type KmMemoryProviderConfig } from './provider-spi.js';

export type CredentialResolverResult =
  | { ok: true; kind: 'env' | 'file'; value: string }
  | { ok: false; kind: 'env' | 'file'; reason: 'missing' | 'outside_secret_dir' | 'not_regular_file' | 'insecure_permissions' };

export function resolveMemoryBackendCredential(input: {
  credentialRef: string;
  env?: NodeJS.ProcessEnv;
  secretDir?: string;
}): CredentialResolverResult {
  const env = input.env ?? process.env;
  if (input.credentialRef.startsWith('env:')) {
    const name = input.credentialRef.slice('env:'.length);
    const value = env[name]?.trim();
    return value ? { ok: true, kind: 'env', value } : { ok: false, kind: 'env', reason: 'missing' };
  }
  const file = input.credentialRef.startsWith('file:') ? input.credentialRef.slice('file:'.length) : '';
  const secretDir = resolve(input.secretDir ?? env.BOTMUX_KM_SECRET_DIR?.trim() ?? join(homedir(), '.botmux', 'secrets'));
  const candidate = resolve(file);
  if (!(candidate === secretDir || candidate.startsWith(`${secretDir}/`))) {
    return { ok: false, kind: 'file', reason: 'outside_secret_dir' };
  }
  try {
    const stat = lstatSync(candidate);
    if (!stat.isFile() || stat.isSymbolicLink()) return { ok: false, kind: 'file', reason: 'not_regular_file' };
    if ((stat.mode & 0o077) !== 0) return { ok: false, kind: 'file', reason: 'insecure_permissions' };
    const value = readFileSync(candidate, 'utf8').trim();
    return value ? { ok: true, kind: 'file', value } : { ok: false, kind: 'file', reason: 'missing' };
  } catch {
    return { ok: false, kind: 'file', reason: 'missing' };
  }
}

export class DisabledRealTransport implements MemoryBackendTransport {
  async request(): Promise<unknown> {
    throw new MemoryBackendError('km_memory_backend_real_transport_disabled', { code: 'network_disabled', retryable: false });
  }
}

export type MemoryBackendFixtureScenario =
  | 'success'
  | 'duplicate'
  | 'not_found'
  | 'rate_limit'
  | 'auth_failure'
  | 'malformed_response'
  | 'timeout'
  | 'pagination'
  | 'partial_error';

function fixtureScenarioKey(input: Pick<MemoryBackendTransportInput, 'providerId' | 'operation'>): string {
  return `${input.providerId}:${input.operation}`;
}

function readString(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback;
}

function providerLogicalId(providerId: string, payload: Record<string, unknown>): string {
  return readString(payload.id)
    || readString(payload.document_id)
    || readString(payload.resource_id)
    || readString(payload.backendRef).replace(new RegExp(`^${providerId}:`), '')
    || `${providerId}-fixture`;
}

function providerText(payload: Record<string, unknown>): string {
  return readString(payload.memory) || readString(payload.content) || readString(payload.text);
}

function providerMetadata(payload: Record<string, unknown>): Record<string, unknown> {
  const metadata = payload.metadata ?? payload.attributes;
  return typeof metadata === 'object' && metadata !== null ? metadata as Record<string, unknown> : {};
}

function providerPutResponse(providerId: string, id: string, duplicate = false): Record<string, unknown> {
  if (providerId === 'mem0') return { id, duplicate };
  if (providerId === 'hindsight') return { document_id: id, duplicate };
  return { resource_id: id, duplicate };
}

function providerRetrieveResponse(providerId: string, records: Array<{ backendRef: string; payload: Record<string, unknown> }>, input: MemoryBackendTransportInput): Record<string, unknown> {
  const offset = Number(input.pageCursor ?? (typeof input.payload.cursor === 'string' ? input.payload.cursor : input.payload.page_cursor) ?? 0);
  const limit = Math.max(1, Math.min(Number(input.payload.limit ?? input.payload.top_k ?? 10), 50));
  const page = records.slice(offset, offset + limit);
  const next = offset + limit < records.length ? String(offset + limit) : undefined;
  const map = (record: { backendRef: string; payload: Record<string, unknown> }) => {
    const metadata = providerMetadata(record.payload);
    const id = providerLogicalId(providerId, record.payload);
    const text = providerText(record.payload);
    if (providerId === 'mem0') return { id, memory: text, score: 0.8, metadata };
    if (providerId === 'hindsight') return { document_id: id, content: text, score: 0.8, metadata };
    return { resource_id: id, text, relevance: 0.8, attributes: metadata };
  };
  if (providerId === 'mem0') return { results: page.map(map), ...(next ? { next_cursor: next } : {}) };
  if (providerId === 'hindsight') return { matches: page.map(map), ...(next ? { next_cursor: next } : {}) };
  return { memories: page.map(map), ...(next ? { next_cursor: next } : {}) };
}

function fixtureError(scenario: MemoryBackendFixtureScenario): Record<string, unknown> | undefined {
  if (scenario === 'not_found') return { error: { code: 'not_found', message: 'fixture not found', status: 404, retryable: false } };
  if (scenario === 'rate_limit') return { error: { code: 'rate_limited', message: 'fixture rate limit', status: 429, retryable: true } };
  if (scenario === 'auth_failure') return { error: { code: 'auth_failed', message: 'fixture auth failure', status: 401, retryable: false } };
  return undefined;
}

export class InMemoryMemoryBackendTransport implements MemoryBackendTransport {
  protected readonly records = new Map<string, { providerId: string; backendRef: string; payload: Record<string, unknown> }>();

  async request(input: MemoryBackendTransportInput): Promise<unknown> {
    if (input.operation === 'health') return { ok: true };
    if (input.operation === 'put') {
      const id = providerLogicalId(input.providerId, input.payload) || `${input.providerId}-${this.records.size + 1}`;
      const backendRef = `${input.providerId}:${id}`;
      this.records.set(backendRef, { providerId: input.providerId, backendRef, payload: input.payload });
      return providerPutResponse(input.providerId, id);
    }
    if (input.operation === 'revoke') {
      const ref = String(input.payload.backendRef ?? '');
      this.records.delete(ref);
      return { ok: true };
    }
    const query = String(input.payload.query ?? input.payload.text ?? '').toLowerCase();
    const limit = Number(input.payload.limit ?? input.payload.top_k ?? 10);
    return [...this.records.values()]
      .filter(record => record.providerId === input.providerId)
      .map(record => {
        const text = String(record.payload.memory ?? record.payload.content ?? record.payload.text ?? '');
        return {
          backendRef: record.backendRef,
          memoryId: String(record.payload.id ?? record.payload.document_id ?? record.payload.resource_id ?? ''),
          text,
          score: query && text.toLowerCase().includes(query) ? 1 : 0.1,
          metadata: record.payload,
        };
      })
      .sort((a, b) => b.score - a.score || a.backendRef.localeCompare(b.backendRef))
      .slice(0, Number.isFinite(limit) ? limit : 10);
  }
}

export class FixtureMemoryBackendTransport extends InMemoryMemoryBackendTransport {
  readonly requests: MemoryBackendTransportInput[] = [];

  constructor(private readonly scenarios: Partial<Record<MemoryBackendFixtureScenario | string, MemoryBackendFixtureScenario>> = {}) {
    super();
  }

  private scenario(input: MemoryBackendTransportInput): MemoryBackendFixtureScenario {
    return this.scenarios[fixtureScenarioKey(input)]
      ?? this.scenarios[input.providerId]
      ?? this.scenarios[input.operation]
      ?? this.scenarios.default
      ?? 'success';
  }

  async request(input: MemoryBackendTransportInput): Promise<unknown> {
    this.requests.push(input);
    const scenario = this.scenario(input);
    if (scenario === 'timeout') return new Promise<never>(() => {});
    if (scenario === 'malformed_response') return { malformed: true };
    const error = fixtureError(scenario);
    if (error && input.operation !== 'put') return error;
    if (input.operation === 'health') return { ok: true, provider: input.providerId };
    if (input.operation === 'put') {
      const id = providerLogicalId(input.providerId, input.payload);
      const backendRef = `${input.providerId}:${id}`;
      if (scenario === 'rate_limit' || scenario === 'auth_failure' || scenario === 'not_found') return error;
      this.records.set(backendRef, { providerId: input.providerId, backendRef, payload: input.payload });
      return providerPutResponse(input.providerId, id, scenario === 'duplicate');
    }
    if (input.operation === 'revoke') {
      const ref = readString(input.payload.backendRef);
      if (!this.records.has(ref) && scenario === 'not_found') return fixtureError('not_found');
      this.records.delete(ref);
      return { ok: true };
    }
    const query = String(input.payload.query ?? input.payload.text ?? '').toLowerCase();
    const source = [...this.records.values()].filter(record => record.providerId === input.providerId);
    const records = (scenario === 'pagination' && source.length < 3)
      ? [0, 1, 2].map(index => ({ providerId: input.providerId, backendRef: `${input.providerId}:fixture-${index}`, payload: {
        id: `fixture-${index}`,
        document_id: `fixture-${index}`,
        resource_id: `fixture-${index}`,
        memory: `fixture ${index} ${query}`,
        content: `fixture ${index} ${query}`,
        text: `fixture ${index} ${query}`,
        metadata: { logical_id: `mem-${index}`, scope: 'user', subject: 'u1' },
        attributes: { logical_id: `mem-${index}`, scope: 'user', subject: 'u1' },
      } }))
      : source;
    const response = providerRetrieveResponse(input.providerId, records, scenario === 'pagination'
      ? { ...input, payload: { ...input.payload, limit: Math.min(Number(input.payload.limit ?? input.payload.top_k ?? 10), 2), top_k: Math.min(Number(input.payload.limit ?? input.payload.top_k ?? 10), 2) } }
      : input);
    if (scenario === 'partial_error') {
      return { ...response, partial_errors: [{ status: 429, message: 'fixture partial rate limit' }] };
    }
    return response;
  }
}

export interface MemoryBackendFactoryResult {
  provider?: MemoryBackendProvider;
  credential?: CredentialResolverResult;
  status: 'ready' | 'disabled' | 'credential_missing' | 'real_transport_disabled' | 'unsafe_endpoint';
  endpointPolicy?: MemoryBackendEndpointPolicyResult;
}

export interface MemoryBackendEndpointPolicyResult {
  ok: boolean;
  mode: 'fixture' | 'blocked-real' | 'invalid';
  reason?: 'mock_or_inmemory_only' | 'invalid_url' | 'https_required' | 'loopback_http_only';
}

export function evaluateMemoryBackendEndpointPolicy(endpoint: string): MemoryBackendEndpointPolicyResult {
  try {
    const url = new URL(endpoint);
    if (url.protocol === 'mock:' || url.protocol === 'inmemory:') return { ok: true, mode: 'fixture' };
    if (url.protocol === 'https:') return { ok: false, mode: 'blocked-real', reason: 'mock_or_inmemory_only' };
    if (url.protocol === 'http:' && (url.hostname === 'localhost' || url.hostname === '127.0.0.1')) {
      return { ok: false, mode: 'blocked-real', reason: 'mock_or_inmemory_only' };
    }
    if (url.protocol === 'http:') return { ok: false, mode: 'invalid', reason: 'https_required' };
    return { ok: false, mode: 'invalid', reason: 'mock_or_inmemory_only' };
  } catch {
    return { ok: false, mode: 'invalid', reason: 'invalid_url' };
  }
}

export const MEMORY_BACKEND_ENDPOINT_POLICIES: Readonly<Record<MemoryBackendProviderId, { allowedOfflineProtocols: readonly string[]; realTransport: 'disabled'; tls: 'https-required-for-future-real-transport' }>> = {
  mem0: { allowedOfflineProtocols: ['mock:', 'inmemory:'], realTransport: 'disabled', tls: 'https-required-for-future-real-transport' },
  hindsight: { allowedOfflineProtocols: ['mock:', 'inmemory:'], realTransport: 'disabled', tls: 'https-required-for-future-real-transport' },
  openviking: { allowedOfflineProtocols: ['mock:', 'inmemory:'], realTransport: 'disabled', tls: 'https-required-for-future-real-transport' },
};

function providerFor(providerId: MemoryBackendProviderId, transport: MemoryBackendTransport, options: { timeoutMs?: number; credentialRef?: string }): MemoryBackendProvider {
  return providerId === 'mem0'
    ? new Mem0MemoryBackend(transport, options)
    : providerId === 'hindsight'
      ? new HindsightMemoryBackend(transport, options)
      : new OpenVikingMemoryBackend(transport, options);
}

export function createMemoryBackendProvider(input: {
  config: KmMemoryProviderConfig;
  transport?: MemoryBackendTransport;
  env?: NodeJS.ProcessEnv;
  secretDir?: string;
  allowRealTransport?: boolean;
}): MemoryBackendFactoryResult {
  const config = KmMemoryProviderConfigSchema.parse(input.config);
  if (!config.enabled) return { status: 'disabled' };
  const endpointPolicy = evaluateMemoryBackendEndpointPolicy(config.endpoint);
  if (!endpointPolicy.ok) return { status: 'unsafe_endpoint', endpointPolicy };
  const credential = resolveMemoryBackendCredential({ credentialRef: config.credentialRef, env: input.env, secretDir: input.secretDir });
  if (!credential.ok) return { status: 'credential_missing', credential, endpointPolicy };
  if (config.realTransportEnabled || input.allowRealTransport) {
    return { status: 'real_transport_disabled', credential, endpointPolicy };
  }
  const transport = input.transport ?? new InMemoryMemoryBackendTransport();
  const provider = providerFor(config.providerId, transport, { timeoutMs: config.timeoutMs, credentialRef: config.credentialRef });
  return { status: 'ready', provider, credential, endpointPolicy };
}

export function memoryBackendContractDescriptors(): Array<Record<string, unknown>> {
  return Object.values(MEMORY_BACKEND_CODECS).map(codec => ({
    ...codec.descriptor,
    health: codec.health,
    endpointPolicy: MEMORY_BACKEND_ENDPOINT_POLICIES[codec.providerId],
  }));
}

export function redactedMemoryBackendFailure(error: unknown): { code: string; retryable: boolean; httpStatus?: number } {
  const classified = classifyMemoryBackendError(error);
  return { code: classified.code, retryable: classified.retryable, ...(classified.httpStatus ? { httpStatus: classified.httpStatus } : {}) };
}

export const __testOnly_isExplicitMockMemoryBackendEndpoint = (endpoint: string) => evaluateMemoryBackendEndpointPolicy(endpoint).ok;
