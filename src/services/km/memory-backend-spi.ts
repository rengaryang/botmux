import { createHash } from 'node:crypto';
import { z } from 'zod';
import { canonicalJsonStringify } from '../../utils/canonical-json.js';
import type { MemoryScope } from './observation-store.js';

export const MemoryBackendCapabilitiesSchema = z.object({
  put: z.boolean(), update: z.boolean(), revoke: z.boolean(), retrieve: z.boolean(),
  metadataFilter: z.boolean(), namespaces: z.boolean(), ttl: z.boolean(), snapshot: z.boolean(),
}).strict();
export type MemoryBackendCapabilities = z.infer<typeof MemoryBackendCapabilitiesSchema>;
export type MemoryBackendProviderId = 'mem0' | 'hindsight' | 'openviking';
export type MemoryBackendOperation = 'health' | 'put' | 'revoke' | 'retrieve';
export type MemoryBackendErrorCode =
  | 'duplicate'
  | 'not_found'
  | 'rate_limited'
  | 'auth_failed'
  | 'malformed_response'
  | 'timeout'
  | 'payload_blocked'
  | 'size_limit_exceeded'
  | 'network_disabled'
  | 'provider_error';

export interface MemoryBackendErrorInfo {
  code: MemoryBackendErrorCode;
  retryable: boolean;
  httpStatus?: number;
  providerId?: string;
  operation?: MemoryBackendOperation;
}

export class MemoryBackendError extends Error {
  readonly code: MemoryBackendErrorCode;
  readonly retryable: boolean;
  readonly httpStatus?: number;
  readonly providerId?: string;
  readonly operation?: MemoryBackendOperation;

  constructor(message: string, info: MemoryBackendErrorInfo) {
    super(message);
    this.name = 'MemoryBackendError';
    this.code = info.code;
    this.retryable = info.retryable;
    this.httpStatus = info.httpStatus;
    this.providerId = info.providerId;
    this.operation = info.operation;
  }
}

export interface MemoryBackendDescriptor {
  id: string;
  version: string;
  kind: 'sqlite' | MemoryBackendProviderId | 'none';
  contractVersion?: 1;
  protocolVersion?: string;
  transport?: 'offline-fixture';
  capabilities: MemoryBackendCapabilities;
  limits?: { maxPayloadBytes: number; maxQueryBytes: number; maxPageSize: number; maxPages: number };
  privacy?: { allowedClasses: Array<BackendMemoryWrite['privacyClass']>; blockedClasses: Array<BackendMemoryWrite['privacyClass']> };
}

export interface BackendMemoryWrite {
  memoryId: string; scope: MemoryScope; subject: string; claimKey: string; claimText: string;
  privacyClass: 'public-to-team' | 'internal' | 'sensitive' | 'secret-reference-only';
  ttlExpiresAt?: string; sourceRefs: unknown[]; contentHash: string;
}
export interface BackendMemoryRef { providerId: string; backendRef: string; contentHash: string }
export interface BackendMemoryQuery {
  text: string; scopes: MemoryScope[]; subject?: string; subjects?: Partial<Record<MemoryScope, string>>; limit: number; botAppId: string; cursor?: string;
}
export interface BackendMemoryResult {
  providerId: string; backendRef: string; memoryId?: string; text: string; score: number;
  scope?: MemoryScope; subject?: string; metadata?: Record<string, unknown>;
}
export interface BackendHealth { status: 'ok' | 'degraded' | 'blocked'; reason?: string }
export interface BackendHealthRequestDescription {
  providerId: string;
  operation: 'health';
  method: 'GET' | 'POST';
  path: string;
  network: 'disabled';
  expected: string;
}

export interface MemoryBackendProvider {
  descriptor: MemoryBackendDescriptor;
  health(): Promise<BackendHealth>;
  put(input: BackendMemoryWrite): Promise<BackendMemoryRef>;
  revoke(ref: BackendMemoryRef, reason: string): Promise<void>;
  retrieve(query: BackendMemoryQuery): Promise<BackendMemoryResult[]>;
  describeHealthRequest?(): BackendHealthRequestDescription;
}

export interface MemoryBackendTransportInput {
  providerId: string;
  operation: MemoryBackendOperation;
  method?: 'GET' | 'POST' | 'DELETE';
  path?: string;
  payload: Record<string, unknown>;
  idempotencyKey?: string;
  pageCursor?: string;
  timeoutMs?: number;
  telemetry?: RedactedMemoryBackendTelemetry;
}

export interface MemoryBackendTransport {
  request(input: MemoryBackendTransportInput): Promise<unknown>;
}

export const MEMORY_BACKEND_LIMITS = {
  maxPayloadBytes: 64 * 1024,
  maxQueryBytes: 4 * 1024,
  maxPageSize: 50,
  maxPages: 3,
} as const;

const MemoryScopeSchema = z.enum(['user', 'bot', 'workspace', 'project', 'skill', 'environment', 'team']);
const ProviderErrorResponseSchema = z.object({
  error: z.object({
    code: z.string().min(1),
    message: z.string().optional(),
    status: z.number().int().optional(),
    retryable: z.boolean().optional(),
  }).strict(),
}).strict();

const RawObjectSchema = z.record(z.unknown());
const RawArraySchema = z.array(RawObjectSchema);

export interface ProviderCodec {
  readonly providerId: MemoryBackendProviderId;
  readonly descriptor: MemoryBackendDescriptor;
  readonly health: BackendHealthRequestDescription;
  put(input: BackendMemoryWrite): { method: 'POST'; path: string; payload: Record<string, unknown>; backendRefFromResponse(response: unknown, fallback: BackendMemoryWrite): string };
  revoke(ref: BackendMemoryRef, reason: string): { method: 'DELETE' | 'POST'; path: string; payload: Record<string, unknown> };
  retrieve(query: BackendMemoryQuery, pageCursor?: string): {
    method: 'POST';
    path: string;
    payload: Record<string, unknown>;
    parse(response: unknown): { items: BackendMemoryResult[]; nextCursor?: string; partialErrors: MemoryBackendError[] };
  };
}

export interface RedactedMemoryBackendTelemetry {
  providerId: string;
  operation: MemoryBackendOperation;
  idempotencyKey?: string;
  payloadBytes: number;
  credentialRef?: string;
  status?: 'ok' | 'failed' | 'timeout';
  errorCode?: MemoryBackendErrorCode;
  retryable?: boolean;
}

function sha256(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function stableKey(providerId: string, operation: MemoryBackendOperation, value: unknown): string {
  return `km:${providerId}:${operation}:${sha256(canonicalJsonStringify(value)).slice(0, 32)}`;
}

export function memoryBackendPutIdempotencyKey(providerId: string, input: BackendMemoryWrite): string {
  return stableKey(providerId, 'put', {
    memoryId: input.memoryId,
    contentHash: input.contentHash,
    scope: input.scope,
    subject: input.subject,
  });
}

export function memoryBackendRevokeIdempotencyKey(providerId: string, ref: BackendMemoryRef, reason: string): string {
  return stableKey(providerId, 'revoke', { backendRef: ref.backendRef, contentHash: ref.contentHash, reason });
}

export function memoryBackendRetrieveIdempotencyKey(providerId: string, input: BackendMemoryQuery, pageCursor?: string): string {
  return stableKey(providerId, 'retrieve', {
    textHash: sha256(input.text),
    scopes: input.scopes,
    subject: input.subject ?? null,
    subjects: input.subjects ?? {},
    limit: clampLimit(input.limit),
    botAppId: input.botAppId,
    pageCursor: pageCursor ?? input.cursor ?? null,
  });
}

function byteLength(value: unknown): number {
  return Buffer.byteLength(canonicalJsonStringify(value), 'utf8');
}

function clampLimit(limit: number): number {
  return Math.max(1, Math.min(Number.isFinite(limit) ? Math.trunc(limit) : 10, MEMORY_BACKEND_LIMITS.maxPageSize));
}

function enforceWritePolicy(input: BackendMemoryWrite): void {
  if (input.privacyClass === 'secret-reference-only' || input.privacyClass === 'sensitive') {
    throw new MemoryBackendError(`km_memory_backend_${input.privacyClass === 'sensitive' ? 'sensitive' : 'secret'}_blocked`, {
      code: 'payload_blocked',
      retryable: false,
    });
  }
  if (byteLength({ ...input, sourceRefs: undefined }) > MEMORY_BACKEND_LIMITS.maxPayloadBytes) {
    throw new MemoryBackendError('km_memory_backend_payload_too_large', { code: 'size_limit_exceeded', retryable: false });
  }
}

function enforceQueryPolicy(input: BackendMemoryQuery): BackendMemoryQuery {
  if (Buffer.byteLength(input.text, 'utf8') > MEMORY_BACKEND_LIMITS.maxQueryBytes) {
    throw new MemoryBackendError('km_memory_backend_query_too_large', { code: 'size_limit_exceeded', retryable: false });
  }
  return { ...input, limit: clampLimit(input.limit) };
}

function providerError(providerId: string, operation: MemoryBackendOperation, response: unknown): never | undefined {
  const parsed = ProviderErrorResponseSchema.safeParse(response);
  if (!parsed.success) return undefined;
  const status = parsed.data.error.status;
  const rawCode = parsed.data.error.code.toLowerCase();
  const code: MemoryBackendErrorCode = rawCode.includes('rate') || status === 429
    ? 'rate_limited'
    : rawCode.includes('auth') || status === 401 || status === 403
      ? 'auth_failed'
      : rawCode.includes('not_found') || status === 404
        ? 'not_found'
        : rawCode.includes('duplicate') || status === 409
          ? 'duplicate'
          : 'provider_error';
  throw new MemoryBackendError(parsed.data.error.message ?? `km_memory_backend_${code}`, {
    code,
    retryable: parsed.data.error.retryable ?? (code === 'rate_limited' || code === 'provider_error'),
    ...(status ? { httpStatus: status } : {}),
    providerId,
    operation,
  });
}

function parseResponse<T>(schema: z.ZodType<T>, providerId: string, operation: MemoryBackendOperation, response: unknown): T {
  providerError(providerId, operation, response);
  const parsed = schema.safeParse(response);
  if (!parsed.success) {
    throw new MemoryBackendError(`km_memory_backend_malformed_response:${providerId}:${operation}`, {
      code: 'malformed_response',
      retryable: false,
      providerId,
      operation,
    });
  }
  return parsed.data;
}

function parseRecordArray(value: unknown, providerId: string, operation: MemoryBackendOperation): Record<string, unknown>[] {
  const parsed = RawArraySchema.safeParse(value);
  if (!parsed.success) {
    throw new MemoryBackendError(`km_memory_backend_malformed_response:${providerId}:${operation}`, {
      code: 'malformed_response',
      retryable: false,
      providerId,
      operation,
    });
  }
  return parsed.data;
}

function getString(value: Record<string, unknown>, key: string): string | undefined {
  const raw = value[key];
  return typeof raw === 'string' && raw.trim() ? raw : undefined;
}

function getNumber(value: Record<string, unknown>, key: string): number | undefined {
  const raw = value[key];
  return typeof raw === 'number' && Number.isFinite(raw) ? raw : undefined;
}

function getRecord(value: Record<string, unknown>, key: string): Record<string, unknown> | undefined {
  const raw = value[key];
  return typeof raw === 'object' && raw !== null && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
}

function parseMetadata(value: Record<string, unknown> | undefined): { memoryId?: string; scope?: MemoryScope; subject?: string; metadata?: Record<string, unknown> } {
  if (!value) return {};
  const scope = typeof value.scope === 'string' && MemoryScopeSchema.safeParse(value.scope).success ? value.scope as MemoryScope
    : typeof value.memory_scope === 'string' && MemoryScopeSchema.safeParse(value.memory_scope).success ? value.memory_scope as MemoryScope
      : undefined;
  const subject = typeof value.subject === 'string' ? value.subject
    : typeof value.user_id === 'string' ? value.user_id
      : undefined;
  const memoryId = typeof value.memoryId === 'string' ? value.memoryId
    : typeof value.memory_id === 'string' ? value.memory_id
      : typeof value.logical_id === 'string' ? value.logical_id
        : undefined;
  return { ...(memoryId ? { memoryId } : {}), ...(scope ? { scope } : {}), ...(subject ? { subject } : {}), metadata: value };
}

function withProviderPrefix(providerId: string, id: string): string {
  return id.startsWith(`${providerId}:`) ? id : `${providerId}:${id}`;
}

function parsePartialErrors(providerId: string, operation: MemoryBackendOperation, response: Record<string, unknown>): MemoryBackendError[] {
  const raw = response.partialErrors ?? response.partial_errors;
  if (!Array.isArray(raw)) return [];
  return raw.map((item, index) => {
    const value = typeof item === 'object' && item !== null ? item as Record<string, unknown> : {};
    const status = typeof value.status === 'number' ? value.status : undefined;
    const code: MemoryBackendErrorCode = status === 429 ? 'rate_limited' : status === 401 || status === 403 ? 'auth_failed' : 'provider_error';
    return new MemoryBackendError(typeof value.message === 'string' ? value.message : `km_memory_backend_partial_error:${index}`, {
      code,
      retryable: code === 'rate_limited' || code === 'provider_error',
      ...(status ? { httpStatus: status } : {}),
      providerId,
      operation,
    });
  });
}

function nextCursor(response: Record<string, unknown>): string | undefined {
  const value = response.nextCursor ?? response.next_cursor ?? (typeof response.page === 'object' && response.page !== null ? (response.page as Record<string, unknown>).next : undefined);
  return typeof value === 'string' && value.trim() ? value : undefined;
}

function descriptor(providerId: MemoryBackendProviderId, capabilities: Partial<MemoryBackendCapabilities>, protocolVersion: string): MemoryBackendDescriptor {
  const base: MemoryBackendCapabilities = { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false };
  return {
    id: providerId,
    version: '2',
    kind: providerId,
    contractVersion: 1,
    protocolVersion,
    transport: 'offline-fixture',
    capabilities: { ...base, ...capabilities },
    limits: { ...MEMORY_BACKEND_LIMITS },
    privacy: {
      allowedClasses: ['public-to-team', 'internal'],
      blockedClasses: ['sensitive', 'secret-reference-only'],
    },
  };
}

const mem0Codec: ProviderCodec = {
  providerId: 'mem0',
  descriptor: descriptor('mem0', { ttl: true }, 'mem0-v1-offline'),
  health: { providerId: 'mem0', operation: 'health', method: 'GET', path: '/v1/health', network: 'disabled', expected: '2xx JSON object' },
  put(input) {
    return {
      method: 'POST',
      path: '/v1/memories',
      payload: {
        id: input.memoryId,
        memory: input.claimText,
        user_id: input.subject,
        metadata: {
          logical_id: input.memoryId,
          scope: input.scope,
          subject: input.subject,
          claim_key: input.claimKey,
          content_hash: input.contentHash,
          ttl_expires_at: input.ttlExpiresAt ?? null,
        },
      },
      backendRefFromResponse: response => {
        const parsed = parseResponse(RawObjectSchema, 'mem0', 'put', response);
        const id = getString(parsed, 'backendRef') ?? getString(parsed, 'id') ?? getString(getRecord(parsed, 'memory') ?? {}, 'id');
        if (!id) throw new MemoryBackendError('km_memory_backend_malformed_response:mem0:put', { code: 'malformed_response', retryable: false, providerId: 'mem0', operation: 'put' });
        return withProviderPrefix('mem0', id);
      },
    };
  },
  revoke(ref, reason) {
    return { method: 'DELETE', path: `/v1/memories/${encodeURIComponent(ref.backendRef.replace(/^mem0:/, ''))}`, payload: { backendRef: ref.backendRef, reason } };
  },
  retrieve(query, pageCursor) {
    return {
      method: 'POST',
      path: '/v1/memories/search',
      payload: {
        query: query.text,
        user_id: query.subject,
        limit: query.limit,
        page_cursor: pageCursor ?? query.cursor,
        filters: { scopes: query.scopes, subjects: query.subjects ?? {}, bot_app_id: query.botAppId },
      },
      parse(response) {
        const object = Array.isArray(response) ? { results: response } : response;
        const parsed = parseResponse(RawObjectSchema, 'mem0', 'retrieve', object);
        const results = parseRecordArray(parsed.results, 'mem0', 'retrieve');
        const items = results.map(item => {
          const backendRef = getString(item, 'backendRef');
          if (backendRef) return {
            providerId: 'mem0',
            backendRef,
            ...(getString(item, 'memoryId') ? { memoryId: getString(item, 'memoryId') } : {}),
            text: getString(item, 'text') ?? '',
            score: getNumber(item, 'score') ?? 0,
            ...(MemoryScopeSchema.safeParse(item.scope).success ? { scope: item.scope as MemoryScope } : {}),
            ...(getString(item, 'subject') ? { subject: getString(item, 'subject') } : {}),
            ...(getRecord(item, 'metadata') ? { metadata: getRecord(item, 'metadata') } : {}),
          };
          const metadata = parseMetadata(getRecord(item, 'metadata'));
          const memory = getString(item, 'memory') ?? '';
          const id = getString(item, 'id') ?? metadata.memoryId;
          return { providerId: 'mem0', backendRef: withProviderPrefix('mem0', id ?? sha256(memory).slice(0, 16)),
            ...(metadata.memoryId ? { memoryId: metadata.memoryId } : {}),
            text: memory, score: getNumber(item, 'score') ?? 0, ...metadata };
        });
        return { items, nextCursor: nextCursor(parsed), partialErrors: parsePartialErrors('mem0', 'retrieve', parsed) };
      },
    };
  },
};

const hindsightCodec: ProviderCodec = {
  providerId: 'hindsight',
  descriptor: descriptor('hindsight', {}, 'hindsight-v1-offline'),
  health: { providerId: 'hindsight', operation: 'health', method: 'GET', path: '/api/health', network: 'disabled', expected: '2xx JSON object' },
  put(input) {
    return {
      method: 'POST',
      path: '/api/documents',
      payload: {
        document_id: input.memoryId,
        content: input.claimText,
        bank_id: `${input.scope}:${input.subject}`,
        metadata: { logical_id: input.memoryId, claim_key: input.claimKey, content_hash: input.contentHash, scope: input.scope, subject: input.subject },
      },
      backendRefFromResponse: response => {
        const parsed = parseResponse(RawObjectSchema, 'hindsight', 'put', response);
        const id = getString(parsed, 'backendRef') ?? getString(parsed, 'document_id') ?? getString(getRecord(parsed, 'document') ?? {}, 'id') ?? getString(parsed, 'id');
        if (!id) throw new MemoryBackendError('km_memory_backend_malformed_response:hindsight:put', { code: 'malformed_response', retryable: false, providerId: 'hindsight', operation: 'put' });
        return withProviderPrefix('hindsight', id);
      },
    };
  },
  revoke(ref, reason) {
    return { method: 'DELETE', path: `/api/documents/${encodeURIComponent(ref.backendRef.replace(/^hindsight:/, ''))}`, payload: { backendRef: ref.backendRef, reason } };
  },
  retrieve(query, pageCursor) {
    return {
      method: 'POST',
      path: '/api/search',
      payload: { query: query.text, bank_id: query.subject ? `user:${query.subject}` : undefined, top_k: query.limit, cursor: pageCursor ?? query.cursor,
        metadata_filter: { scopes: query.scopes, subjects: query.subjects ?? {}, bot_app_id: query.botAppId } },
      parse(response) {
        const object = Array.isArray(response) ? { matches: response } : response;
        const parsed = parseResponse(RawObjectSchema, 'hindsight', 'retrieve', object);
        const matches = parseRecordArray(parsed.matches, 'hindsight', 'retrieve');
        const items = matches.map(item => {
          const backendRef = getString(item, 'backendRef');
          if (backendRef) return {
            providerId: 'hindsight',
            backendRef,
            ...(getString(item, 'memoryId') ? { memoryId: getString(item, 'memoryId') } : {}),
            text: getString(item, 'text') ?? '',
            score: getNumber(item, 'score') ?? 0,
            ...(MemoryScopeSchema.safeParse(item.scope).success ? { scope: item.scope as MemoryScope } : {}),
            ...(getString(item, 'subject') ? { subject: getString(item, 'subject') } : {}),
            ...(getRecord(item, 'metadata') ? { metadata: getRecord(item, 'metadata') } : {}),
          };
          const metadata = parseMetadata(getRecord(item, 'metadata'));
          const content = getString(item, 'content') ?? '';
          const id = getString(item, 'document_id') ?? metadata.memoryId;
          return { providerId: 'hindsight', backendRef: withProviderPrefix('hindsight', id ?? sha256(content).slice(0, 16)),
            ...(metadata.memoryId ? { memoryId: metadata.memoryId } : {}), text: content, score: getNumber(item, 'score') ?? 0, ...metadata };
        });
        return { items, nextCursor: nextCursor(parsed), partialErrors: parsePartialErrors('hindsight', 'retrieve', parsed) };
      },
    };
  },
};

const openVikingCodec: ProviderCodec = {
  providerId: 'openviking',
  descriptor: descriptor('openviking', { snapshot: true }, 'openviking-v1-offline'),
  health: { providerId: 'openviking', operation: 'health', method: 'GET', path: '/v1/status', network: 'disabled', expected: '2xx JSON object' },
  put(input) {
    return {
      method: 'POST',
      path: '/v1/resources',
      payload: {
        resource_id: input.memoryId,
        text: input.claimText,
        namespace: `${input.scope}/${input.subject}`,
        attributes: { logical_id: input.memoryId, claim_key: input.claimKey, content_hash: input.contentHash, scope: input.scope, subject: input.subject },
      },
      backendRefFromResponse: response => {
        const parsed = parseResponse(RawObjectSchema, 'openviking', 'put', response);
        const id = getString(parsed, 'backendRef') ?? getString(parsed, 'resource_id') ?? getString(getRecord(parsed, 'resource') ?? {}, 'id') ?? getString(parsed, 'id');
        if (!id) throw new MemoryBackendError('km_memory_backend_malformed_response:openviking:put', { code: 'malformed_response', retryable: false, providerId: 'openviking', operation: 'put' });
        return withProviderPrefix('openviking', id);
      },
    };
  },
  revoke(ref, reason) {
    return { method: 'DELETE', path: `/v1/resources/${encodeURIComponent(ref.backendRef.replace(/^openviking:/, ''))}`, payload: { backendRef: ref.backendRef, reason } };
  },
  retrieve(query, pageCursor) {
    return {
      method: 'POST',
      path: '/v1/resources/search',
      payload: { text: query.text, namespaces: query.scopes.map(scope => `${scope}/${query.subjects?.[scope] ?? query.subject ?? '*'}`),
        limit: query.limit, cursor: pageCursor ?? query.cursor, attributes: { subjects: query.subjects ?? {}, bot_app_id: query.botAppId } },
      parse(response) {
        const object = Array.isArray(response) ? { memories: response } : response;
        const parsed = parseResponse(RawObjectSchema, 'openviking', 'retrieve', object);
        const memories = parseRecordArray(parsed.memories, 'openviking', 'retrieve');
        const items = memories.map(item => {
          const backendRef = getString(item, 'backendRef');
          if (backendRef) return {
            providerId: 'openviking',
            backendRef,
            ...(getString(item, 'memoryId') ? { memoryId: getString(item, 'memoryId') } : {}),
            text: getString(item, 'text') ?? '',
            score: getNumber(item, 'score') ?? 0,
            ...(MemoryScopeSchema.safeParse(item.scope).success ? { scope: item.scope as MemoryScope } : {}),
            ...(getString(item, 'subject') ? { subject: getString(item, 'subject') } : {}),
            ...(getRecord(item, 'metadata') ? { metadata: getRecord(item, 'metadata') } : {}),
          };
          const metadata = parseMetadata(getRecord(item, 'attributes'));
          const text = getString(item, 'text') ?? '';
          const id = getString(item, 'resource_id') ?? metadata.memoryId;
          return { providerId: 'openviking', backendRef: withProviderPrefix('openviking', id ?? sha256(text).slice(0, 16)),
            ...(metadata.memoryId ? { memoryId: metadata.memoryId } : {}), text, score: getNumber(item, 'relevance') ?? 0, ...metadata };
        });
        return { items, nextCursor: nextCursor(parsed), partialErrors: parsePartialErrors('openviking', 'retrieve', parsed) };
      },
    };
  },
};

export const MEMORY_BACKEND_CODECS: Readonly<Record<MemoryBackendProviderId, ProviderCodec>> = {
  mem0: mem0Codec,
  hindsight: hindsightCodec,
  openviking: openVikingCodec,
};

export function classifyMemoryBackendError(error: unknown): MemoryBackendErrorInfo {
  if (error instanceof MemoryBackendError) {
    return { code: error.code, retryable: error.retryable, ...(error.httpStatus ? { httpStatus: error.httpStatus } : {}),
      ...(error.providerId ? { providerId: error.providerId } : {}), ...(error.operation ? { operation: error.operation } : {}) };
  }
  const message = error instanceof Error ? error.message : String(error);
  if (message.includes('timeout')) return { code: 'timeout', retryable: true };
  if (message.includes('rate')) return { code: 'rate_limited', retryable: true };
  if (message.includes('auth')) return { code: 'auth_failed', retryable: false };
  if (message.includes('not_found')) return { code: 'not_found', retryable: false };
  if (message.includes('malformed')) return { code: 'malformed_response', retryable: false };
  if (message.includes('payload_too_large')) return { code: 'size_limit_exceeded', retryable: false };
  if (message.includes('blocked')) return { code: 'payload_blocked', retryable: false };
  return { code: 'provider_error', retryable: true };
}

export function redactedMemoryBackendTelemetry(input: {
  providerId: string;
  operation: MemoryBackendOperation;
  payload: Record<string, unknown>;
  idempotencyKey?: string;
  credentialRef?: string;
  status?: 'ok' | 'failed' | 'timeout';
  error?: unknown;
}): RedactedMemoryBackendTelemetry {
  const classified = input.error ? classifyMemoryBackendError(input.error) : undefined;
  return {
    providerId: input.providerId,
    operation: input.operation,
    ...(input.idempotencyKey ? { idempotencyKey: input.idempotencyKey } : {}),
    payloadBytes: byteLength(input.payload),
    ...(input.credentialRef ? { credentialRef: input.credentialRef.replace(/^(env|file):(.+)$/, (_m, kind) => `${kind}:***`) } : {}),
    ...(input.status ? { status: input.status } : {}),
    ...(classified ? { errorCode: classified.code, retryable: classified.retryable } : {}),
  };
}

async function withBackendTimeout<T>(promise: Promise<T>, timeoutMs: number, providerId: string, operation: MemoryBackendOperation): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new MemoryBackendError(`km_memory_backend_timeout:${providerId}`, {
          code: 'timeout',
          retryable: true,
          providerId,
          operation,
        })), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

abstract class TransportMemoryBackend implements MemoryBackendProvider {
  readonly descriptor: MemoryBackendDescriptor;
  constructor(protected readonly transport: MemoryBackendTransport, private readonly codec: ProviderCodec, private readonly options: { timeoutMs?: number; credentialRef?: string } = {}) {
    this.descriptor = codec.descriptor;
  }
  describeHealthRequest(): BackendHealthRequestDescription {
    return this.codec.health;
  }
  async health(): Promise<BackendHealth> {
    const payload = { request: this.codec.health };
    const key = stableKey(this.descriptor.id, 'health', this.codec.health);
    try {
      await withBackendTimeout(this.transport.request({
        providerId: this.descriptor.id,
        operation: 'health',
        method: this.codec.health.method,
        path: this.codec.health.path,
        payload,
        idempotencyKey: key,
        timeoutMs: this.options.timeoutMs,
        telemetry: redactedMemoryBackendTelemetry({ providerId: this.descriptor.id, operation: 'health', payload, idempotencyKey: key, credentialRef: this.options.credentialRef }),
      }), this.options.timeoutMs ?? 5_000, this.descriptor.id, 'health');
      return { status: 'ok' };
    } catch (error) {
      const classified = classifyMemoryBackendError(error);
      return { status: classified.retryable ? 'degraded' : 'blocked', reason: error instanceof Error ? error.message : String(error) };
    }
  }
  async put(input: BackendMemoryWrite): Promise<BackendMemoryRef> {
    enforceWritePolicy(input);
    const request = this.codec.put(input);
    const key = memoryBackendPutIdempotencyKey(this.descriptor.id, input);
    const response = await withBackendTimeout(this.transport.request({
      providerId: this.descriptor.id,
      operation: 'put',
      method: request.method,
      path: request.path,
      payload: request.payload,
      idempotencyKey: key,
      timeoutMs: this.options.timeoutMs,
      telemetry: redactedMemoryBackendTelemetry({ providerId: this.descriptor.id, operation: 'put', payload: request.payload, idempotencyKey: key, credentialRef: this.options.credentialRef }),
    }), this.options.timeoutMs ?? 5_000, this.descriptor.id, 'put');
    return { providerId: this.descriptor.id, backendRef: request.backendRefFromResponse(response, input), contentHash: input.contentHash };
  }
  async revoke(ref: BackendMemoryRef, reason: string): Promise<void> {
    const request = this.codec.revoke(ref, reason);
    const key = memoryBackendRevokeIdempotencyKey(this.descriptor.id, ref, reason);
    const response = await withBackendTimeout(this.transport.request({
      providerId: this.descriptor.id,
      operation: 'revoke',
      method: request.method,
      path: request.path,
      payload: request.payload,
      idempotencyKey: key,
      timeoutMs: this.options.timeoutMs,
      telemetry: redactedMemoryBackendTelemetry({ providerId: this.descriptor.id, operation: 'revoke', payload: request.payload, idempotencyKey: key, credentialRef: this.options.credentialRef }),
    }), this.options.timeoutMs ?? 5_000, this.descriptor.id, 'revoke');
    providerError(this.descriptor.id, 'revoke', response);
  }
  async retrieve(query: BackendMemoryQuery): Promise<BackendMemoryResult[]> {
    const bounded = enforceQueryPolicy(query);
    const items: BackendMemoryResult[] = [];
    let pageCursor = bounded.cursor;
    for (let page = 0; page < MEMORY_BACKEND_LIMITS.maxPages && items.length < bounded.limit; page += 1) {
      const request = this.codec.retrieve({ ...bounded, limit: Math.min(bounded.limit - items.length, MEMORY_BACKEND_LIMITS.maxPageSize) }, pageCursor);
      const key = memoryBackendRetrieveIdempotencyKey(this.descriptor.id, bounded, pageCursor);
      const response = await withBackendTimeout(this.transport.request({
        providerId: this.descriptor.id,
        operation: 'retrieve',
        method: request.method,
        path: request.path,
        payload: request.payload,
        idempotencyKey: key,
        pageCursor,
        timeoutMs: this.options.timeoutMs,
        telemetry: redactedMemoryBackendTelemetry({ providerId: this.descriptor.id, operation: 'retrieve', payload: request.payload, idempotencyKey: key, credentialRef: this.options.credentialRef }),
      }), this.options.timeoutMs ?? 5_000, this.descriptor.id, 'retrieve');
      const parsed = request.parse(response);
      items.push(...parsed.items);
      if (parsed.partialErrors.length && parsed.items.length === 0) throw parsed.partialErrors[0];
      pageCursor = parsed.nextCursor;
      if (!pageCursor) break;
    }
    return items.slice(0, bounded.limit).map(item => ({ ...item, providerId: this.descriptor.id }));
  }
}

export class Mem0MemoryBackend extends TransportMemoryBackend {
  constructor(transport: MemoryBackendTransport, options?: { timeoutMs?: number; credentialRef?: string }) { super(transport, mem0Codec, options); }
}
export class HindsightMemoryBackend extends TransportMemoryBackend {
  constructor(transport: MemoryBackendTransport, options?: { timeoutMs?: number; credentialRef?: string }) { super(transport, hindsightCodec, options); }
}
export class OpenVikingMemoryBackend extends TransportMemoryBackend {
  constructor(transport: MemoryBackendTransport, options?: { timeoutMs?: number; credentialRef?: string }) { super(transport, openVikingCodec, options); }
}
