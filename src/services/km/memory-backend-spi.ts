import { z } from 'zod';
import type { MemoryScope } from './observation-store.js';

export const MemoryBackendCapabilitiesSchema = z.object({
  put: z.boolean(), update: z.boolean(), revoke: z.boolean(), retrieve: z.boolean(),
  metadataFilter: z.boolean(), namespaces: z.boolean(), ttl: z.boolean(), snapshot: z.boolean(),
}).strict();
export type MemoryBackendCapabilities = z.infer<typeof MemoryBackendCapabilitiesSchema>;

export interface MemoryBackendDescriptor {
  id: string;
  version: string;
  kind: 'sqlite' | 'mem0' | 'hindsight' | 'openviking' | 'none';
  capabilities: MemoryBackendCapabilities;
}

export interface BackendMemoryWrite {
  memoryId: string; scope: MemoryScope; subject: string; claimKey: string; claimText: string;
  privacyClass: 'public-to-team' | 'internal' | 'sensitive' | 'secret-reference-only';
  ttlExpiresAt?: string; sourceRefs: unknown[]; contentHash: string;
}
export interface BackendMemoryRef { providerId: string; backendRef: string; contentHash: string }
export interface BackendMemoryQuery {
  text: string; scopes: MemoryScope[]; subject?: string; subjects?: Partial<Record<MemoryScope, string>>; limit: number; botAppId: string;
}
export interface BackendMemoryResult {
  providerId: string; backendRef: string; memoryId?: string; text: string; score: number;
  scope?: MemoryScope; subject?: string; metadata?: Record<string, unknown>;
}
export interface BackendHealth { status: 'ok' | 'degraded' | 'blocked'; reason?: string }

export interface MemoryBackendProvider {
  descriptor: MemoryBackendDescriptor;
  health(): Promise<BackendHealth>;
  put(input: BackendMemoryWrite): Promise<BackendMemoryRef>;
  revoke(ref: BackendMemoryRef, reason: string): Promise<void>;
  retrieve(query: BackendMemoryQuery): Promise<BackendMemoryResult[]>;
}

export interface MemoryBackendTransport {
  request(input: { providerId: string; operation: 'health' | 'put' | 'revoke' | 'retrieve'; payload: Record<string, unknown> }): Promise<unknown>;
}

const PutResponseSchema = z.object({ backendRef: z.string().min(1) }).strict();
const RetrievalResponseSchema = z.array(z.object({
  backendRef: z.string().min(1), memoryId: z.string().optional(), text: z.string(), score: z.number(),
  scope: z.enum(['user', 'bot', 'workspace', 'project', 'skill', 'environment', 'team']).optional(),
  subject: z.string().optional(), metadata: z.record(z.unknown()).optional(),
}).strict());

abstract class TransportMemoryBackend implements MemoryBackendProvider {
  abstract descriptor: MemoryBackendDescriptor;
  constructor(protected readonly transport: MemoryBackendTransport) {}
  async health(): Promise<BackendHealth> {
    try { await this.transport.request({ providerId: this.descriptor.id, operation: 'health', payload: {} }); return { status: 'ok' }; }
    catch (error) { return { status: 'degraded', reason: error instanceof Error ? error.message : String(error) }; }
  }
  async put(input: BackendMemoryWrite): Promise<BackendMemoryRef> {
    if (input.privacyClass === 'secret-reference-only') throw new Error('km_memory_backend_secret_blocked');
    const response = PutResponseSchema.parse(await this.transport.request({ providerId: this.descriptor.id, operation: 'put', payload: this.mapPut(input) }));
    return { providerId: this.descriptor.id, backendRef: response.backendRef, contentHash: input.contentHash };
  }
  async revoke(ref: BackendMemoryRef, reason: string): Promise<void> {
    await this.transport.request({ providerId: this.descriptor.id, operation: 'revoke', payload: { backendRef: ref.backendRef, reason } });
  }
  async retrieve(query: BackendMemoryQuery): Promise<BackendMemoryResult[]> {
    const response = RetrievalResponseSchema.parse(await this.transport.request({ providerId: this.descriptor.id, operation: 'retrieve', payload: this.mapQuery(query) }));
    return response.map(item => ({ providerId: this.descriptor.id, ...item }));
  }
  protected abstract mapPut(input: BackendMemoryWrite): Record<string, unknown>;
  protected abstract mapQuery(input: BackendMemoryQuery): Record<string, unknown>;
}

const baseCaps: MemoryBackendCapabilities = { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false };

export class Mem0MemoryBackend extends TransportMemoryBackend {
  descriptor = { id: 'mem0', version: '1', kind: 'mem0' as const, capabilities: { ...baseCaps, ttl: true } };
  protected mapPut(x: BackendMemoryWrite) { return { id: x.memoryId, memory: x.claimText, user_id: x.subject, metadata: { scope: x.scope, claimKey: x.claimKey, contentHash: x.contentHash, ttlExpiresAt: x.ttlExpiresAt } }; }
  protected mapQuery(q: BackendMemoryQuery) { return { query: q.text, user_id: q.subject, limit: q.limit, filters: { scopes: q.scopes, subjects: q.subjects ?? {}, botAppId: q.botAppId } }; }
}
export class HindsightMemoryBackend extends TransportMemoryBackend {
  descriptor = { id: 'hindsight', version: '1', kind: 'hindsight' as const, capabilities: { ...baseCaps } };
  protected mapPut(x: BackendMemoryWrite) { return { document_id: x.memoryId, content: x.claimText, bank_id: `${x.scope}:${x.subject}`, metadata: { claim_key: x.claimKey, content_hash: x.contentHash } }; }
  protected mapQuery(q: BackendMemoryQuery) { return { query: q.text, bank_id: q.subject ? `user:${q.subject}` : undefined, top_k: q.limit, metadata_filter: { scopes: q.scopes, subjects: q.subjects ?? {}, bot_app_id: q.botAppId } }; }
}
export class OpenVikingMemoryBackend extends TransportMemoryBackend {
  descriptor = { id: 'openviking', version: '1', kind: 'openviking' as const, capabilities: { ...baseCaps, snapshot: true } };
  protected mapPut(x: BackendMemoryWrite) { return { resource_id: x.memoryId, text: x.claimText, namespace: `${x.scope}/${x.subject}`, attributes: { claim_key: x.claimKey, content_hash: x.contentHash } }; }
  protected mapQuery(q: BackendMemoryQuery) { return { text: q.text, namespaces: q.scopes.map(scope => `${scope}/${q.subjects?.[scope] ?? q.subject ?? '*'}`), limit: q.limit, attributes: { subjects: q.subjects ?? {}, bot_app_id: q.botAppId } }; }
}
