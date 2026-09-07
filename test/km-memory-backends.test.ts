import { mkdtempSync, rmSync, writeFileSync, chmodSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  HindsightMemoryBackend,
  MEMORY_BACKEND_LIMITS,
  MemoryBackendError,
  Mem0MemoryBackend,
  OpenVikingMemoryBackend,
  memoryBackendPutIdempotencyKey,
  memoryBackendRetrieveIdempotencyKey,
  redactedMemoryBackendTelemetry,
  type BackendMemoryQuery,
  type MemoryBackendProvider,
  type MemoryBackendProviderId,
  type MemoryBackendTransport,
} from '../src/services/km/memory-backend-spi.js';
import { federatedMemoryRetrieve, federatedMemoryRetrieveWithTelemetry, writeMemoryToBackends } from '../src/services/km/memory-backend-coordinator.js';
import {
  DisabledRealTransport,
  FixtureMemoryBackendTransport,
  InMemoryMemoryBackendTransport,
  createMemoryBackendProvider,
  evaluateMemoryBackendEndpointPolicy,
  memoryBackendContractDescriptors,
  redactedMemoryBackendFailure,
  resolveMemoryBackendCredential,
  type MemoryBackendFixtureScenario,
} from '../src/services/km/memory-backend-factory.js';

const write = { memoryId: 'mem-1', scope: 'user' as const, subject: 'u1', claimKey: 'language', claimText: 'Prefer Chinese',
  privacyClass: 'internal' as const, sourceRefs: [{ kind: 'api', ref: 'evt-1' }], contentHash: `sha256:${'a'.repeat(64)}` };
const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-backends-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

function makeProvider(providerId: MemoryBackendProviderId, transport: MemoryBackendTransport, timeoutMs = 500): MemoryBackendProvider {
  if (providerId === 'mem0') return new Mem0MemoryBackend(transport, { timeoutMs, credentialRef: 'env:MEM0_TOKEN' });
  if (providerId === 'hindsight') return new HindsightMemoryBackend(transport, { timeoutMs, credentialRef: 'env:HINDSIGHT_TOKEN' });
  return new OpenVikingMemoryBackend(transport, { timeoutMs, credentialRef: 'env:OPENVIKING_TOKEN' });
}

const providerIds: MemoryBackendProviderId[] = ['mem0', 'hindsight', 'openviking'];
const query: BackendMemoryQuery = { text: 'Chinese', scopes: ['user'], subject: 'u1', limit: 2, botAppId: 'bot' };

describe('pluggable memory backends', () => {
  it('maps one domain write to Mem0, Hindsight and OpenViking without leaking vendor shapes into Core', async () => {
    const calls: any[] = [];
    const transport: MemoryBackendTransport = { request: vi.fn(async request => { calls.push(request); return request.operation === 'put' ? { backendRef: `${request.providerId}-ref` } : []; }) };
    const providers = [new Mem0MemoryBackend(transport), new HindsightMemoryBackend(transport), new OpenVikingMemoryBackend(transport)];
    for (const provider of providers) expect((await provider.put(write)).providerId).toBe(provider.descriptor.id);
    expect(calls.map(call => call.providerId)).toEqual(['mem0', 'hindsight', 'openviking']);
    expect(calls[0].payload).toEqual(expect.objectContaining({ memory: 'Prefer Chinese', user_id: 'u1' }));
    expect(calls[1].payload).toEqual(expect.objectContaining({ content: 'Prefer Chinese', bank_id: 'user:u1' }));
    expect(calls[2].payload).toEqual(expect.objectContaining({ text: 'Prefer Chinese', namespace: 'user/u1' }));
    expect(calls.map(call => call.method)).toEqual(['POST', 'POST', 'POST']);
    expect(new Set(calls.map(call => call.idempotencyKey)).size).toBe(3);
  });

  it('blocks secret-reference-only content for every external backend', async () => {
    const transport: MemoryBackendTransport = { request: vi.fn() };
    await expect(new Mem0MemoryBackend(transport).put({ ...write, privacyClass: 'secret-reference-only' })).rejects.toThrow(/secret_blocked/);
    await expect(new HindsightMemoryBackend(transport).put({ ...write, privacyClass: 'sensitive' })).rejects.toThrow(/sensitive_blocked/);
    expect(transport.request).not.toHaveBeenCalled();
  });

  it.each(providerIds)('%s fixture transport covers success, duplicate, pagination, revoke and retrieval normalization', async providerId => {
    const transport = new FixtureMemoryBackendTransport({ [providerId]: 'success' });
    const provider = makeProvider(providerId, transport);
    await expect(provider.health()).resolves.toEqual({ status: 'ok' });
    const ref = await provider.put(write);
    expect(ref).toEqual({ providerId, backendRef: `${providerId}:mem-1`, contentHash: write.contentHash });
    expect(transport.requests[0]).toEqual(expect.objectContaining({
      providerId,
      operation: 'health',
      path: provider.describeHealthRequest?.().path,
      telemetry: expect.objectContaining({ credentialRef: expect.stringMatching(/^(env|file):\*\*\*$/) }),
    }));
    expect(transport.requests[1]).toEqual(expect.objectContaining({
      providerId,
      operation: 'put',
      idempotencyKey: memoryBackendPutIdempotencyKey(providerId, write),
      telemetry: expect.not.objectContaining({ credentialRef: expect.stringContaining('TOKEN') }),
    }));
    const items = await provider.retrieve(query);
    expect(items).toEqual([expect.objectContaining({
      providerId,
      backendRef: `${providerId}:mem-1`,
      memoryId: 'mem-1',
      text: 'Prefer Chinese',
      score: expect.any(Number),
    })]);
    await provider.revoke(ref, 'test revoke');
    await expect(provider.retrieve(query)).resolves.toEqual([]);

    const duplicateProvider = makeProvider(providerId, new FixtureMemoryBackendTransport({ [providerId]: 'duplicate' }));
    await expect(duplicateProvider.put(write)).resolves.toEqual(expect.objectContaining({ backendRef: `${providerId}:mem-1` }));

    const pagingTransport = new FixtureMemoryBackendTransport({ [`${providerId}:retrieve`]: 'pagination' });
    const pagingProvider = makeProvider(providerId, pagingTransport);
    const paged = await pagingProvider.retrieve({ ...query, limit: 3 });
    expect(paged.map(item => item.memoryId)).toEqual(['mem-0', 'mem-1', 'mem-2']);
    expect(pagingTransport.requests.filter(item => item.operation === 'retrieve').map(item => item.pageCursor)).toEqual([undefined, '2']);
  });

  it.each([
    ['not_found', 'not_found', false],
    ['rate_limit', 'rate_limited', true],
    ['auth_failure', 'auth_failed', false],
    ['malformed_response', 'malformed_response', false],
  ] as const)('classifies %s fixture failures for every provider', async (scenario: MemoryBackendFixtureScenario, code, retryable) => {
    for (const providerId of providerIds) {
      const provider = makeProvider(providerId, new FixtureMemoryBackendTransport({ [providerId]: scenario }));
      await expect(provider.put(write)).rejects.toBeInstanceOf(MemoryBackendError);
      await provider.put(write).catch(error => {
        expect(redactedMemoryBackendFailure(error)).toEqual(expect.objectContaining({ code, retryable }));
      });
    }
  });

  it.each(providerIds)('classifies %s timeout fixtures as retryable and keeps health degraded', async providerId => {
    const provider = makeProvider(providerId, new FixtureMemoryBackendTransport({ [providerId]: 'timeout' }), 25);
    await expect(provider.retrieve(query)).rejects.toMatchObject({ code: 'timeout', retryable: true });
    await expect(provider.health()).resolves.toEqual(expect.objectContaining({ status: 'degraded' }));
  });

  it.each(providerIds)('%s preserves retrievable items when fixture returns partial errors', async providerId => {
    const transport = new FixtureMemoryBackendTransport({ [`${providerId}:retrieve`]: 'partial_error' });
    const provider = makeProvider(providerId, transport);
    await provider.put(write);
    await expect(provider.retrieve(query)).resolves.toEqual([
      expect.objectContaining({
        providerId,
        backendRef: `${providerId}:mem-1`,
        memoryId: 'mem-1',
        text: 'Prefer Chinese',
      }),
    ]);
    const retrieveRequest = transport.requests.find(item => item.operation === 'retrieve');
    expect(retrieveRequest).toEqual(expect.objectContaining({
      providerId,
      idempotencyKey: memoryBackendRetrieveIdempotencyKey(providerId, query),
      telemetry: expect.objectContaining({ providerId, operation: 'retrieve' }),
    }));
  });

  it('enforces size limits, stable idempotency keys and redacted telemetry', () => {
    expect(memoryBackendPutIdempotencyKey('mem0', write)).toBe(memoryBackendPutIdempotencyKey('mem0', { ...write, sourceRefs: [{ secret: 'ignored' }] }));
    expect(memoryBackendRetrieveIdempotencyKey('mem0', { ...query, limit: 10 })).toBe(memoryBackendRetrieveIdempotencyKey('mem0', { ...query, limit: 10 }));
    const telemetry = redactedMemoryBackendTelemetry({ providerId: 'mem0', operation: 'put', payload: { text: 'ok' }, credentialRef: 'env:MEM0_TOKEN' });
    expect(telemetry).toEqual(expect.objectContaining({ credentialRef: 'env:***', payloadBytes: expect.any(Number) }));
    const provider = makeProvider('openviking', new FixtureMemoryBackendTransport());
    expect(provider.descriptor.limits).toEqual(expect.objectContaining({ maxPayloadBytes: MEMORY_BACKEND_LIMITS.maxPayloadBytes, maxPages: 3 }));
    expect(provider.descriptor.privacy).toEqual(expect.objectContaining({ blockedClasses: ['sensitive', 'secret-reference-only'] }));
  });

  it('exposes contract descriptors and strict endpoint allowlist without live transport', () => {
    expect(memoryBackendContractDescriptors().map(item => item.id)).toEqual(['mem0', 'hindsight', 'openviking']);
    expect(memoryBackendContractDescriptors()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        id: 'mem0',
        version: '2',
        transport: 'offline-fixture',
        endpointPolicy: expect.objectContaining({ realTransport: 'disabled', tls: 'https-required-for-future-real-transport' }),
      }),
    ]));
    expect(evaluateMemoryBackendEndpointPolicy('mock://mem0')).toEqual({ ok: true, mode: 'fixture' });
    expect(evaluateMemoryBackendEndpointPolicy('inmemory://openviking')).toEqual({ ok: true, mode: 'fixture' });
    expect(evaluateMemoryBackendEndpointPolicy('https://mem0.example.test')).toEqual({ ok: false, mode: 'blocked-real', reason: 'mock_or_inmemory_only' });
    expect(evaluateMemoryBackendEndpointPolicy('http://mem0.example.test')).toEqual({ ok: false, mode: 'invalid', reason: 'https_required' });
  });

  it('supports primary-mirror without making mirror failure fail the primary commit', async () => {
    const provider = (id: string, fail = false): MemoryBackendProvider => ({
      descriptor: { id, version: '1', kind: id as any, capabilities: { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false } },
      health: async () => ({ status: 'ok' }), put: async item => { if (fail) throw new Error('down'); return { providerId: id, backendRef: `${id}-ref`, contentHash: item.contentHash }; },
      revoke: async () => {}, retrieve: async () => [],
    });
    const report = await writeMemoryToBackends({ item: write, policy: 'primary-mirror', primary: provider('mem0'), mirrors: [provider('hindsight', true), provider('openviking')] });
    expect(report.committed).toBe(true);
    expect(report.results).toEqual([
      expect.objectContaining({ providerId: 'mem0', status: 'active' }),
      expect.objectContaining({ providerId: 'hindsight', status: 'failed' }),
      expect.objectContaining({ providerId: 'openviking', status: 'active' }),
    ]);
  });

  it('fuses and deduplicates multi-backend retrieval by logical memory id', async () => {
    const provider = (id: string, items: any[]): MemoryBackendProvider => ({
      descriptor: { id, version: '1', kind: id as any, capabilities: { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false } },
      health: async () => ({ status: 'ok' }), put: async item => ({ providerId: id, backendRef: id, contentHash: item.contentHash }), revoke: async () => {},
      retrieve: async () => items.map(item => ({ providerId: id, ...item })),
    });
    const result = await federatedMemoryRetrieve({
      providers: [provider('mem0', [{ backendRef: 'a', memoryId: 'mem-1', text: 'Chinese', score: .9 }]), provider('hindsight', [{ backendRef: 'b', memoryId: 'mem-1', text: 'Chinese', score: .7 }]), provider('openviking', [{ backendRef: 'c', memoryId: 'mem-2', text: 'Markdown', score: .8 }])],
      query: { text: 'preference', scopes: ['user'], subject: 'u1', limit: 10, botAppId: 'bot' }, limit: 10,
    });
    expect(result).toHaveLength(2);
    expect(result[0]).toEqual(expect.objectContaining({ memoryId: 'mem-1', providers: ['mem0', 'hindsight'] }));
  });

  it('keeps real transport disabled and allows only mock/in-memory provider wiring', async () => {
    const transport = new InMemoryMemoryBackendTransport();
    const result = createMemoryBackendProvider({
      config: { providerId: 'mem0', endpoint: 'mock://mem0', credentialRef: 'env:MEM0_TOKEN', enabled: true, realTransportEnabled: false, timeoutMs: 500 },
      env: { MEM0_TOKEN: 'token' } as any,
      transport,
    });
    expect(result.status).toBe('ready');
    await expect(result.provider!.put(write)).resolves.toEqual(expect.objectContaining({ providerId: 'mem0', backendRef: 'mem0:mem-1' }));
    await expect(new DisabledRealTransport().request({ providerId: 'mem0', operation: 'health', payload: {} })).rejects.toThrow(/real_transport_disabled/);
    expect(createMemoryBackendProvider({
      config: { providerId: 'mem0', endpoint: 'mock://mem0', credentialRef: 'env:MEM0_TOKEN', enabled: true, realTransportEnabled: false, timeoutMs: 500 },
      env: {} as any,
    })).toEqual(expect.objectContaining({ status: 'credential_missing' }));
    expect(createMemoryBackendProvider({
      config: { providerId: 'mem0', endpoint: 'https://mem0.example.test', credentialRef: 'env:MEM0_TOKEN', enabled: true, realTransportEnabled: false, timeoutMs: 500 },
      env: { MEM0_TOKEN: 'token' } as any,
    })).toEqual(expect.objectContaining({ status: 'unsafe_endpoint' }));
  });

  it('resolves env and file credentials without permitting paths outside the secret dir', () => {
    const dir = tempDir(); const secret = join(dir, 'token'); writeFileSync(secret, 'file-token\n'); chmodSync(secret, 0o600);
    expect(resolveMemoryBackendCredential({ credentialRef: 'env:MEM0_TOKEN', env: { MEM0_TOKEN: 'env-token' } as any })).toEqual({ ok: true, kind: 'env', value: 'env-token' });
    expect(resolveMemoryBackendCredential({ credentialRef: `file:${secret}`, secretDir: dir })).toEqual({ ok: true, kind: 'file', value: 'file-token' });
    expect(resolveMemoryBackendCredential({ credentialRef: 'file:/etc/passwd', secretDir: dir })).toEqual({ ok: false, kind: 'file', reason: 'outside_secret_dir' });
  });

  it('reports provider timeout and partial failure telemetry without failing the fused result', async () => {
    const provider = (id: string, fail = false): MemoryBackendProvider => ({
      descriptor: { id, version: '1', kind: id as any, capabilities: { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false } },
      health: async () => ({ status: 'ok' }), put: async item => ({ providerId: id, backendRef: id, contentHash: item.contentHash }), revoke: async () => {},
      retrieve: async () => { if (fail) throw new Error('provider_down'); return [{ providerId: id, backendRef: 'ok', memoryId: 'mem-ok', text: 'Chinese', score: .8 }]; },
    });
    const report = await federatedMemoryRetrieveWithTelemetry({
      providers: [provider('mem0'), provider('hindsight', true)],
      query: { text: 'Chinese', scopes: ['user'], subject: 'u1', limit: 10, botAppId: 'bot' }, limit: 10, timeoutMs: 500,
    });
    expect(report.items).toEqual([expect.objectContaining({ memoryId: 'mem-ok', providers: ['mem0'] })]);
    expect(report.partialFailure).toBe(true);
    expect(report.telemetry).toEqual(expect.arrayContaining([
      expect.objectContaining({ providerId: 'hindsight', status: 'failed', error: 'provider_down' }),
    ]));
  });
});
