import { mkdtempSync, rmSync, writeFileSync, chmodSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Mem0MemoryBackend, HindsightMemoryBackend, OpenVikingMemoryBackend, type MemoryBackendTransport, type MemoryBackendProvider } from '../src/services/km/memory-backend-spi.js';
import { federatedMemoryRetrieve, federatedMemoryRetrieveWithTelemetry, writeMemoryToBackends } from '../src/services/km/memory-backend-coordinator.js';
import { DisabledRealTransport, InMemoryMemoryBackendTransport, createMemoryBackendProvider, resolveMemoryBackendCredential } from '../src/services/km/memory-backend-factory.js';

const write = { memoryId: 'mem-1', scope: 'user' as const, subject: 'u1', claimKey: 'language', claimText: 'Prefer Chinese',
  privacyClass: 'internal' as const, sourceRefs: [{ kind: 'api', ref: 'evt-1' }], contentHash: `sha256:${'a'.repeat(64)}` };
const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-backends-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

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
  });

  it('blocks secret-reference-only content for every external backend', async () => {
    const transport: MemoryBackendTransport = { request: vi.fn() };
    await expect(new Mem0MemoryBackend(transport).put({ ...write, privacyClass: 'secret-reference-only' })).rejects.toThrow(/secret_blocked/);
    expect(transport.request).not.toHaveBeenCalled();
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
