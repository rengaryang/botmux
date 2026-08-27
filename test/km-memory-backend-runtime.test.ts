import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { defaultShadowProfile } from '../src/services/km/runtime-orchestrator.js';
import {
  createKmMemoryBackendProviders,
  enqueueEligibleMemoryBackendMirrors,
  isKmBackendWorkerEnabled,
  kmBackendRuntimeStatus,
  runKmBackendWorkerOnce,
} from '../src/services/km/memory-backend-runtime.js';
import type { MemoryBackendProvider } from '../src/services/km/memory-backend-spi.js';

const dirs: string[] = [];
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-backend-runtime-'));
  dirs.push(dir);
  return dir;
}

afterEach(() => {
  vi.useRealTimers();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function provider(id: 'mem0' | 'hindsight' | 'openviking', impl: Partial<MemoryBackendProvider> = {}): MemoryBackendProvider {
  return {
    descriptor: { id, version: '1', kind: id, capabilities: { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false } },
    health: async () => ({ status: 'ok' }),
    put: async item => ({ providerId: id, backendRef: `${id}:${item.memoryId}`, contentHash: item.contentHash }),
    revoke: async () => {},
    retrieve: async () => [],
    ...impl,
  };
}

describe('KM memory backend worker runtime', () => {
  it('stays default-off and reports durable runtime state without claiming work', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    store.putMemoryProviderConfig({ providerId: 'mem0', endpoint: 'mock://mem0', credentialRef: 'env:MEM0_TOKEN', enabled: true, realTransportEnabled: false, timeoutMs: 500 });
    const memory = store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Chinese',
      confidence: 'observed', privacyClass: 'internal', sourceRefs: [{ kind: 'api', ref: 'evt' }] }).item;
    store.enqueueMemoryBackendOperation({ memoryId: memory.memoryId, providerId: 'mem0', operation: 'put', payload: { ...memory }, now: 1000 });
    store.close();

    expect(isKmBackendWorkerEnabled({} as any)).toBe(false);
    expect(await runKmBackendWorkerOnce({ dataDir: dir, env: {} as any, now: 2000 })).toEqual(expect.objectContaining({
      enabled: false,
      leaseAcquired: false,
    }));
    await expect(kmBackendRuntimeStatus({ dataDir: dir, env: {} as any, now: 2000 })).resolves.toEqual(expect.objectContaining({
      enabled: false,
      outbox: expect.objectContaining({ total: 1, pending: 1 }),
      providers: [expect.objectContaining({ providerId: 'mem0', status: 'credential_missing' })],
    }));
  });

  it('instantiates only explicitly mock/in-memory enabled providers', () => {
    const result = createKmMemoryBackendProviders({
      configs: [
        { providerId: 'mem0', endpoint: 'mock://mem0', credentialRef: 'env:MEM0_TOKEN', enabled: true, realTransportEnabled: false, timeoutMs: 500 },
        { providerId: 'hindsight', endpoint: 'https://memory.example.test', credentialRef: 'env:HINDSIGHT_TOKEN', enabled: true, realTransportEnabled: false, timeoutMs: 500 },
      ],
      env: { MEM0_TOKEN: 'token', HINDSIGHT_TOKEN: 'token' } as any,
    });
    expect([...result.providers.keys()]).toEqual(['mem0']);
    expect(result.statuses).toEqual([
      expect.objectContaining({ providerId: 'mem0', status: 'ready' }),
      expect.objectContaining({ providerId: 'hindsight', status: 'unsafe_endpoint' }),
    ]);
  });

  it('enqueues only active local-only internal memories from the effective profile', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    store.upsertMemory({ memoryId: 'mem-active', state: 'active', scope: 'user', subject: 'u1', claimKey: 'a', claimText: 'A',
      confidence: 'observed', privacyClass: 'internal', syncPolicy: 'local-only', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.upsertMemory({ memoryId: 'mem-proposed', state: 'proposed', scope: 'user', subject: 'u1', claimKey: 'b', claimText: 'B',
      confidence: 'observed', privacyClass: 'internal', syncPolicy: 'local-only', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.upsertMemory({ memoryId: 'mem-sensitive', state: 'active', scope: 'user', subject: 'u1', claimKey: 'c', claimText: 'C',
      confidence: 'observed', privacyClass: 'sensitive', syncPolicy: 'local-only', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.upsertMemory({ memoryId: 'mem-central', state: 'active', scope: 'user', subject: 'u1', claimKey: 'd', claimText: 'D',
      confidence: 'observed', privacyClass: 'internal', syncPolicy: 'central-approved', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.upsertMemory({ memoryId: 'mem-public-team', state: 'active', scope: 'user', subject: 'u1', claimKey: 'e', claimText: 'E',
      confidence: 'observed', privacyClass: 'public-to-team', syncPolicy: 'local-only', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    const profile = { ...defaultShadowProfile('bot'), memoryBackends: { writePolicy: 'primary-mirror' as const, primary: 'sqlite', mirrors: ['mem0', 'hindsight'] } };
    const report = enqueueEligibleMemoryBackendMirrors({ store, profile, now: 1000 });
    expect(report).toEqual({ scanned: 5, enqueued: 2, skipped: 4, targetProviders: ['mem0', 'hindsight'] });
    expect(store.listMemoryBackendOutbox(10).map(item => [item.memoryId, item.providerId, item.status]).sort()).toEqual([
      ['mem-active', 'hindsight', 'pending'],
      ['mem-active', 'mem0', 'pending'],
    ]);
    store.close();
  });

  it('uses a durable runtime lease so only one daemon drains at a time', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    store.putMemoryProviderConfig({ providerId: 'mem0', endpoint: 'mock://mem0', credentialRef: 'env:MEM0_TOKEN', enabled: true, realTransportEnabled: false, timeoutMs: 500 });
    store.upsertMemory({ memoryId: 'mem-active', state: 'active', scope: 'user', subject: 'u1', claimKey: 'a', claimText: 'A',
      confidence: 'observed', privacyClass: 'internal', syncPolicy: 'local-only', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.putPipelineProfile({ ...defaultShadowProfile('bot'), memoryBackends: { writePolicy: 'primary-mirror' as const, primary: 'sqlite', mirrors: ['mem0'] } }, 'shadow');
    expect(store.acquireRuntimeLease({ leaseName: 'memory-backend-outbox', holderId: 'daemon-a', now: 1000, ttlMs: 5000 })).toBe(true);
    store.close();

    await expect(runKmBackendWorkerOnce({ dataDir: dir, env: { BOTMUX_KM_BACKEND_WORKER_ENABLED: 'true', MEM0_TOKEN: 'token' } as any,
      holderId: 'daemon-b', now: 2000, leaseMs: 5000 })).resolves.toEqual(expect.objectContaining({
      enabled: true,
      leaseAcquired: false,
    }));

    const reopened = await ObservationStore.open(dir);
    expect(reopened.listMemoryBackendOutbox(10)).toEqual([]);
    reopened.releaseRuntimeLease({ leaseName: 'memory-backend-outbox', holderId: 'daemon-a' });
    reopened.close();
  });

  it('mirrors to mock provider and leaves SQLite memory as source of truth', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    store.putMemoryProviderConfig({ providerId: 'mem0', endpoint: 'mock://mem0', credentialRef: 'env:MEM0_TOKEN', enabled: true, realTransportEnabled: false, timeoutMs: 500 });
    store.upsertMemory({ memoryId: 'mem-active', state: 'active', scope: 'user', subject: 'u1', claimKey: 'a', claimText: 'A',
      confidence: 'observed', privacyClass: 'internal', syncPolicy: 'local-only', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.putPipelineProfile({ ...defaultShadowProfile('bot'), memoryBackends: { writePolicy: 'primary-mirror' as const, primary: 'sqlite', mirrors: ['mem0'] } }, 'shadow');
    store.close();

    const result = await runKmBackendWorkerOnce({ dataDir: dir, env: { BOTMUX_KM_BACKEND_WORKER_ENABLED: 'true', MEM0_TOKEN: 'token' } as any,
      holderId: 'daemon-a', now: 2000, leaseMs: 5000 });
    expect(result).toEqual(expect.objectContaining({
      enabled: true,
      leaseAcquired: true,
      enqueue: expect.objectContaining({ enqueued: 1 }),
      worker: expect.objectContaining({ claimed: 1, delivered: 1 }),
    }));
    const reopened = await ObservationStore.open(dir);
    expect(reopened.getMemory('mem-active')).toEqual(expect.objectContaining({ state: 'active', syncPolicy: 'local-only' }));
    expect(reopened.listMemoryBackendBindings('mem-active')).toEqual([expect.objectContaining({ providerId: 'mem0', writeState: 'active' })]);
    reopened.close();
  });

  it('keeps unavailable or timing-out providers bounded and quarantines at the ceiling', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    store.upsertMemory({ memoryId: 'mem-active', state: 'active', scope: 'user', subject: 'u1', claimKey: 'a', claimText: 'A',
      confidence: 'observed', privacyClass: 'internal', syncPolicy: 'local-only', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.enqueueMemoryBackendOperation({ memoryId: 'mem-active', providerId: 'openviking', operation: 'put',
      payload: { memoryId: 'mem-active', scope: 'user', subject: 'u1', claimKey: 'a', claimText: 'A', privacyClass: 'internal', sourceRefs: [], contentHash: `sha256:${'a'.repeat(64)}` }, now: 1000 });
    store.enqueueMemoryBackendOperation({ memoryId: 'mem-active', providerId: 'hindsight', operation: 'put',
      payload: { memoryId: 'mem-active', scope: 'user', subject: 'u1', claimKey: 'a', claimText: 'A', privacyClass: 'internal', sourceRefs: [], contentHash: `sha256:${'a'.repeat(64)}` }, now: 1000 });
    store.close();
    const never = new Promise<never>(() => {});
    const report = await import('../src/services/km/memory-backend-outbox-worker.js').then(module => module.drainMemoryBackendOutbox({
      dataDir: dir,
      providers: { hindsight: provider('hindsight', { put: async () => never }) },
      now: 2000,
      maxAttempts: 1,
      timeoutMs: 100,
    }));
    expect(report).toEqual(expect.objectContaining({ claimed: 2, delivered: 0, quarantined: 2 }));
    expect(report.failures.map(item => item.error).sort()).toEqual(['km_memory_backend_provider_unavailable', 'km_memory_backend_timeout:hindsight']);
    const reopened = await ObservationStore.open(dir);
    expect(reopened.listMemoryBackendOutbox(10).map(item => item.status)).toEqual(['quarantined', 'quarantined']);
    reopened.close();
  });
});
