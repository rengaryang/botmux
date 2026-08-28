import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { redactObservationForSync } from '../src/services/km/sync-redaction.js';
import { MockSyncSinkProvider, runSyncOnce, signSyncBatch } from '../src/services/km/sync-worker.js';
import { centralSinkRuntimeStatus, evaluateCentralSinkEndpointPolicy, runKmCentralSinkOnce } from '../src/services/km/central-sink-runtime.js';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-sync-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
function event(overrides: Partial<ObservationEvent> = {}): ObservationEvent {
  return {
    schemaVersion: 1, eventId: 'evt-sync-1', eventType: 'turn.completed',
    source: { producer: 'turn', adapter: 'pi', resolverStatus: 'resolved', confidence: 'observed' },
    identity: { botAppId: 'bot', sessionId: 'session', turnId: 'turn' },
    ordering: { sourceKey: 'turn', idempotencyKey: 'turn-1', parentEventIds: [], observedAt: '2026-08-26T00:00:00.000Z' },
    provenance: { evidenceLevel: 'runtime', parserVersion: 'v1', sourceRefs: [{ kind: 'api', ref: 'turn/1' }], privacyClass: 'internal', redactionStatus: 'not_needed' },
    content: { hash: null, storageMode: 'none' }, payload: { status: 'completed', ignoredBody: 'not exported' },
    createdAt: '2026-08-26T00:00:01.000Z', ...overrides,
  };
}

describe('KM Phase 4 sync safety', () => {
  it('redacts to an allowlisted metadata envelope', () => {
    const result = redactObservationForSync(event());
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.envelope.payload).toEqual({ status: 'completed' });
      expect(JSON.stringify(result.envelope)).not.toContain('ignoredBody');
    }
  });

  it('blocks secret and pending-review content', () => {
    expect(redactObservationForSync(event({ payload: { status: 'failed', error: 'TOKEN=abc123456789' } }))).toEqual({ ok: false, reason: 'secret_detected' });
    expect(redactObservationForSync(event({ provenance: { ...event().provenance, redactionStatus: 'pending_review' } }))).toEqual({ ok: false, reason: 'redaction_pending_review' });
  });

  it('keeps sinks disabled by default and refuses enabling real endpoints', async () => {
    const store = await ObservationStore.open(tempDir());
    expect(store.schemaVersion()).toBe(17);
    expect(store.configureSyncSink({ sinkId: 'central', protocolVersion: 1, endpointRef: 'https://example.invalid' }))
      .toEqual(expect.objectContaining({ enabled: false, pending: 0, quarantined: 0 }));
    expect(() => store.configureSyncSink({ sinkId: 'real', protocolVersion: 1, endpointRef: 'https://example.invalid', enabled: true }))
      .toThrow(/real_sink_blocked_offline_runtime/);
    expect(store.configureSyncSink({ sinkId: 'mem', protocolVersion: 1, endpointRef: 'inmemory://central', enabled: true }))
      .toEqual(expect.objectContaining({ enabled: true, endpointPolicy: { ok: true, mode: 'offline' } }));
    expect(() => store.enqueueSync({ sinkId: 'central', eventId: 'evt-sync-1', payload: {}, payloadHash: `sha256:${'a'.repeat(64)}` }))
      .toThrow(/sink_disabled/);
    store.close();
  });

  it('signs and sends a bounded mock batch without real network I/O', async () => {
    const store = await ObservationStore.open(tempDir());
    store.append(event());
    store.configureSyncSink({ sinkId: 'mock', protocolVersion: 1, endpointRef: 'mock://local', enabled: true });
    const redacted = redactObservationForSync(event());
    if (!redacted.ok) throw new Error(redacted.reason);
    store.enqueueSync({ sinkId: 'mock', eventId: 'evt-sync-1', payload: redacted.envelope as any, payloadHash: redacted.envelope.payloadHash, now: 1_787_730_000_000 });
    const provider = new MockSyncSinkProvider();
    const result = await runSyncOnce({ store, sinkId: 'mock', sourceHostId: 'host-test', provider, signingSecret: 'test-secret', now: 1_787_730_000_000 });
    expect(result).toEqual({ attempted: 1, accepted: 1, quarantined: 0, status: 'accepted' });
    expect(provider.received[0].signature).toMatch(/^sha256=[a-f0-9]{64}$/);
    expect(signSyncBatch(provider.received[0].batch, 'test-secret')).toBe(provider.received[0].signature);
    expect(store.listSyncStatus()[0]).toEqual(expect.objectContaining({ pending: 0, lastLocalSeq: 1 }));
    store.close();
  });

  it('executes inmemory sinks offline and enforces payload size limits before send', async () => {
    const dataDir = tempDir();
    const store = await ObservationStore.open(dataDir);
    store.append(event());
    store.configureSyncSink({ sinkId: 'mem', protocolVersion: 1, endpointRef: 'inmemory://central', enabled: true, payloadMaxBytes: 1024 });
    await expect(runKmCentralSinkOnce({ dataDir, env: { BOTMUX_KM_CENTRAL_SINK_ENABLED: 'true' }, holderId: 'h1', now: 1_000 }))
      .resolves.toMatchObject({ scanned: 1, enqueued: 1, delivered: 1 });

    store.configureSyncSink({ sinkId: 'small', protocolVersion: 1, endpointRef: 'mock://small', enabled: true, payloadMaxBytes: 1024 });
    expect(() => store.enqueueSync({
      sinkId: 'small',
      eventId: 'evt-sync-1',
      payload: { text: 'x'.repeat(2_000) },
      payloadHash: `sha256:${'b'.repeat(64)}`,
    })).toThrow(/payload_too_large/);
    store.close();
  });

  it('runs the central sink scheduler only when enabled and under a durable lease', async () => {
    const dataDir = tempDir();
    const store = await ObservationStore.open(dataDir);
    store.append(event());
    store.configureSyncSink({
      sinkId: 'central',
      protocolVersion: 1,
      endpointRef: 'mock://central',
      enabled: true,
      credentialRef: 'env:BOTMUX_KM_CENTRAL_SINK_SECRET',
      batchLimit: 10,
      timeoutMs: 500,
    });
    store.close();

    await expect(centralSinkRuntimeStatus({ dataDir, env: {} })).resolves.toMatchObject({
      enabled: false,
      protocol: { envelopeVersion: 1, realTransportEnabled: false, networkLibrariesAllowed: false },
      rollback: { automaticRemoteRollback: false, localDisableOnly: true },
      sinks: [expect.objectContaining({ credentialRef: 'env:***', endpointPolicy: { ok: true, mode: 'offline' } })],
    });

    await expect(runKmCentralSinkOnce({ dataDir, env: {}, holderId: 'h1', now: 1_000 })).resolves.toMatchObject({
      enabled: false,
      leaseAcquired: false,
      delivered: 0,
    });

    const locked = await ObservationStore.open(dataDir);
    expect(locked.acquireRuntimeLease({ leaseName: 'km-central-sink', holderId: 'other-daemon', now: 1_000, ttlMs: 60_000 })).toBe(true);
    await expect(runKmCentralSinkOnce({ dataDir, env: { BOTMUX_KM_CENTRAL_SINK_ENABLED: 'true', BOTMUX_KM_CENTRAL_SINK_SECRET: 'secret' }, holderId: 'h1', now: 1_000 })).resolves.toMatchObject({
      enabled: true,
      leaseAcquired: false,
      delivered: 0,
    });
    locked.releaseRuntimeLease({ leaseName: 'km-central-sink', holderId: 'other-daemon' });
    locked.close();

    await expect(runKmCentralSinkOnce({ dataDir, env: { BOTMUX_KM_CENTRAL_SINK_ENABLED: 'true', BOTMUX_KM_CENTRAL_SINK_SECRET: 'secret' }, holderId: 'h1', now: 1_000 })).resolves.toMatchObject({
      enabled: true,
      leaseAcquired: true,
      scanned: 1,
      enqueued: 1,
      delivered: 1,
    });
    const after = await ObservationStore.open(dataDir);
    expect(after.listSyncStatus()[0]).toEqual(expect.objectContaining({ pending: 0, delivered: 1, lastLocalSeq: 1 }));
    after.close();
  });

  it('applies partial acknowledgements and resumes from the cursor without duplicating delivered events', async () => {
    const dataDir = tempDir();
    const store = await ObservationStore.open(dataDir);
    store.append(event({ eventId: 'evt-sync-1', ordering: { ...event().ordering, idempotencyKey: 'turn-1' } }));
    store.append(event({ eventId: 'evt-sync-2', ordering: { ...event().ordering, idempotencyKey: 'turn-2' } }));
    store.configureSyncSink({ sinkId: 'central', protocolVersion: 1, endpointRef: 'mock://central', enabled: true, batchLimit: 10 });
    store.close();

    const first = await runKmCentralSinkOnce({ dataDir, env: { BOTMUX_KM_CENTRAL_SINK_ENABLED: 'true' }, holderId: 'h1', now: 2_000, scenario: 'partial' });
    expect(first).toEqual(expect.objectContaining({ scanned: 2, enqueued: 2, delivered: 1, quarantined: 1 }));
    const afterPartial = await ObservationStore.open(dataDir);
    expect(afterPartial.listSyncStatus()[0]).toEqual(expect.objectContaining({ pending: 0, delivered: 1, quarantined: 1, lastLocalSeq: 2 }));
    afterPartial.close();

    const second = await runKmCentralSinkOnce({ dataDir, env: { BOTMUX_KM_CENTRAL_SINK_ENABLED: 'true' }, holderId: 'h1', now: 3_000 });
    expect(second).toEqual(expect.objectContaining({ scanned: 0, enqueued: 0, delivered: 0 }));
  });

  it('quarantines after bounded retry attempts and keeps real endpoints fail-closed without fetch', async () => {
    expect(evaluateCentralSinkEndpointPolicy('https://central.example.invalid/ingest'))
      .toEqual({ ok: false, mode: 'blocked-real', reason: 'offline_runtime_allows_mock_or_inmemory_only' });
    const fetchSpy = vi.spyOn(globalThis, 'fetch');
    const dataDir = tempDir();
    const store = await ObservationStore.open(dataDir);
    store.append(event());
    store.configureSyncSink({ sinkId: 'central', protocolVersion: 1, endpointRef: 'mock://central', enabled: true, maxAttempts: 2 });
    store.close();

    await expect(runKmCentralSinkOnce({ dataDir, env: { BOTMUX_KM_CENTRAL_SINK_ENABLED: 'true' }, holderId: 'h1', now: 1_000, scenario: 'error' }))
      .resolves.toMatchObject({ retried: 1 });
    await expect(runKmCentralSinkOnce({ dataDir, env: { BOTMUX_KM_CENTRAL_SINK_ENABLED: 'true' }, holderId: 'h1', now: 2_000, scenario: 'error' }))
      .resolves.toMatchObject({ quarantined: 1 });
    expect(fetchSpy).not.toHaveBeenCalled();
    fetchSpy.mockRestore();
    const after = await ObservationStore.open(dataDir);
    expect(after.listSyncStatus()[0]).toEqual(expect.objectContaining({ failed: 0, quarantined: 1 }));
    after.close();
  });

  it('retries failed claims with exponential backoff', async () => {
    const store = await ObservationStore.open(tempDir());
    store.append(event());
    store.configureSyncSink({ sinkId: 'mock', protocolVersion: 1, endpointRef: 'mock://local', enabled: true });
    const redacted = redactObservationForSync(event()); if (!redacted.ok) throw new Error(redacted.reason);
    store.enqueueSync({ sinkId: 'mock', eventId: 'evt-sync-1', payload: redacted.envelope as any, payloadHash: redacted.envelope.payloadHash, now: 1000 });
    const provider = { send: async () => { throw new Error('temporary'); } };
    await expect(runSyncOnce({ store, sinkId: 'mock', sourceHostId: 'host', provider, signingSecret: 'secret', now: 1000 })).rejects.toThrow('temporary');
    expect(store.claimSyncBatch({ sinkId: 'mock', limit: 10, now: 1500 }).items).toHaveLength(0);
    expect(store.claimSyncBatch({ sinkId: 'mock', limit: 10, now: 2000 }).items).toHaveLength(1);
    store.close();
  });

  it('supports local mock enqueue, idempotent ack, cursor and quarantine', async () => {
    const store = await ObservationStore.open(tempDir());
    store.append(event());
    store.configureSyncSink({ sinkId: 'mock', protocolVersion: 1, endpointRef: 'mock://local', enabled: true });
    const redacted = redactObservationForSync(event());
    if (!redacted.ok) throw new Error(redacted.reason);
    expect(store.enqueueSync({ sinkId: 'mock', eventId: 'evt-sync-1', payload: redacted.envelope as any, payloadHash: redacted.envelope.payloadHash }).created).toBe(true);
    expect(store.enqueueSync({ sinkId: 'mock', eventId: 'evt-sync-1', payload: redacted.envelope as any, payloadHash: redacted.envelope.payloadHash }).created).toBe(false);
    store.acknowledgeSync({ sinkId: 'mock', batchId: 'batch-1', acceptedEventIds: ['evt-sync-1'], centralCursor: 'cursor-1' });
    expect(store.listSyncStatus()[0]).toEqual(expect.objectContaining({ lastLocalSeq: 1, lastBatchId: 'batch-1', centralCursor: 'cursor-1', pending: 0 }));
    store.quarantineSync({ sinkId: 'mock', reason: 'schema_unsupported', payloadHash: redacted.envelope.payloadHash });
    expect(store.listSyncStatus()[0].quarantined).toBe(1);
    store.close();
  });
});
