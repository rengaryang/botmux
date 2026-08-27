import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { redactObservationForSync } from '../src/services/km/sync-redaction.js';
import { MockSyncSinkProvider, runSyncOnce, signSyncBatch } from '../src/services/km/sync-worker.js';
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
    expect(store.schemaVersion()).toBe(13);
    expect(store.configureSyncSink({ sinkId: 'central', protocolVersion: 1, endpointRef: 'https://example.invalid' }))
      .toEqual(expect.objectContaining({ enabled: false, pending: 0, quarantined: 0 }));
    expect(() => store.configureSyncSink({ sinkId: 'real', protocolVersion: 1, endpointRef: 'https://example.invalid', enabled: true }))
      .toThrow(/explicit_external_approval/);
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
