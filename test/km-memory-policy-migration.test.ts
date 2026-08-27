import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { decideSafeMemoryActivation } from '../src/services/km/safe-memory-policy.js';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { drainMemoryBackendOutbox } from '../src/services/km/memory-backend-outbox-worker.js';
import { compareMemoryBackendMigration, enqueueMemoryBackendMigrationBackfill } from '../src/services/km/memory-backend-migration.js';
import type { MemoryBackendProvider } from '../src/services/km/memory-backend-spi.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-m4-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
const base = { confidence: 'observed' as const, explicitUserStatement: true, scope: 'user' as const, subject: 'u1', claimKey: 'output.language', claimText: 'Prefer Chinese', privacyClass: 'internal' as const, sourceRefs: [{ kind: 'api', ref: 'evt-1' }], evidenceSpanComplete: true };

describe('safe memory activation and backend migration', () => {
  it('auto-activates only explicit observed low-risk user/bot preferences', () => {
    const decision = decideSafeMemoryActivation(base, new Date('2026-01-01T00:00:00Z'));
    expect(decision).toEqual(expect.objectContaining({ disposition: 'activate', activationMode: 'policy-auto', policyVersion: 'safe-auto-activation-v1' }));
    expect(decision.memory).toEqual(expect.objectContaining({ state: 'active', reviewAfter: '2026-04-01T00:00:00.000Z' }));
  });

  it('demotes inferred, broad-scope and high-risk preferences to proposed', () => {
    expect(decideSafeMemoryActivation({ ...base, confidence: 'inferred' }).disposition).toBe('propose');
    expect(decideSafeMemoryActivation({ ...base, scope: 'team' }).disposition).toBe('propose');
    expect(decideSafeMemoryActivation({ ...base, policyTags: ['production-operation'] }).disposition).toBe('propose');
    expect(decideSafeMemoryActivation({ ...base, privacyClass: 'sensitive' }).disposition).toBe('reject');
  });

  it('durably deduplicates failed mirror operations in an outbox', async () => {
    const store = await ObservationStore.open(tempDir()); expect(store.schemaVersion()).toBe(15);
    const memory = store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Chinese', confidence: 'observed', privacyClass: 'internal', sourceRefs: base.sourceRefs }).item;
    const first = store.enqueueMemoryBackendOperation({ memoryId: memory.memoryId, providerId: 'hindsight', operation: 'put', payload: { text: 'Chinese' }, now: 1000 });
    expect(first.created).toBe(true);
    expect(store.enqueueMemoryBackendOperation({ memoryId: memory.memoryId, providerId: 'hindsight', operation: 'put', payload: { text: 'Chinese' }, now: 2000 })).toEqual({ outboxId: first.outboxId, created: false });
    store.close();
  });

  it('claims, leases, retries and settles memory backend outbox rows durably', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    const memory = store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Chinese', confidence: 'observed', privacyClass: 'internal', sourceRefs: base.sourceRefs }).item;
    const first = store.enqueueMemoryBackendOperation({ memoryId: memory.memoryId, providerId: 'mem0', operation: 'put',
      payload: { ...memory, contentHash: `sha256:${'b'.repeat(64)}` }, now: 1000 });
    const lost = store.claimMemoryBackendOutboxBatch({ limit: 1, now: 2000, leaseMs: 1000 });
    expect(lost.items).toHaveLength(1);
    const recovered = store.claimMemoryBackendOutboxBatch({ limit: 1, now: 4000, leaseMs: 1000 });
    expect(recovered.items[0].outboxId).toBe(first.outboxId);
    store.close();
    const provider: MemoryBackendProvider = {
      descriptor: { id: 'mem0', version: '1', kind: 'mem0', capabilities: { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false } },
      health: async () => ({ status: 'ok' }),
      put: async item => ({ providerId: 'mem0', backendRef: 'mem0-ref', contentHash: item.contentHash }),
      revoke: async () => {},
      retrieve: async () => [],
    };
    const report = await drainMemoryBackendOutbox({ dataDir: dir, providers: { mem0: provider }, now: 6000, leaseMs: 1000 });
    expect(report).toEqual(expect.objectContaining({ claimed: 1, delivered: 1, retried: 0 }));
    const reopened = await ObservationStore.open(dir);
    expect(reopened.listMemoryBackendOutbox(10)).toEqual([expect.objectContaining({ status: 'delivered', attempts: 3 })]);
    expect(reopened.listMemoryBackendBindings(memory.memoryId)).toEqual([expect.objectContaining({ providerId: 'mem0', writeState: 'active', backendRef: 'mem0-ref' })]);
    reopened.close();
  });

  it('keeps failing outbox work retryable before quarantining at the attempt ceiling', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    const memory = store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Chinese', confidence: 'observed', privacyClass: 'internal', sourceRefs: base.sourceRefs }).item;
    store.enqueueMemoryBackendOperation({ memoryId: memory.memoryId, providerId: 'hindsight', operation: 'put', payload: { ...memory }, now: 1000 });
    store.close();
    const provider: MemoryBackendProvider = {
      descriptor: { id: 'hindsight', version: '1', kind: 'hindsight', capabilities: { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false } },
      health: async () => ({ status: 'ok' }),
      put: async () => { throw new Error('down'); },
      revoke: async () => {},
      retrieve: async () => [],
    };
    expect(await drainMemoryBackendOutbox({ dataDir: dir, providers: { hindsight: provider }, now: 2000, maxAttempts: 2 })).toEqual(expect.objectContaining({ retried: 1, quarantined: 0 }));
    expect(await drainMemoryBackendOutbox({ dataDir: dir, providers: { hindsight: provider }, now: 4000, maxAttempts: 2 })).toEqual(expect.objectContaining({ retried: 0, quarantined: 1 }));
    const reopened = await ObservationStore.open(dir);
    expect(reopened.listMemoryBackendOutbox(10)).toEqual([expect.objectContaining({ status: 'quarantined', attempts: 2, lastError: 'down' })]);
    reopened.close();
  });

  it('enforces backfill/compare/ready/cutover/rollback migration order', async () => {
    const store = await ObservationStore.open(tempDir());
    const id = store.createMemoryBackendMigration({ botAppId: 'bot-1', fromProfile: { primary: 'mem0' }, toProfile: { primary: 'openviking' } });
    expect(() => store.transitionMemoryBackendMigration({ migrationId: id, toState: 'cutover' })).toThrow(/invalid_transition/);
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'backfilling', checkpoint: '0' });
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'comparing', checkpoint: '100', stats: { copied: 100 } });
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'ready', stats: { mismatch: 0 } });
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'cutover' });
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'rolled_back' });
    store.close();
  });

  it('resumes migration backfill from checkpoint and records compare results without cutover', async () => {
    const store = await ObservationStore.open(tempDir());
    const a = store.upsertMemory({ memoryId: 'mem-a', state: 'active', scope: 'user', subject: 'u1', claimKey: 'a', claimText: 'A', confidence: 'observed', privacyClass: 'internal', sourceRefs: base.sourceRefs }).item;
    const b = store.upsertMemory({ memoryId: 'mem-b', state: 'active', scope: 'user', subject: 'u1', claimKey: 'b', claimText: 'B', confidence: 'observed', privacyClass: 'internal', sourceRefs: base.sourceRefs }).item;
    store.upsertMemoryBackendBinding({ memoryId: a.memoryId, providerId: 'sqlite', providerVersion: '1', writeState: 'active', contentHash: 'hash-a' });
    store.upsertMemoryBackendBinding({ memoryId: b.memoryId, providerId: 'sqlite', providerVersion: '1', writeState: 'active', contentHash: 'hash-b' });
    const id = store.createMemoryBackendMigration({ botAppId: 'bot-1', fromProfile: { primary: 'sqlite' }, toProfile: { primary: 'openviking' } });
    expect(enqueueMemoryBackendMigrationBackfill({ store, migrationId: id, toProviderId: 'openviking', limit: 1, now: 1000 })).toEqual(expect.objectContaining({ scanned: 1, enqueued: 1, done: false, checkpoint: 'mem-a' }));
    expect(enqueueMemoryBackendMigrationBackfill({ store, migrationId: id, toProviderId: 'openviking', limit: 1, now: 2000 })).toEqual(expect.objectContaining({ scanned: 1, enqueued: 1, done: false, checkpoint: 'mem-b' }));
    expect(enqueueMemoryBackendMigrationBackfill({ store, migrationId: id, toProviderId: 'openviking', limit: 1, now: 3000 })).toEqual(expect.objectContaining({ scanned: 0, enqueued: 0, done: true, checkpoint: 'mem-b' }));
    store.upsertMemoryBackendBinding({ memoryId: a.memoryId, providerId: 'openviking', providerVersion: '1', writeState: 'shadow', contentHash: 'hash-a', backendRef: 'ov-a' });
    store.upsertMemoryBackendBinding({ memoryId: b.memoryId, providerId: 'openviking', providerVersion: '1', writeState: 'shadow', contentHash: 'hash-b', backendRef: 'ov-b' });
    const report = compareMemoryBackendMigration({ store, migrationId: id, fromProviderId: 'sqlite', toProviderId: 'openviking' });
    expect(report).toEqual(expect.objectContaining({ compared: 2, matched: 2, missing: 0, mismatched: 0 }));
    expect(store.getMemoryBackendMigration(id)).toEqual(expect.objectContaining({ state: 'ready', stats: expect.objectContaining({ compare: report }) }));
    store.close();
  });
});
