import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';

const dirs: string[] = [];
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-phase2-'));
  dirs.push(dir);
  return dir;
}
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const sourceRefs = [{ kind: 'api', ref: 'test/evidence-1' }];

describe('KM Phase 2 knowledge and memory store', () => {
  it('migrates a v1 observation store additively through the latest schema', async () => {
    const dir = tempDir();
    const { DatabaseSync } = await import('node:sqlite');
    const db = new DatabaseSync(join(dir, 'botmux-km.sqlite'));
    db.exec(`
      CREATE TABLE observation_events(event_id TEXT PRIMARY KEY,schema_version INTEGER,event_type TEXT,source_key TEXT,idempotency_key TEXT,payload_hash TEXT,local_seq INTEGER UNIQUE,observed_at TEXT,created_at TEXT,event_json TEXT,UNIQUE(source_key,idempotency_key));
      CREATE TABLE observation_parents(event_id TEXT,parent_event_id TEXT,PRIMARY KEY(event_id,parent_event_id));
      CREATE TABLE content_blobs(content_hash TEXT PRIMARY KEY,storage_mode TEXT,content_ref TEXT,bytes INTEGER,created_at TEXT);
      CREATE TABLE producer_checkpoints(producer_name TEXT,adapter TEXT,cursor TEXT,updated_at TEXT,PRIMARY KEY(producer_name,adapter));
      CREATE TABLE sync_outbox(outbox_id TEXT PRIMARY KEY,event_id TEXT,sink_id TEXT,status TEXT,attempts INTEGER,next_attempt_at INTEGER,claimed_at INTEGER,claim_token TEXT,last_error TEXT,delivered_at TEXT,created_at TEXT,UNIQUE(event_id,sink_id));
      CREATE TABLE quarantine_events(quarantine_id TEXT PRIMARY KEY,event_id TEXT,source_key TEXT,idempotency_key TEXT,reason TEXT,existing_event_id TEXT,existing_payload_hash TEXT,incoming_payload_hash TEXT,event_json TEXT,created_at TEXT);
      CREATE TABLE local_sequence_counter(name TEXT PRIMARY KEY,value INTEGER);
      INSERT INTO local_sequence_counter VALUES('observation_events',0);
      PRAGMA user_version=1;
    `);
    db.close();
    const store = await ObservationStore.open(dir);
    expect(store.schemaVersion()).toBe(12);
    expect(store.listKnowledge({ limit: 10 })).toEqual([]);
    store.close();
  });

  it('creates review-only knowledge candidates and enforces transitions', async () => {
    const store = await ObservationStore.open(tempDir());
    const created = store.proposeKnowledge({
      targetLayer: 'L2', category: 'troubleshooting', title: 'Retry capacity errors',
      claimKey: 'model.capacity.retry', claimText: 'Fail over after two capacity errors.',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    });
    expect(created.created).toBe(true);
    expect(created.item.state).toBe('candidate');
    expect(store.proposeKnowledge({
      targetLayer: 'L2', category: 'troubleshooting', title: 'Duplicate title',
      claimKey: 'model.capacity.retry', claimText: 'Fail over after two capacity errors.',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    }).created).toBe(false);
    const pending = store.transitionKnowledge({ knowledgeId: created.item.knowledgeId, toState: 'review_pending', reasonCode: 'ready', actorId: 'reviewer' });
    expect(pending.state).toBe('review_pending');
    const approved = store.transitionKnowledge({ knowledgeId: created.item.knowledgeId, toState: 'approved', reasonCode: 'verified', actorId: 'human-1' });
    expect(approved.state).toBe('approved');
    expect(() => store.transitionKnowledge({ knowledgeId: created.item.knowledgeId, toState: 'candidate', reasonCode: 'bad', actorId: 'human-1' })).toThrow(/invalid_transition/);
    store.close();
  });

  it('does not let inferred knowledge self-approve', async () => {
    const store = await ObservationStore.open(tempDir());
    const item = store.proposeKnowledge({
      targetLayer: 'reviewed-only', category: 'inference', title: 'Maybe useful', claimKey: 'maybe', claimText: 'An inferred claim.',
      confidence: 'inferred', privacyClass: 'internal', sourceRefs,
    }).item;
    store.transitionKnowledge({ knowledgeId: item.knowledgeId, toState: 'review_pending', reasonCode: 'queued', actorId: 'system' });
    expect(() => store.transitionKnowledge({ knowledgeId: item.knowledgeId, toState: 'approved', reasonCode: 'auto', actorId: 'system' }))
      .toThrow(/requires_human_review/);
    store.close();
  });

  it('keeps export as a G2 dry-run and never executes it', async () => {
    const store = await ObservationStore.open(tempDir());
    const reviewedOnly = store.proposeKnowledge({
      targetLayer: 'reviewed-only', category: 'note', title: 'Reference only', claimKey: 'note', claimText: 'Do not export.',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    }).item;
    expect(store.knowledgeExportDryRun(reviewedOnly.knowledgeId)).toEqual(expect.objectContaining({ allowed: false, reason: 'reviewed_only_not_exportable', requiredApprovalGrade: 'G2' }));

    const exportable = store.proposeKnowledge({
      targetLayer: 'L2', category: 'sop', title: 'Approved SOP', claimKey: 'sop', claimText: 'Reviewed procedure.',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    }).item;
    expect(store.knowledgeExportDryRun(exportable.knowledgeId).allowed).toBe(false);
    store.transitionKnowledge({ knowledgeId: exportable.knowledgeId, toState: 'review_pending', reasonCode: 'ready', actorId: 'human' });
    store.transitionKnowledge({ knowledgeId: exportable.knowledgeId, toState: 'approved', reasonCode: 'approved', actorId: 'human' });
    expect(store.knowledgeExportDryRun(exportable.knowledgeId)).toEqual(expect.objectContaining({ allowed: true, requiredApprovalGrade: 'G2', risk: { mutatesWorkspace: true, automaticExecution: false } }));
    store.close();
  });

  it('tracks multiple external backend bindings without changing the logical memory id', async () => {
    const store = await ObservationStore.open(tempDir());
    const memory = store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Chinese',
      confidence: 'observed', privacyClass: 'internal', sourceRefs }).item;
    for (const providerId of ['mem0', 'hindsight', 'openviking']) store.upsertMemoryBackendBinding({
      memoryId: memory.memoryId, providerId, providerVersion: '1', backendRef: `${providerId}-ref`, writeState: 'active', contentHash: `sha256:${'a'.repeat(64)}`,
    });
    expect(store.listMemoryBackendBindings(memory.memoryId).map(item => item.providerId)).toEqual(['hindsight', 'mem0', 'openviking']);
    store.close();
  });

  it('supports human-reviewed memory approve, conflict, revoke transitions', async () => {
    const store = await ObservationStore.open(tempDir());
    const memory = store.upsertMemory({
      state: 'proposed', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Chinese',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    }).item;
    const rejected = store.upsertMemory({
      state: 'proposed', scope: 'user', subject: 'u2', claimKey: 'language', claimText: 'English',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    }).item;
    expect(() => store.transitionMemory({ memoryId: memory.memoryId, toState: 'active', reasonCode: 'auto', actorId: 'system' }))
      .toThrow(/activation_requires_human_review/);
    expect(store.transitionMemory({ memoryId: rejected.memoryId, toState: 'revoked', reasonCode: 'reject', actorId: 'human-1' }).state)
      .toBe('revoked');
    expect(store.transitionMemory({ memoryId: memory.memoryId, toState: 'active', reasonCode: 'reviewed', actorId: 'human-1' }).state)
      .toBe('active');
    expect(store.transitionMemory({ memoryId: memory.memoryId, toState: 'conflicted', reasonCode: 'conflict', actorId: 'human-1' }).state)
      .toBe('conflicted');
    expect(store.transitionMemory({ memoryId: memory.memoryId, toState: 'revoked', reasonCode: 'reject', actorId: 'human-1' }).state)
      .toBe('revoked');
    expect(() => store.transitionMemory({ memoryId: memory.memoryId, toState: 'active', reasonCode: 'bad', actorId: 'human-1' }))
      .toThrow(/invalid_transition/);
    store.close();
  });

  it('registers builtin KM providers with versions and capabilities', async () => {
    const store = await ObservationStore.open(tempDir());
    const providers = store.listKmProviders();
    expect(providers).toEqual(expect.arrayContaining([
      expect.objectContaining({ providerId: 'observation-source-v1', kind: 'source', version: '1' }),
      expect.objectContaining({ providerId: 'bounded-transcript-window-v1', kind: 'window-resolver', version: '1' }),
      expect.objectContaining({ providerId: 'builtin.rules-v1', kind: 'extractor', version: '1' }),
      expect.objectContaining({ providerId: 'safe-auto-activation-v1', kind: 'memory-policy', version: '1' }),
      expect.objectContaining({ providerId: 'mem0', kind: 'memory-backend', version: '1' }),
    ]));
    expect(providers.find(provider => provider.providerId === 'builtin.rules-v1')?.descriptor).toEqual(expect.objectContaining({
      capabilities: expect.arrayContaining(['explicit-user-preferences', 'mechanical-attribution-only']),
    }));
    store.close();
  });

  it('detects conflicting memory and excludes non-active memory from retrieval', async () => {
    const store = await ObservationStore.open(tempDir());
    const first = store.upsertMemory({
      state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Chinese',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    });
    expect(first.item.state).toBe('active');
    const conflict = store.upsertMemory({
      scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'English',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    });
    expect(conflict.conflicted).toBe(true);
    expect(store.retrieve({ text: 'language', scopes: ['user'], subject: 'u1', limit: 10 })).toEqual([]);
    store.close();
  });

  it('retrieves approved knowledge and active, unexpired memory only', async () => {
    const store = await ObservationStore.open(tempDir());
    const knowledge = store.proposeKnowledge({
      targetLayer: 'L3', category: 'skill', title: 'Capacity failover', claimKey: 'capacity.failover', claimText: 'Switch model after two capacity failures.',
      confidence: 'observed', freshness: 'fresh', privacyClass: 'internal', sourceRefs,
    }).item;
    store.transitionKnowledge({ knowledgeId: knowledge.knowledgeId, toState: 'review_pending', reasonCode: 'ready', actorId: 'human-1' });
    store.transitionKnowledge({ knowledgeId: knowledge.knowledgeId, toState: 'approved', reasonCode: 'verified', actorId: 'human-1' });
    store.upsertMemory({
      state: 'active', scope: 'user', subject: 'u1', claimKey: 'failover.preference', claimText: 'Prefer early model failover.',
      confidence: 'observed', privacyClass: 'internal', sourceRefs,
    });
    store.upsertMemory({
      state: 'active', scope: 'user', subject: 'u2', claimKey: 'expired', claimText: 'Old failover preference.',
      confidence: 'observed', privacyClass: 'internal', sourceRefs, ttlExpiresAt: '2020-01-01T00:00:00.000Z',
    });
    const results = store.retrieve({ text: 'failover', scopes: ['user'], targetLayers: ['L3'], limit: 10 });
    expect(results.map(result => result.kind)).toEqual(['knowledge', 'memory']);
    expect(results.every(result => result.sourceRefs.length > 0)).toBe(true);
    store.close();
  });
});
