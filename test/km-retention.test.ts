import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import {
  isKmRetentionShadowEnabled,
  kmRetentionRuntimeStatus,
  runKmRetentionShadowOnce,
} from '../src/services/km/retention-runtime.js';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';
import { KM_RETENTION_DOMAINS } from '../src/services/km/retention-policy.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-retention-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

function event(id: string, createdAt: string, overrides: Partial<ObservationEvent> = {}): ObservationEvent {
  return {
    schemaVersion: 1,
    eventId: id,
    eventType: 'turn.completed',
    source: { producer: 'turn', adapter: 'pi', resolverStatus: 'resolved', confidence: 'observed' },
    identity: { botAppId: 'bot', sessionId: 'session', turnId: id },
    ordering: { sourceKey: 'turn', idempotencyKey: id, sourceSeq: 1, parentEventIds: [], observedAt: createdAt },
    provenance: { evidenceLevel: 'runtime', parserVersion: 'v1', sourceRefs: [{ kind: 'api', ref: id }], privacyClass: 'internal', redactionStatus: 'not_needed' },
    content: { hash: null, storageMode: 'none' },
    payload: { status: 'completed' },
    createdAt,
    ...overrides,
  };
}

async function sqlite(dataDir: string) {
  const { DatabaseSync } = await import('node:sqlite');
  return new DatabaseSync(join(dataDir, 'botmux-km.sqlite'));
}

async function tableCounts(dataDir: string): Promise<Record<string, number>> {
  const db = await sqlite(dataDir);
  try {
    return Object.fromEntries([
      'observation_events',
      'knowledge_items',
      'memory_items',
      'retrieval_runs',
      'prompt_injection_snapshots',
      'trace_edges',
      'eval_runs',
      'evolution_proposals',
      'distillation_jobs',
      'sync_outbox',
      'memory_backend_outbox',
      'quarantine_events',
      'sync_quarantine',
      'km_retention_reports',
    ].map(table => [table, Number((db.prepare(`SELECT COUNT(*) count FROM ${table}`).get() as any).count)]));
  } finally {
    db.close();
  }
}

async function seedRetentionFixture(dataDir: string): Promise<void> {
  const store = await ObservationStore.open(dataDir);
  store.append(event('evt-old-free', '2024-01-01T00:00:00.000Z'));
  store.append(event('evt-old-held', '2024-01-02T00:00:00.000Z', {
    eventId: 'evt-old-held',
    ordering: { sourceKey: 'turn', idempotencyKey: 'evt-old-held', sourceSeq: 2, parentEventIds: [], observedAt: '2024-01-02T00:00:00.000Z' },
    provenance: { ...event('x', '2024-01-02T00:00:00.000Z').provenance, sourceRefs: [{ kind: 'api', ref: 'legal-hold' }] },
  }));
  store.append(event('evt-new', '2026-08-20T00:00:00.000Z', {
    eventId: 'evt-new',
    ordering: { sourceKey: 'turn', idempotencyKey: 'evt-new', sourceSeq: 3, parentEventIds: ['evt-old-free'], observedAt: '2026-08-20T00:00:00.000Z' },
  }));
  store.append(event('evt-conflict', '2024-01-03T00:00:00.000Z', {
    eventId: 'evt-conflict',
    ordering: { sourceKey: 'turn', idempotencyKey: 'evt-old-free', sourceSeq: 4, parentEventIds: [], observedAt: '2024-01-03T00:00:00.000Z' },
    payload: { status: 'failed' },
  }));
  const knowledge = store.proposeKnowledge({
    knowledgeId: 'kn-old',
    targetLayer: 'L3',
    category: 'ops',
    title: 'Old rejected knowledge',
    claimKey: 'retention.old',
    claimText: 'Old rejected knowledge may be eligible.',
    confidence: 'observed',
    freshness: 'stale',
    privacyClass: 'internal',
    sourceRefs: [{ kind: 'api', ref: 'evt-old-free' }],
  });
  store.transitionKnowledge({ knowledgeId: knowledge.item.knowledgeId, toState: 'rejected', reasonCode: 'test_reject', actorId: 'reviewer' });
  store.proposeKnowledge({
    knowledgeId: 'kn-active',
    targetLayer: 'L3',
    category: 'ops',
    title: 'Approved knowledge',
    claimKey: 'retention.active',
    claimText: 'Approved knowledge is protected.',
    confidence: 'observed',
    freshness: 'fresh',
    privacyClass: 'internal',
    sourceRefs: [{ kind: 'api', ref: 'evt-old-free' }],
  });
  store.transitionKnowledge({ knowledgeId: 'kn-active', toState: 'review_pending', reasonCode: 'review', actorId: 'reviewer' });
  store.transitionKnowledge({ knowledgeId: 'kn-active', toState: 'approved', reasonCode: 'approve', actorId: 'reviewer' });
  store.upsertMemory({ memoryId: 'mem-old', state: 'revoked', scope: 'user', subject: 'u1', claimKey: 'old', claimText: 'Old revoked memory',
    confidence: 'observed', privacyClass: 'internal', sourceRefs: [{ kind: 'api', ref: 'evt-old-free' }] });
  store.upsertMemory({ memoryId: 'mem-active', state: 'active', scope: 'user', subject: 'u1', claimKey: 'active', claimText: 'Active memory',
    confidence: 'observed', privacyClass: 'internal', sourceRefs: [{ kind: 'api', ref: 'evt-old-free' }] });
  const retrievalRunId = store.recordRetrievalAudit({ botAppId: 'bot', sessionId: 's1', queryHash: 'sha256:abc', mode: 'shadow',
    candidateCount: 0, eligibleCount: 0, latencyMs: 10, warnings: [], results: [] });
  store.recordPromptInjectionSnapshot({ retrievalRunId, botAppId: 'bot', mode: 'shadow', disposition: 'would_inject', itemIds: [], prompt: '' });
  store.addTraceEdge({ fromType: 'turn', fromId: 'old', toType: 'memory', toId: 'mem-old', edgeType: 'used' });
  store.recordEval({ evaluatorName: 'test', evaluatorVersion: '1', targetType: 'workflow-artifact', targetId: 'artifact-old',
    results: [{ metricKey: 'quality', score: 0, verdict: 'fail', confidence: 'observed', sourceRefs: [{ kind: 'api', ref: 'evt-old-free' }] }] });
  store.createEvolutionProposal({ proposalType: 'cleanup-action', targetRef: 'retention', approvalGrade: 'G2', summary: 'Old cleanup draft',
    evidenceRefs: [{ kind: 'api', ref: 'evt-old-free' }], proposedAction: { kind: 'preview' }, risk: {}, rollback: {}, createdBy: 'test' });
  store.createDistillationJob({ sourceEventId: 'evt-old-free', profile: {
    schemaVersion: 1, profileId: 'retention', revision: 1, botAppId: 'bot',
    sourceProvider: 'observation-source-v1', windowProvider: 'bounded-transcript-window-v1',
    primaryExtractor: 'builtin.rules-v1', shadowExtractors: [], knowledgeRouter: 'builtin.layer-router-v1',
    memoryPolicy: 'safe-auto-activation-v1',
    memoryBackends: { writePolicy: 'single', primary: 'sqlite', mirrors: [] },
    injectionMode: 'shadow', budgets: { sourceBytes: 1024, sourceTokens: 100, outputClaims: 5, promptTokens: 100 },
  }, now: Date.parse('2024-01-01T00:00:00.000Z') });
  store.configureSyncSink({ sinkId: 'mock', protocolVersion: 1, endpointRef: 'mock://local', enabled: true });
  store.enqueueSync({ sinkId: 'mock', eventId: 'evt-old-free', payload: {}, payloadHash: `sha256:${'a'.repeat(64)}`, now: 1 });
  store.enqueueMemoryBackendOperation({ memoryId: 'mem-active', providerId: 'mem0', operation: 'put', payload: { memoryId: 'mem-active' }, now: 1 });
  store.quarantineSync({ sinkId: 'mock', reason: 'bad_payload', payloadHash: `sha256:${'b'.repeat(64)}` });
  store.close();

  const db = await sqlite(dataDir);
  try {
    db.prepare('UPDATE knowledge_items SET updated_at=? WHERE knowledge_id IN (?,?)').run('2024-01-01T00:00:00.000Z', 'kn-old', 'kn-active');
    db.prepare('UPDATE memory_items SET updated_at=? WHERE memory_id IN (?,?)').run('2024-01-01T00:00:00.000Z', 'mem-old', 'mem-active');
    db.prepare('UPDATE retrieval_runs SET created_at=?').run('2024-01-01T00:00:00.000Z');
    db.prepare('UPDATE prompt_injection_snapshots SET created_at=?').run('2024-01-01T00:00:00.000Z');
    db.prepare('UPDATE trace_edges SET created_at=?').run('2024-01-01T00:00:00.000Z');
    db.prepare('UPDATE eval_runs SET updated_at=?').run('2024-01-01T00:00:00.000Z');
    db.prepare('UPDATE evolution_proposals SET updated_at=?').run('2024-01-01T00:00:00.000Z');
    db.prepare('UPDATE distillation_jobs SET state=?,updated_at=?').run('completed', '2024-01-01T00:00:00.000Z');
    db.prepare('UPDATE sync_outbox SET status=?,created_at=?').run('delivered', '2024-01-01T00:00:00.000Z');
    db.prepare('UPDATE memory_backend_outbox SET status=?,updated_at=?').run('delivered', '2024-01-01T00:00:00.000Z');
  } finally {
    db.close();
  }
}

describe('KM retention shadow preview', () => {
  it('stays default-off and exposes read-only runtime status', async () => {
    const dir = tempDir();
    await seedRetentionFixture(dir);
    expect(isKmRetentionShadowEnabled({} as any)).toBe(false);
    await expect(kmRetentionRuntimeStatus({ dataDir: dir, env: {} as any, now: Date.parse('2026-08-27T00:00:00.000Z') }))
      .resolves.toEqual(expect.objectContaining({
        enabled: false,
        leaseName: 'km-retention-shadow',
        latestPlan: expect.objectContaining({ dryRunOnly: true, destructiveActionsAvailable: false }),
      }));
  });

  it('computes deterministic tiered previews for every KM domain and excludes protected rows', async () => {
    const dir = tempDir();
    await seedRetentionFixture(dir);
    const now = Date.parse('2026-08-27T00:00:00.000Z');
    const store = await ObservationStore.open(dir);
    const first = store.kmRetentionPreview({ now });
    const second = store.kmRetentionPreview({ now });
    expect(first).toEqual(second);
    expect(first.domains.map(item => item.domain)).toEqual([...KM_RETENTION_DOMAINS]);
    expect(first.dryRunOnly).toBe(true);
    expect(first.destructiveActionsAvailable).toBe(false);
    expect(first.domains.find(item => item.domain === 'knowledge')).toEqual(expect.objectContaining({
      totalCount: 2,
      eligibleCount: 1,
      protectedCount: 1,
    }));
    expect(first.domains.find(item => item.domain === 'memory')).toEqual(expect.objectContaining({
      totalCount: 2,
      eligibleCount: 1,
      protectedCount: 1,
    }));
    expect(first.domains.find(item => item.domain === 'retrieval')).toEqual(expect.objectContaining({
      totalCount: 1,
      eligibleCount: 0,
      protectedCount: 1,
    }));
    expect(first.domains.find(item => item.domain === 'sync-outbox')).toEqual(expect.objectContaining({
      totalCount: 1,
      eligibleCount: 1,
      protectedCount: 0,
    }));
    expect(first.domains.find(item => item.domain === 'quarantine-evidence')).toEqual(expect.objectContaining({
      eligibleCount: 0,
      protectedCount: 1,
    }));
    expect(first.slo.map(item => item.key)).toContain('km.quarantine.rows');
    expect(first.slo.find(item => item.key === 'km.quarantine.rows')).toEqual(expect.objectContaining({ state: 'warn', value: 2 }));
    store.close();
  });

  it('records append-only shadow reports without mutating eligible data', async () => {
    const dir = tempDir();
    await seedRetentionFixture(dir);
    const before = await tableCounts(dir);
    const report = await runKmRetentionShadowOnce({ dataDir: dir, env: { BOTMUX_KM_RETENTION_SHADOW_ENABLED: 'true' } as any,
      holderId: 'daemon-a', now: Date.parse('2026-08-27T00:00:00.000Z'), leaseMs: 5_000 });
    expect(report).toEqual(expect.objectContaining({ enabled: true, leaseAcquired: true, report: expect.objectContaining({ totalEligible: expect.any(Number) }) }));
    const after = await tableCounts(dir);
    expect(after).toEqual({ ...before, km_retention_reports: before.km_retention_reports + 1 });
    const store = await ObservationStore.open(dir);
    expect(store.listKmRetentionReports(1)).toEqual([expect.objectContaining({ reportId: report.report?.reportId, reportHash: report.report?.reportHash })]);
    store.close();
  });

  it('uses the durable lease so only one daemon writes a shadow report', async () => {
    const dir = tempDir();
    await seedRetentionFixture(dir);
    const store = await ObservationStore.open(dir);
    expect(store.acquireRuntimeLease({ leaseName: 'km-retention-shadow', holderId: 'daemon-a', now: 1000, ttlMs: 10_000 })).toBe(true);
    store.close();
    const skipped = await runKmRetentionShadowOnce({ dataDir: dir, env: { BOTMUX_KM_RETENTION_SHADOW_ENABLED: 'true' } as any,
      holderId: 'daemon-b', now: 2000, leaseMs: 10_000 });
    expect(skipped).toEqual(expect.objectContaining({ enabled: true, leaseAcquired: false }));
    const counts = await tableCounts(dir);
    expect(counts.km_retention_reports).toBe(0);
  });

  it('keeps report history queries bounded and has no purge/apply retention path', async () => {
    const dir = tempDir();
    await seedRetentionFixture(dir);
    const store = await ObservationStore.open(dir);
    for (let i = 0; i < 120; i += 1) {
      store.recordKmRetentionShadowReport({ holderId: `daemon-${i}`, now: Date.parse('2026-08-27T00:00:00.000Z') + i * 1000 });
    }
    expect(store.listKmRetentionReports(500)).toHaveLength(100);
    expect('purgeRetrievalAudit' in store).toBe(false);
    store.close();
    expect(readFileSync(join(process.cwd(), 'src/services/km/retention-runtime.ts'), 'utf8')).not.toMatch(/\bdelete|DELETE|unlink|rmSync|purge|apply/i);
  });
});
