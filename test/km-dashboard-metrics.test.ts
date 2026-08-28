import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { handleKmObservationApi } from '../src/dashboard/km-observation-api.js';
import { ObservationStore, type KnowledgeItem } from '../src/services/km/observation-store.js';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';

const dirs: string[] = [];
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-dashboard-metrics-'));
  dirs.push(dir);
  return dir;
}
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const sourceRefs: ObservationEvent['provenance']['sourceRefs'] = [{ kind: 'api', ref: 'test/evidence-1' }];
const fixedNow = Date.parse('2026-08-28T12:00:00.000Z');

function response() {
  const bodies: unknown[] = [];
  const res = {
    writeHead: vi.fn(),
    end: vi.fn(value => bodies.push(JSON.parse(String(value)))),
  } as any;
  return { res, bodies };
}

function event(overrides: Partial<ObservationEvent> = {}): ObservationEvent {
  const now = '2026-08-28T09:00:00.000Z';
  return {
    schemaVersion: 1,
    eventId: 'evt-dashboard-1',
    eventType: 'skill.invoked',
    source: {
      producer: 'botmux',
      adapter: 'unit',
      transcriptKind: null,
      nativeSessionId: null,
      resolverStatus: 'resolved',
      confidence: 'observed',
      inferenceReason: null,
    },
    identity: {
      botAppId: 'bot',
      botId: null,
      sessionId: 's1',
      turnId: null,
      nativeSessionId: null,
      workflowId: null,
      nodeId: null,
      attemptId: null,
      taskId: null,
      parentTaskId: null,
      skillName: 'hybrid-env-health-check',
      skillVersion: null,
      pluginId: null,
      chatId: null,
      topicRootId: null,
    },
    ordering: {
      sourceKey: 'unit',
      idempotencyKey: 'evt-dashboard-1',
      parentEventIds: [],
      observedAt: now,
    },
    provenance: {
      evidenceLevel: 'runtime',
      parserVersion: 'unit',
      sourceRefs,
      privacyClass: 'internal',
      redactionStatus: 'not_needed',
    },
    content: {
      hash: null,
      storageMode: 'none',
      ref: null,
      inlinePreview: null,
      encryption: { algorithm: 'none', keyRef: null, nonceRef: null, aad: null },
    },
    payload: { redacted: true },
    createdAt: now,
    ...overrides,
  };
}

async function approvedKnowledge(store: ObservationStore, overrides: Partial<Parameters<ObservationStore['proposeKnowledge']>[0]> = {}): Promise<KnowledgeItem> {
  const item = store.proposeKnowledge({
    targetLayer: 'L2',
    category: 'sop',
    title: 'Failover SOP',
    claimKey: 'ops.failover',
    claimText: 'Do not expose this raw claim in dashboard metrics.',
    confidence: 'observed',
    freshness: 'fresh',
    privacyClass: 'internal',
    sourceRefs,
    ...overrides,
  }).item;
  store.transitionKnowledge({ knowledgeId: item.knowledgeId, toState: 'review_pending', reasonCode: 'ready', actorId: 'human' });
  return store.transitionKnowledge({ knowledgeId: item.knowledgeId, toState: 'approved', reasonCode: 'approved', actorId: 'human' });
}

describe('KM operations dashboard metrics', () => {
  it('returns bounded explicit empty states for a fresh local store', async () => {
    const store = await ObservationStore.open(tempDir());
    const metrics = store.dashboardMetrics({ now: fixedNow });

    expect(metrics).toMatchObject({
      schemaVersion: 1,
      source: 'sqlite',
      generatedAt: '2026-08-28T12:00:00.000Z',
      kpis: {
        totalKnowledge: 0,
        activeMemory: 0,
        healthPercent: 0,
        retrievalRuns: 0,
        auditEvents: 0,
      },
      totals: {
        knowledgeTotal: 0,
        memoryTotal: 0,
        retrievalTotal: 0,
        overallHealthRate: 0,
      },
    });
    expect(metrics.trends.last7d).toHaveLength(7);
    expect(metrics.trends.last30d).toHaveLength(30);
    expect(metrics.emptyStates.filter(item => item.empty).map(item => item.key)).toEqual([
      'knowledge',
      'memory',
      'retrieval',
      'injection',
      'observations',
    ]);
    store.close();
  });

  it('aggregates privacy-safe KPIs, distributions, trends and rankings', async () => {
    const store = await ObservationStore.open(tempDir());
    const knowledge = await approvedKnowledge(store);
    store.proposeKnowledge({
      targetLayer: 'L4',
      category: 'api',
      title: 'Pending API note',
      claimKey: 'api.pending',
      claimText: 'Pending private claim',
      confidence: 'observed',
      freshness: 'unknown',
      privacyClass: 'internal',
      sourceRefs,
    });
    store.proposeKnowledge({
      targetLayer: 'L1',
      category: 'architecture',
      title: 'Stale architecture note',
      claimKey: 'arch.stale',
      claimText: 'Stale private claim',
      confidence: 'observed',
      freshness: 'stale',
      privacyClass: 'internal',
      sourceRefs,
    });
    const memory = store.upsertMemory({
      memoryId: 'mem-active',
      state: 'active',
      scope: 'workspace',
      subject: 'repo',
      claimKey: 'response.language',
      claimText: 'Do not expose this memory text in dashboard metrics.',
      confidence: 'observed',
      privacyClass: 'internal',
      sourceRefs,
    }).item;
    store.upsertMemory({
      memoryId: 'mem-proposed',
      state: 'proposed',
      scope: 'skill',
      subject: 'km',
      claimKey: 'pending.memory',
      claimText: 'Pending memory text',
      confidence: 'observed',
      privacyClass: 'internal',
      sourceRefs,
    });
    const runId = store.recordRetrievalAudit({
      botAppId: 'bot',
      sessionId: 's1',
      queryHash: 'sha256:abc',
      mode: 'shadow',
      candidateCount: 2,
      eligibleCount: 2,
      latencyMs: 9,
      warnings: [],
      results: [
        { itemId: knowledge.knowledgeId, itemKind: 'knowledge', providerIds: ['sqlite'], score: 1, eligible: true },
        { itemId: memory.memoryId, itemKind: 'memory', providerIds: ['sqlite'], score: 0.9, eligible: true },
      ],
    });
    store.recordPromptInjectionSnapshot({
      retrievalRunId: runId,
      botAppId: 'bot',
      mode: 'shadow',
      disposition: 'would_inject',
      itemIds: [memory.memoryId, knowledge.knowledgeId],
      prompt: 'redacted prompt body',
    });
    store.append(event());

    const metrics = store.dashboardMetrics({ now: fixedNow, rankingLimit: 5 });
    expect(metrics.kpis).toEqual({
      totalKnowledge: 3,
      activeMemory: 1,
      healthPercent: 40,
      retrievalRuns: 1,
      auditEvents: 1,
    });
    expect(metrics.totals).toEqual(expect.objectContaining({
      knowledgeTotal: 3,
      knowledgeUsable: 1,
      memoryTotal: 2,
      memoryActive: 1,
      memoryUsable: 1,
      retrievalTotal: 1,
      retrievalLast7d: 1,
      wouldInjectTotal: 1,
      pendingReviewTotal: 3,
      staleKnowledge: 1,
      auditEventsTotal: 1,
    }));
    expect(metrics.distributions.knowledgeByLayer).toEqual(expect.arrayContaining([
      { key: 'L2', count: 1 },
      { key: 'L4', count: 1 },
      { key: 'L1', count: 1 },
    ]));
    expect(metrics.distributions.memoryByScope).toEqual(expect.arrayContaining([
      { key: 'workspace', count: 1 },
      { key: 'skill', count: 1 },
    ]));
    expect(metrics.distributions.observationBySource).toEqual([{ key: 'botmux', adapter: 'unit', count: 1 }]);
    expect(metrics.rankings.recallHot[0]).toEqual(expect.objectContaining({ itemId: knowledge.knowledgeId, itemKind: 'knowledge', title: 'Failover SOP', count: 1 }));
    expect(metrics.rankings.readHot.map(item => item.itemId)).toEqual(expect.arrayContaining([memory.memoryId, knowledge.knowledgeId]));
    expect(metrics.rankings.pendingReview.map(item => item.title)).toEqual(expect.arrayContaining(['Pending API note', 'pending.memory']));
    expect(JSON.stringify(metrics)).not.toContain('Do not expose this raw claim');
    expect(JSON.stringify(metrics)).not.toContain('redacted prompt body');
    store.close();
  });

  it('serves the dashboard metrics API without requiring any mutation dependencies', async () => {
    const dashboardMetrics = vi.fn(() => ({ schemaVersion: 1, source: 'sqlite', generatedAt: '2026-08-28T12:00:00.000Z' }));
    const close = vi.fn();
    const { res, bodies } = response();
    const handled = await handleKmObservationApi(
      Object.assign(Readable.from([]), { method: 'GET', headers: {} }) as any,
      res,
      new URL('http://localhost/api/km/dashboard-metrics?rankingLimit=999'),
      {
        enabled: true,
        openStore: async () => ({
          schemaVersion: vi.fn(),
          pragmas: vi.fn(),
          counts: vi.fn(),
          list: vi.fn(),
          get: vi.fn(),
          close,
          dashboardMetrics,
        }),
      },
    );

    expect(handled).toBe(true);
    expect(dashboardMetrics).toHaveBeenCalledWith({ rankingLimit: 50 });
    expect(res.writeHead).toHaveBeenCalledWith(200, expect.anything());
    expect(bodies).toEqual([{ schemaVersion: 1, source: 'sqlite', generatedAt: '2026-08-28T12:00:00.000Z' }]);
    expect(close).toHaveBeenCalledOnce();
  });
});
