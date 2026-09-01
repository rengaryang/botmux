import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { buildKmDashboardModel, buildKmDashboardModelFromMetrics, KM_DASHBOARD_EXPECTED_CONTRACT } from '../src/dashboard/web/km-dashboard-model.js';
import { filterWorkspaceAssets, paginateWorkspaceAssets, WORKSPACE_ASSET_PAGE_SIZE, type WorkspaceAssetV2 } from '../src/dashboard/web/km-page.js';

describe('KM dashboard redesign', () => {
  it('builds the operations overview from the documented fallback contract', () => {
    const model = buildKmDashboardModel({
      health: {
        enabled: true,
        counts: { observations: 12, quarantined: 1, knowledge: 3, memory: 2 },
        backlog: { queued: 1, retryWait: 0, claimed: 0 },
        capabilities: { effectiveModes: ['shadow'], livePromptInjection: false, realMemoryTransport: false },
        evalEvolution: { evalRuns: 2, failingEvalRuns: 0, reviewPendingProposals: 1 },
      },
      knowledge: [
        { knowledgeId: 'k1', title: 'K8s rollback SOP', targetLayer: 'L2', confidence: 'high', freshness: 'fresh', state: 'approved' },
        { knowledgeId: 'k2', title: 'Gateway dependency map', targetLayer: 'L1', confidence: 'high', freshness: 'fresh', state: 'approved' },
        { knowledgeId: 'k3', title: 'Old API note', targetLayer: 'L4', confidence: 'medium', freshness: 'stale', state: 'review_pending' },
      ],
      memory: [
        { memoryId: 'm1', scope: 'workspace', subject: 'km', claimKey: 'km.active', confidence: 'high', state: 'active' },
        { memoryId: 'm2', scope: 'skill', subject: 'retrieval', claimKey: 'km.shadow', confidence: 'medium', state: 'proposed' },
      ],
      importJobs: [{ state: 'preview' }],
      retrievalQuality: { runs: 9, zeroHits: 2, directHits: 5, normalizedHits: 2, noHits: 2, avgLatencyMs: 38 },
      retention: undefined,
      productionGates: [{ planId: 'pg1', actionKind: 'prompt-canary', state: 'ready', requiredApprovalGrade: 'G2' }],
      events: [
        { eventId: 'e1', eventType: 'skill.invoked', ordering: { observedAt: new Date().toISOString() }, identity: { skillName: 'hybrid-env-health-check', sessionId: 's1' } },
        { eventId: 'e2', eventType: 'skill.invoked', ordering: { observedAt: new Date().toISOString() }, identity: { skillName: 'hybrid-env-health-check', sessionId: 's2' } },
      ],
      retrievals: [{ retrievalRunId: 'r1', botAppId: 'cli_x', mode: 'shadow', candidateCount: 5, eligibleCount: 3, latencyMs: 15 }],
      backendRuntime: { enabled: false, outbox: { pending: 0, total: 0, quarantined: 0 } },
      centralSink: { enabled: false },
      shadowReadiness: { ready: false, reasonCodes: ['min_reviewed_cases'] },
    });

    expect(KM_DASHBOARD_EXPECTED_CONTRACT.path).toBe('/api/km/dashboard-metrics-v2');
    expect(model.kpis).toHaveLength(5);
    expect(model.kpis.map(item => item.label)).toEqual([
      '知识总量',
      '在用记忆',
      '健康知识占比',
      '召回运行次数',
      '审计与闸门',
    ]);
    expect(model.layerDistribution.map(item => item.label)).toContain('排障与经验');
    expect(model.hotSkills[0]).toMatchObject({ title: 'hybrid-env-health-check', value: 2 });
    expect(model.riskBadges.some(item => item.label === 'Shadow 阻塞')).toBe(true);
  });

  it('adapts the real metrics API contract into the overview view model', () => {
    const model = buildKmDashboardModelFromMetrics({
      schemaVersion: 1,
      source: 'sqlite',
      generatedAt: '2026-08-28T12:00:00.000Z',
      kpis: { totalKnowledge: 4, activeMemory: 2, healthPercent: 67, retrievalRuns: 9, auditEvents: 12 },
      totals: {
        knowledgeTotal: 4,
        knowledgeUsable: 3,
        memoryTotal: 3,
        memoryActive: 2,
        memoryUsable: 2,
        retrievalTotal: 10,
        retrievalLast7d: 3,
        retrievalLast30d: 9,
        wouldInjectTotal: 1,
        actualInjectTotal: 0,
        auditEventsTotal: 12,
        pendingReviewTotal: 1,
        conflictTotal: 0,
        staleKnowledge: 1,
        staleMemory: 0,
      },
      distributions: {
        knowledgeByLayer: [{ key: 'L2', count: 3 }, { key: 'L4', count: 1 }],
        knowledgeByState: [{ key: 'approved', count: 3 }, { key: 'review_pending', count: 1 }],
        memoryByState: [{ key: 'approved', count: 1 }, { key: 'active', count: 2 }],
        memoryByScope: [{ key: 'workspace', count: 2 }],
        knowledgeByFreshness: [{ key: 'fresh', count: 3 }, { key: 'stale', count: 1 }],
        knowledgeByCategory: [{ key: 'sop', count: 2 }],
      },
      trends: { last7d: [{ date: '2026-08-28', knowledgeCreated: 2, memoryCreated: 1, retrievalRuns: 3, wouldInject: 1, actualInject: 0 }] },
      rankings: {
        recallHot: [{ itemId: 'kn1', itemKind: 'knowledge', title: 'Failover SOP', count: 5, lastSeenAt: '2026-08-28T11:00:00.000Z', targetLayer: 'L2', state: 'approved' }],
        readHot: [{ itemId: 'mem1', itemKind: 'memory', title: 'response.language', count: 4, lastSeenAt: '2026-08-28T11:30:00.000Z', scope: 'workspace', state: 'active' }],
        pendingReview: [],
        conflicts: [],
        stale: [],
      },
      emptyStates: [],
    });

    expect(model.source).toBe('metrics-api');
    expect(model.generatedAt).toBe('2026-08-28T12:00:00.000Z');
    expect(model.kpis.map(item => item.value)).toEqual([4, 2, 67, 9, 12]);
    expect(model.layerDistribution[0]).toMatchObject({ key: 'L2', label: '排障与经验', value: 3, percent: 75 });
    expect(model.stateDistribution.find(item => item.key === 'approved')).toMatchObject({ value: 4 });
    expect(model.trend).toEqual([{ label: '8/28', observations: 3, retrievals: 3, gates: 1 }]);
    expect(model.hotSkills[0]).toMatchObject({ id: 'kn1', title: 'Failover SOP', value: 5 });
    expect(model.hotKnowledge[0]).toMatchObject({ id: 'mem1', title: 'response.language', value: 4 });
    expect(model.riskBadges.map(item => item.label)).toContain('Metrics API 已接入');
  });

  it('filters and paginates the workspace asset list without hiding its total', () => {
    const assets = Array.from({ length: 123 }, (_, index): WorkspaceAssetV2 => ({
      assetId: `asset-${index}`, workspaceId: index < 100 ? 'main' : 'nested', layer: index % 2 ? 'L2' : 'L4', kind: 'test', title: `Asset ${index}`, relativePath: `asset-${index}.md`, lifecycle: 'not-applicable', freshness: 'not-applicable', contract: { version: 'n/a', valid: true, errors: [], warnings: [] }, retrieval: { recallCount: 0 }, linkage: { relatedCount: 0 },
    }));
    expect(WORKSPACE_ASSET_PAGE_SIZE).toBe(50);
    expect(filterWorkspaceAssets(assets, 'nested', 'L2')).toHaveLength(11);
    expect(paginateWorkspaceAssets(assets, 3)).toMatchObject({ currentPage: 3, pageCount: 3, start: 100, end: 123, total: 123 });
    expect(paginateWorkspaceAssets(assets, 99).items).toHaveLength(23);
  });

  it('keeps KM navigation, overview, and high-risk controls in separate sections', () => {
    const page = readFileSync(new URL('../src/dashboard/web/km-page.tsx', import.meta.url), 'utf8');
    const components = readFileSync(new URL('../src/dashboard/web/km-dashboard-components.tsx', import.meta.url), 'utf8');
    const css = readFileSync(new URL('../src/dashboard/web/style.css', import.meta.url), 'utf8');

    for (const label of ['总览', '知识', '记忆', '质量', '配置', '生产闸门', '审计']) {
      expect(components).toContain(label);
    }
    expect(page).toContain('<TabOnly tab="overview">');
    expect(page).toContain('<TabOnly tab="production">');
    expect(page.indexOf('<KmOverview model={dashboardModel} />')).toBeLessThan(page.indexOf('<TabOnly tab="production">'));
    expect(page).toContain('KM_DASHBOARD_EXPECTED_CONTRACT');
    expect(page).toContain("getJson<KmOpsMetricsRaw>('/api/km/dashboard-metrics?rankingLimit=10')");
    expect(page).toContain('buildKmDashboardModelFromMetrics(dashboardMetrics)');
    expect(page).toContain('筛选 Workspace');
    expect(page).toContain('显示 {workspaceAssetView.total');
    expect(page).toContain('下一页');
    expect(page).toContain('className="danger"');
    expect(css).toContain('.km-kpi-grid');
    expect(css).toContain('@media (prefers-reduced-motion: reduce)');
    expect(components).toContain("props.model.source === 'metrics-api'");
  });
});
