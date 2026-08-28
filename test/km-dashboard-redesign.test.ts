import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { buildKmDashboardModel, KM_DASHBOARD_EXPECTED_CONTRACT } from '../src/dashboard/web/km-dashboard-model.js';

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

    expect(KM_DASHBOARD_EXPECTED_CONTRACT.path).toBe('/api/km/dashboard-metrics');
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
    expect(page).toContain('className="danger"');
    expect(css).toContain('.km-kpi-grid');
    expect(css).toContain('@media (prefers-reduced-motion: reduce)');
  });
});
