export type KmOpsTabId = 'overview' | 'knowledge' | 'memory' | 'quality' | 'configuration' | 'production' | 'audit';

export type KmExpectedMetricsContract = {
  schemaVersion: 2;
  path: '/api/km/dashboard-metrics-v2';
  note: string;
  fields: {
    assetHealth: 'L0-L4 totals, contract/index/retrievable/linkage rates, lifecycle and freshness';
    retrievalQuality: 'index queries, entry recalls, markdown reads, effectiveness/fallback/query-feedback rates';
    kmRuntime: 'the complete backward-compatible SQLite metrics contract v1';
    attention: 'contract errors, pending ingest, stale/purged, never recalled and orphaned assets';
  };
};

export const KM_DASHBOARD_EXPECTED_CONTRACT: KmExpectedMetricsContract = {
  schemaVersion: 2,
  path: '/api/km/dashboard-metrics-v2',
  note: 'Workspace asset health and SQLite runtime health remain separate. Unknown evidence-backed ratios are null, never inferred as healthy.',
  fields: {
    assetHealth: 'L0-L4 totals, contract/index/retrievable/linkage rates, lifecycle and freshness',
    retrievalQuality: 'index queries, entry recalls, markdown reads, effectiveness/fallback/query-feedback rates',
    kmRuntime: 'the complete backward-compatible SQLite metrics contract v1',
    attention: 'contract errors, pending ingest, stale/purged, never recalled and orphaned assets',
  },
};

export type KmMetricPoint = {
  key: string;
  label: string;
  value: number | null;
  unit?: string;
  helper: string;
  tone: 'blue' | 'ink' | 'cyan' | 'green' | 'slate';
  tooltip: string;
};

export type KmDistributionSlice = {
  key: string;
  label: string;
  value: number;
  percent: number;
  color: string;
};

export type KmTrendPoint = {
  label: string;
  observations: number;
  retrievals: number;
  gates: number;
};

export type KmRankingItem = {
  id: string;
  title: string;
  value: number;
  meta: string;
};

export type WorkspaceMetricsV2 = {
  schemaVersion: 2;
  generatedAt: string;
  snapshot: { state: string; hash: string; durationMs: number; roots: Array<{ workspaceId: string; displayRoot: string; state: string; errors: string[] }>; errors: string[] };
  assetHealth: {
    totalsByLayer: Record<'L0'|'L1'|'L2'|'L3'|'L4', number>; totalAssets: number; contractValidRate: number|null;
    indexConsistencyRate: number|null; retrievableRate: number|null; linkageCoverageRate: number|null;
    lifecycle: Record<string, number>; freshness: Record<string, number>; contractErrors: number; legacyAssets: number;
  };
  retrievalQuality: { indexQueries: number; entryRecallEvents: number; neverRecalledAssets: number; markdownReads: number; zeroReadQueries: number|null; zeroReadRate: number|null; effectivenessRate: number|null; fallbackSuccessRate: number|null; queryFeedbackRate: number|null; evidenceState: string; evidenceQueries: number; useLabels: Record<string, number>; invalidEvidenceEvents: number };
  kmRuntime: KmOpsMetricsRaw;
};

export type KmDashboardModel = {
  generatedAt: string;
  source: 'fallback' | 'metrics-api';
  summary: string;
  kpis: KmMetricPoint[];
  layerDistribution: KmDistributionSlice[];
  healthDistribution: KmDistributionSlice[];
  stateDistribution: KmDistributionSlice[];
  categoryDistribution: KmDistributionSlice[];
  trend: KmTrendPoint[];
  hotSkills: KmRankingItem[];
  hotKnowledge: KmRankingItem[];
  riskBadges: Array<{ label: string; tone: 'ok' | 'warn' | 'danger'; detail: string }>;
};

type HealthLike = {
  enabled?: boolean;
  counts?: { observations?: number; quarantined?: number; knowledge?: number; memory?: number };
  backlog?: { queued?: number; retryWait?: number; claimed?: number };
  capabilities?: { effectiveModes?: string[]; livePromptInjection?: boolean; realMemoryTransport?: boolean };
  evalEvolution?: { evalRuns?: number; failingEvalRuns?: number; reviewPendingProposals?: number };
};

type KnowledgeLike = { knowledgeId?: string; targetLayer?: string; title?: string; confidence?: string; freshness?: string; state?: string };
type MemoryLike = { memoryId?: string; state?: string; scope?: string; subject?: string; claimKey?: string };
type RetrievalQualityLike = { runs?: number; zeroHits?: number; directHits?: number; normalizedHits?: number; noHits?: number; avgLatencyMs?: number };
type RetentionLike = {
  enabled?: boolean;
  latestPlan?: {
    domains?: Array<{ domain?: string; table?: string; tier?: string; totalCount?: number; eligibleCount?: number; protectedCount?: number }>;
    slo?: Array<{ key?: string; state?: 'ok' | 'warn' | 'critical'; value?: number; unit?: string }>;
  };
  trend?: Array<{ reportId?: string; completedAt?: string; totalEligible?: number; dbBytes?: number }>;
};
type ProductionGateLike = { planId?: string; actionKind?: string; state?: string; requiredApprovalGrade?: string };
type EventLike = { eventId?: string; eventType?: string; ordering?: { observedAt?: string }; identity?: { skillName?: string | null; nodeId?: string | null; sessionId?: string } };
type RetrievalLike = { retrievalRunId?: string; botAppId?: string; mode?: string; candidateCount?: number; eligibleCount?: number; latencyMs?: number };
type ImportJobLike = { state?: string };
type BackendRuntimeLike = { enabled?: boolean; outbox?: { pending?: number; total?: number; quarantined?: number } };
type CentralSinkLike = { enabled?: boolean };
type ShadowReadinessLike = { ready?: boolean; reasonCodes?: string[] };

export type KmOpsMetricsRaw = {
  schemaVersion: 1;
  source: 'sqlite';
  generatedAt: string;
  kpis: {
    totalKnowledge: number;
    activeMemory: number;
    healthPercent: number;
    retrievalRuns: number;
    auditEvents: number;
  };
  totals: {
    knowledgeTotal: number;
    knowledgeUsable: number;
    memoryTotal: number;
    memoryActive: number;
    memoryUsable: number;
    retrievalTotal: number;
    retrievalLast7d: number;
    retrievalLast30d: number;
    wouldInjectTotal: number;
    actualInjectTotal: number;
    auditEventsTotal: number;
    pendingReviewTotal: number;
    conflictTotal: number;
    staleKnowledge: number;
    staleMemory: number;
  };
  distributions: {
    knowledgeByLayer: Array<{ key: string; count: number }>;
    knowledgeByState: Array<{ key: string; count: number }>;
    memoryByState: Array<{ key: string; count: number }>;
    memoryByScope: Array<{ key: string; count: number }>;
    knowledgeByFreshness: Array<{ key: string; count: number }>;
    knowledgeByCategory: Array<{ key: string; count: number }>;
  };
  trends: {
    last7d: Array<{ date: string; knowledgeCreated: number; memoryCreated: number; retrievalRuns: number; wouldInject: number; actualInject: number }>;
  };
  rankings: {
    recallHot: Array<{ itemId: string; itemKind: string; title: string; count: number; lastSeenAt: string; state?: string; targetLayer?: string; category?: string; scope?: string }>;
    readHot: Array<{ itemId: string; itemKind: string; title: string; count: number; lastSeenAt: string; state?: string; targetLayer?: string; category?: string; scope?: string }>;
    pendingReview: Array<{ itemId: string; itemKind: string; title: string; state: string; updatedAt: string; ageDays: number; targetLayer?: string; category?: string; scope?: string }>;
    conflicts: Array<{ itemId: string; itemKind: string; title: string; state: string; updatedAt: string; ageDays: number; targetLayer?: string; category?: string; scope?: string }>;
    stale: Array<{ itemId: string; itemKind: string; title: string; state: string; updatedAt: string; ageDays: number; targetLayer?: string; category?: string; scope?: string }>;
  };
  emptyStates: Array<{ key: string; empty: boolean; title: string; detail: string }>;
};

export type KmDashboardModelInput = {
  health?: HealthLike;
  knowledge: KnowledgeLike[];
  memory: MemoryLike[];
  importJobs: ImportJobLike[];
  retrievalQuality?: RetrievalQualityLike;
  retention?: RetentionLike;
  productionGates: ProductionGateLike[];
  events: EventLike[];
  retrievals: RetrievalLike[];
  backendRuntime?: BackendRuntimeLike;
  centralSink?: CentralSinkLike;
  shadowReadiness?: ShadowReadinessLike;
};

const LAYER_LABELS: Record<string, string> = {
  L1: '架构与依赖',
  L2: '排障与经验',
  L3: 'Skill 与流程',
  L4: '接口与命令',
  'reviewed-only': '仅保留备查',
};

const HEALTH_LABELS: Record<string, string> = {
  fresh: '新鲜可用',
  stale: '需要复核',
  expired: '已过期',
  unknown: '未标注',
};

const STATE_LABELS: Record<string, string> = {
  active: '已生效',
  proposed: '待审核',
  conflicted: '冲突',
  stale: '陈旧',
  expired: '过期',
  revoked: '已撤销',
  shadowed: '影子态',
  approved: '已批准',
  review_pending: '待复核',
};

const SLICE_COLORS = ['#1d5fd6', '#5aa9f8', '#7a69df', '#25a47f', '#d89b24', '#728094'];

function num(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function pct(part: number, total: number): number {
  if (total <= 0) return 0;
  return Math.round((part / total) * 1000) / 10;
}

function countBy<T>(items: T[], keyOf: (item: T) => string | undefined, fallback = 'unknown'): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const item of items) {
    const key = keyOf(item)?.trim() || fallback;
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return counts;
}

function distribution(counts: Record<string, number>, labels: Record<string, string> = {}): KmDistributionSlice[] {
  const total = Object.values(counts).reduce((sum, value) => sum + value, 0);
  return Object.entries(counts)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([key, value], index) => ({
      key,
      label: labels[key] ?? key,
      value,
      percent: pct(value, total),
      color: SLICE_COLORS[index % SLICE_COLORS.length],
    }));
}

function distributionFromBuckets(items: Array<{ key: string; count: number }>, labels: Record<string, string> = {}): KmDistributionSlice[] {
  const counts: Record<string, number> = {};
  for (const item of items) counts[item.key] = (counts[item.key] ?? 0) + item.count;
  return distribution(counts, labels);
}

function topN(counts: Record<string, number>, limit: number): Array<[string, number]> {
  return Object.entries(counts)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, limit);
}

function eventTrend(events: EventLike[], retrievals: RetrievalLike[], gates: ProductionGateLike[]): KmTrendPoint[] {
  const days: KmTrendPoint[] = [];
  const now = new Date();
  for (let i = 5; i >= 0; i -= 1) {
    const d = new Date(now);
    d.setDate(now.getDate() - i);
    const label = `${d.getMonth() + 1}/${d.getDate()}`;
    const isoDay = d.toISOString().slice(0, 10);
    days.push({
      label,
      observations: events.filter(event => event.ordering?.observedAt?.startsWith(isoDay)).length,
      retrievals: retrievals.filter(run => run.retrievalRunId || run.botAppId).length && i === 0 ? retrievals.length : 0,
      gates: gates.filter(gate => gate.planId).length && i === 0 ? gates.length : 0,
    });
  }
  return days;
}

function shortDateLabel(date: string): string {
  const parts = date.split('-');
  if (parts.length === 3) return `${Number(parts[1])}/${Number(parts[2])}`;
  return date;
}

function rankMeta(item: { itemKind: string; state?: string; targetLayer?: string; category?: string; scope?: string; lastSeenAt?: string }): string {
  const parts = [
    item.itemKind === 'knowledge' ? (LAYER_LABELS[item.targetLayer ?? ''] ?? item.targetLayer) : item.scope,
    STATE_LABELS[item.state ?? ''] ?? item.state,
    item.category,
  ].filter(Boolean);
  return parts.length ? parts.join(' · ') : (item.lastSeenAt ? `最后命中 ${item.lastSeenAt.slice(0, 10)}` : 'metrics API');
}

export function buildKmDashboardModelV2(metrics: WorkspaceMetricsV2): KmDashboardModel {
  const health = metrics.assetHealth;
  const retrieval = metrics.retrievalQuality;
  const layerDistribution = distribution(health.totalsByLayer);
  const healthDistribution = distribution(health.freshness, HEALTH_LABELS);
  const stateDistribution = distribution(health.lifecycle, STATE_LABELS);
  const attention = health.contractErrors + retrieval.neverRecalledAssets;
  const snapshotOk = metrics.snapshot.state === 'complete';
  return {
    generatedAt: metrics.generatedAt,
    source: 'metrics-api',
    summary: `分层资产 ${health.totalAssets} 项（${Object.entries(health.totalsByLayer).map(([key,value]) => `${key} ${value}`).join(' / ')}）；KM 运行库另有 ${metrics.kmRuntime.totals.knowledgeTotal} 条自动候选。`,
    kpis: [
      { key: 'knowledge', label: '分层知识资产', value: health.totalAssets, unit: '项', helper: `L2 ${health.totalsByLayer.L2} · legacy ${health.legacyAssets}`, tone: 'blue', tooltip: '来自 workspace 文件资产只读扫描，不等于 SQLite candidate。' },
      { key: 'health', label: 'v3 契约有效率', value: snapshotOk ? health.contractValidRate : null, unit: '%', helper: health.contractErrors ? `含 legacy/异常 ${health.contractErrors} 项` : '契约检查通过', tone: 'cyan', tooltip: '只衡量 v3 L2 条目；snapshot partial/unavailable 时不可计算。' },
      { key: 'retrieval', label: '可召回率', value: snapshotOk ? health.retrievableRate : null, unit: '%', helper: `STALE/PURGED ${(health.freshness.stale ?? 0) + (health.freshness.purged ?? 0)}`, tone: 'green', tooltip: 'L2 eligible 条目的本地/远端可定位比例。' },
      { key: 'read', label: 'INDEX→正文读取', value: retrieval.zeroReadRate == null ? null : Math.max(0, 100 - retrieval.zeroReadRate), unit: '%', helper: retrieval.evidenceState === 'cold_start' ? 'cold_start · 等待 read evidence' : `read ${retrieval.markdownReads}`, tone: 'green', tooltip: '没有 query/read 对账证据时为未知。' },
      { key: 'memory', label: '检索有效率', value: retrieval.effectivenessRate, unit: '%', helper: retrieval.evidenceState === 'cold_start' ? 'cold_start · 等待 use evidence' : `read ${retrieval.markdownReads}`, tone: 'ink', tooltip: '没有 reasoning use evidence 时为未知。' },
      { key: 'audit', label: '运营待办', value: attention, unit: '项', helper: `未召回 ${retrieval.neverRecalledAssets} · pending ${health.lifecycle['pending-ingest'] ?? 0}`, tone: 'slate', tooltip: '契约异常、legacy 和未召回资产的待办总览。' },
    ],
    layerDistribution, healthDistribution, stateDistribution,
    categoryDistribution: distribution({ '资产契约': health.contractErrors, '待迁移 legacy': health.legacyAssets, '从未召回': retrieval.neverRecalledAssets }),
    trend: metrics.kmRuntime.trends.last7d.map(point => ({ label: shortDateLabel(point.date), observations: point.knowledgeCreated + point.memoryCreated, retrievals: point.retrievalRuns, gates: point.wouldInject + point.actualInject })),
    hotSkills: [], hotKnowledge: [],
    riskBadges: [
      { label: snapshotOk ? '资产扫描完整' : `资产扫描 ${metrics.snapshot.state}`, tone: snapshotOk ? 'ok' : 'danger', detail: metrics.snapshot.errors.join(', ') || `${metrics.snapshot.roots.length} workspace` },
      { label: health.legacyAssets ? '存在 legacy 契约' : 'v3 契约统一', tone: health.legacyAssets ? 'warn' : 'ok', detail: `${health.legacyAssets} 条待迁移` },
      { label: retrieval.evidenceState === 'cold_start' ? '检索证据冷启动' : '检索证据可用', tone: retrieval.evidenceState === 'cold_start' ? 'warn' : 'ok', detail: `INDEX ${retrieval.indexQueries} · recall ${retrieval.entryRecallEvents}` },
      { label: 'KM 运行健康独立统计', tone: 'ok', detail: `Memory ${metrics.kmRuntime.totals.memoryTotal} · retrieval ${metrics.kmRuntime.totals.retrievalLast30d}` },
    ],
  };
}

export function buildKmDashboardModelFromMetrics(metrics: KmOpsMetricsRaw): KmDashboardModel {
  const conflictTotal = num(metrics.totals.conflictTotal);
  const pendingReviewTotal = num(metrics.totals.pendingReviewTotal);
  const staleTotal = num(metrics.totals.staleKnowledge) + num(metrics.totals.staleMemory);
  const totalItems = num(metrics.totals.knowledgeTotal) + num(metrics.totals.memoryTotal);
  const categoryBuckets = metrics.distributions.knowledgeByCategory.length
    ? metrics.distributions.knowledgeByCategory
    : metrics.distributions.memoryByScope;
  const hotKnowledgeSource = metrics.rankings.readHot.length
    ? metrics.rankings.readHot
    : metrics.rankings.pendingReview;

  return {
    generatedAt: metrics.generatedAt,
    source: 'metrics-api',
    summary: totalItems > 0
      ? `指标来自 SQLite 聚合：${metrics.totals.knowledgeTotal} 条知识、${metrics.totals.memoryTotal} 条记忆，近 30 天召回 ${metrics.totals.retrievalLast30d} 次。`
      : '暂无知识指标，已接入 metrics API 并等待本地观测数据写入。',
    kpis: [
      { key: 'knowledge', label: '知识总量', value: metrics.kpis.totalKnowledge, unit: '条', helper: `可用 ${metrics.totals.knowledgeUsable} 条`, tone: 'blue', tooltip: '来自 /api/km/dashboard-metrics 的 totalKnowledge。' },
      { key: 'memory', label: '在用记忆', value: metrics.kpis.activeMemory, unit: '条', helper: `总记忆 ${metrics.totals.memoryTotal} 条`, tone: 'ink', tooltip: 'active 且未过期的本地 memory item 数。' },
      { key: 'health', label: '健康知识占比', value: metrics.kpis.healthPercent, unit: '%', helper: staleTotal ? `陈旧/过期 ${staleTotal} 条` : '无陈旧项', tone: 'cyan', tooltip: '知识与记忆可用项占总项的百分比。' },
      { key: 'retrieval', label: '召回运行次数', value: metrics.kpis.retrievalRuns, unit: '次', helper: `近 7 天 ${metrics.totals.retrievalLast7d} 次`, tone: 'green', tooltip: '近 30 天 retrieval_runs 聚合计数。' },
      { key: 'audit', label: '审计与闸门', value: metrics.kpis.auditEvents, unit: '项', helper: pendingReviewTotal ? `待复核 ${pendingReviewTotal} 项` : '无待复核项', tone: 'slate', tooltip: '观测、配置、导入和 production gate 审计事件总数。' },
    ],
    layerDistribution: distributionFromBuckets(metrics.distributions.knowledgeByLayer, LAYER_LABELS),
    healthDistribution: distributionFromBuckets(metrics.distributions.knowledgeByFreshness, HEALTH_LABELS),
    stateDistribution: distributionFromBuckets([...metrics.distributions.knowledgeByState, ...metrics.distributions.memoryByState], STATE_LABELS),
    categoryDistribution: distributionFromBuckets(categoryBuckets),
    trend: metrics.trends.last7d.map(point => ({
      label: shortDateLabel(point.date),
      observations: num(point.knowledgeCreated) + num(point.memoryCreated),
      retrievals: num(point.retrievalRuns),
      gates: num(point.wouldInject) + num(point.actualInject),
    })),
    hotSkills: metrics.rankings.recallHot.map(item => ({
      id: item.itemId,
      title: item.title,
      value: item.count,
      meta: rankMeta(item),
    })),
    hotKnowledge: hotKnowledgeSource.map(item => ({
      id: item.itemId,
      title: item.title,
      value: 'count' in item ? item.count : 1,
      meta: rankMeta(item),
    })),
    riskBadges: [
      { label: 'Metrics API 已接入', tone: 'ok', detail: `数据源 ${metrics.source}，最近生成 ${metrics.generatedAt.slice(0, 19)}` },
      { label: pendingReviewTotal ? '存在待复核项' : '无待复核项', tone: pendingReviewTotal ? 'warn' : 'ok', detail: pendingReviewTotal ? `需要复核 ${pendingReviewTotal} 条知识或记忆` : '审核队列为空' },
      { label: conflictTotal ? '存在冲突项' : '无冲突项', tone: conflictTotal ? 'danger' : 'ok', detail: conflictTotal ? `冲突或隔离 ${conflictTotal} 条` : '未发现冲突或隔离数据' },
      { label: staleTotal ? '存在陈旧项' : '新鲜度正常', tone: staleTotal ? 'warn' : 'ok', detail: staleTotal ? `陈旧知识/记忆 ${staleTotal} 条` : '没有过期或待复核的陈旧项' },
    ],
  };
}

export function buildKmDashboardModel(input: KmDashboardModelInput): KmDashboardModel {
  const knowledgeTotal = num(input.health?.counts?.knowledge) || input.knowledge.length;
  const memoryTotal = num(input.health?.counts?.memory) || input.memory.length;
  const activeMemory = input.memory.filter(item => item.state === 'active').length || memoryTotal;
  const observations = num(input.health?.counts?.observations) || input.events.length;
  const quarantined = num(input.health?.counts?.quarantined);
  const retrievalRuns = num(input.retrievalQuality?.runs) || input.retrievals.length;
  const zeroHits = num(input.retrievalQuality?.zeroHits);
  const healthPercent = knowledgeTotal > 0
    ? pct(input.knowledge.filter(item => item.freshness !== 'stale' && item.freshness !== 'expired').length || knowledgeTotal, knowledgeTotal)
    : 0;
  const totalBacklog = num(input.health?.backlog?.queued) + num(input.health?.backlog?.retryWait) + num(input.health?.backlog?.claimed);
  const totalAudit = input.events.length + input.productionGates.length;

  const layerDistribution = distribution(countBy(input.knowledge, item => item.targetLayer), LAYER_LABELS);
  const healthDistribution = distribution(countBy(input.knowledge, item => item.freshness), HEALTH_LABELS);
  const stateDistribution = distribution(countBy([...input.memory, ...input.knowledge], item => item.state), STATE_LABELS);
  const categoryDistribution = distribution(countBy(input.memory, item => item.scope ?? item.subject, 'workspace'));
  const skillCounts = countBy(input.events, item => item.identity?.skillName ?? item.identity?.nodeId ?? item.eventType, '未分类事件');

  const hotSkills = topN(skillCounts, 5).map(([key, value]) => ({
    id: key,
    title: key,
    value,
    meta: '最近 100 条观测事件',
  }));
  const hotKnowledge = input.knowledge
    .slice()
    .sort((a, b) => {
      const score = (item: KnowledgeLike) => (item.state === 'approved' ? 3 : 0) + (item.freshness === 'fresh' ? 2 : 0) + (item.confidence === 'high' ? 1 : 0);
      return score(b) - score(a) || String(a.title ?? '').localeCompare(String(b.title ?? ''));
    })
    .slice(0, 5)
    .map((item, index) => ({
      id: item.knowledgeId ?? `${item.title ?? 'knowledge'}-${index}`,
      title: item.title ?? '未命名知识',
      value: item.state === 'approved' ? 1 : 0,
      meta: `${LAYER_LABELS[item.targetLayer ?? ''] ?? item.targetLayer ?? '未分层'} · ${STATE_LABELS[item.state ?? ''] ?? item.state ?? '未知状态'}`,
    }));

  const riskBadges: KmDashboardModel['riskBadges'] = [
    {
      label: input.health?.enabled ? '采集已开启' : '采集未开启',
      tone: input.health?.enabled ? 'ok' : 'warn',
      detail: input.health?.enabled ? '正在接收本地观测事件' : '需要开启 BOTMUX_KM_OBSERVATION_ENABLED 后才会有实时数据',
    },
    {
      label: input.backendRuntime?.enabled ? 'Backend Worker 已开启' : 'Backend Worker 关闭',
      tone: input.backendRuntime?.enabled ? 'ok' : 'warn',
      detail: '外部记忆写入仍由配置和生产闸门控制',
    },
    {
      label: input.centralSink?.enabled ? 'Central Sink 已开启' : 'Central Sink 关闭',
      tone: input.centralSink?.enabled ? 'warn' : 'ok',
      detail: input.centralSink?.enabled ? '检查 sink 配置和 outbox 积压后再推进' : '中心同步处于关闭或 mock 状态',
    },
    {
      label: input.shadowReadiness?.ready ? 'Shadow Ready' : 'Shadow 阻塞',
      tone: input.shadowReadiness?.ready ? 'ok' : 'warn',
      detail: input.shadowReadiness?.reasonCodes?.join(', ') || '质量门禁状态来自本地 readiness 报告',
    },
  ];

  return {
    generatedAt: new Date().toISOString(),
    source: 'fallback',
    summary: knowledgeTotal > 0
      ? `已整理 ${knowledgeTotal} 条知识、${memoryTotal} 条记忆，检索零命中 ${zeroHits}/${retrievalRuns}。`
      : '暂无知识指标，概览使用本地 API fallback 等待 metrics API 接入。',
    kpis: [
      { key: 'knowledge', label: '知识总量', value: knowledgeTotal, unit: '条', helper: layerDistribution.length ? '覆盖分层结构' : '等待知识入库', tone: 'blue', tooltip: '来自 health.counts.knowledge，缺失时回退到 knowledge 列表长度。' },
      { key: 'memory', label: '在用记忆', value: activeMemory, unit: '条', helper: memoryTotal ? `总记忆 ${memoryTotal} 条` : '暂无 memory_items', tone: 'ink', tooltip: 'active 状态记忆条目，缺失状态时回退到总量。' },
      { key: 'health', label: '健康知识占比', value: healthPercent, unit: '%', helper: quarantined ? `隔离 ${quarantined} 条` : '无隔离冲突', tone: 'cyan', tooltip: '非 stale/expired 知识占比；后续可由 metrics API 提供更精确口径。' },
      { key: 'retrieval', label: '召回运行次数', value: retrievalRuns, unit: '次', helper: retrievalRuns ? `零命中 ${zeroHits} 次` : '尚无召回记录', tone: 'green', tooltip: '来自 retrieval quality 或最近 retrieval run 列表。' },
      { key: 'audit', label: '审计与闸门', value: totalAudit, unit: '项', helper: totalBacklog ? `积压 ${totalBacklog} 项` : '无明显积压', tone: 'slate', tooltip: '最近观测事件与 production gate 计划数量。' },
    ],
    layerDistribution,
    healthDistribution,
    stateDistribution,
    categoryDistribution,
    trend: eventTrend(input.events, input.retrievals, input.productionGates),
    hotSkills,
    hotKnowledge,
    riskBadges,
  };
}
