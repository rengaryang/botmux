export type KmOpsTabId = 'overview' | 'knowledge' | 'memory' | 'quality' | 'configuration' | 'production' | 'audit';

export type KmExpectedMetricsContract = {
  schemaVersion: 1;
  path: '/api/km/dashboard-metrics';
  note: string;
  fields: {
    kpis: 'totalKnowledge, activeMemory, healthPercent, retrievalRuns, auditEvents';
    distributions: 'layerDistribution[], healthDistribution[], stateDistribution[], categoryDistribution[]';
    trend: 'time-series points with observations, retrievals, gates';
    rankings: 'hotSkills[], hotKnowledge[]';
  };
};

export const KM_DASHBOARD_EXPECTED_CONTRACT: KmExpectedMetricsContract = {
  schemaVersion: 1,
  path: '/api/km/dashboard-metrics',
  note: 'The dedicated metrics API may replace this fallback. Keep labels, units, and nullability compatible with this view model.',
  fields: {
    kpis: 'totalKnowledge, activeMemory, healthPercent, retrievalRuns, auditEvents',
    distributions: 'layerDistribution[], healthDistribution[], stateDistribution[], categoryDistribution[]',
    trend: 'time-series points with observations, retrievals, gates',
    rankings: 'hotSkills[], hotKnowledge[]',
  },
};

export type KmMetricPoint = {
  key: string;
  label: string;
  value: number;
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

export type KmDashboardModel = {
  generatedAt: string;
  source: 'fallback';
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
