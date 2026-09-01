import React, { useEffect, useMemo, useState } from 'react';
import { mountReactPage, type PageDisposer } from './react-mount.js';
import { controlCsrfHeaders } from './control-csrf.js';
import {
  KmEmptyState,
  KmInlineHelp,
  KmOverview,
  KmPageFrame,
  KmSection,
  type KmOpsTab,
} from './km-dashboard-components.js';
import {
  buildKmDashboardModel,
  buildKmDashboardModelFromMetrics,
  buildKmDashboardModelV2,
  KM_DASHBOARD_EXPECTED_CONTRACT,
  type KmOpsMetricsRaw,
  type WorkspaceMetricsV2,
  type KmOpsTabId,
} from './km-dashboard-model.js';

type Health = {
  enabled: boolean;
  schemaVersion: number;
  pragmas: { journalMode: string; foreignKeys: number; busyTimeout: number };
  counts: { observations: number; quarantined: number; knowledge?: number; memory?: number };
  backlog: { queued: number; retryWait: number; oldestAgeMs: number; claimed: number };
  evalEvolution: { evalRuns: number; failingEvalRuns: number; reviewPendingProposals: number; latestEvalAt?: string; latestProposalAt?: string };
  capabilities: { requestedModes: string[]; effectiveModes: string[]; livePromptInjection: boolean; realMemoryTransport: boolean };
};

type KnowledgeItem = { knowledgeId: string; state: string; targetLayer: string; title: string; confidence: string; freshness: string };
export type WorkspaceAssetV2 = { assetId: string; workspaceId: string; layer: string; kind: string; title: string; relativePath: string; lifecycle: string; freshness: string; contract: { version: string; valid: boolean; errors: string[]; warnings: string[] }; retrieval: { recallCount: number; lastRecalledAt?: string }; linkage: { relatedCount: number; canonicalKey?: string } };
export type KmReviewQueueV2 = {
  schemaVersion: 2;
  generatedAt: string;
  state: 'available' | 'partial' | 'unavailable';
  sources: Array<{ workspaceId: string; kind: string; state: 'available' | 'unavailable'; relativePath: string | null; checksum: string | null; error?: string }>;
  summary: { total: number; unavailableManifests: number; byBatch: Record<string, number>; byRoute: Record<string, number>; byDecision: Record<string, number> };
  items: Array<{
    itemId: string;
    title: string;
    batch: string | null;
    route: string | null;
    decision: string | null;
    blockers: string[];
    planHash: string | null;
    auditTime: string | null;
    sourceRef: string | null;
    manifest: { state: 'available' | 'unavailable'; kind: string | null; relativePath: string | null; checksum: string | null };
  }>;
  errors: string[];
};

export const WORKSPACE_ASSET_PAGE_SIZE = 50;
export function filterWorkspaceAssets(items: WorkspaceAssetV2[], workspaceId: string, layer: string): WorkspaceAssetV2[] {
  return items.filter(item => (!workspaceId || item.workspaceId === workspaceId) && (!layer || item.layer === layer));
}
export function paginateWorkspaceAssets(items: WorkspaceAssetV2[], page: number, pageSize = WORKSPACE_ASSET_PAGE_SIZE) {
  const pageCount = Math.max(1, Math.ceil(items.length / pageSize));
  const currentPage = Math.min(Math.max(1, page), pageCount);
  const start = (currentPage - 1) * pageSize;
  return { items: items.slice(start, start + pageSize), currentPage, pageCount, start, end: Math.min(start + pageSize, items.length), total: items.length };
}
type KnowledgeExportJob = {
  jobId: string;
  state: string;
  plan: { knowledgeId: string; targetLayer: string; allowed: boolean; destination: { relativePath: string; writeMode: string; adapterId?: string; adapterKind?: string }; reasonCodes: string[]; diff: { status: string; lines: string[] } };
  manifest?: { contentHash: string; stagedFile?: string };
  execution?: { state: string; afterHash: string | null; destination: { root: string; relativePath: string }; precondition: { destinationVersion: string } };
};
type KnowledgeExportPreview = {
  jobId: string;
  allowed: boolean;
  reasonCodes: string[];
  confirmationToken: string;
  adapter: { adapterId: string; kind: string; commandPlan: string[] };
  destination: { root: string; relativePath: string; absolutePath: string };
  precondition: { currentTargetHash: string | null; destinationVersion: string };
  patch: { deterministicPatchHash: string; status: string; lines: string[] };
  risk: { mutatesWorkspace: boolean; network: false; gitPush: false; fixtureOnly: boolean };
};
type MemoryItem = { memoryId: string; state: string; scope: string; subject: string; claimKey: string; confidence: string };
type ImportJob = {
  jobId: string; state: string; sourceCount: number; eligibleCount: number; importedCount: number; dedupedCount: number;
  conflictCount: number; skippedCount: number; failedCount: number; outboxEnqueuedCount: number; configHash: string; updatedAt: string;
};
type EvalRun = { evalRunId: string; evaluatorName: string; targetType: string; targetId: string; passCount: number; warnCount: number; failCount: number };
type EvolutionProposal = { proposalId: string; state: string; proposalType: string; targetRef: string; approvalGrade: string; summary: string };
type TraceEdge = { edgeId: string; fromType: string; fromId: string; toType: string; toId: string; edgeType: string };
type SyncStatus = { sinkId: string; endpointRef: string; enabled: boolean; status: string; pending: number; quarantined: number; lastLocalSeq: number; lastAckAt?: string };
type CentralSinkStatus = {
  enabled: boolean;
  leaseName: string;
  protocol: { envelopeVersion: 1; signing: string; credentialMode: string; realTransportEnabled: false; networkLibrariesAllowed: false };
  defaults: { batchLimit: number; leaseMs: number; timeoutMs: number; maxAttempts: number };
  sinks: SyncStatus[];
  rollback: { automaticRemoteRollback: false; localDisableOnly: true };
};
type ProviderStatus = { providerId: string; kind: string; version: string; status: string; descriptor?: { capabilities?: string[]; execution?: string; supportsShadow?: boolean } };
type DistillationJob = { jobId: string; state: string; botAppId: string; profileId: string; attempts: number; lastError?: string };
type RetrievalAudit = { retrievalRunId: string; botAppId: string; mode: string; candidateCount: number; eligibleCount: number; latencyMs: number };
type InjectionSnapshot = { snapshotId: string; botAppId: string; mode: string; disposition: string; itemIds: string[]; promptBytes: number };
type PipelineProfile = { profile: { profileId: string; revision: number; botAppId: string; injectionMode: 'off' | 'shadow' | 'canary' | 'active'; memoryBackends: { writePolicy: string; primary: string; mirrors: string[] }; budgets: { promptTokens: number } }; state: string; requestedMode: string; effectiveMode: string; profileHash: string; createdAt: string };
type ProviderConfig = { providerId: 'mem0' | 'hindsight' | 'openviking'; endpoint: string; credentialRef: string; enabled: boolean; realTransportEnabled: false; timeoutMs: number; updatedAt: string };
type BackendRuntime = {
  enabled: boolean;
  leaseName: string;
  outbox: { total: number; pending: number; inflight: number; failed: number; delivered: number; quarantined: number; oldestPendingAgeMs: number };
  providers: Array<{
    providerId: string;
    endpoint: string;
    enabled: boolean;
    status: string;
    reason?: string;
    descriptor?: { contractVersion?: number; protocolVersion?: string; transport?: string; capabilities?: Record<string, boolean> };
    healthRequest?: { method: string; path: string; network: string };
    endpointPolicy?: { mode: string; reason?: string };
  }>;
};
type BackendOutboxItem = { outboxId: string; memoryId: string; providerId: string; operation: string; status: string; attempts: number; lastError?: string; updatedAt: string };
type BackendMigration = { migrationId: string; botAppId: string; state: string; checkpoint?: string; stats: Record<string, unknown>; updatedAt: string };
type MemoryPolicyDecision = { decisionId: string; sourceEventId: string; memoryId?: string; policyVersion: string; disposition: string; reasonCodes: string[]; evidence: { claimKey?: string; subject?: string }; createdAt: string };
type ConfigAudit = { auditId: string; actorId: string; action: string; targetRef: string; createdAt: string };
type RetrievalQuality = {
  runs: number;
  zeroHits: number;
  candidates: number;
  eligible: number;
  directHits: number;
  normalizedHits: number;
  noHits: number;
  filteredScope: number;
  filteredPrivacy: number;
  filteredState: number;
  avgLatencyMs: number;
};
type RetentionStatus = {
  enabled: boolean;
  leaseName: string;
  latestPlan: {
    policyVersion: string;
    generatedAt: string;
    dryRunOnly: true;
    destructiveActionsAvailable: false;
    domains: Array<{
      domain: string;
      table: string;
      tier: string;
      retentionDays: number;
      cutoff: string;
      totalCount: number;
      eligibleCount: number;
      protectedCount: number;
      oldestRecordAgeDays: number;
      oldestEligibleAgeDays: number;
    }>;
    db: { dbBytes: number; walBytes: number; totalBytes: number };
    operational: {
      backlog: Record<string, number>;
      quarantine: Record<string, number>;
      retry: Record<string, number>;
      providerQuality: Record<string, number>;
      retrievalQuality: Record<string, number>;
    };
    slo: Array<{ key: string; state: 'ok' | 'warn' | 'critical'; value: number; warnAt: number; criticalAt: number; unit: string }>;
    planHash: string;
  };
  reports: Array<{ reportId: string; completedAt: string; totalEligible: number; worstSloState: string; reportHash: string }>;
  trend: Array<{ reportId: string; completedAt: string; totalEligible: number; worstSloState: string; dbBytes: number; walBytes: number }>;
};
type GoldenCase = {
  caseId: string; revision: number; state: string; title: string; queryRedacted: string; expectedClaims: Array<{ claimKey: string; claimTextHash: string }>;
  contentHash: string; createdBy: string; reviewedBy: string; updatedAt: string;
};
type ShadowComparison = {
  comparisonId: string; caseId: string; revision: number;
  metrics: { claimOverlap: number; rulesUnique: number; piUnique: number; routingDisagreement: number; evidenceCoverage: number; privacyBlocks: number; schemaFailures: number; falsePositiveLabels: number; falseNegativeLabels: number };
  latency: Record<string, unknown>; cost: Record<string, unknown>; createdAt: string;
};
type ShadowReadiness = { ready: boolean; reasonCodes: string[]; metrics?: Record<string, number>; createdAt?: string };
type ProductionGatePlan = {
  planId: string;
  actionKind: 'real-memory-transport' | 'real-central-sink' | 'formal-knowledge-export' | 'prompt-canary' | 'retention-purge';
  state: string;
  target: Record<string, unknown>;
  scope: Record<string, unknown>;
  preview: Record<string, unknown>;
  previewHash: string;
  requiredApprovalGrade: string;
  actorId: string;
  riskAck: Record<string, unknown>;
  expiresAt: string;
  confirmationTokenHash: string;
  confirmationTokenUsedAt?: string;
  preflight: Array<Record<string, unknown>>;
  rollback: Record<string, unknown>;
  intent?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
};
type ProductionGateAudit = { auditId: string; planId: string; action: string; fromState?: string; toState: string; actorId: string; details: Record<string, unknown>; createdAt: string };
type ProductionGateKillState = { enabled: boolean; reason: string; actorId: string; updatedAt: string };
type ProductionGateList = { items: ProductionGatePlan[]; killSwitch: ProductionGateKillState };
type CanaryRuntimeStatus = {
  runtime: { active: boolean; planId?: string; botAppId: string; window?: { start: string; end: string }; reason: string };
  legacyEnvironmentActive?: boolean;
  restartRequired: boolean;
  autoFallback: 'shadow';
};

type ObservationEvent = {
  eventId: string;
  eventType: string;
  identity: {
    botAppId: string; sessionId: string; turnId?: string | null;
    skillName?: string | null; workflowId?: string | null; nodeId?: string | null;
  };
  source: { producer: string; adapter: string; confidence: string };
  ordering: { observedAt: string };
  payload: Record<string, unknown>;
};

const FUNNEL_STAGES = ['skill.manifest.resolved', 'skill.invoked', 'skill.completed', 'skill.failed'] as const;
const KM_TAB_STORAGE_KEY = 'botmux.km.activeTab';

function readKmTab(): KmOpsTabId {
  try {
    const value = window.localStorage.getItem(KM_TAB_STORAGE_KEY);
    return value === 'knowledge' || value === 'review' || value === 'memory' || value === 'quality' || value === 'configuration' || value === 'production' || value === 'audit'
      ? value
      : 'overview';
  } catch {
    return 'overview';
  }
}

function persistKmTab(tab: KmOpsTabId): void {
  try { window.localStorage.setItem(KM_TAB_STORAGE_KEY, tab); } catch { /* silent */ }
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path);
  if (!response.ok) throw new Error(await response.text());
  return response.json() as Promise<T>;
}

async function mutateJson<T>(path: string, method: 'POST' | 'PUT' | 'PATCH', body: unknown): Promise<T> {
  const response = await fetch(path, { method, headers: { 'content-type': 'application/json',
    ...controlCsrfHeaders(), 'idempotency-key': crypto.randomUUID() }, body: JSON.stringify(body) });
  if (!response.ok) throw new Error(await response.text());
  return response.json() as Promise<T>;
}

function funnelCounts(events: ObservationEvent[]): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const stage of FUNNEL_STAGES) counts[stage] = 0;
  for (const event of events) {
    if (event.eventType in counts) counts[event.eventType] += 1;
  }
  return counts;
}

function KmPage(): React.JSX.Element {
  const [health, setHealth] = useState<Health>();
  const [events, setEvents] = useState<ObservationEvent[]>([]);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<KmOpsTabId>(() => readKmTab());
  const [typeFilter, setTypeFilter] = useState<string>('');
  const [knowledge, setKnowledge] = useState<KnowledgeItem[]>([]);
  const [exportJobs, setExportJobs] = useState<KnowledgeExportJob[]>([]);
  const [exportPreviews, setExportPreviews] = useState<Record<string, KnowledgeExportPreview>>({});
  const [memory, setMemory] = useState<MemoryItem[]>([]);
  const [importJobs, setImportJobs] = useState<ImportJob[]>([]);
  const [evalRuns, setEvalRuns] = useState<EvalRun[]>([]);
  const [proposals, setProposals] = useState<EvolutionProposal[]>([]);
  const [traceType, setTraceType] = useState('turn');
  const [traceId, setTraceId] = useState('');
  const [traceEdges, setTraceEdges] = useState<TraceEdge[]>([]);
  const [syncStatus, setSyncStatus] = useState<SyncStatus[]>([]);
  const [centralSink, setCentralSink] = useState<CentralSinkStatus>();
  const [providers, setProviders] = useState<ProviderStatus[]>([]);
  const [jobs, setJobs] = useState<DistillationJob[]>([]);
  const [retrievals, setRetrievals] = useState<RetrievalAudit[]>([]);
  const [injections, setInjections] = useState<InjectionSnapshot[]>([]);
  const [profiles, setProfiles] = useState<PipelineProfile[]>([]);
  const [providerConfigs, setProviderConfigs] = useState<ProviderConfig[]>([]);
  const [backendRuntime, setBackendRuntime] = useState<BackendRuntime>();
  const [backendOutbox, setBackendOutbox] = useState<BackendOutboxItem[]>([]);
  const [backendMigrations, setBackendMigrations] = useState<BackendMigration[]>([]);
  const [policyDecisions, setPolicyDecisions] = useState<MemoryPolicyDecision[]>([]);
  const [configAudit, setConfigAudit] = useState<ConfigAudit[]>([]);
  const [retrievalQuality, setRetrievalQuality] = useState<RetrievalQuality>();
  const [retention, setRetention] = useState<RetentionStatus>();
  const [productionGates, setProductionGates] = useState<ProductionGatePlan[]>([]);
  const [productionGateKill, setProductionGateKill] = useState<ProductionGateKillState>();
  const [productionGateAudit, setProductionGateAudit] = useState<ProductionGateAudit[]>([]);
  const [productionGateHandoff, setProductionGateHandoff] = useState<Record<string, unknown>>();
  const [canaryRuntime, setCanaryRuntime] = useState<CanaryRuntimeStatus>();
  const [canaryStep, setCanaryStep] = useState(1);
  const [goldenCases, setGoldenCases] = useState<GoldenCase[]>([]);
  const [shadowComparisons, setShadowComparisons] = useState<ShadowComparison[]>([]);
  const [shadowReadiness, setShadowReadiness] = useState<ShadowReadiness>();
  const [dashboardMetrics, setDashboardMetrics] = useState<KmOpsMetricsRaw>();
  const [workspaceMetrics, setWorkspaceMetrics] = useState<WorkspaceMetricsV2>();
  const [workspaceAssets, setWorkspaceAssets] = useState<WorkspaceAssetV2[]>([]);
  const [reviewQueue, setReviewQueue] = useState<KmReviewQueueV2>();
  const [workspaceAssetWorkspace, setWorkspaceAssetWorkspace] = useState('');
  const [workspaceAssetLayer, setWorkspaceAssetLayer] = useState('');
  const [workspaceAssetPage, setWorkspaceAssetPage] = useState(1);
  const [goldenForm, setGoldenForm] = useState({ title: '', queryRedacted: '', claimKey: '', claimTextHash: `sha256:${'0'.repeat(64)}` });
  const [profileForm, setProfileForm] = useState({ botAppId: '', profileId: '', revision: 1, injectionMode: 'shadow' as const,
    primary: 'sqlite', mirrors: 'mem0,hindsight,openviking', promptTokens: 1800 });
  const [providerForm, setProviderForm] = useState({ providerId: 'mem0' as ProviderConfig['providerId'], endpoint: '', credentialRef: 'env:MEM0_API_KEY', enabled: false, timeoutMs: 5000 });
  const [sinkForm, setSinkForm] = useState({ sinkId: 'central-mock', endpointRef: 'mock://central', enabled: false, batchLimit: 25, timeoutMs: 5000, maxAttempts: 5, credentialRef: 'env:BOTMUX_KM_CENTRAL_SINK_SECRET', payloadMaxBytes: 65536 });
  const [importForm, setImportForm] = useState({ source: 'knowledge-items', allowlistedRoots: '', markdownFiles: '', defaultScope: 'workspace', defaultSubject: 'default' });
  const [productionGateForm, setProductionGateForm] = useState({
    actionKind: 'prompt-canary' as ProductionGatePlan['actionKind'],
    targetJson: JSON.stringify({ botAppId: 'cli_xxx', window: { start: '2026-08-28T00:00:00.000Z', end: '2026-08-28T01:00:00.000Z' } }, null, 2),
    scopeJson: JSON.stringify({ botAppId: 'cli_xxx', sessionClass: 'manual-canary' }, null, 2),
    riskAckJson: JSON.stringify({ acknowledged: true, note: 'reviewed by operator' }, null, 2),
    ttlSeconds: 900,
    confirmationToken: '',
    approvalGrade: 'G2',
  });
  const [canaryForm, setCanaryForm] = useState({
    botAppId: 'cli_aacca607f9ccdcf8',
    durationHours: 168,
    sessionClass: 'dashboard-canary-wizard',
    riskAcknowledged: false,
    rollbackCriteria: '隐私或作用域异常、错误率上升、延迟显著回归时立即回落 Shadow',
    confirmationToken: '',
    planId: '',
  });

  const load = async (type?: string) => {
    try {
      setLoading(true);
      setError('');
      const [workspaceAssetsResult, workspaceMetricsResult, reviewQueueResult, metricsResult, h, list, knowledgeList, exportList, memoryList, importJobList, productionGateList, evalList, proposalList, syncList, centralSinkStatus, providerList, jobList, retrievalList, injectionList, profileList, providerConfigList, backendRuntimeStatus, backendOutboxList, backendMigrationList, policyDecisionList, configAuditList, quality, retentionStatus, goldenList, comparisonList, readiness] = await Promise.all([
        getJson<{ items: WorkspaceAssetV2[] }>('/api/km/knowledge-assets-v2').then(
          result => ({ ok: true as const, result }),
          error => ({ ok: false as const, error }),
        ),
        getJson<WorkspaceMetricsV2>('/api/km/dashboard-metrics-v2?rankingLimit=10').then(
          metrics => ({ ok: true as const, metrics }),
          error => ({ ok: false as const, error }),
        ),
        getJson<KmReviewQueueV2>('/api/km/review-queue-v2').then(
          queue => ({ ok: true as const, queue }),
          error => ({ ok: false as const, error }),
        ),
        getJson<KmOpsMetricsRaw>('/api/km/dashboard-metrics?rankingLimit=10').then(
          metrics => ({ ok: true as const, metrics }),
          error => ({ ok: false as const, error }),
        ),
        getJson<Health>('/api/km/health'),
        getJson<{ items: ObservationEvent[] }>(`/api/km/observations?limit=100${type ? `&type=${encodeURIComponent(type)}` : ''}`),
        getJson<{ items: KnowledgeItem[] }>('/api/km/knowledge?limit=20'),
        getJson<{ items: KnowledgeExportJob[] }>('/api/km/exports'),
        getJson<{ items: MemoryItem[] }>('/api/km/memory?limit=20'),
        getJson<{ items: ImportJob[] }>('/api/km/imports?limit=20'),
        getJson<ProductionGateList>('/api/km/production-gates?limit=20'),
        getJson<{ items: EvalRun[] }>('/api/km/eval/runs?limit=20'),
        getJson<{ items: EvolutionProposal[] }>('/api/km/evolution/proposals?limit=20'),
        getJson<{ items: SyncStatus[] }>('/api/km/sync/sinks'),
        getJson<CentralSinkStatus>('/api/km/central-sink/status'),
        getJson<{ items: ProviderStatus[] }>('/api/km/providers'),
        getJson<{ items: DistillationJob[] }>('/api/km/distillation/jobs?limit=20'),
        getJson<{ items: RetrievalAudit[] }>('/api/km/retrieval/runs?limit=20'),
        getJson<{ items: InjectionSnapshot[] }>('/api/km/injections?limit=20'),
        getJson<{ items: PipelineProfile[] }>('/api/km/profiles'),
        getJson<{ items: ProviderConfig[] }>('/api/km/provider-configs'),
        getJson<BackendRuntime>('/api/km/backend-runtime'),
        getJson<{ items: BackendOutboxItem[] }>('/api/km/backend-outbox?limit=20'),
        getJson<{ items: BackendMigration[] }>('/api/km/backend-migrations?limit=20'),
        getJson<{ items: MemoryPolicyDecision[] }>('/api/km/memory-policy-decisions?limit=20'),
        getJson<{ items: ConfigAudit[] }>('/api/km/config-audit?limit=20'),
        getJson<RetrievalQuality>('/api/km/retrieval/quality'),
        getJson<RetentionStatus>('/api/km/retention'),
        getJson<{ items: GoldenCase[] }>('/api/km/golden-cases?limit=20'),
        getJson<{ items: ShadowComparison[] }>('/api/km/shadow-comparisons?limit=20'),
        getJson<ShadowReadiness>('/api/km/shadow-readiness'),
      ]);
      setHealth(h);
      setEvents(list.items);
      setKnowledge(knowledgeList.items);
      setExportJobs(exportList.items);
      setMemory(memoryList.items);
      setImportJobs(importJobList.items);
      setProductionGates(productionGateList.items);
      setProductionGateKill(productionGateList.killSwitch);
      setEvalRuns(evalList.items);
      setProposals(proposalList.items);
      setSyncStatus(syncList.items);
      setCentralSink(centralSinkStatus);
      setProviders(providerList.items); setJobs(jobList.items); setRetrievals(retrievalList.items); setInjections(injectionList.items);
      setProfiles(profileList.items); setProviderConfigs(providerConfigList.items);
      setBackendRuntime(backendRuntimeStatus); setBackendOutbox(backendOutboxList.items); setBackendMigrations(backendMigrationList.items);
      setPolicyDecisions(policyDecisionList.items); setConfigAudit(configAuditList.items); setRetrievalQuality(quality);
      setRetention(retentionStatus);
      setGoldenCases(goldenList.items); setShadowComparisons(comparisonList.items); setShadowReadiness(readiness);
      setWorkspaceAssets(workspaceAssetsResult.ok ? workspaceAssetsResult.result.items : []);
      setWorkspaceMetrics(workspaceMetricsResult.ok ? workspaceMetricsResult.metrics : undefined);
      setReviewQueue(reviewQueueResult.ok ? reviewQueueResult.queue : undefined);
      setDashboardMetrics(metricsResult.ok ? metricsResult.metrics : undefined);
      const activeCanary = productionGateList.items.find(plan => plan.actionKind === 'prompt-canary' && plan.state === 'executing');
      const botAppId = String((activeCanary?.target as { botAppId?: unknown } | undefined)?.botAppId ?? canaryForm.botAppId).trim();
      if (botAppId) {
        try { setCanaryRuntime(await getJson<CanaryRuntimeStatus>(`/api/km/canary-release/status?botAppId=${encodeURIComponent(botAppId)}`)); }
        catch { setCanaryRuntime(undefined); }
      }
      if (!metricsResult.ok) console.warn('KM dashboard metrics API unavailable, using fallback model', metricsResult.error);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { void load(); }, []);

  const loadTrace = async () => {
    if (!traceType.trim() || !traceId.trim()) return;
    try {
      const result = await getJson<{ items: TraceEdge[] }>(`/api/km/trace?type=${encodeURIComponent(traceType)}&id=${encodeURIComponent(traceId)}&limit=100`);
      setTraceEdges(result.items);
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };

  const saveProfile = async () => {
    try {
      const profile = { schemaVersion: 1, profileId: profileForm.profileId || `bot-${profileForm.botAppId}`, revision: profileForm.revision,
        botAppId: profileForm.botAppId, sourceProvider: 'observation-source-v1', windowProvider: 'bounded-transcript-window-v1',
        primaryExtractor: 'builtin.rules-v1', shadowExtractors: [], knowledgeRouter: 'builtin.layer-router-v1', memoryPolicy: 'safe-auto-activation-v1',
        memoryBackends: { writePolicy: 'primary-mirror', primary: profileForm.primary,
          mirrors: profileForm.mirrors.split(',').map(value => value.trim()).filter(Boolean) }, injectionMode: profileForm.injectionMode,
        budgets: { sourceBytes: 262144, sourceTokens: 32000, outputClaims: 20, promptTokens: profileForm.promptTokens } };
      await mutateJson('/api/km/profiles', 'POST', { profile, state: 'draft' }); setNotice('Profile Draft 已保存'); await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const changeProfileState = async (entry: PipelineProfile, state: string) => {
    if (!window.confirm(`确认将 ${entry.profile.profileId}@${entry.profile.revision} 从 ${entry.state} 切换为 ${state}？`)) return;
    try { await mutateJson(`/api/km/profiles/${encodeURIComponent(entry.profile.profileId)}/${entry.profile.revision}/state`, 'PATCH',
      { state, expectedHash: entry.profileHash }); setNotice(`Profile 已切换为 ${state}`); await load(); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const changeMemoryState = async (item: MemoryItem, state: string, reasonCode: string) => {
    if (!window.confirm(`确认将 ${item.claimKey} 从 ${item.state} 切换为 ${state}？`)) return;
    try {
      await mutateJson(`/api/km/memory/${encodeURIComponent(item.memoryId)}/state`, 'PATCH', { toState: state, reasonCode });
      setNotice(`Memory 已切换为 ${state}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const createExportJob = async (item: KnowledgeItem) => {
    if (!window.confirm(`确认为 ${item.title} 创建 KM 导出审核单？`)) return;
    try {
      await mutateJson('/api/km/exports', 'POST', { knowledgeId: item.knowledgeId });
      setNotice('导出审核单已创建');
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const reviewExportJob = async (job: KnowledgeExportJob, decision: 'approved' | 'rejected') => {
    if (!window.confirm(`确认${decision === 'approved' ? '批准并写入 staging outbox' : '拒绝'} ${job.jobId}？`)) return;
    try {
      await mutateJson(`/api/km/exports/${encodeURIComponent(job.jobId)}/review`, 'POST', {
        decision,
        reasonCode: decision === 'approved' ? 'manual_review_approved' : 'manual_review_rejected',
      });
      setNotice(decision === 'approved' ? '已写入 staging outbox，未修改正式知识目录' : '导出审核单已拒绝');
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const previewFormalExport = async (job: KnowledgeExportJob) => {
    try {
      const preview = await getJson<KnowledgeExportPreview>(`/api/km/exports/${encodeURIComponent(job.jobId)}/preview`);
      setExportPreviews(prev => ({ ...prev, [job.jobId]: preview }));
      setNotice(preview.allowed ? `执行预览已生成：${preview.patch.status}` : `执行仍被阻断：${preview.reasonCodes.join(', ')}`);
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const executeFormalExport = async (job: KnowledgeExportJob) => {
    const preview = exportPreviews[job.jobId];
    if (!preview) return;
    if (!window.confirm(`确认执行 ${job.jobId}？目标：${preview.destination.relativePath}`)) return;
    try {
      await mutateJson(`/api/km/exports/${encodeURIComponent(job.jobId)}/execute`, 'POST', {
        confirmationToken: preview.confirmationToken,
        approvalGrade: 'G2',
        expectedTargetHash: preview.precondition.currentTargetHash,
        destinationVersion: preview.precondition.destinationVersion,
      });
      setNotice(`KM 正式导出已执行：${job.jobId}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const rollbackFormalExport = async (job: KnowledgeExportJob) => {
    const preview = exportPreviews[job.jobId] ?? await getJson<KnowledgeExportPreview>(`/api/km/exports/${encodeURIComponent(job.jobId)}/preview`);
    if (!window.confirm(`确认回滚 ${job.jobId}？目标：${preview.destination.relativePath}`)) return;
    try {
      await mutateJson(`/api/km/exports/${encodeURIComponent(job.jobId)}/rollback`, 'POST', {
        confirmationToken: preview.confirmationToken,
        approvalGrade: 'G2',
        expectedTargetHash: job.execution?.afterHash ?? preview.precondition.currentTargetHash,
        destinationVersion: preview.precondition.destinationVersion,
      });
      setNotice(`KM 正式导出已回滚：${job.jobId}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const saveProvider = async () => {
    try { await mutateJson('/api/km/provider-configs', 'PUT', { ...providerForm, realTransportEnabled: false }); setNotice('Provider 配置已保存，真实 transport 仍关闭'); await load(); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const checkProvider = async (providerId: string) => {
    try { const result = await mutateJson<Record<string, unknown>>(`/api/km/provider-configs/${encodeURIComponent(providerId)}/health`, 'POST', {});
      setNotice(`配置检查：${providerId} = ${String(result.status)}（未发网络请求）`); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const saveSink = async () => {
    try {
      await mutateJson('/api/km/central-sink/sinks', 'PUT', {
        ...sinkForm,
        protocolVersion: 1,
        redactionPolicy: { mode: 'allowlisted-metadata-only' },
        allowlist: [new URL(sinkForm.endpointRef).host].filter(Boolean),
        rollback: { localDisableOnly: true, automaticRemoteRollback: false },
      });
      setNotice('Central sink 配置已保存；真实 transport 仍关闭');
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const runSinkDrill = async (sinkId: string, drill: 'status' | 'partial-ack' | 'replay' | 'conflict') => {
    try {
      const result = await mutateJson<Record<string, unknown>>('/api/km/central-sink/drills', 'POST', { sinkId, drill });
      setNotice(`Central sink drill ${drill}: ${JSON.stringify(result.result ?? result)}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const scanImport = async () => {
    try {
      const config = {
        source: importForm.source,
        allowlistedRoots: importForm.allowlistedRoots.split('\n').map(value => value.trim()).filter(Boolean),
        markdownFiles: importForm.markdownFiles.split('\n').map(value => value.trim()).filter(Boolean),
        defaultScope: importForm.defaultScope,
        defaultSubject: importForm.defaultSubject,
        enqueueBackendOutbox: false,
      };
      const report = await mutateJson<{ job: ImportJob }>('/api/km/imports', 'POST', { config });
      setNotice(`导入预览已创建：${report.job.jobId}，需显式执行`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const executeImport = async (job: ImportJob) => {
    if (!window.confirm(`确认执行 KM 导入 ${job.jobId}？这会写入本地 memory_items，冲突内容只标记不覆盖。`)) return;
    try {
      await mutateJson(`/api/km/imports/${encodeURIComponent(job.jobId)}/execute`, 'POST', { approvalToken: job.jobId });
      setNotice(`导入执行完成：${job.jobId}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const updateProductionGateTemplate = (actionKind: ProductionGatePlan['actionKind']) => {
    const now = new Date();
    const start = now.toISOString();
    const end = new Date(now.getTime() + 60 * 60 * 1000).toISOString();
    const targets: Record<ProductionGatePlan['actionKind'], Record<string, unknown>> = {
      'real-memory-transport': { provider: 'mem0', endpoint: 'https://memory.example.internal', credentialRef: 'env:MEM0_API_KEY' },
      'real-central-sink': { provider: 'central-sink', endpoint: 'https://km-central.example.internal', credentialRef: 'env:BOTMUX_KM_CENTRAL_SINK_SECRET' },
      'formal-knowledge-export': { destinationRoot: '/tmp/botmux-km-formal-export-fixture', manifestHash: `sha256:${'a'.repeat(64)}` },
      'prompt-canary': { botAppId: 'cli_xxx', window: { start, end } },
      'retention-purge': { cutoff: start, expectedCounts: { observations: 0 } },
    };
    setProductionGateForm(form => ({
      ...form,
      actionKind,
      targetJson: JSON.stringify(targets[actionKind], null, 2),
      approvalGrade: actionKind === 'retention-purge' ? 'G4' : actionKind === 'formal-knowledge-export' || actionKind === 'prompt-canary' ? 'G2' : 'G3',
    }));
  };
  const createCanaryPlan = async () => {
    if (!canaryForm.botAppId.trim() || !canaryForm.riskAcknowledged) return;
    const start = new Date();
    const end = new Date(start.getTime() + canaryForm.durationHours * 60 * 60 * 1000);
    const riskAck = { acknowledged: true, rollbackCriteria: canaryForm.rollbackCriteria, source: 'dashboard-canary-wizard' };
    try {
      const response = await mutateJson<{ plan: ProductionGatePlan; confirmationToken: string }>('/api/km/production-gates', 'POST', {
        actionKind: 'prompt-canary',
        target: { botAppId: canaryForm.botAppId.trim(), window: { start: start.toISOString(), end: end.toISOString() } },
        scope: { botAppId: canaryForm.botAppId.trim(), sessionClass: canaryForm.sessionClass },
        riskAck,
        ttlSeconds: Math.max(60, Math.ceil(canaryForm.durationHours * 3600)),
      });
      setCanaryForm(form => ({ ...form, planId: response.plan.planId, confirmationToken: response.confirmationToken }));
      setCanaryStep(3);
      setNotice(`Canary 影响预览已冻结：${response.plan.previewHash}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const activateCanary = async () => {
    const plan = productionGates.find(item => item.planId === canaryForm.planId);
    if (!plan || !window.confirm(`确认仅对 ${canaryForm.botAppId} 开启 ${canaryForm.durationHours} 小时 live Canary？到期自动回落 Shadow。`)) return;
    try {
      const result = await mutateJson<{ runtime: CanaryRuntimeStatus['runtime'] }>('/api/km/canary-release/activate', 'POST', {
        planId: plan.planId, approvalGrade: 'G2', confirmationToken: canaryForm.confirmationToken,
        previewHash: plan.previewHash, riskAck: plan.riskAck,
      });
      setCanaryRuntime({ runtime: result.runtime, restartRequired: false, autoFallback: 'shadow' });
      setCanaryStep(4);
      setNotice(`Canary 已生效：${plan.planId}，无需重启服务`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const rollbackCanary = async () => {
    const planId = canaryRuntime?.runtime.planId;
    if (!planId || !window.confirm('确认立即停止 live Canary 并回落 Shadow？')) return;
    try {
      await mutateJson(`/api/km/canary-release/${encodeURIComponent(planId)}/rollback`, 'POST', { reason: 'dashboard_operator_rollback' });
      setCanaryStep(1); setNotice('Canary 已回落 Shadow，无需重启服务'); await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const createProductionGate = async () => {
    try {
      const response = await mutateJson<{ plan: ProductionGatePlan; confirmationToken: string }>('/api/km/production-gates', 'POST', {
        actionKind: productionGateForm.actionKind,
        target: JSON.parse(productionGateForm.targetJson),
        scope: JSON.parse(productionGateForm.scopeJson),
        riskAck: JSON.parse(productionGateForm.riskAckJson),
        ttlSeconds: productionGateForm.ttlSeconds,
      });
      setProductionGateForm(form => ({ ...form, confirmationToken: response.confirmationToken, approvalGrade: response.plan.requiredApprovalGrade }));
      setNotice(`Production gate plan ready: ${response.plan.planId}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const approveProductionGate = async (plan: ProductionGatePlan) => {
    try {
      await mutateJson(`/api/km/production-gates/${encodeURIComponent(plan.planId)}/approve`, 'POST', {
        approvalGrade: productionGateForm.approvalGrade,
        confirmationToken: productionGateForm.confirmationToken,
        previewHash: plan.previewHash,
        riskAck: plan.riskAck,
      });
      setNotice(`Production gate approved: ${plan.planId}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const createProductionGateIntent = async (plan: ProductionGatePlan) => {
    try {
      const response = await mutateJson<{ intent: Record<string, unknown> }>(`/api/km/production-gates/${encodeURIComponent(plan.planId)}/intent`, 'POST', {
        confirmationToken: productionGateForm.confirmationToken,
        previewHash: plan.previewHash,
      });
      setProductionGateHandoff(response.intent);
      setNotice(`Inert intent created: ${String(response.intent.signedIntentHash)}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const loadProductionGateAudit = async (plan: ProductionGatePlan) => {
    try {
      const [audit, handoff] = await Promise.all([
        getJson<{ items: ProductionGateAudit[] }>(`/api/km/production-gates/${encodeURIComponent(plan.planId)}/audit`),
        getJson<Record<string, unknown>>(`/api/km/production-gates/${encodeURIComponent(plan.planId)}/handoff`),
      ]);
      setProductionGateAudit(audit.items);
      setProductionGateHandoff(handoff);
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const toggleProductionGateKill = async () => {
    try {
      await mutateJson('/api/km/production-gates/kill-switch', 'PUT', {
        enabled: !(productionGateKill?.enabled ?? false),
        reason: productionGateKill?.enabled ? 'dashboard_resume_intent_creation' : 'dashboard_emergency_stop',
      });
      setNotice('Production gate kill switch 已更新');
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const createGoldenCase = async () => {
    try {
      await mutateJson('/api/km/golden-cases', 'POST', {
        title: goldenForm.title,
        queryRedacted: goldenForm.queryRedacted,
        expectedClaims: [{ claimKey: goldenForm.claimKey, claimTextHash: goldenForm.claimTextHash }],
        sourceRefs: [{ kind: 'reviewed-distillation-example', ref: `dashboard:${Date.now()}` }],
        provenance: { explicitlyReviewed: true, redactionStatus: 'redacted', source: 'dashboard-manual' },
      });
      setNotice('Golden case 已保存为 reviewed revision');
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const compareGoldenCase = async (item: GoldenCase) => {
    try {
      await mutateJson('/api/km/shadow-comparisons', 'POST', {
        caseId: item.caseId,
        revision: item.revision,
        rulesClaims: item.expectedClaims.map(claim => ({ claimKey: claim.claimKey, route: 'rules', evidenceRefs: [{ kind: 'golden-case', ref: `${item.caseId}@${item.revision}` }] })),
        piClaims: [],
        latency: { rulesMs: 0, piMs: 0, source: 'stored-summary-only' },
        cost: { externalCalls: 0, piInvoked: false },
      });
      setNotice('已基于本地存储摘要记录 comparison，未调用 Pi 或外部网络');
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const labelComparison = async (item: ShadowComparison, label: 'false_positive' | 'false_negative') => {
    try {
      await mutateJson(`/api/km/shadow-comparisons/${encodeURIComponent(item.comparisonId)}/labels`, 'POST', {
        claimKey: label === 'false_positive' ? 'review.false_positive' : 'review.false_negative',
        extractor: label === 'false_positive' ? 'pi' : 'rules',
        label,
        reasonCode: 'dashboard_review',
      });
      setNotice(`Shadow label 已记录：${label}`);
      await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const refreshReadiness = async () => {
    try { await mutateJson('/api/km/shadow-readiness', 'POST', { thresholds: { minReviewedCases: 1, minComparisons: 1 } }); setNotice('Readiness report 已生成'); await load(); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };

  const funnel = funnelCounts(events);
  const maxFunnel = Math.max(1, ...Object.values(funnel));
  const dashboardModel = useMemo(() => workspaceMetrics ? buildKmDashboardModelV2(workspaceMetrics) : dashboardMetrics ? buildKmDashboardModelFromMetrics(dashboardMetrics) : buildKmDashboardModel({
    health,
    knowledge,
    memory,
    importJobs,
    retrievalQuality,
    retention,
    productionGates,
    events,
    retrievals,
    backendRuntime,
    centralSink,
    shadowReadiness,
  }), [workspaceMetrics, dashboardMetrics, health, knowledge, memory, importJobs, retrievalQuality, retention, productionGates, events, retrievals, backendRuntime, centralSink, shadowReadiness]);
  const selectTab = (tab: KmOpsTabId) => {
    setActiveTab(tab);
    persistKmTab(tab);
  };
  const TabOnly = ({ tab, children }: { tab: KmOpsTab['id']; children: React.ReactNode }) => (
    activeTab === tab ? <>{children}</> : null
  );
  const workspaceAssetWorkspaces = useMemo(() => [...new Set(workspaceAssets.map(item => item.workspaceId))].sort(), [workspaceAssets]);
  const workspaceAssetLayers = useMemo(() => [...new Set(workspaceAssets.map(item => item.layer))].sort(), [workspaceAssets]);
  const filteredWorkspaceAssets = useMemo(() => filterWorkspaceAssets(workspaceAssets, workspaceAssetWorkspace, workspaceAssetLayer), [workspaceAssets, workspaceAssetWorkspace, workspaceAssetLayer]);
  const workspaceAssetView = useMemo(() => paginateWorkspaceAssets(filteredWorkspaceAssets, workspaceAssetPage), [filteredWorkspaceAssets, workspaceAssetPage]);
  useEffect(() => { if (workspaceAssetPage !== workspaceAssetView.currentPage) setWorkspaceAssetPage(workspaceAssetView.currentPage); }, [workspaceAssetPage, workspaceAssetView.currentPage]);

  return (
    <KmPageFrame activeTab={activeTab} onTabChange={selectTab} model={dashboardModel} loading={loading} error={error} notice={notice}>
      <TabOnly tab="overview">
        <KmOverview model={dashboardModel} />
      </TabOnly>

      <TabOnly tab="knowledge">
        <KmSection title="分层知识资产" description="只读聚合自动发现 workspace 的 L0–L4 文件资产；不读取或展示正文。" badge="只读 · Contract v2" risk="low">
          <div className="km-asset-toolbar" aria-label="知识资产筛选">
            <label>Workspace<select aria-label="筛选 Workspace" value={workspaceAssetWorkspace} onChange={event => { setWorkspaceAssetWorkspace(event.target.value); setWorkspaceAssetPage(1); }}><option value="">全部（{workspaceAssetWorkspaces.length}）</option>{workspaceAssetWorkspaces.map(workspaceId => <option key={workspaceId} value={workspaceId}>{workspaceId}</option>)}</select></label>
            <label>层级<select aria-label="筛选知识层级" value={workspaceAssetLayer} onChange={event => { setWorkspaceAssetLayer(event.target.value); setWorkspaceAssetPage(1); }}><option value="">全部层级</option>{workspaceAssetLayers.map(layer => <option key={layer} value={layer}>{layer}</option>)}</select></label>
            <span className="km-asset-count">显示 {workspaceAssetView.total ? `${workspaceAssetView.start + 1}–${workspaceAssetView.end}` : '0'} / {workspaceAssetView.total}（总资产 {workspaceAssets.length}）</span>
          </div>
          <div className="feedback-deliveries">
            {workspaceAssetView.items.map(item => <div key={item.assetId}>
              <code>{item.layer}</code><span>{item.title} · {item.relativePath}</span>
              <span>{item.contract.version} · recall {item.retrieval.recallCount} · related {item.linkage.relatedCount}</span>
              <b>{item.lifecycle} / {item.freshness}{item.contract.valid ? '' : ` · ${item.contract.errors.join(', ') || item.contract.warnings.join(', ')}`}</b>
            </div>)}
            {workspaceAssetView.total === 0 && <KmEmptyState title={workspaceAssets.length ? '没有匹配的知识资产' : '暂无 workspace 资产快照'} message={workspaceAssets.length ? '请调整 Workspace 或层级筛选条件。' : '后台扫描尚未完成或当前会话未发现知识根目录。'} />}
          </div>
          <nav className="km-asset-pagination" aria-label="知识资产分页">
            <button type="button" disabled={workspaceAssetView.currentPage <= 1} onClick={() => setWorkspaceAssetPage(page => Math.max(1, page - 1))}>上一页</button>
            <span>第 {workspaceAssetView.currentPage} / {workspaceAssetView.pageCount} 页 · 每页 {WORKSPACE_ASSET_PAGE_SIZE} 项</span>
            <button type="button" disabled={workspaceAssetView.currentPage >= workspaceAssetView.pageCount} onClick={() => setWorkspaceAssetPage(page => Math.min(workspaceAssetView.pageCount, page + 1))}>下一页</button>
          </nav>
          <KmInlineHelp>workspace 根目录可从 Bot 默认 workingDir、当前 cwd 和会话 workingDir 自动发现；每个发现根仍执行 realpath、symlink 逃逸、文件大小和数量预算校验。</KmInlineHelp>
        </KmSection>

        <KmSection title="自动候选审核" description="处理 SQLite KM 自动提取的候选知识、导出 staging 和知识到记忆导入。" badge="中风险：写入 staging 或本地 memory 前需确认" risk="medium">
          <div className="feedback-deliveries">
            {knowledge.map(item => <div key={item.knowledgeId}><code>{item.targetLayer}</code><span>{item.title}</span><span>{item.confidence} · {item.freshness}</span><b>{item.state}{' '}
              {item.state === 'approved' && item.targetLayer !== 'reviewed-only' && item.freshness === 'fresh' && <button onClick={() => void createExportJob(item)}>Stage Export</button>}
            </b></div>)}
            {knowledge.length === 0 && <KmEmptyState title="暂无知识候选" message="开启观测和蒸馏后，这里会显示待审核知识。" />}
          </div>
        </KmSection>

        <KmSection title="Knowledge → Memory Import" description="扫描只读来源并生成预览，执行动作保留显式确认。" badge="渐进披露" risk="medium">
          <details className="km-advanced-panel">
            <summary>展开导入配置</summary>
            <div className="km-form-grid">
              <select value={importForm.source} onChange={e => setImportForm({ ...importForm, source: e.target.value })}>
                <option value="knowledge-items">approved knowledge_items</option>
                <option value="markdown-files">selected Markdown files</option>
                <option value="mixed">mixed</option>
              </select>
              <input value={importForm.defaultScope} onChange={e => setImportForm({ ...importForm, defaultScope: e.target.value })} placeholder="workspace/project/skill/environment/team" />
              <input value={importForm.defaultSubject} onChange={e => setImportForm({ ...importForm, defaultSubject: e.target.value })} placeholder="Subject" />
              <textarea value={importForm.allowlistedRoots} onChange={e => setImportForm({ ...importForm, allowlistedRoots: e.target.value })} placeholder="Allowlisted roots, one per line" />
              <textarea value={importForm.markdownFiles} onChange={e => setImportForm({ ...importForm, markdownFiles: e.target.value })} placeholder="Explicit Markdown files, one per line" />
              <button disabled={!importForm.allowlistedRoots.trim() || !importForm.defaultSubject.trim()} onClick={() => void scanImport()}>Scan Preview</button>
            </div>
          </details>
          <KmInlineHelp>扫描只读取 approved/fresh/observed/non-sensitive knowledge_items 和显式列出的 allowlist 内 Markdown；执行需确认，不启用外部 transport。</KmInlineHelp>
          <div className="feedback-deliveries">
            {importJobs.map(job => <div key={job.jobId}>
              <code>{job.state}</code>
              <span>{job.jobId}</span>
              <span>source {job.sourceCount} · eligible {job.eligibleCount} · imported {job.importedCount} · deduped {job.dedupedCount} · conflict {job.conflictCount} · skipped {job.skippedCount}</span>
              <b>{job.outboxEnqueuedCount} outbox {job.state === 'preview' || job.state === 'review_pending' || job.state === 'partial' ? <button onClick={() => void executeImport(job)}>Execute</button> : null}</b>
            </div>)}
            {importJobs.length === 0 && <KmEmptyState title="暂无导入任务" message="填写导入配置后可先生成扫描预览。" />}
          </div>
        </KmSection>

        <KmSection title="Knowledge Export Staging" description="导出审核单先进入 staging，再通过显式预览和确认执行。">
          <div className="feedback-deliveries">
            {exportJobs.map(job => {
              const preview = exportPreviews[job.jobId];
              return <div key={job.jobId}>
                <code>{job.plan.targetLayer}</code>
                <span>{job.plan.destination.relativePath}</span>
                <span>{preview ? `${preview.patch.status} · ${preview.allowed ? 'ready' : preview.reasonCodes.join(', ')}` : `${job.plan.diff.status} · ${job.plan.reasonCodes.join(', ') || 'ready'}`}</span>
                <b>{job.state}{' '}
                  {job.state === 'review_pending' && <button onClick={() => void reviewExportJob(job, 'approved')}>Approve</button>}{' '}
                  {job.state === 'review_pending' && <button onClick={() => void reviewExportJob(job, 'rejected')}>Reject</button>}{' '}
                  {(job.state === 'staged' || job.state === 'executing') && <button onClick={() => void previewFormalExport(job)}>Preview</button>}{' '}
                  {preview?.allowed && (job.state === 'staged' || job.state === 'executing') && <button onClick={() => void executeFormalExport(job)}>Execute</button>}{' '}
                  {job.state === 'applied' && <button onClick={() => void rollbackFormalExport(job)}>Rollback</button>}
                </b>
              </div>;
            })}
            {exportJobs.length === 0 && <KmEmptyState title="暂无导出审核单" message="通过知识审核列表创建 staging export。" />}
          </div>
        </KmSection>
      </TabOnly>

      <TabOnly tab="review">
        <KmSection title="Dashboard KM Review Queue" description="审批决策来自独立只读 manifest / registry；缺证据时显示 unavailable 或 null。" badge="只读 · v2" risk="low">
          <div className="km-review-summary" aria-label="Review queue summary">
            <span><small>State</small><strong>{reviewQueue?.state ?? 'unavailable'}</strong></span>
            <span><small>Total</small><strong>{reviewQueue?.summary.total ?? 0}</strong></span>
            <span><small>Missing Manifest</small><strong>{reviewQueue?.summary.unavailableManifests ?? 0}</strong></span>
            <span><small>Updated</small><strong>{reviewQueue?.generatedAt ? new Date(reviewQueue.generatedAt).toLocaleString() : '—'}</strong></span>
          </div>
          <div className="km-review-source-grid" aria-label="Review queue sources">
            {(reviewQueue?.sources ?? []).map(source => (
              <div key={`${source.workspaceId}:${source.relativePath ?? source.kind}`} className="km-review-source">
                <code>{source.kind}</code>
                <span>{source.relativePath ?? 'unavailable'}</span>
                <b>{source.state}</b>
                <small>{source.checksum ?? source.error ?? 'null'}</small>
              </div>
            ))}
            {reviewQueue && reviewQueue.sources.length === 0 ? <KmEmptyState title="暂无独立 registry" message="未发现 review-queue-v2 或 session-distill INDEX 元数据。" /> : null}
          </div>
          <div className="km-review-table" role="table" aria-label="KM review queue">
            <div role="row" className="km-review-row km-review-row--head">
              <span role="columnheader">Batch</span>
              <span role="columnheader">Route</span>
              <span role="columnheader">Decision</span>
              <span role="columnheader">Item</span>
              <span role="columnheader">Blockers</span>
              <span role="columnheader">Plan Hash</span>
              <span role="columnheader">Audit Time</span>
            </div>
            {(reviewQueue?.items ?? []).map(item => (
              <div role="row" className="km-review-row" key={`${item.sourceRef ?? 'source'}:${item.itemId}`}>
                <span role="cell"><code>{item.batch ?? 'unavailable'}</code></span>
                <span role="cell">{item.route ?? 'null'}</span>
                <span role="cell"><b className={`km-review-decision km-review-decision--${decisionTone(item.decision)}`}>{item.decision ?? 'null'}</b></span>
                <span role="cell"><strong>{item.title}</strong><small>{item.itemId} · {item.sourceRef ?? 'null'} · manifest {item.manifest.state}</small></span>
                <span role="cell">{item.blockers.length ? item.blockers.join(', ') : 'none'}</span>
                <span role="cell"><code>{shortHash(item.planHash)}</code></span>
                <span role="cell">{item.auditTime ? new Date(item.auditTime).toLocaleString() : 'null'}</span>
              </div>
            ))}
          </div>
          {(reviewQueue?.items.length ?? 0) === 0 ? <KmEmptyState title="Review queue 不可用" message="没有可展示的只读 registry/manifest 条目。" /> : null}
          <KmInlineHelp>此面板不读取 Markdown 正文，不返回敏感值或绝对路径；`decision` 只来自独立 manifest / registry 元数据，缺失时保持 null。</KmInlineHelp>
        </KmSection>
      </TabOnly>

      <TabOnly tab="memory">
        <KmSection title="Memory Review" description="按状态处理记忆条目，策略决策用于解释为什么被接纳或拦截。" badge="中风险：状态切换需确认" risk="medium">
          <div className="feedback-deliveries">
            {memory.map(item => <div key={item.memoryId}><code>{item.scope}</code><span>{item.subject} · {item.claimKey}</span><span>{item.confidence}</span><b>{item.state}{' '}
              {(item.state === 'proposed' || item.state === 'conflicted' || item.state === 'stale' || item.state === 'expired') && <button onClick={() => void changeMemoryState(item, 'active', 'review_approved')}>Approve</button>}{' '}
              {item.state === 'proposed' && <button onClick={() => void changeMemoryState(item, 'revoked', 'review_rejected')}>Reject</button>}{' '}
              {(item.state === 'active' || item.state === 'conflicted' || item.state === 'stale' || item.state === 'shadowed') && <button onClick={() => void changeMemoryState(item, 'revoked', 'review_revoked')}>Revoke</button>}{' '}
              {item.state === 'active' && <button onClick={() => void changeMemoryState(item, 'conflicted', 'review_conflict')}>Conflict</button>}
            </b></div>)}
            {policyDecisions.map(item => <div key={item.decisionId}><code>policy</code><span>{item.evidence.claimKey ?? item.sourceEventId} · {item.evidence.subject ?? '—'}</span><span>{item.reasonCodes.join(', ')}</span><b>{item.disposition}</b></div>)}
            {memory.length + policyDecisions.length === 0 && <KmEmptyState title="暂无待审核记忆" message="新的记忆候选或策略决策会显示在这里。" />}
          </div>
        </KmSection>

        <KmSection title="Backend Runtime / Migration" description="外部记忆后端 outbox、隔离和迁移状态。">
          <div className="feedback-deliveries">
            {backendOutbox.map(item => <div key={item.outboxId}><code>{item.providerId}</code><span>{item.operation} · {item.memoryId}</span><span>attempt {item.attempts}{item.lastError ? ` · ${item.lastError}` : ''}</span><b>{item.status}</b></div>)}
            {backendMigrations.map(item => <div key={item.migrationId}><code>migration</code><span>{item.botAppId} · {item.migrationId}</span><span>checkpoint {item.checkpoint ?? '—'} · {JSON.stringify(item.stats)}</span><b>{item.state}</b></div>)}
            {backendOutbox.length + backendMigrations.length === 0 && <KmEmptyState title="暂无运行任务" message="Backend outbox 或迁移任务出现后会列在这里。" />}
          </div>
        </KmSection>
      </TabOnly>

      <TabOnly tab="quality">
        {workspaceMetrics && <KmSection title="知识检索证据" description="区分 INDEX 查询、entry recall 与真正影响 reasoning 的 read/use evidence。">
          <div className="feedback-deliveries">
            <div><code>INDEX</code><span>索引查询</span><span>当前 recall log</span><b>{workspaceMetrics.retrievalQuality.indexQueries}</b></div>
            <div><code>recall</code><span>entry recall events</span><span>从未召回 {workspaceMetrics.retrievalQuality.neverRecalledAssets}</span><b>{workspaceMetrics.retrievalQuality.entryRecallEvents}</b></div>
            <div><code>read</code><span>INDEX→正文读取</span><span>read {workspaceMetrics.retrievalQuality.markdownReads} · 0 read {workspaceMetrics.retrievalQuality.zeroReadQueries ?? '—'}</span><b>{workspaceMetrics.retrievalQuality.zeroReadRate == null ? '—' : `${Math.max(0, 100 - workspaceMetrics.retrievalQuality.zeroReadRate)}%`}</b></div>
            <div><code>use</code><span>检索有效率</span><span>{workspaceMetrics.retrievalQuality.evidenceState} · effective {(workspaceMetrics.retrievalQuality.useLabels.direct_apply ?? 0) + (workspaceMetrics.retrievalQuality.useLabels.context_guided ?? 0) + (workspaceMetrics.retrievalQuality.useLabels.pitfall_avoided ?? 0)}</span><b>{workspaceMetrics.retrievalQuality.effectivenessRate == null ? '—' : `${workspaceMetrics.retrievalQuality.effectivenessRate}%`}</b></div>
            <div><code>fallback</code><span>远端 fallback 成功率</span><span>hash-only evidence</span><b>{workspaceMetrics.retrievalQuality.fallbackSuccessRate == null ? '—' : `${workspaceMetrics.retrievalQuality.fallbackSuccessRate}%`}</b></div>
            <div><code>feedback</code><span>Query 反馈覆盖率</span><span>query {workspaceMetrics.retrievalQuality.evidenceQueries} · invalid {workspaceMetrics.retrievalQuality.invalidEvidenceEvents}</span><b>{workspaceMetrics.retrievalQuality.queryFeedbackRate == null ? '—' : `${workspaceMetrics.retrievalQuality.queryFeedbackRate}%`}</b></div>
            <div><code>link</code><span>related_entries 覆盖</span><span>只读统计</span><b>{workspaceMetrics.assetHealth.linkageCoverageRate == null ? '—' : `${workspaceMetrics.assetHealth.linkageCoverageRate}%`}</b></div>
          </div>
          <KmInlineHelp>没有 markdown read/use/fallback/query-feedback 证据时显示 cold_start/—，不会把缺数据误报为 0% 或 100%。</KmInlineHelp>
        </KmSection>}

        <KmSection title="Skill 使用漏斗" description="最近 100 条观测事件的 Skill 分发、调用和完成情况。">
          {Object.entries(funnel).map(([stage, count]) => (
            <div className="feedback-bar" key={stage}>
              <span>{stage}</span>
              <i style={{ width: `${count / maxFunnel * 100}%` }} />
              <b>{count}</b>
            </div>
          ))}
          <KmInlineHelp>manifest.resolved = 会话分发的 Skill；invoked = 模型实际执行 botmux skill show/read 拉取内容；completed/failed = 命令退出状态。</KmInlineHelp>
        </KmSection>

        <KmSection title="Golden Set / Pi Shadow Quality" description="Golden set 只接受脱敏输入，Shadow quality 默认不调用外部模型或网络。">
          <details className="km-advanced-panel">
            <summary>展开 Golden case 输入</summary>
            <div className="km-form-grid">
              <input value={goldenForm.title} onChange={e => setGoldenForm({ ...goldenForm, title: e.target.value })} placeholder="Golden case title" />
              <input value={goldenForm.queryRedacted} onChange={e => setGoldenForm({ ...goldenForm, queryRedacted: e.target.value })} placeholder="Redacted query" />
              <input value={goldenForm.claimKey} onChange={e => setGoldenForm({ ...goldenForm, claimKey: e.target.value })} placeholder="Expected claim key" />
              <input value={goldenForm.claimTextHash} onChange={e => setGoldenForm({ ...goldenForm, claimTextHash: e.target.value })} placeholder="sha256:..." />
              <button disabled={!goldenForm.title.trim() || !goldenForm.queryRedacted.trim() || !goldenForm.claimKey.trim()} onClick={() => void createGoldenCase()}>保存 Golden</button>
              <button onClick={() => void refreshReadiness()}>生成 Readiness</button>
            </div>
          </details>
          <div className="feedback-deliveries">
            {goldenCases.map(item => <div key={`${item.caseId}@${item.revision}`}><code>{item.state}</code><span>{item.title}</span><span>{item.caseId}@{item.revision} · {item.expectedClaims.length} claims</span><b><button onClick={() => void compareGoldenCase(item)}>Compare</button></b></div>)}
            {shadowComparisons.map(item => <div key={item.comparisonId}><code>compare</code><span>{item.caseId}@{item.revision}</span><span>overlap {item.metrics.claimOverlap} · rules {item.metrics.rulesUnique} · pi {item.metrics.piUnique} · evidence {Math.round(item.metrics.evidenceCoverage * 100)}%</span><b>FP {item.metrics.falsePositiveLabels} / FN {item.metrics.falseNegativeLabels} <button onClick={() => void labelComparison(item, 'false_positive')}>FP</button> <button onClick={() => void labelComparison(item, 'false_negative')}>FN</button></b></div>)}
            {shadowReadiness && <div><code>readiness</code><span>{shadowReadiness.reasonCodes.join(', ') || 'thresholds_passed'}</span><span>{JSON.stringify(shadowReadiness.metrics ?? {})}</span><b>{shadowReadiness.ready ? 'ready' : 'not ready'}</b></div>}
            {goldenCases.length + shadowComparisons.length === 0 && <KmEmptyState title="暂无质量样本" message="保存 Golden case 后可生成 shadow comparison 和 readiness。" />}
          </div>
        </KmSection>

        <KmSection title="Trace / Eval / Evolution" description="查看 trace 边、评估运行和演进提案。">
          <div className="km-form-grid km-form-grid--compact">
            <input value={traceType} onChange={e => setTraceType(e.target.value)} placeholder="类型，如 turn" />
            <input value={traceId} onChange={e => setTraceId(e.target.value)} placeholder="ID" />
            <button onClick={() => void loadTrace()}>查询 Trace</button>
          </div>
          <div className="feedback-deliveries">
            {traceEdges.map(edge => <div key={edge.edgeId}><code>{edge.edgeType}</code><span>{edge.fromType}:{edge.fromId}</span><span>{edge.toType}:{edge.toId}</span><b>edge</b></div>)}
            {evalRuns.map(run => <div key={run.evalRunId}><code>{run.evaluatorName}</code><span>{run.targetType}:{run.targetId}</span><span>pass {run.passCount} · warn {run.warnCount}</span><b>fail {run.failCount}</b></div>)}
            {proposals.map(proposal => <div key={proposal.proposalId}><code>{proposal.proposalType}</code><span>{proposal.summary}</span><span>{proposal.targetRef}</span><b>{proposal.approvalGrade} · {proposal.state}</b></div>)}
            {traceEdges.length + evalRuns.length + proposals.length === 0 && <KmEmptyState title="暂无质量追踪" message="Trace、Eval 或 Evolution 数据出现后会聚合到这里。" />}
          </div>
        </KmSection>
      </TabOnly>

      <TabOnly tab="configuration">
        <KmSection title="Metrics Contract" description="并行开发的 metrics API 可直接实现该前端期望契约。">
          <pre className="km-contract-block">{JSON.stringify(KM_DASHBOARD_EXPECTED_CONTRACT, null, 2)}</pre>
        </KmSection>

        <KmSection title="Memory Settings" description="真实 Transport 默认禁用，配置项集中在此处而非总览。">
        <h3>Bot Pipeline Profile</h3>
        <div className="km-form-grid">
          <input value={profileForm.botAppId} onChange={e => setProfileForm({ ...profileForm, botAppId: e.target.value })} placeholder="Bot App ID" />
          <input value={profileForm.profileId} onChange={e => setProfileForm({ ...profileForm, profileId: e.target.value })} placeholder="Profile ID（可选）" />
          <input type="number" min="1" value={profileForm.revision} onChange={e => setProfileForm({ ...profileForm, revision: Number(e.target.value) })} title="Revision" />
          <select value={profileForm.injectionMode} onChange={e => setProfileForm({ ...profileForm, injectionMode: e.target.value as typeof profileForm.injectionMode })}>
            <option value="off">off</option><option value="shadow">shadow</option>
          </select>
          <input value={profileForm.primary} onChange={e => setProfileForm({ ...profileForm, primary: e.target.value })} placeholder="Primary backend" />
          <input value={profileForm.mirrors} onChange={e => setProfileForm({ ...profileForm, mirrors: e.target.value })} placeholder="Mirrors，逗号分隔" />
          <input type="number" min="1" max="8000" value={profileForm.promptTokens} onChange={e => setProfileForm({ ...profileForm, promptTokens: Number(e.target.value) })} title="Prompt token budget" />
          <button disabled={!profileForm.botAppId.trim()} onClick={() => void saveProfile()}>保存 Draft</button>
        </div>
        <div className="feedback-deliveries">
          {profiles.map(entry => <div key={`${entry.profile.profileId}@${entry.profile.revision}`}><code>{entry.state}</code>
            <span>{entry.profile.botAppId} · {entry.profile.profileId}@{entry.profile.revision}</span>
            <span>{entry.profile.memoryBackends.primary} + {entry.profile.memoryBackends.mirrors.join(', ')} · requested {entry.requestedMode} / effective {entry.effectiveMode}</span>
            <b><button onClick={() => void changeProfileState(entry, 'shadow')}>Shadow</button>{' '}<button onClick={() => void changeProfileState(entry, 'retired')}>Retire</button></b>
          </div>)}
        </div>
        <h3>External Provider Connection</h3>
        <div className="km-form-grid">
          <select value={providerForm.providerId} onChange={e => setProviderForm({ ...providerForm, providerId: e.target.value as ProviderConfig['providerId'] })}>
            <option value="mem0">Mem0</option><option value="hindsight">Hindsight</option><option value="openviking">OpenViking</option>
          </select>
          <input value={providerForm.endpoint} onChange={e => setProviderForm({ ...providerForm, endpoint: e.target.value })} placeholder="https://endpoint" />
          <input value={providerForm.credentialRef} onChange={e => setProviderForm({ ...providerForm, credentialRef: e.target.value })} placeholder="env:API_KEY" />
          <input type="number" min="100" max="30000" value={providerForm.timeoutMs} onChange={e => setProviderForm({ ...providerForm, timeoutMs: Number(e.target.value) })} title="Timeout ms" />
          <label><input type="checkbox" checked={providerForm.enabled} onChange={e => setProviderForm({ ...providerForm, enabled: e.target.checked })} /> Enabled</label>
          <button disabled={!providerForm.endpoint.trim()} onClick={() => void saveProvider()}>保存连接配置</button>
        </div>
        <KmInlineHelp>只保存 endpoint 与 credential reference；不保存密钥，不发网络请求，realTransportEnabled 固定为 false。</KmInlineHelp>
        <div className="feedback-deliveries">
          {providerConfigs.map(config => <div key={config.providerId}><code>{config.providerId}</code><span>{config.endpoint}</span><span>{config.credentialRef} · {config.timeoutMs}ms</span><b>{config.enabled ? 'configured' : 'disabled'} / transport off <button onClick={() => void checkProvider(config.providerId)}>检查</button></b></div>)}
          {backendRuntime?.providers.map(provider => {
            const caps = provider.descriptor?.capabilities ? Object.entries(provider.descriptor.capabilities).filter(([, enabled]) => enabled).map(([key]) => key).slice(0, 4).join(', ') : '—';
            const contract = provider.descriptor ? `contract v${provider.descriptor.contractVersion ?? '—'} · ${provider.descriptor.protocolVersion ?? provider.descriptor.transport ?? '—'}` : 'descriptor unavailable';
            const health = provider.healthRequest ? `${provider.healthRequest.method} ${provider.healthRequest.path} · ${provider.healthRequest.network}` : 'health descriptor unavailable';
            return <div key={`runtime-${provider.providerId}`}><code>runtime</code><span>{provider.providerId} · {provider.endpoint}</span><span>{provider.reason ?? provider.endpointPolicy?.reason ?? contract} · {health} · {caps}</span><b>{provider.status}</b></div>;
          })}
          {configAudit.map(item => <div key={item.auditId}><code>audit</code><span>{item.action} · {item.targetRef}</span><span>{item.actorId}</span><b>{item.createdAt}</b></div>)}
        </div>
      </KmSection>

        <KmSection title="Providers / Distillation / Retrieval Shadow" description="Provider 描述、蒸馏任务、召回和注入快照。">
        <div className="feedback-deliveries">
          {providers.map(provider => <div key={`${provider.providerId}@${provider.version}`}><code>{provider.kind}</code><span>{provider.providerId}</span><span>{provider.descriptor?.execution ?? '—'} · {(provider.descriptor?.capabilities ?? []).slice(0, 3).join(', ')}</span><b>v{provider.version} · {provider.status}</b></div>)}
          {jobs.map(job => <div key={job.jobId}><code>distill</code><span>{job.profileId} · {job.botAppId}</span><span>attempt {job.attempts}</span><b>{job.state}</b></div>)}
          {retrievals.map(run => <div key={run.retrievalRunId}><code>retrieve</code><span>{run.botAppId} · {run.mode}</span><span>{run.candidateCount} → {run.eligibleCount}</span><b>{run.latencyMs}ms</b></div>)}
          {injections.map(item => <div key={item.snapshotId}><code>inject</code><span>{item.botAppId} · {item.mode}</span><span>{item.itemIds.length} items · {item.promptBytes} bytes</span><b>{item.disposition}</b></div>)}
          {providers.length + jobs.length + retrievals.length + injections.length === 0 && <KmEmptyState title="暂无影子运行数据" message="自动蒸馏和检索影子尚未启用。" />}
        </div>
      </KmSection>

        <KmSection title="Central Sink / Sync" description="中心同步默认禁用，HTTP/HTTPS 只能保存为关闭配置。" badge="外部同步风险" risk="medium">
        <div className="km-form-grid">
          <input value={sinkForm.sinkId} onChange={e => setSinkForm({ ...sinkForm, sinkId: e.target.value })} placeholder="Sink ID" />
          <input value={sinkForm.endpointRef} onChange={e => setSinkForm({ ...sinkForm, endpointRef: e.target.value })} placeholder="mock://central 或 inmemory://central" />
          <input value={sinkForm.credentialRef} onChange={e => setSinkForm({ ...sinkForm, credentialRef: e.target.value })} placeholder="env:SECRET_NAME" />
          <input type="number" min="1" max="100" value={sinkForm.batchLimit} onChange={e => setSinkForm({ ...sinkForm, batchLimit: Number(e.target.value) })} title="Batch limit" />
          <input type="number" min="100" max="30000" value={sinkForm.timeoutMs} onChange={e => setSinkForm({ ...sinkForm, timeoutMs: Number(e.target.value) })} title="Timeout ms" />
          <input type="number" min="1" max="50" value={sinkForm.maxAttempts} onChange={e => setSinkForm({ ...sinkForm, maxAttempts: Number(e.target.value) })} title="Max attempts" />
          <input type="number" min="1024" max="262144" value={sinkForm.payloadMaxBytes} onChange={e => setSinkForm({ ...sinkForm, payloadMaxBytes: Number(e.target.value) })} title="Payload max bytes" />
          <label><input type="checkbox" checked={sinkForm.enabled} onChange={e => setSinkForm({ ...sinkForm, enabled: e.target.checked })} /> Enabled</label>
          <button disabled={!sinkForm.sinkId.trim() || !sinkForm.endpointRef.trim()} onClick={() => void saveSink()}>保存 Sink</button>
        </div>
        <KmInlineHelp>Runtime 需要 BOTMUX_KM_CENTRAL_SINK_ENABLED=true；只执行 mock:// 与 inmemory://。HTTPS/HTTP 仅可保存为关闭配置，不能启用真实传输。</KmInlineHelp>
        {centralSink && <KmInlineHelp>
          protocol v{centralSink.protocol.envelopeVersion} · {centralSink.protocol.signing} · {centralSink.protocol.credentialMode} · rollback: local-disable-only
        </KmInlineHelp>}
        <div className="feedback-deliveries">
          {syncStatus.map(sink => <div key={sink.sinkId}><code>{sink.sinkId}</code><span>{sink.endpointRef}</span><span>pending {sink.pending} · quarantine {sink.quarantined} · seq {sink.lastLocalSeq}</span><b>{sink.enabled ? sink.status : 'disabled'}{' '}
            <button onClick={() => void runSinkDrill(sink.sinkId, 'status')}>Status</button>{' '}
            <button onClick={() => void runSinkDrill(sink.sinkId, 'replay')}>Replay</button>{' '}
            <button onClick={() => void runSinkDrill(sink.sinkId, 'partial-ack')}>Partial Ack</button>{' '}
            <button onClick={() => void runSinkDrill(sink.sinkId, 'conflict')}>Conflict</button>
          </b></div>)}
          {syncStatus.length === 0 && <KmEmptyState title="未配置 Sink" message="本地能力不受影响。" />}
        </div>
      </KmSection>
      </TabOnly>

      <TabOnly tab="production">
        <KmSection title="Canary 发布向导" description="精确 Bot、限时生效、G2 审批；无需修改 PM2 环境变量或重启服务，到期自动回落 Shadow。" badge="LIVE · 精确作用域" risk="high">
          <ol className="km-canary-steps" aria-label="Canary 发布步骤">
            {['选择 Bot 与窗口', '确认影响与回滚', 'G2 确认发布', '观察与回落'].map((label, index) => <li key={label} className={canaryStep >= index + 1 ? 'is-active' : ''}><b>{index + 1}</b><span>{label}</span></li>)}
          </ol>
          <div className="km-canary-console">
            <div className="km-form-grid">
              <label>精确 Bot App ID<input value={canaryForm.botAppId} onChange={e => setCanaryForm({ ...canaryForm, botAppId: e.target.value })} placeholder="cli_xxx" /></label>
              <label>观察窗口（小时）<input type="number" min="1" max="744" value={canaryForm.durationHours} onChange={e => setCanaryForm({ ...canaryForm, durationHours: Number(e.target.value) })} /></label>
              <label>会话范围<input value={canaryForm.sessionClass} onChange={e => setCanaryForm({ ...canaryForm, sessionClass: e.target.value })} /></label>
              <label className="km-canary-wide">回滚条件<textarea value={canaryForm.rollbackCriteria} onChange={e => setCanaryForm({ ...canaryForm, rollbackCriteria: e.target.value })} /></label>
            </div>
            <div className="km-canary-impact">
              <span><small>运行边界</small><strong>仅 {canaryForm.botAppId || '未选择'}</strong></span>
              <span><small>持续时间</small><strong>{canaryForm.durationHours} 小时</strong></span>
              <span><small>到期策略</small><strong>自动 Shadow</strong></span>
              <span><small>服务操作</small><strong>无需重启</strong></span>
            </div>
            <label className="km-canary-ack"><input type="checkbox" checked={canaryForm.riskAcknowledged} onChange={e => { setCanaryForm({ ...canaryForm, riskAcknowledged: e.target.checked }); setCanaryStep(e.target.checked ? 2 : 1); }} /> 我已核对精确 Bot、观察窗口、隐私边界和回滚条件</label>
            <div className="km-canary-actions">
              <button disabled={!canaryForm.riskAcknowledged || !canaryForm.botAppId.trim()} onClick={() => void createCanaryPlan()}>生成影响预览</button>
              <button className="danger" disabled={!canaryForm.planId || !canaryForm.confirmationToken} onClick={() => void activateCanary()}>G2 确认并发布</button>
              <button disabled={!canaryRuntime?.runtime.active} onClick={() => void rollbackCanary()}>立即回落 Shadow</button>
            </div>
          </div>
          <div className={`km-canary-status ${canaryRuntime?.runtime.active || canaryRuntime?.legacyEnvironmentActive ? 'is-live' : ''}`}>
            <span className="km-canary-pulse" aria-hidden="true" />
            <div><small>当前运行态</small><strong>{canaryRuntime?.runtime.active ? 'LIVE CANARY' : canaryRuntime?.legacyEnvironmentActive ? 'LIVE · 旧版环境配置' : 'SHADOW / 未激活'}</strong></div>
            <div><small>Bot</small><strong>{canaryRuntime?.runtime.botAppId ?? canaryForm.botAppId}</strong></div>
            <div><small>窗口结束</small><strong>{canaryRuntime?.runtime.window?.end ? new Date(canaryRuntime.runtime.window.end).toLocaleString() : '—'}</strong></div>
            <div><small>原因</small><strong>{canaryRuntime?.runtime.reason ?? 'not_loaded'}</strong></div>
          </div>
          <KmInlineHelp>发布会写入 SQLite 的 action-scoped runtime intent；每次请求实时校验 exact Bot、G2 状态、Kill Switch 与时间窗口。窗口结束后即使进程不重启，也会 fail-closed 回到 Shadow。</KmInlineHelp>
        </KmSection>

        <KmSection title="Production Gates（高级）" description="通用生产闸门：计划、审批、Intent 与审计。Canary 日常发布优先使用上方向导。" badge="高风险：需要审批令牌" risk="high">
          <details className="km-advanced-panel">
            <summary>展开生产闸门表单</summary>
            <div className="km-form-grid">
              <select value={productionGateForm.actionKind} onChange={e => updateProductionGateTemplate(e.target.value as ProductionGatePlan['actionKind'])}>
                <option value="real-memory-transport">real-memory-transport</option>
                <option value="real-central-sink">real-central-sink</option>
                <option value="formal-knowledge-export">formal-knowledge-export</option>
                <option value="prompt-canary">prompt-canary</option>
                <option value="retention-purge">retention-purge</option>
              </select>
              <input type="number" min="60" max="2678400" value={productionGateForm.ttlSeconds} onChange={e => setProductionGateForm({ ...productionGateForm, ttlSeconds: Number(e.target.value) })} title="TTL seconds" />
              <select value={productionGateForm.approvalGrade} onChange={e => setProductionGateForm({ ...productionGateForm, approvalGrade: e.target.value })}>
                <option value="G0">G0</option><option value="G1">G1</option><option value="G2">G2</option><option value="G3">G3</option><option value="G4">G4</option>
              </select>
              <input value={productionGateForm.confirmationToken} onChange={e => setProductionGateForm({ ...productionGateForm, confirmationToken: e.target.value })} placeholder="Confirmation token from created plan" />
              <button onClick={() => void createProductionGate()}>Create Plan</button>
              <button className="danger" onClick={() => void toggleProductionGateKill()}>{productionGateKill?.enabled ? 'Disable Kill' : 'Enable Kill'}</button>
              <textarea value={productionGateForm.targetJson} onChange={e => setProductionGateForm({ ...productionGateForm, targetJson: e.target.value })} placeholder="Exact target JSON" />
              <textarea value={productionGateForm.scopeJson} onChange={e => setProductionGateForm({ ...productionGateForm, scopeJson: e.target.value })} placeholder="Exact non-wildcard scope JSON" />
              <textarea value={productionGateForm.riskAckJson} onChange={e => setProductionGateForm({ ...productionGateForm, riskAckJson: e.target.value })} placeholder="Risk acknowledgement JSON" />
            </div>
          </details>
          <KmInlineHelp>effective=false，sideEffectsExecuted=false，不触发网络、导出、注入、删除或调度器。</KmInlineHelp>
          {productionGateKill && <p className={`km-kill-state ${productionGateKill.enabled ? 'enabled' : ''}`}>
            kill switch: {productionGateKill.enabled ? 'enabled' : 'disabled'} · {productionGateKill.reason} · {productionGateKill.updatedAt}
          </p>}
          <div className="feedback-deliveries">
            {productionGates.map(plan => <div key={plan.planId}>
              <code>{plan.actionKind}</code>
              <span>{plan.planId} · {plan.previewHash}</span>
              <span>{plan.requiredApprovalGrade} · expires {plan.expiresAt} · token {plan.confirmationTokenUsedAt ? 'used' : 'unused'}</span>
              <b>{plan.state}{' '}
                {plan.state === 'ready' && <button onClick={() => void approveProductionGate(plan)}>Approve</button>}{' '}
                {plan.state === 'approved' && <button onClick={() => void createProductionGateIntent(plan)}>Intent</button>}{' '}
                <button onClick={() => void loadProductionGateAudit(plan)}>Audit</button>
              </b>
            </div>)}
            {productionGates.length === 0 && <KmEmptyState title="暂无 production gate plan" message="需要执行高风险上线前，先在这里创建可审计计划。" />}
            {productionGateAudit.map(item => <div key={item.auditId}>
              <code>audit</code>
              <span>{item.action} · {item.fromState ?? 'none'} → {item.toState}</span>
              <span>{item.actorId}</span>
              <b>{item.createdAt}</b>
            </div>)}
          </div>
          {productionGateHandoff && <pre className="km-contract-block">{JSON.stringify(productionGateHandoff, null, 2)}</pre>}
        </KmSection>
      </TabOnly>

      <TabOnly tab="audit">
        <KmSection title="Retention / GC Preview（只读）" description="策略只生成 eligibility preview 和 shadow report，没有 purge/apply 按钮。" badge="只读" risk="low">
          <div className="feedback-deliveries">
            {retention?.latestPlan.domains.map(domain => <div key={domain.domain}>
              <code>{domain.tier}</code>
              <span>{domain.domain} · {domain.table}</span>
              <span>total {domain.totalCount} · protected {domain.protectedCount} · eligible {domain.eligibleCount}</span>
              <b>{domain.retentionDays}d · oldest {domain.oldestRecordAgeDays}d</b>
            </div>)}
            {retention?.latestPlan.slo.map(metric => <div key={metric.key}>
              <code>SLO</code>
              <span>{metric.key}</span>
              <span>{metric.value} {metric.unit} · warn {metric.warnAt} · critical {metric.criticalAt}</span>
              <b>{metric.state}</b>
            </div>)}
            {retention?.reports.slice(0, 5).map(report => <div key={report.reportId}>
              <code>report</code>
              <span>{report.completedAt}</span>
              <span>{report.reportHash}</span>
              <b>{report.totalEligible} · {report.worstSloState}</b>
            </div>)}
            {!retention && <KmEmptyState title="Retention 状态暂不可用" message="GC preview API 返回后会显示保留策略和 SLO。" />}
          </div>
        </KmSection>

        <KmSection title="事件列表" description="按事件类型筛选最近观测，便于审计输入来源。">
        <div className="km-form-grid km-form-grid--compact">
          <select value={typeFilter} onChange={e => { setTypeFilter(e.target.value); void load(e.target.value || undefined); }}>
            <option value="">全部类型</option>
            {FUNNEL_STAGES.map(stage => <option key={stage} value={stage}>{stage}</option>)}
            <option value="turn.completed">turn.completed</option>
            <option value="workflow.artifact.produced">workflow.artifact.produced</option>
          </select>
        </div>
        <div className="feedback-deliveries">
          {events.map(event => (
            <div key={event.eventId}>
              <code>{event.eventType}</code>
              <span>{event.ordering.observedAt}</span>
              <span>
                {event.identity.skillName ?? event.identity.nodeId ?? event.identity.sessionId.slice(0, 8)}
                {' · '}{event.identity.botAppId.slice(-8)} · {event.source.adapter}
              </span>
              <b>{event.source.confidence === 'observed' ? '✓ observed' : '~ inferred'}</b>
            </div>
          ))}
          {events.length === 0 && <KmEmptyState title="暂无事件" message="开启 BOTMUX_KM_OBSERVATION_ENABLED=true 并使用一段时间后刷新。" />}
        </div>
      </KmSection>
      </TabOnly>
    </KmPageFrame>
  );
}

function decisionTone(decision: string | null): 'approved' | 'pending' | 'blocked' | 'neutral' {
  if (!decision) return 'neutral';
  if (decision.includes('approved') || decision.includes('human-confirmed') || decision.includes('staged') || decision.includes('applied')) return 'approved';
  if (decision.includes('reject') || decision.includes('block') || decision.includes('conflict') || decision.includes('fail')) return 'blocked';
  if (decision.includes('pending') || decision.includes('review') || decision.includes('ready')) return 'pending';
  return 'neutral';
}

function shortHash(value: string | null): string {
  if (!value) return 'null';
  return value.length > 20 ? `${value.slice(0, 17)}...` : value;
}

export function renderKmPage(root: HTMLElement): PageDisposer {
  return mountReactPage(root, <KmPage />);
}
