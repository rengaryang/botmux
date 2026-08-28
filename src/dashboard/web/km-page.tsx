import React, { useEffect, useState } from 'react';
import { mountReactPage, type PageDisposer } from './react-mount.js';
import { controlCsrfHeaders } from './control-csrf.js';

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
type KnowledgeExportJob = {
  jobId: string;
  state: string;
  plan: { knowledgeId: string; targetLayer: string; allowed: boolean; destination: { relativePath: string; writeMode: string }; reasonCodes: string[]; diff: { status: string; lines: string[] } };
  manifest?: { contentHash: string; stagedFile?: string };
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
  const [typeFilter, setTypeFilter] = useState<string>('');
  const [knowledge, setKnowledge] = useState<KnowledgeItem[]>([]);
  const [exportJobs, setExportJobs] = useState<KnowledgeExportJob[]>([]);
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
  const [goldenCases, setGoldenCases] = useState<GoldenCase[]>([]);
  const [shadowComparisons, setShadowComparisons] = useState<ShadowComparison[]>([]);
  const [shadowReadiness, setShadowReadiness] = useState<ShadowReadiness>();
  const [goldenForm, setGoldenForm] = useState({ title: '', queryRedacted: '', claimKey: '', claimTextHash: `sha256:${'0'.repeat(64)}` });
  const [profileForm, setProfileForm] = useState({ botAppId: '', profileId: '', revision: 1, injectionMode: 'shadow' as const,
    primary: 'sqlite', mirrors: 'mem0,hindsight,openviking', promptTokens: 1800 });
  const [providerForm, setProviderForm] = useState({ providerId: 'mem0' as ProviderConfig['providerId'], endpoint: '', credentialRef: 'env:MEM0_API_KEY', enabled: false, timeoutMs: 5000 });
  const [sinkForm, setSinkForm] = useState({ sinkId: 'central-mock', endpointRef: 'mock://central', enabled: false, batchLimit: 25, timeoutMs: 5000, maxAttempts: 5, credentialRef: 'env:BOTMUX_KM_CENTRAL_SINK_SECRET', payloadMaxBytes: 65536 });
  const [importForm, setImportForm] = useState({ source: 'knowledge-items', allowlistedRoots: '', markdownFiles: '', defaultScope: 'workspace', defaultSubject: 'default' });

  const load = async (type?: string) => {
    try {
      setError('');
      const [h, list, knowledgeList, exportList, memoryList, importJobList, evalList, proposalList, syncList, centralSinkStatus, providerList, jobList, retrievalList, injectionList, profileList, providerConfigList, backendRuntimeStatus, backendOutboxList, backendMigrationList, policyDecisionList, configAuditList, quality, retentionStatus, goldenList, comparisonList, readiness] = await Promise.all([
        getJson<Health>('/api/km/health'),
        getJson<{ items: ObservationEvent[] }>(`/api/km/observations?limit=100${type ? `&type=${encodeURIComponent(type)}` : ''}`),
        getJson<{ items: KnowledgeItem[] }>('/api/km/knowledge?limit=20'),
        getJson<{ items: KnowledgeExportJob[] }>('/api/km/exports'),
        getJson<{ items: MemoryItem[] }>('/api/km/memory?limit=20'),
        getJson<{ items: ImportJob[] }>('/api/km/imports?limit=20'),
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
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
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

  return (
    <div className="page km-page">
      <header className="sect-header">
        <div>
          <h1>Skill Intelligence（KM）</h1>
          <p>本地观测数据 · 默认关闭 · BOTMUX_KM_OBSERVATION_ENABLED=true 时采集</p>
        </div>
      </header>
      {error && <p className="error-banner">{error}</p>}
      {notice && <p style={{ color: 'var(--success, #248a3d)' }}>{notice}</p>}

      <section className="feedback-kpis">
        <article><span>观测事件</span><strong>{health?.counts.observations ?? '—'}</strong></article>
        <article><span>隔离冲突</span><strong>{health?.counts.quarantined ?? '—'}</strong></article>
        <article><span>知识候选</span><strong>{health?.counts.knowledge ?? '—'}</strong></article>
        <article><span>记忆条目</span><strong>{health?.counts.memory ?? '—'}</strong></article>
        <article><span>导入任务</span><strong>{importJobs.length}</strong></article>
        <article><span>Schema 版本</span><strong>{health?.schemaVersion ?? '—'}</strong></article>
        <article><span>WAL 模式</span><strong>{health?.pragmas.journalMode ?? '—'}</strong></article>
        <article><span>采集状态</span><strong>{health?.enabled ? '已开启' : '未开启'}</strong></article>
        <article><span>蒸馏积压</span><strong>{(health?.backlog.queued ?? 0) + (health?.backlog.retryWait ?? 0)}</strong></article>
        <article><span>有效模式</span><strong>{health?.capabilities.effectiveModes.join('/') ?? '—'}</strong></article>
        <article><span>Backend Worker</span><strong>{backendRuntime?.enabled ? '已开启' : '未开启'}</strong></article>
        <article><span>Central Sink</span><strong>{centralSink?.enabled ? '已开启' : '未开启'}</strong></article>
        <article><span>Backend Outbox</span><strong>{backendRuntime?.outbox.pending ?? '—'}/{backendRuntime?.outbox.total ?? '—'}</strong></article>
        <article><span>Backend 隔离</span><strong>{backendRuntime?.outbox.quarantined ?? '—'}</strong></article>
        <article><span>Eval 运行</span><strong>{health?.evalEvolution.evalRuns ?? '—'}</strong></article>
        <article><span>失败 Eval</span><strong>{health?.evalEvolution.failingEvalRuns ?? '—'}</strong></article>
        <article><span>待审提案</span><strong>{health?.evalEvolution.reviewPendingProposals ?? '—'}</strong></article>
        <article><span>Retrieval 零命中</span><strong>{retrievalQuality ? `${retrievalQuality.zeroHits}/${retrievalQuality.runs}` : '—'}</strong></article>
        <article><span>Retrieval 直接命中</span><strong>{retrievalQuality?.directHits ?? '—'}</strong></article>
        <article><span>Retrieval 归一命中</span><strong>{retrievalQuality?.normalizedHits ?? '—'}</strong></article>
        <article><span>Retrieval 未匹配</span><strong>{retrievalQuality?.noHits ?? '—'}</strong></article>
        <article><span>Retrieval Scope 过滤</span><strong>{retrievalQuality?.filteredScope ?? '—'}</strong></article>
        <article><span>Retrieval 隐私过滤</span><strong>{retrievalQuality?.filteredPrivacy ?? '—'}</strong></article>
        <article><span>Retrieval 状态过滤</span><strong>{retrievalQuality?.filteredState ?? '—'}</strong></article>
        <article><span>Retrieval 均延迟</span><strong>{retrievalQuality ? `${retrievalQuality.avgLatencyMs}ms` : '—'}</strong></article>
        <article><span>Retention Shadow</span><strong>{retention?.enabled ? '已开启' : '未开启'}</strong></article>
        <article><span>可归档预览</span><strong>{retention ? retention.latestPlan.domains.reduce((sum, item) => sum + item.eligibleCount, 0) : '—'}</strong></article>
        <article><span>KM DB+WAL</span><strong>{retention ? `${Math.round(retention.latestPlan.db.totalBytes / 1024)} KiB` : '—'}</strong></article>
        <article><span>SLO 状态</span><strong>{retention ? retention.latestPlan.slo.find(item => item.state === 'critical')?.state ?? retention.latestPlan.slo.find(item => item.state === 'warn')?.state ?? 'ok' : '—'}</strong></article>
      </section>

      <section className="panel">
        <h2>Retention / GC Preview（只读）</h2>
        <p style={{ color: 'var(--text-dim)', fontSize: 12 }}>
          策略只生成 eligibility preview 和 shadow report；没有 purge/apply 按钮，BOTMUX_KM_AUTO_GC_ENABLED 不会触发删除。
        </p>
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
          {!retention && <p style={{ color: 'var(--text-dim)' }}>Retention 状态暂不可用。</p>}
        </div>
        <article><span>Golden Cases</span><strong>{goldenCases.length}</strong></article>
        <article><span>Shadow 对比</span><strong>{shadowComparisons.length}</strong></article>
        <article><span>Readiness</span><strong>{shadowReadiness?.ready ? 'ready' : 'blocked'}</strong></article>
      </section>

      <section className="panel">
        <h2>Skill 使用漏斗（最近 100 条）</h2>
        {Object.entries(funnel).map(([stage, count]) => (
          <div className="feedback-bar" key={stage}>
            <span>{stage}</span>
            <i style={{ width: `${count / maxFunnel * 100}%` }} />
            <b>{count}</b>
          </div>
        ))}
        <p style={{ color: 'var(--text-dim)', fontSize: 12 }}>
          manifest.resolved = 会话分发的 Skill；invoked = 模型实际执行 botmux skill show/read 拉取内容；completed/failed = 命令退出状态。
        </p>
      </section>

      <section className="panel">
        <h2>Knowledge / Memory Review</h2>
        <div className="feedback-deliveries">
          {knowledge.map(item => <div key={item.knowledgeId}><code>{item.targetLayer}</code><span>{item.title}</span><span>{item.confidence} · {item.freshness}</span><b>{item.state}{' '}
            {item.state === 'approved' && item.targetLayer !== 'reviewed-only' && item.freshness === 'fresh' && <button onClick={() => void createExportJob(item)}>Stage Export</button>}
          </b></div>)}
          {memory.map(item => <div key={item.memoryId}><code>{item.scope}</code><span>{item.subject} · {item.claimKey}</span><span>{item.confidence}</span><b>{item.state}{' '}
            {(item.state === 'proposed' || item.state === 'conflicted' || item.state === 'stale' || item.state === 'expired') && <button onClick={() => void changeMemoryState(item, 'active', 'review_approved')}>Approve</button>}{' '}
            {item.state === 'proposed' && <button onClick={() => void changeMemoryState(item, 'revoked', 'review_rejected')}>Reject</button>}{' '}
            {(item.state === 'active' || item.state === 'conflicted' || item.state === 'stale' || item.state === 'shadowed') && <button onClick={() => void changeMemoryState(item, 'revoked', 'review_revoked')}>Revoke</button>}{' '}
            {item.state === 'active' && <button onClick={() => void changeMemoryState(item, 'conflicted', 'review_conflict')}>Conflict</button>}
          </b></div>)}
          {policyDecisions.map(item => <div key={item.decisionId}><code>policy</code><span>{item.evidence.claimKey ?? item.sourceEventId} · {item.evidence.subject ?? '—'}</span><span>{item.reasonCodes.join(', ')}</span><b>{item.disposition}</b></div>)}
          {knowledge.length + memory.length + policyDecisions.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无待审核知识或记忆。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>Knowledge → Memory Import</h2>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 8, marginBottom: 10 }}>
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
        <p style={{ color: 'var(--text-dim)', fontSize: 12 }}>扫描只读取 approved/fresh/observed/non-sensitive knowledge_items 和显式列出的 allowlist 内 Markdown；执行需确认，不启用外部 transport。</p>
        <div className="feedback-deliveries">
          {importJobs.map(job => <div key={job.jobId}>
            <code>{job.state}</code>
            <span>{job.jobId}</span>
            <span>source {job.sourceCount} · eligible {job.eligibleCount} · imported {job.importedCount} · deduped {job.dedupedCount} · conflict {job.conflictCount} · skipped {job.skippedCount}</span>
            <b>{job.outboxEnqueuedCount} outbox {job.state === 'preview' || job.state === 'review_pending' || job.state === 'partial' ? <button onClick={() => void executeImport(job)}>Execute</button> : null}</b>
          </div>)}
          {importJobs.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无导入任务。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>Golden Set / Pi Shadow Quality</h2>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 10 }}>
          <input value={goldenForm.title} onChange={e => setGoldenForm({ ...goldenForm, title: e.target.value })} placeholder="Golden case title" />
          <input value={goldenForm.queryRedacted} onChange={e => setGoldenForm({ ...goldenForm, queryRedacted: e.target.value })} placeholder="Redacted query" />
          <input value={goldenForm.claimKey} onChange={e => setGoldenForm({ ...goldenForm, claimKey: e.target.value })} placeholder="Expected claim key" />
          <input value={goldenForm.claimTextHash} onChange={e => setGoldenForm({ ...goldenForm, claimTextHash: e.target.value })} placeholder="sha256:..." />
          <button disabled={!goldenForm.title.trim() || !goldenForm.queryRedacted.trim() || !goldenForm.claimKey.trim()} onClick={() => void createGoldenCase()}>保存 Golden</button>
          <button onClick={() => void refreshReadiness()}>生成 Readiness</button>
        </div>
        <p style={{ color: 'var(--text-dim)', fontSize: 12 }}>Golden set 只接受显式 review/redacted 输入；Shadow quality 只读取本地已存摘要，默认关闭且不调用 Pi/LLM/网络。</p>
        <div className="feedback-deliveries">
          {goldenCases.map(item => <div key={`${item.caseId}@${item.revision}`}><code>{item.state}</code><span>{item.title}</span><span>{item.caseId}@{item.revision} · {item.expectedClaims.length} claims</span><b><button onClick={() => void compareGoldenCase(item)}>Compare</button></b></div>)}
          {shadowComparisons.map(item => <div key={item.comparisonId}><code>compare</code><span>{item.caseId}@{item.revision}</span><span>overlap {item.metrics.claimOverlap} · rules {item.metrics.rulesUnique} · pi {item.metrics.piUnique} · evidence {Math.round(item.metrics.evidenceCoverage * 100)}%</span><b>FP {item.metrics.falsePositiveLabels} / FN {item.metrics.falseNegativeLabels} <button onClick={() => void labelComparison(item, 'false_positive')}>FP</button> <button onClick={() => void labelComparison(item, 'false_negative')}>FN</button></b></div>)}
          {shadowReadiness && <div><code>readiness</code><span>{shadowReadiness.reasonCodes.join(', ') || 'thresholds_passed'}</span><span>{JSON.stringify(shadowReadiness.metrics ?? {})}</span><b>{shadowReadiness.ready ? 'ready' : 'not ready'}</b></div>}
          {goldenCases.length + shadowComparisons.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无 Golden case 或 Shadow comparison。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>Knowledge Export Staging</h2>
        <div className="feedback-deliveries">
          {exportJobs.map(job => <div key={job.jobId}><code>{job.plan.targetLayer}</code><span>{job.plan.destination.relativePath}</span><span>{job.plan.diff.status} · {job.plan.reasonCodes.join(', ') || 'ready'}</span><b>{job.state}{' '}
            {job.state === 'review_pending' && <button onClick={() => void reviewExportJob(job, 'approved')}>Approve</button>}{' '}
            {job.state === 'review_pending' && <button onClick={() => void reviewExportJob(job, 'rejected')}>Reject</button>}
          </b></div>)}
          {exportJobs.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无导出审核单。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>Trace / Eval / Evolution</h2>
        <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
          <input value={traceType} onChange={e => setTraceType(e.target.value)} placeholder="类型，如 turn" />
          <input value={traceId} onChange={e => setTraceId(e.target.value)} placeholder="ID" />
          <button onClick={() => void loadTrace()}>查询 Trace</button>
        </div>
        <div className="feedback-deliveries">
          {traceEdges.map(edge => <div key={edge.edgeId}><code>{edge.edgeType}</code><span>{edge.fromType}:{edge.fromId}</span><span>{edge.toType}:{edge.toId}</span><b>edge</b></div>)}
          {evalRuns.map(run => <div key={run.evalRunId}><code>{run.evaluatorName}</code><span>{run.targetType}:{run.targetId}</span><span>pass {run.passCount} · warn {run.warnCount}</span><b>fail {run.failCount}</b></div>)}
          {proposals.map(proposal => <div key={proposal.proposalId}><code>{proposal.proposalType}</code><span>{proposal.summary}</span><span>{proposal.targetRef}</span><b>{proposal.approvalGrade} · {proposal.state}</b></div>)}
          {traceEdges.length + evalRuns.length + proposals.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无 Trace、Eval 或 Evolution 数据。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>Memory Settings（真实 Transport 默认禁用）</h2>
        <h3>Bot Pipeline Profile</h3>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 10 }}>
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
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 10 }}>
          <select value={providerForm.providerId} onChange={e => setProviderForm({ ...providerForm, providerId: e.target.value as ProviderConfig['providerId'] })}>
            <option value="mem0">Mem0</option><option value="hindsight">Hindsight</option><option value="openviking">OpenViking</option>
          </select>
          <input value={providerForm.endpoint} onChange={e => setProviderForm({ ...providerForm, endpoint: e.target.value })} placeholder="https://endpoint" />
          <input value={providerForm.credentialRef} onChange={e => setProviderForm({ ...providerForm, credentialRef: e.target.value })} placeholder="env:API_KEY" />
          <input type="number" min="100" max="30000" value={providerForm.timeoutMs} onChange={e => setProviderForm({ ...providerForm, timeoutMs: Number(e.target.value) })} title="Timeout ms" />
          <label><input type="checkbox" checked={providerForm.enabled} onChange={e => setProviderForm({ ...providerForm, enabled: e.target.checked })} /> Enabled</label>
          <button disabled={!providerForm.endpoint.trim()} onClick={() => void saveProvider()}>保存连接配置</button>
        </div>
        <p style={{ color: 'var(--text-dim)', fontSize: 12 }}>只保存 endpoint 与 credential reference；不保存密钥，不发网络请求，realTransportEnabled 固定为 false。</p>
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
      </section>

      <section className="panel">
        <h2>Backend Runtime / Migration</h2>
        <div className="feedback-deliveries">
          {backendOutbox.map(item => <div key={item.outboxId}><code>{item.providerId}</code><span>{item.operation} · {item.memoryId}</span><span>attempt {item.attempts}{item.lastError ? ` · ${item.lastError}` : ''}</span><b>{item.status}</b></div>)}
          {backendMigrations.map(item => <div key={item.migrationId}><code>migration</code><span>{item.botAppId} · {item.migrationId}</span><span>checkpoint {item.checkpoint ?? '—'} · {JSON.stringify(item.stats)}</span><b>{item.state}</b></div>)}
          {backendOutbox.length + backendMigrations.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无 backend outbox 或迁移任务。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>Providers / Distillation / Retrieval Shadow</h2>
        <div className="feedback-deliveries">
          {providers.map(provider => <div key={`${provider.providerId}@${provider.version}`}><code>{provider.kind}</code><span>{provider.providerId}</span><span>{provider.descriptor?.execution ?? '—'} · {(provider.descriptor?.capabilities ?? []).slice(0, 3).join(', ')}</span><b>v{provider.version} · {provider.status}</b></div>)}
          {jobs.map(job => <div key={job.jobId}><code>distill</code><span>{job.profileId} · {job.botAppId}</span><span>attempt {job.attempts}</span><b>{job.state}</b></div>)}
          {retrievals.map(run => <div key={run.retrievalRunId}><code>retrieve</code><span>{run.botAppId} · {run.mode}</span><span>{run.candidateCount} → {run.eligibleCount}</span><b>{run.latencyMs}ms</b></div>)}
          {injections.map(item => <div key={item.snapshotId}><code>inject</code><span>{item.botAppId} · {item.mode}</span><span>{item.itemIds.length} items · {item.promptBytes} bytes</span><b>{item.disposition}</b></div>)}
          {providers.length + jobs.length + retrievals.length + injections.length === 0 && <p style={{ color: 'var(--text-dim)' }}>自动蒸馏和检索影子尚未启用。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>Central Sink / Sync（默认禁用）</h2>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 10 }}>
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
        <p style={{ color: 'var(--text-dim)', fontSize: 12 }}>
          Runtime 需要 BOTMUX_KM_CENTRAL_SINK_ENABLED=true；只执行 mock:// 与 inmemory://。HTTPS/HTTP 仅可保存为关闭配置，不能启用真实传输。
        </p>
        {centralSink && <p style={{ color: 'var(--text-dim)', fontSize: 12 }}>
          protocol v{centralSink.protocol.envelopeVersion} · {centralSink.protocol.signing} · {centralSink.protocol.credentialMode} · rollback: local-disable-only
        </p>}
        <div className="feedback-deliveries">
          {syncStatus.map(sink => <div key={sink.sinkId}><code>{sink.sinkId}</code><span>{sink.endpointRef}</span><span>pending {sink.pending} · quarantine {sink.quarantined} · seq {sink.lastLocalSeq}</span><b>{sink.enabled ? sink.status : 'disabled'}{' '}
            <button onClick={() => void runSinkDrill(sink.sinkId, 'status')}>Status</button>{' '}
            <button onClick={() => void runSinkDrill(sink.sinkId, 'replay')}>Replay</button>{' '}
            <button onClick={() => void runSinkDrill(sink.sinkId, 'partial-ack')}>Partial Ack</button>{' '}
            <button onClick={() => void runSinkDrill(sink.sinkId, 'conflict')}>Conflict</button>
          </b></div>)}
          {syncStatus.length === 0 && <p style={{ color: 'var(--text-dim)' }}>未配置 Sink；本地能力不受影响。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>事件列表</h2>
        <div style={{ marginBottom: 8 }}>
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
          {events.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无事件。开启 BOTMUX_KM_OBSERVATION_ENABLED=true 并使用一段时间后刷新。</p>}
        </div>
      </section>
    </div>
  );
}

export function renderKmPage(root: HTMLElement): PageDisposer {
  return mountReactPage(root, <KmPage />);
}
