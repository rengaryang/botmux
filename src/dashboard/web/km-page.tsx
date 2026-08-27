import React, { useEffect, useState } from 'react';
import { mountReactPage, type PageDisposer } from './react-mount.js';
import { controlCsrfHeaders } from './control-csrf.js';

type Health = {
  enabled: boolean;
  schemaVersion: number;
  pragmas: { journalMode: string; foreignKeys: number; busyTimeout: number };
  counts: { observations: number; quarantined: number; knowledge?: number; memory?: number };
  backlog: { queued: number; retryWait: number; oldestAgeMs: number; claimed: number };
  capabilities: { requestedModes: string[]; effectiveModes: string[]; livePromptInjection: boolean; realMemoryTransport: boolean };
};

type KnowledgeItem = { knowledgeId: string; state: string; targetLayer: string; title: string; confidence: string; freshness: string };
type MemoryItem = { memoryId: string; state: string; scope: string; subject: string; claimKey: string; confidence: string };
type EvalRun = { evalRunId: string; evaluatorName: string; targetType: string; targetId: string; passCount: number; warnCount: number; failCount: number };
type EvolutionProposal = { proposalId: string; state: string; proposalType: string; targetRef: string; approvalGrade: string; summary: string };
type TraceEdge = { edgeId: string; fromType: string; fromId: string; toType: string; toId: string; edgeType: string };
type SyncStatus = { sinkId: string; endpointRef: string; enabled: boolean; status: string; pending: number; quarantined: number; lastLocalSeq: number; lastAckAt?: string };
type ProviderStatus = { providerId: string; kind: string; version: string; status: string };
type DistillationJob = { jobId: string; state: string; botAppId: string; profileId: string; attempts: number; lastError?: string };
type RetrievalAudit = { retrievalRunId: string; botAppId: string; mode: string; candidateCount: number; eligibleCount: number; latencyMs: number };
type InjectionSnapshot = { snapshotId: string; botAppId: string; mode: string; disposition: string; itemIds: string[]; promptBytes: number };
type PipelineProfile = { profile: { profileId: string; revision: number; botAppId: string; injectionMode: 'off' | 'shadow' | 'canary' | 'active'; memoryBackends: { writePolicy: string; primary: string; mirrors: string[] }; budgets: { promptTokens: number } }; state: string; requestedMode: string; effectiveMode: string; profileHash: string; createdAt: string };
type ProviderConfig = { providerId: 'mem0' | 'hindsight' | 'openviking'; endpoint: string; credentialRef: string; enabled: boolean; realTransportEnabled: false; timeoutMs: number; updatedAt: string };
type MemoryPolicyDecision = { decisionId: string; sourceEventId: string; memoryId?: string; policyVersion: string; disposition: string; reasonCodes: string[]; evidence: { claimKey?: string; subject?: string }; createdAt: string };
type ConfigAudit = { auditId: string; actorId: string; action: string; targetRef: string; createdAt: string };

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
  const [typeFilter, setTypeFilter] = useState<string>('');
  const [knowledge, setKnowledge] = useState<KnowledgeItem[]>([]);
  const [memory, setMemory] = useState<MemoryItem[]>([]);
  const [evalRuns, setEvalRuns] = useState<EvalRun[]>([]);
  const [proposals, setProposals] = useState<EvolutionProposal[]>([]);
  const [traceType, setTraceType] = useState('turn');
  const [traceId, setTraceId] = useState('');
  const [traceEdges, setTraceEdges] = useState<TraceEdge[]>([]);
  const [syncStatus, setSyncStatus] = useState<SyncStatus[]>([]);
  const [providers, setProviders] = useState<ProviderStatus[]>([]);
  const [jobs, setJobs] = useState<DistillationJob[]>([]);
  const [retrievals, setRetrievals] = useState<RetrievalAudit[]>([]);
  const [injections, setInjections] = useState<InjectionSnapshot[]>([]);
  const [profiles, setProfiles] = useState<PipelineProfile[]>([]);
  const [providerConfigs, setProviderConfigs] = useState<ProviderConfig[]>([]);
  const [policyDecisions, setPolicyDecisions] = useState<MemoryPolicyDecision[]>([]);
  const [configAudit, setConfigAudit] = useState<ConfigAudit[]>([]);
  const [profileForm, setProfileForm] = useState({ botAppId: '', profileId: '', revision: 1, injectionMode: 'shadow' as const,
    primary: 'sqlite', mirrors: 'mem0,hindsight,openviking', promptTokens: 1800 });
  const [providerForm, setProviderForm] = useState({ providerId: 'mem0' as ProviderConfig['providerId'], endpoint: '', credentialRef: 'env:MEM0_API_KEY', enabled: false, timeoutMs: 5000 });

  const load = async (type?: string) => {
    try {
      setError('');
      const [h, list, knowledgeList, memoryList, evalList, proposalList, syncList, providerList, jobList, retrievalList, injectionList, profileList, providerConfigList, policyDecisionList, configAuditList] = await Promise.all([
        getJson<Health>('/api/km/health'),
        getJson<{ items: ObservationEvent[] }>(`/api/km/observations?limit=100${type ? `&type=${encodeURIComponent(type)}` : ''}`),
        getJson<{ items: KnowledgeItem[] }>('/api/km/knowledge?limit=20'),
        getJson<{ items: MemoryItem[] }>('/api/km/memory?limit=20'),
        getJson<{ items: EvalRun[] }>('/api/km/eval/runs?limit=20'),
        getJson<{ items: EvolutionProposal[] }>('/api/km/evolution/proposals?limit=20'),
        getJson<{ items: SyncStatus[] }>('/api/km/sync/sinks'),
        getJson<{ items: ProviderStatus[] }>('/api/km/providers'),
        getJson<{ items: DistillationJob[] }>('/api/km/distillation/jobs?limit=20'),
        getJson<{ items: RetrievalAudit[] }>('/api/km/retrieval/runs?limit=20'),
        getJson<{ items: InjectionSnapshot[] }>('/api/km/injections?limit=20'),
        getJson<{ items: PipelineProfile[] }>('/api/km/profiles'),
        getJson<{ items: ProviderConfig[] }>('/api/km/provider-configs'),
        getJson<{ items: MemoryPolicyDecision[] }>('/api/km/memory-policy-decisions?limit=20'),
        getJson<{ items: ConfigAudit[] }>('/api/km/config-audit?limit=20'),
      ]);
      setHealth(h);
      setEvents(list.items);
      setKnowledge(knowledgeList.items);
      setMemory(memoryList.items);
      setEvalRuns(evalList.items);
      setProposals(proposalList.items);
      setSyncStatus(syncList.items);
      setProviders(providerList.items); setJobs(jobList.items); setRetrievals(retrievalList.items); setInjections(injectionList.items);
      setProfiles(profileList.items); setProviderConfigs(providerConfigList.items);
      setPolicyDecisions(policyDecisionList.items); setConfigAudit(configAuditList.items);
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
      await mutateJson('/api/km/profiles', 'POST', { profile, state: 'draft' }); await load();
    } catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const changeProfileState = async (entry: PipelineProfile, state: string) => {
    try { await mutateJson(`/api/km/profiles/${encodeURIComponent(entry.profile.profileId)}/${entry.profile.revision}/state`, 'PATCH', { state }); await load(); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const saveProvider = async () => {
    try { await mutateJson('/api/km/provider-configs', 'PUT', { ...providerForm, realTransportEnabled: false }); await load(); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const checkProvider = async (providerId: string) => {
    try { const result = await mutateJson<Record<string, unknown>>(`/api/km/provider-configs/${encodeURIComponent(providerId)}/health`, 'POST', {});
      setError(`配置检查：${providerId} = ${String(result.status)}（未发网络请求）`); }
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

      <section className="feedback-kpis">
        <article><span>观测事件</span><strong>{health?.counts.observations ?? '—'}</strong></article>
        <article><span>隔离冲突</span><strong>{health?.counts.quarantined ?? '—'}</strong></article>
        <article><span>知识候选</span><strong>{health?.counts.knowledge ?? '—'}</strong></article>
        <article><span>记忆条目</span><strong>{health?.counts.memory ?? '—'}</strong></article>
        <article><span>Schema 版本</span><strong>{health?.schemaVersion ?? '—'}</strong></article>
        <article><span>WAL 模式</span><strong>{health?.pragmas.journalMode ?? '—'}</strong></article>
        <article><span>采集状态</span><strong>{health?.enabled ? '已开启' : '未开启'}</strong></article>
        <article><span>蒸馏积压</span><strong>{(health?.backlog.queued ?? 0) + (health?.backlog.retryWait ?? 0)}</strong></article>
        <article><span>有效模式</span><strong>{health?.capabilities.effectiveModes.join('/') ?? '—'}</strong></article>
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
          {knowledge.map(item => <div key={item.knowledgeId}><code>{item.targetLayer}</code><span>{item.title}</span><span>{item.confidence} · {item.freshness}</span><b>{item.state}</b></div>)}
          {memory.map(item => <div key={item.memoryId}><code>{item.scope}</code><span>{item.subject} · {item.claimKey}</span><span>{item.confidence}</span><b>{item.state}</b></div>)}
          {policyDecisions.map(item => <div key={item.decisionId}><code>policy</code><span>{item.evidence.claimKey ?? item.sourceEventId} · {item.evidence.subject ?? '—'}</span><span>{item.reasonCodes.join(', ')}</span><b>{item.disposition}</b></div>)}
          {knowledge.length + memory.length + policyDecisions.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无待审核知识或记忆。</p>}
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
          {configAudit.map(item => <div key={item.auditId}><code>audit</code><span>{item.action} · {item.targetRef}</span><span>{item.actorId}</span><b>{item.createdAt}</b></div>)}
        </div>
      </section>

      <section className="panel">
        <h2>Providers / Distillation / Retrieval Shadow</h2>
        <div className="feedback-deliveries">
          {providers.map(provider => <div key={`${provider.providerId}@${provider.version}`}><code>{provider.kind}</code><span>{provider.providerId}</span><span>v{provider.version}</span><b>{provider.status}</b></div>)}
          {jobs.map(job => <div key={job.jobId}><code>distill</code><span>{job.profileId} · {job.botAppId}</span><span>attempt {job.attempts}</span><b>{job.state}</b></div>)}
          {retrievals.map(run => <div key={run.retrievalRunId}><code>retrieve</code><span>{run.botAppId} · {run.mode}</span><span>{run.candidateCount} → {run.eligibleCount}</span><b>{run.latencyMs}ms</b></div>)}
          {injections.map(item => <div key={item.snapshotId}><code>inject</code><span>{item.botAppId} · {item.mode}</span><span>{item.itemIds.length} items · {item.promptBytes} bytes</span><b>{item.disposition}</b></div>)}
          {providers.length + jobs.length + retrievals.length + injections.length === 0 && <p style={{ color: 'var(--text-dim)' }}>自动蒸馏和检索影子尚未启用。</p>}
        </div>
      </section>

      <section className="panel">
        <h2>Central Sink / Sync（默认禁用）</h2>
        <div className="feedback-deliveries">
          {syncStatus.map(sink => <div key={sink.sinkId}><code>{sink.sinkId}</code><span>{sink.endpointRef}</span><span>pending {sink.pending} · quarantine {sink.quarantined} · seq {sink.lastLocalSeq}</span><b>{sink.enabled ? sink.status : 'disabled'}</b></div>)}
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
