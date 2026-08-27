import React, { useEffect, useState } from 'react';
import { mountReactPage, type PageDisposer } from './react-mount.js';

type Health = {
  enabled: boolean;
  schemaVersion: number;
  pragmas: { journalMode: string; foreignKeys: number; busyTimeout: number };
  counts: { observations: number; quarantined: number; knowledge?: number; memory?: number };
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

  const load = async (type?: string) => {
    try {
      setError('');
      const [h, list, knowledgeList, memoryList, evalList, proposalList, syncList, providerList, jobList, retrievalList, injectionList] = await Promise.all([
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
      ]);
      setHealth(h);
      setEvents(list.items);
      setKnowledge(knowledgeList.items);
      setMemory(memoryList.items);
      setEvalRuns(evalList.items);
      setProposals(proposalList.items);
      setSyncStatus(syncList.items);
      setProviders(providerList.items); setJobs(jobList.items); setRetrievals(retrievalList.items); setInjections(injectionList.items);
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
          {knowledge.length + memory.length === 0 && <p style={{ color: 'var(--text-dim)' }}>暂无待审核知识或记忆。</p>}
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
