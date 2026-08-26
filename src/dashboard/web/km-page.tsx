import React, { useEffect, useState } from 'react';
import { mountReactPage, type PageDisposer } from './react-mount.js';

type Health = {
  enabled: boolean;
  schemaVersion: number;
  pragmas: { journalMode: string; foreignKeys: number; busyTimeout: number };
  counts: { observations: number; quarantined: number };
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

  const load = async (type?: string) => {
    try {
      setError('');
      const [h, list] = await Promise.all([
        getJson<Health>('/api/km/health'),
        getJson<{ items: ObservationEvent[] }>(`/api/km/observations?limit=100${type ? `&type=${encodeURIComponent(type)}` : ''}`),
      ]);
      setHealth(h);
      setEvents(list.items);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => { void load(); }, []);

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
