import type React from 'react';
import type { KmDashboardModel, KmDistributionSlice, KmMetricPoint, KmOpsTabId, KmRankingItem, KmTrendPoint } from './km-dashboard-model.js';

export type KmOpsTab = {
  id: KmOpsTabId;
  label: string;
  description: string;
};

export const KM_OPS_TABS: KmOpsTab[] = [
  { id: 'overview', label: '总览', description: '先看规模、健康、趋势和热度。' },
  { id: 'knowledge', label: '知识', description: '审核知识候选、导入与导出 staging。' },
  { id: 'memory', label: '记忆', description: '查看 memory item、策略命中和外部后端。' },
  { id: 'quality', label: '质量', description: 'Golden set、Shadow 对比、Eval 与召回质量。' },
  { id: 'configuration', label: '配置', description: 'Pipeline profile、provider 与 central sink。' },
  { id: 'production', label: '生产闸门', description: '高风险意图与显式审批。' },
  { id: 'audit', label: '审计', description: '事件、trace、审计日志与迁移。' },
];

export function KmPageFrame(props: {
  activeTab: KmOpsTabId;
  onTabChange(tab: KmOpsTabId): void;
  model: KmDashboardModel;
  loading: boolean;
  error: string;
  notice: string;
  children: React.ReactNode;
}): React.JSX.Element {
  const activeIndex = KM_OPS_TABS.findIndex(tab => tab.id === props.activeTab);
  const moveTab = (direction: -1 | 1) => {
    const next = (activeIndex + direction + KM_OPS_TABS.length) % KM_OPS_TABS.length;
    props.onTabChange(KM_OPS_TABS[next].id);
  };

  return (
    <div className="page km-page km-ops-page">
      <header className="km-hero">
        <div className="km-hero-mark" aria-hidden="true">
          <svg viewBox="0 0 28 28">
            <path d="M6 6.5h6.8c2 0 3.7 1.6 3.7 3.7v11.3H9.7A3.7 3.7 0 0 1 6 17.8z" />
            <path d="M22 6.5h-6.8c-2 0-3.7 1.6-3.7 3.7v11.3h6.8c2 0 3.7-1.6 3.7-3.7z" />
          </svg>
        </div>
        <div className="km-hero-copy">
          <p className="km-eyebrow">KM OPERATIONS</p>
          <h1>知识健康度运营看板</h1>
          <p>{props.model.summary}</p>
        </div>
        <div className="km-hero-meta" aria-label="看板元信息">
          <span>UPDATED</span>
          <b>{formatDateTime(props.model.generatedAt)}</b>
          <small>{props.model.source === 'metrics-api' ? 'metrics api' : 'metrics fallback'} · contract v1</small>
        </div>
      </header>

      {props.error ? <KmStateBanner tone="error" title="加载失败" message={props.error} /> : null}
      {!props.error && props.loading ? <KmStateBanner tone="loading" title="正在读取 KM 数据" message="已保留页面结构，数据返回后会自动填充。" /> : null}
      {props.notice ? <KmStateBanner tone="success" title="操作已记录" message={props.notice} /> : null}

      <nav
        className="km-tabs"
        aria-label="KM dashboard sections"
        role="tablist"
        onKeyDown={event => {
          if (event.key === 'ArrowRight') {
            event.preventDefault();
            moveTab(1);
          } else if (event.key === 'ArrowLeft') {
            event.preventDefault();
            moveTab(-1);
          }
        }}
      >
        {KM_OPS_TABS.map(tab => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={tab.id === props.activeTab}
            className={tab.id === props.activeTab ? 'active' : undefined}
            onClick={() => props.onTabChange(tab.id)}
            title={tab.description}
          >
            <span>{tab.label}</span>
            <small>{tab.description}</small>
          </button>
        ))}
      </nav>

      <section className="km-tab-panel" role="tabpanel" aria-label={KM_OPS_TABS.find(tab => tab.id === props.activeTab)?.label}>
        {props.children}
      </section>
    </div>
  );
}

export function KmOverview({ model }: { model: KmDashboardModel }): React.JSX.Element {
  return (
    <div className="km-overview">
      <section className="km-kpi-grid" aria-label="关键指标">
        {model.kpis.map(kpi => <KmKpiCard key={kpi.key} kpi={kpi} />)}
      </section>

      <section className="km-overview-grid">
        <KmDonutPanel title="知识分层结构" icon="bars" data={model.layerDistribution} emptyText="暂无可分层知识。" />
        <KmTrendPanel data={model.trend} />
      </section>

      <section className="km-overview-grid">
        <KmBarsPanel title="分类分布" data={model.categoryDistribution} emptyText="暂无分类或 scope 数据。" />
        <KmDonutPanel title="状态分布" icon="pulse" data={model.stateDistribution} emptyText="暂无状态数据。" />
      </section>

      <section className="km-overview-grid km-rank-grid">
        <KmRanking title="召回热度排行" items={model.hotSkills} emptyText="暂无 skill / 事件热度。" />
        <KmRanking title="阅读热度排行" items={model.hotKnowledge} emptyText="暂无可展示知识条目。" />
      </section>

      <section className="km-risk-strip" aria-label="运行风险提示">
        {model.riskBadges.map(badge => (
          <article key={badge.label} className={`km-risk-badge km-risk-badge--${badge.tone}`}>
            <b>{badge.label}</b>
            <span>{badge.detail}</span>
          </article>
        ))}
      </section>
    </div>
  );
}

export function KmSection(props: {
  title: string;
  description: string;
  badge?: string;
  risk?: 'low' | 'medium' | 'high';
  children: React.ReactNode;
}): React.JSX.Element {
  return (
    <section className={`km-section ${props.risk ? `km-section--${props.risk}` : ''}`}>
      <header className="km-section-head">
        <div>
          <h2>{props.title}</h2>
          <p>{props.description}</p>
        </div>
        {props.badge ? <span className={`km-risk-pill km-risk-pill--${props.risk ?? 'low'}`}>{props.badge}</span> : null}
      </header>
      {props.children}
    </section>
  );
}

export function KmInlineHelp({ children }: { children: React.ReactNode }): React.JSX.Element {
  return <p className="km-inline-help">{children}</p>;
}

export function KmEmptyState({ title, message }: { title: string; message: string }): React.JSX.Element {
  return (
    <div className="km-empty" role="status">
      <b>{title}</b>
      <span>{message}</span>
    </div>
  );
}

function KmKpiCard({ kpi }: { kpi: KmMetricPoint }): React.JSX.Element {
  return (
    <article className={`km-kpi-card km-kpi-card--${kpi.tone}`} title={kpi.tooltip}>
      <div className="km-kpi-label">
        <KmMiniIcon kind={kpi.key} />
        <span>{kpi.label}</span>
      </div>
      <strong>
        {formatNumber(kpi.value)}
        {kpi.unit ? <small>{kpi.unit}</small> : null}
      </strong>
      <p>{kpi.helper}</p>
    </article>
  );
}

function KmDonutPanel({ title, icon, data, emptyText }: { title: string; icon: 'bars' | 'pulse'; data: KmDistributionSlice[]; emptyText: string }): React.JSX.Element {
  const total = data.reduce((sum, item) => sum + item.value, 0);
  return (
    <article className="km-chart-panel">
      <KmPanelTitle icon={icon} title={title} />
      {total > 0 ? (
        <div className="km-donut-wrap">
          <svg className="km-donut" viewBox="0 0 160 160" aria-label={`${title} 环形图`}>
            <circle cx="80" cy="80" r="48" className="km-donut-track" />
            {donutSegments(data).map(segment => (
              <circle
                key={segment.key}
                cx="80"
                cy="80"
                r="48"
                className="km-donut-segment"
                stroke={segment.color}
                strokeDasharray={`${segment.length} ${segment.gap}`}
                strokeDashoffset={segment.offset}
              />
            ))}
          </svg>
          <div className="km-legend">
            {data.slice(0, 5).map(item => (
              <span key={item.key}>
                <i style={{ background: item.color }} />
                {item.label} {item.percent}%
              </span>
            ))}
          </div>
        </div>
      ) : <KmEmptyState title="暂无数据" message={emptyText} />}
    </article>
  );
}

function KmTrendPanel({ data }: { data: KmTrendPoint[] }): React.JSX.Element {
  const max = Math.max(1, ...data.flatMap(item => [item.observations, item.retrievals, item.gates]));
  return (
    <article className="km-chart-panel">
      <KmPanelTitle icon="pulse" title="运营趋势" />
      <div className="km-trend-chart" aria-label="最近趋势柱状图">
        {data.map(point => (
          <div className="km-trend-day" key={point.label}>
            <div className="km-trend-bars">
              <i className="obs" style={{ height: `${Math.max(4, point.observations / max * 100)}%` }} title={`观测 ${point.observations}`} />
              <i className="ret" style={{ height: `${Math.max(4, point.retrievals / max * 100)}%` }} title={`召回 ${point.retrievals}`} />
              <i className="gate" style={{ height: `${Math.max(4, point.gates / max * 100)}%` }} title={`闸门 ${point.gates}`} />
            </div>
            <span>{point.label}</span>
          </div>
        ))}
      </div>
      <div className="km-chart-caption">
        <span><i className="obs" />观测</span>
        <span><i className="ret" />召回</span>
        <span><i className="gate" />闸门</span>
      </div>
    </article>
  );
}

function KmBarsPanel({ title, data, emptyText }: { title: string; data: KmDistributionSlice[]; emptyText: string }): React.JSX.Element {
  const max = Math.max(1, ...data.map(item => item.value));
  return (
    <article className="km-chart-panel">
      <KmPanelTitle icon="bars" title={title} />
      {data.length ? (
        <div className="km-bars">
          {data.slice(0, 7).map(item => (
            <div className="km-bar-row" key={item.key}>
              <span>{item.label}</span>
              <div><i style={{ width: `${Math.max(3, item.value / max * 100)}%`, background: item.color }} /></div>
              <b>{item.value}</b>
            </div>
          ))}
        </div>
      ) : <KmEmptyState title="暂无数据" message={emptyText} />}
    </article>
  );
}

function KmRanking({ title, items, emptyText }: { title: string; items: KmRankingItem[]; emptyText: string }): React.JSX.Element {
  const max = Math.max(1, ...items.map(item => item.value));
  return (
    <article className="km-ranking">
      <header>
        <KmPanelTitle icon={title.includes('阅读') ? 'book' : 'trend'} title={title} />
        <a href="#/km" aria-label={`${title} 查看全部`}>查看全部</a>
      </header>
      {items.length ? (
        <ol>
          {items.map((item, index) => (
            <li key={item.id}>
              <span className="km-rank-no">{index + 1}</span>
              <div>
                <b>{item.title}</b>
                <i style={{ width: `${Math.max(8, item.value / max * 100)}%` }} />
              </div>
              <strong>{item.value}</strong>
              <small>{item.meta}</small>
            </li>
          ))}
        </ol>
      ) : <KmEmptyState title="暂无排行" message={emptyText} />}
    </article>
  );
}

function KmPanelTitle({ icon, title }: { icon: 'bars' | 'pulse' | 'book' | 'trend'; title: string }): React.JSX.Element {
  return (
    <h2 className="km-panel-title">
      <KmMiniIcon kind={icon} />
      {title}
    </h2>
  );
}

function KmMiniIcon({ kind }: { kind: string }): React.JSX.Element {
  const path = (() => {
    if (kind === 'book' || kind === 'knowledge') return <path d="M4 5.5c2-1 4-1 6 .1v13c-2-1.1-4-1.1-6-.1zm6 .1c2-1.1 4-1.1 6-.1v13c-2-1-4-1-6 .1" />;
    if (kind === 'pulse' || kind === 'health') return <path d="M3 12h4l2-7 4 14 2-7h6" />;
    if (kind === 'trend' || kind === 'retrieval') return <><path d="M4 17 9 12l4 3 7-8" /><path d="M15 7h5v5" /></>;
    if (kind === 'audit') return <><path d="M5 4h10l3 3v12H5z" /><path d="M14 4v4h4M8 12h7M8 16h5" /></>;
    if (kind === 'memory') return <><rect x="5" y="6" width="14" height="12" rx="3" /><path d="M9 6V3M15 6V3M9 21v-3M15 21v-3M2 10h3M2 14h3M19 10h3M19 14h3" /></>;
    return <><path d="M4 19V8M10 19V4M16 19v-7M22 19H2" /></>;
  })();
  return <svg viewBox="0 0 24 24" aria-hidden="true">{path}</svg>;
}

function KmStateBanner({ tone, title, message }: { tone: 'loading' | 'error' | 'success'; title: string; message: string }): React.JSX.Element {
  return (
    <div className={`km-state-banner km-state-banner--${tone}`} role={tone === 'error' ? 'alert' : 'status'}>
      <b>{title}</b>
      <span>{message}</span>
    </div>
  );
}

function donutSegments(data: KmDistributionSlice[]): Array<KmDistributionSlice & { length: number; gap: number; offset: number }> {
  const circumference = 2 * Math.PI * 48;
  let consumed = 0;
  return data.map(item => {
    const length = circumference * item.percent / 100;
    const segment = { ...item, length, gap: circumference - length, offset: -consumed };
    consumed += length;
    return segment;
  });
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat('zh-CN', { maximumFractionDigits: value % 1 === 0 ? 0 : 1 }).format(value);
}

function formatDateTime(value: string): string {
  try {
    return new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).format(new Date(value));
  } catch {
    return value;
  }
}
