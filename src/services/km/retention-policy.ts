import { createHash } from 'node:crypto';

export const KM_RETENTION_POLICY_VERSION = 'km-retention-tiered-v1';

export const KM_RETENTION_DOMAINS = [
  'observations',
  'knowledge',
  'memory',
  'retrieval',
  'injection',
  'trace',
  'eval',
  'evolution',
  'distillation',
  'sync-outbox',
  'backend-outbox',
  'quarantine-evidence',
] as const;

export type KmRetentionDomain = typeof KM_RETENTION_DOMAINS[number];
export type KmRetentionTier = 'hot' | 'warm' | 'cold' | 'archive';
export type KmSloState = 'ok' | 'warn' | 'critical';

export interface KmRetentionDomainPolicy {
  domain: KmRetentionDomain;
  tier: KmRetentionTier;
  retentionDays: number;
  protects: string[];
}

export interface KmRetentionEligibleSample {
  id: string;
  ageDays: number;
  createdAt: string;
  reason: string;
}

export interface KmRetentionDomainPreview {
  domain: KmRetentionDomain;
  table: string;
  tier: KmRetentionTier;
  retentionDays: number;
  cutoff: string;
  totalCount: number;
  eligibleCount: number;
  protectedCount: number;
  oldestRecordAgeDays: number;
  oldestEligibleAgeDays: number;
  protectedReasonCounts: Record<string, number>;
  eligibleSamples: KmRetentionEligibleSample[];
}

export interface KmRetentionSloMetric {
  key: string;
  state: KmSloState;
  value: number;
  warnAt: number;
  criticalAt: number;
  unit: string;
}

export interface KmRetentionPlan {
  policyVersion: string;
  generatedAt: string;
  dryRunOnly: true;
  destructiveActionsAvailable: false;
  domains: KmRetentionDomainPreview[];
  db: {
    dbBytes: number;
    walBytes: number;
    totalBytes: number;
  };
  operational: {
    backlog: {
      distillationQueued: number;
      distillationRetryWait: number;
      distillationOldestAgeMs: number;
      distillationClaimed: number;
      syncPending: number;
      syncInflight: number;
      syncFailed: number;
      backendPending: number;
      backendInflight: number;
      backendFailed: number;
    };
    quarantine: {
      observations: number;
      sync: number;
      backend: number;
    };
    retry: {
      distillationRetryWait: number;
      syncFailed: number;
      backendFailed: number;
    };
    providerQuality: {
      configuredProviders: number;
      unavailableProviders: number;
      quarantinedBackendOutbox: number;
    };
    retrievalQuality: Record<string, number>;
  };
  slo: KmRetentionSloMetric[];
  planHash: string;
}

export interface KmRetentionReportSummary {
  reportId: string;
  policyVersion: string;
  holderId: string;
  startedAt: string;
  completedAt: string;
  reportHash: string;
  totalEligible: number;
  worstSloState: KmSloState;
}

export interface KmRetentionRuntimeStatus {
  enabled: boolean;
  leaseName: string;
  latestPlan: KmRetentionPlan;
  reports: KmRetentionReportSummary[];
  trend: Array<{
    reportId: string;
    completedAt: string;
    totalEligible: number;
    worstSloState: KmSloState;
    dbBytes: number;
    walBytes: number;
  }>;
}

export const DEFAULT_RETENTION_POLICIES: Record<KmRetentionDomain, KmRetentionDomainPolicy> = {
  observations: {
    domain: 'observations',
    tier: 'archive',
    retentionDays: 365,
    protects: ['legal_hold', 'referenced_evidence', 'quarantine_evidence'],
  },
  knowledge: {
    domain: 'knowledge',
    tier: 'archive',
    retentionDays: 540,
    protects: ['approved', 'exported', 'review_pending', 'candidate', 'conflict', 'legal_hold'],
  },
  memory: {
    domain: 'memory',
    tier: 'archive',
    retentionDays: 540,
    protects: ['active', 'proposed', 'conflicted', 'legal_hold', 'source_evidence'],
  },
  retrieval: {
    domain: 'retrieval',
    tier: 'warm',
    retentionDays: 90,
    protects: ['referenced_by_injection', 'referenced_by_eval', 'legal_hold'],
  },
  injection: {
    domain: 'injection',
    tier: 'warm',
    retentionDays: 90,
    protects: ['referenced_by_eval', 'injected_live', 'legal_hold'],
  },
  trace: {
    domain: 'trace',
    tier: 'archive',
    retentionDays: 540,
    protects: ['causal_evidence', 'approved', 'conflicted', 'synced', 'legal_hold'],
  },
  eval: {
    domain: 'eval',
    tier: 'cold',
    retentionDays: 180,
    protects: ['accepted', 'running', 'queued', 'legal_hold'],
  },
  evolution: {
    domain: 'evolution',
    tier: 'cold',
    retentionDays: 180,
    protects: ['review_pending', 'approved', 'executing', 'applied', 'verified', 'legal_hold'],
  },
  distillation: {
    domain: 'distillation',
    tier: 'warm',
    retentionDays: 120,
    protects: ['queued', 'running', 'retry_wait', 'quarantined', 'source_evidence', 'legal_hold'],
  },
  'sync-outbox': {
    domain: 'sync-outbox',
    tier: 'warm',
    retentionDays: 30,
    protects: ['pending', 'inflight', 'failed', 'quarantined', 'legal_hold'],
  },
  'backend-outbox': {
    domain: 'backend-outbox',
    tier: 'warm',
    retentionDays: 30,
    protects: ['pending', 'inflight', 'failed', 'quarantined', 'legal_hold'],
  },
  'quarantine-evidence': {
    domain: 'quarantine-evidence',
    tier: 'archive',
    retentionDays: 3650,
    protects: ['all_quarantine_evidence', 'legal_hold'],
  },
};

export function ageDays(nowMs: number, iso: string | undefined): number {
  const time = iso ? Date.parse(iso) : Number.NaN;
  if (!Number.isFinite(time)) return 0;
  return Math.max(0, Math.floor((nowMs - time) / 86_400_000));
}

function ordered(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(ordered);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value as Record<string, unknown>)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, child]) => [key, ordered(child)]));
  }
  return value;
}

export function stableHash(value: unknown): string {
  return `sha256:${createHash('sha256').update(JSON.stringify(ordered(value))).digest('hex')}`;
}

export function finalizeRetentionPlan(plan: Omit<KmRetentionPlan, 'planHash'>): KmRetentionPlan {
  return { ...plan, planHash: stableHash(plan) };
}

function stateForHigherIsWorse(value: number, warnAt: number, criticalAt: number): KmSloState {
  if (value >= criticalAt) return 'critical';
  if (value >= warnAt) return 'warn';
  return 'ok';
}

export function worstSloState(metrics: KmRetentionSloMetric[]): KmSloState {
  if (metrics.some(metric => metric.state === 'critical')) return 'critical';
  if (metrics.some(metric => metric.state === 'warn')) return 'warn';
  return 'ok';
}

export function totalEligible(plan: Pick<KmRetentionPlan, 'domains'>): number {
  return plan.domains.reduce((sum, domain) => sum + domain.eligibleCount, 0);
}

export function buildRetentionSloMetrics(input: {
  dbBytes: number;
  walBytes: number;
  distillationOldestAgeMs: number;
  syncPending: number;
  syncInflight: number;
  syncFailed: number;
  backendPending: number;
  backendInflight: number;
  backendFailed: number;
  observationQuarantine: number;
  syncQuarantine: number;
  backendQuarantine: number;
  unavailableProviders: number;
  retrievalRuns: number;
  retrievalZeroHits: number;
  retrievalAvgLatencyMs: number;
}): KmRetentionSloMetric[] {
  const zeroHitRatio = input.retrievalRuns > 0 ? input.retrievalZeroHits / input.retrievalRuns : 0;
  return [
    {
      key: 'km.db.total_bytes',
      value: input.dbBytes + input.walBytes,
      warnAt: 512 * 1024 * 1024,
      criticalAt: 1024 * 1024 * 1024,
      unit: 'bytes',
      state: stateForHigherIsWorse(input.dbBytes + input.walBytes, 512 * 1024 * 1024, 1024 * 1024 * 1024),
    },
    {
      key: 'km.db.wal_bytes',
      value: input.walBytes,
      warnAt: 128 * 1024 * 1024,
      criticalAt: 512 * 1024 * 1024,
      unit: 'bytes',
      state: stateForHigherIsWorse(input.walBytes, 128 * 1024 * 1024, 512 * 1024 * 1024),
    },
    {
      key: 'km.distillation.oldest_backlog_age',
      value: input.distillationOldestAgeMs,
      warnAt: 30 * 60_000,
      criticalAt: 6 * 60 * 60_000,
      unit: 'ms',
      state: stateForHigherIsWorse(input.distillationOldestAgeMs, 30 * 60_000, 6 * 60 * 60_000),
    },
    {
      key: 'km.outbox.pending_or_inflight',
      value: input.syncPending + input.syncInflight + input.backendPending + input.backendInflight,
      warnAt: 100,
      criticalAt: 1000,
      unit: 'rows',
      state: stateForHigherIsWorse(input.syncPending + input.syncInflight + input.backendPending + input.backendInflight, 100, 1000),
    },
    {
      key: 'km.retry.failed_rows',
      value: input.syncFailed + input.backendFailed,
      warnAt: 10,
      criticalAt: 100,
      unit: 'rows',
      state: stateForHigherIsWorse(input.syncFailed + input.backendFailed, 10, 100),
    },
    {
      key: 'km.quarantine.rows',
      value: input.observationQuarantine + input.syncQuarantine + input.backendQuarantine,
      warnAt: 1,
      criticalAt: 25,
      unit: 'rows',
      state: stateForHigherIsWorse(input.observationQuarantine + input.syncQuarantine + input.backendQuarantine, 1, 25),
    },
    {
      key: 'km.provider.unavailable',
      value: input.unavailableProviders,
      warnAt: 1,
      criticalAt: 3,
      unit: 'providers',
      state: stateForHigherIsWorse(input.unavailableProviders, 1, 3),
    },
    {
      key: 'km.retrieval.zero_hit_ratio',
      value: Number(zeroHitRatio.toFixed(4)),
      warnAt: 0.25,
      criticalAt: 0.5,
      unit: 'ratio',
      state: stateForHigherIsWorse(zeroHitRatio, 0.25, 0.5),
    },
    {
      key: 'km.retrieval.avg_latency',
      value: input.retrievalAvgLatencyMs,
      warnAt: 500,
      criticalAt: 2000,
      unit: 'ms',
      state: stateForHigherIsWorse(input.retrievalAvgLatencyMs, 500, 2000),
    },
  ];
}
