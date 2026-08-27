import { ObservationStore } from './observation-store.js';
import type { KmRetentionRuntimeStatus } from './retention-policy.js';
export type { KmRetentionRuntimeStatus } from './retention-policy.js';

const LEASE_NAME = 'km-retention-shadow';
const DEFAULT_INTERVAL_MS = 60 * 60_000;
const DEFAULT_LEASE_MS = 15 * 60_000;
const DEFAULT_SAMPLE_LIMIT = 10;

function envOn(name: string, env: NodeJS.ProcessEnv = process.env): boolean {
  return ['1', 'true', 'yes'].includes(env[name]?.trim().toLowerCase() ?? '');
}

function boundedInteger(value: string | undefined, fallback: number, min: number, max: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(Math.trunc(parsed), max));
}

export interface KmRetentionShadowRunReport {
  enabled: boolean;
  leaseName: string;
  leaseAcquired: boolean;
  holderId: string;
  report?: ReturnType<ObservationStore['recordKmRetentionShadowReport']>;
}

export function isKmRetentionShadowEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return envOn('BOTMUX_KM_RETENTION_SHADOW_ENABLED', env);
}

export async function kmRetentionRuntimeStatus(input: {
  dataDir: string;
  env?: NodeJS.ProcessEnv;
  now?: number;
}): Promise<KmRetentionRuntimeStatus> {
  const env = input.env ?? process.env;
  const store = await ObservationStore.open(input.dataDir);
  try {
    return store.kmRetentionStatus({
      enabled: isKmRetentionShadowEnabled(env),
      leaseName: LEASE_NAME,
      now: input.now,
    });
  } finally {
    store.close();
  }
}

export async function runKmRetentionShadowOnce(input: {
  dataDir: string;
  env?: NodeJS.ProcessEnv;
  holderId?: string;
  leaseMs?: number;
  now?: number;
  sampleLimit?: number;
}): Promise<KmRetentionShadowRunReport> {
  const env = input.env ?? process.env;
  const holderId = input.holderId ?? `pid:${process.pid}`;
  const leaseMs = input.leaseMs ?? boundedInteger(env.BOTMUX_KM_RETENTION_SHADOW_LEASE_MS, DEFAULT_LEASE_MS, 1_000, 60 * 60_000);
  const report: KmRetentionShadowRunReport = {
    enabled: isKmRetentionShadowEnabled(env),
    leaseName: LEASE_NAME,
    leaseAcquired: false,
    holderId,
  };
  if (!report.enabled) return report;

  const store = await ObservationStore.open(input.dataDir);
  try {
    report.leaseAcquired = store.acquireRuntimeLease({ leaseName: LEASE_NAME, holderId, ttlMs: leaseMs, now: input.now });
    if (!report.leaseAcquired) return report;
    report.report = store.recordKmRetentionShadowReport({
      holderId,
      now: input.now,
      sampleLimit: input.sampleLimit ?? boundedInteger(env.BOTMUX_KM_RETENTION_SAMPLE_LIMIT, DEFAULT_SAMPLE_LIMIT, 0, 50),
    });
    return report;
  } finally {
    if (report.leaseAcquired) {
      try {
        store.releaseRuntimeLease({ leaseName: LEASE_NAME, holderId });
      } catch {
        // A stale release must not fail the daemon's shadow scheduling loop.
      }
    }
    store.close();
  }
}

export function kmRetentionShadowIntervalMs(env: NodeJS.ProcessEnv = process.env): number {
  return boundedInteger(env.BOTMUX_KM_RETENTION_SHADOW_INTERVAL_MS, DEFAULT_INTERVAL_MS, 60_000, 24 * 60 * 60_000);
}

export function kmRetentionShadowStartupDelayMs(idx: number): number {
  return 7_000 + Math.max(0, idx) * 1_000;
}
