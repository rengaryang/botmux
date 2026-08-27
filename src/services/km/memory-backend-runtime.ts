import { createHash } from 'node:crypto';
import {
  createMemoryBackendProvider,
  InMemoryMemoryBackendTransport,
  type MemoryBackendFactoryResult,
} from './memory-backend-factory.js';
import type { MemoryBackendProvider } from './memory-backend-spi.js';
import { drainMemoryBackendOutbox, type MemoryBackendOutboxWorkerReport } from './memory-backend-outbox-worker.js';
import { ObservationStore, type MemoryBackendOutboxRow, type MemoryItem } from './observation-store.js';
import type { KmMemoryProviderConfig, KmPipelineProfile } from './provider-spi.js';

const DEFAULT_BATCH_LIMIT = 25;
const DEFAULT_LEASE_MS = 60_000;
const DEFAULT_MAX_ATTEMPTS = 5;
const DEFAULT_PROVIDER_TIMEOUT_MS = 5_000;

function envOn(name: string, env: NodeJS.ProcessEnv = process.env): boolean {
  return ['1', 'true', 'yes'].includes(env[name]?.trim().toLowerCase() ?? '');
}

function boundedInteger(value: string | undefined, fallback: number, min: number, max: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(Math.trunc(parsed), max));
}

export function isKmBackendWorkerEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return envOn('BOTMUX_KM_BACKEND_WORKER_ENABLED', env);
}

function isExplicitMockEndpoint(endpoint: string): boolean {
  try {
    const url = new URL(endpoint);
    if (url.protocol === 'mock:' || url.protocol === 'inmemory:') return true;
  } catch {
    return false;
  }
  return false;
}

function runtimeConfig(config: KmMemoryProviderConfig): KmMemoryProviderConfig {
  return {
    providerId: config.providerId,
    endpoint: config.endpoint,
    credentialRef: config.credentialRef,
    enabled: config.enabled,
    realTransportEnabled: config.realTransportEnabled,
    timeoutMs: config.timeoutMs,
  };
}

export interface KmBackendProviderRuntimeStatus {
  providerId: string;
  endpoint: string;
  enabled: boolean;
  status: MemoryBackendFactoryResult['status'] | 'unsafe_endpoint';
  reason?: string;
}

export interface KmBackendRuntimeStatus {
  enabled: boolean;
  leaseName: string;
  outbox: {
    total: number;
    pending: number;
    inflight: number;
    failed: number;
    delivered: number;
    quarantined: number;
    oldestPendingAgeMs: number;
  };
  providers: KmBackendProviderRuntimeStatus[];
}

export interface KmBackendWorkerRuntimeReport {
  enabled: boolean;
  leaseAcquired: boolean;
  holderId: string;
  providers: KmBackendProviderRuntimeStatus[];
  enqueue?: { scanned: number; enqueued: number; skipped: number; targetProviders: string[] };
  worker?: MemoryBackendOutboxWorkerReport;
}

function contentHash(memory: MemoryItem): string {
  const payload = {
    scope: memory.scope,
    subject: memory.subject,
    claimKey: memory.claimKey,
    claimText: memory.claimText,
    privacyClass: memory.privacyClass,
    ttlExpiresAt: memory.ttlExpiresAt ?? null,
  };
  return `sha256:${createHash('sha256').update(JSON.stringify(payload)).digest('hex')}`;
}

function eligibleMirrorMemory(memory: MemoryItem): boolean {
  return memory.state === 'active'
    && memory.syncPolicy === 'local-only'
    && memory.privacyClass === 'internal';
}

function targetProviderIds(profile: KmPipelineProfile): string[] {
  const ids = profile.memoryBackends.writePolicy === 'single'
    ? [profile.memoryBackends.primary]
    : profile.memoryBackends.writePolicy === 'shadow-write'
      ? profile.memoryBackends.mirrors
      : [profile.memoryBackends.primary, ...profile.memoryBackends.mirrors];
  return [...new Set(ids)].filter(id => id !== 'sqlite');
}

export function enqueueEligibleMemoryBackendMirrors(input: {
  store: ObservationStore;
  profile: KmPipelineProfile;
  limit?: number;
  now?: number;
}): { scanned: number; enqueued: number; skipped: number; targetProviders: string[] } {
  const targetProviders = targetProviderIds(input.profile);
  const rows = input.store.listMemory({ limit: input.limit ?? 500 });
  let enqueued = 0;
  let skipped = 0;
  for (const memory of rows) {
    if (!eligibleMirrorMemory(memory)) {
      skipped += 1;
      continue;
    }
    const hash = contentHash(memory);
    for (const providerId of targetProviders) {
      const result = input.store.enqueueMemoryBackendOperation({
        memoryId: memory.memoryId,
        providerId,
        operation: 'put',
        now: input.now,
        payload: {
          memoryId: memory.memoryId,
          scope: memory.scope,
          subject: memory.subject,
          claimKey: memory.claimKey,
          claimText: memory.claimText,
          privacyClass: memory.privacyClass,
          ttlExpiresAt: memory.ttlExpiresAt,
          sourceRefs: memory.sourceRefs,
          contentHash: hash,
        },
      });
      if (result.created) enqueued += 1;
    }
  }
  return { scanned: rows.length, enqueued, skipped, targetProviders };
}

function summarizeOutbox(rows: MemoryBackendOutboxRow[], now = Date.now()): KmBackendRuntimeStatus['outbox'] {
  const status = { total: rows.length, pending: 0, inflight: 0, failed: 0, delivered: 0, quarantined: 0, oldestPendingAgeMs: 0 };
  let oldest: number | undefined;
  for (const row of rows) {
    status[row.status] += 1;
    if ((row.status === 'pending' || row.status === 'failed') && row.nextAttemptAt <= now) {
      const created = Date.parse(row.createdAt);
      if (Number.isFinite(created)) oldest = oldest === undefined ? created : Math.min(oldest, created);
    }
  }
  status.oldestPendingAgeMs = oldest === undefined ? 0 : Math.max(0, now - oldest);
  return status;
}

export function createKmMemoryBackendProviders(input: {
  configs: KmMemoryProviderConfig[];
  env?: NodeJS.ProcessEnv;
  secretDir?: string;
  transport?: InMemoryMemoryBackendTransport;
}): { providers: Map<string, MemoryBackendProvider>; statuses: KmBackendProviderRuntimeStatus[] } {
  const providers = new Map<string, MemoryBackendProvider>();
  const statuses: KmBackendProviderRuntimeStatus[] = [];
  const transport = input.transport ?? new InMemoryMemoryBackendTransport();
  for (const config of input.configs) {
    const safeConfig = runtimeConfig(config);
    if (!safeConfig.enabled) {
      statuses.push({ providerId: safeConfig.providerId, endpoint: safeConfig.endpoint, enabled: false, status: 'disabled' });
      continue;
    }
    if (!isExplicitMockEndpoint(safeConfig.endpoint)) {
      statuses.push({
        providerId: safeConfig.providerId,
        endpoint: safeConfig.endpoint,
        enabled: safeConfig.enabled,
        status: 'unsafe_endpoint',
        reason: 'km_memory_backend_requires_explicit_mock_or_inmemory_endpoint',
      });
      continue;
    }
    const result = createMemoryBackendProvider({
      config: safeConfig,
      env: input.env,
      secretDir: input.secretDir,
      transport,
      allowRealTransport: false,
    });
    statuses.push({
      providerId: config.providerId,
      endpoint: config.endpoint,
      enabled: config.enabled,
      status: result.status,
      ...(result.credential && !result.credential.ok ? { reason: result.credential.reason } : {}),
    });
    if (result.provider) providers.set(result.provider.descriptor.id, result.provider);
  }
  return { providers, statuses };
}

export async function kmBackendRuntimeStatus(input: {
  dataDir: string;
  env?: NodeJS.ProcessEnv;
  now?: number;
  outboxLimit?: number;
}): Promise<KmBackendRuntimeStatus> {
  const env = input.env ?? process.env;
  const store = await ObservationStore.open(input.dataDir);
  try {
    const configs = store.listMemoryProviderConfigs({ redactCredentials: false });
    const { statuses } = createKmMemoryBackendProviders({ configs, env });
    return {
      enabled: isKmBackendWorkerEnabled(env),
      leaseName: 'memory-backend-outbox',
      outbox: summarizeOutbox(store.listMemoryBackendOutbox(input.outboxLimit ?? 500), input.now),
      providers: statuses,
    };
  } finally {
    store.close();
  }
}

export async function runKmBackendWorkerOnce(input: {
  dataDir: string;
  env?: NodeJS.ProcessEnv;
  holderId?: string;
  batchLimit?: number;
  leaseMs?: number;
  maxAttempts?: number;
  now?: number;
}): Promise<KmBackendWorkerRuntimeReport> {
  const env = input.env ?? process.env;
  const holderId = input.holderId ?? `pid:${process.pid}`;
  const batchLimit = input.batchLimit ?? boundedInteger(env.BOTMUX_KM_BACKEND_WORKER_BATCH_LIMIT, DEFAULT_BATCH_LIMIT, 1, 100);
  const leaseMs = input.leaseMs ?? boundedInteger(env.BOTMUX_KM_BACKEND_WORKER_LEASE_MS, DEFAULT_LEASE_MS, 1_000, 15 * 60_000);
  const maxAttempts = input.maxAttempts ?? boundedInteger(env.BOTMUX_KM_BACKEND_WORKER_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS, 1, 50);
  const report: KmBackendWorkerRuntimeReport = {
    enabled: isKmBackendWorkerEnabled(env),
    leaseAcquired: false,
    holderId,
    providers: [],
  };
  if (!report.enabled) return report;

  const store = await ObservationStore.open(input.dataDir);
  try {
    report.leaseAcquired = store.acquireRuntimeLease({ leaseName: 'memory-backend-outbox', holderId, ttlMs: leaseMs, now: input.now });
    if (!report.leaseAcquired) return report;
    const configs = store.listMemoryProviderConfigs({ redactCredentials: false });
    const { providers, statuses } = createKmMemoryBackendProviders({ configs, env });
    report.providers = statuses;
    const botAppIds = new Set<string>();
    for (const entry of store.listPipelineProfiles()) {
      const profile = entry.profile as Partial<KmPipelineProfile> | undefined;
      if (typeof profile?.botAppId === 'string' && profile.botAppId.trim()) botAppIds.add(profile.botAppId);
    }
    const profiles = [...botAppIds]
      .map(botAppId => store.getEffectivePipelineProfile(botAppId))
      .filter((profile): profile is KmPipelineProfile => Boolean(profile));
    report.enqueue = { scanned: 0, enqueued: 0, skipped: 0, targetProviders: [] };
    for (const profile of profiles) {
      const enqueue = enqueueEligibleMemoryBackendMirrors({ store, profile, limit: batchLimit, now: input.now });
      report.enqueue.scanned += enqueue.scanned;
      report.enqueue.enqueued += enqueue.enqueued;
      report.enqueue.skipped += enqueue.skipped;
      for (const providerId of enqueue.targetProviders) {
        if (!report.enqueue.targetProviders.includes(providerId)) report.enqueue.targetProviders.push(providerId);
      }
    }
    if (providers.size === 0) {
      report.worker = { claimed: 0, delivered: 0, retried: 0, quarantined: 0, failures: [] };
      return report;
    }
    report.worker = await drainMemoryBackendOutbox({
      dataDir: input.dataDir,
      providers,
      limit: batchLimit,
      leaseMs,
      maxAttempts,
      timeoutMs: boundedInteger(env.BOTMUX_KM_BACKEND_WORKER_TIMEOUT_MS, DEFAULT_PROVIDER_TIMEOUT_MS, 100, 30_000),
      now: input.now,
    });
    return report;
  } finally {
    if (report.leaseAcquired) {
      try {
        store.releaseRuntimeLease({ leaseName: 'memory-backend-outbox', holderId });
      } catch {
        // A stale release must not fail the daemon's scheduling loop.
      }
    }
    store.close();
  }
}

export function kmBackendWorkerIntervalMs(env: NodeJS.ProcessEnv = process.env): number {
  return boundedInteger(env.BOTMUX_KM_BACKEND_WORKER_INTERVAL_MS, 30_000, 5_000, 10 * 60_000);
}

export function kmBackendWorkerStartupDelayMs(idx: number): number {
  return 3_000 + Math.max(0, idx) * 750;
}

export const __testOnly_isExplicitMockEndpoint = isExplicitMockEndpoint;
export const __testOnly_summarizeOutbox = summarizeOutbox;
