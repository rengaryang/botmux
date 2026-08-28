import { createHash } from 'node:crypto';
import { ObservationStore, type SyncStatus } from './observation-store.js';
import { redactObservationForSync } from './sync-redaction.js';
import { MockSyncSinkProvider, runSyncOnce, type SyncBatch, type SyncBatchAck, type SyncSinkProvider } from './sync-worker.js';

const DEFAULT_BATCH_LIMIT = 25;
const DEFAULT_LEASE_MS = 60_000;
const DEFAULT_MAX_ATTEMPTS = 5;
const DEFAULT_TIMEOUT_MS = 5_000;
const CENTRAL_SINK_LEASE = 'km-central-sink';

function envOn(name: string, env: NodeJS.ProcessEnv = process.env): boolean {
  return ['1', 'true', 'yes'].includes(env[name]?.trim().toLowerCase() ?? '');
}

function boundedInteger(value: string | undefined, fallback: number, min: number, max: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(Math.trunc(parsed), max));
}

function sha256Json(value: unknown): string {
  return `sha256:${createHash('sha256').update(JSON.stringify(value)).digest('hex')}`;
}

export function isKmCentralSinkEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return envOn('BOTMUX_KM_CENTRAL_SINK_ENABLED', env);
}

export function kmCentralSinkIntervalMs(env: NodeJS.ProcessEnv = process.env): number {
  return boundedInteger(env.BOTMUX_KM_CENTRAL_SINK_INTERVAL_MS, 30_000, 5_000, 10 * 60_000);
}

export function kmCentralSinkStartupDelayMs(idx: number): number {
  return 4_000 + Math.max(0, idx) * 750;
}

export interface CentralSinkEndpointPolicy {
  ok: boolean;
  mode: 'offline' | 'blocked-real' | 'invalid';
  reason?: string;
}

export function evaluateCentralSinkEndpointPolicy(endpointRef: string): CentralSinkEndpointPolicy {
  try {
    const url = new URL(endpointRef);
    if (url.protocol === 'mock:' || url.protocol === 'inmemory:') return { ok: true, mode: 'offline' };
    if (url.protocol === 'https:') return { ok: false, mode: 'blocked-real', reason: 'offline_runtime_allows_mock_or_inmemory_only' };
    if (url.protocol === 'http:') return { ok: false, mode: 'invalid', reason: 'tls_required_for_future_real_transport' };
    return { ok: false, mode: 'invalid', reason: 'unsupported_protocol' };
  } catch {
    return { ok: false, mode: 'invalid', reason: 'invalid_url' };
  }
}

export interface CentralSinkRuntimeStatus {
  enabled: boolean;
  leaseName: string;
  protocol: {
    envelopeVersion: 1;
    signing: 'hmac-sha256-over-canonical-batch';
    credentialMode: 'reference-only';
    realTransportEnabled: false;
    networkLibrariesAllowed: false;
  };
  defaults: { batchLimit: number; leaseMs: number; timeoutMs: number; maxAttempts: number };
  sinks: SyncStatus[];
  rollback: { automaticRemoteRollback: false; localDisableOnly: true };
}

export interface CentralSinkWorkerReport {
  enabled: boolean;
  leaseName: string;
  leaseAcquired: boolean;
  holderId: string;
  scanned: number;
  enqueued: number;
  skipped: number;
  delivered: number;
  retried: number;
  quarantined: number;
  failures: Array<{ sinkId: string; error: string }>;
}

export async function centralSinkRuntimeStatus(input: {
  dataDir: string;
  env?: NodeJS.ProcessEnv;
  now?: number;
}): Promise<CentralSinkRuntimeStatus> {
  const env = input.env ?? process.env;
  const store = await ObservationStore.open(input.dataDir);
  try {
    return {
      enabled: isKmCentralSinkEnabled(env),
      leaseName: CENTRAL_SINK_LEASE,
      protocol: {
        envelopeVersion: 1,
        signing: 'hmac-sha256-over-canonical-batch',
        credentialMode: 'reference-only',
        realTransportEnabled: false,
        networkLibrariesAllowed: false,
      },
      defaults: {
        batchLimit: boundedInteger(env.BOTMUX_KM_CENTRAL_SINK_BATCH_LIMIT, DEFAULT_BATCH_LIMIT, 1, 100),
        leaseMs: boundedInteger(env.BOTMUX_KM_CENTRAL_SINK_LEASE_MS, DEFAULT_LEASE_MS, 1_000, 15 * 60_000),
        timeoutMs: boundedInteger(env.BOTMUX_KM_CENTRAL_SINK_TIMEOUT_MS, DEFAULT_TIMEOUT_MS, 100, 30_000),
        maxAttempts: boundedInteger(env.BOTMUX_KM_CENTRAL_SINK_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS, 1, 50),
      },
      sinks: store.listSyncStatus(),
      rollback: { automaticRemoteRollback: false, localDisableOnly: true },
    };
  } finally {
    store.close();
  }
}

type CentralSinkDrillScenario = 'success' | 'partial' | 'reject' | 'error';

class PartialMockSyncSinkProvider extends MockSyncSinkProvider {
  async send(batch: SyncBatch, signature: string): Promise<SyncBatchAck> {
    this.received.push({ batch, signature });
    const accepted = batch.events.slice(0, 1).map(event => event.eventId);
    const rejected = batch.events.slice(1).map(event => ({
      eventId: event.eventId,
      code: 'fixture_partial_reject',
      message: 'central sink drill rejection',
    }));
    return { status: rejected.length > 0 ? 'partial' : 'accepted', acceptedEventIds: accepted, rejected, cursor: batch.batchId };
  }
}

class RejectingMockSyncSinkProvider extends MockSyncSinkProvider {
  async send(batch: SyncBatch, signature: string): Promise<SyncBatchAck> {
    this.received.push({ batch, signature });
    return {
      status: 'rejected',
      acceptedEventIds: [],
      rejected: batch.events.map(event => ({ eventId: event.eventId, code: 'fixture_reject', message: 'central sink drill rejection' })),
      cursor: batch.batchId,
    };
  }
}

class ErrorMockSyncSinkProvider extends MockSyncSinkProvider {
  async send(batch: SyncBatch, signature: string): Promise<SyncBatchAck> {
    this.received.push({ batch, signature });
    throw new Error('fixture_transport_error');
  }
}

function providerFor(status: SyncStatus, scenario?: CentralSinkDrillScenario): SyncSinkProvider | null {
  const policy = evaluateCentralSinkEndpointPolicy(status.endpointRef);
  if (!policy.ok) return null;
  if (scenario === 'partial') return new PartialMockSyncSinkProvider();
  if (scenario === 'reject') return new RejectingMockSyncSinkProvider();
  if (scenario === 'error') return new ErrorMockSyncSinkProvider();
  return new MockSyncSinkProvider();
}

function offlineFixtureSigningSecret(_status: SyncStatus): string {
  // Offline drills must never resolve or use configured credentials. Real
  // signing is reserved for the separately approved network transport.
  return 'offline-fixture-secret';
}

export async function runKmCentralSinkOnce(input: {
  dataDir: string;
  env?: NodeJS.ProcessEnv;
  holderId?: string;
  now?: number;
  scenario?: CentralSinkDrillScenario;
}): Promise<CentralSinkWorkerReport> {
  const env = input.env ?? process.env;
  const holderId = input.holderId ?? `pid:${process.pid}`;
  const report: CentralSinkWorkerReport = {
    enabled: isKmCentralSinkEnabled(env),
    leaseName: CENTRAL_SINK_LEASE,
    leaseAcquired: false,
    holderId,
    scanned: 0,
    enqueued: 0,
    skipped: 0,
    delivered: 0,
    retried: 0,
    quarantined: 0,
    failures: [],
  };
  if (!report.enabled) return report;

  const store = await ObservationStore.open(input.dataDir);
  try {
    const leaseMs = boundedInteger(env.BOTMUX_KM_CENTRAL_SINK_LEASE_MS, DEFAULT_LEASE_MS, 1_000, 15 * 60_000);
    report.leaseAcquired = store.acquireRuntimeLease({ leaseName: CENTRAL_SINK_LEASE, holderId, ttlMs: leaseMs, now: input.now });
    if (!report.leaseAcquired) return report;
    const fallbackLimit = boundedInteger(env.BOTMUX_KM_CENTRAL_SINK_BATCH_LIMIT, DEFAULT_BATCH_LIMIT, 1, 100);
    for (const sink of store.listSyncStatus({ redactCredentials: false }).filter(item => item.enabled)) {
      const policy = evaluateCentralSinkEndpointPolicy(sink.endpointRef);
      if (!policy.ok) {
        report.failures.push({ sinkId: sink.sinkId, error: policy.reason ?? 'central_sink_endpoint_blocked' });
        continue;
      }
      const enqueued = store.enqueueSyncFromCursor({
        sinkId: sink.sinkId,
        limit: sink.batchLimit ?? fallbackLimit,
        now: input.now,
        redact: event => {
          const redacted = redactObservationForSync(event);
          return redacted.ok
            ? { ok: true, envelope: { protocolVersion: 1, kind: 'km.observation', ...redacted.envelope }, payloadHash: redacted.envelope.payloadHash }
            : redacted;
        },
      });
      report.scanned += enqueued.scanned;
      report.enqueued += enqueued.enqueued;
      report.skipped += enqueued.skipped;
      report.quarantined += enqueued.quarantined;
      const provider = providerFor(sink, input.scenario);
      if (!provider) continue;
      try {
        const drained = await runSyncOnce({
          store,
          sinkId: sink.sinkId,
          sourceHostId: holderId,
          provider,
          signingSecret: offlineFixtureSigningSecret(sink),
          limit: sink.batchLimit ?? fallbackLimit,
          leaseMs,
          timeoutMs: sink.timeoutMs ?? DEFAULT_TIMEOUT_MS,
          maxAttempts: sink.maxAttempts,
          baseDelayMs: boundedInteger(env.BOTMUX_KM_CENTRAL_SINK_RETRY_BASE_MS, 1_000, 100, 300_000),
          now: input.now,
        });
        report.delivered += drained.accepted;
        report.quarantined += drained.quarantined;
        if (drained.status === 'auth_failed' || drained.status === 'rejected') report.retried += drained.attempted;
      } catch (error) {
        const state = store.listSyncOutbox({ sinkId: sink.sinkId, limit: 100 })
          .find(item => item.status === 'quarantined' && item.lastError)?.status;
        if (state === 'quarantined') report.quarantined += 1;
        else report.retried += 1;
        report.failures.push({ sinkId: sink.sinkId, error: error instanceof Error ? error.message : String(error) });
      }
    }
    return report;
  } finally {
    if (report.leaseAcquired) {
      try { store.releaseRuntimeLease({ leaseName: CENTRAL_SINK_LEASE, holderId }); } catch {}
    }
    store.close();
  }
}

export async function runKmCentralSinkDrill(input: {
  dataDir: string;
  sinkId: string;
  drill: 'status' | 'partial-ack' | 'replay' | 'conflict';
  actorId: string;
  idempotencyKey: string;
  now?: number;
}): Promise<Record<string, unknown>> {
  const store = await ObservationStore.open(input.dataDir);
  try {
    const before = store.listSyncStatus().find(item => item.sinkId === input.sinkId);
    if (!before) throw new Error('km_central_sink_not_found');
    if (input.drill === 'status') return { drill: 'status', sink: before, rollback: { automaticRemoteRollback: false, localDisableOnly: true } };
    if (input.drill === 'replay') {
      const rows = store.listSyncOutbox({ sinkId: input.sinkId, limit: 50 });
      return { drill: 'replay', replayable: true, outboxHash: sha256Json(rows.map(row => ({ eventId: row.eventId, payloadHash: row.payloadHash, status: row.status }))), rows: rows.length };
    }
    if (input.drill === 'conflict') {
      const qid = store.quarantineSync({ sinkId: input.sinkId, reason: `drill_conflict:${input.actorId}`, payloadHash: sha256Json({ drill: input.drill, idempotencyKey: input.idempotencyKey }) });
      return { drill: 'conflict', quarantineId: qid, rollback: { localDisableOnly: true } };
    }
    const result = await runKmCentralSinkOnce({
      dataDir: input.dataDir,
      env: { ...process.env, BOTMUX_KM_CENTRAL_SINK_ENABLED: 'true' },
      holderId: `drill:${input.actorId}`,
      now: input.now,
      scenario: 'partial',
    });
    return { drill: 'partial-ack', result, sink: store.listSyncStatus().find(item => item.sinkId === input.sinkId) };
  } finally {
    store.close();
  }
}

export const __testOnly_CENTRAL_SINK_LEASE = CENTRAL_SINK_LEASE;
