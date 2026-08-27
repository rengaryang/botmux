import { createHash } from 'node:crypto';
import type { BackendMemoryRef, BackendMemoryWrite, MemoryBackendProvider } from './memory-backend-spi.js';
import { ObservationStore, type MemoryBackendOutboxItem } from './observation-store.js';

function sha256Json(value: unknown): string {
  return `sha256:${createHash('sha256').update(JSON.stringify(value)).digest('hex')}`;
}

function payloadToWrite(item: MemoryBackendOutboxItem): BackendMemoryWrite {
  const payload = item.payload as Partial<BackendMemoryWrite>;
  const claimText = typeof payload.claimText === 'string' ? payload.claimText : String((item.payload as any).text ?? '');
  return {
    memoryId: item.memoryId,
    scope: payload.scope ?? 'user',
    subject: typeof payload.subject === 'string' ? payload.subject : '',
    claimKey: typeof payload.claimKey === 'string' ? payload.claimKey : item.memoryId,
    claimText,
    privacyClass: payload.privacyClass ?? 'internal',
    ...(typeof payload.ttlExpiresAt === 'string' ? { ttlExpiresAt: payload.ttlExpiresAt } : {}),
    sourceRefs: Array.isArray(payload.sourceRefs) ? payload.sourceRefs : [],
    contentHash: typeof payload.contentHash === 'string' ? payload.contentHash : sha256Json(item.payload),
  };
}

function payloadToRef(item: MemoryBackendOutboxItem): BackendMemoryRef {
  const payload = item.payload as { backendRef?: unknown; contentHash?: unknown };
  const backendRef = typeof payload.backendRef === 'string' && payload.backendRef.trim()
    ? payload.backendRef
    : item.memoryId;
  const contentHash = typeof payload.contentHash === 'string' && payload.contentHash.trim()
    ? payload.contentHash
    : sha256Json(item.payload);
  return { providerId: item.providerId, backendRef, contentHash };
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`km_memory_backend_timeout:${label}`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export interface MemoryBackendOutboxWorkerReport {
  claimed: number; delivered: number; retried: number; quarantined: number;
  failures: Array<{ outboxId: string; providerId: string; error: string; retryable: boolean }>;
}

export async function drainMemoryBackendOutbox(input: {
  dataDir: string;
  providers: Map<string, MemoryBackendProvider> | Record<string, MemoryBackendProvider>;
  limit?: number;
  leaseMs?: number;
  maxAttempts?: number;
  timeoutMs?: number;
  now?: number;
}): Promise<MemoryBackendOutboxWorkerReport> {
  const store = await ObservationStore.open(input.dataDir);
  const providers = input.providers instanceof Map ? input.providers : new Map(Object.entries(input.providers));
  const timeoutMs = Math.max(100, Math.min(input.timeoutMs ?? 5_000, 30_000));
  const report: MemoryBackendOutboxWorkerReport = { claimed: 0, delivered: 0, retried: 0, quarantined: 0, failures: [] };
  try {
    const claim = store.claimMemoryBackendOutboxBatch({ limit: input.limit ?? 25, leaseMs: input.leaseMs, now: input.now });
    report.claimed = claim.items.length;
    for (const item of claim.items) {
      const provider = providers.get(item.providerId);
      if (!provider) {
        const error = 'km_memory_backend_provider_unavailable';
        store.failMemoryBackendOutboxItem({ outboxId: item.outboxId, claimToken: claim.claimToken, error, retry: true,
          maxAttempts: input.maxAttempts, now: input.now });
        const state = store.listMemoryBackendOutbox(500).find(row => row.outboxId === item.outboxId)?.status;
        if (state === 'quarantined') report.quarantined += 1; else report.retried += 1;
        report.failures.push({ outboxId: item.outboxId, providerId: item.providerId, error, retryable: true });
        continue;
      }
      try {
        if (item.operation === 'put') {
          const ref = await withTimeout(provider.put(payloadToWrite(item)), timeoutMs, item.providerId);
          store.settleMemoryBackendOutboxItem({ outboxId: item.outboxId, claimToken: claim.claimToken, providerVersion: provider.descriptor.version,
            writeState: 'active', backendRef: ref.backendRef, contentHash: ref.contentHash, now: input.now });
        } else if (item.operation === 'revoke') {
          await withTimeout(provider.revoke(payloadToRef(item), String((item.payload as any).reason ?? 'outbox_revoke')), timeoutMs, item.providerId);
          store.settleMemoryBackendOutboxItem({ outboxId: item.outboxId, claimToken: claim.claimToken, providerVersion: provider.descriptor.version,
            writeState: 'revoked', contentHash: payloadToRef(item).contentHash, now: input.now });
        } else {
          await withTimeout(provider.health(), timeoutMs, item.providerId);
          const ref = payloadToRef(item);
          store.settleMemoryBackendOutboxItem({ outboxId: item.outboxId, claimToken: claim.claimToken, providerVersion: provider.descriptor.version,
            writeState: 'active', backendRef: ref.backendRef, contentHash: ref.contentHash, now: input.now });
        }
        report.delivered += 1;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        store.failMemoryBackendOutboxItem({ outboxId: item.outboxId, claimToken: claim.claimToken, error: message, retry: true,
          maxAttempts: input.maxAttempts, now: input.now });
        const state = store.listMemoryBackendOutbox(500).find(row => row.outboxId === item.outboxId)?.status;
        if (state === 'quarantined') report.quarantined += 1; else report.retried += 1;
        report.failures.push({ outboxId: item.outboxId, providerId: item.providerId, error: message, retryable: state !== 'quarantined' });
      }
    }
    return report;
  } finally {
    store.close();
  }
}
