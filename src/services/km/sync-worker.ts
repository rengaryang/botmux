import { createHmac } from 'node:crypto';
import type { ObservationStore } from './observation-store.js';

export interface SyncBatch {
  batchId: string;
  sourceHostId: string;
  protocolVersion: 1;
  events: Array<{ eventId: string; payloadHash: string; payload: Record<string, unknown> }>;
  createdAt: string;
}

export interface SyncBatchAck {
  status: 'accepted' | 'partial' | 'rejected' | 'auth_failed';
  acceptedEventIds: string[];
  rejected?: Array<{ eventId: string; code: string; message: string }>;
  cursor?: string;
}

export interface SyncSinkProvider {
  send(batch: SyncBatch, signature: string): Promise<SyncBatchAck>;
}

export function signSyncBatch(batch: SyncBatch, secret: string): string {
  if (!secret) throw new Error('km_sync_signing_secret_required');
  return `sha256=${createHmac('sha256', secret).update(JSON.stringify(batch)).digest('hex')}`;
}

export class MockSyncSinkProvider implements SyncSinkProvider {
  readonly received: Array<{ batch: SyncBatch; signature: string }> = [];
  async send(batch: SyncBatch, signature: string): Promise<SyncBatchAck> {
    this.received.push({ batch, signature });
    return { status: 'accepted', acceptedEventIds: batch.events.map(event => event.eventId), cursor: batch.batchId };
  }
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`km_sync_timeout:${label}`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** One bounded pass. The caller owns scheduling; no background network loop is started here. */
export async function runSyncOnce(input: {
  store: ObservationStore; sinkId: string; sourceHostId: string;
  provider: SyncSinkProvider; signingSecret: string; limit?: number; now?: number; leaseMs?: number;
  timeoutMs?: number; baseDelayMs?: number; maxAttempts?: number;
}): Promise<{ attempted: number; accepted: number; quarantined: number; status: SyncBatchAck['status'] | 'idle' }> {
  const claim = input.store.claimSyncBatch({ sinkId: input.sinkId, limit: input.limit ?? 50, now: input.now, leaseMs: input.leaseMs });
  if (claim.items.length === 0) return { attempted: 0, accepted: 0, quarantined: 0, status: 'idle' };
  const createdAt = new Date(input.now ?? Date.now()).toISOString();
  const batch: SyncBatch = {
    batchId: `batch_${claim.claimToken}`,
    sourceHostId: input.sourceHostId,
    protocolVersion: 1,
    events: claim.items.map(item => ({ eventId: item.eventId, payloadHash: item.payloadHash, payload: item.payload })),
    createdAt,
  };
  try {
    const ack = await withTimeout(
      input.provider.send(batch, signSyncBatch(batch, input.signingSecret)),
      Math.max(100, Math.min(input.timeoutMs ?? 5_000, 30_000)),
      input.sinkId,
    );
    if (ack.status === 'auth_failed') {
      input.store.failSyncClaim({ claimToken: claim.claimToken, error: 'auth_failed', now: input.now, baseDelayMs: input.baseDelayMs, maxAttempts: input.maxAttempts });
      return { attempted: claim.items.length, accepted: 0, quarantined: 0, status: ack.status };
    }
    input.store.acknowledgeSync({ sinkId: input.sinkId, batchId: batch.batchId, acceptedEventIds: ack.acceptedEventIds, centralCursor: ack.cursor });
    let quarantined = 0;
    for (const rejection of ack.rejected ?? []) {
      const item = claim.items.find(candidate => candidate.eventId === rejection.eventId);
      input.store.quarantineSync({ sinkId: input.sinkId, eventId: rejection.eventId, reason: `${rejection.code}:${rejection.message}`, payloadHash: item?.payloadHash ?? 'unknown' });
      quarantined += 1;
    }
    return { attempted: claim.items.length, accepted: ack.acceptedEventIds.length, quarantined, status: ack.status };
  } catch (error) {
    input.store.failSyncClaim({ claimToken: claim.claimToken, error: error instanceof Error ? error.message : String(error), now: input.now, baseDelayMs: input.baseDelayMs, maxAttempts: input.maxAttempts });
    throw error;
  }
}
