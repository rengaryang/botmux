import type { ObservationEvent } from './observation-schema.js';
import { ObservationStore, type ObservationAppendResult } from './observation-store.js';

export interface EnqueueObservationInput {
  dataDir: string;
  event: ObservationEvent;
  onResult?: (result: ObservationAppendResult) => void;
  onError?: (error: unknown) => void;
}

interface PendingItem {
  input: EnqueueObservationInput;
  attempts: number;
  promise: Promise<void>;
  resolve: () => void;
  timer?: ReturnType<typeof setTimeout>;
}

const pending = new Map<string, PendingItem>();
const stores = new Map<string, Promise<ObservationStore>>();
let admissionClosed = false;

export function isKmObservationEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const value = env.BOTMUX_KM_OBSERVATION_ENABLED?.trim().toLowerCase();
  return value === '1' || value === 'true' || value === 'yes';
}

function queueKey(input: EnqueueObservationInput): string {
  return `${input.dataDir}|${input.event.ordering.sourceKey}|${input.event.ordering.idempotencyKey}`;
}

function getStore(dataDir: string): Promise<ObservationStore> {
  let current = stores.get(dataDir);
  if (current) return current;
  current = ObservationStore.open(dataDir).catch(error => {
    stores.delete(dataDir);
    throw error;
  });
  stores.set(dataDir, current);
  return current;
}

/**
 * Queue one observation without performing synchronous SQLite work on the
 * caller's stack. The returned promise never rejects; failures are surfaced via
 * onError so telemetry cannot fail the daemon's primary chat path.
 */
export function enqueueObservation(input: EnqueueObservationInput): Promise<void> {
  const key = queueKey(input);
  const existing = pending.get(key);
  if (existing) return existing.promise;
  if (admissionClosed) {
    input.onError?.(new Error(`km_observation_refused_shutdown:${input.event.eventId}`));
    return Promise.resolve();
  }

  let resolve!: () => void;
  const promise = new Promise<void>(r => { resolve = r; });
  pending.set(key, { input, attempts: 0, promise, resolve });
  queueMicrotask(() => { void persist(key); });
  return promise;
}

async function persist(key: string): Promise<void> {
  const item = pending.get(key);
  if (!item) return;
  item.attempts += 1;
  try {
    const store = await getStore(item.input.dataDir);
    const attempt = store.tryAppend(item.input.event);
    if (!attempt.done) {
      if (item.attempts >= 50) {
        item.input.onError?.(new Error(`km_observation_persist_gave_up:${item.input.event.eventId}:busy_after_${item.attempts}`));
        finish(key);
        return;
      }
      scheduleRetry(key, item);
      return;
    }
    item.input.onResult?.(attempt.result);
    finish(key);
  } catch (error) {
    item.input.onError?.(error);
    finish(key);
  }
}

function scheduleRetry(key: string, item: PendingItem): void {
  const delay = Math.min(25 * item.attempts, 500);
  item.timer = setTimeout(() => { void persist(key); }, delay);
  item.timer.unref?.();
}

function finish(key: string): void {
  const item = pending.get(key);
  if (!item) return;
  if (item.timer) clearTimeout(item.timer);
  pending.delete(key);
  item.resolve();
}

/** Close queue admission and await already accepted writes within a bound. */
export async function drainObservationQueue(timeoutMs = 3_000): Promise<number> {
  admissionClosed = true;
  if (pending.size === 0) return 0;
  const deadline = Date.now() + timeoutMs;
  const keepalive = setInterval(() => { /* keep event loop alive during bounded drain */ }, 1_000);
  try {
    while (pending.size > 0 && Date.now() < deadline) {
      const remainingMs = Math.max(0, deadline - Date.now());
      const all = Promise.all([...pending.values()].map(item => item.promise));
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<'timeout'>(resolve => {
        timer = setTimeout(() => resolve('timeout'), remainingMs);
        timer.unref?.();
      });
      await Promise.race([all.then(() => 'drained' as const), timeout]);
      if (timer) clearTimeout(timer);
    }
    return pending.size;
  } finally {
    clearInterval(keepalive);
  }
}

export function __testOnly_pendingObservationCount(): number {
  return pending.size;
}

export function __testOnly_reopenObservationAdmission(): void {
  admissionClosed = false;
}

export async function __testOnly_closeObservationStores(): Promise<void> {
  const open = [...stores.values()];
  stores.clear();
  const settled = await Promise.allSettled(open);
  for (const result of settled) {
    if (result.status === 'fulfilled') result.value.close();
  }
}
