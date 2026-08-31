import { randomUUID } from 'node:crypto';

export const MODEL_PICKER_TTL_MS = 15 * 60 * 1000;

export interface ModelPickerServerBinding {
  readonly larkAppId: string;
  readonly rootId: string;
  readonly sessionId: string;
  readonly cliId: string;
  readonly invokerOpenId: string;
}

interface Entry {
  binding: ModelPickerServerBinding;
  expiresAt: number;
}

const entries = new Map<string, Entry>();

function prune(now: number): void {
  for (const [nonce, entry] of entries) {
    if (entry.expiresAt <= now) entries.delete(nonce);
  }
}

export function issueModelPickerBinding(binding: ModelPickerServerBinding, now = Date.now()): string {
  prune(now);
  const nonce = randomUUID();
  entries.set(nonce, { binding: { ...binding }, expiresAt: now + MODEL_PICKER_TTL_MS });
  return nonce;
}

/** One-shot claim. Every successful callback rotates to a fresh nonce, making
 * already-rendered/stale cards fail closed. The verified callback app/operator
 * are checked BEFORE consumption, so another chat member cannot burn the
 * invoker's card merely by clicking it. Daemon restart drops all bindings, so
 * historical cards cannot mutate a newly-restored session. */
export function claimModelPickerBinding(
  nonce: string,
  expected?: { larkAppId?: string; invokerOpenId?: string },
  now = Date.now(),
): ModelPickerServerBinding | undefined {
  prune(now);
  const entry = entries.get(nonce);
  if (!entry) return undefined;
  if (expected?.larkAppId && entry.binding.larkAppId !== expected.larkAppId) return undefined;
  if (expected?.invokerOpenId && entry.binding.invokerOpenId !== expected.invokerOpenId) return undefined;
  entries.delete(nonce);
  return { ...entry.binding };
}

export function __testOnlyResetModelPickerBindings(): void {
  entries.clear();
}
