import type { MemoryBackendProvider, BackendMemoryQuery, BackendMemoryResult, BackendMemoryWrite } from './memory-backend-spi.js';

export type BackendWritePolicy = 'single' | 'primary-mirror' | 'all' | 'shadow-write';

export interface BackendWriteReport {
  committed: boolean;
  results: Array<{ providerId: string; status: 'active' | 'failed' | 'shadow'; backendRef?: string; error?: string }>;
}

export async function writeMemoryToBackends(input: {
  item: BackendMemoryWrite; policy: BackendWritePolicy; primary: MemoryBackendProvider; mirrors: MemoryBackendProvider[];
}): Promise<BackendWriteReport> {
  const write = async (provider: MemoryBackendProvider, shadow: boolean) => {
    try {
      const ref = await provider.put(input.item);
      return { providerId: provider.descriptor.id, status: shadow ? 'shadow' as const : 'active' as const, backendRef: ref.backendRef };
    } catch (error) {
      return { providerId: provider.descriptor.id, status: 'failed' as const, error: error instanceof Error ? error.message : String(error) };
    }
  };
  if (input.policy === 'single') {
    const result = await write(input.primary, false);
    return { committed: result.status === 'active', results: [result] };
  }
  if (input.policy === 'all') {
    const results = await Promise.all([input.primary, ...input.mirrors].map(provider => write(provider, false)));
    return { committed: results.every(result => result.status === 'active'), results };
  }
  if (input.policy === 'shadow-write') {
    const primary = await write(input.primary, false);
    const mirrors = await Promise.all(input.mirrors.map(provider => write(provider, true)));
    return { committed: primary.status === 'active', results: [primary, ...mirrors] };
  }
  const primary = await write(input.primary, false);
  if (primary.status !== 'active') return { committed: false, results: [primary] };
  const mirrors = await Promise.all(input.mirrors.map(provider => write(provider, false)));
  return { committed: true, results: [primary, ...mirrors] };
}

/** Reciprocal-rank fusion; backend-native scores remain metadata, not directly comparable. */
export async function federatedMemoryRetrieve(input: {
  providers: MemoryBackendProvider[]; query: BackendMemoryQuery; limit: number;
}): Promise<Array<BackendMemoryResult & { fusedScore: number; providers: string[] }>> {
  return (await federatedMemoryRetrieveWithTelemetry(input)).items;
}

export interface FederatedMemoryRetrievalTelemetry {
  providerId: string;
  status: 'ok' | 'failed' | 'timeout';
  latencyMs: number;
  itemCount: number;
  error?: string;
}

export interface FederatedMemoryRetrievalReport {
  items: Array<BackendMemoryResult & { fusedScore: number; providers: string[] }>;
  telemetry: FederatedMemoryRetrievalTelemetry[];
  warnings: string[];
  partialFailure: boolean;
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
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

export async function federatedMemoryRetrieveWithTelemetry(input: {
  providers: MemoryBackendProvider[]; query: BackendMemoryQuery; limit: number; timeoutMs?: number;
}): Promise<FederatedMemoryRetrievalReport> {
  const timeoutMs = Math.max(100, Math.min(input.timeoutMs ?? 2_000, 30_000));
  const batches = await Promise.all(input.providers.map(async provider => {
    const started = Date.now();
    try {
      const items = await withTimeout(provider.retrieve(input.query), timeoutMs, provider.descriptor.id);
      return { providerId: provider.descriptor.id, items, telemetry: { providerId: provider.descriptor.id, status: 'ok' as const,
        latencyMs: Date.now() - started, itemCount: items.length } };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const status = message.includes('km_memory_backend_timeout') ? 'timeout' as const : 'failed' as const;
      return { providerId: provider.descriptor.id, items: [], telemetry: { providerId: provider.descriptor.id, status,
        latencyMs: Date.now() - started, itemCount: 0, error: message } };
    }
  }));
  const merged = new Map<string, BackendMemoryResult & { fusedScore: number; providers: string[] }>();
  for (const batch of batches) {
    batch.items.forEach((item, rank) => {
      const key = item.memoryId ?? `${item.scope ?? ''}|${item.subject ?? ''}|${item.text.trim().toLowerCase()}`;
      const score = 1 / (60 + rank + 1);
      const existing = merged.get(key);
      if (existing) {
        existing.fusedScore += score;
        if (!existing.providers.includes(batch.providerId)) existing.providers.push(batch.providerId);
      } else merged.set(key, { ...item, fusedScore: score, providers: [batch.providerId] });
    });
  }
  const telemetry = batches.map(batch => batch.telemetry);
  const warnings = telemetry.filter(item => item.status !== 'ok').map(item => `${item.providerId}:${item.status}`);
  return {
    items: [...merged.values()].sort((a, b) => b.fusedScore - a.fusedScore || a.text.localeCompare(b.text)).slice(0, input.limit),
    telemetry,
    warnings,
    partialFailure: warnings.length > 0,
  };
}
