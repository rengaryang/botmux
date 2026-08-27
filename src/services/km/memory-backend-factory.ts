import { lstatSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import {
  HindsightMemoryBackend,
  Mem0MemoryBackend,
  OpenVikingMemoryBackend,
  type MemoryBackendProvider,
  type MemoryBackendTransport,
} from './memory-backend-spi.js';
import { KmMemoryProviderConfigSchema, type KmMemoryProviderConfig } from './provider-spi.js';

export type CredentialResolverResult =
  | { ok: true; kind: 'env' | 'file'; value: string }
  | { ok: false; kind: 'env' | 'file'; reason: 'missing' | 'outside_secret_dir' | 'not_regular_file' | 'insecure_permissions' };

export function resolveMemoryBackendCredential(input: {
  credentialRef: string;
  env?: NodeJS.ProcessEnv;
  secretDir?: string;
}): CredentialResolverResult {
  const env = input.env ?? process.env;
  if (input.credentialRef.startsWith('env:')) {
    const name = input.credentialRef.slice('env:'.length);
    const value = env[name]?.trim();
    return value ? { ok: true, kind: 'env', value } : { ok: false, kind: 'env', reason: 'missing' };
  }
  const file = input.credentialRef.startsWith('file:') ? input.credentialRef.slice('file:'.length) : '';
  const secretDir = resolve(input.secretDir ?? env.BOTMUX_KM_SECRET_DIR?.trim() ?? join(homedir(), '.botmux', 'secrets'));
  const candidate = resolve(file);
  if (!(candidate === secretDir || candidate.startsWith(`${secretDir}/`))) {
    return { ok: false, kind: 'file', reason: 'outside_secret_dir' };
  }
  try {
    const stat = lstatSync(candidate);
    if (!stat.isFile() || stat.isSymbolicLink()) return { ok: false, kind: 'file', reason: 'not_regular_file' };
    if ((stat.mode & 0o077) !== 0) return { ok: false, kind: 'file', reason: 'insecure_permissions' };
    const value = readFileSync(candidate, 'utf8').trim();
    return value ? { ok: true, kind: 'file', value } : { ok: false, kind: 'file', reason: 'missing' };
  } catch {
    return { ok: false, kind: 'file', reason: 'missing' };
  }
}

export class DisabledRealTransport implements MemoryBackendTransport {
  async request(): Promise<unknown> {
    throw new Error('km_memory_backend_real_transport_disabled');
  }
}

export class InMemoryMemoryBackendTransport implements MemoryBackendTransport {
  private readonly records = new Map<string, { providerId: string; backendRef: string; payload: Record<string, unknown> }>();

  async request(input: {
    providerId: string;
    operation: 'health' | 'put' | 'revoke' | 'retrieve';
    payload: Record<string, unknown>;
  }): Promise<unknown> {
    if (input.operation === 'health') return { ok: true };
    if (input.operation === 'put') {
      const id = String(input.payload.id ?? input.payload.document_id ?? input.payload.resource_id ?? `${input.providerId}-${this.records.size + 1}`);
      const backendRef = `${input.providerId}:${id}`;
      this.records.set(backendRef, { providerId: input.providerId, backendRef, payload: input.payload });
      return { backendRef };
    }
    if (input.operation === 'revoke') {
      const ref = String(input.payload.backendRef ?? '');
      this.records.delete(ref);
      return { ok: true };
    }
    const query = String(input.payload.query ?? input.payload.text ?? '').toLowerCase();
    const limit = Number(input.payload.limit ?? input.payload.top_k ?? 10);
    return [...this.records.values()]
      .filter(record => record.providerId === input.providerId)
      .map(record => {
        const text = String(record.payload.memory ?? record.payload.content ?? record.payload.text ?? '');
        return {
          backendRef: record.backendRef,
          memoryId: String(record.payload.id ?? record.payload.document_id ?? record.payload.resource_id ?? ''),
          text,
          score: query && text.toLowerCase().includes(query) ? 1 : 0.1,
          metadata: record.payload,
        };
      })
      .sort((a, b) => b.score - a.score || a.backendRef.localeCompare(b.backendRef))
      .slice(0, Number.isFinite(limit) ? limit : 10);
  }
}

export interface MemoryBackendFactoryResult {
  provider?: MemoryBackendProvider;
  credential?: CredentialResolverResult;
  status: 'ready' | 'disabled' | 'credential_missing' | 'real_transport_disabled';
}

export function createMemoryBackendProvider(input: {
  config: KmMemoryProviderConfig;
  transport?: MemoryBackendTransport;
  env?: NodeJS.ProcessEnv;
  secretDir?: string;
  allowRealTransport?: boolean;
}): MemoryBackendFactoryResult {
  const config = KmMemoryProviderConfigSchema.parse(input.config);
  if (!config.enabled) return { status: 'disabled' };
  const credential = resolveMemoryBackendCredential({ credentialRef: config.credentialRef, env: input.env, secretDir: input.secretDir });
  if (!credential.ok) return { status: 'credential_missing', credential };
  if (config.realTransportEnabled || input.allowRealTransport) {
    return { status: 'real_transport_disabled', credential };
  }
  const transport = input.transport ?? new InMemoryMemoryBackendTransport();
  const provider = config.providerId === 'mem0'
    ? new Mem0MemoryBackend(transport)
    : config.providerId === 'hindsight'
      ? new HindsightMemoryBackend(transport)
      : new OpenVikingMemoryBackend(transport);
  return { status: 'ready', provider, credential };
}
