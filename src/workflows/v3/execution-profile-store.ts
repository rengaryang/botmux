import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join } from 'node:path';
import { homedir } from 'node:os';
import type { CliId } from '../../adapters/cli/types.js';
import { V3_SUPPORTED_CLIS } from './contract.js';

export interface WorkflowExecutionProfile {
  schemaVersion: 1;
  profileId: string;
  displayName: string;
  cli: CliId;
  provider?: string;
  model?: string;
  workingDir: string;
  sandbox: { enabled: boolean; network: boolean; readWrite: string[]; readOnly: string[]; deny: string[] };
  envPolicy: { inherit: 'safe-host'; allow: string[]; deny: string[] };
  timeoutPolicy: { defaultSec: number; maxSec: number };
  costTier: 'low' | 'medium' | 'high';
  enabled: boolean;
  revision: number;
  updatedAt: string;
}

interface Catalog { schemaVersion: 1; profiles: WorkflowExecutionProfile[] }
const PROFILE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/u;

export function defaultExecutionProfilePath(): string {
  return process.env.BOTMUX_WORKFLOW_EXECUTION_PROFILES_PATH?.trim()
    || join(homedir(), '.botmux', 'workflow-execution-profiles.json');
}

export function validateWorkflowExecutionProfile(raw: unknown, now = new Date().toISOString()): WorkflowExecutionProfile {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('workflow_profile_invalid');
  const value = raw as Record<string, unknown>;
  const profileId = String(value.profileId ?? '').trim();
  const displayName = String(value.displayName ?? profileId).trim();
  const cli = String(value.cli ?? '').trim() as CliId;
  const workingDir = String(value.workingDir ?? '').trim();
  if (!PROFILE_ID.test(profileId)) throw new Error('workflow_profile_id_invalid');
  if (!displayName) throw new Error('workflow_profile_name_required');
  if (!V3_SUPPORTED_CLIS.includes(cli)) throw new Error('workflow_profile_cli_unsupported');
  if (!isAbsolute(workingDir)) throw new Error('workflow_profile_working_dir_absolute_required');
  const provider = typeof value.provider === 'string' && value.provider.trim() ? value.provider.trim() : undefined;
  const rawModel = typeof value.model === 'string' && value.model.trim() ? value.model.trim() : undefined;
  if (provider && rawModel?.includes('/') && !rawModel.startsWith(`${provider}/`)) throw new Error('workflow_profile_provider_model_mismatch');
  const model = provider && rawModel && !rawModel.includes('/') ? `${provider}/${rawModel}` : rawModel;
  const sandboxRaw = (value.sandbox ?? {}) as Record<string, unknown>;
  const envRaw = (value.envPolicy ?? {}) as Record<string, unknown>;
  const timeoutRaw = (value.timeoutPolicy ?? {}) as Record<string, unknown>;
  const strings = (candidate: unknown): string[] => Array.isArray(candidate)
    ? [...new Set(candidate.filter((item): item is string => typeof item === 'string' && item.trim() !== '').map(item => item.trim()))]
    : [];
  const defaultSec = Number(timeoutRaw.defaultSec ?? 1800);
  const maxSec = Number(timeoutRaw.maxSec ?? 14400);
  if (!Number.isInteger(defaultSec) || !Number.isInteger(maxSec) || defaultSec < 60 || maxSec < defaultSec || maxSec > 14400) {
    throw new Error('workflow_profile_timeout_invalid');
  }
  const costTier = String(value.costTier ?? 'medium');
  if (!['low', 'medium', 'high'].includes(costTier)) throw new Error('workflow_profile_cost_tier_invalid');
  return {
    schemaVersion: 1, profileId, displayName, cli, ...(provider ? { provider } : {}), ...(model ? { model } : {}), workingDir,
    sandbox: {
      enabled: sandboxRaw.enabled !== false,
      network: sandboxRaw.network !== false,
      readWrite: strings(sandboxRaw.readWrite), readOnly: strings(sandboxRaw.readOnly), deny: strings(sandboxRaw.deny),
    },
    envPolicy: { inherit: 'safe-host', allow: strings(envRaw.allow), deny: strings(envRaw.deny) },
    timeoutPolicy: { defaultSec, maxSec },
    costTier: costTier as WorkflowExecutionProfile['costTier'], enabled: value.enabled !== false,
    revision: Number.isInteger(value.revision) && Number(value.revision) > 0 ? Number(value.revision) : 1,
    updatedAt: typeof value.updatedAt === 'string' ? value.updatedAt : now,
  };
}

export class WorkflowExecutionProfileStore {
  constructor(readonly path = defaultExecutionProfilePath()) {}
  list(): WorkflowExecutionProfile[] { return this.read().profiles.sort((a, b) => a.profileId.localeCompare(b.profileId)); }
  get(profileId: string): WorkflowExecutionProfile | undefined { return this.list().find(item => item.profileId === profileId); }
  put(raw: unknown): WorkflowExecutionProfile {
    const prior = raw && typeof raw === 'object' ? this.get(String((raw as any).profileId ?? '')) : undefined;
    const profile = validateWorkflowExecutionProfile({ ...(raw as object), revision: (prior?.revision ?? 0) + 1, updatedAt: new Date().toISOString() });
    const catalog = this.read();
    catalog.profiles = [...catalog.profiles.filter(item => item.profileId !== profile.profileId), profile];
    this.write(catalog); return profile;
  }
  disable(profileId: string): WorkflowExecutionProfile {
    const prior = this.get(profileId); if (!prior) throw new Error('workflow_profile_not_found');
    return this.put({ ...prior, enabled: false });
  }
  private read(): Catalog {
    try {
      const parsed = JSON.parse(readFileSync(this.path, 'utf8')) as Catalog;
      if (parsed.schemaVersion !== 1 || !Array.isArray(parsed.profiles)) throw new Error('catalog malformed');
      return { schemaVersion: 1, profiles: parsed.profiles.map(item => validateWorkflowExecutionProfile(item)) };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { schemaVersion: 1, profiles: [] };
      throw new Error(`workflow_profile_catalog_invalid:${error instanceof Error ? error.message : String(error)}`);
    }
  }
  private write(catalog: Catalog): void {
    mkdirSync(dirname(this.path), { recursive: true });
    const temp = `${this.path}.tmp-${process.pid}-${Date.now()}`;
    writeFileSync(temp, `${JSON.stringify(catalog, null, 2)}\n`, { mode: 0o600 }); renameSync(temp, this.path);
  }
}
