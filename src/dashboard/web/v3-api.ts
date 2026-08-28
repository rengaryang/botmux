import type { RunSummary, RunView } from '../../workflows/v3/ops-projection.js';
import type { V3RunStatus } from '../../workflows/v3/state.js';
import type { WorkflowExecutionProfile } from '../../workflows/v3/execution-profile-store.js';
import type { WorkflowProfileRecommendation } from '../../workflows/v3/model-recommender.js';
import { controlCsrfHeaders } from './control-csrf.js';

export type V3Fetch = (input: string, init?: RequestInit) => Promise<Response>;

export interface WorkflowModelChoice {
  value: string;
  provider?: string;
  model: string;
  label: string;
}

export interface WorkflowModelChoices {
  models: string[];
  choices: WorkflowModelChoice[];
  source: 'static' | 'live';
  detectedAt?: number;
}

export interface V3RunDetailOk {
  ok: true;
  view: RunView;
}

export interface V3RunDetailErr {
  ok: false;
  status: number;
}

export type V3RunDetailResult = V3RunDetailOk | V3RunDetailErr;

export type V3RunCancelResult =
  | {
    ok: true;
    runId?: string;
    runStatus?: V3RunStatus;
    alreadyTerminal?: boolean;
  }
  | {
    ok: false;
    status: number;
    error: string;
  };

export async function fetchWorkflowModelChoices(cli: string, fetcher: V3Fetch = fetch): Promise<WorkflowModelChoices> {
  const response = await fetcher(`/api/cli-options/models?key=${encodeURIComponent(cli)}`);
  if (!response.ok) return { models: [], choices: [], source: 'static' };
  const body = await response.json() as Partial<WorkflowModelChoices>;
  const models = Array.isArray(body.models) ? body.models.filter((item): item is string => typeof item === 'string') : [];
  const choices = Array.isArray(body.choices) ? body.choices.filter((item): item is WorkflowModelChoice => Boolean(item) && typeof item.value === 'string' && typeof item.model === 'string') : models.map(value => ({ value, model: value, label: value }));
  return { models, choices, source: body.source === 'live' ? 'live' : 'static', ...(typeof body.detectedAt === 'number' ? { detectedAt: body.detectedAt } : {}) };
}

export async function fetchWorkflowExecutionProfiles(goal = '', fetcher: V3Fetch = fetch): Promise<{ profiles: WorkflowExecutionProfile[]; recommendations: WorkflowProfileRecommendation[] }> {
  const response = await fetcher(`/api/v3/execution-profiles?goal=${encodeURIComponent(goal)}`);
  if (!response.ok) return { profiles: [], recommendations: [] };
  return response.json() as Promise<{ profiles: WorkflowExecutionProfile[]; recommendations: WorkflowProfileRecommendation[] }>;
}

export async function saveWorkflowExecutionProfile(profile: Partial<WorkflowExecutionProfile>, fetcher: V3Fetch = fetch): Promise<WorkflowExecutionProfile> {
  const response = await fetcher('/api/v3/execution-profiles', { method: 'PUT', headers: { 'content-type': 'application/json', ...controlCsrfHeaders() }, body: JSON.stringify(profile) });
  const body = await response.json() as { profile?: WorkflowExecutionProfile; error?: string };
  if (!response.ok || !body.profile) throw new Error(body.error ?? `http_${response.status}`);
  return body.profile;
}

export async function disableWorkflowExecutionProfile(profileId: string, fetcher: V3Fetch = fetch): Promise<void> {
  const response = await fetcher(`/api/v3/execution-profiles/${encodeURIComponent(profileId)}/disable`, { method: 'POST', headers: { 'content-type': 'application/json', ...controlCsrfHeaders() }, body: '{}' });
  if (!response.ok) throw new Error(`http_${response.status}`);
}

export async function fetchV3Runs(fetcher: V3Fetch = fetch): Promise<RunSummary[]> {
  const response = await fetcher('/api/v3/runs');
  if (!response.ok) return [];
  const body = await response.json() as { runs?: unknown };
  return Array.isArray(body.runs) ? body.runs as RunSummary[] : [];
}

export async function fetchV3RunDetail(runId: string, fetcher: V3Fetch = fetch): Promise<V3RunDetailResult> {
  const response = await fetcher(`/api/v3/runs/${encodeURIComponent(runId)}`);
  if (!response.ok) return { ok: false, status: response.status };
  return { ok: true, view: await response.json() as RunView };
}

export async function cancelV3Run(runId: string, fetcher: V3Fetch = fetch): Promise<V3RunCancelResult> {
  const response = await fetcher(`/api/v3/runs/${encodeURIComponent(runId)}/cancel`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({}),
  });
  let body: Record<string, unknown> = {};
  try {
    const parsed = await response.json() as unknown;
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      body = parsed as Record<string, unknown>;
    }
  } catch {
    // The auth wall can return HTML; preserve only the HTTP status below.
  }
  if (!response.ok || body.ok === false) {
    return {
      ok: false,
      status: response.status,
      error: typeof body.error === 'string' ? body.error : `http_${response.status}`,
    };
  }
  const rawStatus = body.status;
  return {
    ok: true,
    ...(typeof body.runId === 'string' ? { runId: body.runId } : {}),
    ...(isV3RunStatus(rawStatus) ? { runStatus: rawStatus } : {}),
    ...(body.alreadyTerminal === true ? { alreadyTerminal: true } : {}),
  };
}

function isV3RunStatus(value: unknown): value is V3RunStatus {
  return value === 'running' || value === 'cancelling' || value === 'cancelled' ||
    value === 'succeeded' || value === 'failed' || value === 'blocked';
}
