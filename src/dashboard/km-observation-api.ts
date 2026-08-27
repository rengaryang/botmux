import type { IncomingMessage, ServerResponse } from 'node:http';
import { jsonRes } from './http.js';
import { ObservationEventTypeSchema } from '../services/km/observation-schema.js';
import type { ObservationStore } from '../services/km/observation-store.js';

export interface KmObservationApiStore {
  schemaVersion(): number;
  pragmas(): { journalMode: string; foreignKeys: number; busyTimeout: number };
  counts(): { observations: number; quarantined: number; knowledge?: number; memory?: number };
  list(filter: Parameters<ObservationStore['list']>[0]): ReturnType<ObservationStore['list']>;
  get(eventId: string): ReturnType<ObservationStore['get']>;
  listKnowledge?(filter: Parameters<ObservationStore['listKnowledge']>[0]): ReturnType<ObservationStore['listKnowledge']>;
  listMemory?(filter: Parameters<ObservationStore['listMemory']>[0]): ReturnType<ObservationStore['listMemory']>;
  retrieve?(query: Parameters<ObservationStore['retrieve']>[0]): ReturnType<ObservationStore['retrieve']>;
  transitionKnowledge?(input: Parameters<ObservationStore['transitionKnowledge']>[0]): ReturnType<ObservationStore['transitionKnowledge']>;
  knowledgeExportDryRun?(knowledgeId: string): ReturnType<ObservationStore['knowledgeExportDryRun']>;
  listTrace?(input: Parameters<ObservationStore['listTrace']>[0]): ReturnType<ObservationStore['listTrace']>;
  listEvolution?(limit: number): ReturnType<ObservationStore['listEvolution']>;
  listEvalRuns?(limit: number): ReturnType<ObservationStore['listEvalRuns']>;
  decideProposal?(input: Parameters<ObservationStore['decideProposal']>[0]): ReturnType<ObservationStore['decideProposal']>;
  listSyncStatus?(): ReturnType<ObservationStore['listSyncStatus']>;
  listKmProviders?(): ReturnType<ObservationStore['listKmProviders']>;
  listDistillationJobs?(limit: number): ReturnType<ObservationStore['listDistillationJobs']>;
  listRetrievalAudits?(limit: number): ReturnType<ObservationStore['listRetrievalAudits']>;
  listInjectionSnapshots?(limit: number): ReturnType<ObservationStore['listInjectionSnapshots']>;
  close(): void;
}

export interface KmObservationApiDeps {
  enabled: boolean;
  openStore(): Promise<KmObservationApiStore>;
}

function positiveInteger(raw: string | null, fallback: number, max: number): number {
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) throw new Error('km_observation_invalid_integer');
  return Math.min(value, max);
}

/** Read-only KM observation API. Authentication is enforced by the Dashboard's
 * outer request gate before this handler is reached. */
export async function handleKmObservationApi(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: KmObservationApiDeps,
): Promise<boolean> {
  const kmReadPath = url.pathname === '/api/km/health'
    || url.pathname.startsWith('/api/km/observations')
    || url.pathname === '/api/km/knowledge'
    || url.pathname === '/api/km/memory'
    || url.pathname === '/api/km/retrieve'
    || /^\/api\/km\/knowledge\/[^/]+\/(state|export-dry-run)$/.test(url.pathname)
    || url.pathname === '/api/km/trace'
    || url.pathname === '/api/km/eval/runs'
    || url.pathname === '/api/km/evolution/proposals'
    || url.pathname === '/api/km/sync/sinks'
    || url.pathname === '/api/km/providers'
    || url.pathname === '/api/km/distillation/jobs'
    || url.pathname === '/api/km/retrieval/runs'
    || url.pathname === '/api/km/injections'
    || /^\/api\/km\/evolution\/proposals\/[^/]+\/decision$/.test(url.pathname);
  if (!kmReadPath) return false;
  if (!deps.enabled) {
    jsonRes(res, 404, { error: 'km_observation_disabled' });
    return true;
  }
  let store: KmObservationApiStore | undefined;
  try {
    store = await deps.openStore();
    const transition = url.pathname.match(/^\/api\/km\/knowledge\/([^/]+)\/state$/);
    if (transition) {
      if (req.method !== 'PATCH') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.transitionKnowledge) throw new Error('km_knowledge_review_unavailable');
      const idempotencyKey = req.headers['idempotency-key'];
      if (typeof idempotencyKey !== 'string' || !idempotencyKey.trim()) { jsonRes(res, 400, { error: 'idempotency_key_required' }); return true; }
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, unknown>;
      const actorId = req.headers['x-km-actor-id'];
      if (typeof actorId !== 'string' || !actorId.trim()) { jsonRes(res, 403, { error: 'reviewer_actor_required' }); return true; }
      const item = store.transitionKnowledge({ knowledgeId: decodeURIComponent(transition[1]),
        toState: String(body.toState) as any, reasonCode: String(body.reasonCode ?? ''), actorId });
      jsonRes(res, 200, item);
      return true;
    }

    const dryRun = url.pathname.match(/^\/api\/km\/knowledge\/([^/]+)\/export-dry-run$/);
    if (dryRun) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.knowledgeExportDryRun) throw new Error('km_knowledge_export_unavailable');
      jsonRes(res, 200, store.knowledgeExportDryRun(decodeURIComponent(dryRun[1])));
      return true;
    }

    const proposalDecision = url.pathname.match(/^\/api\/km\/evolution\/proposals\/([^/]+)\/decision$/);
    if (proposalDecision) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.decideProposal) throw new Error('km_evolution_decision_unavailable');
      const chunks: Buffer[] = []; for await (const chunk of req) chunks.push(chunk as Buffer);
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, unknown>;
      const actorId = req.headers['x-km-actor-id'];
      if (typeof actorId !== 'string' || !actorId.trim()) { jsonRes(res, 403, { error: 'reviewer_actor_required' }); return true; }
      jsonRes(res, 200, store.decideProposal({ proposalId: decodeURIComponent(proposalDecision[1]),
        decision: String(body.decision) as any, actorId, grade: String(body.grade) as any,
        scope: (body.scope ?? {}) as Record<string, unknown>, riskAck: (body.riskAck ?? {}) as Record<string, unknown> }));
      return true;
    }

    if (req.method !== 'GET') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }

    if (url.pathname === '/api/km/trace') {
      if (!store.listTrace) throw new Error('km_trace_unavailable');
      const type = url.searchParams.get('type')?.trim(); const id = url.searchParams.get('id')?.trim();
      if (!type || !id) { jsonRes(res, 400, { error: 'trace_type_and_id_required' }); return true; }
      jsonRes(res, 200, { items: store.listTrace({ type, id, limit: positiveInteger(url.searchParams.get('limit'), 100, 500) }) });
      return true;
    }

    if (url.pathname === '/api/km/providers') {
      if (!store.listKmProviders) throw new Error('km_providers_unavailable');
      jsonRes(res, 200, { items: store.listKmProviders() }); return true;
    }
    if (url.pathname === '/api/km/distillation/jobs') {
      if (!store.listDistillationJobs) throw new Error('km_distillation_jobs_unavailable');
      jsonRes(res, 200, { items: store.listDistillationJobs(positiveInteger(url.searchParams.get('limit'), 50, 100)) }); return true;
    }
    if (url.pathname === '/api/km/retrieval/runs') {
      if (!store.listRetrievalAudits) throw new Error('km_retrieval_runs_unavailable');
      jsonRes(res, 200, { items: store.listRetrievalAudits(positiveInteger(url.searchParams.get('limit'), 50, 100)) }); return true;
    }
    if (url.pathname === '/api/km/injections') {
      if (!store.listInjectionSnapshots) throw new Error('km_injections_unavailable');
      jsonRes(res, 200, { items: store.listInjectionSnapshots(positiveInteger(url.searchParams.get('limit'), 50, 100)) }); return true;
    }

    if (url.pathname === '/api/km/sync/sinks') {
      if (!store.listSyncStatus) throw new Error('km_sync_unavailable');
      jsonRes(res, 200, { items: store.listSyncStatus() });
      return true;
    }

    if (url.pathname === '/api/km/eval/runs') {
      if (!store.listEvalRuns) throw new Error('km_eval_unavailable');
      jsonRes(res, 200, { items: store.listEvalRuns(positiveInteger(url.searchParams.get('limit'), 50, 100)) });
      return true;
    }

    if (url.pathname === '/api/km/evolution/proposals') {
      if (!store.listEvolution) throw new Error('km_evolution_unavailable');
      jsonRes(res, 200, { items: store.listEvolution(positiveInteger(url.searchParams.get('limit'), 50, 100)) });
      return true;
    }

    if (url.pathname === '/api/km/health') {
      jsonRes(res, 200, {
        enabled: true,
        schemaVersion: store.schemaVersion(),
        pragmas: store.pragmas(),
        counts: store.counts(),
      });
      return true;
    }

    if (url.pathname === '/api/km/knowledge') {
      if (!store.listKnowledge) throw new Error('km_knowledge_unavailable');
      const limit = positiveInteger(url.searchParams.get('limit'), 50, 100);
      const state = url.searchParams.get('state') ?? undefined;
      const targetLayer = url.searchParams.get('targetLayer') ?? undefined;
      jsonRes(res, 200, { items: store.listKnowledge({ limit, ...(state ? { state: state as any } : {}), ...(targetLayer ? { targetLayer: targetLayer as any } : {}) }) });
      return true;
    }

    if (url.pathname === '/api/km/memory') {
      if (!store.listMemory) throw new Error('km_memory_unavailable');
      const limit = positiveInteger(url.searchParams.get('limit'), 50, 100);
      const state = url.searchParams.get('state') ?? undefined;
      const scope = url.searchParams.get('scope') ?? undefined;
      const subject = url.searchParams.get('subject') ?? undefined;
      jsonRes(res, 200, { items: store.listMemory({ limit, ...(state ? { state: state as any } : {}), ...(scope ? { scope: scope as any } : {}), ...(subject ? { subject } : {}) }) });
      return true;
    }

    if (url.pathname === '/api/km/retrieve') {
      if (!store.retrieve) throw new Error('km_retrieval_unavailable');
      const limit = positiveInteger(url.searchParams.get('limit'), 20, 100);
      const text = url.searchParams.get('q') ?? '';
      const subject = url.searchParams.get('subject') ?? undefined;
      const scopes = url.searchParams.getAll('scope') as any[];
      const targetLayers = url.searchParams.getAll('targetLayer') as any[];
      jsonRes(res, 200, { items: store.retrieve({ text, limit, ...(subject ? { subject } : {}), ...(scopes.length ? { scopes } : {}), ...(targetLayers.length ? { targetLayers } : {}) }) });
      return true;
    }

    if (url.pathname === '/api/km/observations') {
      const limit = positiveInteger(url.searchParams.get('limit'), 50, 100);
      const beforeRaw = url.searchParams.get('before');
      const beforeLocalSeq = beforeRaw ? positiveInteger(beforeRaw, 1, Number.MAX_SAFE_INTEGER) : undefined;
      const typeRaw = url.searchParams.get('type');
      const eventType = typeRaw ? ObservationEventTypeSchema.parse(typeRaw) : undefined;
      const filter = {
        limit,
        ...(beforeLocalSeq !== undefined ? { beforeLocalSeq } : {}),
        ...(eventType !== undefined ? { eventType } : {}),
      };
      jsonRes(res, 200, { items: store.list(filter) });
      return true;
    }

    const match = url.pathname.match(/^\/api\/km\/observations\/([^/]+)$/);
    if (match) {
      let eventId: string;
      try { eventId = decodeURIComponent(match[1]); }
      catch { jsonRes(res, 400, { error: 'invalid_event_id' }); return true; }
      const event = store.get(eventId);
      if (!event) jsonRes(res, 404, { error: 'observation_not_found' });
      else jsonRes(res, 200, event);
      return true;
    }

    jsonRes(res, 404, { error: 'not_found' });
  } catch (error) {
    jsonRes(res, 400, { error: error instanceof Error ? error.message : 'km_observation_invalid_request' });
  } finally {
    store?.close();
  }
  return true;
}
