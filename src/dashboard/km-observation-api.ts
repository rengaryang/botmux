import { createHash } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { jsonRes } from './http.js';
import { ObservationEventTypeSchema } from '../services/km/observation-schema.js';
import { KmMemoryProviderConfigSchema, KmPipelineProfileSchema } from '../services/km/provider-spi.js';
import type { ObservationStore } from '../services/km/observation-store.js';
import type { KmBackendRuntimeStatus } from '../services/km/memory-backend-runtime.js';
import type { KmRetentionRuntimeStatus } from '../services/km/retention-runtime.js';
import { compareMemoryBackendMigration, enqueueMemoryBackendMigrationBackfill } from '../services/km/memory-backend-migration.js';
import {
  createKnowledgeExportJob,
  executeKmFormalExport,
  getKnowledgeExportJob,
  listKnowledgeExportJobs,
  planKnowledgeExport,
  previewKmFormalExport,
  reviewKnowledgeExportJob,
  rollbackKmFormalExport,
} from '../services/km/knowledge-export-staging.js';
import {
  createKnowledgeToMemoryImportPreview,
  executeKnowledgeToMemoryImport,
} from '../services/km/knowledge-to-memory-import.js';
import type { CentralSinkRuntimeStatus } from '../services/km/central-sink-runtime.js';
import {
  approveKmProductionGatePlan,
  buildKmProductionGateHandoff,
  buildKmProductionGatePlan,
  createKmProductionGateIntent,
  expireKmProductionGatePlan,
} from '../services/km/production-gate.js';
import {
  KM_CANARY_BOT_APP_ID,
  buildKmCanaryCloseoutReport,
  renderKmCanaryCloseoutMarkdown,
} from '../services/km/canary-closeout-report.js';
import {
  activateKmCanaryRelease,
  resolveKmCanaryRuntimeAuthorization,
  rollbackKmCanaryRelease,
} from '../services/km/canary-release.js';

export interface KmObservationApiStore {
  schemaVersion(): number;
  pragmas(): { journalMode: string; foreignKeys: number; busyTimeout: number };
  counts(): { observations: number; quarantined: number; knowledge?: number; memory?: number };
  distillationBacklogStatus?(): ReturnType<ObservationStore['distillationBacklogStatus']>;
  list(filter: Parameters<ObservationStore['list']>[0]): ReturnType<ObservationStore['list']>;
  get(eventId: string): ReturnType<ObservationStore['get']>;
  getKnowledge?(knowledgeId: string): ReturnType<ObservationStore['getKnowledge']>;
  listKnowledge?(filter: Parameters<ObservationStore['listKnowledge']>[0]): ReturnType<ObservationStore['listKnowledge']>;
  listMemory?(filter: Parameters<ObservationStore['listMemory']>[0]): ReturnType<ObservationStore['listMemory']>;
  transitionMemory?(input: Parameters<ObservationStore['transitionMemory']>[0]): ReturnType<ObservationStore['transitionMemory']>;
  retrieve?(query: Parameters<ObservationStore['retrieve']>[0]): ReturnType<ObservationStore['retrieve']>;
  retrieveWithMetrics?(query: Parameters<ObservationStore['retrieveWithMetrics']>[0]): ReturnType<ObservationStore['retrieveWithMetrics']>;
  transitionKnowledge?(input: Parameters<ObservationStore['transitionKnowledge']>[0]): ReturnType<ObservationStore['transitionKnowledge']>;
  knowledgeExportDryRun?(knowledgeId: string): ReturnType<ObservationStore['knowledgeExportDryRun']>;
  listTrace?(input: Parameters<ObservationStore['listTrace']>[0]): ReturnType<ObservationStore['listTrace']>;
  listEvolution?(limit: number): ReturnType<ObservationStore['listEvolution']>;
  listEvalRuns?(limit: number): ReturnType<ObservationStore['listEvalRuns']>;
  decideProposal?(input: Parameters<ObservationStore['decideProposal']>[0]): ReturnType<ObservationStore['decideProposal']>;
  listSyncStatus?(): ReturnType<ObservationStore['listSyncStatus']>;
  listSyncOutbox?(input: Parameters<ObservationStore['listSyncOutbox']>[0]): ReturnType<ObservationStore['listSyncOutbox']>;
  configureSyncSink?(input: Parameters<ObservationStore['configureSyncSink']>[0]): ReturnType<ObservationStore['configureSyncSink']>;
  listKmProviders?(): ReturnType<ObservationStore['listKmProviders']>;
  listDistillationJobs?(limit: number): ReturnType<ObservationStore['listDistillationJobs']>;
  listRetrievalAudits?(limit: number): ReturnType<ObservationStore['listRetrievalAudits']>;
  listInjectionSnapshots?(limit: number): ReturnType<ObservationStore['listInjectionSnapshots']>;
  listPipelineProfiles?(botAppId?: string): ReturnType<ObservationStore['listPipelineProfiles']>;
  getEffectivePipelineProfile?(botAppId: string): ReturnType<ObservationStore['getEffectivePipelineProfile']>;
  putPipelineProfile?(profile: Parameters<ObservationStore['putPipelineProfile']>[0], state?: Parameters<ObservationStore['putPipelineProfile']>[1]): ReturnType<ObservationStore['putPipelineProfile']>;
  setPipelineProfileState?(input: Parameters<ObservationStore['setPipelineProfileState']>[0]): ReturnType<ObservationStore['setPipelineProfileState']>;
  listMemoryProviderConfigs?(): ReturnType<ObservationStore['listMemoryProviderConfigs']>;
  putMemoryProviderConfig?(input: Parameters<ObservationStore['putMemoryProviderConfig']>[0]): ReturnType<ObservationStore['putMemoryProviderConfig']>;
  memoryProviderConfigurationHealth?(providerId: string): ReturnType<ObservationStore['memoryProviderConfigurationHealth']>;
  listMemoryBackendOutbox?(limit: number): ReturnType<ObservationStore['listMemoryBackendOutbox']>;
  listMemoryBackendMigrations?(limit: number): ReturnType<ObservationStore['listMemoryBackendMigrations']>;
  createMemoryBackendMigration?(input: Parameters<ObservationStore['createMemoryBackendMigration']>[0]): ReturnType<ObservationStore['createMemoryBackendMigration']>;
  getMemoryBackendMigration?(migrationId: string): ReturnType<ObservationStore['getMemoryBackendMigration']>;
  transitionMemoryBackendMigration?(input: Parameters<ObservationStore['transitionMemoryBackendMigration']>[0]): ReturnType<ObservationStore['transitionMemoryBackendMigration']>;
  listMemoryForBackendMigration?(input: Parameters<ObservationStore['listMemoryForBackendMigration']>[0]): ReturnType<ObservationStore['listMemoryForBackendMigration']>;
  enqueueMemoryBackendOperation?(input: Parameters<ObservationStore['enqueueMemoryBackendOperation']>[0]): ReturnType<ObservationStore['enqueueMemoryBackendOperation']>;
  compareMemoryBackendBindings?(input: Parameters<ObservationStore['compareMemoryBackendBindings']>[0]): ReturnType<ObservationStore['compareMemoryBackendBindings']>;
  executeKmMutation?<T>(input: { actorId: string; idempotencyKey: string; route: string; requestHash: string; statusCode: number;
    action: string; targetRef: string; beforeHash?: string; afterHash?: (response: T) => string | undefined }, operation: () => T):
    { statusCode: number; response: T; replayed: boolean };
  getKmMutationReplay?<T>(input: { actorId: string; idempotencyKey: string; route: string; requestHash: string }): { statusCode: number; response: T; replayed: true } | null;
  recordKmMutation?<T>(input: { actorId: string; idempotencyKey: string; route: string; requestHash: string; statusCode: number;
    action: string; targetRef: string; response: T; beforeHash?: string; afterHash?: string }): { statusCode: number; response: T; replayed: false };
  listKmConfigAudit?(limit: number): ReturnType<ObservationStore['listKmConfigAudit']>;
  listMemoryPolicyDecisions?(limit: number): ReturnType<ObservationStore['listMemoryPolicyDecisions']>;
  retrievalQualitySummary?(): ReturnType<ObservationStore['retrievalQualitySummary']>;
  dashboardMetrics?(input?: Parameters<ObservationStore['dashboardMetrics']>[0]): ReturnType<ObservationStore['dashboardMetrics']>;
  evalEvolutionStatus?(): ReturnType<ObservationStore['evalEvolutionStatus']>;
  kmRetentionStatus?(input?: Parameters<ObservationStore['kmRetentionStatus']>[0]): ReturnType<ObservationStore['kmRetentionStatus']>;
  listKmRetentionReports?(limit: number): ReturnType<ObservationStore['listKmRetentionReports']>;
  upsertGoldenCase?(input: Parameters<ObservationStore['upsertGoldenCase']>[0]): ReturnType<ObservationStore['upsertGoldenCase']>;
  listGoldenCases?(input: Parameters<ObservationStore['listGoldenCases']>[0]): ReturnType<ObservationStore['listGoldenCases']>;
  retireGoldenCase?(input: Parameters<ObservationStore['retireGoldenCase']>[0]): ReturnType<ObservationStore['retireGoldenCase']>;
  recordShadowComparison?(input: Parameters<ObservationStore['recordShadowComparison']>[0]): ReturnType<ObservationStore['recordShadowComparison']>;
  listShadowComparisons?(input: Parameters<ObservationStore['listShadowComparisons']>[0]): ReturnType<ObservationStore['listShadowComparisons']>;
  addShadowReviewLabel?(input: Parameters<ObservationStore['addShadowReviewLabel']>[0]): ReturnType<ObservationStore['addShadowReviewLabel']>;
  listShadowReviewLabels?(limit: number): ReturnType<ObservationStore['listShadowReviewLabels']>;
  shadowReadinessReport?(input?: Parameters<ObservationStore['shadowReadinessReport']>[0]): ReturnType<ObservationStore['shadowReadinessReport']>;
  shadowReadinessReportLatest?(): ReturnType<ObservationStore['shadowReadinessReportLatest']>;
  listKnowledgeToMemoryImportJobs?(limit: number): ReturnType<ObservationStore['listKnowledgeToMemoryImportJobs']>;
  getKnowledgeToMemoryImportReport?(jobId: string): ReturnType<ObservationStore['getKnowledgeToMemoryImportReport']>;
  createKnowledgeToMemoryImportPreview?(input: Parameters<ObservationStore['createKnowledgeToMemoryImportPreview']>[0]): ReturnType<ObservationStore['createKnowledgeToMemoryImportPreview']>;
  submitKnowledgeToMemoryImportReview?(input: Parameters<ObservationStore['submitKnowledgeToMemoryImportReview']>[0]): ReturnType<ObservationStore['submitKnowledgeToMemoryImportReview']>;
  runKnowledgeToMemoryImport?(input: Parameters<ObservationStore['runKnowledgeToMemoryImport']>[0]): ReturnType<ObservationStore['runKnowledgeToMemoryImport']>;
  createProductionGatePlan?(input: Parameters<ObservationStore['createProductionGatePlan']>[0]): ReturnType<ObservationStore['createProductionGatePlan']>;
  getProductionGatePlan?(planId: string): ReturnType<ObservationStore['getProductionGatePlan']>;
  listProductionGatePlans?(input?: Parameters<ObservationStore['listProductionGatePlans']>[0]): ReturnType<ObservationStore['listProductionGatePlans']>;
  listProductionGateAudit?(planId: string, limit?: number): ReturnType<ObservationStore['listProductionGateAudit']>;
  transitionProductionGatePlan?(input: Parameters<ObservationStore['transitionProductionGatePlan']>[0]): ReturnType<ObservationStore['transitionProductionGatePlan']>;
  getProductionGateKillState?(): ReturnType<ObservationStore['getProductionGateKillState']>;
  setProductionGateKillState?(input: Parameters<ObservationStore['setProductionGateKillState']>[0]): ReturnType<ObservationStore['setProductionGateKillState']>;
  close(): void;
}

export interface KmObservationApiDeps {
  enabled: boolean;
  actorId?: string;
  dataDir?: string;
  openStore(): Promise<KmObservationApiStore>;
  backendRuntimeStatus?(): Promise<KmBackendRuntimeStatus>;
  retentionRuntimeStatus?(): Promise<KmRetentionRuntimeStatus>;
  centralSinkRuntimeStatus?(): Promise<CentralSinkRuntimeStatus>;
  centralSinkDrill?(input: { sinkId: string; drill: 'status' | 'partial-ack' | 'replay' | 'conflict'; actorId: string; idempotencyKey: string }): Promise<Record<string, unknown>>;
}

class KmApiError extends Error { constructor(readonly status: number, message: string) { super(message); } }
const MAX_BODY_BYTES = 128 * 1024;
async function readBody(req: IncomingMessage): Promise<{ body: Record<string, unknown>; raw: string }> {
  const chunks: Buffer[] = []; let bytes = 0;
  for await (const chunk of req) { const value = chunk as Buffer; bytes += value.length; if (bytes > MAX_BODY_BYTES) throw new KmApiError(413, 'request_body_too_large'); chunks.push(value); }
  const raw = Buffer.concat(chunks).toString('utf8');
  try { return { body: JSON.parse(raw) as Record<string, unknown>, raw }; }
  catch { throw new KmApiError(400, 'invalid_json'); }
}
function mutationContext(req: IncomingMessage, deps: KmObservationApiDeps, route: string, raw: string) {
  const actorId = deps.actorId?.trim(); if (!actorId) throw new KmApiError(403, 'reviewer_actor_required');
  const key = req.headers['idempotency-key']; if (typeof key !== 'string' || !key.trim()) throw new KmApiError(400, 'idempotency_key_required');
  return { actorId, idempotencyKey: key.trim(), route, requestHash: `sha256:${createHash('sha256').update(raw).digest('hex')}` };
}

function stableCanaryResponseHash(value: unknown): string {
  return `sha256:${createHash('sha256').update(JSON.stringify(value)).digest('hex')}`;
}

function positiveInteger(raw: string | null, fallback: number, max: number): number {
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) throw new KmApiError(400, 'km_observation_invalid_integer');
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
    || /^\/api\/km\/memory\/[^/]+\/state$/.test(url.pathname)
    || url.pathname === '/api/km/trace'
    || url.pathname === '/api/km/eval/runs'
    || url.pathname === '/api/km/evolution/proposals'
    || url.pathname === '/api/km/sync/sinks'
    || url.pathname === '/api/km/sync/outbox'
    || url.pathname === '/api/km/central-sink/status'
    || url.pathname === '/api/km/central-sink/sinks'
    || url.pathname === '/api/km/central-sink/drills'
    || url.pathname === '/api/km/providers'
    || url.pathname === '/api/km/exports'
    || /^\/api\/km\/exports\/[^/]+$/.test(url.pathname)
    || /^\/api\/km\/exports\/[^/]+\/review$/.test(url.pathname)
    || /^\/api\/km\/exports\/[^/]+\/(preview|execute|rollback|status)$/.test(url.pathname)
    || url.pathname === '/api/km/distillation/jobs'
    || url.pathname === '/api/km/retrieval/runs'
    || url.pathname === '/api/km/injections'
    || url.pathname === '/api/km/profiles'
    || url.pathname === '/api/km/provider-configs'
    || url.pathname === '/api/km/imports'
    || /^\/api\/km\/imports\/[^/]+$/.test(url.pathname)
    || /^\/api\/km\/imports\/[^/]+\/execute$/.test(url.pathname)
    || url.pathname === '/api/km/production-gates'
    || url.pathname === '/api/km/production-gates/kill-switch'
    || /^\/api\/km\/production-gates\/[^/]+$/.test(url.pathname)
    || /^\/api\/km\/production-gates\/[^/]+\/(approve|intent|expire|audit|handoff)$/.test(url.pathname)
    || url.pathname === '/api/km/canary-closeout'
    || url.pathname === '/api/km/canary-release/status'
    || url.pathname === '/api/km/canary-release/activate'
    || /^\/api\/km\/canary-release\/[^/]+\/rollback$/.test(url.pathname)
    || url.pathname === '/api/km/backend-runtime'
    || url.pathname === '/api/km/backend-outbox'
    || url.pathname === '/api/km/backend-migrations'
    || url.pathname === '/api/km/config-audit'
    || url.pathname === '/api/km/memory-policy-decisions'
    || url.pathname === '/api/km/dashboard-metrics'
    || url.pathname === '/api/km/retrieval/quality'
    || url.pathname === '/api/km/retention'
    || url.pathname === '/api/km/retention/reports'
    || url.pathname === '/api/km/golden-cases'
    || /^\/api\/km\/golden-cases\/[^/]+\/retire$/.test(url.pathname)
    || url.pathname === '/api/km/shadow-comparisons'
    || /^\/api\/km\/shadow-comparisons\/[^/]+\/labels$/.test(url.pathname)
    || url.pathname === '/api/km/shadow-labels'
    || url.pathname === '/api/km/shadow-readiness'
    || /^\/api\/km\/provider-configs\/[^/]+\/health$/.test(url.pathname)
    || /^\/api\/km\/backend-migrations\/[^/]+\/(backfill|compare)$/.test(url.pathname)
    || /^\/api\/km\/profiles\/[^/]+\/\d+\/state$/.test(url.pathname)
    || /^\/api\/km\/evolution\/proposals\/[^/]+\/decision$/.test(url.pathname);
  if (!kmReadPath) return false;
  if (!deps.enabled) {
    jsonRes(res, 404, { error: 'km_observation_disabled' });
    return true;
  }
  let store: KmObservationApiStore | undefined;
  try {
    store = await deps.openStore();
    const executeMutation = <T>(ctx: ReturnType<typeof mutationContext>, statusCode: number, action: string, targetRef: string,
      operation: () => T, hashes?: { beforeHash?: string; afterHash?: (response: T) => string | undefined }) => {
      if (!store!.executeKmMutation) throw new Error('km_mutation_guard_unavailable');
      const result = store!.executeKmMutation({ ...ctx, statusCode, action, targetRef, beforeHash: hashes?.beforeHash,
        afterHash: hashes?.afterHash }, operation); jsonRes(res, result.statusCode, result.response);
    };

    const providerHealth = url.pathname.match(/^\/api\/km\/provider-configs\/([^/]+)\/health$/);
    if (providerHealth) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.memoryProviderConfigurationHealth) throw new Error('km_provider_configs_unavailable');
      const { raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'provider.configuration_health', decodeURIComponent(providerHealth[1]),
        () => store!.memoryProviderConfigurationHealth!(decodeURIComponent(providerHealth[1]))); return true;
    }

    const profileState = url.pathname.match(/^\/api\/km\/profiles\/([^/]+)\/(\d+)\/state$/);
    if (profileState) {
      if (req.method !== 'PATCH') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.setPipelineProfileState) throw new Error('km_profiles_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const state = String(body.state); if (!['draft','shadow','retired'].includes(state)) throw new KmApiError(422, 'km_profile_mode_not_open');
      executeMutation(ctx, 200, 'profile.state_changed', `${decodeURIComponent(profileState[1])}@${profileState[2]}`,
        () => store!.setPipelineProfileState!({ profileId: decodeURIComponent(profileState[1]), revision: Number(profileState[2]),
          state: state as any, expectedHash: typeof body.expectedHash === 'string' ? body.expectedHash : undefined })); return true;
    }

    if (url.pathname === '/api/km/profiles' && req.method === 'POST') {
      if (!store.putPipelineProfile) throw new Error('km_profiles_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const profile = KmPipelineProfileSchema.parse(body.profile); if (!['off','shadow'].includes(profile.injectionMode)) throw new KmApiError(422, 'km_profile_mode_not_open');
      const state = String(body.state ?? 'draft'); if (!['draft','shadow'].includes(state)) throw new KmApiError(422, 'km_profile_state_not_open');
      executeMutation(ctx, 201, 'profile.created', `${profile.profileId}@${profile.revision}`, () => {
        const profileHash = store!.putPipelineProfile!(profile, state as 'draft' | 'shadow'); return { profile, state, profileHash, requestedMode: profile.injectionMode, effectiveMode: profile.injectionMode };
      }, { afterHash: response => response.profileHash }); return true;
    }

    if (url.pathname === '/api/km/provider-configs' && req.method === 'PUT') {
      if (!store.putMemoryProviderConfig) throw new Error('km_provider_configs_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw); const config = KmMemoryProviderConfigSchema.parse(body);
      executeMutation(ctx, 200, 'provider.configured', config.providerId, () => ({ providerId: config.providerId,
        configHash: store!.putMemoryProviderConfig!(config), realTransportEnabled: false }), { afterHash: response => response.configHash }); return true;
    }

    if (url.pathname === '/api/km/central-sink/sinks' && req.method === 'PUT') {
      if (!store.configureSyncSink) throw new Error('km_sync_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const sinkId = String(body.sinkId ?? '').trim();
      const endpointRef = String(body.endpointRef ?? '').trim();
      if (!sinkId || !endpointRef) throw new KmApiError(422, 'km_central_sink_config_required');
      executeMutation(ctx, 200, 'central_sink.configured', sinkId, () => ({
        sink: store!.configureSyncSink!({
          sinkId,
          protocolVersion: Number(body.protocolVersion ?? 1),
          endpointRef,
          enabled: Boolean(body.enabled),
          redactionPolicy: typeof body.redactionPolicy === 'object' && body.redactionPolicy !== null ? body.redactionPolicy as Record<string, unknown> : {},
          batchLimit: typeof body.batchLimit === 'number' ? body.batchLimit : undefined,
          timeoutMs: typeof body.timeoutMs === 'number' ? body.timeoutMs : undefined,
          maxAttempts: typeof body.maxAttempts === 'number' ? body.maxAttempts : undefined,
          credentialRef: typeof body.credentialRef === 'string' ? body.credentialRef : undefined,
          allowlist: Array.isArray(body.allowlist) ? body.allowlist.map(String) : undefined,
          payloadMaxBytes: typeof body.payloadMaxBytes === 'number' ? body.payloadMaxBytes : undefined,
          rollback: typeof body.rollback === 'object' && body.rollback !== null ? body.rollback as Record<string, unknown> : undefined,
        }),
        realTransportEnabled: false,
      }), { afterHash: response => createHash('sha256').update(JSON.stringify(response.sink)).digest('hex') }); return true;
    }

    if (url.pathname === '/api/km/central-sink/drills' && req.method === 'POST') {
      if (!deps.centralSinkDrill) throw new Error('km_central_sink_drill_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const sinkId = String(body.sinkId ?? '').trim();
      const drill = String(body.drill ?? '').trim();
      if (!sinkId || !['status','partial-ack','replay','conflict'].includes(drill)) throw new KmApiError(422, 'km_central_sink_drill_invalid');
      if (!store.getKmMutationReplay || !store.recordKmMutation) throw new Error('km_mutation_guard_unavailable');
      const replay = store.getKmMutationReplay<Record<string, unknown>>(ctx);
      if (replay) { jsonRes(res, replay.statusCode, replay.response); return true; }
      const drillResult = await deps.centralSinkDrill({ sinkId, drill: drill as any, actorId: ctx.actorId, idempotencyKey: ctx.idempotencyKey });
      const response = { accepted: true, drill, sinkId, realTransportEnabled: false, result: drillResult };
      const recorded = store.recordKmMutation({
        ...ctx,
        statusCode: 200,
        action: `central_sink.drill.${drill}`,
        targetRef: sinkId,
        response,
        afterHash: createHash('sha256').update(JSON.stringify(drillResult)).digest('hex'),
      });
      jsonRes(res, recorded.statusCode, recorded.response);
      return true;
    }

    if (url.pathname === '/api/km/imports' && req.method === 'POST') {
      if (!store.listKnowledge || !store.createKnowledgeToMemoryImportPreview) throw new Error('km_import_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 201, 'knowledge_to_memory_import.preview', String(body?.config && typeof body.config === 'object' ? (body.config as any).source ?? 'import' : 'import'), () =>
        createKnowledgeToMemoryImportPreview({
          store: {
            listKnowledge: store!.listKnowledge!.bind(store),
            createKnowledgeToMemoryImportPreview: store!.createKnowledgeToMemoryImportPreview!.bind(store),
          },
          config: (body.config ?? body) as any,
          actorId: ctx.actorId,
          idempotencyKey: ctx.idempotencyKey,
        }), { afterHash: response => response.job.configHash }); return true;
    }

    if (url.pathname === '/api/km/production-gates' && req.method === 'POST') {
      if (!store.createProductionGatePlan) throw new Error('km_production_gate_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 201, 'production_gate.plan_created', String(body.actionKind ?? 'production-gate'), () => {
        const built = buildKmProductionGatePlan({
          actionKind: String(body.actionKind ?? '') as any,
          target: typeof body.target === 'object' && body.target !== null ? body.target as Record<string, unknown> : {},
          scope: typeof body.scope === 'object' && body.scope !== null ? body.scope as Record<string, unknown> : {},
          actorId: ctx.actorId,
          riskAck: typeof body.riskAck === 'object' && body.riskAck !== null ? body.riskAck as Record<string, unknown> : {},
          ttlSeconds: typeof body.ttlSeconds === 'number' ? body.ttlSeconds : undefined,
          confirmationToken: typeof body.confirmationToken === 'string' ? body.confirmationToken : undefined,
        });
        const plan = store!.createProductionGatePlan!(built.plan);
        return {
          plan,
          confirmationToken: built.confirmationToken,
          handoff: buildKmProductionGateHandoff(plan),
          effective: false,
          sideEffectsExecuted: false,
        };
      }, { afterHash: response => response.plan.previewHash });
      return true;
    }

    const productionGateApprove = url.pathname.match(/^\/api\/km\/production-gates\/([^/]+)\/approve$/);
    if (productionGateApprove) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.getProductionGatePlan || !store.transitionProductionGatePlan) throw new Error('km_production_gate_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'production_gate.approved', decodeURIComponent(productionGateApprove[1]), () => ({
        plan: approveKmProductionGatePlan(store as any, {
          planId: decodeURIComponent(productionGateApprove[1]),
          actorId: ctx.actorId,
          approvalGrade: String(body.approvalGrade ?? '') as any,
          confirmationToken: String(body.confirmationToken ?? ''),
          previewHash: String(body.previewHash ?? ''),
          riskAck: typeof body.riskAck === 'object' && body.riskAck !== null ? body.riskAck as Record<string, unknown> : {},
        }),
        effective: false,
        sideEffectsExecuted: false,
      }), { afterHash: response => response.plan.previewHash });
      return true;
    }

    if (url.pathname === '/api/km/canary-release/activate' && req.method === 'POST') {
      if (!store.getProductionGatePlan || !store.listProductionGatePlans || !store.transitionProductionGatePlan || !store.getProductionGateKillState || !store.getEffectivePipelineProfile) {
        throw new Error('km_canary_release_unavailable');
      }
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const requestedPlan = store.getProductionGatePlan(String(body.planId ?? ''));
      const exactBotAppId = String((requestedPlan?.target as { botAppId?: unknown } | undefined)?.botAppId ?? '');
      if (requestedPlan?.actionKind !== 'prompt-canary' || store.getEffectivePipelineProfile(exactBotAppId)?.injectionMode !== 'canary') {
        throw new KmApiError(422, 'km_canary_release_profile_not_canary');
      }
      executeMutation(ctx, 200, 'canary.release_activated', String(body.planId ?? ''), () => {
        const plan = activateKmCanaryRelease(store as any, {
          planId: String(body.planId ?? ''), actorId: ctx.actorId,
          approvalGrade: String(body.approvalGrade ?? 'G2') as any,
          confirmationToken: String(body.confirmationToken ?? ''), previewHash: String(body.previewHash ?? ''),
          riskAck: typeof body.riskAck === 'object' && body.riskAck !== null ? body.riskAck as Record<string, unknown> : {},
        });
        return {
          plan,
          runtime: resolveKmCanaryRuntimeAuthorization(store as any, String((plan.target as any).botAppId ?? '')),
          restartRequired: false,
          autoFallback: 'shadow',
        };
      }, { afterHash: response => stableCanaryResponseHash(response) });
      return true;
    }

    const canaryRollback = url.pathname.match(/^\/api\/km\/canary-release\/([^/]+)\/rollback$/);
    if (canaryRollback) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.getProductionGatePlan || !store.transitionProductionGatePlan) throw new Error('km_canary_release_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'canary.release_rolled_back', decodeURIComponent(canaryRollback[1]), () => {
        const plan = rollbackKmCanaryRelease(store as any, {
          planId: decodeURIComponent(canaryRollback[1]), actorId: ctx.actorId, reason: String(body.reason ?? 'dashboard_operator_rollback'),
        });
        return { plan, effective: false, fallback: 'shadow', restartRequired: false };
      }, { afterHash: response => stableCanaryResponseHash(response) });
      return true;
    }

    const productionGateIntent = url.pathname.match(/^\/api\/km\/production-gates\/([^/]+)\/intent$/);
    if (productionGateIntent) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.getProductionGatePlan || !store.transitionProductionGatePlan || !store.getProductionGateKillState) throw new Error('km_production_gate_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'production_gate.intent_created', decodeURIComponent(productionGateIntent[1]), () =>
        createKmProductionGateIntent(store as any, {
          planId: decodeURIComponent(productionGateIntent[1]),
          actorId: ctx.actorId,
          confirmationToken: String(body.confirmationToken ?? ''),
          previewHash: String(body.previewHash ?? ''),
        }), { afterHash: response => response.intent.signedIntentHash });
      return true;
    }

    const productionGateExpire = url.pathname.match(/^\/api\/km\/production-gates\/([^/]+)\/expire$/);
    if (productionGateExpire) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.getProductionGatePlan || !store.transitionProductionGatePlan) throw new Error('km_production_gate_unavailable');
      const { raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'production_gate.expired', decodeURIComponent(productionGateExpire[1]), () => ({
        plan: expireKmProductionGatePlan(store as any, { planId: decodeURIComponent(productionGateExpire[1]), actorId: ctx.actorId }),
      }), { afterHash: response => response.plan.previewHash });
      return true;
    }

    if (url.pathname === '/api/km/production-gates/kill-switch' && req.method === 'PUT') {
      if (!store.setProductionGateKillState) throw new Error('km_production_gate_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'production_gate.kill_switch_changed', 'global', () => ({
        killSwitch: store!.setProductionGateKillState!({
          enabled: Boolean(body.enabled),
          reason: String(body.reason ?? ''),
          actorId: ctx.actorId,
        }),
        mutatesExistingRuntimeGates: false,
      }), { afterHash: response => createHash('sha256').update(JSON.stringify(response.killSwitch)).digest('hex') });
      return true;
    }

    const importExecute = url.pathname.match(/^\/api\/km\/imports\/([^/]+)\/execute$/);
    if (importExecute) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.submitKnowledgeToMemoryImportReview || !store.runKnowledgeToMemoryImport) throw new Error('km_import_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'knowledge_to_memory_import.execute', decodeURIComponent(importExecute[1]), () =>
        executeKnowledgeToMemoryImport({
          store: {
            submitKnowledgeToMemoryImportReview: store!.submitKnowledgeToMemoryImportReview!.bind(store),
            runKnowledgeToMemoryImport: store!.runKnowledgeToMemoryImport!.bind(store),
          },
          jobId: decodeURIComponent(importExecute[1]),
          actorId: ctx.actorId,
          idempotencyKey: ctx.idempotencyKey,
          approvalToken: typeof body.approvalToken === 'string' ? body.approvalToken : undefined,
          maxItems: typeof body.maxItems === 'number' ? body.maxItems : undefined,
        }), { afterHash: response => response.job.configHash }); return true;
    }

    if (url.pathname === '/api/km/backend-migrations' && req.method === 'POST') {
      if (!store.createMemoryBackendMigration) throw new Error('km_memory_migrations_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const botAppId = String(body.botAppId ?? '').trim();
      const fromProfile = (body.fromProfile ?? {}) as Record<string, unknown>;
      const toProfile = (body.toProfile ?? {}) as Record<string, unknown>;
      if (!botAppId) throw new KmApiError(422, 'km_memory_migration_bot_required');
      executeMutation(ctx, 201, 'memory_backend_migration.created', botAppId, () => ({
        migrationId: store!.createMemoryBackendMigration!({ botAppId, fromProfile, toProfile }),
        state: 'draft',
        automaticCutover: false,
      })); return true;
    }

    const migrationBackfill = url.pathname.match(/^\/api\/km\/backend-migrations\/([^/]+)\/backfill$/);
    if (migrationBackfill) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.getMemoryBackendMigration || !store.transitionMemoryBackendMigration
        || !store.listMemoryForBackendMigration || !store.enqueueMemoryBackendOperation) throw new Error('km_memory_migrations_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const toProviderId = String(body.toProviderId ?? '').trim();
      if (!toProviderId) throw new KmApiError(422, 'km_memory_migration_to_provider_required');
      executeMutation(ctx, 200, 'memory_backend_migration.backfill_dry_run', decodeURIComponent(migrationBackfill[1]), () =>
        enqueueMemoryBackendMigrationBackfill({ store: {
          getMemoryBackendMigration: store!.getMemoryBackendMigration!.bind(store),
          transitionMemoryBackendMigration: store!.transitionMemoryBackendMigration!.bind(store),
          listMemoryForBackendMigration: store!.listMemoryForBackendMigration!.bind(store),
          enqueueMemoryBackendOperation: store!.enqueueMemoryBackendOperation!.bind(store),
        }, migrationId: decodeURIComponent(migrationBackfill[1]),
          toProviderId, limit: typeof body.limit === 'number' ? body.limit : undefined }), {
        afterHash: response => `sha256:${createHash('sha256').update(JSON.stringify(response.migration)).digest('hex')}`,
      }); return true;
    }

    const migrationCompare = url.pathname.match(/^\/api\/km\/backend-migrations\/([^/]+)\/compare$/);
    if (migrationCompare) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.getMemoryBackendMigration || !store.transitionMemoryBackendMigration || !store.compareMemoryBackendBindings) throw new Error('km_memory_migrations_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const fromProviderId = String(body.fromProviderId ?? '').trim();
      const toProviderId = String(body.toProviderId ?? '').trim();
      if (!fromProviderId || !toProviderId) throw new KmApiError(422, 'km_memory_migration_compare_providers_required');
      executeMutation(ctx, 200, 'memory_backend_migration.compare_dry_run', decodeURIComponent(migrationCompare[1]), () =>
        compareMemoryBackendMigration({ store: {
          getMemoryBackendMigration: store!.getMemoryBackendMigration!.bind(store),
          transitionMemoryBackendMigration: store!.transitionMemoryBackendMigration!.bind(store),
          compareMemoryBackendBindings: store!.compareMemoryBackendBindings!.bind(store),
        }, migrationId: decodeURIComponent(migrationCompare[1]),
          fromProviderId, toProviderId, sampleLimit: typeof body.sampleLimit === 'number' ? body.sampleLimit : undefined }), {
        afterHash: response => `sha256:${createHash('sha256').update(JSON.stringify(response)).digest('hex')}`,
      }); return true;
    }

    const transition = url.pathname.match(/^\/api\/km\/knowledge\/([^/]+)\/state$/);
    if (transition) {
      if (req.method !== 'PATCH') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.transitionKnowledge) throw new Error('km_knowledge_review_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'knowledge.state_changed', decodeURIComponent(transition[1]), () => store!.transitionKnowledge!({
        knowledgeId: decodeURIComponent(transition[1]), toState: String(body.toState) as any,
        reasonCode: String(body.reasonCode ?? ''), actorId: ctx.actorId })); return true;
    }

    const memoryTransition = url.pathname.match(/^\/api\/km\/memory\/([^/]+)\/state$/);
    if (memoryTransition) {
      if (req.method !== 'PATCH') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.transitionMemory) throw new Error('km_memory_review_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'memory.state_changed', decodeURIComponent(memoryTransition[1]), () => store!.transitionMemory!({
        memoryId: decodeURIComponent(memoryTransition[1]), toState: String(body.toState) as any,
        reasonCode: String(body.reasonCode ?? ''), actorId: ctx.actorId,
      })); return true;
    }

    const dryRun = url.pathname.match(/^\/api\/km\/knowledge\/([^/]+)\/export-dry-run$/);
    if (dryRun) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      const item = store.getKnowledge ? store.getKnowledge(decodeURIComponent(dryRun[1])) : undefined;
      if (!item) throw new Error('km_knowledge_not_found');
      jsonRes(res, 200, planKnowledgeExport(deps.dataDir, item));
      return true;
    }

    if (url.pathname === '/api/km/exports' && req.method === 'POST') {
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const knowledgeId = String(body.knowledgeId ?? '').trim();
      if (!knowledgeId) throw new KmApiError(422, 'km_knowledge_id_required');
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      const item = store.getKnowledge ? store.getKnowledge(knowledgeId) : undefined;
      if (!item) throw new Error('km_knowledge_not_found');
      executeMutation(ctx, 201, 'knowledge.export_job_created', knowledgeId,
        () => createKnowledgeExportJob({ dataDir: deps.dataDir!, knowledge: item, actorId: ctx.actorId, idempotencyKey: ctx.idempotencyKey }),
        { afterHash: response => response.plan.file.contentHash });
      return true;
    }

    const exportReview = url.pathname.match(/^\/api\/km\/exports\/([^/]+)\/review$/);
    if (exportReview) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const decision = String(body.decision);
      if (decision !== 'approved' && decision !== 'rejected') throw new KmApiError(422, 'km_export_review_decision_invalid');
      executeMutation(ctx, 200, `knowledge.export_${decision}`, decodeURIComponent(exportReview[1]),
        () => reviewKnowledgeExportJob({ dataDir: deps.dataDir!, jobId: decodeURIComponent(exportReview[1]), decision,
          actorId: ctx.actorId, idempotencyKey: ctx.idempotencyKey, reasonCode: String(body.reasonCode ?? '') }),
        { afterHash: response => response.manifest?.contentHash ?? response.plan.file.contentHash });
      return true;
    }

    const exportPreview = url.pathname.match(/^\/api\/km\/exports\/([^/]+)\/preview$/);
    if (exportPreview) {
      if (req.method !== 'GET') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      jsonRes(res, 200, previewKmFormalExport({
        dataDir: deps.dataDir,
        jobId: decodeURIComponent(exportPreview[1]),
        workspaceRoot: url.searchParams.get('workspaceRoot') ?? undefined,
      }));
      return true;
    }

    const exportExecute = url.pathname.match(/^\/api\/km\/exports\/([^/]+)\/execute$/);
    if (exportExecute) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const confirmationToken = String(body.confirmationToken ?? '').trim();
      const destinationVersion = String(body.destinationVersion ?? '').trim();
      const approvalGrade = String(body.approvalGrade ?? 'G2');
      if (!confirmationToken || !destinationVersion) throw new KmApiError(422, 'km_export_execution_confirmation_required');
      if (!['G2','G3','G4'].includes(approvalGrade)) throw new KmApiError(422, 'km_export_approval_grade_invalid');
      executeMutation(ctx, 200, 'knowledge.export_execute', decodeURIComponent(exportExecute[1]), () =>
        executeKmFormalExport({
          dataDir: deps.dataDir!,
          jobId: decodeURIComponent(exportExecute[1]),
          workspaceRoot: typeof body.workspaceRoot === 'string' ? body.workspaceRoot : undefined,
          actorId: ctx.actorId,
          idempotencyKey: ctx.idempotencyKey,
          approvalGrade: approvalGrade as any,
          confirmationToken,
          expectedTargetHash: typeof body.expectedTargetHash === 'string' ? body.expectedTargetHash : null,
          destinationVersion,
          maxAttempts: typeof body.maxAttempts === 'number' ? body.maxAttempts : undefined,
        }), { afterHash: response => response.execution?.afterHash ?? response.plan.file.contentHash });
      return true;
    }

    const exportRollback = url.pathname.match(/^\/api\/km\/exports\/([^/]+)\/rollback$/);
    if (exportRollback) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      const confirmationToken = String(body.confirmationToken ?? '').trim();
      const approvalGrade = String(body.approvalGrade ?? 'G2');
      if (!confirmationToken) throw new KmApiError(422, 'km_export_rollback_confirmation_required');
      if (!['G2','G3','G4'].includes(approvalGrade)) throw new KmApiError(422, 'km_export_approval_grade_invalid');
      executeMutation(ctx, 200, 'knowledge.export_rollback', decodeURIComponent(exportRollback[1]), () =>
        rollbackKmFormalExport({
          dataDir: deps.dataDir!,
          jobId: decodeURIComponent(exportRollback[1]),
          workspaceRoot: typeof body.workspaceRoot === 'string' ? body.workspaceRoot : undefined,
          actorId: ctx.actorId,
          idempotencyKey: ctx.idempotencyKey,
          approvalGrade: approvalGrade as any,
          confirmationToken,
          expectedTargetHash: typeof body.expectedTargetHash === 'string' ? body.expectedTargetHash : undefined,
          destinationVersion: typeof body.destinationVersion === 'string' ? body.destinationVersion : undefined,
        }), { afterHash: response => response.execution?.afterHash ?? response.plan.file.contentHash });
      return true;
    }

    const proposalDecision = url.pathname.match(/^\/api\/km\/evolution\/proposals\/([^/]+)\/decision$/);
    if (proposalDecision) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.decideProposal) throw new Error('km_evolution_decision_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'evolution.decision', decodeURIComponent(proposalDecision[1]), () => store!.decideProposal!({
        proposalId: decodeURIComponent(proposalDecision[1]), decision: String(body.decision) as any, actorId: ctx.actorId,
        grade: String(body.grade) as any, scope: (body.scope ?? {}) as Record<string, unknown>,
        riskAck: (body.riskAck ?? {}) as Record<string, unknown> })); return true;
    }

    if (url.pathname === '/api/km/golden-cases' && req.method === 'POST') {
      if (!store.upsertGoldenCase) throw new Error('km_golden_cases_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 201, 'golden.created', String(body.caseId ?? body.title ?? 'golden'), () => store!.upsertGoldenCase!({
        caseId: typeof body.caseId === 'string' ? body.caseId : undefined,
        title: String(body.title ?? ''),
        queryRedacted: String(body.queryRedacted ?? ''),
        expectedClaims: Array.isArray(body.expectedClaims) ? body.expectedClaims as any : [],
        sourceRefs: Array.isArray(body.sourceRefs) ? body.sourceRefs : [],
        provenance: typeof body.provenance === 'object' && body.provenance !== null ? body.provenance as Record<string, unknown> : {},
        privacyClass: body.privacyClass === 'public-to-team' ? 'public-to-team' : 'internal',
        actorId: ctx.actorId,
      }), { afterHash: response => response.item.contentHash }); return true;
    }

    const goldenRetire = url.pathname.match(/^\/api\/km\/golden-cases\/([^/]+)\/retire$/);
    if (goldenRetire) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.retireGoldenCase) throw new Error('km_golden_cases_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'golden.retired', decodeURIComponent(goldenRetire[1]), () => store!.retireGoldenCase!({
        caseId: decodeURIComponent(goldenRetire[1]),
        revision: typeof body.revision === 'number' ? body.revision : undefined,
        actorId: ctx.actorId,
        reasonCode: String(body.reasonCode ?? 'review_retired'),
      }), { afterHash: response => response.contentHash }); return true;
    }

    if (url.pathname === '/api/km/shadow-comparisons' && req.method === 'POST') {
      if (!store.recordShadowComparison) throw new Error('km_shadow_comparisons_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 201, 'shadow_comparison.recorded', String(body.caseId ?? 'comparison'), () => store!.recordShadowComparison!({
        caseId: String(body.caseId ?? ''),
        revision: typeof body.revision === 'number' ? body.revision : undefined,
        rulesClaims: Array.isArray(body.rulesClaims) ? body.rulesClaims as any : [],
        piClaims: Array.isArray(body.piClaims) ? body.piClaims as any : [],
        latency: typeof body.latency === 'object' && body.latency !== null ? body.latency as Record<string, unknown> : {},
        cost: typeof body.cost === 'object' && body.cost !== null ? body.cost as Record<string, unknown> : {},
      }), { afterHash: response => response.item.comparisonId }); return true;
    }

    const comparisonLabels = url.pathname.match(/^\/api\/km\/shadow-comparisons\/([^/]+)\/labels$/);
    if (comparisonLabels) {
      if (req.method !== 'POST') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }
      if (!store.addShadowReviewLabel) throw new Error('km_shadow_labels_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 201, 'shadow_label.created', decodeURIComponent(comparisonLabels[1]), () => store!.addShadowReviewLabel!({
        comparisonId: decodeURIComponent(comparisonLabels[1]),
        claimKey: String(body.claimKey ?? ''),
        extractor: String(body.extractor ?? '') as any,
        label: String(body.label ?? '') as any,
        actorId: ctx.actorId,
        reasonCode: String(body.reasonCode ?? 'manual_review'),
      })); return true;
    }

    if (url.pathname === '/api/km/shadow-readiness' && req.method === 'POST') {
      if (!store.shadowReadinessReport) throw new Error('km_shadow_readiness_unavailable');
      const { body, raw } = await readBody(req); const ctx = mutationContext(req, deps, url.pathname, raw);
      executeMutation(ctx, 200, 'shadow_readiness.reported', 'shadow-quality', () => store!.shadowReadinessReport!({
        thresholds: typeof body.thresholds === 'object' && body.thresholds !== null ? body.thresholds as Record<string, number> : undefined,
      }), { afterHash: response => response.windowHash }); return true;
    }

    if (req.method !== 'GET') { jsonRes(res, 405, { error: 'method_not_allowed' }); return true; }

    if (url.pathname === '/api/km/trace') {
      if (!store.listTrace) throw new Error('km_trace_unavailable');
      const type = url.searchParams.get('type')?.trim(); const id = url.searchParams.get('id')?.trim();
      if (!type || !id) { jsonRes(res, 400, { error: 'trace_type_and_id_required' }); return true; }
      jsonRes(res, 200, { items: store.listTrace({ type, id, limit: positiveInteger(url.searchParams.get('limit'), 100, 500) }) });
      return true;
    }

    if (url.pathname === '/api/km/retrieval/quality') {
      if (!store.retrievalQualitySummary) throw new Error('km_retrieval_quality_unavailable');
      jsonRes(res, 200, store.retrievalQualitySummary()); return true;
    }
    if (url.pathname === '/api/km/dashboard-metrics') {
      if (!store.dashboardMetrics) throw new Error('km_dashboard_metrics_unavailable');
      jsonRes(res, 200, store.dashboardMetrics({ rankingLimit: positiveInteger(url.searchParams.get('rankingLimit'), 10, 50) }));
      return true;
    }
    if (url.pathname === '/api/km/retention') {
      if (deps.retentionRuntimeStatus) jsonRes(res, 200, await deps.retentionRuntimeStatus());
      else {
        if (!store.kmRetentionStatus) throw new Error('km_retention_unavailable');
        jsonRes(res, 200, store.kmRetentionStatus({ enabled: false }));
      }
      return true;
    }
    if (url.pathname === '/api/km/retention/reports') {
      if (!store.listKmRetentionReports) throw new Error('km_retention_unavailable');
      jsonRes(res, 200, { items: store.listKmRetentionReports(positiveInteger(url.searchParams.get('limit'), 30, 100)) }); return true;
    }
    if (url.pathname === '/api/km/golden-cases') {
      if (!store.listGoldenCases) throw new Error('km_golden_cases_unavailable');
      const state = url.searchParams.get('state') ?? undefined;
      jsonRes(res, 200, { items: store.listGoldenCases({ limit: positiveInteger(url.searchParams.get('limit'), 50, 100),
        ...(state ? { state: state as any } : {}) }) }); return true;
    }
    if (url.pathname === '/api/km/shadow-comparisons') {
      if (!store.listShadowComparisons) throw new Error('km_shadow_comparisons_unavailable');
      const caseId = url.searchParams.get('caseId') ?? undefined;
      jsonRes(res, 200, { items: store.listShadowComparisons({ limit: positiveInteger(url.searchParams.get('limit'), 50, 100),
        ...(caseId ? { caseId } : {}) }) }); return true;
    }
    if (url.pathname === '/api/km/shadow-labels') {
      if (!store.listShadowReviewLabels) throw new Error('km_shadow_labels_unavailable');
      jsonRes(res, 200, { items: store.listShadowReviewLabels(positiveInteger(url.searchParams.get('limit'), 50, 100)) }); return true;
    }
    if (url.pathname === '/api/km/shadow-readiness') {
      if (!store.shadowReadinessReportLatest) throw new Error('km_shadow_readiness_unavailable');
      jsonRes(res, 200, store.shadowReadinessReportLatest() ?? { ready: false, reasonCodes: ['no_readiness_report'] }); return true;
    }
    if (url.pathname === '/api/km/config-audit') {
      if (!store.listKmConfigAudit) throw new Error('km_config_audit_unavailable');
      jsonRes(res, 200, { items: store.listKmConfigAudit(positiveInteger(url.searchParams.get('limit'), 50, 100)) }); return true;
    }
    if (url.pathname === '/api/km/memory-policy-decisions') {
      if (!store.listMemoryPolicyDecisions) throw new Error('km_memory_policy_decisions_unavailable');
      jsonRes(res, 200, { items: store.listMemoryPolicyDecisions(positiveInteger(url.searchParams.get('limit'), 50, 100)) }); return true;
    }
    if (url.pathname === '/api/km/profiles') {
      if (!store.listPipelineProfiles) throw new Error('km_profiles_unavailable');
      jsonRes(res, 200, { items: store.listPipelineProfiles(url.searchParams.get('botAppId') ?? undefined) }); return true;
    }
    if (url.pathname === '/api/km/provider-configs') {
      if (!store.listMemoryProviderConfigs) throw new Error('km_provider_configs_unavailable');
      jsonRes(res, 200, { items: store.listMemoryProviderConfigs() }); return true;
    }
    if (url.pathname === '/api/km/imports') {
      if (!store.listKnowledgeToMemoryImportJobs) throw new Error('km_import_unavailable');
      jsonRes(res, 200, { items: store.listKnowledgeToMemoryImportJobs(positiveInteger(url.searchParams.get('limit'), 20, 100)) }); return true;
    }
    if (url.pathname === '/api/km/production-gates') {
      if (!store.listProductionGatePlans || !store.getProductionGateKillState) throw new Error('km_production_gate_unavailable');
      const actionKind = url.searchParams.get('actionKind') ?? undefined;
      const state = url.searchParams.get('state') ?? undefined;
      jsonRes(res, 200, {
        items: store.listProductionGatePlans({
          limit: positiveInteger(url.searchParams.get('limit'), 20, 100),
          ...(actionKind ? { actionKind: actionKind as any } : {}),
          ...(state ? { state: state as any } : {}),
        }),
        killSwitch: store.getProductionGateKillState(),
      }); return true;
    }
    if (url.pathname === '/api/km/production-gates/kill-switch') {
      if (!store.getProductionGateKillState) throw new Error('km_production_gate_unavailable');
      jsonRes(res, 200, store.getProductionGateKillState()); return true;
    }
    if (url.pathname === '/api/km/canary-release/status') {
      if (!store.listProductionGatePlans || !store.getProductionGateKillState) throw new Error('km_canary_release_unavailable');
      const botAppId = String(url.searchParams.get('botAppId') ?? '').trim();
      if (!botAppId) throw new KmApiError(400, 'km_canary_release_bot_required');
      const runtime = resolveKmCanaryRuntimeAuthorization(store as any, botAppId);
      const legacyEnvironmentActive = !runtime.active
        && ['1', 'true', 'yes'].includes(process.env.BOTMUX_KM_LIVE_INJECTION_ENABLED?.trim().toLowerCase() ?? '')
        && ['1', 'true', 'yes'].includes(process.env.BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED?.trim().toLowerCase() ?? '')
        && (process.env.BOTMUX_KM_CANARY_BOT_APP_IDS ?? '').split(/[,\s]+/u).includes(botAppId)
        && store.listProductionGatePlans({ limit: 200, actionKind: 'prompt-canary' }).some(plan => {
          const target = plan.target as { botAppId?: unknown; window?: { start?: unknown; end?: unknown } };
          const now = Date.now();
          return plan.state === 'approved' && target.botAppId === botAppId
            && typeof target.window?.start === 'string' && typeof target.window?.end === 'string'
            && Date.parse(plan.expiresAt) > now && Date.parse(target.window.start) <= now && Date.parse(target.window.end) > now;
        });
      jsonRes(res, 200, {
        runtime,
        legacyEnvironmentActive,
        restartRequired: legacyEnvironmentActive,
        autoFallback: 'shadow',
      });
      return true;
    }

    if (url.pathname === '/api/km/canary-closeout') {
      if (!store.listGoldenCases || !store.listShadowComparisons || !store.shadowReadinessReportLatest
        || !store.listRetrievalAudits || !store.listInjectionSnapshots || !store.listProductionGatePlans
        || !store.getProductionGateKillState) throw new Error('km_canary_closeout_unavailable');
      const report = buildKmCanaryCloseoutReport({
        store: store as any,
        botAppId: url.searchParams.get('botAppId') ?? KM_CANARY_BOT_APP_ID,
        now: url.searchParams.get('now') ?? undefined,
        windowHours: url.searchParams.get('windowHours') ? positiveInteger(url.searchParams.get('windowHours'), 24, 168) : undefined,
        reportLimit: url.searchParams.get('limit') ? positiveInteger(url.searchParams.get('limit'), 100, 500) : undefined,
      });
      if (url.searchParams.get('format') === 'markdown') {
        res.writeHead(200, { 'Content-Type': 'text/markdown; charset=utf-8' });
        res.end(renderKmCanaryCloseoutMarkdown(report));
      } else {
        jsonRes(res, 200, report);
      }
      return true;
    }
    const productionGateAudit = url.pathname.match(/^\/api\/km\/production-gates\/([^/]+)\/audit$/);
    if (productionGateAudit) {
      if (!store.listProductionGateAudit) throw new Error('km_production_gate_unavailable');
      jsonRes(res, 200, { items: store.listProductionGateAudit(decodeURIComponent(productionGateAudit[1]), positiveInteger(url.searchParams.get('limit'), 100, 500)) }); return true;
    }
    const productionGateHandoff = url.pathname.match(/^\/api\/km\/production-gates\/([^/]+)\/handoff$/);
    if (productionGateHandoff) {
      if (!store.getProductionGatePlan) throw new Error('km_production_gate_unavailable');
      const plan = store.getProductionGatePlan(decodeURIComponent(productionGateHandoff[1]));
      if (!plan) throw new Error('km_production_gate_plan_not_found');
      jsonRes(res, 200, buildKmProductionGateHandoff(plan)); return true;
    }
    const productionGateStatus = url.pathname.match(/^\/api\/km\/production-gates\/([^/]+)$/);
    if (productionGateStatus) {
      if (!store.getProductionGatePlan) throw new Error('km_production_gate_unavailable');
      const plan = store.getProductionGatePlan(decodeURIComponent(productionGateStatus[1]));
      if (!plan) throw new Error('km_production_gate_plan_not_found');
      jsonRes(res, 200, plan); return true;
    }
    const importStatus = url.pathname.match(/^\/api\/km\/imports\/([^/]+)$/);
    if (importStatus) {
      if (!store.getKnowledgeToMemoryImportReport) throw new Error('km_import_unavailable');
      const report = store.getKnowledgeToMemoryImportReport(decodeURIComponent(importStatus[1]));
      if (!report) throw new Error('km_import_job_not_found');
      jsonRes(res, 200, report); return true;
    }
    if (url.pathname === '/api/km/backend-runtime') {
      if (!deps.backendRuntimeStatus) throw new Error('km_backend_runtime_unavailable');
      jsonRes(res, 200, await deps.backendRuntimeStatus()); return true;
    }
    if (url.pathname === '/api/km/backend-outbox') {
      if (!store.listMemoryBackendOutbox) throw new Error('km_backend_outbox_unavailable');
      jsonRes(res, 200, { items: store.listMemoryBackendOutbox(positiveInteger(url.searchParams.get('limit'), 50, 100)) }); return true;
    }
    if (url.pathname === '/api/km/backend-migrations') {
      if (!store.listMemoryBackendMigrations) throw new Error('km_memory_migrations_unavailable');
      jsonRes(res, 200, { items: store.listMemoryBackendMigrations(positiveInteger(url.searchParams.get('limit'), 50, 100)) }); return true;
    }
    if (url.pathname === '/api/km/providers') {
      if (!store.listKmProviders) throw new Error('km_providers_unavailable');
      jsonRes(res, 200, { items: store.listKmProviders() }); return true;
    }
    if (url.pathname === '/api/km/exports') {
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      jsonRes(res, 200, { items: listKnowledgeExportJobs(deps.dataDir) }); return true;
    }
    const exportStatus = url.pathname.match(/^\/api\/km\/exports\/([^/]+)$/);
    if (exportStatus) {
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      const job = getKnowledgeExportJob(deps.dataDir, decodeURIComponent(exportStatus[1]));
      if (!job) throw new Error('km_export_job_not_found');
      jsonRes(res, 200, job); return true;
    }
    const exportStatusAlias = url.pathname.match(/^\/api\/km\/exports\/([^/]+)\/status$/);
    if (exportStatusAlias) {
      if (!deps.dataDir) throw new Error('km_export_data_dir_required');
      const job = getKnowledgeExportJob(deps.dataDir, decodeURIComponent(exportStatusAlias[1]));
      if (!job) throw new Error('km_export_job_not_found');
      jsonRes(res, 200, job); return true;
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

    if (url.pathname === '/api/km/central-sink/sinks') {
      if (!store.listSyncStatus) throw new Error('km_sync_unavailable');
      jsonRes(res, 200, { items: store.listSyncStatus() });
      return true;
    }

    if (url.pathname === '/api/km/sync/outbox') {
      if (!store.listSyncOutbox) throw new Error('km_sync_unavailable');
      const sinkId = url.searchParams.get('sinkId') ?? undefined;
      jsonRes(res, 200, { items: store.listSyncOutbox({ ...(sinkId ? { sinkId } : {}), limit: positiveInteger(url.searchParams.get('limit'), 50, 100) }) });
      return true;
    }

    if (url.pathname === '/api/km/central-sink/status') {
      if (!deps.centralSinkRuntimeStatus) throw new Error('km_central_sink_unavailable');
      jsonRes(res, 200, await deps.centralSinkRuntimeStatus());
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
        backlog: store.distillationBacklogStatus?.() ?? { queued: 0, retryWait: 0, oldestAgeMs: 0, claimed: 0 },
        evalEvolution: store.evalEvolutionStatus?.() ?? { evalRuns: 0, failingEvalRuns: 0, reviewPendingProposals: 0 },
        capabilities: { requestedModes: ['off', 'shadow'], effectiveModes: ['off', 'shadow'], livePromptInjection: false, realMemoryTransport: false },
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
      if (!store.retrieve && !store.retrieveWithMetrics) throw new Error('km_retrieval_unavailable');
      const limit = positiveInteger(url.searchParams.get('limit'), 20, 100);
      const text = url.searchParams.get('q') ?? '';
      const subject = url.searchParams.get('subject') ?? undefined;
      const scopes = url.searchParams.getAll('scope') as any[];
      const targetLayers = url.searchParams.getAll('targetLayer') as any[];
      const query = { text, limit, ...(subject ? { subject } : {}), ...(scopes.length ? { scopes } : {}), ...(targetLayers.length ? { targetLayers } : {}) };
      if (store.retrieveWithMetrics) jsonRes(res, 200, store.retrieveWithMetrics(query));
      else jsonRes(res, 200, { items: store.retrieve!(query) });
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
    const message = error instanceof Error ? error.message : 'km_observation_invalid_request';
    const zodError = error instanceof Error && error.name === 'ZodError';
    const status = error instanceof KmApiError ? error.status : message === 'km_idempotency_conflict' ? 409
      : message.includes('not_found') ? 404 : zodError || message.startsWith('km_') ? 422 : 500;
    jsonRes(res, status, { error: status >= 500 ? 'km_internal_error' : zodError ? 'km_request_schema_invalid' : message });
  } finally {
    store?.close();
  }
  return true;
}
