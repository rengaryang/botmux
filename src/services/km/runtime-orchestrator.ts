import { createHash } from 'node:crypto';
import { lstatSync, openSync, closeSync, readSync } from 'node:fs';
import type { CliId } from '../../adapters/cli/types.js';
import { resolveSessionTranscriptPath } from '../transcript-resolver.js';
import type { ObservationEvent } from './observation-schema.js';
import { ObservationStore } from './observation-store.js';
import type { KmPipelineProfile, EvidenceWindow } from './provider-spi.js';
import { extractKnowledgeCandidates } from './knowledge-extractor.js';
import { PiDistillationExecutor } from './pi-distillation-executor.js';
import { runCliDistillation } from './cli-distillation-runner.js';
import { decideSafeMemoryActivation } from './safe-memory-policy.js';
import { planPromptMemory, retrievalQueryHash, type PromptMemoryCandidate } from './prompt-memory.js';

function envOn(name: string, env: NodeJS.ProcessEnv = process.env): boolean {
  return ['1', 'true', 'yes'].includes(env[name]?.trim().toLowerCase() ?? '');
}
export function isKmAutoDistillationEnabled(env = process.env): boolean { return envOn('BOTMUX_KM_AUTO_DISTILLATION_ENABLED', env); }
export function isKmPiShadowEnabled(env = process.env): boolean { return envOn('BOTMUX_KM_PI_SHADOW_ENABLED', env); }
export function isKmRetrievalShadowEnabled(env = process.env): boolean { return envOn('BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED', env); }

export function defaultShadowProfile(botAppId: string, cliId = 'pi', model?: string): KmPipelineProfile {
  return {
    schemaVersion: 1, profileId: `bot-${botAppId}`, revision: 1, botAppId,
    sourceProvider: 'observation-source-v1', windowProvider: 'bounded-transcript-window-v1',
    primaryExtractor: 'builtin.rules-v1', shadowExtractors: isKmPiShadowEnabled() ? [`botmux-cli:${cliId}:${model ?? 'default'}`] : [],
    knowledgeRouter: 'builtin.layer-router-v1', memoryPolicy: 'safe-auto-activation-v1',
    memoryBackends: { writePolicy: 'primary-mirror', primary: 'sqlite', mirrors: ['mem0', 'hindsight', 'openviking'] },
    injectionMode: 'shadow', budgets: { sourceBytes: 262_144, sourceTokens: 32_000, outputClaims: 20, promptTokens: 1_800 },
  };
}

/** Triggered only after the observation itself was durably accepted/deduped. */
export async function enqueueAutomaticDistillation(input: { dataDir: string; event: ObservationEvent; cliId?: string; model?: string; cliSessionId?: string; cwd?: string }): Promise<void> {
  if (!isKmAutoDistillationEnabled() || process.env.BOTMUX_KM_WORKLOAD === 'distillation') return;
  const store = await ObservationStore.open(input.dataDir);
  try { store.createDistillationJob({ sourceEventId: input.event.eventId, profile: defaultShadowProfile(input.event.identity.botAppId, input.cliId, input.model),
    evidenceContext: { cliId: input.cliId, model: input.model, cliSessionId: input.cliSessionId, cwd: input.cwd } }); }
  finally { store.close(); }
}

/** Metadata/artifact bounded window. Transcript text is supplied only by an explicitly wired resolver. */
export function boundedEvidenceWindow(event: ObservationEvent, transcriptText?: string): EvidenceWindow {
  const source = transcriptText ?? JSON.stringify({ eventType: event.eventType, payload: event.payload, contentRef: event.content.ref ?? null });
  const bytes = Buffer.byteLength(source);
  const text = bytes <= 262_144 ? source : Buffer.from(source).subarray(0, 262_144).toString('utf8');
  return { status: bytes <= 262_144 ? 'resolved' : 'partial', contentHash: `sha256:${createHash('sha256').update(text).digest('hex')}`,
    segments: [{ id: 'event', text, start: 0, end: text.length }], warnings: bytes > 262_144 ? ['window_truncated'] : [] };
}

export function resolveBoundedTranscriptWindow(input: {
  event: ObservationEvent; cliId?: string; cliSessionId?: string; cwd?: string; larkAppId?: string; maxBytes?: number;
}): EvidenceWindow {
  if (!input.cliId) return boundedEvidenceWindow(input.event);
  const resolved = resolveSessionTranscriptPath({ cliId: input.cliId as CliId, sessionId: input.event.identity.sessionId,
    cliSessionId: input.cliSessionId, cwd: input.cwd, larkAppId: input.larkAppId, fresh: true });
  if (!resolved) return { status: 'missing', segments: [], warnings: ['transcript_not_found'] };
  try {
    const stat = lstatSync(resolved.path);
    if (!stat.isFile() || stat.isSymbolicLink()) return { status: 'unsupported', segments: [], warnings: ['transcript_not_regular_file'] };
    const max = Math.max(1_024, Math.min(input.maxBytes ?? 262_144, 1_048_576));
    const bytes = Math.min(stat.size, max); const start = Math.max(0, stat.size - bytes);
    const buffer = Buffer.alloc(bytes); const fd = openSync(resolved.path, 'r');
    try { readSync(fd, buffer, 0, bytes, start); } finally { closeSync(fd); }
    const text = buffer.toString('utf8');
    return { status: start > 0 ? 'partial' : 'resolved', contentHash: `sha256:${createHash('sha256').update(text).digest('hex')}`,
      segments: [{ id: `${resolved.kind}-tail`, text, start, end: stat.size }], warnings: start > 0 ? ['transcript_tail_truncated'] : [] };
  } catch { return { status: 'stale', segments: [], warnings: ['transcript_read_failed'] }; }
}

export async function runOneDistillationJob(input: { dataDir: string; cliId?: string; model?: string; piBin?: string }): Promise<'idle' | 'completed' | 'inconclusive'> {
  const store = await ObservationStore.open(input.dataDir);
  const claim = store.claimDistillationJob({});
  if (!claim) { store.close(); return 'idle'; }
  try {
    const event = store.get(claim.sourceEventId);
    if (!event) throw new Error('km_distillation_source_missing');
    const context = claim.evidenceContext;
    const window = resolveBoundedTranscriptWindow({ event,
      cliId: typeof context.cliId === 'string' ? context.cliId : input.cliId,
      cliSessionId: typeof context.cliSessionId === 'string' ? context.cliSessionId : undefined,
      cwd: typeof context.cwd === 'string' ? context.cwd : undefined,
      larkAppId: event.identity.botAppId, maxBytes: claim.profile.budgets.sourceBytes });
    const effectiveWindow = window.status === 'missing' || window.status === 'unsupported' || window.status === 'stale'
      ? boundedEvidenceWindow(event) : window;
    for (const candidate of extractKnowledgeCandidates(event)) store.proposeKnowledge(candidate, 'auto-rules');
    if (isKmPiShadowEnabled() && (input.cliId ?? 'pi') === 'pi') {
      await runCliDistillation({ cliId: 'pi', model: typeof context.model === 'string' ? context.model : input.model,
        sourceEventId: event.eventId, profile: claim.profile, window: effectiveWindow }, new PiDistillationExecutor({ piBin: input.piBin }));
      // Shadow output is intentionally not persisted into main candidates yet.
    }
    store.finishDistillationJob({ jobId: claim.jobId, claimToken: claim.claimToken, outputHash: effectiveWindow.contentHash! });
    return 'completed';
  } catch (error) {
    store.failDistillationJob({ jobId: claim.jobId, claimToken: claim.claimToken, error: error instanceof Error ? error.message : String(error), retry: true });
    return 'inconclusive';
  } finally { store.close(); }
}

export async function runRetrievalShadow(input: { dataDir: string; botAppId: string; sessionId: string; turnId?: string; userId?: string; queryText: string }): Promise<void> {
  if (!isKmRetrievalShadowEnabled()) return;
  const started = Date.now(); const store = await ObservationStore.open(input.dataDir);
  try {
    const raw = store.retrieve({ text: input.queryText, scopes: ['user', 'bot'], subject: input.userId, limit: 50 });
    const candidates: PromptMemoryCandidate[] = raw.map(item => ({ ...item, state: item.kind === 'memory' ? 'active' : 'approved',
      ...(item.kind === 'memory' ? { scope: 'user', subject: input.userId } : {}), providerIds: ['sqlite'] }));
    const plan = planPromptMemory(candidates, { botAppId: input.botAppId, userId: input.userId, mode: 'shadow', promptTokenBudget: 1_800 });
    const runId = store.recordRetrievalAudit({ botAppId: input.botAppId, sessionId: input.sessionId, turnId: input.turnId,
      queryHash: retrievalQueryHash({ text: input.queryText, botAppId: input.botAppId, userId: input.userId }), mode: 'shadow',
      candidateCount: candidates.length, eligibleCount: plan.eligible.length, latencyMs: Date.now() - started, warnings: [],
      results: [...plan.eligible.map(item => ({ itemId: item.id, itemKind: item.kind, providerIds: item.providerIds ?? [], score: item.score, eligible: true })),
        ...plan.filtered.map(value => ({ itemId: value.item.id, itemKind: value.item.kind, providerIds: value.item.providerIds ?? [], score: value.item.score, eligible: false, filterReason: value.reason }))] });
    store.recordPromptInjectionSnapshot({ retrievalRunId: runId, botAppId: input.botAppId, mode: 'shadow', disposition: plan.disposition,
      itemIds: plan.eligible.map(item => item.id), prompt: plan.prompt, reason: plan.reason });
  } finally { store.close(); }
}
