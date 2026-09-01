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
import { extractAttributedUserEvidence, extractExplicitPreferences } from './preference-extractor.js';
import {
  composeLivePromptMemory,
  planPromptMemory,
  retrievalQueryHash,
  type PromptMemoryCandidate,
  type PromptMemoryMode,
} from './prompt-memory.js';
import { federatedMemoryRetrieveWithTelemetry } from './memory-backend-coordinator.js';
import type { MemoryBackendProvider } from './memory-backend-spi.js';
import { resolveRetrievalScopeSubjects, visibleScopes } from './retrieval-quality.js';
import type { MemoryScope, RetrievalQualityCounters } from './observation-store.js';
import { resolveKmCanaryRuntimeAuthorization } from './canary-release.js';
import { recordAutomaticWorkspaceRetrievalEvidence } from './workspace-knowledge/retrieval-evidence.js';

function envOn(name: string, env: NodeJS.ProcessEnv = process.env): boolean {
  return ['1', 'true', 'yes'].includes(env[name]?.trim().toLowerCase() ?? '');
}
export function isKmAutoDistillationEnabled(env = process.env): boolean { return envOn('BOTMUX_KM_AUTO_DISTILLATION_ENABLED', env); }
export function isKmPiShadowEnabled(env = process.env): boolean { return envOn('BOTMUX_KM_PI_SHADOW_ENABLED', env); }
export function isKmRetrievalShadowEnabled(env = process.env): boolean { return envOn('BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED', env); }
export function isKmFederatedRetrievalEnabled(env = process.env): boolean { return envOn('BOTMUX_KM_FEDERATED_RETRIEVAL_ENABLED', env); }
export function isKmLiveInjectionEnabled(env = process.env): boolean { return envOn('BOTMUX_KM_LIVE_INJECTION_ENABLED', env); }
export function isKmEffectiveModeAuthorized(env = process.env): boolean { return envOn('BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED', env); }

function parseBotAllowlist(value: string | undefined): string[] {
  return (value ?? '').split(/[,\s]+/u).map(item => item.trim()).filter(Boolean);
}

function scopeVisible(scope: MemoryScope | undefined, subject: string | undefined, subjects: Partial<Record<MemoryScope, string>>): boolean {
  if (!scope || !subject) return false;
  return subjects[scope] === subject;
}

function mergeRetrievalMetrics(base: RetrievalQualityCounters, extra: Partial<RetrievalQualityCounters>): RetrievalQualityCounters {
  return {
    directHitCount: base.directHitCount + (extra.directHitCount ?? 0),
    normalizedHitCount: base.normalizedHitCount + (extra.normalizedHitCount ?? 0),
    noHitCount: base.noHitCount + (extra.noHitCount ?? 0),
    filteredScopeCount: base.filteredScopeCount + (extra.filteredScopeCount ?? 0),
    filteredPrivacyCount: base.filteredPrivacyCount + (extra.filteredPrivacyCount ?? 0),
    filteredStateCount: base.filteredStateCount + (extra.filteredStateCount ?? 0),
  };
}

export function defaultShadowProfile(botAppId: string, cliId = 'pi', model?: string): KmPipelineProfile {
  return {
    schemaVersion: 1, profileId: `bot-${botAppId}`, revision: 1, botAppId,
    sourceProvider: 'observation-source-v1', windowProvider: 'bounded-transcript-window-v1',
    primaryExtractor: 'builtin.rules-v1', shadowExtractors: isKmPiShadowEnabled() ? [`botmux-cli:${cliId}:${model ?? 'default'}`] : [],
    knowledgeRouter: 'builtin.layer-router-v1', memoryPolicy: 'safe-auto-activation-v1',
    memoryBackends: { writePolicy: 'single', primary: 'sqlite', mirrors: [] },
    injectionMode: 'shadow', budgets: { sourceBytes: 262_144, sourceTokens: 32_000, outputClaims: 20, promptTokens: 1_800 },
  };
}

/** Triggered only after the observation itself was durably accepted/deduped. */
export async function enqueueAutomaticDistillation(input: { dataDir: string; event: ObservationEvent; cliId?: string; model?: string; cliSessionId?: string; cwd?: string }): Promise<void> {
  if (!isKmAutoDistillationEnabled() || process.env.BOTMUX_KM_WORKLOAD === 'distillation') return;
  const store = await ObservationStore.open(input.dataDir);
  try {
    const profile = store.getEffectivePipelineProfile(input.event.identity.botAppId)
      ?? defaultShadowProfile(input.event.identity.botAppId, input.cliId, input.model);
    store.createDistillationJob({ sourceEventId: input.event.eventId, profile,
      evidenceContext: { cliId: input.cliId, model: input.model, cliSessionId: input.cliSessionId, cwd: input.cwd } });
  } finally { store.close(); }
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
    const evidenceText = effectiveWindow.segments.map(segment => segment.text).join('\n');
    const requesterSubjectId = typeof event.payload.requesterSubjectId === 'string' ? event.payload.requesterSubjectId : undefined;
    for (const attributed of extractAttributedUserEvidence(evidenceText)) {
      for (const preference of extractExplicitPreferences({ event, evidenceText: attributed.text, userId: requesterSubjectId,
        evidenceOffset: attributed.start, roleAttributed: true })) {
        const decision = decideSafeMemoryActivation(preference);
        let memoryId: string | undefined;
        if (decision.memory) memoryId = store.upsertMemory({ ...decision.memory, evidenceEventId: event.eventId }).item.memoryId;
        store.recordMemoryPolicyDecision({ sourceEventId: event.eventId, memoryId, policyVersion: decision.policyVersion,
          disposition: decision.disposition, reasonCodes: decision.reasonCodes,
          evidence: { claimKey: preference.claimKey, subject: preference.subject, span: { start: preference.evidenceStart, end: preference.evidenceEnd },
            evidenceTextHash: `sha256:${createHash('sha256').update(preference.evidenceText).digest('hex')}` } });
      }
    }
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

export async function drainDistillationJobs(input: { dataDir: string; cliId?: string; model?: string; piBin?: string; maxJobs?: number; holderId?: string }): Promise<number> {
  if (!isKmAutoDistillationEnabled()) return 0;
  const holderId = input.holderId ?? `pid:${process.pid}`; const leaseStore = await ObservationStore.open(input.dataDir);
  let acquired = false;
  try { acquired = leaseStore.acquireRuntimeLease({ leaseName: 'distillation-recovery', holderId, ttlMs: 45_000 }); }
  finally { leaseStore.close(); }
  if (!acquired) return 0;
  const max = Math.max(1, Math.min(input.maxJobs ?? 10, 100)); let processed = 0;
  try {
    while (processed < max) {
      const result = await runOneDistillationJob(input);
      if (result === 'idle') break;
      processed += 1;
    }
    return processed;
  } finally {
    const releaseStore = await ObservationStore.open(input.dataDir);
    try { releaseStore.releaseRuntimeLease({ leaseName: 'distillation-recovery', holderId }); } finally { releaseStore.close(); }
  }
}

export async function runRetrievalShadow(input: {
  dataDir: string; botAppId: string; sessionId: string; turnId?: string; userId?: string; queryText: string;
  projectId?: string; skillName?: string; environmentId?: string; teamId?: string; workspaceId?: string; workingDir?: string;
  providers?: MemoryBackendProvider[]; env?: NodeJS.ProcessEnv; providerTimeoutMs?: number;
}): Promise<void> {
  if (!isKmRetrievalShadowEnabled(input.env)) return;
  const started = Date.now(); const store = await ObservationStore.open(input.dataDir);
  try {
    const profile = store.getEffectivePipelineProfile(input.botAppId);
    if (profile?.injectionMode === 'off') return;
    const subjects = resolveRetrievalScopeSubjects(input);
    const local = store.retrieveWithMetrics({ text: input.queryText, scopes: visibleScopes(subjects), subjects, limit: 50 });
    const candidates: PromptMemoryCandidate[] = local.items.map(item => ({ ...item, state: item.kind === 'memory' ? 'active' : 'approved',
      providerIds: ['sqlite'] }));
    const warnings: string[] = [];
    if (isKmFederatedRetrievalEnabled(input.env) && input.providers?.length) {
      const remote = await federatedMemoryRetrieveWithTelemetry({ providers: input.providers,
        query: { text: input.queryText, scopes: visibleScopes(subjects), subject: input.userId, subjects, limit: 50, botAppId: input.botAppId },
        limit: 50, timeoutMs: input.providerTimeoutMs });
      warnings.push(...remote.warnings.map(warning => `federated_${warning}`));
      let filteredRemoteScope = 0;
      for (const item of remote.items) {
        if (!scopeVisible(item.scope, item.subject, subjects)) { filteredRemoteScope += 1; continue; }
        candidates.push({ id: item.memoryId ?? `${item.providerId}:${item.backendRef}`, kind: 'memory',
        title: item.metadata?.title ? String(item.metadata.title) : item.backendRef, text: item.text, score: item.fusedScore,
        sourceRefs: [{ kind: 'memory-backend', ref: item.backendRef, providers: item.providers }], privacyClass: 'internal',
        freshness: 'fresh', state: 'active', scope: item.scope ?? 'user', subject: item.subject ?? input.userId,
        providerIds: item.providers });
      }
      local.metrics = mergeRetrievalMetrics(local.metrics, { filteredScopeCount: filteredRemoteScope });
    } else if (input.providers?.length) {
      warnings.push('federated_retrieval_gate_disabled');
    }
    // This runtime path remains fail-closed in Shadow even if a stored profile
    // requests canary/active; live prompt mutation requires a separate gate.
    const plan = planPromptMemory(candidates, { botAppId: input.botAppId, userId: input.userId, mode: 'shadow',
      projectId: input.projectId, skillName: input.skillName, environmentId: input.environmentId, teamId: input.teamId,
      workspaceId: input.workspaceId, promptTokenBudget: profile?.budgets.promptTokens ?? 1_800 });
    const queryHash = retrievalQueryHash({ text: input.queryText, botAppId: input.botAppId, userId: input.userId });
    const runId = store.recordRetrievalAudit({ botAppId: input.botAppId, sessionId: input.sessionId, turnId: input.turnId,
      queryHash, mode: 'shadow',
      candidateCount: candidates.length, eligibleCount: plan.eligible.length, latencyMs: Date.now() - started,
      metrics: local.metrics,
      warnings: [...(profile && profile.injectionMode !== 'shadow' ? [`configured_mode_${profile.injectionMode}_forced_shadow`] : []), ...warnings],
      results: [...plan.eligible.map(item => ({ itemId: item.id, itemKind: item.kind, providerIds: item.providerIds ?? [], score: item.score, eligible: true })),
        ...plan.filtered.map(value => ({ itemId: value.item.id, itemKind: value.item.kind, providerIds: value.item.providerIds ?? [], score: value.item.score, eligible: false, filterReason: value.reason }))] });
    store.recordPromptInjectionSnapshot({ retrievalRunId: runId, botAppId: input.botAppId, mode: 'shadow', disposition: plan.disposition,
      requestedMode: plan.requestedMode, effectiveMode: plan.effectiveMode,
      itemIds: plan.selectedItemIds, prompt: plan.prompt, reason: plan.reason });
    const evidence = recordAutomaticWorkspaceRetrievalEvidence({
      workingDir: input.workingDir,
      queryHash,
      botAppId: input.botAppId,
      sessionId: input.sessionId,
      ...(input.turnId ? { turnId: input.turnId } : {}),
      retrievalRunId: runId,
      source: 'runtime_retrieval',
      candidates,
      selectedItemIds: [],
      fallbackOutcome: candidates.length === 0 ? 'no_hit' : undefined,
      useLabel: 'context_guided',
    });
    warnings.push(...evidence.warnings);
  } finally { store.close(); }
}

export async function composePromptMemoryForTurn(input: {
  dataDir: string; botAppId: string; sessionId: string; turnId?: string; userId?: string; queryText: string; promptContent: string;
  projectId?: string; skillName?: string; environmentId?: string; teamId?: string; workspaceId?: string; workingDir?: string;
  env?: NodeJS.ProcessEnv; providers?: MemoryBackendProvider[]; providerTimeoutMs?: number;
}): Promise<{ promptContent: string; injected: boolean; reason?: string; queryHash?: string; retrievalRunId?: string; workspaceEvidenceWarnings?: string[] }> {
  const env = input.env ?? process.env;
  if (!isKmRetrievalShadowEnabled(env) && !isKmLiveInjectionEnabled(env)) {
    return { promptContent: input.promptContent, injected: false, reason: 'retrieval_gate_disabled' };
  }
  const started = Date.now(); const store = await ObservationStore.open(input.dataDir);
  try {
    const profile = store.getEffectivePipelineProfile(input.botAppId);
    const requestedMode = (profile?.injectionMode ?? 'shadow') as PromptMemoryMode;
    const subjects = resolveRetrievalScopeSubjects(input);
    const local = store.retrieveWithMetrics({ text: input.queryText, scopes: visibleScopes(subjects), subjects, limit: 50 });
    const candidates: PromptMemoryCandidate[] = local.items.map(item => ({ ...item, state: item.kind === 'memory' ? 'active' : 'approved',
      providerIds: ['sqlite'] }));
    const warnings: string[] = [];
    if (isKmFederatedRetrievalEnabled(input.env) && input.providers?.length) {
      warnings.push('federated_retrieval_not_live_prompt_boundary');
    } else if (input.providers?.length) {
      warnings.push('federated_retrieval_gate_disabled');
    }
    const canaryAuthorization = resolveKmCanaryRuntimeAuthorization(store, input.botAppId);
    // Compatibility path for canaries activated before the Dashboard runtime
    // executor existed. It still requires all legacy env gates plus an exact,
    // approved, in-window plan; the wizard path needs no process reload.
    const legacyApprovalPresent = store.listProductionGatePlans({ limit: 200, actionKind: 'prompt-canary' }).some(plan => {
      if (plan.state !== 'approved') return false;
      const target = plan.target as { botAppId?: unknown; window?: { start?: unknown; end?: unknown } };
      const now = Date.now();
      return target.botAppId === input.botAppId
        && typeof target.window?.start === 'string' && typeof target.window?.end === 'string'
        && Date.parse(plan.expiresAt) > now && Date.parse(target.window.start) <= now && Date.parse(target.window.end) > now;
    });
    const legacyLiveAuthorized = legacyApprovalPresent
      && isKmEffectiveModeAuthorized(env)
      && isKmLiveInjectionEnabled(env)
      && parseBotAllowlist(env.BOTMUX_KM_CANARY_BOT_APP_IDS).includes(input.botAppId);
    const composed = composeLivePromptMemory(input.promptContent, candidates, {
      botAppId: input.botAppId,
      userId: input.userId,
      projectId: input.projectId,
      skillName: input.skillName,
      environmentId: input.environmentId,
      teamId: input.teamId,
      workspaceId: input.workspaceId,
      requestedMode,
      effectiveModeAuthorized: canaryAuthorization.active || legacyLiveAuthorized,
      liveInjectionEnabled: canaryAuthorization.active || legacyLiveAuthorized,
      canaryBotIds: canaryAuthorization.active || legacyLiveAuthorized ? [input.botAppId] : [],
      promptTokenBudget: profile?.budgets.promptTokens ?? 1_800,
    });
    const queryHash = retrievalQueryHash({ text: input.queryText, botAppId: input.botAppId, userId: input.userId });
    const runId = store.recordRetrievalAudit({ botAppId: input.botAppId, sessionId: input.sessionId, turnId: input.turnId,
      queryHash, mode: composed.plan.effectiveMode,
      candidateCount: candidates.length, eligibleCount: composed.plan.eligible.length, latencyMs: Date.now() - started,
      metrics: local.metrics,
      warnings,
      results: [...composed.plan.eligible.map(item => ({ itemId: item.id, itemKind: item.kind, providerIds: item.providerIds ?? [], score: item.score, eligible: true })),
        ...composed.plan.filtered.map(value => ({ itemId: value.item.id, itemKind: value.item.kind, providerIds: value.item.providerIds ?? [], score: value.item.score, eligible: false, filterReason: value.reason }))] });
    store.recordPromptInjectionSnapshot({ retrievalRunId: runId, botAppId: input.botAppId, mode: composed.plan.effectiveMode,
      requestedMode: composed.plan.requestedMode, effectiveMode: composed.plan.effectiveMode, disposition: composed.plan.disposition,
      itemIds: composed.plan.selectedItemIds, prompt: composed.plan.prompt, reason: composed.plan.reason });
    const evidence = recordAutomaticWorkspaceRetrievalEvidence({
      workingDir: input.workingDir,
      queryHash,
      botAppId: input.botAppId,
      sessionId: input.sessionId,
      ...(input.turnId ? { turnId: input.turnId } : {}),
      retrievalRunId: runId,
      source: 'prompt_memory',
      candidates,
      selectedItemIds: composed.mutated ? composed.plan.selectedItemIds : [],
      fallbackOutcome: candidates.length === 0 ? 'no_hit' : undefined,
      useLabel: composed.mutated ? 'direct_apply' : 'context_guided',
    });
    return {
      promptContent: composed.content,
      injected: composed.mutated,
      reason: composed.plan.reason,
      queryHash,
      retrievalRunId: runId,
      ...(evidence.warnings.length ? { workspaceEvidenceWarnings: evidence.warnings } : {}),
    };
  } finally { store.close(); }
}
