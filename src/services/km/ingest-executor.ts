import { createHash, timingSafeEqual } from 'node:crypto';
import { canonicalJsonStringify } from '../../utils/canonical-json.js';
import type {
  KmIngestCandidateInput,
  KmIngestItemInput,
  KmIngestRunReport,
  KmIngestRunState,
  KmIngestTargetConfig,
  KmIngestTargetRecord,
  KnowledgeCandidateInput,
  ObservationStore,
} from './observation-store.js';

export const KM_INGEST_EXECUTOR_VERSION = 'km-ingest-executor-v1';

export type KmIngestExecutorStore = Pick<ObservationStore,
  'putKmIngestTarget' | 'getKmIngestTarget' | 'listKmIngestTargets'
  | 'createKmIngestRun' | 'transitionKmIngestRun' | 'runKmIngestOffline'
  | 'rollbackKmIngestRun' | 'getKmIngestRunReport' | 'listKmIngestRuns' | 'getDistillationJob'
>;

export interface KmIngestPlanInput {
  store: KmIngestExecutorStore;
  targetId: string;
  sourceRunId: string;
  extractorProviderId: string;
  candidates: KmIngestCandidateInput[];
  actorId: string;
  idempotencyKey: string;
  confirmationToken: string;
  env?: NodeJS.ProcessEnv;
  credentialResolver?: (ref: string) => boolean;
}

export interface KmIngestApproveInput {
  store: KmIngestExecutorStore;
  runId: string;
  actorId: string;
  confirmationToken: string;
  expectedPlanHash: string;
  externalAck: Record<string, unknown>;
}

export interface KmIngestExecuteInput {
  store: KmIngestExecutorStore;
  runId: string;
  actorId: string;
  confirmationToken: string;
  expectedPlanHash: string;
  maxItems?: number;
  env?: NodeJS.ProcessEnv;
  credentialResolver?: (ref: string) => boolean;
}

export interface KmIngestRollbackInput {
  store: KmIngestExecutorStore;
  runId: string;
  actorId: string;
  expectedPlanHash: string;
  reasonCode: string;
}

export function registerKmIngestTarget(
  store: KmIngestExecutorStore,
  config: KmIngestTargetConfig,
  actorId: string,
): KmIngestTargetRecord {
  return store.putKmIngestTarget({ config, actorId });
}

export function planKmIngest(input: KmIngestPlanInput): KmIngestRunReport {
  const actorId = requireNonEmpty(input.actorId, 'km_ingest_actor_required');
  const target = input.store.getKmIngestTarget(requireNonEmpty(input.targetId, 'km_ingest_target_required'));
  const missingReasons: string[] = [];
  if (!target) missingReasons.push('target_missing');
  else {
    if (target.state !== 'ready') missingReasons.push('target_disabled');
    if (!credentialAvailable(target.credentialRef, input.env, input.credentialResolver)) missingReasons.push('credential_missing');
  }
  const sourceRunId = requireNonEmpty(input.sourceRunId, 'km_ingest_source_run_required');
  const extractorRun = input.store.getDistillationJob(sourceRunId);
  if (!extractorRun) missingReasons.push('extractor_run_missing');
  const extractorRunState = String(extractorRun?.state ?? 'missing');
  if (extractorRun && !['completed', 'persisted'].includes(extractorRunState)) missingReasons.push(`extractor_run_not_ready:${extractorRunState}`);
  const extractorProviderId = requireNonEmpty(input.extractorProviderId, 'km_ingest_extractor_provider_required');
  if (target?.target.allowedProviderIds.length && !target.target.allowedProviderIds.includes(extractorProviderId)) {
    missingReasons.push('extractor_provider_not_allowed');
  }

  const normalized = normalizeCandidates(input.candidates, sourceRunId, extractorProviderId);
  const canonicalKeys = normalized.map(item => item.canonicalKey).sort((a, b) => a.localeCompare(b));
  const plan = {
    schemaVersion: 1 as const,
    targetId: input.targetId,
    targetHash: target?.targetHash ?? 'sha256:missing-target',
    sourceRunId,
    extractorRunState,
    extractorProviderId,
    mode: 'offline' as const,
    dryRun: true,
    planCalls: { markIngested: false },
    canonicalKeys,
  };
  const report = input.store.createKmIngestRun({
    actorId,
    idempotencyKey: input.idempotencyKey,
    targetId: input.targetId,
    confirmationTokenHash: hashKmIngestConfirmationToken(input.confirmationToken),
    plan,
    items: normalized,
  });
  if (missingReasons.length) {
    if (report.run.state === 'blocked') return report;
    input.store.transitionKmIngestRun({
      runId: report.run.runId,
      toState: 'blocked',
      actorId,
      action: 'plan.blocked',
      details: { reasons: missingReasons, failClosed: true },
      expectedPlanHash: report.run.planHash,
      lastError: missingReasons.join(','),
    });
    return input.store.getKmIngestRunReport(report.run.runId)!;
  }
  return report;
}

export function approveKmIngestRun(input: KmIngestApproveInput): KmIngestRunReport {
  const report = requireRun(input.store, input.runId);
  assertExpectedPlanHash(report.run.planHash, input.expectedPlanHash);
  assertConfirmationToken(input.confirmationToken, report.run.confirmationTokenHash);
  assertExternalAck(input.externalAck, report.run.planHash);
  input.store.transitionKmIngestRun({
    runId: input.runId,
    toState: 'approved',
    actorId: requireNonEmpty(input.actorId, 'km_ingest_actor_required'),
    action: 'run.approved',
    expectedPlanHash: input.expectedPlanHash,
    externalAck: input.externalAck,
    details: { externalAckHash: hashJson(input.externalAck), planHash: input.expectedPlanHash },
  });
  return input.store.getKmIngestRunReport(input.runId)!;
}

export function executeKmIngestOffline(input: KmIngestExecuteInput): KmIngestRunReport {
  const report = requireRun(input.store, input.runId);
  assertExpectedPlanHash(report.run.planHash, input.expectedPlanHash);
  assertConfirmationToken(input.confirmationToken, report.run.confirmationTokenHash);
  if (report.run.state === 'blocked') return report;
  if (report.run.state === 'planned') {
    const blocked = input.store.transitionKmIngestRun({
      runId: input.runId,
      toState: 'blocked',
      actorId: input.actorId,
      action: 'execution.blocked',
      expectedPlanHash: input.expectedPlanHash,
      lastError: 'run_approval_missing',
      details: { reason: 'run_approval_missing', failClosed: true },
    });
    return input.store.getKmIngestRunReport(blocked.runId)!;
  }
  const target = input.store.getKmIngestTarget(report.run.targetId);
  if (!target || target.state !== 'ready' || target.targetHash !== report.run.plan.targetHash || !credentialAvailable(target.credentialRef, input.env, input.credentialResolver)) {
    const reason = !target ? 'target_missing'
      : target.state !== 'ready' ? 'target_disabled'
        : target.targetHash !== report.run.plan.targetHash ? 'target_hash_mismatch'
          : 'credential_missing';
    const blocked = input.store.transitionKmIngestRun({
      runId: input.runId,
      toState: 'blocked',
      actorId: input.actorId,
      action: 'execution.blocked',
      expectedPlanHash: input.expectedPlanHash,
      lastError: reason,
      details: { reason, failClosed: true },
    });
    return input.store.getKmIngestRunReport(blocked.runId)!;
  }
  if (!report.run.externalAck) {
    const blocked = input.store.transitionKmIngestRun({
      runId: input.runId,
      toState: 'blocked',
      actorId: input.actorId,
      action: 'execution.blocked',
      expectedPlanHash: input.expectedPlanHash,
      lastError: 'external_ack_missing',
      details: { reason: 'external_ack_missing', failClosed: true },
    });
    return input.store.getKmIngestRunReport(blocked.runId)!;
  }
  assertExternalAck(report.run.externalAck, report.run.planHash);
  return input.store.runKmIngestOffline({
    runId: input.runId,
    actorId: requireNonEmpty(input.actorId, 'km_ingest_actor_required'),
    ...(input.maxItems ? { maxItems: input.maxItems } : {}),
  });
}

export function rollbackKmIngest(input: KmIngestRollbackInput): KmIngestRunReport {
  return input.store.rollbackKmIngestRun({
    runId: input.runId,
    actorId: requireNonEmpty(input.actorId, 'km_ingest_actor_required'),
    expectedPlanHash: input.expectedPlanHash,
    reasonCode: input.reasonCode,
  });
}

export function hashKmIngestConfirmationToken(token: string): string {
  return hashJson({ tokenScope: 'km-ingest-confirmation', token: requireNonEmpty(token, 'km_ingest_confirmation_required') });
}

export function kmIngestCandidateCanonicalKey(candidate: KmIngestCandidateInput): string {
  return candidate.canonicalKey?.trim() || hashJson({
    targetLayer: candidate.targetLayer,
    claimKey: candidate.claimKey,
    claimText: candidate.claimText,
    sourceRefs: candidate.sourceRefs,
  });
}

function normalizeCandidates(
  candidates: KmIngestCandidateInput[],
  sourceRunId: string,
  extractorProviderId: string,
): KmIngestItemInput[] {
  const seen = new Set<string>();
  return candidates.map(candidate => {
    const canonicalKey = requireNonEmpty(kmIngestCandidateCanonicalKey(candidate), 'km_ingest_canonical_key_required');
    if (seen.has(canonicalKey)) throw new Error('km_ingest_canonical_key_duplicate');
    seen.add(canonicalKey);
    const normalizedCandidate: KnowledgeCandidateInput & { providerId?: string; sourceRunId?: string } = {
      ...candidate,
      providerId: candidate.providerId ?? extractorProviderId,
      sourceRunId: candidate.sourceRunId ?? sourceRunId,
      sourceRefs: candidate.sourceRefs,
    };
    return {
      canonicalKey,
      candidate: normalizedCandidate,
      candidateHash: hashJson(normalizedCandidate),
    };
  });
}

function assertExternalAck(ack: Record<string, unknown>, planHash: string): void {
  if (ack.approved !== true) throw new Error('km_ingest_external_ack_required');
  if (ack.planHash !== planHash) throw new Error('km_ingest_external_ack_plan_hash_mismatch');
  requireNonEmpty(String(ack.approvedBy ?? ''), 'km_ingest_external_ack_actor_required');
}

function assertExpectedPlanHash(actual: string, expected: string): void {
  if (actual !== requireNonEmpty(expected, 'km_ingest_plan_hash_required')) throw new Error('km_ingest_plan_hash_mismatch');
}

function assertConfirmationToken(token: string, expectedHash: string): void {
  const actual = hashKmIngestConfirmationToken(token);
  const expected = Buffer.from(expectedHash);
  const got = Buffer.from(actual);
  if (expected.length !== got.length || !timingSafeEqual(expected, got)) throw new Error('km_ingest_confirmation_token_invalid');
}

function credentialAvailable(
  ref: string,
  env: NodeJS.ProcessEnv = process.env,
  credentialResolver?: (ref: string) => boolean,
): boolean {
  const credentialRef = ref.trim();
  if (!credentialRef) return false;
  if (credentialRef.startsWith('local-secret:')) return credentialResolver?.(credentialRef) === true;
  if (credentialRef.startsWith('mock:')) return true;
  if (credentialRef.startsWith('file:/')) return true;
  if (!credentialRef.startsWith('env:')) return false;
  const name = credentialRef.slice('env:'.length);
  return Boolean(env[name]?.trim());
}

function requireRun(store: KmIngestExecutorStore, runId: string): KmIngestRunReport {
  const report = store.getKmIngestRunReport(requireNonEmpty(runId, 'km_ingest_run_required'));
  if (!report) throw new Error('km_ingest_run_not_found');
  return report;
}

function requireNonEmpty(value: string, code: string): string {
  const text = value.trim();
  if (!text) throw new Error(code);
  return text;
}

function hashJson(value: unknown): string {
  return `sha256:${createHash('sha256').update(canonicalJsonStringify(value)).digest('hex')}`;
}
