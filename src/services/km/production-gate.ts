import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { canonicalJsonStringify } from '../../utils/canonical-json.js';
import type {
  ApprovalGrade,
  KmProductionGateActionKind,
  KmProductionGateAuditRecord,
  KmProductionGateKillState,
  KmProductionGatePlanInsertInput,
  KmProductionGatePlanRecord,
  KmProductionGateState,
} from './observation-store.js';

export const KM_PRODUCTION_GATE_SCHEMA_VERSION = 1;
export const KM_PRODUCTION_GATE_ORCHESTRATOR_VERSION = 'km-production-gate-orchestrator-v1';
export const KM_PRODUCTION_GATE_INTENT_VERSION = 'km-production-gate-inert-intent-v1';

export type KmProductionGateStore = {
  createProductionGatePlan(input: KmProductionGatePlanInsertInput): KmProductionGatePlanRecord;
  getProductionGatePlan(planId: string): KmProductionGatePlanRecord | null;
  listProductionGatePlans(input?: { limit?: number; actionKind?: KmProductionGateActionKind; state?: KmProductionGateState }): KmProductionGatePlanRecord[];
  listProductionGateAudit(planId: string, limit?: number): KmProductionGateAuditRecord[];
  transitionProductionGatePlan(input: {
    planId: string;
    toState: KmProductionGateState;
    actorId: string;
    action: string;
    details?: Record<string, unknown>;
    expectedPreviewHash?: string;
    intent?: Record<string, unknown>;
    now?: string;
  }): KmProductionGatePlanRecord;
  getProductionGateKillState(): KmProductionGateKillState;
  setProductionGateKillState(input: { enabled: boolean; reason: string; actorId: string; now?: string }): KmProductionGateKillState;
};

export type KmProductionGateTarget =
  | { provider: string; endpoint: string; credentialRef: string }
  | { destinationRoot: string; manifestHash: string }
  | { botAppId: string; window: { start: string; end: string } }
  | { cutoff: string; expectedCounts: Record<string, number> };

export interface KmProductionGatePlanRequest {
  actionKind: KmProductionGateActionKind;
  target: Record<string, unknown>;
  scope: Record<string, unknown>;
  actorId: string;
  riskAck: Record<string, unknown>;
  ttlSeconds?: number;
  now?: string;
  confirmationToken?: string;
}

export interface KmProductionGatePlanBuildResult {
  plan: KmProductionGatePlanInsertInput;
  confirmationToken: string;
  handoff: KmProductionGateHandoffBundle;
}

export interface KmProductionGateApprovalRequest {
  planId: string;
  actorId: string;
  approvalGrade: ApprovalGrade;
  confirmationToken: string;
  previewHash: string;
  riskAck: Record<string, unknown>;
  now?: string;
}

export interface KmProductionGateIntentRequest {
  planId: string;
  actorId: string;
  confirmationToken: string;
  previewHash: string;
  now?: string;
}

export interface KmProductionGateIntent {
  schemaVersion: 1;
  intentVersion: string;
  planId: string;
  actionKind: KmProductionGateActionKind;
  actorId: string;
  previewHash: string;
  signedIntentHash: string;
  effective: false;
  sideEffectsExecuted: false;
  executorAvailable: false;
  createdAt: string;
  expiresAt: string;
  safety: {
    noNetwork: true;
    noFileWritesOutsideTestDirs: true;
    noExport: true;
    noInjection: true;
    noDeletion: true;
    noProviderCalls: true;
    noDaemonScheduler: true;
  };
}

export interface KmProductionGateHandoffBundle {
  schemaVersion: 1;
  orchestratorVersion: string;
  planId: string;
  actionKind: KmProductionGateActionKind;
  state: KmProductionGateState;
  target: Record<string, unknown>;
  scope: Record<string, unknown>;
  previewHash: string;
  preview: Record<string, unknown>;
  requiredApprovalGrade: ApprovalGrade;
  expiresAt: string;
  preflight: Array<Record<string, unknown>>;
  rollback: Record<string, unknown>;
  operatorChecklist: string[];
  effective: false;
  sideEffectsExecuted: false;
}

const ACTION_KINDS: readonly KmProductionGateActionKind[] = [
  'real-memory-transport',
  'real-central-sink',
  'formal-knowledge-export',
  'prompt-canary',
  'retention-purge',
];
const APPROVAL_GRADES: readonly ApprovalGrade[] = ['G0', 'G1', 'G2', 'G3', 'G4'];
const ACTION_APPROVAL_GRADE: Readonly<Record<KmProductionGateActionKind, ApprovalGrade>> = {
  'real-memory-transport': 'G3',
  'real-central-sink': 'G3',
  'formal-knowledge-export': 'G2',
  'prompt-canary': 'G2',
  'retention-purge': 'G4',
};
const STATE_TRANSITIONS: Readonly<Record<KmProductionGateState, readonly KmProductionGateState[]>> = {
  draft: ['ready', 'expired', 'failed'],
  ready: ['approved', 'expired', 'failed'],
  approved: ['executing', 'expired', 'failed'],
  executing: ['completed', 'failed', 'rolled_back'],
  completed: [],
  failed: ['rolled_back'],
  rolled_back: [],
  expired: [],
};

export function stableKmProductionGateHash(value: unknown): string {
  return `sha256:${createHash('sha256').update(canonicalJsonStringify(value)).digest('hex')}`;
}

export function hashKmProductionGateToken(actionKind: KmProductionGateActionKind, token: string): string {
  const normalized = requireNonEmpty(token, 'confirmation_token');
  return stableKmProductionGateHash({ tokenScope: 'km-production-gate-confirmation', actionKind, token: normalized });
}

export function verifyKmProductionGateToken(actionKind: KmProductionGateActionKind, token: string, expectedHash: string): boolean {
  const actual = hashKmProductionGateToken(actionKind, token);
  const expected = Buffer.from(expectedHash);
  const got = Buffer.from(actual);
  return expected.length === got.length && timingSafeEqual(expected, got);
}

export function assertKmProductionGateTokenMatchesOnlyActionKind(
  actionKind: KmProductionGateActionKind,
  token: string,
  expectedHash: string,
): void {
  if (!verifyKmProductionGateToken(actionKind, token, expectedHash)) {
    throw new Error('km_production_gate_confirmation_token_invalid');
  }
  for (const other of ACTION_KINDS) {
    if (other !== actionKind && verifyKmProductionGateToken(other, token, expectedHash)) {
      throw new Error('km_production_gate_confirmation_token_cross_action');
    }
  }
}

export function kmProductionGateGradeRank(grade: ApprovalGrade): number {
  if (!APPROVAL_GRADES.includes(grade)) throw new Error('km_production_gate_approval_grade_invalid');
  return Number(grade.slice(1));
}

export function assertKmProductionGateTransition(from: KmProductionGateState, to: KmProductionGateState): void {
  if (!STATE_TRANSITIONS[from]?.includes(to)) throw new Error(`km_production_gate_invalid_transition:${from}:${to}`);
}

export function buildKmProductionGatePlan(input: KmProductionGatePlanRequest): KmProductionGatePlanBuildResult {
  const actionKind = normalizeActionKind(input.actionKind);
  const actorId = requireNonEmpty(input.actorId, 'actor');
  const now = parseIso(input.now ?? new Date().toISOString(), 'now');
  const ttlSeconds = Math.max(60, Math.min(Math.trunc(input.ttlSeconds ?? 900), 31 * 86_400));
  const expiresAt = new Date(now.getTime() + ttlSeconds * 1000).toISOString();
  const target = normalizeTarget(actionKind, input.target);
  const scope = normalizeScope(input.scope);
  const riskAck = normalizeRiskAck(input.riskAck);
  const preflight = buildPreflight(actionKind, target, scope);
  const rollback = buildRollbackPlan(actionKind, target, scope);
  const preview = buildImpactPreview(actionKind, target, scope, preflight, rollback);
  const previewHash = stableKmProductionGateHash(preview);
  const confirmationToken = input.confirmationToken ?? randomBytes(18).toString('base64url');
  const plan = {
    planId: `pg_${createHash('sha256').update(`${actionKind}|${previewHash}|${actorId}|${now.toISOString()}`).digest('hex').slice(0, 32)}`,
    actionKind,
    state: 'ready' as const,
    target,
    scope,
    preview,
    previewHash,
    requiredApprovalGrade: ACTION_APPROVAL_GRADE[actionKind],
    actorId,
    riskAck,
    expiresAt,
    confirmationTokenHash: hashKmProductionGateToken(actionKind, confirmationToken),
    preflight,
    rollback,
    now: now.toISOString(),
  };
  return { plan, confirmationToken, handoff: buildKmProductionGateHandoff({ ...plan, createdAt: now.toISOString(), updatedAt: now.toISOString() }) };
}

export function approveKmProductionGatePlan(store: KmProductionGateStore, input: KmProductionGateApprovalRequest): KmProductionGatePlanRecord {
  const plan = requirePlan(store, input.planId);
  assertNotExpired(plan, input.now);
  assertFreshPreview(plan, input.previewHash);
  assertRiskAcknowledgement(plan, input.riskAck);
  assertKmProductionGateTokenMatchesOnlyActionKind(plan.actionKind, input.confirmationToken, plan.confirmationTokenHash);
  if (kmProductionGateGradeRank(input.approvalGrade) < kmProductionGateGradeRank(plan.requiredApprovalGrade)) {
    throw new Error('km_production_gate_approval_grade_insufficient');
  }
  return store.transitionProductionGatePlan({
    planId: plan.planId,
    toState: 'approved',
    actorId: input.actorId,
    action: 'approved',
    expectedPreviewHash: plan.previewHash,
    details: { approvalGrade: input.approvalGrade, previewHash: input.previewHash, riskAckHash: stableKmProductionGateHash(input.riskAck) },
    now: input.now,
  });
}

export function createKmProductionGateIntent(store: KmProductionGateStore, input: KmProductionGateIntentRequest): { plan: KmProductionGatePlanRecord; intent: KmProductionGateIntent } {
  const kill = store.getProductionGateKillState();
  if (kill.enabled) throw new Error('km_production_gate_kill_switch_enabled');
  const plan = requirePlan(store, input.planId);
  if (plan.state !== 'approved') throw new Error(`km_production_gate_intent_requires_approved:${plan.state}`);
  assertNotExpired(plan, input.now);
  assertFreshPreview(plan, input.previewHash);
  assertKmProductionGateTokenMatchesOnlyActionKind(plan.actionKind, input.confirmationToken, plan.confirmationTokenHash);
  const now = input.now ?? new Date().toISOString();
  const intentBase = {
    schemaVersion: KM_PRODUCTION_GATE_SCHEMA_VERSION,
    intentVersion: KM_PRODUCTION_GATE_INTENT_VERSION,
    planId: plan.planId,
    actionKind: plan.actionKind,
    actorId: requireNonEmpty(input.actorId, 'intent_actor'),
    previewHash: plan.previewHash,
    effective: false,
    sideEffectsExecuted: false,
    executorAvailable: false,
    createdAt: now,
    expiresAt: plan.expiresAt,
    safety: {
      noNetwork: true,
      noFileWritesOutsideTestDirs: true,
      noExport: true,
      noInjection: true,
      noDeletion: true,
      noProviderCalls: true,
      noDaemonScheduler: true,
    },
  } as const;
  const intent: KmProductionGateIntent = {
    ...intentBase,
    signedIntentHash: stableKmProductionGateHash({ ...intentBase, confirmationTokenHash: plan.confirmationTokenHash }),
  };
  const updated = store.transitionProductionGatePlan({
    planId: plan.planId,
    toState: 'executing',
    actorId: input.actorId,
    action: 'intent.created',
    expectedPreviewHash: plan.previewHash,
    intent: intent as unknown as Record<string, unknown>,
    details: { intentHash: intent.signedIntentHash, effective: false, sideEffectsExecuted: false },
    now,
  });
  return { plan: updated, intent };
}

export function expireKmProductionGatePlan(store: KmProductionGateStore, input: { planId: string; actorId: string; now?: string }): KmProductionGatePlanRecord {
  const plan = requirePlan(store, input.planId);
  if (Date.parse(plan.expiresAt) > Date.parse(input.now ?? new Date().toISOString())) throw new Error('km_production_gate_plan_not_expired');
  return store.transitionProductionGatePlan({
    planId: plan.planId,
    toState: 'expired',
    actorId: input.actorId,
    action: 'expired',
    expectedPreviewHash: plan.previewHash,
    details: { expiresAt: plan.expiresAt },
    now: input.now,
  });
}

export function buildKmProductionGateHandoff(plan: KmProductionGatePlanRecord): KmProductionGateHandoffBundle {
  return {
    schemaVersion: KM_PRODUCTION_GATE_SCHEMA_VERSION,
    orchestratorVersion: KM_PRODUCTION_GATE_ORCHESTRATOR_VERSION,
    planId: plan.planId,
    actionKind: plan.actionKind,
    state: plan.state,
    target: plan.target,
    scope: plan.scope,
    previewHash: plan.previewHash,
    preview: plan.preview,
    requiredApprovalGrade: plan.requiredApprovalGrade,
    expiresAt: plan.expiresAt,
    preflight: plan.preflight,
    rollback: plan.rollback,
    operatorChecklist: [
      'Verify previewHash still matches the current status response.',
      'Confirm the approval grade meets or exceeds requiredApprovalGrade.',
      'Use the one-time confirmation token only with this plan action kind.',
      'Treat the generated intent as inert evidence only; no executor is wired in this milestone.',
    ],
    effective: false,
    sideEffectsExecuted: false,
  };
}

function requirePlan(store: KmProductionGateStore, planId: string): KmProductionGatePlanRecord {
  const plan = store.getProductionGatePlan(requireNonEmpty(planId, 'plan_id'));
  if (!plan) throw new Error('km_production_gate_plan_not_found');
  return plan;
}

function normalizeActionKind(value: unknown): KmProductionGateActionKind {
  if (typeof value !== 'string' || !ACTION_KINDS.includes(value as KmProductionGateActionKind)) {
    throw new Error('km_production_gate_action_kind_invalid');
  }
  return value as KmProductionGateActionKind;
}

function normalizeTarget(actionKind: KmProductionGateActionKind, raw: Record<string, unknown>): Record<string, unknown> {
  switch (actionKind) {
    case 'real-memory-transport': {
      const provider = requireExact(raw.provider, 'target_provider');
      const endpoint = requireEndpoint(raw.endpoint);
      const credentialRef = requireCredentialRef(raw.credentialRef);
      return { provider, endpoint, credentialRef };
    }
    case 'real-central-sink': {
      const provider = requireExact(raw.provider, 'target_provider');
      const endpoint = requireEndpoint(raw.endpoint);
      const credentialRef = requireCredentialRef(raw.credentialRef);
      return { provider, endpoint, credentialRef };
    }
    case 'formal-knowledge-export': {
      const destinationRoot = requireAbsolutePath(raw.destinationRoot, 'destination_root');
      const manifestHash = requireSha256(raw.manifestHash, 'manifest_hash');
      return { destinationRoot, manifestHash };
    }
    case 'prompt-canary': {
      const botAppId = requireExact(raw.botAppId, 'bot_app_id');
      const window = raw.window;
      if (!window || typeof window !== 'object' || Array.isArray(window)) throw new Error('km_production_gate_canary_window_required');
      const start = parseIso((window as Record<string, unknown>).start, 'canary_window_start').toISOString();
      const end = parseIso((window as Record<string, unknown>).end, 'canary_window_end').toISOString();
      if (Date.parse(start) >= Date.parse(end)) throw new Error('km_production_gate_canary_window_invalid');
      return { botAppId, window: { start, end } };
    }
    case 'retention-purge': {
      const cutoff = parseIso(raw.cutoff, 'retention_cutoff').toISOString();
      const expectedCounts = normalizeExpectedCounts(raw.expectedCounts);
      return { cutoff, expectedCounts };
    }
  }
}

function normalizeScope(raw: Record<string, unknown>): Record<string, unknown> {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw) || Object.keys(raw).length === 0) {
    throw new Error('km_production_gate_scope_required');
  }
  const scope = canonicalJsonStringify(raw);
  if (scope.includes('*')) throw new Error('km_production_gate_scope_wildcard_rejected');
  for (const [key, value] of Object.entries(raw)) {
    requireNonEmpty(key, 'scope_key');
    if (typeof value === 'string') requireExact(value, `scope_${key}`);
    else if (value === null || value === undefined) throw new Error(`km_production_gate_scope_${key}_required`);
    else if (Array.isArray(value) && value.length === 0) throw new Error(`km_production_gate_scope_${key}_required`);
  }
  return JSON.parse(canonicalJsonStringify(raw)) as Record<string, unknown>;
}

function normalizeRiskAck(raw: Record<string, unknown>): Record<string, unknown> {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('km_production_gate_risk_ack_required');
  if (raw.acknowledged !== true) throw new Error('km_production_gate_risk_ack_required');
  return JSON.parse(canonicalJsonStringify(raw)) as Record<string, unknown>;
}

function normalizeExpectedCounts(raw: unknown): Record<string, number> {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw) || Object.keys(raw).length === 0) {
    throw new Error('km_production_gate_expected_counts_required');
  }
  const result: Record<string, number> = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    const name = requireExact(key, 'expected_count_key');
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) throw new Error('km_production_gate_expected_count_invalid');
    result[name] = value;
  }
  return result;
}

function requireEndpoint(value: unknown): string {
  const endpoint = requireExact(value, 'target_endpoint');
  const url = new URL(endpoint);
  if (url.protocol !== 'https:') throw new Error('km_production_gate_endpoint_https_required');
  return url.toString();
}

function requireCredentialRef(value: unknown): string {
  const ref = requireExact(value, 'target_credential_ref');
  if (!/^env:[A-Z_][A-Z0-9_]*$/u.test(ref) && !/^secret:[A-Za-z0-9_.:/-]+$/u.test(ref)) {
    throw new Error('km_production_gate_credential_ref_invalid');
  }
  return ref;
}

function requireAbsolutePath(value: unknown, field: string): string {
  const text = requireExact(value, field);
  if (!text.startsWith('/')) throw new Error(`km_production_gate_${field}_absolute_required`);
  return text;
}

function requireSha256(value: unknown, field: string): string {
  const text = requireExact(value, field);
  if (!/^sha256:[a-f0-9]{64}$/u.test(text)) throw new Error(`km_production_gate_${field}_invalid`);
  return text;
}

function requireExact(value: unknown, field: string): string {
  const text = requireNonEmpty(value, field);
  if (text === '*' || text.toLowerCase() === 'all' || text.includes('*')) throw new Error(`km_production_gate_${field}_wildcard_rejected`);
  return text;
}

function requireNonEmpty(value: unknown, field: string): string {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`km_production_gate_${field}_required`);
  return value.trim();
}

function parseIso(value: unknown, field: string): Date {
  const text = requireNonEmpty(value, field);
  const date = new Date(text);
  if (!Number.isFinite(date.getTime())) throw new Error(`km_production_gate_${field}_invalid`);
  return date;
}

function buildPreflight(actionKind: KmProductionGateActionKind, target: Record<string, unknown>, scope: Record<string, unknown>): Array<Record<string, unknown>> {
  return [
    { name: 'action_kind_supported', passed: true, actionKind },
    { name: 'exact_target_parameters_present', passed: true, targetHash: stableKmProductionGateHash(target) },
    { name: 'non_empty_exact_scope_present', passed: true, scopeHash: stableKmProductionGateHash(scope) },
    { name: 'side_effect_executors_absent', passed: true, effective: false },
  ];
}

function buildRollbackPlan(actionKind: KmProductionGateActionKind, target: Record<string, unknown>, scope: Record<string, unknown>): Record<string, unknown> {
  return {
    actionKind,
    automaticRollback: false,
    operatorRollbackOnly: true,
    targetHash: stableKmProductionGateHash(target),
    scopeHash: stableKmProductionGateHash(scope),
    steps: [
      'Do not execute side effects from this milestone.',
      'If a future executor consumes this plan, disable the corresponding runtime gate first.',
      'Use the audit trail and previewHash to reconstruct the approved intent.',
    ],
  };
}

function buildImpactPreview(
  actionKind: KmProductionGateActionKind,
  target: Record<string, unknown>,
  scope: Record<string, unknown>,
  preflight: Array<Record<string, unknown>>,
  rollback: Record<string, unknown>,
): Record<string, unknown> {
  const base = {
    schemaVersion: KM_PRODUCTION_GATE_SCHEMA_VERSION,
    orchestratorVersion: KM_PRODUCTION_GATE_ORCHESTRATOR_VERSION,
    actionKind,
    target,
    scope,
    requiredApprovalGrade: ACTION_APPROVAL_GRADE[actionKind],
    preflight,
    rollbackHash: stableKmProductionGateHash(rollback),
    impact: {
      effective: false,
      sideEffectsExecuted: false,
      networkCalls: 0,
      filesWrittenOutsideTestDirs: 0,
      providerCalls: 0,
      daemonSchedulerChanges: 0,
    },
  };
  switch (actionKind) {
    case 'real-memory-transport':
      return { ...base, summary: 'Would enable a real memory backend transport after a future executor is added.' };
    case 'real-central-sink':
      return { ...base, summary: 'Would enable a real central sink after a future executor is added.' };
    case 'formal-knowledge-export':
      return { ...base, summary: 'Would apply a reviewed formal knowledge export after a future executor is added.' };
    case 'prompt-canary':
      return { ...base, summary: 'Would enable prompt memory canary routing for the exact bot and window after a future executor is added.' };
    case 'retention-purge':
      return { ...base, summary: 'Would purge eligible retention rows after a future executor is added.' };
  }
}

function assertNotExpired(plan: KmProductionGatePlanRecord, now = new Date().toISOString()): void {
  if (Date.parse(plan.expiresAt) <= Date.parse(now)) throw new Error('km_production_gate_plan_expired');
}

function assertFreshPreview(plan: KmProductionGatePlanRecord, previewHash: string): void {
  if (previewHash !== plan.previewHash) throw new Error('km_production_gate_preview_stale');
}

function assertRiskAcknowledgement(plan: KmProductionGatePlanRecord, riskAck: Record<string, unknown>): void {
  if (stableKmProductionGateHash(riskAck) !== stableKmProductionGateHash(plan.riskAck)) {
    throw new Error('km_production_gate_risk_ack_mismatch');
  }
}
