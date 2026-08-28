import type {
  KmProductionGateAuditRecord,
  KmProductionGateKillState,
  KmProductionGatePlanInsertInput,
  KmProductionGatePlanRecord,
} from './observation-store.js';
import {
  approveKmProductionGatePlan,
  assertKmProductionGateTokenMatchesOnlyActionKind,
  stableKmProductionGateHash,
} from './production-gate.js';
import type { ApprovalGrade } from './observation-store.js';

export const KM_CANARY_RUNTIME_INTENT = 'prompt-canary-runtime-v1';

export interface KmCanaryReleaseStore {
  createProductionGatePlan(input: KmProductionGatePlanInsertInput): KmProductionGatePlanRecord;
  getProductionGatePlan(planId: string): KmProductionGatePlanRecord | null;
  listProductionGatePlans(input?: { limit?: number; actionKind?: 'prompt-canary' }): KmProductionGatePlanRecord[];
  transitionProductionGatePlan(input: {
    planId: string; toState: 'approved' | 'executing' | 'rolled_back'; actorId: string; action: string;
    expectedPreviewHash?: string; intent?: Record<string, unknown>; details?: Record<string, unknown>; now?: string;
  }): KmProductionGatePlanRecord;
  getProductionGateKillState(): KmProductionGateKillState;
  listProductionGateAudit(planId: string, limit?: number): KmProductionGateAuditRecord[];
  setProductionGateKillState(input: { enabled: boolean; reason: string; actorId: string; now?: string }): KmProductionGateKillState;
}

export interface KmCanaryRuntimeAuthorization {
  active: boolean;
  planId?: string;
  botAppId: string;
  window?: { start: string; end: string };
  reason: 'active' | 'not_found' | 'outside_window' | 'expired' | 'kill_switch_enabled';
}

function runtimeIntent(plan: KmProductionGatePlanRecord): Record<string, unknown> | undefined {
  const intent = plan.intent;
  return intent && intent.kind === KM_CANARY_RUNTIME_INTENT && intent.effective === true ? intent : undefined;
}

export function resolveKmCanaryRuntimeAuthorization(
  store: Pick<KmCanaryReleaseStore, 'listProductionGatePlans' | 'getProductionGateKillState'>,
  botAppId: string,
  now = new Date().toISOString(),
): KmCanaryRuntimeAuthorization {
  if (store.getProductionGateKillState().enabled) return { active: false, botAppId, reason: 'kill_switch_enabled' };
  const nowMs = Date.parse(now);
  const plans = store.listProductionGatePlans({ limit: 200, actionKind: 'prompt-canary' });
  let outsideWindow: KmProductionGatePlanRecord | undefined;
  let expired: KmProductionGatePlanRecord | undefined;
  for (const plan of plans) {
    if (plan.state !== 'executing' || !runtimeIntent(plan)) continue;
    const target = plan.target as { botAppId?: unknown; window?: { start?: unknown; end?: unknown } };
    if (target.botAppId !== botAppId || typeof target.window?.start !== 'string' || typeof target.window.end !== 'string') continue;
    if (Date.parse(plan.expiresAt) <= nowMs) { expired = plan; continue; }
    if (Date.parse(target.window.start) > nowMs || Date.parse(target.window.end) <= nowMs) { outsideWindow = plan; continue; }
    return { active: true, planId: plan.planId, botAppId, window: { start: target.window.start, end: target.window.end }, reason: 'active' };
  }
  const candidate = expired ?? outsideWindow;
  return {
    active: false, botAppId, planId: candidate?.planId,
    window: candidate ? (candidate.target as any).window : undefined,
    reason: expired ? 'expired' : outsideWindow ? 'outside_window' : 'not_found',
  };
}

export function activateKmCanaryRelease(store: KmCanaryReleaseStore, input: {
  planId: string; actorId: string; approvalGrade: ApprovalGrade; confirmationToken: string;
  previewHash: string; riskAck: Record<string, unknown>; now?: string;
}): KmProductionGatePlanRecord {
  if (store.getProductionGateKillState().enabled) throw new Error('km_production_gate_kill_switch_enabled');
  let plan = store.getProductionGatePlan(input.planId);
  if (!plan || plan.actionKind !== 'prompt-canary') throw new Error('km_canary_release_plan_invalid');
  if (plan.state === 'ready') {
    plan = approveKmProductionGatePlan(store, input);
  }
  if (plan.state !== 'approved') throw new Error(`km_canary_release_requires_approved:${plan.state}`);
  if (plan.previewHash !== input.previewHash) throw new Error('km_production_gate_preview_stale');
  if (stableKmProductionGateHash(plan.riskAck) !== stableKmProductionGateHash(input.riskAck)) throw new Error('km_production_gate_risk_ack_mismatch');
  assertKmProductionGateTokenMatchesOnlyActionKind('prompt-canary', input.confirmationToken, plan.confirmationTokenHash);
  const now = input.now ?? new Date().toISOString();
  const target = plan.target as { botAppId: string; window: { start: string; end: string } };
  if (Date.parse(plan.expiresAt) <= Date.parse(now) || Date.parse(target.window.end) <= Date.parse(now)) throw new Error('km_production_gate_plan_expired');
  return store.transitionProductionGatePlan({
    planId: plan.planId, toState: 'executing', actorId: input.actorId, action: 'canary.runtime_activated',
    expectedPreviewHash: plan.previewHash, now,
    intent: {
      kind: KM_CANARY_RUNTIME_INTENT, effective: true, exactBotAppId: target.botAppId,
      window: target.window, activatedAt: now, autoFallback: 'shadow', restartRequired: false,
    },
    details: { exactBotAppId: target.botAppId, window: target.window, effective: true, restartRequired: false },
  });
}

export function rollbackKmCanaryRelease(store: KmCanaryReleaseStore, input: {
  planId: string; actorId: string; reason: string; now?: string;
}): KmProductionGatePlanRecord {
  const plan = store.getProductionGatePlan(input.planId);
  if (!plan || plan.actionKind !== 'prompt-canary') throw new Error('km_canary_release_plan_invalid');
  if (plan.state !== 'executing' || !runtimeIntent(plan)) throw new Error(`km_canary_release_not_active:${plan.state}`);
  return store.transitionProductionGatePlan({
    planId: plan.planId, toState: 'rolled_back', actorId: input.actorId, action: 'canary.runtime_rolled_back',
    expectedPreviewHash: plan.previewHash, now: input.now,
    details: { reason: input.reason.trim() || 'operator_rollback', effective: false, fallback: 'shadow' },
  });
}
