import type { MemoryScope, MemoryUpsertInput } from './observation-store.js';

export interface ExplicitPreferenceEvidence {
  confidence: 'observed' | 'inferred';
  explicitUserStatement: boolean;
  scope: MemoryScope;
  subject: string;
  claimKey: string;
  claimText: string;
  privacyClass: MemoryUpsertInput['privacyClass'];
  sourceRefs: unknown[];
  evidenceSpanComplete: boolean;
  policyTags?: string[];
  ttlExpiresAt?: string;
  reviewAfter?: string;
}

export interface SafeMemoryPolicyDecision {
  disposition: 'reject' | 'propose' | 'activate';
  reasonCodes: string[];
  memory?: MemoryUpsertInput;
  activationMode?: 'policy-auto';
  policyVersion: 'safe-auto-activation-v1';
}

const HIGH_RISK = new Set(['permission', 'security', 'credential', 'financial', 'production-operation', 'identity', 'approval', 'external-action']);

/** Only explicit, low-risk, observed user/bot preferences may auto-activate. */
export function decideSafeMemoryActivation(input: ExplicitPreferenceEvidence, now = new Date()): SafeMemoryPolicyDecision {
  const reasons: string[] = [];
  if (input.confidence !== 'observed') reasons.push('not_observed');
  if (!input.explicitUserStatement) reasons.push('not_explicit_user_statement');
  if (input.scope !== 'user' && input.scope !== 'bot') reasons.push('scope_not_auto_eligible');
  if (!input.evidenceSpanComplete || input.sourceRefs.length === 0) reasons.push('evidence_incomplete');
  if (input.privacyClass === 'sensitive' || input.privacyClass === 'secret-reference-only') reasons.push('privacy_not_auto_eligible');
  if ((input.policyTags ?? []).some(tag => HIGH_RISK.has(tag))) reasons.push('high_risk_topic');
  if (!input.claimKey.trim() || !input.claimText.trim() || !input.subject.trim()) reasons.push('invalid_claim');
  const hardReject = reasons.includes('privacy_not_auto_eligible') || reasons.includes('invalid_claim');
  const disposition = hardReject ? 'reject' : reasons.length > 0 ? 'propose' : 'activate';
  if (disposition === 'reject') return { disposition, reasonCodes: reasons, policyVersion: 'safe-auto-activation-v1' };
  const reviewAfter = input.reviewAfter ?? new Date(now.getTime() + 90 * 24 * 60 * 60 * 1000).toISOString();
  return {
    disposition,
    reasonCodes: reasons.length ? reasons : ['explicit_observed_low_risk_preference'],
    policyVersion: 'safe-auto-activation-v1',
    ...(disposition === 'activate' ? { activationMode: 'policy-auto' as const } : {}),
    memory: {
      state: disposition === 'activate' ? 'active' : 'proposed', scope: input.scope, subject: input.subject,
      claimKey: input.claimKey.trim(), claimText: input.claimText.trim(), confidence: input.confidence,
      sourceRefs: input.sourceRefs, reviewAfter, syncPolicy: 'local-only', privacyClass: input.privacyClass,
    },
  };
}
