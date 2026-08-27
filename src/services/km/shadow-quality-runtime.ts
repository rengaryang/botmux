import type { KmShadowComparisonInput } from './observation-store.js';
import { ObservationStore } from './observation-store.js';

const DEFAULT_LEASE_MS = 45_000;
const DEFAULT_MAX_CASES = 50;

function envOn(name: string, env: NodeJS.ProcessEnv = process.env): boolean {
  return ['1', 'true', 'yes'].includes(env[name]?.trim().toLowerCase() ?? '');
}

function positiveEnv(name: string, fallback: number, env: NodeJS.ProcessEnv): number {
  const value = Number(env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

export function isKmShadowQualityEnabled(env = process.env): boolean {
  return envOn('BOTMUX_KM_SHADOW_QUALITY_ENABLED', env);
}

export interface KmShadowQualitySummary {
  enabled: boolean;
  leaseAcquired: boolean;
  scannedCases: number;
  createdComparisons: number;
  reusedComparisons: number;
  readinessReady: boolean;
  readinessReasonCodes: string[];
}

function storedClaimsFromProvenance(provenance: Record<string, unknown>, key: 'rulesClaims' | 'piClaims'): KmShadowComparisonInput['rulesClaims'] {
  const value = provenance[key];
  if (!Array.isArray(value)) return [];
  return value.flatMap(item => {
    if (!item || typeof item !== 'object') return [];
    const claim = item as Record<string, unknown>;
    if (typeof claim.claimKey !== 'string' || !claim.claimKey.trim()) return [];
    return [{
      claimKey: claim.claimKey,
      ...(typeof claim.route === 'string' ? { route: claim.route } : {}),
      ...(Array.isArray(claim.evidenceRefs) ? { evidenceRefs: claim.evidenceRefs } : {}),
      ...(claim.privacyBlocked === true ? { privacyBlocked: true } : {}),
      ...(claim.schemaFailure === true ? { schemaFailure: true } : {}),
    }];
  });
}

/** Default-off local shadow quality pass. It never invokes Pi, external LLMs,
 * network providers, approvals, prompt mutation, or proposal application. */
export async function runKmShadowQualityOnce(input: {
  dataDir: string;
  env?: NodeJS.ProcessEnv;
  holderId?: string;
  maxCases?: number;
  leaseMs?: number;
  thresholds?: Record<string, number>;
}): Promise<KmShadowQualitySummary> {
  const env = input.env ?? process.env;
  const summary: KmShadowQualitySummary = {
    enabled: isKmShadowQualityEnabled(env),
    leaseAcquired: false,
    scannedCases: 0,
    createdComparisons: 0,
    reusedComparisons: 0,
    readinessReady: false,
    readinessReasonCodes: [],
  };
  if (!summary.enabled) return summary;

  const holderId = input.holderId ?? `pid:${process.pid}`;
  const store = await ObservationStore.open(input.dataDir);
  try {
    summary.leaseAcquired = store.acquireRuntimeLease({ leaseName: 'shadow-quality-runtime', holderId, ttlMs: input.leaseMs ?? DEFAULT_LEASE_MS });
    if (!summary.leaseAcquired) return summary;
    const maxCases = Math.max(1, Math.min(input.maxCases ?? positiveEnv('BOTMUX_KM_SHADOW_QUALITY_MAX_CASES', DEFAULT_MAX_CASES, env), 500));
    const cases = store.listGoldenCases({ limit: maxCases, state: 'reviewed' });
    for (const golden of cases) {
      summary.scannedCases += 1;
      const rulesClaims = storedClaimsFromProvenance(golden.provenance, 'rulesClaims');
      const piClaims = storedClaimsFromProvenance(golden.provenance, 'piClaims');
      if (rulesClaims.length === 0 && piClaims.length === 0) continue;
      const result = store.recordShadowComparison({
        caseId: golden.caseId,
        revision: golden.revision,
        rulesClaims,
        piClaims,
        latency: typeof golden.provenance.latency === 'object' && golden.provenance.latency !== null ? golden.provenance.latency as Record<string, unknown> : {},
        cost: typeof golden.provenance.cost === 'object' && golden.provenance.cost !== null ? golden.provenance.cost as Record<string, unknown> : {},
      });
      if (result.created) summary.createdComparisons += 1;
      else summary.reusedComparisons += 1;
    }
    const readiness = store.shadowReadinessReport({ thresholds: input.thresholds });
    summary.readinessReady = readiness.ready;
    summary.readinessReasonCodes = readiness.reasonCodes;
    return summary;
  } finally {
    if (summary.leaseAcquired) store.releaseRuntimeLease({ leaseName: 'shadow-quality-runtime', holderId });
    store.close();
  }
}
