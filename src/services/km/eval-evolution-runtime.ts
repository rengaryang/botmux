import type { EvalResultInput, EvolutionProposalInput, KmEvalMetricWindow, KmEvalTarget } from './observation-store.js';
import { ObservationStore } from './observation-store.js';
import { evaluateArtifactCompleteness } from './artifact-completeness-evaluator.js';

const EVALUATOR_VERSION = 'v1';
const DEFAULT_MAX_TARGETS = 100;
const DEFAULT_MIN_EVIDENCE = 3;
const DEFAULT_FAIL_RATIO = 0.34;
const DEFAULT_LEASE_MS = 45_000;

function envOn(name: string, env: NodeJS.ProcessEnv = process.env): boolean {
  return ['1', 'true', 'yes'].includes(env[name]?.trim().toLowerCase() ?? '');
}

export function isKmAutoEvalEnabled(env = process.env): boolean {
  return envOn('BOTMUX_KM_AUTO_EVAL_ENABLED', env);
}

export function isKmAutoEvolutionEnabled(env = process.env): boolean {
  return envOn('BOTMUX_KM_AUTO_EVOLUTION_ENABLED', env);
}

function numberEnv(name: string, fallback: number, env: NodeJS.ProcessEnv): number {
  const value = Number(env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function metric(metricKey: string, ok: boolean, details: Record<string, unknown>, sourceRef: unknown): EvalResultInput {
  return {
    metricKey,
    score: ok ? 1 : 0,
    verdict: ok ? 'pass' : 'fail',
    confidence: 'observed',
    details,
    sourceRefs: [sourceRef],
  };
}

function warn(metricKey: string, details: Record<string, unknown>, sourceRef: unknown): EvalResultInput {
  return { metricKey, score: 0.5, verdict: 'warn', confidence: 'observed', details, sourceRefs: [sourceRef] };
}

function evaluateDistillation(target: KmEvalTarget): { evaluatorName: string; results: EvalResultInput[] } {
  const state = String(target.payload.state ?? '');
  const attempts = Number(target.payload.attempts ?? 0);
  const outputHash = typeof target.payload.outputHash === 'string' ? target.payload.outputHash : '';
  const hasTerminalEvidence = ['completed', 'inconclusive', 'failed', 'quarantined'].includes(state);
  return {
    evaluatorName: 'km.distillation-quality',
    results: [
      metric('distillation.terminal_state.valid', hasTerminalEvidence, { state }, target.sourceRef),
      metric('distillation.output_hash.present', state !== 'completed' || /^sha256:[a-f0-9]{64}$/u.test(outputHash), { state, outputHash: outputHash || null }, target.sourceRef),
      attempts > 2
        ? warn('distillation.retry_budget.stressed', { attempts }, target.sourceRef)
        : metric('distillation.retry_budget.ok', true, { attempts }, target.sourceRef),
    ],
  };
}

function evaluateRetrieval(target: KmEvalTarget): { evaluatorName: string; results: EvalResultInput[] } {
  const candidateCount = Number(target.payload.candidateCount ?? 0);
  const eligibleCount = Number(target.payload.eligibleCount ?? 0);
  const latencyMs = Number(target.payload.latencyMs ?? 0);
  const warnings = Array.isArray(target.payload.warnings) ? target.payload.warnings.map(String) : [];
  return {
    evaluatorName: 'km.retrieval-quality',
    results: [
      metric('retrieval.candidates.present', candidateCount > 0, { candidateCount }, target.sourceRef),
      metric('retrieval.eligible.not_exceed_candidates', eligibleCount <= candidateCount, { candidateCount, eligibleCount }, target.sourceRef),
      latencyMs > 2_000
        ? warn('retrieval.latency.within_budget', { latencyMs, budgetMs: 2_000 }, target.sourceRef)
        : metric('retrieval.latency.within_budget', true, { latencyMs, budgetMs: 2_000 }, target.sourceRef),
      metric('retrieval.warnings.absent', warnings.length === 0, { warnings }, target.sourceRef),
    ],
  };
}

function evaluateInjection(target: KmEvalTarget): { evaluatorName: string; results: EvalResultInput[] } {
  const requestedMode = String(target.payload.requestedMode ?? target.payload.mode ?? '');
  const effectiveMode = String(target.payload.effectiveMode ?? target.payload.mode ?? '');
  const disposition = String(target.payload.disposition ?? '');
  const promptBytes = Number(target.payload.promptBytes ?? 0);
  const unsafeLiveWithoutGate = ['active', 'canary'].includes(requestedMode) && effectiveMode === requestedMode && disposition === 'injected';
  return {
    evaluatorName: 'km.injection-safety',
    results: [
      metric('injection.fail_closed.shadow_boundary', !unsafeLiveWithoutGate, { requestedMode, effectiveMode, disposition }, target.sourceRef),
      metric('injection.prompt_size.bounded', promptBytes <= 128_000, { promptBytes, budgetBytes: 128_000 }, target.sourceRef),
    ],
  };
}

function evaluateMemoryPolicy(target: KmEvalTarget): { evaluatorName: string; results: EvalResultInput[] } {
  const disposition = String(target.payload.disposition ?? '');
  const reasonCodes = Array.isArray(target.payload.reasonCodes) ? target.payload.reasonCodes.map(String) : [];
  const evidence = typeof target.payload.evidence === 'object' && target.payload.evidence !== null
    ? target.payload.evidence as Record<string, unknown>
    : {};
  const activatedWithSafeReason = disposition !== 'activate'
    || reasonCodes.includes('explicit_observed_low_risk_preference');
  return {
    evaluatorName: 'km.memory-policy-quality',
    results: [
      metric('memory_policy.evidence.present', Object.keys(evidence).length > 0, { evidenceKeys: Object.keys(evidence).sort() }, target.sourceRef),
      metric('memory_policy.activation.fail_closed', activatedWithSafeReason, { disposition, reasonCodes }, target.sourceRef),
    ],
  };
}

function evaluateWorkflowArtifact(target: KmEvalTarget): { evaluatorName: string; results: EvalResultInput[] } {
  return {
    evaluatorName: 'km.workflow-artifact-quality',
    results: evaluateArtifactCompleteness({
      outputKey: typeof target.payload.outputKey === 'string' ? target.payload.outputKey : undefined,
      relativePath: typeof target.payload.path === 'string' ? target.payload.path : undefined,
      kind: typeof target.payload.kind === 'string' ? target.payload.kind : undefined,
      bytes: typeof target.payload.bytes === 'number' ? target.payload.bytes : undefined,
      sha256: typeof target.payload.sha256 === 'string' ? target.payload.sha256 : undefined,
      promptRequirements: Array.isArray(target.payload.promptRequirements) ? target.payload.promptRequirements.map(String) : [],
      coveredRequirements: Array.isArray(target.payload.coveredRequirements) ? target.payload.coveredRequirements.map(String) : [],
      sourceRef: target.sourceRef,
    }),
  };
}

export function evaluateKmTarget(target: KmEvalTarget): { evaluatorName: string; evaluatorVersion: string; targetType: KmEvalTarget['targetType']; targetId: string; results: EvalResultInput[] } {
  const evaluated = target.sourceKind === 'distillation-job' ? evaluateDistillation(target)
    : target.sourceKind === 'retrieval-run' ? evaluateRetrieval(target)
      : target.sourceKind === 'prompt-injection' ? evaluateInjection(target)
        : target.sourceKind === 'memory-policy-decision' ? evaluateMemoryPolicy(target)
          : evaluateWorkflowArtifact(target);
  return {
    evaluatorName: evaluated.evaluatorName,
    evaluatorVersion: EVALUATOR_VERSION,
    targetType: target.targetType,
    targetId: target.targetId,
    results: evaluated.results,
  };
}

const PROPOSAL_METRICS = [
  'artifact.prompt_coverage.complete',
  'artifact.relative_path.safe',
  'artifact.sha256.valid',
  'distillation.output_hash.present',
  'retrieval.candidates.present',
  'retrieval.warnings.absent',
  'memory_policy.activation.fail_closed',
  'injection.fail_closed.shadow_boundary',
] as const;

function proposalForWindow(window: KmEvalMetricWindow): EvolutionProposalInput {
  const targetRef = `km-quality:${window.metricKey}`;
  const evidenceRefs = [{
    kind: 'sqlite-row',
    ref: `eval_results:${window.metricKey}:${window.windowHash}`,
    sha256: window.windowHash,
    failedTargetIds: window.failedTargetIds,
  }];
  return {
    proposalType: window.metricKey.startsWith('artifact.') ? 'workflow-revision'
      : window.metricKey.startsWith('memory_policy.') ? 'memory-policy'
        : window.metricKey.startsWith('retrieval.') ? 'dashboard-warning'
          : window.metricKey.startsWith('distillation.') ? 'knowledge-promotion'
            : 'dashboard-warning',
    targetRef,
    approvalGrade: window.metricKey === 'injection.fail_closed.shadow_boundary' ? 'G3' : 'G2',
    summary: `KM quality threshold failed: ${window.metricKey} (${window.failCount}/${window.totalCount})`,
    evidenceRefs,
    proposedAction: {
      kind: 'review-km-quality-threshold',
      metricKey: window.metricKey,
      failRatio: window.failRatio,
      failedTargetIds: window.failedTargetIds,
      automaticExecution: false,
    },
    risk: {
      mutatesWorkspace: false,
      mutatesRuntimeConfig: false,
      requiresHumanApproval: true,
      reason: 'proposal_only_no_auto_apply',
    },
    rollback: {
      kind: 'mark-proposal-rejected',
      noRuntimeMutationToUndo: true,
    },
    createdBy: 'km-evolution-planner-v1',
  };
}

export interface KmEvalEvolutionSummary {
  evalEnabled: boolean;
  evolutionEnabled: boolean;
  leaseAcquired: boolean;
  evaluatedTargets: number;
  createdEvalRuns: number;
  skippedExistingEvalRuns: number;
  consideredWindows: number;
  createdProposals: number;
  reusedProposals: number;
  insufficientEvidence: boolean;
}

export async function runKmEvalEvolutionOnce(input: {
  dataDir: string;
  env?: NodeJS.ProcessEnv;
  holderId?: string;
  maxTargets?: number;
  minEvidence?: number;
  failRatioThreshold?: number;
  leaseMs?: number;
}): Promise<KmEvalEvolutionSummary> {
  const env = input.env ?? process.env;
  const evalEnabled = isKmAutoEvalEnabled(env);
  const evolutionEnabled = isKmAutoEvolutionEnabled(env);
  const summary: KmEvalEvolutionSummary = {
    evalEnabled,
    evolutionEnabled,
    leaseAcquired: false,
    evaluatedTargets: 0,
    createdEvalRuns: 0,
    skippedExistingEvalRuns: 0,
    consideredWindows: 0,
    createdProposals: 0,
    reusedProposals: 0,
    insufficientEvidence: false,
  };
  if (!evalEnabled) return summary;

  const holderId = input.holderId ?? `pid:${process.pid}`;
  const leaseMs = input.leaseMs ?? DEFAULT_LEASE_MS;
  const store = await ObservationStore.open(input.dataDir);
  try {
    summary.leaseAcquired = store.acquireRuntimeLease({ leaseName: 'eval-evolution-runtime', holderId, ttlMs: leaseMs });
    if (!summary.leaseAcquired) return summary;
    const maxTargets = Math.max(1, Math.min(input.maxTargets ?? numberEnv('BOTMUX_KM_AUTO_EVAL_MAX_TARGETS', DEFAULT_MAX_TARGETS, env), 500));
    for (const target of store.listPendingEvalTargets({ limit: maxTargets })) {
      const evaluated = evaluateKmTarget(target);
      const result = store.recordEval(evaluated);
      summary.evaluatedTargets += 1;
      if (result.created) summary.createdEvalRuns += 1;
      else summary.skippedExistingEvalRuns += 1;
    }

    const minEvidence = Math.max(1, Math.floor(input.minEvidence ?? numberEnv('BOTMUX_KM_EVOLUTION_MIN_EVIDENCE', DEFAULT_MIN_EVIDENCE, env)));
    const failRatioThreshold = Math.min(1, Math.max(0, input.failRatioThreshold ?? numberEnv('BOTMUX_KM_EVOLUTION_FAIL_RATIO', DEFAULT_FAIL_RATIO, env)));
    const windows = store.evalMetricWindows({ metricKeys: [...PROPOSAL_METRICS], minCount: minEvidence });
    summary.consideredWindows = windows.length;
    summary.insufficientEvidence = windows.length === 0;
    if (evolutionEnabled) {
      for (const window of windows) {
        if (window.failCount === 0 || window.failRatio < failRatioThreshold) continue;
        const result = store.createEvolutionProposalOnce(proposalForWindow(window));
        if (result.created) summary.createdProposals += 1;
        else summary.reusedProposals += 1;
      }
    }
    return summary;
  } finally {
    if (summary.leaseAcquired) store.releaseRuntimeLease({ leaseName: 'eval-evolution-runtime', holderId });
    store.close();
  }
}
