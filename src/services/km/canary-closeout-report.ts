import { createHash } from 'node:crypto';
import { canonicalJsonStringify } from '../../utils/canonical-json.js';
import type {
  KmGoldenCase,
  KmProductionGatePlanRecord,
  KmShadowComparison,
  KmShadowReadinessReport,
  ObservationStore,
} from './observation-store.js';
import { buildKmProductionGatePlan, buildKmProductionGateHandoff } from './production-gate.js';

export const KM_CANARY_CLOSEOUT_REPORT_VERSION = 'km-canary-closeout-report-v1';
export const KM_CANARY_BOT_APP_ID = 'cli_aacca607f9ccdcf8';

export interface KmCanaryCloseoutStore extends Pick<ObservationStore,
  'listGoldenCases' | 'listShadowComparisons' | 'shadowReadinessReportLatest' | 'listRetrievalAudits' | 'listInjectionSnapshots' |
  'listProductionGatePlans' | 'getProductionGateKillState'> {
  listKnowledgeToMemoryImportJobs?(limit: number): ReturnType<ObservationStore['listKnowledgeToMemoryImportJobs']>;
}

export interface KmCanaryCloseoutReportInput {
  store: KmCanaryCloseoutStore;
  botAppId?: string;
  now?: string;
  windowHours?: number;
  reportLimit?: number;
}

export interface KmCanaryCloseoutReport {
  schemaVersion: 1;
  reportVersion: string;
  generatedAt: string;
  botAppId: string;
  reportHash: string;
  baseline: {
    reviewedGoldenCases: number;
    shadowComparisons: number;
    importJobs: Record<string, number>;
    productionGateKillSwitch: ReturnType<ObservationStore['getProductionGateKillState']>;
  };
  bootstrapValidation: {
    reviewedOnly: boolean;
    redactedOnly: boolean;
    reviewedDistillationSourceOnly: boolean;
    rawLeakCount: number;
    sourceRefKindViolations: number;
  };
  readiness: KmShadowReadinessReport | { ready: false; reasonCodes: ['no_readiness_report'] };
  calibration: {
    rules: { truePositive: number; falsePositive: number; falseNegative: number; falsePositiveRate: number; falseNegativeRate: number };
    pi: { truePositive: number; falsePositive: number; falseNegative: number; falsePositiveRate: number; falseNegativeRate: number };
    disagreement: { claimOverlap: number; rulesUnique: number; piUnique: number; routingDisagreement: number; extractorDisagreement: number; extractorDisagreementRate: number };
    reviewLabels: { falsePositive: number; falseNegative: number };
  };
  observation: {
    retrievalRuns: number;
    injectionSnapshots: number;
    wouldInjectSnapshots: number;
    injectedSnapshots: number;
    skippedSnapshots: number;
    unexpectedLiveInjection: number;
    reasonCounts: Record<string, number>;
  };
  productionGate: {
    exactBotOnly: true;
    validActionScopedApprovalPresent: boolean;
    existingPromptCanaryPlans: Array<Pick<KmProductionGatePlanRecord, 'planId' | 'state' | 'previewHash' | 'requiredApprovalGrade' | 'expiresAt'>>;
    previewHandoff: ReturnType<typeof buildKmProductionGateHandoff>;
  };
  canaryPlan: {
    mode: 'preview-inert';
    steps: string[];
    gates: string[];
  };
  rollback: {
    automaticRollback: false;
    steps: string[];
  };
  safety: {
    previewOnly: true;
    noNetwork: true;
    noExternalProviders: true;
    piShadowDisabled: true;
    noLiveInjectionActivatedByReport: true;
    centralKmPageUnchanged: true;
  };
}

function sha256(value: string): string {
  return `sha256:${createHash('sha256').update(value).digest('hex')}`;
}

function exactBotAppId(value: string | undefined): string {
  const botAppId = (value ?? KM_CANARY_BOT_APP_ID).trim();
  if (!botAppId || botAppId.includes('*') || botAppId.toLowerCase() === 'all') throw new Error('km_canary_report_exact_bot_app_id_required');
  if (botAppId !== KM_CANARY_BOT_APP_ID) throw new Error('km_canary_report_unsupported_bot_app_id');
  return botAppId;
}

function hasRawLeak(value: unknown): boolean {
  if (typeof value === 'string') return /<raw_transcript>|<\/raw_transcript>/iu.test(value);
  if (!value || typeof value !== 'object') return false;
  if (Array.isArray(value)) return value.some(hasRawLeak);
  return Object.entries(value as Record<string, unknown>).some(([key, child]) =>
    /^(raw|text|transcript|rawTranscript|content)$/u.test(key) || hasRawLeak(child));
}

function sourceRefKindViolation(golden: KmGoldenCase): boolean {
  return golden.sourceRefs.some(ref => {
    if (!ref || typeof ref !== 'object' || Array.isArray(ref)) return true;
    const kind = (ref as Record<string, unknown>).kind;
    return kind !== 'distillation-example' && kind !== 'reviewed-distillation-example';
  });
}

function sumComparisons(comparisons: KmShadowComparison[]) {
  const totals = {
    expectedCount: 0,
    rulesTruePositive: 0,
    rulesFalsePositive: 0,
    rulesFalseNegative: 0,
    piTruePositive: 0,
    piFalsePositive: 0,
    piFalseNegative: 0,
    claimOverlap: 0,
    rulesUnique: 0,
    piUnique: 0,
    routingDisagreement: 0,
    extractorDisagreement: 0,
    falsePositiveLabels: 0,
    falseNegativeLabels: 0,
  };
  for (const comparison of comparisons) {
    const metrics = comparison.metrics;
    totals.expectedCount += Number(metrics.expectedCount ?? 0);
    totals.rulesTruePositive += Number(metrics.rulesTruePositive ?? 0);
    totals.rulesFalsePositive += Number(metrics.rulesFalsePositive ?? 0);
    totals.rulesFalseNegative += Number(metrics.rulesFalseNegative ?? 0);
    totals.piTruePositive += Number(metrics.piTruePositive ?? 0);
    totals.piFalsePositive += Number(metrics.piFalsePositive ?? 0);
    totals.piFalseNegative += Number(metrics.piFalseNegative ?? 0);
    totals.claimOverlap += Number(metrics.claimOverlap ?? 0);
    totals.rulesUnique += Number(metrics.rulesUnique ?? 0);
    totals.piUnique += Number(metrics.piUnique ?? 0);
    totals.routingDisagreement += Number(metrics.routingDisagreement ?? 0);
    totals.extractorDisagreement += Number(metrics.extractorDisagreement ?? (Number(metrics.rulesUnique ?? 0) + Number(metrics.piUnique ?? 0) + Number(metrics.routingDisagreement ?? 0)));
    totals.falsePositiveLabels += Number(metrics.falsePositiveLabels ?? 0);
    totals.falseNegativeLabels += Number(metrics.falseNegativeLabels ?? 0);
  }
  return totals;
}

function rate(numerator: number, denominator: number): number {
  return Number((numerator / Math.max(1, denominator)).toFixed(4));
}

function countBy<T extends string>(values: T[]): Record<string, number> {
  const result: Record<string, number> = {};
  for (const value of values) result[value] = (result[value] ?? 0) + 1;
  return result;
}

function plannedWindow(now: string, windowHours: number): { start: string; end: string } {
  const start = new Date(now);
  const end = new Date(start.getTime() + Math.max(1, Math.min(Math.trunc(windowHours), 168)) * 60 * 60_000);
  return { start: start.toISOString(), end: end.toISOString() };
}

export function buildKmCanaryCloseoutReport(input: KmCanaryCloseoutReportInput): KmCanaryCloseoutReport {
  const botAppId = exactBotAppId(input.botAppId);
  const generatedAt = new Date(input.now ?? new Date().toISOString()).toISOString();
  const limit = Math.max(1, Math.min(input.reportLimit ?? 100, 500));
  const goldenCases = input.store.listGoldenCases({ limit, state: 'reviewed' });
  const comparisons = input.store.listShadowComparisons({ limit });
  const readiness = input.store.shadowReadinessReportLatest() ?? { ready: false as const, reasonCodes: ['no_readiness_report'] as const };
  const retrievalRuns = input.store.listRetrievalAudits(limit).filter(run => run.botAppId === botAppId);
  const injectionSnapshots = input.store.listInjectionSnapshots(limit).filter(snapshot => snapshot.botAppId === botAppId);
  const promptCanaryPlans = input.store.listProductionGatePlans({ limit, actionKind: 'prompt-canary' })
    .filter(plan => plan.target.botAppId === botAppId && plan.scope.botAppId === botAppId);
  const window = plannedWindow(generatedAt, input.windowHours ?? 24);
  const activeApproval = promptCanaryPlans.some(plan =>
    (plan.state === 'approved' || plan.state === 'executing') && Date.parse(plan.expiresAt) > Date.parse(generatedAt));
  const preview = buildKmProductionGatePlan({
    actionKind: 'prompt-canary',
    target: { botAppId, window },
    scope: { botAppId, sessionClass: 'exact-bot-canary' },
    actorId: 'km-canary-closeout-report',
    riskAck: { acknowledged: true, reportOnly: true, botAppId },
    confirmationToken: 'km-canary-closeout-report-inert-token',
    now: generatedAt,
    ttlSeconds: Math.max(3600, Math.min(Math.trunc((input.windowHours ?? 24) * 3600), 86_400)),
  });
  const totals = sumComparisons(comparisons);
  const importJobs = input.store.listKnowledgeToMemoryImportJobs
    ? countBy(input.store.listKnowledgeToMemoryImportJobs(limit).map(job => String(job.state)))
    : {};
  const observationReasons = injectionSnapshots.map(snapshot => String(snapshot.reason ?? snapshot.disposition));
  const withoutHash = {
    schemaVersion: 1 as const,
    reportVersion: KM_CANARY_CLOSEOUT_REPORT_VERSION,
    generatedAt,
    botAppId,
    baseline: {
      reviewedGoldenCases: goldenCases.length,
      shadowComparisons: comparisons.length,
      importJobs,
      productionGateKillSwitch: input.store.getProductionGateKillState(),
    },
    bootstrapValidation: {
      reviewedOnly: goldenCases.every(item => item.state === 'reviewed'),
      redactedOnly: goldenCases.every(item => !hasRawLeak(item)),
      reviewedDistillationSourceOnly: goldenCases.every(item => !sourceRefKindViolation(item)),
      rawLeakCount: goldenCases.filter(hasRawLeak).length,
      sourceRefKindViolations: goldenCases.filter(sourceRefKindViolation).length,
    },
    readiness,
    calibration: {
      rules: { truePositive: totals.rulesTruePositive, falsePositive: totals.rulesFalsePositive, falseNegative: totals.rulesFalseNegative,
        falsePositiveRate: rate(totals.rulesFalsePositive, totals.rulesTruePositive + totals.rulesFalsePositive), falseNegativeRate: rate(totals.rulesFalseNegative, totals.expectedCount) },
      pi: { truePositive: totals.piTruePositive, falsePositive: totals.piFalsePositive, falseNegative: totals.piFalseNegative,
        falsePositiveRate: rate(totals.piFalsePositive, totals.piTruePositive + totals.piFalsePositive), falseNegativeRate: rate(totals.piFalseNegative, totals.expectedCount) },
      disagreement: { claimOverlap: totals.claimOverlap, rulesUnique: totals.rulesUnique, piUnique: totals.piUnique,
        routingDisagreement: totals.routingDisagreement, extractorDisagreement: totals.extractorDisagreement,
        extractorDisagreementRate: rate(totals.extractorDisagreement, totals.claimOverlap + totals.rulesUnique + totals.piUnique) },
      reviewLabels: { falsePositive: totals.falsePositiveLabels, falseNegative: totals.falseNegativeLabels },
    },
    observation: {
      retrievalRuns: retrievalRuns.length,
      injectionSnapshots: injectionSnapshots.length,
      wouldInjectSnapshots: injectionSnapshots.filter(snapshot => snapshot.disposition === 'would_inject').length,
      injectedSnapshots: injectionSnapshots.filter(snapshot => snapshot.disposition === 'injected').length,
      skippedSnapshots: injectionSnapshots.filter(snapshot => snapshot.disposition === 'skipped').length,
      unexpectedLiveInjection: injectionSnapshots.filter(snapshot => snapshot.disposition === 'injected' && !activeApproval).length,
      reasonCounts: countBy(observationReasons),
    },
    productionGate: {
      exactBotOnly: true as const,
      validActionScopedApprovalPresent: activeApproval,
      existingPromptCanaryPlans: promptCanaryPlans.map(plan => ({ planId: plan.planId, state: plan.state, previewHash: plan.previewHash,
        requiredApprovalGrade: plan.requiredApprovalGrade, expiresAt: plan.expiresAt })),
      previewHandoff: preview.handoff,
    },
    canaryPlan: {
      mode: 'preview-inert' as const,
      steps: [
        'Run shadow retrieval and prompt composition with live injection disabled.',
        'Review golden-case readiness, FP/FN calibration, and disagreement metrics.',
        'Create and approve a unified production-gate prompt-canary plan for the exact bot only.',
        'Activate canary only after the action-scoped approval exists and the allowlist contains exactly this bot app id.',
        'Observe retrieval_runs and prompt_injection_snapshots for would_inject/injected drift.',
      ],
      gates: [
        'BOTMUX_KM_LIVE_INJECTION_ENABLED=true',
        'BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED=true',
        `BOTMUX_KM_CANARY_BOT_APP_IDS=${botAppId}`,
        'stored profile requests canary or active',
        'valid prompt-canary production gate approval for this bot and window',
      ],
    },
    rollback: {
      automaticRollback: false as const,
      steps: [
        'Disable BOTMUX_KM_LIVE_INJECTION_ENABLED first or remove this bot app id from BOTMUX_KM_CANARY_BOT_APP_IDS.',
        'Restart only after operator confirmation for the exact daemon scope.',
        'Keep km observation, retrieval, prompt snapshots, production-gate audit, and golden cases readable for forensic review.',
        'Expire or roll back the production gate plan; do not delete evidence rows.',
      ],
    },
    safety: {
      previewOnly: true as const,
      noNetwork: true as const,
      noExternalProviders: true as const,
      piShadowDisabled: true as const,
      noLiveInjectionActivatedByReport: true as const,
      centralKmPageUnchanged: true as const,
    },
  };
  return { ...withoutHash, reportHash: sha256(canonicalJsonStringify(withoutHash)) };
}

export function renderKmCanaryCloseoutMarkdown(report: KmCanaryCloseoutReport): string {
  const reasons = Object.entries(report.observation.reasonCounts)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `- ${key}: ${value}`)
    .join('\n') || '- none: 0';
  return [
    '# KM Canary Closeout Report',
    '',
    `- reportVersion: ${report.reportVersion}`,
    `- generatedAt: ${report.generatedAt}`,
    `- botAppId: ${report.botAppId}`,
    `- reportHash: ${report.reportHash}`,
    '',
    '## Baseline And Readiness',
    '',
    `- reviewedGoldenCases: ${report.baseline.reviewedGoldenCases}`,
    `- shadowComparisons: ${report.baseline.shadowComparisons}`,
    `- readinessReady: ${report.readiness.ready}`,
    `- readinessReasonCodes: ${report.readiness.reasonCodes.join(', ') || 'none'}`,
    `- productionGateKillSwitch: ${report.baseline.productionGateKillSwitch.enabled ? 'enabled' : 'disabled'}`,
    '',
    '## Bootstrap Import Validation',
    '',
    `- reviewedOnly: ${report.bootstrapValidation.reviewedOnly}`,
    `- redactedOnly: ${report.bootstrapValidation.redactedOnly}`,
    `- reviewedDistillationSourceOnly: ${report.bootstrapValidation.reviewedDistillationSourceOnly}`,
    `- rawLeakCount: ${report.bootstrapValidation.rawLeakCount}`,
    `- sourceRefKindViolations: ${report.bootstrapValidation.sourceRefKindViolations}`,
    '',
    '## FP/FN And Disagreement Calibration',
    '',
    `- rules: TP ${report.calibration.rules.truePositive}, FP ${report.calibration.rules.falsePositive} (${report.calibration.rules.falsePositiveRate}), FN ${report.calibration.rules.falseNegative} (${report.calibration.rules.falseNegativeRate})`,
    `- pi: TP ${report.calibration.pi.truePositive}, FP ${report.calibration.pi.falsePositive} (${report.calibration.pi.falsePositiveRate}), FN ${report.calibration.pi.falseNegative} (${report.calibration.pi.falseNegativeRate})`,
    `- disagreement: overlap ${report.calibration.disagreement.claimOverlap}, rulesUnique ${report.calibration.disagreement.rulesUnique}, piUnique ${report.calibration.disagreement.piUnique}, routing ${report.calibration.disagreement.routingDisagreement}, rate ${report.calibration.disagreement.extractorDisagreementRate}`,
    '',
    '## Canary Observation',
    '',
    `- retrievalRuns: ${report.observation.retrievalRuns}`,
    `- injectionSnapshots: ${report.observation.injectionSnapshots}`,
    `- wouldInjectSnapshots: ${report.observation.wouldInjectSnapshots}`,
    `- injectedSnapshots: ${report.observation.injectedSnapshots}`,
    `- unexpectedLiveInjection: ${report.observation.unexpectedLiveInjection}`,
    reasons,
    '',
    '## Production Gate',
    '',
    `- exactBotOnly: ${report.productionGate.exactBotOnly}`,
    `- validActionScopedApprovalPresent: ${report.productionGate.validActionScopedApprovalPresent}`,
    `- previewPlanId: ${report.productionGate.previewHandoff.planId}`,
    `- previewHash: ${report.productionGate.previewHandoff.previewHash}`,
    `- requiredApprovalGrade: ${report.productionGate.previewHandoff.requiredApprovalGrade}`,
    `- effective: ${report.productionGate.previewHandoff.effective}`,
    `- sideEffectsExecuted: ${report.productionGate.previewHandoff.sideEffectsExecuted}`,
    '',
    '## Rollback',
    '',
    ...report.rollback.steps.map(step => `- ${step}`),
    '',
    '## Safety',
    '',
    `- previewOnly: ${report.safety.previewOnly}`,
    `- noNetwork: ${report.safety.noNetwork}`,
    `- noExternalProviders: ${report.safety.noExternalProviders}`,
    `- piShadowDisabled: ${report.safety.piShadowDisabled}`,
    `- noLiveInjectionActivatedByReport: ${report.safety.noLiveInjectionActivatedByReport}`,
    '',
  ].join('\n');
}
