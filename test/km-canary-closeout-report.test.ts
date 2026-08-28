import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  KM_CANARY_BOT_APP_ID,
  buildKmCanaryCloseoutReport,
  renderKmCanaryCloseoutMarkdown,
} from '../src/services/km/canary-closeout-report.js';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { buildKmProductionGatePlan, approveKmProductionGatePlan } from '../src/services/km/production-gate.js';

const dirs: string[] = [];
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-canary-closeout-'));
  dirs.push(dir);
  return dir;
}

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const claimHash = `sha256:${'a'.repeat(64)}`;

describe('KM canary closeout report', () => {
  it('summarizes reviewed golden bootstrap, FP/FN calibration, observation, and inert exact-bot gate plan', async () => {
    const store = await ObservationStore.open(tempDir());
    const golden = store.upsertGoldenCase({
      title: 'Chinese reply preference',
      queryRedacted: '以后请用中文回复',
      expectedClaims: [
        { claimKey: 'response.language', claimTextHash: claimHash },
        { claimKey: 'response.verbosity', claimTextHash: `sha256:${'b'.repeat(64)}` },
      ],
      sourceRefs: [{ kind: 'reviewed-distillation-example', ref: 'case-1', sha256: `sha256:${'c'.repeat(64)}` }],
      provenance: {
        explicitlyReviewed: true,
        redactionStatus: 'redacted',
        rulesClaims: [
          { claimKey: 'response.language', route: 'memory:user', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] },
          { claimKey: 'rules.extra', route: 'knowledge:L2', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] },
        ],
        piClaims: [
          { claimKey: 'response.language', route: 'knowledge:L2', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] },
          { claimKey: 'pi.extra', route: 'memory:user', schemaFailure: true },
        ],
        latency: { rulesMs: 1, piMs: 2 },
        cost: { piInvoked: false, externalCalls: 0 },
      },
      actorId: 'reviewer-1',
    }).item;
    const comparison = store.recordShadowComparison({
      caseId: golden.caseId,
      revision: golden.revision,
      rulesClaims: [
        { claimKey: 'response.language', route: 'memory:user', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] },
        { claimKey: 'rules.extra', route: 'knowledge:L2', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] },
      ],
      piClaims: [
        { claimKey: 'response.language', route: 'knowledge:L2', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] },
        { claimKey: 'pi.extra', route: 'memory:user', schemaFailure: true },
      ],
      cost: { piInvoked: false, externalCalls: 0 },
    }).item;
    store.addShadowReviewLabel({
      comparisonId: comparison.comparisonId,
      claimKey: 'pi.extra',
      extractor: 'pi',
      label: 'false_positive',
      actorId: 'reviewer-1',
      reasonCode: 'not_expected',
    });
    store.shadowReadinessReport({ thresholds: { minReviewedCases: 1, minComparisons: 1, maxSchemaFailures: 0 } });
    const retrievalRunId = store.recordRetrievalAudit({
      botAppId: KM_CANARY_BOT_APP_ID,
      sessionId: 'session-1',
      turnId: 'turn-1',
      queryHash: `sha256:${'d'.repeat(64)}`,
      mode: 'shadow',
      candidateCount: 1,
      eligibleCount: 1,
      latencyMs: 5,
      warnings: [],
      results: [{ itemId: 'mem-1', itemKind: 'memory', providerIds: ['sqlite'], score: 1, eligible: true }],
    });
    store.recordPromptInjectionSnapshot({
      retrievalRunId,
      botAppId: KM_CANARY_BOT_APP_ID,
      mode: 'shadow',
      requestedMode: 'canary',
      effectiveMode: 'shadow',
      disposition: 'would_inject',
      itemIds: ['mem-1'],
      prompt: '<botmux_km_context>redacted</botmux_km_context>',
      reason: 'live_gate_disabled',
    });
    const gate = buildKmProductionGatePlan({
      actionKind: 'prompt-canary',
      target: { botAppId: KM_CANARY_BOT_APP_ID, window: { start: '2026-08-28T00:00:00.000Z', end: '2026-08-28T01:00:00.000Z' } },
      scope: { botAppId: KM_CANARY_BOT_APP_ID, sessionClass: 'manual-canary' },
      actorId: 'operator-1',
      riskAck: { acknowledged: true, ticket: 'KM-CANARY' },
      confirmationToken: 'approval-token',
      now: '2026-08-28T00:00:00.000Z',
    });
    store.createProductionGatePlan(gate.plan);
    approveKmProductionGatePlan(store, {
      planId: gate.plan.planId,
      actorId: 'operator-2',
      approvalGrade: 'G2',
      confirmationToken: 'approval-token',
      previewHash: gate.plan.previewHash,
      riskAck: { acknowledged: true, ticket: 'KM-CANARY' },
      now: '2026-08-28T00:00:10.000Z',
    });

    const report = buildKmCanaryCloseoutReport({ store, now: '2026-08-28T00:10:00.000Z', windowHours: 24 });
    expect(report.botAppId).toBe(KM_CANARY_BOT_APP_ID);
    expect(report.baseline).toEqual(expect.objectContaining({ reviewedGoldenCases: 1, shadowComparisons: 1 }));
    expect(report.bootstrapValidation).toEqual(expect.objectContaining({
      reviewedOnly: true,
      redactedOnly: true,
      reviewedDistillationSourceOnly: true,
      rawLeakCount: 0,
    }));
    expect(report.calibration.rules).toEqual(expect.objectContaining({ truePositive: 1, falsePositive: 1, falseNegative: 1 }));
    expect(report.calibration.pi).toEqual(expect.objectContaining({ truePositive: 1, falsePositive: 1, falseNegative: 1 }));
    expect(report.calibration.disagreement).toEqual(expect.objectContaining({ routingDisagreement: 1, extractorDisagreement: 3 }));
    expect(report.observation).toEqual(expect.objectContaining({
      retrievalRuns: 1,
      injectionSnapshots: 1,
      wouldInjectSnapshots: 1,
      injectedSnapshots: 0,
      unexpectedLiveInjection: 0,
    }));
    expect(report.productionGate).toEqual(expect.objectContaining({
      exactBotOnly: true,
      validActionScopedApprovalPresent: true,
    }));
    expect(report.productionGate.previewHandoff).toEqual(expect.objectContaining({
      actionKind: 'prompt-canary',
      effective: false,
      sideEffectsExecuted: false,
    }));
    expect(report.safety).toEqual(expect.objectContaining({
      previewOnly: true,
      noNetwork: true,
      noExternalProviders: true,
      piShadowDisabled: true,
      noLiveInjectionActivatedByReport: true,
    }));
    expect(JSON.stringify(report)).not.toContain('<botmux_km_context>');
    expect(renderKmCanaryCloseoutMarkdown(report)).toContain('## FP/FN And Disagreement Calibration');
    store.close();
  });

  it('does not treat expired prompt-canary approval as valid action-scoped approval', async () => {
    const store = await ObservationStore.open(tempDir());
    const gate = buildKmProductionGatePlan({
      actionKind: 'prompt-canary',
      target: { botAppId: KM_CANARY_BOT_APP_ID, window: { start: '2026-08-28T00:00:00.000Z', end: '2026-08-28T01:00:00.000Z' } },
      scope: { botAppId: KM_CANARY_BOT_APP_ID, sessionClass: 'manual-canary' },
      actorId: 'operator-1',
      riskAck: { acknowledged: true, ticket: 'KM-CANARY' },
      confirmationToken: 'approval-token',
      now: '2026-08-28T00:00:00.000Z',
      ttlSeconds: 60,
    });
    store.createProductionGatePlan(gate.plan);
    approveKmProductionGatePlan(store, {
      planId: gate.plan.planId,
      actorId: 'operator-2',
      approvalGrade: 'G2',
      confirmationToken: 'approval-token',
      previewHash: gate.plan.previewHash,
      riskAck: { acknowledged: true, ticket: 'KM-CANARY' },
      now: '2026-08-28T00:00:10.000Z',
    });

    const report = buildKmCanaryCloseoutReport({ store, now: '2026-08-28T00:02:00.000Z' });
    expect(report.productionGate.validActionScopedApprovalPresent).toBe(false);
    store.close();
  });

  it('rejects wildcard and non-target bot ids', async () => {
    const store = await ObservationStore.open(tempDir());
    expect(() => buildKmCanaryCloseoutReport({ store, botAppId: '*' })).toThrow(/exact_bot_app_id/);
    expect(() => buildKmCanaryCloseoutReport({ store, botAppId: 'cli_other' })).toThrow(/unsupported_bot_app_id/);
    store.close();
  });
});
