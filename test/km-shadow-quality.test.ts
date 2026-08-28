import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { isKmShadowQualityEnabled, runKmShadowQualityOnce } from '../src/services/km/shadow-quality-runtime.js';

const dirs: string[] = [];
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-shadow-quality-'));
  dirs.push(dir);
  return dir;
}
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const claimHash = `sha256:${'a'.repeat(64)}`;
const reviewedDistillationProvenance = {
  explicitlyReviewed: true,
  redactionStatus: 'redacted',
  rulesClaims: [{ claimKey: 'response.language', route: 'memory:user', evidenceRefs: [{ kind: 'golden-case', ref: 'example-1' }] }],
  piClaims: [{ claimKey: 'response.language', route: 'memory:user', evidenceRefs: [{ kind: 'golden-case', ref: 'example-1' }] }],
  latency: { rulesMs: 3, piMs: 5 },
  cost: { piInvoked: false, externalCalls: 0 },
};

async function createGolden(dir: string) {
  const store = await ObservationStore.open(dir);
  const result = store.upsertGoldenCase({
    title: 'Chinese reply preference',
    queryRedacted: '以后请用中文回复',
    expectedClaims: [{ claimKey: 'response.language', claimTextHash: claimHash }],
    sourceRefs: [{ kind: 'distillation-example', ref: 'example-1', sha256: `sha256:${'b'.repeat(64)}` }],
    provenance: reviewedDistillationProvenance,
    actorId: 'reviewer-1',
  });
  store.close();
  return result.item;
}

describe('KM golden set and Pi shadow quality', () => {
  it('migrates schema v15 and keeps the shadow quality scheduler default-off', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    expect(store.schemaVersion()).toBe(17);
    store.close();
    expect(isKmShadowQualityEnabled({} as NodeJS.ProcessEnv)).toBe(false);
    await expect(runKmShadowQualityOnce({ dataDir: dir, env: {} as NodeJS.ProcessEnv })).resolves.toEqual({
      enabled: false,
      leaseAcquired: false,
      scannedCases: 0,
      createdComparisons: 0,
      reusedComparisons: 0,
      readinessReady: false,
      readinessReasonCodes: [],
    });
  });

  it('stores only reviewed and redacted golden cases with immutable revisions and content hashes', async () => {
    const dir = tempDir();
    const golden = await createGolden(dir);
    const store = await ObservationStore.open(dir);
    expect(golden).toEqual(expect.objectContaining({
      revision: 1,
      state: 'reviewed',
      queryHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
      contentHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
      reviewedBy: 'reviewer-1',
    }));
    const duplicate = store.upsertGoldenCase({
      title: 'Chinese reply preference',
      queryRedacted: '以后请用中文回复',
      expectedClaims: [{ claimKey: 'response.language', claimTextHash: claimHash }],
      sourceRefs: [{ kind: 'distillation-example', ref: 'example-1', sha256: `sha256:${'b'.repeat(64)}` }],
      provenance: reviewedDistillationProvenance,
      actorId: 'reviewer-1',
    });
    expect(duplicate).toEqual(expect.objectContaining({ created: false }));
    expect(duplicate.item).toEqual(expect.objectContaining({ revision: 1, contentHash: golden.contentHash }));
    expect(JSON.stringify(store.listGoldenCases({ limit: 10 }))).not.toContain('<raw_transcript>');
    expect(() => store.upsertGoldenCase({
      title: 'Unsafe',
      queryRedacted: 'redacted',
      expectedClaims: [{ claimKey: 'unsafe', claimTextHash: claimHash }],
      sourceRefs: [{ kind: 'distillation-example', ref: 'example-2' }],
      provenance: { explicitlyReviewed: true, redactionStatus: 'redacted', rawTranscript: 'secret' },
      actorId: 'reviewer-1',
    })).toThrow(/raw_text_field/);
    expect(() => store.upsertGoldenCase({
      title: 'Unsafe',
      queryRedacted: 'redacted',
      expectedClaims: [{ claimKey: 'unsafe', claimTextHash: claimHash }],
      sourceRefs: [{ kind: 'api', ref: 'example-2' }],
      provenance: { explicitlyReviewed: true, redactionStatus: 'redacted' },
      actorId: 'reviewer-1',
    })).toThrow(/source_ref_not_reviewed_distillation_example/);
    expect(() => store.upsertGoldenCase({
      title: 'Unsafe',
      queryRedacted: 'redacted',
      expectedClaims: [{ claimKey: 'unsafe', claimTextHash: claimHash }],
      sourceRefs: [{ kind: 'distillation-example', ref: 'example-2' }],
      provenance: { explicitlyReviewed: false, redactionStatus: 'redacted' },
      actorId: 'reviewer-1',
    })).toThrow(/reviewed_redacted/);
    store.close();
  });

  it('records idempotent local-only comparisons, labels, and deterministic readiness metrics', async () => {
    const dir = tempDir();
    const golden = await createGolden(dir);
    const store = await ObservationStore.open(dir);
    const first = store.recordShadowComparison({
      caseId: golden.caseId,
      revision: golden.revision,
      rulesClaims: [
        { claimKey: 'response.language', route: 'memory:user', evidenceRefs: [{ kind: 'golden-case', ref: `${golden.caseId}@${golden.revision}` }] },
        { claimKey: 'rules.only', route: 'knowledge:L2', evidenceRefs: [{ kind: 'golden-case', ref: `${golden.caseId}@${golden.revision}` }] },
      ],
      piClaims: [
        { claimKey: 'response.language', route: 'knowledge:L2', evidenceRefs: [{ kind: 'golden-case', ref: `${golden.caseId}@${golden.revision}` }] },
        { claimKey: 'pi.only', route: 'memory:user', schemaFailure: true },
      ],
      latency: { rulesMs: 2, piMs: 4 },
      cost: { piInvoked: false, externalCalls: 0 },
    });
    const second = store.recordShadowComparison({
      caseId: golden.caseId,
      revision: golden.revision,
      rulesClaims: first.item.rulesClaims,
      piClaims: first.item.piClaims,
      latency: { rulesMs: 2, piMs: 4 },
      cost: { piInvoked: false, externalCalls: 0 },
    });
    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
    expect(first.item.metrics).toEqual(expect.objectContaining({
      claimOverlap: 1,
      rulesUnique: 1,
      piUnique: 1,
      routingDisagreement: 1,
      evidenceCoverage: 0.75,
      schemaFailures: 1,
    }));
    expect(store.addShadowReviewLabel({
      comparisonId: first.item.comparisonId,
      claimKey: 'pi.only',
      extractor: 'pi',
      label: 'false_positive',
      actorId: 'reviewer-1',
      reasonCode: 'bad_claim',
    }).created).toBe(true);
    expect(store.addShadowReviewLabel({
      comparisonId: first.item.comparisonId,
      claimKey: 'pi.only',
      extractor: 'pi',
      label: 'false_positive',
      actorId: 'reviewer-1',
      reasonCode: 'bad_claim',
    }).created).toBe(false);
    const relabeled = store.getShadowComparison(first.item.comparisonId)!;
    expect(relabeled.metrics.falsePositiveLabels).toBe(1);
    const blocked = store.shadowReadinessReport({ thresholds: { minReviewedCases: 1, minComparisons: 1, maxSchemaFailures: 0, maxFalsePositiveLabels: 0 } });
    expect(blocked.ready).toBe(false);
    expect(blocked.reasonCodes).toEqual(expect.arrayContaining(['schema_failures_above_threshold', 'false_positive_labels_above_threshold']));
    const latest = store.shadowReadinessReportLatest();
    expect(latest?.windowHash).toBe(blocked.windowHash);
    store.close();
  });

  it('runs bounded default-off shadow quality from stored outputs only and respects the runtime lease', async () => {
    const dir = tempDir();
    await createGolden(dir);
    const leaseStore = await ObservationStore.open(dir);
    expect(leaseStore.acquireRuntimeLease({ leaseName: 'shadow-quality-runtime', holderId: 'other', now: Date.now(), ttlMs: 60_000 })).toBe(true);
    leaseStore.close();
    await expect(runKmShadowQualityOnce({ dataDir: dir, env: { BOTMUX_KM_SHADOW_QUALITY_ENABLED: 'true' } as NodeJS.ProcessEnv, holderId: 'worker' }))
      .resolves.toEqual(expect.objectContaining({ enabled: true, leaseAcquired: false, scannedCases: 0, createdComparisons: 0 }));
    const releaseStore = await ObservationStore.open(dir);
    releaseStore.releaseRuntimeLease({ leaseName: 'shadow-quality-runtime', holderId: 'other' });
    releaseStore.close();

    const result = await runKmShadowQualityOnce({ dataDir: dir, env: { BOTMUX_KM_SHADOW_QUALITY_ENABLED: 'true' } as NodeJS.ProcessEnv, holderId: 'worker' });
    expect(result).toEqual(expect.objectContaining({ enabled: true, leaseAcquired: true, scannedCases: 1, createdComparisons: 1 }));
    const rerun = await runKmShadowQualityOnce({ dataDir: dir, env: { BOTMUX_KM_SHADOW_QUALITY_ENABLED: 'true' } as NodeJS.ProcessEnv, holderId: 'worker' });
    expect(rerun).toEqual(expect.objectContaining({ createdComparisons: 0, reusedComparisons: 1 }));
    const store = await ObservationStore.open(dir);
    expect(store.listShadowComparisons({ limit: 10 })[0].cost).toEqual({ piInvoked: false, externalCalls: 0 });
    store.close();
  });

  it('keeps the shadow quality runtime free of Pi, LLM, network, and process execution hooks', () => {
    const source = readFileSync(new URL('../src/services/km/shadow-quality-runtime.ts', import.meta.url), 'utf8');
    expect(source).not.toMatch(/PiDistillationExecutor|runCliDistillation|BOTMUX_KM_PI_SHADOW_ENABLED/u);
    expect(source).not.toMatch(/\bfetch\s*\(|from 'node:child_process'|from 'node:http'|from 'node:https'/u);
    expect(source).not.toMatch(/createEvolutionProposal|decideProposal|putPipelineProfile|setPipelineProfileState/u);
  });
});
