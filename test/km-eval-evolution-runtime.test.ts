import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  isKmAutoEvalEnabled,
  isKmAutoEvolutionEnabled,
  runKmEvalEvolutionOnce,
} from '../src/services/km/eval-evolution-runtime.js';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { defaultShadowProfile } from '../src/services/km/runtime-orchestrator.js';

const dirs: string[] = [];
let retrievalSeq = 0;
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-eval-evolution-'));
  dirs.push(dir);
  return dir;
}
afterEach(() => {
  retrievalSeq = 0;
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function recordRetrieval(store: ObservationStore, input: { candidateCount: number; warnings?: string[] }): void {
  retrievalSeq += 1;
  store.recordRetrievalAudit({
    botAppId: 'bot',
    sessionId: `session-${retrievalSeq}`,
    queryHash: `sha256:${'a'.repeat(64)}`,
    mode: 'shadow',
    candidateCount: input.candidateCount,
    eligibleCount: 0,
    latencyMs: 10,
    warnings: input.warnings ?? [],
    results: [],
  });
}

function artifactEvent(overrides: Partial<ObservationEvent> = {}): ObservationEvent {
  const hash = `sha256:${'b'.repeat(64)}`;
  return {
    schemaVersion: 1,
    eventId: 'evt-artifact-1',
    eventType: 'workflow.artifact.produced',
    source: {
      producer: 'workflow',
      adapter: 'v3',
      resolverStatus: 'resolved',
      confidence: 'observed',
    },
    identity: {
      botAppId: 'bot',
      sessionId: 'session-artifact',
      workflowId: 'wf',
      nodeId: 'node',
      attemptId: 'attempt',
    },
    ordering: {
      sourceKey: 'workflow:wf/node',
      idempotencyKey: 'wf/node/attempt/report',
      parentEventIds: [],
      observedAt: '2026-08-27T00:00:00.000Z',
    },
    provenance: {
      evidenceLevel: 'workflow-artifact',
      parserVersion: 'v1',
      sourceRefs: [{ kind: 'workflow-artifact', ref: 'wf/node/report.md', sha256: hash }],
      privacyClass: 'internal',
      redactionStatus: 'not_needed',
    },
    content: {
      hash,
      storageMode: 'local_blob',
      ref: 'report.md',
    },
    payload: {
      outputKey: 'report',
      path: 'report.md',
      kind: 'markdown',
      bytes: 120,
      sha256: 'b'.repeat(64),
      promptRequirements: ['summary', 'tests'],
      coveredRequirements: ['summary', 'tests'],
    },
    createdAt: '2026-08-27T00:00:01.000Z',
    ...overrides,
  };
}

describe('KM eval/evolution runtime', () => {
  it('keeps automatic eval and evolution disabled by default', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    recordRetrieval(store, { candidateCount: 0 });
    store.close();

    expect(isKmAutoEvalEnabled({} as NodeJS.ProcessEnv)).toBe(false);
    expect(isKmAutoEvolutionEnabled({} as NodeJS.ProcessEnv)).toBe(false);
    await expect(runKmEvalEvolutionOnce({ dataDir: dir, env: {} as NodeJS.ProcessEnv })).resolves.toEqual(expect.objectContaining({
      evalEnabled: false,
      evolutionEnabled: false,
      leaseAcquired: false,
      createdEvalRuns: 0,
      createdProposals: 0,
    }));

    const reopened = await ObservationStore.open(dir);
    expect(reopened.evalEvolutionStatus()).toEqual({ evalRuns: 0, failingEvalRuns: 0, reviewPendingProposals: 0 });
    reopened.close();
  });

  it('evaluates pending retrieval evidence idempotently and recovers under a runtime lease', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    recordRetrieval(store, { candidateCount: 0 });
    recordRetrieval(store, { candidateCount: 0 });
    recordRetrieval(store, { candidateCount: 0 });
    store.close();

    const env = { BOTMUX_KM_AUTO_EVAL_ENABLED: 'true' } as NodeJS.ProcessEnv;
    const leaseStore = await ObservationStore.open(dir);
    expect(leaseStore.acquireRuntimeLease({ leaseName: 'eval-evolution-runtime', holderId: 'other', now: Date.now(), ttlMs: 60_000 }))
      .toBe(true);
    leaseStore.close();
    expect(await runKmEvalEvolutionOnce({ dataDir: dir, env, holderId: 'a', maxTargets: 10 })).toEqual(expect.objectContaining({
      leaseAcquired: false,
      evaluatedTargets: 0,
      createdEvalRuns: 0,
      createdProposals: 0,
    }));
    const releaseStore = await ObservationStore.open(dir);
    releaseStore.releaseRuntimeLease({ leaseName: 'eval-evolution-runtime', holderId: 'other' });
    releaseStore.close();

    expect(await runKmEvalEvolutionOnce({ dataDir: dir, env, holderId: 'a', maxTargets: 10 })).toEqual(expect.objectContaining({
      evalEnabled: true,
      evolutionEnabled: false,
      leaseAcquired: true,
      evaluatedTargets: 3,
      createdEvalRuns: 3,
      skippedExistingEvalRuns: 0,
      createdProposals: 0,
    }));
    expect(await runKmEvalEvolutionOnce({ dataDir: dir, env, holderId: 'a', maxTargets: 10 })).toEqual(expect.objectContaining({
      leaseAcquired: true,
      evaluatedTargets: 0,
      createdEvalRuns: 0,
      skippedExistingEvalRuns: 0,
      createdProposals: 0,
    }));

    const reopened = await ObservationStore.open(dir);
    expect(reopened.evalEvolutionStatus()).toEqual(expect.objectContaining({
      evalRuns: 3,
      failingEvalRuns: 3,
      reviewPendingProposals: 0,
    }));
    reopened.close();
  });

  it('does not create evolution proposals without enough evidence', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    recordRetrieval(store, { candidateCount: 0 });
    recordRetrieval(store, { candidateCount: 0 });
    store.close();

    const result = await runKmEvalEvolutionOnce({
      dataDir: dir,
      env: { BOTMUX_KM_AUTO_EVAL_ENABLED: 'true', BOTMUX_KM_AUTO_EVOLUTION_ENABLED: 'true' } as NodeJS.ProcessEnv,
      minEvidence: 3,
    });
    expect(result).toEqual(expect.objectContaining({
      createdEvalRuns: 2,
      consideredWindows: 0,
      createdProposals: 0,
      insufficientEvidence: true,
    }));

    const reopened = await ObservationStore.open(dir);
    expect(reopened.listEvolution(10)).toEqual([]);
    reopened.close();
  });

  it('creates deterministic eval runs for distillation, retrieval, injection, policy, and workflow artifacts', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    const artifact = artifactEvent();
    store.append(artifact);
    const job = store.createDistillationJob({ sourceEventId: artifact.eventId, profile: defaultShadowProfile('bot') });
    const claim = store.claimDistillationJob({});
    expect(claim?.jobId).toBe(job.jobId);
    store.finishDistillationJob({ jobId: job.jobId, claimToken: claim!.claimToken, outputHash: `sha256:${'c'.repeat(64)}` });
    const runId = store.recordRetrievalAudit({
      botAppId: 'bot',
      sessionId: 'session-retrieval',
      queryHash: `sha256:${'d'.repeat(64)}`,
      mode: 'shadow',
      candidateCount: 1,
      eligibleCount: 1,
      latencyMs: 12,
      warnings: [],
      results: [{ itemId: 'mem-1', itemKind: 'memory', providerIds: ['sqlite'], score: 1, eligible: true }],
    });
    store.recordPromptInjectionSnapshot({
      retrievalRunId: runId,
      botAppId: 'bot',
      mode: 'shadow',
      requestedMode: 'shadow',
      effectiveMode: 'shadow',
      disposition: 'would_inject',
      itemIds: ['mem-1'],
      prompt: 'Prefer Chinese',
    });
    store.recordMemoryPolicyDecision({
      sourceEventId: artifact.eventId,
      policyVersion: 'safe-auto-activation-v1',
      disposition: 'activate',
      reasonCodes: ['explicit_observed_low_risk_preference'],
      evidence: { claimKey: 'response.language', subject: 'u1' },
    });
    store.close();

    const result = await runKmEvalEvolutionOnce({
      dataDir: dir,
      env: { BOTMUX_KM_AUTO_EVAL_ENABLED: 'true' } as NodeJS.ProcessEnv,
      maxTargets: 10,
    });
    expect(result).toEqual(expect.objectContaining({
      evaluatedTargets: 5,
      createdEvalRuns: 5,
      createdProposals: 0,
    }));

    const reopened = await ObservationStore.open(dir);
    const evalRuns = reopened.listEvalRuns(10);
    expect(evalRuns.map(row => row.evaluatorName).sort()).toEqual([
      'km.distillation-quality',
      'km.injection-safety',
      'km.memory-policy-quality',
      'km.retrieval-quality',
      'km.workflow-artifact-quality',
    ]);
    expect(evalRuns.every(row => row.failCount === 0)).toBe(true);
    reopened.close();
  });

  it('creates only review_pending proposals when evidenced thresholds fail and deduplicates reruns', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    recordRetrieval(store, { candidateCount: 0 });
    recordRetrieval(store, { candidateCount: 0 });
    recordRetrieval(store, { candidateCount: 0 });
    store.close();

    const env = {
      BOTMUX_KM_AUTO_EVAL_ENABLED: 'true',
      BOTMUX_KM_AUTO_EVOLUTION_ENABLED: 'true',
    } as NodeJS.ProcessEnv;
    const first = await runKmEvalEvolutionOnce({ dataDir: dir, env, minEvidence: 3, failRatioThreshold: 0.34 });
    expect(first).toEqual(expect.objectContaining({
      createdEvalRuns: 3,
      consideredWindows: 2,
      createdProposals: 1,
      reusedProposals: 0,
      insufficientEvidence: false,
    }));
    const second = await runKmEvalEvolutionOnce({ dataDir: dir, env, minEvidence: 3, failRatioThreshold: 0.34 });
    expect(second).toEqual(expect.objectContaining({
      createdEvalRuns: 0,
      createdProposals: 0,
      reusedProposals: 1,
    }));

    const reopened = await ObservationStore.open(dir);
    const proposals = reopened.listEvolution(10);
    expect(proposals).toHaveLength(1);
    expect(proposals[0]).toEqual(expect.objectContaining({
      state: 'review_pending',
      proposalType: 'dashboard-warning',
      targetRef: 'km-quality:retrieval.candidates.present',
      approvalGrade: 'G2',
      createdBy: 'km-evolution-planner-v1',
    }));
    expect(proposals[0].evidenceRefs).toEqual([expect.objectContaining({
      kind: 'sqlite-row',
      ref: expect.stringContaining('eval_results:retrieval.candidates.present:'),
      failedTargetIds: expect.any(Array),
    })]);
    expect(proposals[0].proposedAction).toEqual(expect.objectContaining({
      automaticExecution: false,
      metricKey: 'retrieval.candidates.present',
    }));
    expect(proposals[0].risk).toEqual(expect.objectContaining({
      mutatesWorkspace: false,
      mutatesRuntimeConfig: false,
      requiresHumanApproval: true,
    }));
    expect(proposals[0].rollback).toEqual(expect.objectContaining({
      noRuntimeMutationToUndo: true,
    }));
    expect(reopened.evalEvolutionStatus()).toEqual(expect.objectContaining({
      evalRuns: 3,
      failingEvalRuns: 3,
      reviewPendingProposals: 1,
    }));
    reopened.close();
  });

  it('fails closed on unsafe memory activation decisions without auto-approving the proposal', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    for (let i = 0; i < 3; i += 1) {
      store.recordMemoryPolicyDecision({
        sourceEventId: `evt-${i}`,
        policyVersion: 'safe-auto-activation-v1',
        disposition: 'activate',
        reasonCodes: ['not_explicit_user_statement'],
        evidence: { claimKey: 'response.language', subject: 'u1', index: i },
      });
    }
    store.close();

    const result = await runKmEvalEvolutionOnce({
      dataDir: dir,
      env: { BOTMUX_KM_AUTO_EVAL_ENABLED: 'true', BOTMUX_KM_AUTO_EVOLUTION_ENABLED: 'true' } as NodeJS.ProcessEnv,
      minEvidence: 3,
      failRatioThreshold: 1,
    });
    expect(result).toEqual(expect.objectContaining({ createdEvalRuns: 3, createdProposals: 1 }));

    const reopened = await ObservationStore.open(dir);
    expect(reopened.listEvolution(10)[0]).toEqual(expect.objectContaining({
      state: 'review_pending',
      proposalType: 'memory-policy',
      targetRef: 'km-quality:memory_policy.activation.fail_closed',
      approvalGrade: 'G2',
    }));
    expect(reopened.listEvolution(10)[0]).not.toHaveProperty('approvedBy');
    reopened.close();
  });
});
