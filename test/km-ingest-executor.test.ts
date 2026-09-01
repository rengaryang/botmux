import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { handleKmObservationApi } from '../src/dashboard/km-observation-api.js';
import {
  approveKmIngestRun,
  executeKmIngestOffline,
  hashKmIngestConfirmationToken,
  planKmIngest,
  registerKmIngestTarget,
  rollbackKmIngest,
} from '../src/services/km/ingest-executor.js';
import { ObservationStore, type KmIngestCandidateInput } from '../src/services/km/observation-store.js';
import { defaultShadowProfile } from '../src/services/km/runtime-orchestrator.js';

const dirs: string[] = [];
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-ingest-'));
  dirs.push(dir);
  return dir;
}
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function candidate(overrides: Partial<KmIngestCandidateInput> = {}): KmIngestCandidateInput {
  return {
    targetLayer: 'L2',
    category: 'runbook',
    title: 'Retry failed importer',
    claimKey: 'ingest.retry',
    claimText: 'Resume a partial ingest run with the same approved plan hash.',
    confidence: 'observed',
    freshness: 'fresh',
    privacyClass: 'internal',
    sourceRefs: [{ kind: 'distillation-job', ref: 'distill-1' }],
    ...overrides,
  };
}

function sha256(value: string): string {
  return `sha256:${createHash('sha256').update(value).digest('hex')}`;
}

function completedExtractorRun(store: ObservationStore): string {
  const job = store.createDistillationJob({ sourceEventId: 'evt-1', profile: defaultShadowProfile('bot-1'), now: Date.now() - 1 });
  const claim = store.claimDistillationJob({})!;
  store.finishDistillationJob({ jobId: job.jobId, claimToken: claim.claimToken, outputHash: sha256('ok') });
  return job.jobId;
}

describe('KM ingest executor', () => {
  it('fails closed when target, credential, or extractor approval is missing', async () => {
    const store = await ObservationStore.open(tempDir());
    const missingTarget = planKmIngest({
      store,
      targetId: 'missing',
      sourceRunId: 'missing-run',
      extractorProviderId: 'builtin.rules-v1',
      candidates: [candidate()],
      actorId: 'operator',
      idempotencyKey: 'missing-target',
      confirmationToken: 'token-1',
      env: {},
    });
    expect(missingTarget.run.state).toBe('blocked');
    expect(missingTarget.run.lastError).toContain('target_missing');
    expect(missingTarget.audit.map(item => item.action)).toEqual(['plan.created', 'plan.blocked']);

    const target = registerKmIngestTarget(store, {
      targetId: 'mock-target',
      endpointRef: 'mock://km-ingest',
      credentialRef: 'env:KM_INGEST_TOKEN',
      enabled: true,
      allowedProviderIds: ['builtin.rules-v1'],
    }, 'operator');
    expect(target.state).toBe('ready');
    const blocked = planKmIngest({
      store,
      targetId: target.targetId,
      sourceRunId: 'missing-run',
      extractorProviderId: 'builtin.rules-v1',
      candidates: [candidate()],
      actorId: 'operator',
      idempotencyKey: 'missing-credential-run',
      confirmationToken: 'token-2',
      env: {},
    });
    expect(blocked.run.state).toBe('blocked');
    expect(blocked.run.lastError).toContain('credential_missing');
    expect(blocked.run.lastError).toContain('extractor_run_missing');
    const queued = store.createDistillationJob({ sourceEventId: 'evt-queued', profile: defaultShadowProfile('bot-1'), now: Date.now() - 1 });
    const notReady = planKmIngest({
      store,
      targetId: target.targetId,
      sourceRunId: queued.jobId,
      extractorProviderId: 'builtin.rules-v1',
      candidates: [candidate({ canonicalKey: 'queued-run' })],
      actorId: 'operator',
      idempotencyKey: 'queued-run',
      confirmationToken: 'token-queued',
      env: { KM_INGEST_TOKEN: 'present' },
    });
    expect(notReady.run.state).toBe('blocked');
    expect(notReady.run.lastError).toContain('extractor_run_not_ready:queued');
    store.close();
  });

  it('requires unique canonical keys before creating a plan', async () => {
    const store = await ObservationStore.open(tempDir());
    const sourceRunId = completedExtractorRun(store);
    registerKmIngestTarget(store, {
      targetId: 'mock-target',
      endpointRef: 'mock://km-ingest',
      credentialRef: 'mock:token',
      enabled: true,
      allowedProviderIds: ['builtin.rules-v1'],
    }, 'operator');
    expect(() => planKmIngest({
      store,
      targetId: 'mock-target',
      sourceRunId,
      extractorProviderId: 'builtin.rules-v1',
      candidates: [candidate({ canonicalKey: 'same' }), candidate({ canonicalKey: 'same', claimKey: 'other' })],
      actorId: 'operator',
      idempotencyKey: 'duplicate',
      confirmationToken: 'token-3',
    })).toThrow('km_ingest_canonical_key_duplicate');
    store.close();
  });

  it('rejects direct store plans whose canonical key set does not match items', async () => {
    const store = await ObservationStore.open(tempDir());
    expect(() => store.createKmIngestRun({
      actorId: 'operator',
      idempotencyKey: 'bad-key-set',
      targetId: 'mock-target',
      confirmationTokenHash: hashKmIngestConfirmationToken('token'),
      plan: {
        schemaVersion: 1,
        targetId: 'mock-target',
        targetHash: sha256('target'),
        sourceRunId: 'distill-1',
        extractorRunState: 'completed',
        extractorProviderId: 'builtin.rules-v1',
        mode: 'offline',
        dryRun: true,
        planCalls: { markIngested: false },
        canonicalKeys: ['different'],
      },
      items: [{
        canonicalKey: 'actual',
        candidate: candidate({ canonicalKey: 'actual' }),
        candidateHash: sha256('candidate'),
      }],
    })).toThrow('km_ingest_canonical_key_set_mismatch');
    store.close();
  });

  it('rejects direct store plans that schedule mark-ingested before external ACK', async () => {
    const store = await ObservationStore.open(tempDir());
    expect(() => store.createKmIngestRun({
      actorId: 'operator',
      idempotencyKey: 'early-mark-ingested',
      targetId: 'mock-target',
      confirmationTokenHash: hashKmIngestConfirmationToken('token'),
      plan: {
        schemaVersion: 1,
        targetId: 'mock-target',
        targetHash: sha256('target'),
        sourceRunId: 'distill-1',
        extractorRunState: 'completed',
        extractorProviderId: 'builtin.rules-v1',
        mode: 'offline',
        dryRun: true,
        planCalls: { markIngested: true },
        canonicalKeys: ['actual'],
      },
      items: [{
        canonicalKey: 'actual',
        candidate: candidate({ canonicalKey: 'actual' }),
        candidateHash: sha256('candidate'),
      }],
    })).toThrow('km_ingest_mark_ingested_requires_external_ack');
    store.close();
  });

  it('requires plan hash, confirmation token, run approval, and external ACK before local execution', async () => {
    const store = await ObservationStore.open(tempDir());
    const sourceRunId = completedExtractorRun(store);
    registerKmIngestTarget(store, {
      targetId: 'mock-target',
      endpointRef: 'mock://km-ingest',
      credentialRef: 'mock:token',
      enabled: true,
      allowedProviderIds: ['builtin.rules-v1'],
      markIngestedCommand: 'mark-ingested',
    }, 'operator');
    const planned = planKmIngest({
      store,
      targetId: 'mock-target',
      sourceRunId,
      extractorProviderId: 'builtin.rules-v1',
      candidates: [candidate({ canonicalKey: 'runbook/ingest-retry' })],
      actorId: 'operator',
      idempotencyKey: 'plan-ok',
      confirmationToken: 'token-4',
    });
    expect(planned.run.state).toBe('planned');
    expect(() => approveKmIngestRun({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      confirmationToken: 'bad-token',
      expectedPlanHash: planned.run.planHash,
      externalAck: { approved: true, approvedBy: 'human', planHash: planned.run.planHash },
    })).toThrow('km_ingest_confirmation_token_invalid');
    expect(() => approveKmIngestRun({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      confirmationToken: 'token-4',
      expectedPlanHash: sha256('stale'),
      externalAck: { approved: true, approvedBy: 'human', planHash: planned.run.planHash },
    })).toThrow('km_ingest_plan_hash_mismatch');
    const missingApproval = executeKmIngestOffline({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      confirmationToken: 'token-4',
      expectedPlanHash: planned.run.planHash,
    });
    expect(missingApproval.run.state).toBe('blocked');
    expect(missingApproval.run.lastError).toBe('run_approval_missing');

    const approved = approveKmIngestRun({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      confirmationToken: 'token-4',
      expectedPlanHash: planned.run.planHash,
      externalAck: { approved: true, approvedBy: 'human', planHash: planned.run.planHash },
    });
    expect(approved.run.state).toBe('approved');
    const executed = executeKmIngestOffline({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      confirmationToken: 'token-4',
      expectedPlanHash: planned.run.planHash,
    });
    expect(executed.run.state).toBe('completed');
    expect(executed.run.ingestedCount).toBe(1);
    expect(executed.run.markIngestedPlannedCount).toBe(1);
    expect(executed.items[0].markIngestedPlan).toEqual(expect.objectContaining({
      command: 'mark-ingested',
      dryRun: true,
      sideEffectsExecuted: false,
      planHash: planned.run.planHash,
    }));
    expect(executed.audit.map(item => item.action)).toEqual(expect.arrayContaining([
      'plan.created',
      'execution.blocked',
      'run.approved',
      'execution.started',
      'execution.finished',
    ]));
    expect(store.listKnowledge({ limit: 10, state: 'candidate' })).toEqual([
      expect.objectContaining({ claimKey: 'ingest.retry' }),
    ]);
    store.close();
  });

  it('supports partial retry and local rollback without calling mark-ingested', async () => {
    const store = await ObservationStore.open(tempDir());
    const sourceRunId = completedExtractorRun(store);
    registerKmIngestTarget(store, {
      targetId: 'mock-target',
      endpointRef: 'mock://km-ingest',
      credentialRef: 'mock:token',
      enabled: true,
    }, 'operator');
    const planned = planKmIngest({
      store,
      targetId: 'mock-target',
      sourceRunId,
      extractorProviderId: 'builtin.rules-v1',
      candidates: [
        candidate({ canonicalKey: 'one', claimKey: 'one', claimText: 'First ingest item.' }),
        candidate({ canonicalKey: 'two', claimKey: 'two', claimText: 'Second ingest item.' }),
      ],
      actorId: 'operator',
      idempotencyKey: 'partial',
      confirmationToken: 'token-5',
    });
    approveKmIngestRun({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      confirmationToken: 'token-5',
      expectedPlanHash: planned.run.planHash,
      externalAck: { approved: true, approvedBy: 'human', planHash: planned.run.planHash },
    });
    const first = executeKmIngestOffline({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      confirmationToken: 'token-5',
      expectedPlanHash: planned.run.planHash,
      maxItems: 1,
    });
    expect(first.run.state).toBe('partial');
    expect(first.run.ingestedCount).toBe(1);
    const second = executeKmIngestOffline({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      confirmationToken: 'token-5',
      expectedPlanHash: planned.run.planHash,
      maxItems: 1,
    });
    expect(second.run.state).toBe('completed');
    expect(second.run.ingestedCount).toBe(2);
    const rolledBack = rollbackKmIngest({
      store,
      runId: planned.run.runId,
      actorId: 'operator',
      expectedPlanHash: planned.run.planHash,
      reasonCode: 'test_rollback',
    });
    expect(rolledBack.run.state).toBe('rolled_back');
    expect(rolledBack.run.rollbackCount).toBe(2);
    expect(store.listKnowledge({ limit: 10, state: 'rejected' })).toHaveLength(2);
    store.close();
  });

  it('exposes KM API read-only ingest status without execution routes', async () => {
    const dataDir = tempDir();
    const store = await ObservationStore.open(dataDir);
    registerKmIngestTarget(store, {
      targetId: 'mock-target',
      endpointRef: 'mock://km-ingest',
      credentialRef: 'mock:token',
      enabled: true,
    }, 'operator');
    const sourceRunId = completedExtractorRun(store);
    const planned = planKmIngest({
      store,
      targetId: 'mock-target',
      sourceRunId,
      extractorProviderId: 'builtin.rules-v1',
      candidates: [candidate({ canonicalKey: 'api' })],
      actorId: 'operator',
      idempotencyKey: 'api',
      confirmationToken: 'token-6',
    });
    store.close();
    const deps = { enabled: true, openStore: async () => ObservationStore.open(dataDir) };
    const makeRes = () => {
      const res: any = { statusCode: 0, headers: {}, body: '', writeHead(status: number, headers: Record<string, string>) { this.statusCode = status; Object.assign(this.headers, headers); }, setHeader(k: string, v: string) { this.headers[k] = v; }, end(v: string) { this.body = v; } };
      return res;
    };
    const targets = makeRes();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, targets, new URL('http://localhost/api/km/ingest/targets'), deps);
    expect(targets.statusCode).toBe(200);
    expect(JSON.parse(targets.body).executor).toEqual({ enabled: false, mode: 'offline', executionApiEnabled: false });
    expect(JSON.parse(targets.body).items[0].targetId).toBe('mock-target');
    const runs = makeRes();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, runs, new URL('http://localhost/api/km/ingest'), deps);
    expect(runs.statusCode).toBe(200);
    expect(JSON.parse(runs.body).items[0].runId).toBe(planned.run.runId);
    const status = makeRes();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, status, new URL(`http://localhost/api/km/ingest/${planned.run.runId}`), deps);
    expect(status.statusCode).toBe(200);
    expect(JSON.parse(status.body).items[0].canonicalKey).toBe('api');
    const executeRoute = makeRes();
    await handleKmObservationApi({ method: 'POST', headers: {}, async *[Symbol.asyncIterator]() { yield Buffer.from('{}'); } } as any,
      executeRoute, new URL(`http://localhost/api/km/ingest/${planned.run.runId}/execute`), deps);
    expect(executeRoute.statusCode).toBe(405);
  });

  it('uses action-scoped confirmation hashes for ingest runs', () => {
    expect(hashKmIngestConfirmationToken('same-token')).toMatch(/^sha256:[a-f0-9]{64}$/);
  });
});
