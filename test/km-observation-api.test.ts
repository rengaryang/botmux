import { Readable } from 'node:stream';
import { mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { handleKmObservationApi } from '../src/dashboard/km-observation-api.js';
import type { KnowledgeItem } from '../src/services/km/observation-store.js';
import { ObservationStore } from '../src/services/km/observation-store.js';

function response() {
  const bodies: unknown[] = [];
  const res = {
    writeHead: vi.fn(),
    end: vi.fn(value => bodies.push(JSON.parse(String(value)))),
  } as any;
  return { res, bodies };
}

function knowledge(overrides: Partial<KnowledgeItem> = {}): KnowledgeItem {
  const now = '2026-08-27T00:00:00.000Z';
  return {
    knowledgeId: 'kn-api',
    state: 'approved',
    targetLayer: 'L3',
    category: 'skill',
    title: 'Skill route',
    claimKey: 'skill.route',
    claimText: 'Route explicit export requests through the KM exporter.',
    confidence: 'observed',
    freshness: 'fresh',
    privacyClass: 'internal',
    sourceRefs: [{ kind: 'api', ref: 'evt-1' }],
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

describe('KM observation dashboard API', () => {
  it('does not expose routes while the feature is disabled', async () => {
    const { res, bodies } = response();
    const handled = await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/health'),
      { enabled: false, openStore: vi.fn() },
    );
    expect(handled).toBe(true);
    expect(res.writeHead).toHaveBeenCalledWith(404, expect.anything());
    expect(bodies).toEqual([{ error: 'km_observation_disabled' }]);
  });

  it('activates and rolls back an exact-bot Canary through the governed runtime API', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'botmux-km-canary-api-'));
    try {
      const store = await ObservationStore.open(dir);
      const riskAck = { acknowledged: true, rollback: 'shadow' };
      const now = new Date();
      const { buildKmProductionGatePlan } = await import('../src/services/km/production-gate.js');
      const built = buildKmProductionGatePlan({
        actionKind: 'prompt-canary', actorId: 'operator-1', confirmationToken: 'api-token', ttlSeconds: 3600,
        now: now.toISOString(), riskAck,
        target: { botAppId: 'cli_api', window: { start: new Date(now.getTime() - 1000).toISOString(), end: new Date(now.getTime() + 3600000).toISOString() } },
        scope: { botAppId: 'cli_api', sessionClass: 'wizard' },
      });
      store.createProductionGatePlan(built.plan);
      store.putPipelineProfile({
        schemaVersion: 1, profileId: 'api-canary', revision: 1, botAppId: 'cli_api', sourceProvider: 'observation-source-v1',
        windowProvider: 'bounded-transcript-window-v1', primaryExtractor: 'builtin.rules-v1', shadowExtractors: [],
        knowledgeRouter: 'builtin.layer-router-v1', memoryPolicy: 'safe-auto-activation-v1',
        memoryBackends: { writePolicy: 'single', primary: 'sqlite', mirrors: [] }, injectionMode: 'canary',
        budgets: { sourceBytes: 262144, sourceTokens: 32000, outputClaims: 20, promptTokens: 1800 },
      }, 'shadow');
      store.close();
      const deps = { enabled: true, actorId: 'operator-2', openStore: () => ObservationStore.open(dir) };
      const activate = response();
      await handleKmObservationApi(Object.assign(Readable.from([Buffer.from(JSON.stringify({
        planId: built.plan.planId, approvalGrade: 'G2', confirmationToken: 'api-token', previewHash: built.plan.previewHash, riskAck,
      }))]), { method: 'POST', headers: { 'idempotency-key': 'activate-1' } }) as any, activate.res,
      new URL('http://localhost/api/km/canary-release/activate'), deps);
      expect(activate.bodies[0]).toMatchObject({ runtime: { active: true, botAppId: 'cli_api', reason: 'active' }, restartRequired: false, autoFallback: 'shadow' });
      const rollback = response();
      await handleKmObservationApi(Object.assign(Readable.from([Buffer.from(JSON.stringify({ reason: 'operator' }))]), { method: 'POST', headers: { 'idempotency-key': 'rollback-1' } }) as any,
        rollback.res, new URL(`http://localhost/api/km/canary-release/${built.plan.planId}/rollback`), deps);
      expect(rollback.bodies[0]).toMatchObject({ plan: { state: 'rolled_back' }, effective: false, fallback: 'shadow' });
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it('returns store health and closes the request-scoped store', async () => {
    const close = vi.fn();
    const { res, bodies } = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/health'),
      {
        enabled: true,
        openStore: async () => ({
          schemaVersion: () => 1,
          pragmas: () => ({ journalMode: 'wal', foreignKeys: 1, busyTimeout: 5000 }),
          counts: () => ({ observations: 3, quarantined: 1, knowledge: 2, memory: 4 }),
          list: vi.fn(), get: vi.fn(), close,
        }),
      },
    );
    expect(bodies).toEqual([{
      enabled: true,
      schemaVersion: 1,
      pragmas: { journalMode: 'wal', foreignKeys: 1, busyTimeout: 5000 },
      counts: { observations: 3, quarantined: 1, knowledge: 2, memory: 4 },
      backlog: { queued: 0, retryWait: 0, oldestAgeMs: 0, claimed: 0 },
      evalEvolution: { evalRuns: 0, failingEvalRuns: 0, reviewPendingProposals: 0 },
      capabilities: { requestedModes: ['off', 'shadow'], effectiveModes: ['off', 'shadow'], livePromptInjection: false, realMemoryTransport: false },
    }]);
    expect(close).toHaveBeenCalledOnce();
  });

  it('bounds list pagination and forwards typed filters', async () => {
    const list = vi.fn(() => [{ eventId: 'evt-1' }]);
    const { res } = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/observations?limit=999&before=42&type=turn.completed'),
      {
        enabled: true,
        openStore: async () => ({
          schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(),
          list, get: vi.fn(), close: vi.fn(),
        }),
      },
    );
    expect(list).toHaveBeenCalledWith({ limit: 100, beforeLocalSeq: 42, eventType: 'turn.completed' });
  });

  it('serves Phase 2 knowledge, memory and retrieval reads', async () => {
    const listKnowledge = vi.fn(() => [{ knowledgeId: 'kn-1' }]);
    const listMemory = vi.fn(() => [{ memoryId: 'mem-1' }]);
    const retrieveWithMetrics = vi.fn(() => ({ items: [{ id: 'kn-1', kind: 'knowledge' }], metrics: { directHitCount: 1, normalizedHitCount: 0,
      noHitCount: 2, filteredScopeCount: 3, filteredPrivacyCount: 4, filteredStateCount: 5 } }));
    const deps = {
      enabled: true,
      openStore: async () => ({
        schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
        listKnowledge, listMemory, retrieveWithMetrics,
      }),
    };
    const knowledge = response();
    await handleKmObservationApi({ method: 'GET' } as any, knowledge.res,
      new URL('http://localhost/api/km/knowledge?state=approved&targetLayer=L2'), deps);
    expect(listKnowledge).toHaveBeenCalledWith({ limit: 50, state: 'approved', targetLayer: 'L2' });
    expect(knowledge.bodies).toEqual([{ items: [{ knowledgeId: 'kn-1' }] }]);

    const memory = response();
    await handleKmObservationApi({ method: 'GET' } as any, memory.res,
      new URL('http://localhost/api/km/memory?state=active&scope=user&subject=u1'), deps);
    expect(listMemory).toHaveBeenCalledWith({ limit: 50, state: 'active', scope: 'user', subject: 'u1' });

    const retrieval = response();
    await handleKmObservationApi({ method: 'GET' } as any, retrieval.res,
      new URL('http://localhost/api/km/retrieve?q=failover&scope=user&targetLayer=L3&subject=u1'), deps);
    expect(retrieveWithMetrics).toHaveBeenCalledWith({ text: 'failover', limit: 20, subject: 'u1', scopes: ['user'], targetLayers: ['L3'] });
    expect(retrieval.bodies).toEqual([{ items: [{ id: 'kn-1', kind: 'knowledge' }], metrics: { directHitCount: 1, normalizedHitCount: 0,
      noHitCount: 2, filteredScopeCount: 3, filteredPrivacyCount: 4, filteredStateCount: 5 } }]);
  });

  it('serves guarded memory review state transitions', async () => {
    const transitionMemory = vi.fn(() => ({ memoryId: 'mem-1', state: 'active' }));
    const executeKmMutation = vi.fn((input, operation) => ({ statusCode: input.statusCode, response: operation(), replayed: false }));
    const deps = {
      enabled: true,
      actorId: 'reviewer-1',
      openStore: async () => ({
        schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
        transitionMemory, executeKmMutation,
      }),
    };
    const result = response();
    const req = Object.assign(Readable.from([Buffer.from(JSON.stringify({ toState: 'active', reasonCode: 'review_approved' }))]), {
      method: 'PATCH',
      headers: { 'idempotency-key': 'memory-key-1' },
    });
    await handleKmObservationApi(req as any, result.res, new URL('http://localhost/api/km/memory/mem-1/state'), deps);
    expect(transitionMemory).toHaveBeenCalledWith({
      memoryId: 'mem-1',
      toState: 'active',
      reasonCode: 'review_approved',
      actorId: 'reviewer-1',
    });
    expect(executeKmMutation).toHaveBeenCalledWith(expect.objectContaining({
      action: 'memory.state_changed',
      targetRef: 'mem-1',
    }), expect.any(Function));
    expect(result.bodies).toEqual([{ memoryId: 'mem-1', state: 'active' }]);
  });

  it('serves KM export dry-run, create, review and status without formal destination writes', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'botmux-km-export-api-'));
    const getKnowledge = vi.fn(() => knowledge());
    const executeKmMutation = vi.fn((input, operation) => ({ statusCode: input.statusCode, response: operation(), replayed: false }));
    const deps = {
      enabled: true,
      actorId: 'reviewer-1',
      dataDir,
      openStore: async () => ({
        schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
        getKnowledge, executeKmMutation,
      }),
    };

    const dryRun = response();
    await handleKmObservationApi({ method: 'POST', headers: {} } as any, dryRun.res,
      new URL('http://localhost/api/km/knowledge/kn-api/export-dry-run'), deps);
    expect(dryRun.bodies[0]).toMatchObject({
      knowledgeId: 'kn-api',
      allowed: true,
      destination: { layer: 'L3', root: 'l3-skills', writeMode: 'staging-only' },
      risk: { mutatesFormalDestination: false, stagingOnly: true, automaticExecution: false },
    });

    const create = response();
    const createReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ knowledgeId: 'kn-api' }))]), {
      method: 'POST',
      headers: { 'idempotency-key': 'export-create-1' },
    });
    await handleKmObservationApi(createReq as any, create.res, new URL('http://localhost/api/km/exports'), deps);
    expect(create.res.writeHead).toHaveBeenCalledWith(201, expect.anything());
    const created = create.bodies[0] as any;
    expect(created.state).toBe('review_pending');
    expect(executeKmMutation).toHaveBeenCalledWith(expect.objectContaining({
      action: 'knowledge.export_job_created',
      targetRef: 'kn-api',
    }), expect.any(Function));
    expect(() => statSync(join(dataDir, 'knowledge', created.plan.file.relativePath))).toThrow();

    const review = response();
    const reviewReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ decision: 'approved', reasonCode: 'reviewed' }))]), {
      method: 'POST',
      headers: { 'idempotency-key': 'export-review-1' },
    });
    await handleKmObservationApi(reviewReq as any, review.res, new URL(`http://localhost/api/km/exports/${created.jobId}/review`), deps);
    expect(review.bodies[0]).toMatchObject({ jobId: created.jobId, state: 'staged' });
    const staged = readFileSync(join(dataDir, 'km-export-staging', 'staged', created.plan.file.relativePath), 'utf8');
    expect(staged).toContain('Route explicit export requests through the KM exporter.');
    expect(() => statSync(join(dataDir, 'knowledge', created.plan.file.relativePath))).toThrow();

    const status = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, status.res,
      new URL(`http://localhost/api/km/exports/${created.jobId}`), deps);
    expect(status.bodies[0]).toMatchObject({ jobId: created.jobId, state: 'staged' });
  });

  it('serves KM formal export preview, execute, rollback and status in fixture mode', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'botmux-km-export-api-'));
    const getKnowledge = vi.fn(() => knowledge({ targetLayer: 'L2' }));
    const executeKmMutation = vi.fn((input, operation) => ({ statusCode: input.statusCode, response: operation(), replayed: false }));
    const deps = {
      enabled: true,
      actorId: 'reviewer-1',
      dataDir,
      openStore: async () => ({
        schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
        getKnowledge, executeKmMutation,
      }),
    };
    const create = response();
    await handleKmObservationApi(Object.assign(Readable.from([Buffer.from(JSON.stringify({ knowledgeId: 'kn-api' }))]), {
      method: 'POST',
      headers: { 'idempotency-key': 'formal-create-1' },
    }) as any, create.res, new URL('http://localhost/api/km/exports'), deps);
    const created = create.bodies[0] as any;
    const review = response();
    await handleKmObservationApi(Object.assign(Readable.from([Buffer.from(JSON.stringify({ decision: 'approved', reasonCode: 'reviewed' }))]), {
      method: 'POST',
      headers: { 'idempotency-key': 'formal-review-1' },
    }) as any, review.res, new URL(`http://localhost/api/km/exports/${created.jobId}/review`), deps);

    const preview = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, preview.res,
      new URL(`http://localhost/api/km/exports/${created.jobId}/preview`), deps);
    const previewBody = preview.bodies[0] as any;
    expect(previewBody).toMatchObject({
      allowed: true,
      adapter: { kind: 'plain-markdown' },
      risk: { fixtureOnly: true, network: false, gitPush: false },
    });

    const execute = response();
    await handleKmObservationApi(Object.assign(Readable.from([Buffer.from(JSON.stringify({
      confirmationToken: previewBody.confirmationToken,
      approvalGrade: 'G2',
      expectedTargetHash: previewBody.precondition.currentTargetHash,
      destinationVersion: previewBody.precondition.destinationVersion,
    }))]), {
      method: 'POST',
      headers: { 'idempotency-key': 'formal-execute-1' },
    }) as any, execute.res, new URL(`http://localhost/api/km/exports/${created.jobId}/execute`), deps);
    expect(execute.bodies[0]).toMatchObject({ jobId: created.jobId, state: 'applied' });
    const target = previewBody.destination.absolutePath;
    expect(readFileSync(target, 'utf8')).toContain('Route explicit export requests through the KM exporter.');

    const rollbackPreview = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, rollbackPreview.res,
      new URL(`http://localhost/api/km/exports/${created.jobId}/preview`), deps);
    const rollbackPreviewBody = rollbackPreview.bodies[0] as any;
    const rollback = response();
    await handleKmObservationApi(Object.assign(Readable.from([Buffer.from(JSON.stringify({
      confirmationToken: rollbackPreviewBody.confirmationToken,
      approvalGrade: 'G2',
      expectedTargetHash: (execute.bodies[0] as any).execution.afterHash,
      destinationVersion: rollbackPreviewBody.precondition.destinationVersion,
    }))]), {
      method: 'POST',
      headers: { 'idempotency-key': 'formal-rollback-1' },
    }) as any, rollback.res, new URL(`http://localhost/api/km/exports/${created.jobId}/rollback`), deps);
    expect(rollback.bodies[0]).toMatchObject({ jobId: created.jobId, state: 'rolled_back' });
    expect(() => statSync(target)).toThrow();
  });

  it('serves KM production gate plan, approval, inert intent, audit and kill switch APIs without side effects', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'botmux-km-production-gate-api-'));
    const deps = {
      enabled: true,
      actorId: 'operator-1',
      openStore: async () => ObservationStore.open(dataDir),
    };
    const target = { botAppId: 'cli_a', window: { start: '2026-08-28T00:00:00.000Z', end: '2026-08-28T01:00:00.000Z' } };
    const scope = { botAppId: 'cli_a', sessionClass: 'manual-canary' };
    const riskAck = { acknowledged: true, ticket: 'KM-GATE-API' };

    const createdRes = response();
    const createReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({
      actionKind: 'prompt-canary',
      target,
      scope,
      riskAck,
      ttlSeconds: 900,
      confirmationToken: 'api-token',
    }))]), { method: 'POST', headers: { 'idempotency-key': 'pg-create-api' } });
    await handleKmObservationApi(createReq as any, createdRes.res, new URL('http://localhost/api/km/production-gates'), deps);
    expect(createdRes.res.writeHead).toHaveBeenCalledWith(201, expect.anything());
    const created = createdRes.bodies[0] as any;
    expect(created).toEqual(expect.objectContaining({ effective: false, sideEffectsExecuted: false }));
    expect(created.plan).toEqual(expect.objectContaining({
      actionKind: 'prompt-canary',
      state: 'ready',
      requiredApprovalGrade: 'G2',
      target,
      scope,
    }));
    expect(created.handoff).toEqual(expect.objectContaining({ effective: false, sideEffectsExecuted: false }));

    const approvedRes = response();
    const approveReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({
      approvalGrade: 'G2',
      confirmationToken: 'api-token',
      previewHash: created.plan.previewHash,
      riskAck,
    }))]), { method: 'POST', headers: { 'idempotency-key': 'pg-approve-api' } });
    await handleKmObservationApi(approveReq as any, approvedRes.res,
      new URL(`http://localhost/api/km/production-gates/${created.plan.planId}/approve`), deps);
    expect(approvedRes.bodies[0]).toEqual(expect.objectContaining({
      effective: false,
      sideEffectsExecuted: false,
      plan: expect.objectContaining({ state: 'approved' }),
    }));

    const intentRes = response();
    const intentReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({
      confirmationToken: 'api-token',
      previewHash: created.plan.previewHash,
    }))]), { method: 'POST', headers: { 'idempotency-key': 'pg-intent-api' } });
    await handleKmObservationApi(intentReq as any, intentRes.res,
      new URL(`http://localhost/api/km/production-gates/${created.plan.planId}/intent`), deps);
    expect(intentRes.bodies[0]).toEqual(expect.objectContaining({
      intent: expect.objectContaining({
        effective: false,
        sideEffectsExecuted: false,
        executorAvailable: false,
        safety: expect.objectContaining({ noNetwork: true, noDeletion: true, noProviderCalls: true }),
      }),
      plan: expect.objectContaining({ state: 'executing' }),
    }));

    const auditRes = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, auditRes.res,
      new URL(`http://localhost/api/km/production-gates/${created.plan.planId}/audit`), deps);
    expect((auditRes.bodies[0] as any).items.map((item: any) => item.action)).toEqual(['plan.created', 'approved', 'intent.created']);

    const killRes = response();
    const killReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ enabled: true, reason: 'freeze' }))]), {
      method: 'PUT',
      headers: { 'idempotency-key': 'pg-kill-api' },
    });
    await handleKmObservationApi(killReq as any, killRes.res, new URL('http://localhost/api/km/production-gates/kill-switch'), deps);
    expect(killRes.bodies[0]).toEqual(expect.objectContaining({
      killSwitch: expect.objectContaining({ enabled: true, reason: 'freeze' }),
      mutatesExistingRuntimeGates: false,
    }));
  });

  it('serves exact-bot canary closeout JSON and markdown without creating production intents', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'botmux-km-canary-closeout-api-'));
    const deps = {
      enabled: true,
      actorId: 'operator-1',
      openStore: async () => ObservationStore.open(dataDir),
    };
    const seed = await ObservationStore.open(dataDir);
    const golden = seed.upsertGoldenCase({
      title: 'Chinese reply preference',
      queryRedacted: '以后请用中文回复',
      expectedClaims: [{ claimKey: 'response.language', claimTextHash: `sha256:${'a'.repeat(64)}` }],
      sourceRefs: [{ kind: 'reviewed-distillation-example', ref: 'case-1' }],
      provenance: {
        explicitlyReviewed: true,
        redactionStatus: 'redacted',
        rulesClaims: [{ claimKey: 'response.language', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] }],
        piClaims: [{ claimKey: 'response.language', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] }],
        cost: { piInvoked: false, externalCalls: 0 },
      },
      actorId: 'reviewer-1',
    }).item;
    seed.recordShadowComparison({
      caseId: golden.caseId,
      revision: golden.revision,
      rulesClaims: [{ claimKey: 'response.language', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] }],
      piClaims: [{ claimKey: 'response.language', evidenceRefs: [{ kind: 'golden-case', ref: 'case-1' }] }],
      cost: { piInvoked: false, externalCalls: 0 },
    });
    seed.shadowReadinessReport({ thresholds: { minReviewedCases: 1, minComparisons: 1 } });
    seed.close();

    const json = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, json.res,
      new URL('http://localhost/api/km/canary-closeout?now=2026-08-28T00:00:00.000Z'), deps);
    expect(json.bodies[0]).toEqual(expect.objectContaining({
      botAppId: 'cli_aacca607f9ccdcf8',
      safety: expect.objectContaining({ previewOnly: true, noLiveInjectionActivatedByReport: true }),
      productionGate: expect.objectContaining({
        exactBotOnly: true,
        validActionScopedApprovalPresent: false,
        previewHandoff: expect.objectContaining({ effective: false, sideEffectsExecuted: false }),
      }),
    }));
    const after = await ObservationStore.open(dataDir);
    expect(after.listProductionGatePlans({ limit: 10, actionKind: 'prompt-canary' })).toEqual([]);
    after.close();

    const markdownChunks: string[] = [];
    const mdRes = {
      writeHead: vi.fn(),
      end: vi.fn(value => markdownChunks.push(String(value))),
    } as any;
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, mdRes,
      new URL('http://localhost/api/km/canary-closeout?format=markdown&now=2026-08-28T00:00:00.000Z'), deps);
    expect(mdRes.writeHead).toHaveBeenCalledWith(200, { 'Content-Type': 'text/markdown; charset=utf-8' });
    expect(markdownChunks[0]).toContain('KM Canary Closeout Report');
  });

  it('serves trace/evolution reads and enforces approval grade through the store', async () => {
    const listTrace = vi.fn(() => [{ edgeId: 'edge-1' }]);
    const listEvolution = vi.fn(() => [{ proposalId: 'evo-1' }]);
    const decideProposal = vi.fn(() => ({ approvalId: 'approval-1', state: 'approved' }));
    const listEvalRuns = vi.fn(() => [{ evalRunId: 'eval-1' }]);
    const listSyncStatus = vi.fn(() => [{ sinkId: 'mock', enabled: false }]);
    const listKmProviders = vi.fn(() => [{
      providerId: 'builtin.rules-v1',
      kind: 'extractor',
      version: '1',
      status: 'validated',
      descriptor: { capabilities: ['explicit-user-preferences'], execution: 'in-process' },
    }]);
    const executeKmMutation = vi.fn((input, operation) => ({ statusCode: input.statusCode, response: operation(), replayed: false }));
    const deps = {
      enabled: true,
      actorId: 'reviewer-1',
      openStore: async () => ({
        schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
        listTrace, listEvolution, decideProposal, listEvalRuns, listSyncStatus, listKmProviders, executeKmMutation,
      }),
    };
    const trace = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, trace.res,
      new URL('http://localhost/api/km/trace?type=turn&id=turn-1&limit=999'), deps);
    expect(listTrace).toHaveBeenCalledWith({ type: 'turn', id: 'turn-1', limit: 500 });
    expect(trace.bodies).toEqual([{ items: [{ edgeId: 'edge-1' }] }]);

    const sync = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, sync.res,
      new URL('http://localhost/api/km/sync/sinks'), deps);
    expect(sync.bodies).toEqual([{ items: [{ sinkId: 'mock', enabled: false }] }]);

    const providers = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, providers.res,
      new URL('http://localhost/api/km/providers'), deps);
    expect(providers.bodies).toEqual([{ items: [{
      providerId: 'builtin.rules-v1',
      kind: 'extractor',
      version: '1',
      status: 'validated',
      descriptor: { capabilities: ['explicit-user-preferences'], execution: 'in-process' },
    }] }]);

    const evals = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, evals.res,
      new URL('http://localhost/api/km/eval/runs'), deps);
    expect(evals.bodies).toEqual([{ items: [{ evalRunId: 'eval-1' }] }]);

    const proposals = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, proposals.res,
      new URL('http://localhost/api/km/evolution/proposals'), deps);
    expect(proposals.bodies).toEqual([{ items: [{ proposalId: 'evo-1' }] }]);

    const decision = response();
    const req = Object.assign(Readable.from([Buffer.from(JSON.stringify({ decision: 'approved', grade: 'G2', scope: { target: 'skill' }, riskAck: {} }))]), {
      method: 'POST', headers: { 'idempotency-key': 'evo-key-1' },
    });
    await handleKmObservationApi(req as any, decision.res,
      new URL('http://localhost/api/km/evolution/proposals/evo-1/decision'), deps);
    expect(decideProposal).toHaveBeenCalledWith({ proposalId: 'evo-1', decision: 'approved', actorId: 'reviewer-1', grade: 'G2', scope: { target: 'skill' }, riskAck: {} });
  });

  it('serves guarded pipeline profile and provider configuration mutations', async () => {
    const listPipelineProfiles = vi.fn(() => [{ state: 'draft' }]);
    const putPipelineProfile = vi.fn(() => `sha256:${'a'.repeat(64)}`);
    const setPipelineProfileState = vi.fn(() => ({ state: 'shadow' }));
    const listMemoryProviderConfigs = vi.fn(() => [{ providerId: 'mem0', credentialRef: 'env:***' }]);
    const putMemoryProviderConfig = vi.fn(() => `sha256:${'b'.repeat(64)}`);
    const executeKmMutation = vi.fn((input, operation) => ({ statusCode: input.statusCode, response: operation(), replayed: false }));
    const deps = { enabled: true, actorId: 'reviewer', openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
      listPipelineProfiles, putPipelineProfile, setPipelineProfileState, listMemoryProviderConfigs, putMemoryProviderConfig, executeKmMutation }) };
    const list = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, list.res, new URL('http://localhost/api/km/profiles?botAppId=bot-1'), deps);
    expect(listPipelineProfiles).toHaveBeenCalledWith('bot-1');

    const configReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ providerId: 'mem0', endpoint: 'https://memory.example.test',
      credentialRef: 'env:MEM0_KEY', enabled: true, realTransportEnabled: false, timeoutMs: 5000 }))]),
      { method: 'PUT', headers: { 'idempotency-key': 'key-1' } });
    const config = response();
    await handleKmObservationApi(configReq as any, config.res, new URL('http://localhost/api/km/provider-configs'), deps);
    expect(putMemoryProviderConfig).toHaveBeenCalledWith(expect.objectContaining({ providerId: 'mem0', realTransportEnabled: false }));

    const stateReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ state: 'shadow' }))]),
      { method: 'PATCH', headers: { 'idempotency-key': 'key-2' } });
    const state = response();
    await handleKmObservationApi(stateReq as any, state.res, new URL('http://localhost/api/km/profiles/default/1/state'), deps);
    expect(setPipelineProfileState).toHaveBeenCalledWith({ profileId: 'default', revision: 1, state: 'shadow' });
    expect(executeKmMutation).toHaveBeenCalledTimes(2);
  });

  it('serves golden cases, local shadow comparisons, review labels, and readiness behind mutation guard', async () => {
    const upsertGoldenCase = vi.fn(() => ({ item: { caseId: 'gold-1', revision: 1, contentHash: `sha256:${'a'.repeat(64)}` }, created: true }));
    const listGoldenCases = vi.fn(() => [{ caseId: 'gold-1', revision: 1, state: 'reviewed' }]);
    const retireGoldenCase = vi.fn(() => ({ caseId: 'gold-1', revision: 1, state: 'retired', contentHash: `sha256:${'b'.repeat(64)}` }));
    const recordShadowComparison = vi.fn(() => ({ item: { comparisonId: 'cmp-1', metrics: { claimOverlap: 1 } }, created: true }));
    const listShadowComparisons = vi.fn(() => [{ comparisonId: 'cmp-1' }]);
    const addShadowReviewLabel = vi.fn(() => ({ labelId: 'label-1', created: true }));
    const listShadowReviewLabels = vi.fn(() => [{ labelId: 'label-1' }]);
    const shadowReadinessReport = vi.fn(() => ({ ready: false, windowHash: `sha256:${'c'.repeat(64)}`, reasonCodes: ['insufficient_shadow_comparisons'] }));
    const shadowReadinessReportLatest = vi.fn(() => ({ ready: false, reasonCodes: ['no_readiness_report'] }));
    const executeKmMutation = vi.fn((input, operation) => ({ statusCode: input.statusCode, response: operation(), replayed: false }));
    const deps = { enabled: true, actorId: 'reviewer', openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(),
      list: vi.fn(), get: vi.fn(), close: vi.fn(), upsertGoldenCase, listGoldenCases, retireGoldenCase, recordShadowComparison,
      listShadowComparisons, addShadowReviewLabel, listShadowReviewLabels, shadowReadinessReport, shadowReadinessReportLatest, executeKmMutation }) };

    const createGoldenReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({
      title: 'case', queryRedacted: 'redacted', expectedClaims: [{ claimKey: 'k', claimTextHash: `sha256:${'d'.repeat(64)}` }],
      sourceRefs: [{ kind: 'example', ref: 'e1' }], provenance: { explicitlyReviewed: true, redactionStatus: 'redacted' },
    }))]), { method: 'POST', headers: { 'idempotency-key': 'gold-create' } });
    const createGolden = response();
    await handleKmObservationApi(createGoldenReq as any, createGolden.res, new URL('http://localhost/api/km/golden-cases'), deps);
    expect(upsertGoldenCase).toHaveBeenCalledWith(expect.objectContaining({ actorId: 'reviewer', queryRedacted: 'redacted' }));
    expect(createGolden.res.writeHead).toHaveBeenCalledWith(201, expect.anything());

    const listGolden = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, listGolden.res, new URL('http://localhost/api/km/golden-cases?state=reviewed'), deps);
    expect(listGoldenCases).toHaveBeenCalledWith({ limit: 50, state: 'reviewed' });

    const retireReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ revision: 1, reasonCode: 'obsolete' }))]),
      { method: 'POST', headers: { 'idempotency-key': 'gold-retire' } });
    const retire = response();
    await handleKmObservationApi(retireReq as any, retire.res, new URL('http://localhost/api/km/golden-cases/gold-1/retire'), deps);
    expect(retireGoldenCase).toHaveBeenCalledWith({ caseId: 'gold-1', revision: 1, actorId: 'reviewer', reasonCode: 'obsolete' });

    const comparisonReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ caseId: 'gold-1', rulesClaims: [{ claimKey: 'k' }], piClaims: [] }))]),
      { method: 'POST', headers: { 'idempotency-key': 'cmp-create' } });
    const comparison = response();
    await handleKmObservationApi(comparisonReq as any, comparison.res, new URL('http://localhost/api/km/shadow-comparisons'), deps);
    expect(recordShadowComparison).toHaveBeenCalledWith(expect.objectContaining({ caseId: 'gold-1', rulesClaims: [{ claimKey: 'k' }], piClaims: [] }));

    const labels = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, labels.res, new URL('http://localhost/api/km/shadow-labels?limit=999'), deps);
    expect(listShadowReviewLabels).toHaveBeenCalledWith(100);

    const labelReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ claimKey: 'k', extractor: 'pi', label: 'false_positive', reasonCode: 'bad' }))]),
      { method: 'POST', headers: { 'idempotency-key': 'label-create' } });
    const label = response();
    await handleKmObservationApi(labelReq as any, label.res, new URL('http://localhost/api/km/shadow-comparisons/cmp-1/labels'), deps);
    expect(addShadowReviewLabel).toHaveBeenCalledWith({ comparisonId: 'cmp-1', claimKey: 'k', extractor: 'pi', label: 'false_positive', actorId: 'reviewer', reasonCode: 'bad' });

    const readinessReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ thresholds: { minComparisons: 1 } }))]),
      { method: 'POST', headers: { 'idempotency-key': 'ready-create' } });
    const readiness = response();
    await handleKmObservationApi(readinessReq as any, readiness.res, new URL('http://localhost/api/km/shadow-readiness'), deps);
    expect(shadowReadinessReport).toHaveBeenCalledWith({ thresholds: { minComparisons: 1 } });

    const comparisonList = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, comparisonList.res, new URL('http://localhost/api/km/shadow-comparisons?caseId=gold-1'), deps);
    expect(listShadowComparisons).toHaveBeenCalledWith({ limit: 50, caseId: 'gold-1' });
    const readinessLatest = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, readinessLatest.res, new URL('http://localhost/api/km/shadow-readiness'), deps);
    expect(shadowReadinessReportLatest).toHaveBeenCalledOnce();
    expect(executeKmMutation).toHaveBeenCalledTimes(5);
  });

  it('serves backend runtime status and backend outbox/migration reads', async () => {
    const listMemoryBackendOutbox = vi.fn(() => [{ outboxId: 'mout-1', status: 'pending' }]);
    const listMemoryBackendMigrations = vi.fn(() => [{ migrationId: 'mmig-1', state: 'draft' }]);
    const backendRuntimeStatus = vi.fn(async () => ({
      enabled: false,
      leaseName: 'memory-backend-outbox',
      outbox: { total: 1, pending: 1, inflight: 0, failed: 0, delivered: 0, quarantined: 0, oldestPendingAgeMs: 10 },
      providers: [{ providerId: 'mem0', endpoint: 'mock://mem0', enabled: true, status: 'ready' }],
    }));
    const deps = { enabled: true, backendRuntimeStatus, openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(),
      list: vi.fn(), get: vi.fn(), close: vi.fn(), listMemoryBackendOutbox, listMemoryBackendMigrations }) };

    const runtime = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, runtime.res, new URL('http://localhost/api/km/backend-runtime'), deps);
    expect(backendRuntimeStatus).toHaveBeenCalledOnce();
    expect(runtime.bodies[0]).toEqual(expect.objectContaining({ leaseName: 'memory-backend-outbox' }));

    const outbox = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, outbox.res, new URL('http://localhost/api/km/backend-outbox?limit=999'), deps);
    expect(listMemoryBackendOutbox).toHaveBeenCalledWith(100);

    const migrations = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, migrations.res, new URL('http://localhost/api/km/backend-migrations?limit=999'), deps);
    expect(listMemoryBackendMigrations).toHaveBeenCalledWith(100);
  });

  it('serves authenticated central sink status, config and drill APIs with idempotency', async () => {
    const configureSyncSink = vi.fn(input => ({ sinkId: input.sinkId, endpointRef: input.endpointRef, enabled: input.enabled, pending: 0, inflight: 0, failed: 0, delivered: 0, quarantined: 0, lastLocalSeq: 0, status: 'idle' }));
    const listSyncOutbox = vi.fn(() => [{ outboxId: 'out-1', sinkId: 'central', eventId: 'evt-1', status: 'pending' }]);
    const centralSinkRuntimeStatus = vi.fn(async () => ({
      enabled: false,
      leaseName: 'km-central-sink',
      protocol: { envelopeVersion: 1, signing: 'hmac-sha256-over-canonical-batch', credentialMode: 'reference-only', realTransportEnabled: false, networkLibrariesAllowed: false },
      defaults: { batchLimit: 25, leaseMs: 60_000, timeoutMs: 5_000, maxAttempts: 5 },
      sinks: [],
      rollback: { automaticRemoteRollback: false, localDisableOnly: true },
    }));
    const centralSinkDrill = vi.fn(async () => ({ drill: 'status', ok: true }));
    const seen = new Map<string, any>();
    const executeKmMutation = vi.fn((input, operation) => ({ statusCode: input.statusCode, response: operation(), replayed: false }));
    const getKmMutationReplay = vi.fn((input) => seen.get(input.idempotencyKey) ?? null);
    const recordKmMutation = vi.fn((input) => {
      const value = { statusCode: input.statusCode, response: input.response, replayed: false };
      seen.set(input.idempotencyKey, { statusCode: input.statusCode, response: input.response, replayed: true });
      return value;
    });
    const deps = { enabled: true, actorId: 'reviewer', centralSinkRuntimeStatus, centralSinkDrill, openStore: async () => ({
      schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
      configureSyncSink, listSyncOutbox, executeKmMutation, getKmMutationReplay, recordKmMutation,
    }) };

    const status = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, status.res, new URL('http://localhost/api/km/central-sink/status'), deps);
    expect(centralSinkRuntimeStatus).toHaveBeenCalledOnce();
    expect(status.bodies[0]).toEqual(expect.objectContaining({ enabled: false, leaseName: 'km-central-sink' }));

    const putReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({
      sinkId: 'central',
      endpointRef: 'mock://central',
      enabled: true,
      credentialRef: 'env:BOTMUX_KM_CENTRAL_SINK_SECRET',
      batchLimit: 10,
      timeoutMs: 500,
      maxAttempts: 2,
      payloadMaxBytes: 4096,
    }))]), { method: 'PUT', headers: { 'idempotency-key': 'sink-put-1' } });
    const put = response();
    await handleKmObservationApi(putReq as any, put.res, new URL('http://localhost/api/km/central-sink/sinks'), deps);
    expect(configureSyncSink).toHaveBeenCalledWith(expect.objectContaining({
      sinkId: 'central',
      endpointRef: 'mock://central',
      enabled: true,
      credentialRef: 'env:BOTMUX_KM_CENTRAL_SINK_SECRET',
      batchLimit: 10,
      timeoutMs: 500,
      maxAttempts: 2,
      payloadMaxBytes: 4096,
    }));
    expect(executeKmMutation).toHaveBeenCalledWith(expect.objectContaining({ action: 'central_sink.configured', targetRef: 'central' }), expect.any(Function));

    const outbox = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, outbox.res, new URL('http://localhost/api/km/sync/outbox?sinkId=central&limit=999'), deps);
    expect(listSyncOutbox).toHaveBeenCalledWith({ sinkId: 'central', limit: 100 });

    const drillReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ sinkId: 'central', drill: 'status' }))]), { method: 'POST', headers: { 'idempotency-key': 'drill-1' } });
    const drill = response();
    await handleKmObservationApi(drillReq as any, drill.res, new URL('http://localhost/api/km/central-sink/drills'), deps);
    expect(centralSinkDrill).toHaveBeenCalledOnce();
    expect(recordKmMutation).toHaveBeenCalledWith(expect.objectContaining({ action: 'central_sink.drill.status', targetRef: 'central' }));
    expect(drill.bodies[0]).toMatchObject({ accepted: true, drill: 'status', realTransportEnabled: false, result: { ok: true } });

    const replayReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ sinkId: 'central', drill: 'status' }))]), { method: 'POST', headers: { 'idempotency-key': 'drill-1' } });
    const replay = response();
    await handleKmObservationApi(replayReq as any, replay.res, new URL('http://localhost/api/km/central-sink/drills'), deps);
    expect(centralSinkDrill).toHaveBeenCalledOnce();
    expect(replay.bodies[0]).toEqual(drill.bodies[0]);
  });

  it('serves read-only retention status and bounded report history', async () => {
    const kmRetentionStatus = vi.fn(() => ({
      enabled: false,
      leaseName: 'km-retention-shadow',
      latestPlan: {
        policyVersion: 'km-retention-tiered-v1',
        generatedAt: '2026-08-27T00:00:00.000Z',
        dryRunOnly: true,
        destructiveActionsAvailable: false,
        domains: [{ domain: 'retrieval', table: 'retrieval_runs', tier: 'warm', retentionDays: 90, cutoff: '2026-05-29T00:00:00.000Z',
          totalCount: 1, eligibleCount: 0, protectedCount: 1, oldestRecordAgeDays: 1, oldestEligibleAgeDays: 0, protectedReasonCounts: {}, eligibleSamples: [] }],
        db: { dbBytes: 1, walBytes: 2, totalBytes: 3 },
        operational: { backlog: {}, quarantine: {}, retry: {}, providerQuality: {}, retrievalQuality: {} },
        slo: [{ key: 'km.db.total_bytes', state: 'ok', value: 3, warnAt: 1, criticalAt: 2, unit: 'bytes' }],
        planHash: `sha256:${'a'.repeat(64)}`,
      },
      reports: [],
      trend: [],
    }));
    const listKmRetentionReports = vi.fn(() => [{ reportId: 'kmret-1', totalEligible: 0 }]);
    const retentionRuntimeStatus = vi.fn(async () => ({ ...kmRetentionStatus(), enabled: true }));
    const deps = { enabled: true, retentionRuntimeStatus, openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(),
      list: vi.fn(), get: vi.fn(), close: vi.fn(), kmRetentionStatus, listKmRetentionReports }) };

    const status = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, status.res, new URL('http://localhost/api/km/retention'), deps);
    expect(retentionRuntimeStatus).toHaveBeenCalledOnce();
    expect(status.bodies[0]).toEqual(expect.objectContaining({
      enabled: true,
      latestPlan: expect.objectContaining({ dryRunOnly: true, destructiveActionsAvailable: false }),
    }));

    const reports = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, reports.res, new URL('http://localhost/api/km/retention/reports?limit=999'), deps);
    expect(listKmRetentionReports).toHaveBeenCalledWith(100);
    expect(reports.bodies).toEqual([{ items: [{ reportId: 'kmret-1', totalEligible: 0 }] }]);

    const rejected = response();
    await handleKmObservationApi({ method: 'POST', headers: {} } as any, rejected.res, new URL('http://localhost/api/km/retention'), deps);
    expect(rejected.bodies).toEqual([{ error: 'method_not_allowed' }]);
  });

  it('serves guarded backend migration create/backfill/compare without exposing cutover', async () => {
    const createMemoryBackendMigration = vi.fn(() => 'mmig-1');
    let migrationState = 'draft';
    const getMemoryBackendMigration = vi.fn(() => ({ migrationId: 'mmig-1', botAppId: 'bot-1', state: migrationState, stats: {}, createdAt: 'now', updatedAt: 'now' }));
    const transitionMemoryBackendMigration = vi.fn((input) => { migrationState = input.toState; });
    const listMemoryForBackendMigration = vi.fn(() => []);
    const enqueueMemoryBackendOperation = vi.fn();
    const compareMemoryBackendBindings = vi.fn(() => ({ fromProviderId: 'sqlite', toProviderId: 'mem0', compared: 0, matched: 0, missing: 0, mismatched: 0, samples: [] }));
    const executeKmMutation = vi.fn((input, operation) => ({ statusCode: input.statusCode, response: operation(), replayed: false }));
    const deps = { enabled: true, actorId: 'reviewer', openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(),
      list: vi.fn(), get: vi.fn(), close: vi.fn(), createMemoryBackendMigration, getMemoryBackendMigration, transitionMemoryBackendMigration,
      listMemoryForBackendMigration, enqueueMemoryBackendOperation, compareMemoryBackendBindings, executeKmMutation }) };

    const createReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ botAppId: 'bot-1', fromProfile: { primary: 'sqlite' }, toProfile: { primary: 'mem0' } }))]),
      { method: 'POST', headers: { 'idempotency-key': 'mig-create' } });
    const created = response();
    await handleKmObservationApi(createReq as any, created.res, new URL('http://localhost/api/km/backend-migrations'), deps);
    expect(createMemoryBackendMigration).toHaveBeenCalledWith({ botAppId: 'bot-1', fromProfile: { primary: 'sqlite' }, toProfile: { primary: 'mem0' } });
    expect(created.bodies).toEqual([{ migrationId: 'mmig-1', state: 'draft', automaticCutover: false }]);

    const backfillReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ toProviderId: 'mem0', limit: 10 }))]),
      { method: 'POST', headers: { 'idempotency-key': 'mig-backfill' } });
    const backfill = response();
    await handleKmObservationApi(backfillReq as any, backfill.res, new URL('http://localhost/api/km/backend-migrations/mmig-1/backfill'), deps);
    expect(transitionMemoryBackendMigration).toHaveBeenCalledWith({ migrationId: 'mmig-1', toState: 'backfilling', checkpoint: undefined, stats: {} });
    expect(listMemoryForBackendMigration).toHaveBeenCalledWith({ afterMemoryId: undefined, limit: 10 });

    const compareReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ fromProviderId: 'sqlite', toProviderId: 'mem0' }))]),
      { method: 'POST', headers: { 'idempotency-key': 'mig-compare' } });
    const compared = response();
    await handleKmObservationApi(compareReq as any, compared.res, new URL('http://localhost/api/km/backend-migrations/mmig-1/compare'), deps);
    expect(compareMemoryBackendBindings).toHaveBeenCalledWith({ fromProviderId: 'sqlite', toProviderId: 'mem0', sampleLimit: undefined });
    expect(compared.bodies).toEqual([{ fromProviderId: 'sqlite', toProviderId: 'mem0', compared: 0, matched: 0, missing: 0, mismatched: 0, samples: [] }]);

    const forbidden = response();
    const handled = await handleKmObservationApi({ method: 'POST', headers: { 'idempotency-key': 'mig-cutover' } } as any, forbidden.res,
      new URL('http://localhost/api/km/backend-migrations/mmig-1/cutover'), deps);
    expect(handled).toBe(false);
    expect(forbidden.bodies).toEqual([]);
  });

  it('rejects oversized mutation bodies and unopened modes', async () => {
    const executeKmMutation = vi.fn(); const putPipelineProfile = vi.fn();
    const deps = { enabled: true, actorId: 'reviewer', openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(),
      list: vi.fn(), get: vi.fn(), close: vi.fn(), executeKmMutation, putPipelineProfile }) };
    const oversized = response(); const oversizedReq = Object.assign(Readable.from([Buffer.alloc(129 * 1024, 1)]),
      { method: 'POST', headers: { 'idempotency-key': 'large' } });
    await handleKmObservationApi(oversizedReq as any, oversized.res, new URL('http://localhost/api/km/profiles'), deps);
    expect(oversized.res.writeHead).toHaveBeenCalledWith(413, expect.anything());

    const profile = { schemaVersion: 1, profileId: 'p', revision: 1, botAppId: 'bot', sourceProvider: 'source', windowProvider: 'window',
      primaryExtractor: 'rules', shadowExtractors: [], knowledgeRouter: 'router', memoryPolicy: 'policy',
      memoryBackends: { writePolicy: 'single', primary: 'sqlite', mirrors: [] }, injectionMode: 'active',
      budgets: { sourceBytes: 1024, sourceTokens: 100, outputClaims: 1, promptTokens: 100 } };
    const active = response(); const activeReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ profile, state: 'draft' }))]),
      { method: 'POST', headers: { 'idempotency-key': 'active' } });
    await handleKmObservationApi(activeReq as any, active.res, new URL('http://localhost/api/km/profiles'), deps);
    expect(active.res.writeHead).toHaveBeenCalledWith(422, expect.anything());
    expect(executeKmMutation).not.toHaveBeenCalled();
  });

  it('serves workspace knowledge contract v2 without exposing asset bodies', async () => {
    const snapshot: any = {
      schemaVersion: 2, generatedAt: '2026-09-01T00:00:00.000Z', state: 'complete', hash: 'sha256:test', durationMs: 3,
      roots: [{ workspaceId: 'ws', displayRoot: 'repo', state: 'complete', errors: [] }], errors: [],
      assets: [{ assetId: 'ws:L2:id:l2k-1', workspaceId: 'ws', layer: 'L2', kind: 'l2-entry', title: 'SOP', relativePath: 'l2-knowledge/x.md', lifecycle: 'pending-ingest', freshness: 'fresh', contract: { version: 'v3', valid: true, errors: [], warnings: [] }, retrieval: { recallCount: 0 }, linkage: { relatedCount: 0 } }],
      health: { totalsByLayer: { L0: 1, L1: 0, L2: 1, L3: 0, L4: 0 }, totalAssets: 2, contractValidRate: 100, indexConsistencyRate: 100, retrievableRate: 100, linkageCoverageRate: 0, lifecycle: { 'pending-ingest': 1 }, freshness: { fresh: 1 }, contractErrors: 0, legacyAssets: 0 },
      retrievalQuality: { indexQueries: 0, entryRecallEvents: 0, neverRecalledAssets: 1, markdownReads: 0, zeroReadQueries: null, zeroReadRate: null, effectivenessRate: null, fallbackSuccessRate: null, queryFeedbackRate: null, evidenceState: 'cold_start', evidenceQueries: 0, useLabels: { direct_apply: 0, context_guided: 0, pitfall_avoided: 0, not_used: 0, misleading: 0 }, invalidEvidenceEvents: 0 },
      attention: { contractErrors: [], pendingIngest: [], staleOrPurged: [], neverRecalled: [], orphaned: [] },
    };
    const close = vi.fn(); const result = response();
    await handleKmObservationApi({ method: 'GET' } as any, result.res, new URL('http://localhost/api/km/dashboard-metrics-v2'), {
      enabled: true, workspaceKnowledgeSnapshot: () => snapshot,
      openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close,
        dashboardMetrics: vi.fn(() => ({ schemaVersion: 1, source: 'sqlite', totals: { knowledgeTotal: 1 } })) }),
    });
    expect(result.bodies[0]).toMatchObject({ schemaVersion: 2, assetHealth: { totalAssets: 2 }, retrievalQuality: { effectivenessRate: null }, kmRuntime: { schemaVersion: 1 } });
    expect(JSON.stringify(result.bodies[0])).not.toContain('claimText');
    expect(close).toHaveBeenCalledOnce();
  });

  it('publishes the retrieval evidence privacy contract without raw query or reasoning fields', async () => {
    const result = response(); const close = vi.fn();
    const snapshot: any = { schemaVersion: 2, generatedAt: '', state: 'complete', hash: '', durationMs: 0, roots: [], errors: [], assets: [], health: {}, retrievalQuality: { evidenceState: 'cold_start' }, attention: {} };
    await handleKmObservationApi({ method: 'GET' } as any, result.res, new URL('http://localhost/api/km/retrieval-usage-v2'), {
      enabled: true, workspaceKnowledgeSnapshot: () => snapshot,
      openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close }),
    });
    expect(result.bodies[0]).toMatchObject({ privacy: { queryTextStored: false, reasoningStored: false, acceptedQueryIdentifier: 'sha256' } });
    expect(result.bodies[0]).not.toHaveProperty('queryText');
    expect(result.bodies[0]).not.toHaveProperty('reasoningText');
  });

  it('redacts source metadata from the workspace asset list API', async () => {
    const result = response(); const close = vi.fn();
    const asset: any = { assetId: 'ws:L2:id:x', workspaceId: 'ws', layer: 'L2', kind: 'l2-entry', title: 'x', relativePath: 'l2/x.md', lifecycle: 'pending-ingest', freshness: 'fresh', contract: { version: 'v3', valid: true, errors: [], warnings: [] }, retrieval: { recallCount: 0 }, linkage: { relatedCount: 0, source: '/secret/source', ingestRunId: 'private-run' } };
    await handleKmObservationApi({ method: 'GET' } as any, result.res, new URL('http://localhost/api/km/knowledge-assets-v2'), {
      enabled: true, workspaceKnowledgeSnapshot: () => ({ schemaVersion: 2, generatedAt: '', state: 'complete', hash: '', durationMs: 0, roots: [], errors: [], assets: [asset], health: {} as any, retrievalQuality: {} as any, attention: {} as any }),
      openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close }),
    });
    expect(result.bodies[0]).toMatchObject({ items: [{ linkage: { relatedCount: 0 } }] });
    expect(JSON.stringify(result.bodies[0])).not.toContain('secret/source');
    expect(JSON.stringify(result.bodies[0])).not.toContain('private-run');
  });

  it('returns one event and rejects unsupported methods', async () => {
    const get = vi.fn(() => ({ eventId: 'evt-1', payload: { status: 'completed' } }));
    const first = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      first.res,
      new URL('http://localhost/api/km/observations/evt-1'),
      {
        enabled: true,
        openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get, close: vi.fn() }),
      },
    );
    expect(first.bodies).toEqual([{ eventId: 'evt-1', payload: { status: 'completed' } }]);

    const second = response();
    await handleKmObservationApi(
      { method: 'POST', headers: {} } as any,
      second.res,
      new URL('http://localhost/api/km/observations'),
      { enabled: true, openStore: vi.fn() },
    );
    expect(second.bodies).toEqual([{ error: 'method_not_allowed' }]);
  });
});
