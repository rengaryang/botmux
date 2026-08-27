import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { boundedEvidenceWindow, composePromptMemoryForTurn, defaultShadowProfile, drainDistillationJobs, enqueueAutomaticDistillation, isKmAutoDistillationEnabled, isKmRetrievalShadowEnabled, resolveBoundedTranscriptWindow, runOneDistillationJob, runRetrievalShadow } from '../src/services/km/runtime-orchestrator.js';
import { observationFromTurnCompletion } from '../src/services/km/observation-producers.js';
import { SkillFeedbackStore } from '../src/services/skill-feedback-store.js';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';
import type { MemoryBackendProvider } from '../src/services/km/memory-backend-spi.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-runtime-')); dirs.push(dir); return dir; }
afterEach(() => { delete process.env.BOTMUX_KM_AUTO_DISTILLATION_ENABLED; delete process.env.BOTMUX_KM_PI_SHADOW_ENABLED; delete process.env.BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED; delete process.env.BOTMUX_KM_LIVE_INJECTION_ENABLED; delete process.env.BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED; delete process.env.BOTMUX_KM_CANARY_BOT_APP_IDS; delete process.env.BOTMUX_KM_FEDERATED_RETRIEVAL_ENABLED; delete process.env.CODEX_HOME; for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
const event: ObservationEvent = {
  schemaVersion: 1, eventId: 'evt-runtime-1', eventType: 'workflow.artifact.produced',
  source: { producer: 'workflow', adapter: 'workflow', resolverStatus: 'resolved', confidence: 'observed' },
  identity: { botAppId: 'bot', sessionId: 's1', workflowId: 'wf', nodeId: 'node' },
  ordering: { sourceKey: 'wf', idempotencyKey: 'wf-1', parentEventIds: [], observedAt: '2026-08-26T00:00:00.000Z' },
  provenance: { evidenceLevel: 'workflow-artifact', parserVersion: 'v1', sourceRefs: [{ kind: 'workflow-artifact', ref: 'wf/node' }], privacyClass: 'internal', redactionStatus: 'not_needed' },
  content: { hash: null, storageMode: 'none' }, payload: { outputKey: 'report' }, createdAt: '2026-08-26T00:00:01.000Z',
};

describe('KM runtime orchestrator', () => {
  it('keeps auto distillation and retrieval off by default', () => {
    expect(isKmAutoDistillationEnabled({})).toBe(false);
    expect(isKmRetrievalShadowEnabled({})).toBe(false);
    expect(defaultShadowProfile('bot').injectionMode).toBe('shadow');
  });

  it('creates one durable job only when enabled and runs rules primary', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir); store.append(event); store.close();
    await enqueueAutomaticDistillation({ dataDir: dir, event });
    expect(await runOneDistillationJob({ dataDir: dir })).toBe('idle');
    process.env.BOTMUX_KM_AUTO_DISTILLATION_ENABLED = 'true';
    await enqueueAutomaticDistillation({ dataDir: dir, event });
    await enqueueAutomaticDistillation({ dataDir: dir, event });
    expect(await runOneDistillationJob({ dataDir: dir })).toBe('completed');
    const reopened = await ObservationStore.open(dir);
    expect(reopened.listKnowledge({ limit: 10 })).toEqual([expect.objectContaining({ category: 'workflow-artifact', state: 'candidate' })]);
    reopened.close();
  });

  it('extracts an explicit observed user preference through the safe policy', async () => {
    const preferenceEvent: ObservationEvent = { ...event, eventId: 'evt-preference', eventType: 'turn.completed',
      ordering: { ...event.ordering, idempotencyKey: 'pref-1' },
      payload: { requesterSubjectId: 'u1', knowledgeCandidate: '<user_message>以后请用中文回复</user_message>' }, content: { hash: null, storageMode: 'none' } };
    const dir = tempDir(); const store = await ObservationStore.open(dir); store.append(preferenceEvent); store.close();
    process.env.BOTMUX_KM_AUTO_DISTILLATION_ENABLED = 'true';
    await enqueueAutomaticDistillation({ dataDir: dir, event: preferenceEvent });
    expect(await runOneDistillationJob({ dataDir: dir })).toBe('completed');
    const reopened = await ObservationStore.open(dir);
    expect(reopened.listMemory({ limit: 10 })).toEqual([expect.objectContaining({ state: 'active', subject: 'u1', claimKey: 'response.language' })]);
    expect(reopened.listMemoryPolicyDecisions(10)).toEqual([expect.objectContaining({ disposition: 'activate', reasonCodes: ['explicit_observed_low_risk_preference'] })]);
    reopened.close();
  });

  it('uses a stored bot profile snapshot when enqueueing', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir); store.append(event);
    const profile = { ...defaultShadowProfile('bot'), profileId: 'configured', revision: 2,
      memoryBackends: { writePolicy: 'single' as const, primary: 'sqlite', mirrors: [] } };
    store.putPipelineProfile(profile, 'shadow'); store.close();
    process.env.BOTMUX_KM_AUTO_DISTILLATION_ENABLED = 'true';
    await enqueueAutomaticDistillation({ dataDir: dir, event });
    const reopened = await ObservationStore.open(dir); const claim = reopened.claimDistillationJob({});
    expect(claim?.profile).toEqual(profile); reopened.close();
  });

  it('drains durable backlog for cold-start recovery', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir); store.append(event); store.close();
    process.env.BOTMUX_KM_AUTO_DISTILLATION_ENABLED = 'true';
    await enqueueAutomaticDistillation({ dataDir: dir, event });
    expect(await drainDistillationJobs({ dataDir: dir, maxJobs: 10 })).toBe(1);
    expect(await drainDistillationJobs({ dataDir: dir, maxJobs: 10 })).toBe(0);
  });

  it('elects one durable recovery lease holder', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    expect(store.acquireRuntimeLease({ leaseName: 'distillation-recovery', holderId: 'a', now: 1000, ttlMs: 5000 })).toBe(true);
    expect(store.acquireRuntimeLease({ leaseName: 'distillation-recovery', holderId: 'b', now: 1001, ttlMs: 5000 })).toBe(false);
    expect(store.acquireRuntimeLease({ leaseName: 'distillation-recovery', holderId: 'b', now: 7000, ttlMs: 5000 })).toBe(true);
    store.close();
  });

  it('builds a bounded evidence window', () => {
    const window = boundedEvidenceWindow(event, 'x'.repeat(300_000));
    expect(window.status).toBe('partial');
    expect(Buffer.byteLength(window.segments[0].text)).toBeLessThanOrEqual(262_144);
    expect(window.warnings).toEqual(['window_truncated']);
  });

  it('falls back to missing when a native transcript cannot be resolved', () => {
    const window = resolveBoundedTranscriptWindow({ event, cliId: 'pi', cliSessionId: 'does-not-exist', cwd: tempDir() });
    expect(window).toEqual(expect.objectContaining({ status: 'missing', warnings: ['transcript_not_found'] }));
  });

  it('records retrieval wouldInject only when shadow is enabled', async () => {
    const dir = tempDir(); let store = await ObservationStore.open(dir);
    const memory = store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Prefer Chinese', confidence: 'observed', privacyClass: 'internal', sourceRefs: [{ kind: 'api', ref: 'evt' }] }).item;
    expect(memory.state).toBe('active'); store.close();
    await runRetrievalShadow({ dataDir: dir, botAppId: 'bot', sessionId: 's1', userId: 'u1', queryText: 'Chinese' });
    process.env.BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED = 'true';
    await runRetrievalShadow({ dataDir: dir, botAppId: 'bot', sessionId: 's1', userId: 'u1', queryText: 'Chinese' });
    const { DatabaseSync } = await import('node:sqlite'); const db = new DatabaseSync(join(dir, 'botmux-km.sqlite'), { readOnly: true });
    expect(db.prepare('select count(*) n from retrieval_runs').get()).toEqual(expect.objectContaining({ n: 1 }));
    expect(db.prepare('select disposition from prompt_injection_snapshots').get()).toEqual(expect.objectContaining({ disposition: 'would_inject' })); db.close();
  });

  it('keeps federated retrieval behind its own disabled gate', async () => {
    const dir = tempDir();
    const provider: MemoryBackendProvider = {
      descriptor: { id: 'mem0', version: '1', kind: 'mem0', capabilities: { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false } },
      health: async () => ({ status: 'ok' }),
      put: async item => ({ providerId: 'mem0', backendRef: 'mem0-ref', contentHash: item.contentHash }),
      revoke: async () => {},
      retrieve: async () => [{ providerId: 'mem0', backendRef: 'remote-1', memoryId: 'remote-1', text: 'Remote Chinese preference', score: 1, scope: 'user', subject: 'u1' }],
    };
    await runRetrievalShadow({ dataDir: dir, botAppId: 'bot', sessionId: 's1', userId: 'u1', queryText: 'Chinese',
      providers: [provider], env: { BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED: 'true' } as any });
    let store = await ObservationStore.open(dir);
    expect(store.listRetrievalAudits(1)).toEqual([expect.objectContaining({ candidateCount: 0, warnings: ['federated_retrieval_gate_disabled'] })]);
    store.close();
    await runRetrievalShadow({ dataDir: dir, botAppId: 'bot', sessionId: 's2', userId: 'u1', queryText: 'Chinese',
      providers: [provider], env: { BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED: 'true', BOTMUX_KM_FEDERATED_RETRIEVAL_ENABLED: 'true' } as any });
    store = await ObservationStore.open(dir);
    expect(store.listRetrievalAudits(1)).toEqual([expect.objectContaining({ candidateCount: 1, warnings: [] })]);
    store.close();
  });

  it('composes live prompt memory only after all canary gates pass and audits requested/effective mode', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Prefer Chinese', confidence: 'observed',
      privacyClass: 'internal', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.putPipelineProfile({ ...defaultShadowProfile('bot'), profileId: 'canary', revision: 1, injectionMode: 'canary' }, 'shadow');
    store.close();

    const blocked = await composePromptMemoryForTurn({ dataDir: dir, botAppId: 'bot', sessionId: 's1', turnId: 't1', userId: 'u1',
      queryText: 'Chinese', promptContent: 'hello', env: { BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED: 'true' } as any });
    expect(blocked).toEqual({ promptContent: 'hello', injected: false, reason: 'live_gate_disabled' });

    let reopened = await ObservationStore.open(dir);
    expect(reopened.listInjectionSnapshots(1)).toEqual([expect.objectContaining({
      requestedMode: 'canary',
      effectiveMode: 'shadow',
      disposition: 'would_inject',
      reason: 'live_gate_disabled',
      itemIds: expect.arrayContaining([expect.any(String)]),
    })]);
    reopened.close();

    const injected = await composePromptMemoryForTurn({ dataDir: dir, botAppId: 'bot', sessionId: 's1', turnId: 't2', userId: 'u1',
      queryText: 'Chinese', promptContent: 'hello', env: {
        BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED: 'true',
        BOTMUX_KM_LIVE_INJECTION_ENABLED: 'true',
        BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED: 'true',
        BOTMUX_KM_CANARY_BOT_APP_IDS: 'bot',
      } as any });
    expect(injected.injected).toBe(true);
    expect(injected.promptContent).toContain('<botmux_km_context');
    expect(injected.promptContent.endsWith('\n\nhello')).toBe(true);

    reopened = await ObservationStore.open(dir);
    expect(reopened.listInjectionSnapshots(1)).toEqual([expect.objectContaining({
      requestedMode: 'canary',
      effectiveMode: 'canary',
      disposition: 'injected',
      promptHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
    })]);
    reopened.close();
  });

  it('uses process.env consistently for the allowlist when daemon callers omit env', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    store.upsertMemory({ state: 'active', scope: 'bot', subject: 'bot', claimKey: 'style', claimText: 'Use concise bullets', confidence: 'observed',
      privacyClass: 'internal', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.putPipelineProfile({ ...defaultShadowProfile('bot'), profileId: 'process-env-canary', revision: 1, injectionMode: 'canary' }, 'shadow');
    store.close();
    process.env.BOTMUX_KM_LIVE_INJECTION_ENABLED = 'true';
    process.env.BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED = 'true';
    process.env.BOTMUX_KM_CANARY_BOT_APP_IDS = 'other-bot, bot';

    const result = await composePromptMemoryForTurn({ dataDir: dir, botAppId: 'bot', sessionId: 's-process-env', turnId: 't-process-env',
      queryText: 'bullets', promptContent: 'hello' });
    expect(result.injected).toBe(true);
    expect(result.promptContent).toContain('Use concise bullets');
    const reopened = await ObservationStore.open(dir);
    expect(reopened.listInjectionSnapshots(1)).toEqual([expect.objectContaining({
      requestedMode: 'canary', effectiveMode: 'canary', disposition: 'injected',
    })]);
    reopened.close();
  });

  it('does not require the shadow retrieval gate once the explicit live gates pass', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    store.upsertMemory({ state: 'active', scope: 'bot', subject: 'bot', claimKey: 'style', claimText: 'Use concise bullets', confidence: 'observed',
      privacyClass: 'internal', sourceRefs: [{ kind: 'api', ref: 'evt' }] });
    store.putPipelineProfile({ ...defaultShadowProfile('bot'), profileId: 'active-live', revision: 1, injectionMode: 'active' }, 'shadow');
    store.close();
    const result = await composePromptMemoryForTurn({ dataDir: dir, botAppId: 'bot', sessionId: 's1', turnId: 't-live',
      queryText: 'bullets', promptContent: 'hello', env: {
        BOTMUX_KM_LIVE_INJECTION_ENABLED: 'true',
        BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED: 'true',
        BOTMUX_KM_CANARY_BOT_APP_IDS: 'bot',
      } as any });
    expect(result.injected).toBe(true);
    expect(result.promptContent).toContain('Use concise bullets');
  });

  it('does not call federated providers from the live prompt boundary', async () => {
    const dir = tempDir(); const provider: MemoryBackendProvider = {
      descriptor: { id: 'mem0', version: '1', kind: 'mem0', capabilities: { put: true, update: true, revoke: true, retrieve: true, metadataFilter: true, namespaces: true, ttl: false, snapshot: false } },
      health: async () => ({ status: 'ok' }),
      put: async item => ({ providerId: 'mem0', backendRef: 'mem0-ref', contentHash: item.contentHash }),
      revoke: async () => {},
      retrieve: async () => { throw new Error('must_not_probe_network'); },
    };
    await composePromptMemoryForTurn({ dataDir: dir, botAppId: 'bot', sessionId: 's1', userId: 'u1',
      queryText: 'Chinese', promptContent: 'hello', providers: [provider], env: {
        BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED: 'true',
        BOTMUX_KM_FEDERATED_RETRIEVAL_ENABLED: 'true',
      } as any });
    const store = await ObservationStore.open(dir);
    expect(store.listRetrievalAudits(1)).toEqual([expect.objectContaining({ warnings: ['federated_retrieval_not_live_prompt_boundary'] })]);
    store.close();
  });

  it('runs the real delivery-after-terminal path into observation, durable job, memory, and wouldInject audit', async () => {
    const dir = tempDir();
    const codexHome = join(dir, 'codex-home');
    process.env.CODEX_HOME = codexHome;
    const cliSessionId = '00000000-0000-4000-8000-000000000001';
    const rolloutDir = join(codexHome, 'sessions', '2026', '08', '26');
    mkdirSync(rolloutDir, { recursive: true });
    writeFileSync(join(rolloutDir, `rollout-2026-08-26T00-00-00-${cliSessionId}.jsonl`), [
      JSON.stringify({
        timestamp: '2026-08-26T00:00:00.000Z',
        type: 'response_item',
        payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text: '以后请用中文回复' }] },
      }),
      JSON.stringify({
        timestamp: '2026-08-26T00:00:01.000Z',
        type: 'event_msg',
        payload: { type: 'task_complete', last_agent_message: '好的。' },
      }),
      '',
    ].join('\n'));
    const feedback = await SkillFeedbackStore.open(dir);
    feedback.recordTurnTerminal({
      botAppId: 'bot',
      sessionId: 'session-real',
      turnId: 'turn-real',
      dispatchAttempt: 0,
      status: 'completed',
      completedAt: '2026-08-26T00:00:02.000Z',
    });
    const completion = feedback.recordTurnDeliveryWithCompletion({
      botAppId: 'bot',
      sessionId: 'session-real',
      turnId: 'turn-real',
      dispatchAttempt: 0,
      platform: 'lark',
      platformAppId: 'bot',
      platformMessageId: 'om-real',
      content: 'assistant body is private',
      contentRef: 'lark://om-real',
      cliId: 'codex',
      nativeSessionId: cliSessionId,
      cardMode: 'feedback',
      status: 'delivered',
      requesterSubjectId: 'u1',
    }).completion;
    feedback.close();
    expect(completion).toBeTruthy();
    const observation = observationFromTurnCompletion(completion!);
    const store = await ObservationStore.open(dir);
    store.append(observation);
    store.close();

    process.env.BOTMUX_KM_AUTO_DISTILLATION_ENABLED = 'true';
    await enqueueAutomaticDistillation({ dataDir: dir, event: observation, cliId: 'codex', cliSessionId });
    expect(await runOneDistillationJob({ dataDir: dir, cliId: 'codex' })).toBe('completed');

    const reopened = await ObservationStore.open(dir);
    expect(reopened.listDistillationJobs(10)).toEqual([expect.objectContaining({
      state: 'completed',
      sourceEventId: observation.eventId,
    })]);
    expect(reopened.listMemory({ limit: 10 })).toEqual([expect.objectContaining({
      state: 'active',
      subject: 'u1',
      claimKey: 'response.language',
    })]);
    reopened.close();

    process.env.BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED = 'true';
    await runRetrievalShadow({ dataDir: dir, botAppId: 'bot', sessionId: 'session-real', turnId: 'next-turn', userId: 'u1', queryText: 'Chinese' });
    const audit = await ObservationStore.open(dir);
    expect(audit.listInjectionSnapshots(10)).toEqual([expect.objectContaining({ disposition: 'would_inject' })]);
    audit.close();
  });
});
