import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { boundedEvidenceWindow, defaultShadowProfile, drainDistillationJobs, enqueueAutomaticDistillation, isKmAutoDistillationEnabled, isKmRetrievalShadowEnabled, resolveBoundedTranscriptWindow, runOneDistillationJob, runRetrievalShadow } from '../src/services/km/runtime-orchestrator.js';
import { observationFromTurnCompletion } from '../src/services/km/observation-producers.js';
import { SkillFeedbackStore } from '../src/services/skill-feedback-store.js';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-runtime-')); dirs.push(dir); return dir; }
afterEach(() => { delete process.env.BOTMUX_KM_AUTO_DISTILLATION_ENABLED; delete process.env.BOTMUX_KM_PI_SHADOW_ENABLED; delete process.env.BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED; delete process.env.CODEX_HOME; for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
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
