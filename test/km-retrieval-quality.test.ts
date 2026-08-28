import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore, type MemoryScope } from '../src/services/km/observation-store.js';
import { normalizeRetrievalQuery } from '../src/services/km/retrieval-quality.js';
import { composePromptMemoryForTurn, defaultShadowProfile, runRetrievalShadow } from '../src/services/km/runtime-orchestrator.js';
import { planPromptMemory, type PromptMemoryCandidate } from '../src/services/km/prompt-memory.js';
import { approveKmProductionGatePlan, buildKmProductionGatePlan } from '../src/services/km/production-gate.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-retrieval-quality-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

const sourceRefs = [{ kind: 'api', ref: 'evt-quality' }];
const scopes: Array<{ scope: MemoryScope; subject: string; context: Record<string, string> }> = [
  { scope: 'user', subject: 'u1', context: { userId: 'u1' } },
  { scope: 'bot', subject: 'bot-1', context: { botAppId: 'bot-1' } },
  { scope: 'project', subject: 'proj-1', context: { projectId: 'proj-1' } },
  { scope: 'skill', subject: 'deploy', context: { skillName: 'deploy' } },
  { scope: 'environment', subject: 'e10', context: { environmentId: 'e10' } },
  { scope: 'team', subject: 'team-1', context: { teamId: 'team-1' } },
  { scope: 'workspace', subject: '/repo/app', context: { workspaceId: '/repo/app' } },
];

function memory(scope: MemoryScope, subject: string, text = `Prefer Chinese response for ${scope}`): PromptMemoryCandidate {
  return { id: `mem-${scope}`, kind: 'memory', title: `${scope}.preference`, text, score: 1,
    sourceRefs, privacyClass: 'internal', freshness: 'fresh', state: 'active', scope, subject, providerIds: ['sqlite'] };
}

describe('KM retrieval normalization and visibility', () => {
  it('expands deterministic bilingual query terms while preserving exact evidence text', async () => {
    const store = await ObservationStore.open(tempDir());
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'response.language',
      claimText: 'Prefer Chinese response. Keep Markdown headings.', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    const zhToEn = store.retrieveWithMetrics({ text: '请用中文回复', scopes: ['user'], subjects: { user: 'u1' }, limit: 10 });
    expect(zhToEn.items).toEqual([expect.objectContaining({
      title: 'response.language',
      text: 'Prefer Chinese response. Keep Markdown headings.',
      matchKind: 'normalized',
    })]);
    expect(zhToEn.metrics.normalizedHitCount).toBe(1);

    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u2', claimKey: 'response.language',
      claimText: '以后请用中文回复，保留 Markdown 标题。', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    const enToZh = store.retrieveWithMetrics({ text: 'Chinese answer in markdown', scopes: ['user'], subjects: { user: 'u2' }, limit: 10 });
    expect(enToZh.items[0]).toEqual(expect.objectContaining({
      text: '以后请用中文回复，保留 Markdown 标题。',
      matchKind: 'normalized',
    }));
    expect(enToZh.items[0].text).toBe('以后请用中文回复，保留 Markdown 标题。');
    store.close();
  });

  it('handles phrases and token boundaries without matching substrings accidentally', () => {
    const normalized = normalizeRetrievalQuery('environment permission and node pool');
    expect(normalized.groups.map(group => group.canonical)).toEqual(expect.arrayContaining(['environment', 'permission', 'nodepool']));
    expect(normalized.groups.map(group => group.canonical)).not.toContain('english');
  });

  it('keeps ranking deterministic across normalized and direct lexical hits', async () => {
    const store = await ObservationStore.open(tempDir());
    store.upsertMemory({ state: 'active', scope: 'bot', subject: 'bot-1', claimKey: 'b.preference',
      claimText: 'Prefer Chinese response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.upsertMemory({ state: 'active', scope: 'bot', subject: 'bot-1', claimKey: 'a.preference',
      claimText: 'Prefer Chinese response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    const first = store.retrieve({ text: '中文回复', scopes: ['bot'], subjects: { bot: 'bot-1' }, limit: 10 }).map(item => item.id);
    const second = store.retrieve({ text: '中文回复', scopes: ['bot'], subjects: { bot: 'bot-1' }, limit: 10 }).map(item => item.id);
    expect(first).toEqual(second);
    expect(first).toEqual([...first].sort());
    const direct = store.retrieveWithMetrics({ text: 'Prefer Chinese response', scopes: ['bot'], subjects: { bot: 'bot-1' }, limit: 10 });
    expect(direct.metrics.directHitCount).toBe(2);
    expect(direct.metrics.normalizedHitCount).toBe(0);
    store.close();
  });

  it('requires an explicit matching context for every memory scope', () => {
    const candidates = scopes.map(({ scope, subject }) => memory(scope, subject));
    const allContext = scopes.reduce((acc, value) => ({ ...acc, ...value.context }), {});
    const all = planPromptMemory(candidates, { botAppId: 'bot-1', mode: 'shadow', promptTokenBudget: 1800, ...allContext });
    expect(all.eligible.map(item => item.scope)).toEqual(['bot', 'environment', 'project', 'skill', 'team', 'user', 'workspace']);

    for (const { scope, context } of scopes) {
      const plan = planPromptMemory(candidates, { botAppId: 'wrong-bot', mode: 'shadow', promptTokenBudget: 1800, ...context });
      expect(plan.eligible.map(item => item.scope)).toEqual([scope]);
      expect(plan.filtered.filter(item => item.reason === 'scope_mismatch')).toHaveLength(scopes.length - 1);
    }
  });

  it('does not make broader scopes eligible when their context is absent', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    for (const { scope, subject } of scopes) store.upsertMemory({ state: 'active', scope, subject, claimKey: `${scope}.language`,
      claimText: `Prefer Chinese response for ${scope}`, confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.close();
    await runRetrievalShadow({ dataDir: dir, botAppId: 'bot-1', sessionId: 's1', userId: 'u1', queryText: '中文回复',
      env: { BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED: 'true' } as any });
    const audit = await ObservationStore.open(dir);
    expect(audit.listRetrievalAudits(1)).toEqual([expect.objectContaining({
      candidateCount: 2,
      normalizedHitCount: 2,
      filteredScopeCount: 5,
    })]);
    audit.close();
  });

  it('filters memory scopes fail-closed when no exact subject is available', async () => {
    const store = await ObservationStore.open(tempDir());
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language',
      claimText: 'Prefer Chinese response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    const result = store.retrieveWithMetrics({ text: '中文回复', scopes: ['user'], limit: 10 });
    expect(result.items).toEqual([]);
    expect(result.metrics.filteredScopeCount).toBe(1);
    store.close();
  });

  it('does not count filtered scope, privacy, or state rows as lexical no-hit', async () => {
    const store = await ObservationStore.open(tempDir());
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'other-user', claimKey: 'unrelated.scope',
      claimText: 'Always escalate deployment failures', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.upsertMemory({ state: 'proposed', scope: 'user', subject: 'u1', claimKey: 'unrelated.state',
      claimText: 'Always escalate deployment failures', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'unrelated.privacy',
      claimText: 'Always escalate deployment failures', confidence: 'observed', privacyClass: 'sensitive', sourceRefs });
    const result = store.retrieveWithMetrics({ text: '中文回复', scopes: ['user'], subjects: { user: 'u1' }, limit: 10 });
    expect(result.items).toEqual([]);
    expect(result.metrics.noHitCount).toBe(0);
    expect(result.metrics.filteredScopeCount).toBe(1);
    expect(result.metrics.filteredStateCount).toBe(1);
    expect(result.metrics.filteredPrivacyCount).toBe(1);
    store.close();
  });

  it('keeps cross-user and cross-bot memories isolated in live composition', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u-other', claimKey: 'other.language',
      claimText: 'Prefer English response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.upsertMemory({ state: 'active', scope: 'bot', subject: 'bot-other', claimKey: 'other.bot.language',
      claimText: 'Prefer English response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'own.language',
      claimText: 'Prefer Chinese response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.putPipelineProfile({ ...defaultShadowProfile('bot-1'), profileId: 'active-quality', revision: 1, injectionMode: 'active' }, 'shadow');
    const now = new Date();
    const gate = buildKmProductionGatePlan({
      actionKind: 'prompt-canary',
      target: { botAppId: 'bot-1', window: { start: new Date(now.getTime() - 60_000).toISOString(), end: new Date(now.getTime() + 60 * 60_000).toISOString() } },
      scope: { botAppId: 'bot-1', sessionClass: 'manual-canary' },
      actorId: 'operator-1', riskAck: { acknowledged: true, ticket: 'KM-QUALITY' }, confirmationToken: 'quality-token',
      ttlSeconds: 3600, now: now.toISOString(),
    });
    store.createProductionGatePlan(gate.plan);
    approveKmProductionGatePlan(store, {
      planId: gate.plan.planId, actorId: 'operator-2', approvalGrade: 'G2', confirmationToken: 'quality-token',
      previewHash: gate.plan.previewHash, riskAck: { acknowledged: true, ticket: 'KM-QUALITY' },
      now: new Date(now.getTime() + 1_000).toISOString(),
    });
    store.close();
    const result = await composePromptMemoryForTurn({ dataDir: dir, botAppId: 'bot-1', sessionId: 's1', turnId: 't1', userId: 'u1',
      queryText: 'response language', promptContent: 'hello', env: {
        BOTMUX_KM_LIVE_INJECTION_ENABLED: 'true',
        BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED: 'true',
        BOTMUX_KM_CANARY_BOT_APP_IDS: 'bot-1',
      } as any });
    expect(result.injected).toBe(true);
    expect(result.promptContent).toContain('Prefer Chinese response');
    expect(result.promptContent).not.toContain('Prefer English response');
  });

  it('audits normalized hits, no-hit, filtered scope, privacy, state, and latency', async () => {
    const dir = tempDir();
    const store = await ObservationStore.open(dir);
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language',
      claimText: 'Prefer Chinese response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u2', claimKey: 'language.other',
      claimText: 'Prefer Chinese response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.upsertMemory({ state: 'proposed', scope: 'user', subject: 'u1', claimKey: 'language.pending',
      claimText: 'Prefer Chinese response', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language.private',
      claimText: 'Prefer Chinese response', confidence: 'observed', privacyClass: 'sensitive', sourceRefs });
    store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'unrelated',
      claimText: 'Always use terse prose', confidence: 'observed', privacyClass: 'internal', sourceRefs });
    store.close();

    await runRetrievalShadow({ dataDir: dir, botAppId: 'bot-1', sessionId: 's1', userId: 'u1', queryText: '中文回复',
      env: { BOTMUX_KM_RETRIEVAL_SHADOW_ENABLED: 'true' } as any });
    const audit = await ObservationStore.open(dir);
    expect(audit.listRetrievalAudits(1)).toEqual([expect.objectContaining({
      candidateCount: 1,
      eligibleCount: 1,
      directHitCount: 0,
      normalizedHitCount: 1,
      noHitCount: 1,
      filteredScopeCount: 1,
      filteredPrivacyCount: 1,
      filteredStateCount: 1,
      latencyMs: expect.any(Number),
    })]);
    expect(audit.retrievalQualitySummary()).toEqual(expect.objectContaining({
      runs: 1,
      directHits: 0,
      normalizedHits: 1,
      noHits: 1,
      filteredScope: 1,
      filteredPrivacy: 1,
      filteredState: 1,
    }));
    audit.close();
  });
});
