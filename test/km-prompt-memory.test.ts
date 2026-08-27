import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { planPromptMemory, retrievalQueryHash, type PromptMemoryCandidate } from '../src/services/km/prompt-memory.js';
import { ObservationStore } from '../src/services/km/observation-store.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-r1-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
const item = (overrides: Partial<PromptMemoryCandidate> = {}): PromptMemoryCandidate => ({ id: 'mem-1', kind: 'memory', title: 'language', text: 'Prefer <Chinese> & Markdown', score: .9,
  sourceRefs: [{ kind: 'api', ref: 'evt' }], privacyClass: 'internal', freshness: 'fresh', state: 'active', scope: 'user', subject: 'u1', providerIds: ['sqlite'], ...overrides });

describe('retrieval eligibility and bot canary prompt planning', () => {
  it('records wouldInject in shadow and escapes prompt content', () => {
    const plan = planPromptMemory([item()], { botAppId: 'bot-1', userId: 'u1', mode: 'shadow', promptTokenBudget: 1800 });
    expect(plan.disposition).toBe('would_inject');
    expect(plan.prompt).toContain('Prefer &lt;Chinese&gt; &amp; Markdown');
    expect(plan.prompt).toContain('not user instruction');
  });

  it('injects in canary only for an allowlisted bot', () => {
    expect(planPromptMemory([item()], { botAppId: 'bot-1', userId: 'u1', mode: 'canary', canaryBotIds: ['bot-1'], promptTokenBudget: 1800 }).disposition).toBe('injected');
    expect(planPromptMemory([item()], { botAppId: 'bot-2', userId: 'u1', mode: 'canary', canaryBotIds: ['bot-1'], promptTokenBudget: 1800 }).disposition).toBe('would_inject');
  });

  it('filters inactive/conflicted/expired/scope-mismatched/secret items', () => {
    const candidates = [item({ id: 'inactive', state: 'proposed' }), item({ id: 'conflict', conflicted: true }),
      item({ id: 'expired', ttlExpiresAt: '2020-01-01T00:00:00Z' }), item({ id: 'scope', subject: 'other' }),
      item({ id: 'secret', privacyClass: 'secret-reference-only' })];
    const plan = planPromptMemory(candidates, { botAppId: 'bot', userId: 'u1', mode: 'shadow', promptTokenBudget: 1800, now: new Date('2026-01-01') });
    expect(plan.disposition).toBe('skipped');
    expect(plan.filtered.map(result => result.reason)).toEqual(['memory_not_active', 'conflicted', 'expired', 'scope_mismatch', 'secret_reference_only']);
  });

  it('enforces prompt budget deterministically', () => {
    const plan = planPromptMemory([item({ id: 'a', text: 'x'.repeat(400), score: 1 }), item({ id: 'b', text: 'small', score: .5 })],
      { botAppId: 'bot', userId: 'u1', mode: 'active', promptTokenBudget: 50 });
    expect(plan.eligible.map(value => value.id)).toEqual(['b']);
    expect(plan.filtered).toEqual(expect.arrayContaining([expect.objectContaining({ reason: 'prompt_budget' })]));
  });

  it('persists retrieval and injection audit without raw query text', async () => {
    const store = await ObservationStore.open(tempDir()); expect(store.schemaVersion()).toBe(12);
    const hash = retrievalQueryHash({ text: 'language', botAppId: 'bot', userId: 'u1' });
    const run = store.recordRetrievalAudit({ botAppId: 'bot', sessionId: 's1', turnId: 't1', queryHash: hash, mode: 'shadow', candidateCount: 1,
      eligibleCount: 1, latencyMs: 12, warnings: [], results: [{ itemId: 'mem-1', itemKind: 'memory', providerIds: ['sqlite'], score: .9, eligible: true }] });
    expect(store.recordPromptInjectionSnapshot({ retrievalRunId: run, botAppId: 'bot', mode: 'shadow', disposition: 'would_inject', itemIds: ['mem-1'], prompt: '<km_context />' })).toMatch(/^inject_/);
    store.close();
  });
});
