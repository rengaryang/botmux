import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { decideSafeMemoryActivation } from '../src/services/km/safe-memory-policy.js';
import { ObservationStore } from '../src/services/km/observation-store.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-m4-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
const base = { confidence: 'observed' as const, explicitUserStatement: true, scope: 'user' as const, subject: 'u1', claimKey: 'output.language', claimText: 'Prefer Chinese', privacyClass: 'internal' as const, sourceRefs: [{ kind: 'api', ref: 'evt-1' }], evidenceSpanComplete: true };

describe('safe memory activation and backend migration', () => {
  it('auto-activates only explicit observed low-risk user/bot preferences', () => {
    const decision = decideSafeMemoryActivation(base, new Date('2026-01-01T00:00:00Z'));
    expect(decision).toEqual(expect.objectContaining({ disposition: 'activate', activationMode: 'policy-auto', policyVersion: 'safe-auto-activation-v1' }));
    expect(decision.memory).toEqual(expect.objectContaining({ state: 'active', reviewAfter: '2026-04-01T00:00:00.000Z' }));
  });

  it('demotes inferred, broad-scope and high-risk preferences to proposed', () => {
    expect(decideSafeMemoryActivation({ ...base, confidence: 'inferred' }).disposition).toBe('propose');
    expect(decideSafeMemoryActivation({ ...base, scope: 'team' }).disposition).toBe('propose');
    expect(decideSafeMemoryActivation({ ...base, policyTags: ['production-operation'] }).disposition).toBe('propose');
    expect(decideSafeMemoryActivation({ ...base, privacyClass: 'sensitive' }).disposition).toBe('reject');
  });

  it('durably deduplicates failed mirror operations in an outbox', async () => {
    const store = await ObservationStore.open(tempDir()); expect(store.schemaVersion()).toBe(10);
    const memory = store.upsertMemory({ state: 'active', scope: 'user', subject: 'u1', claimKey: 'language', claimText: 'Chinese', confidence: 'observed', privacyClass: 'internal', sourceRefs: base.sourceRefs }).item;
    const first = store.enqueueMemoryBackendOperation({ memoryId: memory.memoryId, providerId: 'hindsight', operation: 'put', payload: { text: 'Chinese' }, now: 1000 });
    expect(first.created).toBe(true);
    expect(store.enqueueMemoryBackendOperation({ memoryId: memory.memoryId, providerId: 'hindsight', operation: 'put', payload: { text: 'Chinese' }, now: 2000 })).toEqual({ outboxId: first.outboxId, created: false });
    store.close();
  });

  it('enforces backfill/compare/ready/cutover/rollback migration order', async () => {
    const store = await ObservationStore.open(tempDir());
    const id = store.createMemoryBackendMigration({ botAppId: 'bot-1', fromProfile: { primary: 'mem0' }, toProfile: { primary: 'openviking' } });
    expect(() => store.transitionMemoryBackendMigration({ migrationId: id, toState: 'cutover' })).toThrow(/invalid_transition/);
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'backfilling', checkpoint: '0' });
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'comparing', checkpoint: '100', stats: { copied: 100 } });
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'ready', stats: { mismatch: 0 } });
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'cutover' });
    store.transitionMemoryBackendMigration({ migrationId: id, toState: 'rolled_back' });
    store.close();
  });
});
