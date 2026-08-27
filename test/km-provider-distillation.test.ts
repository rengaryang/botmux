import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { KmPipelineProfileSchema, KmProviderDescriptorSchema, type KmPipelineProfile } from '../src/services/km/provider-spi.js';
import { buildCliDistillationInvocation, runCliDistillation } from '../src/services/km/cli-distillation-runner.js';

const dirs: string[] = [];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-v6-')); dirs.push(dir); return dir; }
afterEach(() => { delete process.env.BOTMUX_KM_WORKLOAD; for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

const profile: KmPipelineProfile = {
  schemaVersion: 1, profileId: 'default', revision: 1, botAppId: 'bot-1',
  sourceProvider: 'observation-v1', windowProvider: 'transcript-window-v1',
  primaryExtractor: 'botmux-cli:pi:default', shadowExtractors: ['builtin.rules-v1'],
  knowledgeRouter: 'layer-router-v1', memoryPolicy: 'safe-auto-v1',
  memoryBackends: { writePolicy: 'primary-mirror', primary: 'mem0', mirrors: ['hindsight', 'openviking'] },
  injectionMode: 'shadow', budgets: { sourceBytes: 262144, sourceTokens: 32000, outputClaims: 20, promptTokens: 1800 },
};
const window = { status: 'resolved' as const, segments: [{ id: 's1', text: 'User explicitly prefers Chinese.', start: 0, end: 32 }], warnings: [] };

describe('KM provider SPI and durable distillation', () => {
  it('validates descriptors and bot-scoped profiles', () => {
    expect(KmProviderDescriptorSchema.parse({ id: 'mem0', kind: 'memory-backend', version: '1', contractVersion: 1,
      capabilities: ['put', 'retrieve'], execution: 'service', deterministic: false, supportsShadow: true, maxBatchSize: 50 }).id).toBe('mem0');
    expect(KmPipelineProfileSchema.parse(profile).botAppId).toBe('bot-1');
    expect(() => KmProviderDescriptorSchema.parse({})).toThrow();
    expect(() => KmPipelineProfileSchema.parse({ ...profile, shadowExtractors: [profile.primaryExtractor] })).toThrow(/primary cannot also be shadow/);
  });

  it('persists provider/profile and claims a snapshot-stable job idempotently', async () => {
    const store = await ObservationStore.open(tempDir());
    expect(store.schemaVersion()).toBe(10);
    store.registerKmProvider({ id: 'botmux-cli:pi:default', kind: 'extractor', version: '1', contractVersion: 1,
      capabilities: ['strict-json'], execution: 'botmux-cli', deterministic: false, supportsShadow: true, maxBatchSize: 1 });
    expect(store.putPipelineProfile(profile, 'active')).toMatch(/^sha256:/);
    expect(store.listPipelineProfiles('bot-1')).toEqual([expect.objectContaining({ state: 'active', profile })]);
    expect(store.getEffectivePipelineProfile('bot-1')).toEqual(profile);
    store.putMemoryProviderConfig({ providerId: 'mem0', endpoint: 'https://memory.example.test', credentialRef: 'env:MEM0_API_KEY',
      enabled: true, realTransportEnabled: false, timeoutMs: 5000 });
    expect(store.listMemoryProviderConfigs()).toEqual([expect.objectContaining({ providerId: 'mem0', credentialRef: 'env:***',
      enabled: true, realTransportEnabled: false })]);
    expect(store.memoryProviderConfigurationHealth('mem0', {})).toEqual(expect.objectContaining({ status: 'credential_missing',
      transportChecked: false, realTransportEnabled: false }));
    const first = store.createDistillationJob({ sourceEventId: 'evt-1', profile, now: 1000 });
    expect(first.created).toBe(true);
    expect(store.createDistillationJob({ sourceEventId: 'evt-1', profile, now: 1000 })).toEqual({ jobId: first.jobId, created: false });
    const claimed = store.claimDistillationJob({ now: 1000 });
    expect(claimed).toEqual(expect.objectContaining({ jobId: first.jobId, sourceEventId: 'evt-1' }));
    expect(claimed?.profile).toEqual(profile);
    store.finishDistillationJob({ jobId: first.jobId, claimToken: claimed!.claimToken, outputHash: `sha256:${'a'.repeat(64)}` });
    expect(store.claimDistillationJob({ now: 1000 })).toBeNull();
    expect(store.setPipelineProfileState({ profileId: 'default', revision: 1, state: 'retired' })).toEqual(expect.objectContaining({ state: 'retired' }));
    store.close();
  });

  it('builds a no-tool isolated workload and blocks recursive distillation', () => {
    const invocation = buildCliDistillationInvocation({ cliId: 'pi', sourceEventId: 'evt-1', profile, window });
    expect(invocation.env.BOTMUX_KM_WORKLOAD).toBe('distillation');
    expect(invocation.systemPrompt).toMatch(/no tools/i);
    expect(invocation.userPrompt).toContain('<untrusted_evidence>');
    process.env.BOTMUX_KM_WORKLOAD = 'distillation';
    expect(() => buildCliDistillationInvocation({ cliId: 'pi', sourceEventId: 'evt-1', profile, window })).toThrow(/recursion_blocked/);
  });

  it('parses strict JSON and demotes inferred active memory', async () => {
    const invoke = vi.fn(async () => JSON.stringify({
      knowledge: [{ targetLayer: 'reviewed-only', category: 'preference', title: 'Language', claimKey: 'user.language',
        claimText: 'Prefers Chinese', confidence: 'observed', freshness: 'fresh', privacyClass: 'internal', sourceRefs: [{ kind: 'api', ref: 'evt-1' }] }],
      memories: [{ state: 'active', scope: 'user', subject: 'u1', claimKey: 'user.language', claimText: 'Prefers Chinese',
        confidence: 'inferred', sourceRefs: [{ kind: 'api', ref: 'evt-1' }], syncPolicy: 'local-only', privacyClass: 'internal' }],
      discarded: [], warnings: [],
    }));
    const result = await runCliDistillation({ cliId: 'pi', sourceEventId: 'evt-1', profile, window }, { invoke });
    expect(result.output.memories[0].state).toBe('proposed');
    expect(result.outputHash).toMatch(/^sha256:[a-f0-9]{64}$/);
  });

  it('rejects non-JSON, missing evidence and claim budget overflow', async () => {
    await expect(runCliDistillation({ cliId: 'pi', sourceEventId: 'evt-1', profile, window }, { invoke: async () => 'nope' })).rejects.toThrow(/invalid_json/);
    const bad = JSON.stringify({ knowledge: [{ targetLayer: 'L2', category: 'x', title: 'x', claimKey: 'x', claimText: 'x', confidence: 'observed', freshness: 'fresh', privacyClass: 'internal', sourceRefs: [] }], memories: [], discarded: [], warnings: [] });
    await expect(runCliDistillation({ cliId: 'pi', sourceEventId: 'evt-1', profile, window }, { invoke: async () => bad })).rejects.toThrow();
  });
});
