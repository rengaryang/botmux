import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { WorkflowExecutionProfileStore } from '../src/workflows/v3/execution-profile-store.js';
import { collectWorkflowProfileHistory, recommendWorkflowProfiles } from '../src/workflows/v3/model-recommender.js';
import { executionProfileToSnapshot } from '../src/workflows/v3/bot-resolve.js';
import { validateDag } from '../src/workflows/v3/dag.js';

const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
function dir() { const value = mkdtempSync(join(tmpdir(), 'v3-profile-')); dirs.push(value); return value; }

describe('Workflow execution profiles', () => {
  it('persists versioned direct CLI profiles and freezes no Lark secret identity', () => {
    const root = dir(); const store = new WorkflowExecutionProfileStore(join(root, 'profiles.json'));
    const saved = store.put({ profileId: 'code-fast', displayName: 'Code Fast', cli: 'codex', model: 'gpt-code', workingDir: root, costTier: 'medium' });
    expect(saved.revision).toBe(1);
    const updated = store.put({ ...saved, model: 'gpt-code-2' });
    expect(updated.revision).toBe(2);
    expect(store.list()).toHaveLength(1);
    expect(executionProfileToSnapshot(updated)).toMatchObject({ larkAppId: 'profile:code-fast', directCli: true, cliId: 'codex', model: 'gpt-code-2', workingDir: root });
  });

  it('persists provider metadata and freezes a provider-qualified Pi model', () => {
    const root = dir(); const store = new WorkflowExecutionProfileStore(join(root, 'profiles.json'));
    const saved = store.put({ profileId: 'pi-glm', displayName: 'Pi GLM', cli: 'pi', provider: 'bytedance-hybrid', model: 'glm-5.3', workingDir: root, costTier: 'low' });
    expect(saved).toMatchObject({ provider: 'bytedance-hybrid', model: 'bytedance-hybrid/glm-5.3' });
    expect(executionProfileToSnapshot(saved)).toMatchObject({ provider: 'bytedance-hybrid', model: 'bytedance-hybrid/glm-5.3', directCli: true });
    expect(() => store.put({ ...saved, provider: 'codex-lb' })).toThrow(/provider_model_mismatch/);
  });

  it('validates executionProfile as mutually exclusive with legacy bot', () => {
    expect(validateDag({ schemaVersion: 2, runId: 'profile-dag', nodes: [{ id: 'code', type: 'goal', goal: '实现代码', executionProfile: 'code-fast' }] }).nodes[0]).toMatchObject({ executionProfile: 'code-fast' });
    expect(() => validateDag({ schemaVersion: 2, runId: 'bad-profile-dag', nodes: [{ id: 'code', type: 'goal', goal: '实现代码', executionProfile: 'code-fast', bot: 'legacy' }] })).toThrow(/cannot set both/);
  });

  it('scores deterministically and marks missing history as cold start', () => {
    const root = dir(); const store = new WorkflowExecutionProfileStore(join(root, 'profiles.json'));
    const profile = store.put({ profileId: 'code-fast', displayName: 'Code Codex', cli: 'codex', workingDir: root, costTier: 'low' });
    const result = recommendWorkflowProfiles({ goal: '实现并修复代码', profiles: [profile], history: collectWorkflowProfileHistory(join(root, 'runs')) });
    expect(result[0]).toMatchObject({ profileId: 'code-fast', taskType: 'code', coldStart: true });
    expect(result[0]!.reasons).toContain('cold_start:samples=0');
  });
});
