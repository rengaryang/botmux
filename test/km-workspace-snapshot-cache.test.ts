import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { WorkspaceKnowledgeSnapshotCache } from '../src/services/km/workspace-knowledge/snapshot-cache.js';

const dirs: string[] = [];
afterEach(() => dirs.splice(0).forEach(dir => rmSync(dir, { recursive: true, force: true })));

describe('workspace knowledge snapshot cache', () => {
  it('discovers roots from dynamic working-directory candidates', () => {
    const root = mkdtempSync(join(tmpdir(), 'km-cache-')); dirs.push(root);
    mkdirSync(join(root, 'nested'), { recursive: true }); writeFileSync(join(root, 'AGENTS.md'), '# Rules');
    const cache = new WorkspaceKnowledgeSnapshotCache(() => [join(root, 'nested')]);
    expect(cache.get().state).toBe('unavailable');
    expect(cache.refresh()).toMatchObject({ state: 'complete', health: { totalsByLayer: { L0: 1 } } });
  });

  it('cancels scheduled refresh work on stop and remains safe across repeated starts', () => {
    vi.useFakeTimers();
    try {
      const candidates = vi.fn(() => ['/definitely/missing']); const cache = new WorkspaceKnowledgeSnapshotCache(candidates, 10);
      cache.start(); cache.start(); cache.stop(); vi.runAllTimers();
      expect(candidates).not.toHaveBeenCalled();
    } finally { vi.useRealTimers(); }
  });

  it('returns an explicit unavailable snapshot when no root can be discovered', () => {
    const cache = new WorkspaceKnowledgeSnapshotCache(() => ['/definitely/missing']);
    expect(cache.refresh()).toMatchObject({ state: 'unavailable', health: { totalAssets: 0 } });
  });
});
