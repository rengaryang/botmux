import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { discoverWorkspaceKnowledgeRoots, scanWorkspaceKnowledge } from '../src/services/km/workspace-knowledge/scanner.js';

const dirs: string[] = [];
function fixture(): string { const root = mkdtempSync(join(tmpdir(), 'km-workspace-v2-')); dirs.push(root); return root; }
afterEach(() => dirs.splice(0).forEach(dir => rmSync(dir, { recursive: true, force: true })));

describe('workspace knowledge adapter v2', () => {
  it('discovers a workspace from a nested session working directory', () => {
    const root = fixture(); mkdirSync(join(root, 'repo', 'nested'), { recursive: true }); writeFileSync(join(root, 'AGENTS.md'), '# Rules');
    expect(discoverWorkspaceKnowledgeRoots([join(root, 'repo', 'nested')])).toEqual([root]);
  });

  it('scans all layers and tolerates mixed legacy/v3 L2 entries', () => {
    const root = fixture();
    writeFileSync(join(root, 'AGENTS.md'), '# Rules');
    mkdirSync(join(root, 'docs/wiki'), { recursive: true }); writeFileSync(join(root, 'docs/wiki/arch.md'), '# Architecture');
    mkdirSync(join(root, '.agents/skills/demo/references'), { recursive: true });
    writeFileSync(join(root, '.agents/skills/demo/SKILL.md'), '# Demo'); writeFileSync(join(root, '.agents/skills/demo/references/api.md'), '# API');
    mkdirSync(join(root, 'l2-knowledge/new'), { recursive: true }); mkdirSync(join(root, 'l2-knowledge/old'), { recursive: true });
    writeFileSync(join(root, 'l2-knowledge/new/item.md'), `---\nid: l2k-1\nstatus: pending-ingest\ncreated_at: 2026-09-01T00:00:00Z\ntitle: New\ntags:\n  - new\ntrigger_scenarios:\n  - issue\ncategory: 故障复盘与问题沉淀\nknowledge_type: SOP\n---\n# New`);
    writeFileSync(join(root, 'l2-knowledge/old/item.md'), '# Legacy');
    writeFileSync(join(root, 'l2-knowledge/INDEX.json'), JSON.stringify({ entries: [
      { id: 'l2k-1', file: 'new/item.md', title: 'New' },
      { id: 'legacy-1', path: 'old/item.md', title: 'Legacy', recall_count: 0 },
    ] }));
    writeFileSync(join(root, 'l2-knowledge/.recall_log.jsonl'), `${JSON.stringify({ event_type: 'index_query' })}\n${JSON.stringify({ entry_id: 'l2k-1', recalled_at: '2026-09-01T01:00:00Z' })}\n`);

    const result = scanWorkspaceKnowledge({ roots: [root], now: Date.parse('2026-09-01T02:00:00Z') });
    expect(result.state).toBe('complete');
    expect(result.health.totalsByLayer).toEqual({ L0: 1, L1: 1, L2: 2, L3: 1, L4: 1 });
    expect(result.health.legacyAssets).toBe(1);
    expect(result.health.contractValidRate).toBe(100);
    expect(result.health.contractErrors).toBe(1);
    expect(result.health.lifecycle).toMatchObject({ 'pending-ingest': 1, legacy: 1 });
    expect(result.retrievalQuality).toMatchObject({ indexQueries: 1, entryRecallEvents: 1, neverRecalledAssets: 1, effectivenessRate: null, evidenceState: 'cold_start' });
    expect(result.assets.find(item => item.assetId.endsWith('L2:id:l2k-1'))).toMatchObject({ freshness: 'fresh', retrieval: { recallCount: 1 } });
  });

  it('does not follow a symlink outside the discovered root', () => {
    const root = fixture(); const outside = fixture(); writeFileSync(join(root, 'AGENTS.md'), '# Rules'); writeFileSync(join(outside, 'secret.md'), '# Secret');
    mkdirSync(join(root, 'docs'), { recursive: true }); symlinkSync(outside, join(root, 'docs/wiki'));
    const result = scanWorkspaceKnowledge({ roots: [root] });
    expect(result.assets.some(asset => asset.title === 'Secret')).toBe(false);
  });
});
