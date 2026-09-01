import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { recordWorkspaceRetrievalEvidence } from '../src/services/km/workspace-knowledge/retrieval-evidence.js';

const dirs: string[] = [];
afterEach(() => dirs.splice(0).forEach(dir => rmSync(dir, { recursive: true, force: true })));
function fixture(): string { const root = mkdtempSync(join(tmpdir(), 'km-evidence-')); dirs.push(root); mkdirSync(join(root, 'l2-knowledge')); writeFileSync(join(root, 'l2-knowledge/INDEX.json'), '{"entries":[]}'); return root; }

describe('workspace retrieval evidence producer', () => {
  it('appends only a hash-only allowlisted event to the discovered workspace', () => {
    const root = fixture(); const queryHash = `sha256:${'a'.repeat(64)}`;
    recordWorkspaceRetrievalEvidence({ workingDir: root, eventType: 'entry_used', queryHash, entryId: 'l2k-1', useLabel: 'direct_apply', observedAt: '2026-09-01T00:00:00Z' });
    const row = JSON.parse(readFileSync(join(root, 'l2-knowledge/.recall_log.jsonl'), 'utf8'));
    expect(row).toEqual({ event_type: 'entry_used', query_hash: queryHash, observed_at: '2026-09-01T00:00:00Z', entry_id: 'l2k-1', use_label: 'direct_apply' });
    expect(JSON.stringify(row)).not.toContain('query_text');
  });

  it('rejects raw query identifiers, unsafe entry IDs, and unknown workspaces', () => {
    const root = fixture(); const queryHash = `sha256:${'b'.repeat(64)}`;
    expect(() => recordWorkspaceRetrievalEvidence({ workingDir: root, eventType: 'index_query', queryHash: 'raw query' })).toThrow('query_hash_invalid');
    expect(() => recordWorkspaceRetrievalEvidence({ workingDir: root, eventType: 'entry_read', queryHash, entryId: '../escape' })).toThrow('entry_id_invalid');
    expect(() => recordWorkspaceRetrievalEvidence({ workingDir: '/definitely/missing', eventType: 'index_query', queryHash })).toThrow('workspace_not_found');
  });
});
