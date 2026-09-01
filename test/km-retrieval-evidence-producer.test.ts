import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  findWorkspaceL2EntryIds,
  recordAutomaticWorkspaceRetrievalEvidence,
  recordWorkspaceRetrievalEvidence,
} from '../src/services/km/workspace-knowledge/retrieval-evidence.js';
import { recordTranscriptWorkspaceRetrievalEvidence } from '../src/services/km/workspace-knowledge/transcript-evidence.js';

const dirs: string[] = [];
afterEach(() => dirs.splice(0).forEach(dir => rmSync(dir, { recursive: true, force: true })));
function fixture(): string {
  const root = mkdtempSync(join(tmpdir(), 'km-evidence-')); dirs.push(root); mkdirSync(join(root, 'l2-knowledge/items'), { recursive: true });
  writeFileSync(join(root, 'l2-knowledge/items/one.md'), '# One');
  writeFileSync(join(root, 'l2-knowledge/INDEX.json'), JSON.stringify({ entries: [{ id: 'l2k_one', file: 'items/one.md' }] }));
  return root;
}

describe('workspace retrieval evidence producer', () => {
  it('appends only a hash-only allowlisted event to the discovered workspace', () => {
    const root = fixture(); const queryHash = `sha256:${'a'.repeat(64)}`;
    recordWorkspaceRetrievalEvidence({ workingDir: root, eventType: 'entry_used', queryHash, entryId: 'l2k-1', useLabel: 'direct_apply', observedAt: '2026-09-01T00:00:00Z' });
    const row = JSON.parse(readFileSync(join(root, 'l2-knowledge/.recall_log.jsonl'), 'utf8'));
    expect(row).toEqual(expect.objectContaining({ event_type: 'entry_used', query_hash: queryHash, observed_at: '2026-09-01T00:00:00Z', entry_id: 'l2k-1', use_label: 'direct_apply' }));
    expect(row.event_id).toMatch(/^kre_[a-f0-9]{64}$/);
    expect(JSON.stringify(row)).not.toContain('query_text');
  });

  it('rejects raw query identifiers, unsafe entry IDs, and unknown workspaces', () => {
    const root = fixture(); const queryHash = `sha256:${'b'.repeat(64)}`;
    expect(() => recordWorkspaceRetrievalEvidence({ workingDir: root, eventType: 'index_query', queryHash: 'raw query' })).toThrow('query_hash_invalid');
    expect(() => recordWorkspaceRetrievalEvidence({ workingDir: root, eventType: 'entry_read', queryHash, entryId: '../escape' })).toThrow('entry_id_invalid');
    expect(() => recordWorkspaceRetrievalEvidence({ workingDir: '/definitely/missing', eventType: 'index_query', queryHash })).toThrow('workspace_not_found');
  });

  it('deduplicates repeated automatic events while preserving correlation fields', () => {
    const root = fixture(); const queryHash = `sha256:${'c'.repeat(64)}`;
    const input = {
      workingDir: root,
      queryHash,
      botAppId: 'bot_1',
      sessionId: 'sess_1',
      turnId: 'turn_1',
      retrievalRunId: 'retr_1',
      source: 'prompt_memory' as const,
      candidates: [{ id: 'mem_1', sourceRefs: [{ kind: 'km-import', sourceRef: { kind: 'markdown_file', relativePath: 'l2-knowledge/items/one.md' } }] }],
      selectedItemIds: ['mem_1'],
      useLabel: 'direct_apply' as const,
      observedAt: '2026-09-01T00:00:00Z',
    };
    expect(recordAutomaticWorkspaceRetrievalEvidence(input)).toMatchObject({ recorded: 3, skipped: false, warnings: [] });
    expect(recordAutomaticWorkspaceRetrievalEvidence(input)).toMatchObject({ recorded: 0, skipped: false, warnings: [] });
    const rows = readFileSync(join(root, 'l2-knowledge/.recall_log.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    expect(rows.map(row => row.event_type)).toEqual(['index_query', 'entry_read', 'entry_used']);
    expect(rows.every(row => row.query_hash === queryHash && row.bot_app_id === 'bot_1' && !('query_text' in row))).toBe(true);
  });

  it('maps L2 entries from ids and nested sourceRefs without following unrelated paths', () => {
    const root = fixture();
    expect(findWorkspaceL2EntryIds({
      workingDir: root,
      itemIds: ['l2k_one', 'mem_other'],
      sourceRefs: [{ sourceRef: { path: join(root, 'l2-knowledge/items/one.md') } }, { path: '/etc/passwd' }],
    })).toEqual(['l2k_one']);
  });

  it('records query feedback without entry or query text', () => {
    const root = fixture(); const queryHash = `sha256:${'d'.repeat(64)}`;
    expect(recordAutomaticWorkspaceRetrievalEvidence({
      workingDir: root,
      queryHash,
      source: 'feedback_card',
      feedback: 'not_helpful',
      observedAt: '2026-09-01T00:00:00Z',
    })).toMatchObject({ recorded: 1, skipped: false, warnings: [] });
    const rows = readFileSync(join(root, 'l2-knowledge/.recall_log.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    expect(rows).toEqual([expect.objectContaining({ event_type: 'query_feedback', query_hash: queryHash, feedback: 'not_helpful' })]);
    expect(JSON.stringify(rows)).not.toContain('not helpful');
  });

  it('derives transcript evidence only from observable recall/read/fallback boundaries', () => {
    const root = fixture(); const queryHash = `sha256:${'e'.repeat(64)}`;
    const transcriptText = [
      '$ python3 recall.py --working-dir . --list-index',
      '# L2 Index (1 entries, L2_TODAY=2026-09-01)',
      '$ sed -n 1,80p l2-knowledge/items/one.md',
      '# If no entry is relevant, fallback to knowledge-retrieval.',
      'assistant answer later mentions l2k_one but that is not use evidence',
    ].join('\n');
    expect(recordTranscriptWorkspaceRetrievalEvidence({
      workingDir: root,
      queryHash,
      transcriptText,
      botAppId: 'bot',
      sessionId: 'sess',
      turnId: 'turn',
      observedAt: '2026-09-01T00:00:00Z',
    })).toMatchObject({ recorded: 3, skipped: false, warnings: [] });
    const rows = readFileSync(join(root, 'l2-knowledge/.recall_log.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    expect(rows.map(row => row.event_type)).toEqual(['index_query', 'entry_read', 'fallback']);
    expect(rows.some(row => row.event_type === 'entry_used')).toBe(false);
    expect(JSON.stringify(rows)).not.toContain('assistant answer later');
  });
});
