import { appendFileSync, existsSync, realpathSync } from 'node:fs';
import { join, resolve, sep } from 'node:path';
import { withFileLockSync } from '../../../utils/file-lock.js';
import { discoverWorkspaceKnowledgeRoots } from './scanner.js';

export const RETRIEVAL_USE_LABELS = ['direct_apply', 'context_guided', 'pitfall_avoided', 'not_used', 'misleading'] as const;
export type RetrievalUseLabel = typeof RETRIEVAL_USE_LABELS[number];
export type RetrievalEvidenceInput =
  | { eventType: 'index_query'; queryHash: string; entryCount?: number }
  | { eventType: 'entry_read'; queryHash: string; entryId: string }
  | { eventType: 'entry_used'; queryHash: string; entryId: string; useLabel: RetrievalUseLabel }
  | { eventType: 'fallback'; queryHash: string; outcome: 'success' | 'no_hit' | 'error' }
  | { eventType: 'query_feedback'; queryHash: string; feedback: 'helpful' | 'not_helpful' };

export function recordWorkspaceRetrievalEvidence(input: RetrievalEvidenceInput & { workingDir: string; observedAt?: string }): { workspaceId: string; eventType: RetrievalEvidenceInput['eventType'] } {
  if (!/^sha256:[a-f0-9]{64}$/i.test(input.queryHash)) throw new Error('km_retrieval_evidence_query_hash_invalid');
  const roots = discoverWorkspaceKnowledgeRoots([input.workingDir]);
  if (roots.length !== 1) throw new Error('km_retrieval_evidence_workspace_not_found');
  const root = roots[0];
  const l2Root = realpathSync(join(root, 'l2-knowledge'));
  if (l2Root !== root && !l2Root.startsWith(`${root}${sep}`)) throw new Error('km_retrieval_evidence_root_escape');
  if (!existsSync(join(l2Root, 'INDEX.json'))) throw new Error('km_retrieval_evidence_index_missing');
  const observedAt = input.observedAt ?? new Date().toISOString();
  if (!Number.isFinite(Date.parse(observedAt))) throw new Error('km_retrieval_evidence_timestamp_invalid');
  const event: Record<string, unknown> = { event_type: input.eventType, query_hash: input.queryHash.toLowerCase(), observed_at: observedAt };
  if (input.eventType === 'index_query' && input.entryCount !== undefined) event.entry_count = nonNegativeInteger(input.entryCount, 'entry_count');
  if (input.eventType === 'entry_read' || input.eventType === 'entry_used') event.entry_id = safeIdentifier(input.entryId, 'entry_id');
  if (input.eventType === 'entry_used') event.use_label = input.useLabel;
  if (input.eventType === 'fallback') event.outcome = input.outcome;
  if (input.eventType === 'query_feedback') event.feedback = input.feedback;
  const log = join(l2Root, '.recall_log.jsonl');
  withFileLockSync(log, () => appendFileSync(log, `${JSON.stringify(event)}\n`, { encoding: 'utf8', mode: 0o600 }));
  return { workspaceId: `${resolve(root).split(sep).pop() || 'workspace'}`, eventType: input.eventType };
}

function safeIdentifier(value: string, name: string): string {
  const normalized = value.trim();
  if (!normalized || normalized.length > 200 || !/^[a-zA-Z0-9._:-]+$/.test(normalized)) throw new Error(`km_retrieval_evidence_${name}_invalid`);
  return normalized;
}
function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`km_retrieval_evidence_${name}_invalid`);
  return value;
}
