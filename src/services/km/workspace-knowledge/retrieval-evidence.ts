import { createHash } from 'node:crypto';
import { appendFileSync, existsSync, readFileSync, realpathSync, statSync } from 'node:fs';
import { join, relative, resolve, sep } from 'node:path';
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

export interface RetrievalEvidenceCorrelation {
  botAppId?: string;
  sessionId?: string;
  turnId?: string;
  retrievalRunId?: string;
  source?: 'runtime_retrieval' | 'prompt_memory' | 'feedback_card' | 'cli' | 'transcript_observation';
}

export interface WorkspaceL2IndexEntry {
  id: string;
  relativePath?: string;
}

export interface RetrievalCandidateForEvidence {
  id: string;
  kind?: string;
  sourceRefs?: unknown[];
}

export interface AutoWorkspaceRetrievalEvidenceInput extends RetrievalEvidenceCorrelation {
  workingDir?: string;
  queryHash: string;
  candidates?: RetrievalCandidateForEvidence[];
  selectedItemIds?: string[];
  fallbackOutcome?: 'success' | 'no_hit' | 'error';
  feedback?: 'helpful' | 'not_helpful';
  useLabel?: RetrievalUseLabel;
  observedAt?: string;
}

export interface RecordRetrievalEvidenceResult {
  workspaceId: string;
  eventType: RetrievalEvidenceInput['eventType'];
  eventId: string;
  appended: boolean;
}

type Json = Record<string, any>;

export function recordWorkspaceRetrievalEvidence(
  input: RetrievalEvidenceInput & { workingDir: string; observedAt?: string } & RetrievalEvidenceCorrelation,
): RecordRetrievalEvidenceResult {
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
  for (const [key, value] of Object.entries({
    bot_app_id: input.botAppId,
    session_id: input.sessionId,
    turn_id: input.turnId,
    retrieval_run_id: input.retrievalRunId,
    source: input.source,
  })) {
    if (value !== undefined) event[key] = safeIdentifier(value, key);
  }
  const eventId = evidenceEventId(event);
  event.event_id = eventId;
  const log = join(l2Root, '.recall_log.jsonl');
  let appended = false;
  withFileLockSync(log, () => {
    if (evidenceEventAlreadyPresent(log, eventId)) return;
    appendFileSync(log, `${JSON.stringify(event)}\n`, { encoding: 'utf8', mode: 0o600 });
    appended = true;
  });
  return { workspaceId: `${resolve(root).split(sep).pop() || 'workspace'}`, eventType: input.eventType, eventId, appended };
}

export function listWorkspaceL2IndexEntries(workingDir: string): WorkspaceL2IndexEntry[] {
  const roots = discoverWorkspaceKnowledgeRoots([workingDir]);
  if (roots.length !== 1) throw new Error('km_retrieval_evidence_workspace_not_found');
  const root = roots[0];
  const indexPath = join(root, 'l2-knowledge', 'INDEX.json');
  const l2Root = realpathSync(join(root, 'l2-knowledge'));
  if (l2Root !== root && !l2Root.startsWith(`${root}${sep}`)) throw new Error('km_retrieval_evidence_root_escape');
  if (!existsSync(indexPath)) throw new Error('km_retrieval_evidence_index_missing');
  const raw = JSON.parse(readFileSync(indexPath, 'utf8')) as Json;
  if (!Array.isArray(raw.entries)) throw new Error('km_retrieval_evidence_index_entries_missing');
  return raw.entries
    .filter(entry => entry && typeof entry === 'object' && !['rejected', 'deprecated'].includes(String(entry.status ?? '')))
    .map(entry => {
      const id = typeof entry.id === 'string' ? entry.id.trim() : '';
      const rel = typeof entry.file === 'string' ? entry.file.trim() : typeof entry.path === 'string' ? entry.path.trim() : '';
      if (!isSafeIdentifier(id)) return undefined;
      return { id, ...(rel ? { relativePath: normalizeRelativePath(rel) } : {}) };
    })
    .filter((entry): entry is WorkspaceL2IndexEntry => Boolean(entry));
}

export function findWorkspaceL2EntryIds(input: { workingDir: string; itemIds?: string[]; sourceRefs?: unknown[] }): string[] {
  return findWorkspaceL2EntryIdsFromEntries(input.workingDir, listWorkspaceL2IndexEntries(input.workingDir), input);
}

function findWorkspaceL2EntryIdsFromEntries(
  workingDir: string,
  entries: WorkspaceL2IndexEntry[],
  input: { itemIds?: string[]; sourceRefs?: unknown[] },
): string[] {
  const ids = new Set<string>();
  const byPath = new Map<string, string>();
  for (const entry of entries) {
    ids.add(entry.id);
    if (entry.relativePath) {
      byPath.set(entry.relativePath, entry.id);
      byPath.set(`l2-knowledge/${entry.relativePath}`, entry.id);
    }
  }
  const matched = new Set<string>();
  for (const id of input.itemIds ?? []) {
    if (ids.has(id)) matched.add(id);
  }
  const root = realpathSync(discoverWorkspaceKnowledgeRoots([workingDir])[0]);
  const l2Root = realpathSync(join(root, 'l2-knowledge'));
  const visit = (value: unknown): void => {
    if (!value || typeof value !== 'object') return;
    const record = value as Json;
    const rawId = typeof record.entryId === 'string' ? record.entryId
      : typeof record.entry_id === 'string' ? record.entry_id
        : typeof record.id === 'string' ? record.id
          : undefined;
    if (rawId && ids.has(rawId)) matched.add(rawId);
    for (const key of ['relativePath', 'relative_path', 'file', 'path']) {
      const rawPath = typeof record[key] === 'string' ? record[key] : undefined;
      const entryId = rawPath ? entryIdForPath(rawPath, root, l2Root, byPath) : undefined;
      if (entryId) matched.add(entryId);
    }
    if (record.sourceRef) visit(record.sourceRef);
    if (Array.isArray(record.sourceRefs)) for (const nested of record.sourceRefs) visit(nested);
  };
  for (const ref of input.sourceRefs ?? []) visit(ref);
  return [...matched].sort();
}

export function recordAutomaticWorkspaceRetrievalEvidence(input: AutoWorkspaceRetrievalEvidenceInput): {
  recorded: number;
  skipped: boolean;
  warnings: string[];
} {
  if (!input.workingDir) return { recorded: 0, skipped: true, warnings: ['workspace_evidence_cwd_missing'] };
  const warnings: string[] = [];
  let entries: WorkspaceL2IndexEntry[];
  try {
    entries = listWorkspaceL2IndexEntries(input.workingDir);
  } catch (error) {
    return { recorded: 0, skipped: true, warnings: [`workspace_evidence_unavailable:${safeWarning(error)}`] };
  }
  const common = {
    workingDir: input.workingDir,
    queryHash: input.queryHash,
    ...(input.observedAt ? { observedAt: input.observedAt } : {}),
    ...(input.botAppId ? { botAppId: input.botAppId } : {}),
    ...(input.sessionId ? { sessionId: input.sessionId } : {}),
    ...(input.turnId ? { turnId: input.turnId } : {}),
    ...(input.retrievalRunId ? { retrievalRunId: input.retrievalRunId } : {}),
    ...(input.source ? { source: input.source } : {}),
  };
  let recorded = 0;
  const record = (event: RetrievalEvidenceInput): void => {
    try {
      if (recordWorkspaceRetrievalEvidence({ ...common, ...event }).appended) recorded += 1;
    } catch (error) {
      warnings.push(`workspace_evidence_${event.eventType}_failed:${safeWarning(error)}`);
    }
  };
  const feedbackOnly = input.feedback && !(input.candidates?.length) && !input.fallbackOutcome && !(input.selectedItemIds?.length);
  if (!feedbackOnly) record({ eventType: 'index_query', queryHash: input.queryHash, entryCount: entries.length });
  const readIds = new Set<string>();
  for (const candidate of input.candidates ?? []) {
    for (const entryId of findWorkspaceL2EntryIdsFromEntries(input.workingDir, entries, { itemIds: [candidate.id], sourceRefs: candidate.sourceRefs })) {
      readIds.add(entryId);
    }
  }
  for (const entryId of readIds) record({ eventType: 'entry_read', queryHash: input.queryHash, entryId });
  const selected = new Set(input.selectedItemIds ?? []);
  const selectedL2Ids = new Set<string>();
  for (const candidate of input.candidates ?? []) {
    if (!selected.has(candidate.id)) continue;
    for (const entryId of findWorkspaceL2EntryIdsFromEntries(input.workingDir, entries, { itemIds: [candidate.id], sourceRefs: candidate.sourceRefs })) {
      selectedL2Ids.add(entryId);
    }
  }
  for (const entryId of selectedL2Ids) record({ eventType: 'entry_used', queryHash: input.queryHash, entryId, useLabel: input.useLabel ?? 'context_guided' });
  if (input.fallbackOutcome) record({ eventType: 'fallback', queryHash: input.queryHash, outcome: input.fallbackOutcome });
  if (input.feedback) record({ eventType: 'query_feedback', queryHash: input.queryHash, feedback: input.feedback });
  return { recorded, skipped: false, warnings };
}

function safeIdentifier(value: string, name: string): string {
  const normalized = value.trim();
  if (!isSafeIdentifier(normalized)) throw new Error(`km_retrieval_evidence_${name}_invalid`);
  return normalized;
}
function isSafeIdentifier(value: string): boolean {
  return Boolean(value) && value.length <= 200 && /^[a-zA-Z0-9._:_-]+$/.test(value);
}
function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`km_retrieval_evidence_${name}_invalid`);
  return value;
}

function safeWarning(error: unknown): string {
  return String(error instanceof Error ? error.message : error).replace(/[^a-zA-Z0-9._:-]+/gu, '_').slice(0, 120);
}

function evidenceEventId(event: Record<string, unknown>): string {
  const stable = Object.fromEntries(Object.entries(event)
    .filter(([key]) => key !== 'observed_at' && key !== 'event_id')
    .sort(([a], [b]) => a.localeCompare(b)));
  return `kre_${createHash('sha256').update(JSON.stringify(stable)).digest('hex')}`;
}

function evidenceEventAlreadyPresent(log: string, eventId: string): boolean {
  if (!existsSync(log)) return false;
  try {
    if (statSync(log).size > 8 * 1024 * 1024) return false;
    return readFileSync(log, 'utf8').split(/\r?\n/u).some(line => {
      try { return JSON.parse(line).event_id === eventId; } catch { return false; }
    });
  } catch {
    return false;
  }
}

function normalizeRelativePath(value: string): string {
  return value.replaceAll('\\', '/').replace(/^\/+/u, '').replace(/^l2-knowledge\//u, '');
}

function entryIdForPath(rawPath: string, root: string, l2Root: string, byPath: Map<string, string>): string | undefined {
  const normalized = normalizeRelativePath(rawPath);
  if (byPath.has(normalized)) return byPath.get(normalized);
  if (byPath.has(`l2-knowledge/${normalized}`)) return byPath.get(`l2-knowledge/${normalized}`);
  try {
    const resolved = realpathSync(resolve(root, rawPath));
    if (resolved === l2Root || !resolved.startsWith(`${l2Root}${sep}`)) return undefined;
    const rel = relative(l2Root, resolved).split(sep).join('/');
    return byPath.get(rel);
  } catch {
    return undefined;
  }
}
