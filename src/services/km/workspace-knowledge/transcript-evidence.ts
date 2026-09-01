import { createHash } from 'node:crypto';
import {
  findWorkspaceL2EntryIds,
  listWorkspaceL2IndexEntries,
  recordWorkspaceRetrievalEvidence,
  type RetrievalEvidenceCorrelation,
} from './retrieval-evidence.js';

export interface TranscriptRetrievalEvidenceInput extends RetrievalEvidenceCorrelation {
  workingDir?: string;
  queryText?: string;
  queryHash?: string;
  transcriptText: string;
  userId?: string;
  observedAt?: string;
}

export interface TranscriptRetrievalEvidenceResult {
  recorded: number;
  skipped: boolean;
  warnings: string[];
}

const L2_PATH_RE = /(?:^|[\s"'`(])(?:\.\/)?l2-knowledge\/([A-Za-z0-9._/-]+\.md)\b/gu;

export function recordTranscriptWorkspaceRetrievalEvidence(input: TranscriptRetrievalEvidenceInput): TranscriptRetrievalEvidenceResult {
  if (!input.workingDir) return { recorded: 0, skipped: true, warnings: ['workspace_evidence_cwd_missing'] };
  const queryHash = input.queryHash ?? (input.queryText ? retrievalEvidenceQueryHash({
    text: input.queryText,
    botAppId: input.botAppId,
    userId: input.userId,
  }) : undefined);
  if (!queryHash) return { recorded: 0, skipped: true, warnings: ['workspace_evidence_query_hash_missing'] };
  const common = {
    workingDir: input.workingDir,
    queryHash,
    ...(input.observedAt ? { observedAt: input.observedAt } : {}),
    ...(input.botAppId ? { botAppId: input.botAppId } : {}),
    ...(input.sessionId ? { sessionId: input.sessionId } : {}),
    ...(input.turnId ? { turnId: input.turnId } : {}),
    ...(input.retrievalRunId ? { retrievalRunId: input.retrievalRunId } : {}),
    source: input.source ?? 'transcript_observation' as const,
  };
  const warnings: string[] = [];
  let entries: ReturnType<typeof listWorkspaceL2IndexEntries>;
  try {
    entries = listWorkspaceL2IndexEntries(input.workingDir);
  } catch (error) {
    return { recorded: 0, skipped: true, warnings: [`workspace_evidence_unavailable:${safeWarning(error)}`] };
  }
  let recorded = 0;
  const record = (event: Parameters<typeof recordWorkspaceRetrievalEvidence>[0]): void => {
    try {
      if (recordWorkspaceRetrievalEvidence(event).appended) recorded += 1;
    } catch (error) {
      warnings.push(`workspace_evidence_${event.eventType}_failed:${safeWarning(error)}`);
    }
  };
  const observedIndex = /\brecall\.py\b[\s\S]{0,200}\b--list-index\b/u.test(input.transcriptText)
    || /^# L2 Index \(/mu.test(input.transcriptText);
  if (observedIndex) record({ ...common, eventType: 'index_query', entryCount: entries.length });
  const entryIds = new Set<string>();
  for (const match of input.transcriptText.matchAll(L2_PATH_RE)) {
    const rel = match[1];
    if (!rel) continue;
    for (const entryId of findWorkspaceL2EntryIds({ workingDir: input.workingDir, sourceRefs: [{ relativePath: `l2-knowledge/${rel}` }] })) {
      entryIds.add(entryId);
    }
  }
  for (const entryId of entryIds) record({ ...common, eventType: 'entry_read', entryId });
  if (/fallback:\s*load_skills=\['knowledge-retrieval'\]|If no entry is relevant,\s*fallback to knowledge-retrieval/iu.test(input.transcriptText)) {
    record({ ...common, eventType: 'fallback', outcome: entryIds.size > 0 ? 'success' : 'no_hit' });
  }
  return { recorded, skipped: false, warnings };
}

function retrievalEvidenceQueryHash(input: { text: string; botAppId?: string; userId?: string }): string {
  return `sha256:${createHash('sha256').update(JSON.stringify({
    text: input.text,
    ...(input.botAppId ? { botAppId: input.botAppId } : {}),
    ...(input.userId ? { userId: input.userId } : {}),
  })).digest('hex')}`;
}

function safeWarning(error: unknown): string {
  return String(error instanceof Error ? error.message : error).replace(/[^a-zA-Z0-9._:-]+/gu, '_').slice(0, 120);
}
