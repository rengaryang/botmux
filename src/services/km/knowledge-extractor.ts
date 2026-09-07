import { createHash } from 'node:crypto';
import type { KnowledgeCandidateInput, KnowledgeLayer, KmPrivacyClass } from './observation-store.js';
import type { ObservationEvent } from './observation-schema.js';

export interface KnowledgeExtractionRule {
  id: string;
  targetLayer: KnowledgeLayer;
  category: string;
  match: (event: ObservationEvent) => string | null;
}

const DEFAULT_RULES: KnowledgeExtractionRule[] = [
  {
    id: 'workflow-artifact-reference', targetLayer: 'reviewed-only', category: 'workflow-artifact',
    match: event => event.eventType === 'workflow.artifact.produced'
      ? String(event.payload.outputKey ?? event.payload.path ?? '').trim() || null : null,
  },
  {
    id: 'failed-skill-run', targetLayer: 'reviewed-only', category: 'skill-failure',
    match: event => event.eventType === 'skill.failed'
      ? `Skill ${event.identity.skillName ?? 'unknown'} failed: ${String(event.payload.error ?? event.payload.exitCode ?? 'unknown')}` : null,
  },
  {
    id: 'explicit-knowledge-payload', targetLayer: 'reviewed-only', category: 'explicit-candidate',
    match: event => typeof event.payload.knowledgeCandidate === 'string'
      ? event.payload.knowledgeCandidate.trim() || null : null,
  },
];

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

/** Deterministic, review-only extraction. It never promotes or exports. */
export function extractKnowledgeCandidates(
  event: ObservationEvent,
  rules: readonly KnowledgeExtractionRule[] = DEFAULT_RULES,
): KnowledgeCandidateInput[] {
  const sourceRef = { kind: 'api' as const, ref: `observation/${event.eventId}` };
  const privacyClass: KmPrivacyClass = event.provenance.privacyClass;
  return rules.flatMap(rule => {
    const claimText = rule.match(event);
    if (!claimText) return [];
    const claimKey = `extract:${rule.id}:${hash(claimText.toLowerCase()).slice(0, 24)}`;
    return [{
      targetLayer: rule.targetLayer,
      category: rule.category,
      title: claimText.slice(0, 120),
      claimKey,
      claimText,
      confidence: event.source.confidence,
      freshness: 'unknown' as const,
      privacyClass,
      sourceRefs: [sourceRef],
      evidenceEventId: event.eventId,
    }];
  });
}
