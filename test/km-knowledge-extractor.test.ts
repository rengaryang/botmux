import { describe, expect, it } from 'vitest';
import { extractKnowledgeCandidates } from '../src/services/km/knowledge-extractor.js';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';

function event(overrides: Partial<ObservationEvent> = {}): ObservationEvent {
  return {
    schemaVersion: 1, eventId: 'evt-1', eventType: 'workflow.artifact.produced',
    source: { producer: 'workflow', adapter: 'workflow', resolverStatus: 'resolved', confidence: 'observed' },
    identity: { botAppId: 'bot', sessionId: 'session', workflowId: 'wf', nodeId: 'node' },
    ordering: { sourceKey: 'wf', idempotencyKey: 'wf-1', parentEventIds: [], observedAt: '2026-08-26T00:00:00.000Z' },
    provenance: { evidenceLevel: 'workflow-artifact', parserVersion: 'v1', sourceRefs: [{ kind: 'workflow-artifact', ref: 'wf/node' }], privacyClass: 'internal', redactionStatus: 'not_needed' },
    content: { hash: null, storageMode: 'none' }, payload: { outputKey: 'report' }, createdAt: '2026-08-26T00:00:01.000Z',
    ...overrides,
  };
}

describe('knowledge candidate extractor', () => {
  it('creates deterministic reviewed-only candidates with provenance', () => {
    const first = extractKnowledgeCandidates(event());
    const second = extractKnowledgeCandidates(event());
    expect(first).toHaveLength(1);
    expect(first).toEqual(second);
    expect(first[0]).toEqual(expect.objectContaining({ targetLayer: 'reviewed-only', confidence: 'observed', evidenceEventId: 'evt-1' }));
    expect(first[0].sourceRefs).toEqual([{ kind: 'api', ref: 'observation/evt-1' }]);
  });

  it('does not invent candidates for unrelated observations', () => {
    expect(extractKnowledgeCandidates(event({ eventType: 'turn.completed', payload: { status: 'completed' } }))).toEqual([]);
  });
});
