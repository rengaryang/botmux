import { describe, expect, it } from 'vitest';
import { extractAttributedUserEvidence, extractExplicitPreferences } from '../src/services/km/preference-extractor.js';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';

const event: ObservationEvent = {
  schemaVersion: 1, eventId: 'evt-1', eventType: 'turn.completed',
  source: { producer: 'turn', adapter: 'pi', resolverStatus: 'resolved', confidence: 'observed' },
  identity: { botAppId: 'bot', sessionId: 's1', turnId: 't1' },
  ordering: { sourceKey: 'turn', idempotencyKey: 't1', parentEventIds: [], observedAt: '2026-08-27T00:00:00.000Z' },
  provenance: { evidenceLevel: 'runtime', parserVersion: 'v1', sourceRefs: [], privacyClass: 'internal', redactionStatus: 'not_needed' },
  content: { hash: null, storageMode: 'none' }, payload: {}, createdAt: '2026-08-27T00:00:01.000Z',
};

describe('deterministic preference extractor', () => {
  it('extracts narrow explicit preferences with complete spans', () => {
    expect(extractExplicitPreferences({ event, evidenceText: '好的，以后请用中文回复。', userId: 'u1', roleAttributed: true })).toEqual([
      expect.objectContaining({ claimKey: 'response.language', claimText: 'response.language=Chinese', subject: 'u1',
        explicitUserStatement: true, evidenceSpanComplete: true }),
    ]);
  });
  it('does not infer a preference without explicit language or a user identity', () => {
    expect(extractExplicitPreferences({ event, evidenceText: '这段中文写得不错', userId: 'u1', roleAttributed: true })).toEqual([]);
    expect(extractExplicitPreferences({ event, evidenceText: '以后请用中文回复', userId: 'u1' })).toEqual([]);
    expect(extractExplicitPreferences({ event, evidenceText: '以后请用中文回复', roleAttributed: true })).toEqual([]);
  });
  it('extracts only mechanically attributed user regions', () => {
    const source = '<user_message>以后请用中文回复</user_message>\n' + JSON.stringify({ type: 'message', message: { role: 'assistant', content: '以后请用英文回复' } });
    expect(extractAttributedUserEvidence(source)).toEqual([expect.objectContaining({ text: '以后请用中文回复' })]);
  });
});
