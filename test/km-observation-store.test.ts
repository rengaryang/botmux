import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  ObservationEventSchema,
  type ObservationEvent,
} from '../src/services/km/observation-schema.js';
import { ObservationStore } from '../src/services/km/observation-store.js';

const dirs: string[] = [];

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-'));
  dirs.push(dir);
  return dir;
}

function event(overrides: Partial<ObservationEvent> = {}): ObservationEvent {
  return {
    schemaVersion: 1,
    eventId: 'evt-turn-1',
    eventType: 'turn.completed',
    source: {
      producer: 'turn-terminal',
      adapter: 'traex',
      resolverStatus: 'resolved',
      confidence: 'observed',
    },
    identity: {
      botAppId: 'cli_test',
      sessionId: 'session-1',
      turnId: 'turn-1',
    },
    ordering: {
      sourceKey: 'turn-terminal:cli_test',
      idempotencyKey: 'cli_test|session-1|turn-1|0',
      sourceSeq: 1,
      parentEventIds: [],
      observedAt: '2026-08-26T00:00:00.000Z',
    },
    provenance: {
      evidenceLevel: 'runtime',
      parserVersion: 'turn-terminal/v1',
      sourceRefs: [{ kind: 'sqlite-row', ref: 'turn_terminals/turn-1' }],
      privacyClass: 'internal',
      redactionStatus: 'not_needed',
    },
    content: {
      hash: null,
      storageMode: 'none',
    },
    payload: { status: 'completed' },
    createdAt: '2026-08-26T00:00:01.000Z',
    ...overrides,
  };
}

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe('ObservationEventSchema', () => {
  it('accepts an observed event with source evidence', () => {
    expect(ObservationEventSchema.parse(event()).eventType).toBe('turn.completed');
  });

  it('requires inferenceReason for inferred observations', () => {
    const input = event({
      source: {
        producer: 'transcript-parser',
        adapter: 'traex',
        resolverStatus: 'partial',
        confidence: 'inferred',
      },
    });
    expect(() => ObservationEventSchema.parse(input)).toThrow(/inferenceReason/);
  });

  it('rejects inline secret content', () => {
    const input = event({
      provenance: {
        evidenceLevel: 'runtime',
        parserVersion: 'turn-terminal/v1',
        sourceRefs: [{ kind: 'api', ref: 'secret-manager/ref-1' }],
        privacyClass: 'secret-reference-only',
        redactionStatus: 'not_needed',
      },
      content: {
        hash: null,
        storageMode: 'inline_preview_only',
        inlinePreview: 'secret value',
      },
    });
    expect(() => ObservationEventSchema.parse(input)).toThrow(/secret-reference-only/);
  });
});

describe('ObservationStore', () => {
  it('uses an isolated botmux-km.sqlite with hardened pragmas', async () => {
    const store = await ObservationStore.open(tempDir());
    expect(store.path.endsWith('botmux-km.sqlite')).toBe(true);
    expect(store.schemaVersion()).toBe(18);
    expect(store.pragmas()).toEqual(expect.objectContaining({
      journalMode: 'wal',
      foreignKeys: 1,
    }));
    store.close();
  });

  it('appends observations with monotonic local sequence and parent edges', async () => {
    const store = await ObservationStore.open(tempDir());
    const first = store.append(event());
    const second = store.append(event({
      eventId: 'evt-feedback-1',
      eventType: 'feedback.revised',
      ordering: {
        ...event().ordering,
        idempotencyKey: 'feedback-1|revision-1',
        sourceSeq: 2,
        parentEventIds: ['evt-turn-1'],
      },
      payload: { semantic: 'positive', revision: 1 },
    }));

    expect(first).toEqual(expect.objectContaining({ status: 'accepted', localSeq: 1 }));
    expect(second).toEqual(expect.objectContaining({ status: 'accepted', localSeq: 2 }));
    expect(store.listParents('evt-feedback-1')).toEqual(['evt-turn-1']);
    expect(store.list({ limit: 10 }).map(item => item.eventId)).toEqual([
      'evt-feedback-1',
      'evt-turn-1',
    ]);
    store.close();
  });

  it('deduplicates a replay with the same source key, idempotency key and payload', async () => {
    const store = await ObservationStore.open(tempDir());
    const first = store.append(event());
    const replay = store.append(event({ eventId: 'evt-turn-replayed' }));

    expect(first.status).toBe('accepted');
    expect(replay).toEqual(expect.objectContaining({
      status: 'deduped',
      eventId: 'evt-turn-1',
      localSeq: 1,
    }));
    expect(store.counts()).toEqual({ observations: 1, quarantined: 0, knowledge: 0, memory: 0 });
    store.close();
  });

  it('quarantines an idempotency collision instead of overwriting evidence', async () => {
    const store = await ObservationStore.open(tempDir());
    store.append(event());
    const collision = store.append(event({
      eventId: 'evt-turn-conflict',
      payload: { status: 'failed' },
    }));

    expect(collision).toEqual(expect.objectContaining({ status: 'quarantined' }));
    expect(store.counts()).toEqual({ observations: 1, quarantined: 1, knowledge: 0, memory: 0 });
    expect(store.listQuarantined(10)[0]).toEqual(expect.objectContaining({
      eventId: 'evt-turn-conflict',
      reason: 'idempotency_collision',
    }));
    store.close();
  });

  it('persists accepted observations across reopen', async () => {
    const dir = tempDir();
    const first = await ObservationStore.open(dir);
    first.append(event());
    first.close();

    const reopened = await ObservationStore.open(dir);
    expect(reopened.get('evt-turn-1')).toEqual(expect.objectContaining({
      eventId: 'evt-turn-1',
      payload: { status: 'completed' },
    }));
    reopened.close();
  });
});
