import { createRequire } from 'node:module';
import { join } from 'node:path';
import type { DatabaseSync as DatabaseSyncType } from 'node:sqlite';
import type { TurnCompletionEventPayload } from '../skill-feedback-store.js';
import { observationFromTurnCompletion } from './observation-producers.js';
import { ObservationEventSchema, type ObservationEvent } from './observation-schema.js';

const require = createRequire(import.meta.url);

export interface KmBackfillInput {
  dataDir: string;
  /** ISO datetime — only backfill events at or after this time. */
  since?: string;
  limit?: number;
}

export interface KmBackfillResult {
  scanned: number;
  eligible: number;
  skippedBeforeSince: number;
  readErrors: number;
  /** The createdAt of the last successfully read event, usable as a resume cursor. */
  lastCursor: string | null;
}

interface FeedbackRow {
  payload_json: string;
  created_at: string;
}

/**
 * Read-only backfill: converts historical turn.completed rows in the feedback
 * DB into KM ObservationEvents. Opens the feedback DB in readOnly mode so it
 * can never mutate it; callers pass the events to the normal enqueue path.
 */
export function backfillTurnCompletionObservations(
  input: KmBackfillInput,
): { result: KmBackfillResult; events: ObservationEvent[] } {
  const { DatabaseSync } = require('node:sqlite') as { DatabaseSync: typeof DatabaseSyncType };
  const feedbackPath = join(input.dataDir, 'botmux-feedback.sqlite');
  const limit = Math.max(1, Math.min(input.limit ?? 500, 5_000));

  let db: DatabaseSyncType;
  try {
    db = new DatabaseSync(feedbackPath, { readOnly: true });
  } catch {
    // No feedback DB yet (fresh install) — nothing to backfill.
    return {
      result: { scanned: 0, eligible: 0, skippedBeforeSince: 0, readErrors: 0, lastCursor: null },
      events: [],
    };
  }

  const result: KmBackfillResult = { scanned: 0, eligible: 0, skippedBeforeSince: 0, readErrors: 0, lastCursor: null };
  const events: ObservationEvent[] = [];

  try {
    let rows: FeedbackRow[];
    if (input.since) {
      rows = db.prepare(
        'SELECT payload_json, created_at FROM turn_completion_events WHERE created_at >= ? ORDER BY created_at ASC LIMIT ?',
      ).all(input.since, limit) as unknown as FeedbackRow[];
    } else {
      rows = db.prepare(
        'SELECT payload_json, created_at FROM turn_completion_events ORDER BY created_at ASC LIMIT ?',
      ).all(limit) as unknown as FeedbackRow[];
    }

    for (const row of rows) {
      result.scanned += 1;
      result.lastCursor = row.created_at;
      if (input.since && row.created_at < input.since) {
        result.skippedBeforeSince += 1;
        continue;
      }
      try {
        const payload = JSON.parse(row.payload_json) as TurnCompletionEventPayload;
        // Reuse the same producer the live path uses — but mark backfill
        // provenance so downstream consumers can distinguish historical
        // imports from realtime observations.
        const event = observationFromTurnCompletion(payload);
        const backfilled: ObservationEvent = ObservationEventSchema.parse({
          ...event,
          eventId: `bfill_${event.eventId}`,
          ordering: {
            ...event.ordering,
            sourceKey: 'backfill:turn-completion',
            idempotencyKey: `bfill|${event.ordering.idempotencyKey}`,
          },
          provenance: {
            ...event.provenance,
            parserVersion: 'backfill/turn-completion/v1',
          },
        });
        events.push(backfilled);
        result.eligible += 1;
      } catch {
        result.readErrors += 1;
      }
    }
  } finally {
    db.close();
  }

  return { result, events };
}
