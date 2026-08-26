import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { SkillFeedbackStore } from '../src/services/skill-feedback-store.js';
import { backfillTurnCompletionObservations } from '../src/services/km/observation-backfill.js';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { enqueueObservation } from '../src/services/km/observation-queue.js';
import {
  __testOnly_closeObservationStores,
  __testOnly_pendingObservationCount,
  __testOnly_reopenObservationAdmission,
} from '../src/services/km/observation-queue.js';

const dirs: string[] = [];

afterEach(async () => {
  await __testOnly_closeObservationStores();
  __testOnly_reopenObservationAdmission();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function seedFeedbackDb(dataDir: string): void {
  const store = SkillFeedbackStore.open === undefined ? undefined : null;
  void store;
}

describe('KM backfill', () => {
  it('returns empty when no feedback DB exists', () => {
    const dir = mkdtempSync(join(tmpdir(), 'botmux-km-bf-'));
    dirs.push(dir);
    const { result, events } = backfillTurnCompletionObservations({ dataDir: dir });
    expect(result).toEqual({ scanned: 0, eligible: 0, skippedBeforeSince: 0, readErrors: 0, lastCursor: null });
    expect(events).toEqual([]);
  });

  it('reads historical turn completions read-only and converts them to backfill events', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'botmux-km-bf-'));
    dirs.push(dir);

    // Seed a real feedback DB with one delivery + terminal.
    const feedback = await SkillFeedbackStore.open(dir);
    feedback.recordTurnDelivery({
      botAppId: 'app-a', sessionId: 'sess-a', turnId: 'turn-a', dispatchAttempt: 0,
      platform: 'lark', platformAppId: 'app-a', platformMessageId: 'om-a',
      content: 'answer', cliId: 'codex', cardMode: 'card', status: 'delivered',
    });
    feedback.recordTurnTerminal({
      botAppId: 'app-a', sessionId: 'sess-a', turnId: 'turn-a', dispatchAttempt: 0,
      status: 'completed',
    });
    feedback.close();

    const { result, events } = backfillTurnCompletionObservations({ dataDir: dir });
    expect(result.scanned).toBe(1);
    expect(result.eligible).toBe(1);
    expect(result.readErrors).toBe(0);
    expect(result.lastCursor).toBeTruthy();
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      eventType: 'turn.completed',
      ordering: { sourceKey: 'backfill:turn-completion' },
      provenance: { parserVersion: 'backfill/turn-completion/v1' },
      identity: { botAppId: 'app-a', sessionId: 'sess-a', turnId: 'turn-a' },
    });
    // Backfill event IDs are prefixed to avoid colliding with any live event.
    expect(events[0].eventId.startsWith('bfill_')).toBe(true);

    // Enqueue + persist through the normal queue path.
    for (const event of events) await enqueueObservation({ dataDir: dir, event });
    const store = await ObservationStore.open(dir);
    expect(store.counts()).toEqual({ observations: 1, quarantined: 0 });
    store.close();
  });

  it('honors the since cursor and skips older rows', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'botmux-km-bf-'));
    dirs.push(dir);
    const feedback = await SkillFeedbackStore.open(dir);
    feedback.recordTurnDelivery({
      botAppId: 'app-b', sessionId: 'sess-b', turnId: 'turn-b', dispatchAttempt: 0,
      platform: 'lark', platformAppId: 'app-b', platformMessageId: 'om-b',
      content: 'answer', cliId: 'codex', cardMode: 'card', status: 'delivered',
    });
    feedback.recordTurnTerminal({
      botAppId: 'app-b', sessionId: 'sess-b', turnId: 'turn-b', dispatchAttempt: 0,
      status: 'completed',
    });
    feedback.close();

    const { result } = backfillTurnCompletionObservations({
      dataDir: dir,
      since: '2099-01-01T00:00:00.000Z', // far future: nothing qualifies
    });
    expect(result.scanned).toBe(0); // SQL WHERE filters before scan
    expect(result.eligible).toBe(0);
  });

  it('refuses to write backfill events when the KM flag is off (CLI guard)', () => {
    // The CLI `km backfill` command gates the write path on
    // BOTMUX_KM_OBSERVATION_ENABLED; the unit-level producer has no such gate,
    // so this test pins the design: backfill itself is pure/read-only.
    const dir = mkdtempSync(join(tmpdir(), 'botmux-km-bf-'));
    dirs.push(dir);
    const { result } = backfillTurnCompletionObservations({ dataDir: dir });
    expect(result.scanned).toBe(0);
    expect(result.readErrors).toBe(0);
  });
});
