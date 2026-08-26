import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import type { ObservationEvent } from '../src/services/km/observation-schema.js';
import {
  __testOnly_closeObservationStores,
  __testOnly_pendingObservationCount,
  __testOnly_reopenObservationAdmission,
  drainObservationQueue,
  enqueueObservation,
  isKmObservationEnabled,
} from '../src/services/km/observation-queue.js';
import { ObservationStore } from '../src/services/km/observation-store.js';

const dirs: string[] = [];

function freshDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-queue-'));
  dirs.push(dir);
  return dir;
}

function event(id = 'evt-1'): ObservationEvent {
  return {
    schemaVersion: 1,
    eventId: id,
    eventType: 'turn.completed',
    source: {
      producer: 'test', adapter: 'traex', resolverStatus: 'resolved', confidence: 'observed',
    },
    identity: { botAppId: 'app', sessionId: 'session', turnId: 'turn' },
    ordering: {
      sourceKey: 'test:app', idempotencyKey: 'session|turn|0', parentEventIds: [], observedAt: '2026-08-26T00:00:00.000Z',
    },
    provenance: {
      evidenceLevel: 'runtime', parserVersion: 'test/v1',
      sourceRefs: [{ kind: 'api', ref: 'test/evt-1' }], privacyClass: 'internal', redactionStatus: 'not_needed',
    },
    content: { hash: null, storageMode: 'none' },
    payload: { status: 'completed' },
    createdAt: '2026-08-26T00:00:01.000Z',
  };
}

afterEach(async () => {
  await drainObservationQueue(3_000);
  await __testOnly_closeObservationStores();
  __testOnly_reopenObservationAdmission();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe('KM observation queue', () => {
  it('is disabled by default and accepts explicit true-like values', () => {
    expect(isKmObservationEnabled({})).toBe(false);
    expect(isKmObservationEnabled({ BOTMUX_KM_OBSERVATION_ENABLED: 'true' })).toBe(true);
    expect(isKmObservationEnabled({ BOTMUX_KM_OBSERVATION_ENABLED: '1' })).toBe(true);
    expect(isKmObservationEnabled({ BOTMUX_KM_OBSERVATION_ENABLED: 'false' })).toBe(false);
  });

  it('persists an observation asynchronously and deduplicates an in-flight replay', async () => {
    const dir = freshDir();
    const a = enqueueObservation({ dataDir: dir, event: event() });
    const b = enqueueObservation({ dataDir: dir, event: event('evt-replay') });
    expect(a).toBe(b);
    await a;
    expect(__testOnly_pendingObservationCount()).toBe(0);

    const store = await ObservationStore.open(dir);
    expect(store.counts()).toEqual({ observations: 1, quarantined: 0, knowledge: 0, memory: 0 });
    store.close();
  });

  it('retries a busy SQLite writer without blocking the event loop', async () => {
    const dir = freshDir();
    const store = await ObservationStore.open(dir);
    (store as any).db.exec('BEGIN IMMEDIATE;');

    let resolved = false;
    const queued = enqueueObservation({ dataDir: dir, event: event() }).then(() => { resolved = true; });
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(resolved).toBe(false);
    expect(__testOnly_pendingObservationCount()).toBe(1);

    (store as any).db.exec('COMMIT;');
    await queued;
    expect(resolved).toBe(true);
    store.close();
  });

  it('drains accepted work and refuses work submitted after admission closes', async () => {
    const dir = freshDir();
    void enqueueObservation({ dataDir: dir, event: event() });
    expect(await drainObservationQueue(3_000)).toBe(0);

    const errors: unknown[] = [];
    await enqueueObservation({
      dataDir: dir,
      event: event('evt-late'),
      onError: error => errors.push(error),
    });
    expect(String(errors[0])).toContain('km_observation_refused_shutdown');
  });
});
