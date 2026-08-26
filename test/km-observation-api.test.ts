import { describe, expect, it, vi } from 'vitest';
import { handleKmObservationApi } from '../src/dashboard/km-observation-api.js';

function response() {
  const bodies: unknown[] = [];
  const res = {
    writeHead: vi.fn(),
    end: vi.fn(value => bodies.push(JSON.parse(String(value)))),
  } as any;
  return { res, bodies };
}

describe('KM observation dashboard API', () => {
  it('does not expose routes while the feature is disabled', async () => {
    const { res, bodies } = response();
    const handled = await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/health'),
      { enabled: false, openStore: vi.fn() },
    );
    expect(handled).toBe(true);
    expect(res.writeHead).toHaveBeenCalledWith(404, expect.anything());
    expect(bodies).toEqual([{ error: 'km_observation_disabled' }]);
  });

  it('returns store health and closes the request-scoped store', async () => {
    const close = vi.fn();
    const { res, bodies } = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/health'),
      {
        enabled: true,
        openStore: async () => ({
          schemaVersion: () => 1,
          pragmas: () => ({ journalMode: 'wal', foreignKeys: 1, busyTimeout: 5000 }),
          counts: () => ({ observations: 3, quarantined: 1 }),
          list: vi.fn(), get: vi.fn(), close,
        }),
      },
    );
    expect(bodies).toEqual([{
      enabled: true,
      schemaVersion: 1,
      pragmas: { journalMode: 'wal', foreignKeys: 1, busyTimeout: 5000 },
      counts: { observations: 3, quarantined: 1 },
    }]);
    expect(close).toHaveBeenCalledOnce();
  });

  it('bounds list pagination and forwards typed filters', async () => {
    const list = vi.fn(() => [{ eventId: 'evt-1' }]);
    const { res } = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/observations?limit=999&before=42&type=turn.completed'),
      {
        enabled: true,
        openStore: async () => ({
          schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(),
          list, get: vi.fn(), close: vi.fn(),
        }),
      },
    );
    expect(list).toHaveBeenCalledWith({ limit: 100, beforeLocalSeq: 42, eventType: 'turn.completed' });
  });

  it('returns one event and rejects unsupported methods', async () => {
    const get = vi.fn(() => ({ eventId: 'evt-1', payload: { status: 'completed' } }));
    const first = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      first.res,
      new URL('http://localhost/api/km/observations/evt-1'),
      {
        enabled: true,
        openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get, close: vi.fn() }),
      },
    );
    expect(first.bodies).toEqual([{ eventId: 'evt-1', payload: { status: 'completed' } }]);

    const second = response();
    await handleKmObservationApi(
      { method: 'POST' } as any,
      second.res,
      new URL('http://localhost/api/km/observations'),
      { enabled: true, openStore: vi.fn() },
    );
    expect(second.bodies).toEqual([{ error: 'method_not_allowed' }]);
  });
});
