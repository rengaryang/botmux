import type { IncomingMessage, ServerResponse } from 'node:http';
import { jsonRes } from './http.js';
import { ObservationEventTypeSchema } from '../services/km/observation-schema.js';
import type { ObservationStore } from '../services/km/observation-store.js';

export interface KmObservationApiStore {
  schemaVersion(): number;
  pragmas(): { journalMode: string; foreignKeys: number; busyTimeout: number };
  counts(): { observations: number; quarantined: number };
  list(filter: Parameters<ObservationStore['list']>[0]): ReturnType<ObservationStore['list']>;
  get(eventId: string): ReturnType<ObservationStore['get']>;
  close(): void;
}

export interface KmObservationApiDeps {
  enabled: boolean;
  openStore(): Promise<KmObservationApiStore>;
}

function positiveInteger(raw: string | null, fallback: number, max: number): number {
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) throw new Error('km_observation_invalid_integer');
  return Math.min(value, max);
}

/** Read-only KM observation API. Authentication is enforced by the Dashboard's
 * outer request gate before this handler is reached. */
export async function handleKmObservationApi(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: KmObservationApiDeps,
): Promise<boolean> {
  if (url.pathname !== '/api/km/health' && !url.pathname.startsWith('/api/km/observations')) return false;
  if (!deps.enabled) {
    jsonRes(res, 404, { error: 'km_observation_disabled' });
    return true;
  }
  if (req.method !== 'GET') {
    jsonRes(res, 405, { error: 'method_not_allowed' });
    return true;
  }

  let store: KmObservationApiStore | undefined;
  try {
    store = await deps.openStore();
    if (url.pathname === '/api/km/health') {
      jsonRes(res, 200, {
        enabled: true,
        schemaVersion: store.schemaVersion(),
        pragmas: store.pragmas(),
        counts: store.counts(),
      });
      return true;
    }

    if (url.pathname === '/api/km/observations') {
      const limit = positiveInteger(url.searchParams.get('limit'), 50, 100);
      const beforeRaw = url.searchParams.get('before');
      const beforeLocalSeq = beforeRaw ? positiveInteger(beforeRaw, 1, Number.MAX_SAFE_INTEGER) : undefined;
      const typeRaw = url.searchParams.get('type');
      const eventType = typeRaw ? ObservationEventTypeSchema.parse(typeRaw) : undefined;
      const filter = {
        limit,
        ...(beforeLocalSeq !== undefined ? { beforeLocalSeq } : {}),
        ...(eventType !== undefined ? { eventType } : {}),
      };
      jsonRes(res, 200, { items: store.list(filter) });
      return true;
    }

    const match = url.pathname.match(/^\/api\/km\/observations\/([^/]+)$/);
    if (match) {
      let eventId: string;
      try { eventId = decodeURIComponent(match[1]); }
      catch { jsonRes(res, 400, { error: 'invalid_event_id' }); return true; }
      const event = store.get(eventId);
      if (!event) jsonRes(res, 404, { error: 'observation_not_found' });
      else jsonRes(res, 200, event);
      return true;
    }

    jsonRes(res, 404, { error: 'not_found' });
  } catch (error) {
    jsonRes(res, 400, { error: error instanceof Error ? error.message : 'km_observation_invalid_request' });
  } finally {
    store?.close();
  }
  return true;
}
