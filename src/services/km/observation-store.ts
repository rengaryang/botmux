import { createHash, randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseSync as DatabaseSyncType } from 'node:sqlite';
import {
  ObservationEventSchema,
  type ObservationEvent,
  type ObservationEventType,
} from './observation-schema.js';

const SCHEMA_VERSION = 1;
const BUSY_TIMEOUT_MS = 5_000;

function isSqliteBusyError(error: unknown): boolean {
  const code = (error as { code?: unknown })?.code;
  if (code === 'SQLITE_BUSY' || code === 'SQLITE_LOCKED') return true;
  const message = String((error as { message?: unknown })?.message ?? error).toLowerCase();
  return message.includes('database is locked')
    || message.includes('database table is locked')
    || message.includes('sqlite_busy')
    || message.includes('sqlite_locked');
}

const FRESH_SCHEMA = `
  CREATE TABLE observation_events (
    event_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK(schema_version=1),
    event_type TEXT NOT NULL,
    source_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    local_seq INTEGER NOT NULL UNIQUE,
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    event_json TEXT NOT NULL,
    UNIQUE(source_key, idempotency_key)
  );
  CREATE INDEX observation_events_type_seq ON observation_events(event_type, local_seq DESC);
  CREATE INDEX observation_events_observed_seq ON observation_events(observed_at, local_seq DESC);

  CREATE TABLE observation_parents (
    event_id TEXT NOT NULL REFERENCES observation_events(event_id) ON DELETE CASCADE,
    parent_event_id TEXT NOT NULL,
    PRIMARY KEY(event_id, parent_event_id)
  );

  CREATE TABLE content_blobs (
    content_hash TEXT PRIMARY KEY,
    storage_mode TEXT NOT NULL,
    content_ref TEXT,
    bytes INTEGER,
    created_at TEXT NOT NULL
  );

  CREATE TABLE producer_checkpoints (
    producer_name TEXT NOT NULL,
    adapter TEXT NOT NULL,
    cursor TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(producer_name, adapter)
  );

  CREATE TABLE sync_outbox (
    outbox_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES observation_events(event_id) ON DELETE CASCADE,
    sink_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','inflight','delivered','failed','quarantined')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,
    claimed_at INTEGER,
    claim_token TEXT,
    last_error TEXT,
    delivered_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(event_id, sink_id)
  );
  CREATE INDEX sync_outbox_due ON sync_outbox(status, next_attempt_at);

  CREATE TABLE quarantine_events (
    quarantine_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    existing_event_id TEXT,
    existing_payload_hash TEXT,
    incoming_payload_hash TEXT NOT NULL,
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL
  );
  CREATE INDEX quarantine_events_created ON quarantine_events(created_at DESC, quarantine_id);

  CREATE TABLE local_sequence_counter (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
  );
  INSERT INTO local_sequence_counter(name, value) VALUES('observation_events', 0);
`;

export interface ObservationAppendAccepted {
  status: 'accepted';
  eventId: string;
  localSeq: number;
}

export interface ObservationAppendDeduped {
  status: 'deduped';
  eventId: string;
  localSeq: number;
}

export interface ObservationAppendQuarantined {
  status: 'quarantined';
  eventId: string;
  quarantineId: string;
}

export type ObservationAppendResult =
  | ObservationAppendAccepted
  | ObservationAppendDeduped
  | ObservationAppendQuarantined;

export interface ObservationListFilter {
  limit: number;
  beforeLocalSeq?: number;
  eventType?: ObservationEventType;
}

export interface QuarantinedObservation {
  quarantineId: string;
  eventId: string;
  reason: string;
  existingEventId?: string;
  createdAt: string;
}

interface ExistingIdentityRow {
  event_id: string;
  payload_hash: string;
  local_seq: number;
}

function sha256(value: string): string {
  return `sha256:${createHash('sha256').update(value).digest('hex')}`;
}

function payloadHash(event: ObservationEvent): string {
  return sha256(JSON.stringify(event.payload));
}

function quarantineId(): string {
  return `q_${randomUUID().replaceAll('-', '')}`;
}

export class ObservationStore {
  readonly path: string;
  private readonly db: DatabaseSyncType;

  private constructor(dataDir: string, db: DatabaseSyncType) {
    this.path = join(dataDir, 'botmux-km.sqlite');
    this.db = db;
    // Opening from a daemon telemetry queue must fail fast under cross-process
    // contention rather than blocking the Node event loop for busy_timeout.
    // Once initialization is complete, ordinary callers keep the conventional
    // 5s timeout; tryAppend borrows timeout=0 for the hot path.
    this.db.exec('PRAGMA busy_timeout=0;');
    this.db.exec('PRAGMA foreign_keys=ON;');
    const mode = String((this.db.prepare('PRAGMA journal_mode=WAL').get() as any)?.journal_mode ?? '').toLowerCase();
    if (mode !== 'wal') throw new Error(`km_observation_wal_mode_not_set:${mode || 'unknown'}`);

    const version = this.schemaVersion();
    if (version > SCHEMA_VERSION) throw new Error(`km_observation_schema_newer:${version}`);
    if (version === 0) this.createFreshSchema();
    this.validateSchema();
    this.db.exec(`PRAGMA busy_timeout=${BUSY_TIMEOUT_MS};`);
  }

  static async open(dataDir: string): Promise<ObservationStore> {
    const { DatabaseSync } = await import('node:sqlite');
    mkdirSync(dataDir, { recursive: true });
    const path = join(dataDir, 'botmux-km.sqlite');
    const db = new DatabaseSync(path);
    try {
      return new ObservationStore(dataDir, db);
    } catch (error) {
      db.close();
      throw error;
    }
  }

  close(): void {
    this.db.close();
  }

  schemaVersion(): number {
    return Number((this.db.prepare('PRAGMA user_version').get() as any)?.user_version ?? 0);
  }

  pragmas(): { journalMode: string; foreignKeys: number; busyTimeout: number } {
    return {
      journalMode: String((this.db.prepare('PRAGMA journal_mode').get() as any)?.journal_mode ?? '').toLowerCase(),
      foreignKeys: Number((this.db.prepare('PRAGMA foreign_keys').get() as any)?.foreign_keys ?? 0),
      busyTimeout: Number((this.db.prepare('PRAGMA busy_timeout').get() as any)?.timeout ?? 0),
    };
  }

  counts(): { observations: number; quarantined: number } {
    return {
      observations: Number((this.db.prepare('SELECT COUNT(*) AS count FROM observation_events').get() as any).count),
      quarantined: Number((this.db.prepare('SELECT COUNT(*) AS count FROM quarantine_events').get() as any).count),
    };
  }

  /** Nonblocking hot-path append. A competing process holding SQLite's write
   * lock returns {busy:true}; callers retry on a timer instead of blocking the
   * daemon event loop inside node:sqlite's synchronous busy timeout. */
  tryAppend(input: ObservationEvent):
    | { done: true; result: ObservationAppendResult }
    | { done: false; busy: true } {
    this.db.exec('PRAGMA busy_timeout=0;');
    try {
      return { done: true, result: this.append(input) };
    } catch (error) {
      if (isSqliteBusyError(error)) return { done: false, busy: true };
      throw error;
    } finally {
      this.db.exec(`PRAGMA busy_timeout=${BUSY_TIMEOUT_MS};`);
    }
  }

  append(input: ObservationEvent): ObservationAppendResult {
    const event = ObservationEventSchema.parse(input);
    const eventJson = JSON.stringify(event);
    const incomingHash = payloadHash(event);

    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const existing = this.db.prepare(`
        SELECT event_id, payload_hash, local_seq
        FROM observation_events
        WHERE source_key=? AND idempotency_key=?
      `).get(event.ordering.sourceKey, event.ordering.idempotencyKey) as ExistingIdentityRow | undefined;

      if (existing) {
        if (existing.payload_hash === incomingHash) {
          this.db.exec('COMMIT;');
          return { status: 'deduped', eventId: existing.event_id, localSeq: existing.local_seq };
        }
        const id = quarantineId();
        this.db.prepare(`
          INSERT INTO quarantine_events(
            quarantine_id,event_id,source_key,idempotency_key,reason,existing_event_id,
            existing_payload_hash,incoming_payload_hash,event_json,created_at
          ) VALUES(?,?,?,?,?,?,?,?,?,?)
        `).run(
          id,
          event.eventId,
          event.ordering.sourceKey,
          event.ordering.idempotencyKey,
          'idempotency_collision',
          existing.event_id,
          existing.payload_hash,
          incomingHash,
          eventJson,
          new Date().toISOString(),
        );
        this.db.exec('COMMIT;');
        return { status: 'quarantined', eventId: event.eventId, quarantineId: id };
      }

      const nextRow = this.db.prepare(`
        UPDATE local_sequence_counter
        SET value=value+1
        WHERE name='observation_events'
        RETURNING value
      `).get() as { value: number } | undefined;
      if (!nextRow) throw new Error('km_observation_sequence_missing');
      const localSeq = Number(nextRow.value);

      this.db.prepare(`
        INSERT INTO observation_events(
          event_id,schema_version,event_type,source_key,idempotency_key,payload_hash,
          local_seq,observed_at,created_at,event_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
      `).run(
        event.eventId,
        event.schemaVersion,
        event.eventType,
        event.ordering.sourceKey,
        event.ordering.idempotencyKey,
        incomingHash,
        localSeq,
        event.ordering.observedAt,
        event.createdAt,
        eventJson,
      );

      const insertParent = this.db.prepare(
        'INSERT INTO observation_parents(event_id,parent_event_id) VALUES(?,?)',
      );
      for (const parentId of event.ordering.parentEventIds) insertParent.run(event.eventId, parentId);

      if (event.content.hash) {
        this.db.prepare(`
          INSERT OR IGNORE INTO content_blobs(content_hash,storage_mode,content_ref,bytes,created_at)
          VALUES(?,?,?,?,?)
        `).run(
          event.content.hash,
          event.content.storageMode,
          event.content.ref ?? null,
          event.content.inlinePreview ? Buffer.byteLength(event.content.inlinePreview) : null,
          event.createdAt,
        );
      }

      this.db.exec('COMMIT;');
      return { status: 'accepted', eventId: event.eventId, localSeq };
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  get(eventId: string): ObservationEvent | null {
    const row = this.db.prepare('SELECT event_json FROM observation_events WHERE event_id=?').get(eventId) as { event_json: string } | undefined;
    if (!row) return null;
    return ObservationEventSchema.parse(JSON.parse(row.event_json));
  }

  list(filter: ObservationListFilter): ObservationEvent[] {
    const limit = Math.max(1, Math.min(filter.limit, 500));
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (filter.beforeLocalSeq !== undefined) {
      where.push('local_seq < ?');
      args.push(filter.beforeLocalSeq);
    }
    if (filter.eventType !== undefined) {
      where.push('event_type = ?');
      args.push(filter.eventType);
    }
    const clause = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';
    const rows = this.db.prepare(`
      SELECT event_json FROM observation_events
      ${clause}
      ORDER BY local_seq DESC
      LIMIT ?
    `).all(...args, limit) as unknown as Array<{ event_json: string }>;
    return rows.map(row => ObservationEventSchema.parse(JSON.parse(row.event_json)));
  }

  listParents(eventId: string): string[] {
    const rows = this.db.prepare(`
      SELECT parent_event_id FROM observation_parents
      WHERE event_id=? ORDER BY parent_event_id
    `).all(eventId) as unknown as Array<{ parent_event_id: string }>;
    return rows.map(row => row.parent_event_id);
  }

  listQuarantined(limit: number): QuarantinedObservation[] {
    const rows = this.db.prepare(`
      SELECT quarantine_id,event_id,reason,existing_event_id,created_at
      FROM quarantine_events ORDER BY created_at DESC, quarantine_id DESC LIMIT ?
    `).all(Math.max(1, Math.min(limit, 500))) as unknown as Array<{
      quarantine_id: string;
      event_id: string;
      reason: string;
      existing_event_id: string | null;
      created_at: string;
    }>;
    return rows.map(row => ({
      quarantineId: row.quarantine_id,
      eventId: row.event_id,
      reason: row.reason,
      ...(row.existing_event_id ? { existingEventId: row.existing_event_id } : {}),
      createdAt: row.created_at,
    }));
  }

  private createFreshSchema(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const version = this.schemaVersion();
      if (version !== 0) {
        this.db.exec('COMMIT;');
        return;
      }
      this.db.exec(FRESH_SCHEMA);
      this.db.exec(`PRAGMA user_version=${SCHEMA_VERSION};`);
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  private validateSchema(): void {
    const required = [
      'observation_events',
      'observation_parents',
      'content_blobs',
      'producer_checkpoints',
      'sync_outbox',
      'quarantine_events',
      'local_sequence_counter',
    ];
    for (const table of required) {
      const row = this.db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(table) as { name: string } | undefined;
      if (!row) throw new Error(`km_observation_schema_invalid:missing_${table}`);
    }
  }
}
