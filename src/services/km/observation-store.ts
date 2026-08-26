import { createHash, randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseSync as DatabaseSyncType } from 'node:sqlite';
import {
  ObservationEventSchema,
  type ObservationEvent,
  type ObservationEventType,
} from './observation-schema.js';

const SCHEMA_VERSION = 2;
const BUSY_TIMEOUT_MS = 5_000;

const KNOWLEDGE_STATES = [
  'observed', 'candidate', 'deduped', 'review_pending', 'approved', 'exported',
  'stale', 'conflict', 'rejected', 'deprecated', 'purged_local',
] as const;
const MEMORY_STATES = [
  'proposed', 'active', 'stale', 'conflicted', 'shadowed', 'expired', 'revoked', 'purged_local',
] as const;

export type KnowledgeState = typeof KNOWLEDGE_STATES[number];
export type MemoryState = typeof MEMORY_STATES[number];
export type KnowledgeLayer = 'L1' | 'L2' | 'L3' | 'L4' | 'reviewed-only';
export type MemoryScope = 'user' | 'bot' | 'workspace' | 'project' | 'skill' | 'environment' | 'team';
export type KmConfidence = 'observed' | 'inferred';
export type KmPrivacyClass = 'public-to-team' | 'internal' | 'sensitive' | 'secret-reference-only';

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

const PHASE2_SCHEMA = `
  CREATE TABLE IF NOT EXISTS knowledge_items (
    knowledge_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('observed','candidate','deduped','review_pending','approved','exported','stale','conflict','rejected','deprecated','purged_local')),
    target_layer TEXT NOT NULL CHECK(target_layer IN ('L1','L2','L3','L4','reviewed-only')),
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    claim_key TEXT NOT NULL,
    claim_text TEXT NOT NULL,
    confidence TEXT NOT NULL CHECK(confidence IN ('observed','inferred')),
    freshness TEXT NOT NULL CHECK(freshness IN ('fresh','stale','purged','unknown')),
    privacy_class TEXT NOT NULL CHECK(privacy_class IN ('public-to-team','internal','sensitive','secret-reference-only')),
    source_refs_json TEXT NOT NULL CHECK(json_valid(source_refs_json)),
    review_after TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(target_layer,claim_key,claim_text)
  );
  CREATE INDEX IF NOT EXISTS knowledge_items_state_layer ON knowledge_items(state,target_layer,updated_at DESC);
  CREATE INDEX IF NOT EXISTS knowledge_items_claim ON knowledge_items(claim_key);

  CREATE TABLE IF NOT EXISTS knowledge_state_history (
    history_id TEXT PRIMARY KEY,
    knowledge_id TEXT NOT NULL REFERENCES knowledge_items(knowledge_id) ON DELETE CASCADE,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    evidence_event_id TEXT REFERENCES observation_events(event_id),
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS memory_items (
    memory_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('proposed','active','stale','conflicted','shadowed','expired','revoked','purged_local')),
    scope TEXT NOT NULL CHECK(scope IN ('user','bot','workspace','project','skill','environment','team')),
    subject TEXT NOT NULL,
    claim_key TEXT NOT NULL,
    claim_text TEXT NOT NULL,
    confidence TEXT NOT NULL CHECK(confidence IN ('observed','inferred')),
    source_refs_json TEXT NOT NULL CHECK(json_valid(source_refs_json)),
    ttl_expires_at TEXT,
    review_after TEXT,
    sync_policy TEXT NOT NULL CHECK(sync_policy IN ('local-only','redacted-central','central-approved')),
    privacy_class TEXT NOT NULL CHECK(privacy_class IN ('public-to-team','internal','sensitive','secret-reference-only')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(scope,subject,claim_key)
  );
  CREATE INDEX IF NOT EXISTS memory_items_scope_subject ON memory_items(scope,subject,state);
  CREATE INDEX IF NOT EXISTS memory_items_claim ON memory_items(claim_key);

  CREATE TABLE IF NOT EXISTS memory_state_history (
    history_id TEXT PRIMARY KEY,
    memory_id TEXT NOT NULL REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    evidence_event_id TEXT REFERENCES observation_events(event_id),
    created_at TEXT NOT NULL
  );
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

export interface KnowledgeItem {
  knowledgeId: string;
  state: KnowledgeState;
  targetLayer: KnowledgeLayer;
  category: string;
  title: string;
  claimKey: string;
  claimText: string;
  confidence: KmConfidence;
  freshness: 'fresh' | 'stale' | 'purged' | 'unknown';
  privacyClass: KmPrivacyClass;
  sourceRefs: unknown[];
  reviewAfter?: string;
  createdAt: string;
  updatedAt: string;
}

export interface KnowledgeCandidateInput {
  knowledgeId?: string;
  targetLayer: KnowledgeLayer;
  category: string;
  title: string;
  claimKey: string;
  claimText: string;
  confidence: KmConfidence;
  freshness?: KnowledgeItem['freshness'];
  privacyClass: KmPrivacyClass;
  sourceRefs: unknown[];
  reviewAfter?: string;
  evidenceEventId?: string;
}

export interface MemoryItem {
  memoryId: string;
  state: MemoryState;
  scope: MemoryScope;
  subject: string;
  claimKey: string;
  claimText: string;
  confidence: KmConfidence;
  sourceRefs: unknown[];
  ttlExpiresAt?: string;
  reviewAfter?: string;
  syncPolicy: 'local-only' | 'redacted-central' | 'central-approved';
  privacyClass: KmPrivacyClass;
  createdAt: string;
  updatedAt: string;
}

export interface MemoryUpsertInput {
  memoryId?: string;
  state?: 'proposed' | 'active';
  scope: MemoryScope;
  subject: string;
  claimKey: string;
  claimText: string;
  confidence: KmConfidence;
  sourceRefs: unknown[];
  ttlExpiresAt?: string;
  reviewAfter?: string;
  syncPolicy?: MemoryItem['syncPolicy'];
  privacyClass: KmPrivacyClass;
  evidenceEventId?: string;
}

export interface RetrievalQuery {
  text: string;
  scopes?: MemoryScope[];
  subject?: string;
  targetLayers?: KnowledgeLayer[];
  limit: number;
}

export interface RetrievalItem {
  id: string;
  kind: 'knowledge' | 'memory';
  title: string;
  text: string;
  score: number;
  sourceRefs: unknown[];
  privacyClass: KmPrivacyClass;
  freshness: 'fresh' | 'stale' | 'purged' | 'unknown';
}

export interface KnowledgeExportDryRun {
  knowledgeId: string;
  targetLayer: KnowledgeLayer;
  allowed: boolean;
  requiredApprovalGrade: 'G2';
  reason?: string;
  action: { kind: 'knowledge-export'; targetLayer: KnowledgeLayer; claimKey: string; title: string };
  risk: { mutatesWorkspace: true; automaticExecution: false };
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

function kmId(prefix: 'kn' | 'mem' | 'hist'): string {
  return `${prefix}_${randomUUID().replaceAll('-', '')}`;
}

function requireText(value: string, field: string): string {
  const result = value.trim();
  if (!result) throw new Error(`km_${field}_required`);
  return result;
}

function parseJsonArray(value: string): unknown[] {
  const parsed = JSON.parse(value);
  return Array.isArray(parsed) ? parsed : [];
}

const KNOWLEDGE_TRANSITIONS: Readonly<Record<KnowledgeState, readonly KnowledgeState[]>> = {
  observed: ['candidate'],
  candidate: ['deduped', 'review_pending', 'rejected'],
  deduped: ['review_pending'],
  review_pending: ['approved', 'rejected', 'conflict'],
  approved: ['exported', 'stale', 'conflict', 'deprecated'],
  exported: ['stale', 'deprecated', 'purged_local'],
  stale: ['review_pending', 'deprecated', 'purged_local'],
  conflict: ['review_pending', 'rejected'],
  rejected: [],
  deprecated: ['purged_local'],
  purged_local: [],
};

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
    if (this.schemaVersion() < 2) this.migrateToPhase2();
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

  counts(): { observations: number; quarantined: number; knowledge: number; memory: number } {
    return {
      observations: Number((this.db.prepare('SELECT COUNT(*) AS count FROM observation_events').get() as any).count),
      quarantined: Number((this.db.prepare('SELECT COUNT(*) AS count FROM quarantine_events').get() as any).count),
      knowledge: Number((this.db.prepare('SELECT COUNT(*) AS count FROM knowledge_items').get() as any).count),
      memory: Number((this.db.prepare('SELECT COUNT(*) AS count FROM memory_items').get() as any).count),
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

  proposeKnowledge(input: KnowledgeCandidateInput, actorId = 'system'): { item: KnowledgeItem; created: boolean } {
    const claimKey = requireText(input.claimKey, 'knowledge_claim_key');
    const claimText = requireText(input.claimText, 'knowledge_claim_text');
    if (input.sourceRefs.length === 0) throw new Error('km_knowledge_source_refs_required');
    const existing = this.db.prepare(`
      SELECT * FROM knowledge_items WHERE target_layer=? AND claim_key=? AND claim_text=?
    `).get(input.targetLayer, claimKey, claimText) as any;
    if (existing) return { item: this.knowledgeFromRow(existing), created: false };

    const now = new Date().toISOString();
    const knowledgeId = input.knowledgeId ?? kmId('kn');
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`
        INSERT INTO knowledge_items(
          knowledge_id,state,target_layer,category,title,claim_key,claim_text,confidence,
          freshness,privacy_class,source_refs_json,review_after,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      `).run(
        knowledgeId, 'candidate', input.targetLayer, requireText(input.category, 'knowledge_category'),
        requireText(input.title, 'knowledge_title'), claimKey, claimText, input.confidence,
        input.freshness ?? 'unknown', input.privacyClass, JSON.stringify(input.sourceRefs),
        input.reviewAfter ?? null, now, now,
      );
      this.db.prepare(`
        INSERT INTO knowledge_state_history(history_id,knowledge_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
        VALUES(?,?,?,?,?,?,?,?)
      `).run(kmId('hist'), knowledgeId, null, 'candidate', 'candidate_proposed', actorId, input.evidenceEventId ?? null, now);
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
    return { item: this.getKnowledge(knowledgeId)!, created: true };
  }

  getKnowledge(knowledgeId: string): KnowledgeItem | null {
    const row = this.db.prepare('SELECT * FROM knowledge_items WHERE knowledge_id=?').get(knowledgeId) as any;
    return row ? this.knowledgeFromRow(row) : null;
  }

  listKnowledge(input: { limit: number; state?: KnowledgeState; targetLayer?: KnowledgeLayer }): KnowledgeItem[] {
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (input.state) { where.push('state=?'); args.push(input.state); }
    if (input.targetLayer) { where.push('target_layer=?'); args.push(input.targetLayer); }
    const clause = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const rows = this.db.prepare(`SELECT * FROM knowledge_items ${clause} ORDER BY updated_at DESC,knowledge_id DESC LIMIT ?`)
      .all(...args, Math.max(1, Math.min(input.limit, 500))) as any[];
    return rows.map(row => this.knowledgeFromRow(row));
  }

  transitionKnowledge(input: {
    knowledgeId: string;
    toState: KnowledgeState;
    reasonCode: string;
    actorId: string;
    evidenceEventId?: string;
  }): KnowledgeItem {
    const current = this.getKnowledge(input.knowledgeId);
    if (!current) throw new Error('km_knowledge_not_found');
    if (!KNOWLEDGE_TRANSITIONS[current.state].includes(input.toState)) {
      throw new Error(`km_knowledge_invalid_transition:${current.state}:${input.toState}`);
    }
    if (current.confidence === 'inferred' && (input.toState === 'approved' || input.toState === 'exported')) {
      if (!input.actorId.trim() || input.actorId === 'system') throw new Error('km_knowledge_inferred_requires_human_review');
    }
    const now = new Date().toISOString();
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare('UPDATE knowledge_items SET state=?,updated_at=? WHERE knowledge_id=?')
        .run(input.toState, now, input.knowledgeId);
      this.db.prepare(`
        INSERT INTO knowledge_state_history(history_id,knowledge_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
        VALUES(?,?,?,?,?,?,?,?)
      `).run(kmId('hist'), input.knowledgeId, current.state, input.toState,
        requireText(input.reasonCode, 'knowledge_reason'), requireText(input.actorId, 'actor_id'), input.evidenceEventId ?? null, now);
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
    return this.getKnowledge(input.knowledgeId)!;
  }

  knowledgeExportDryRun(knowledgeId: string): KnowledgeExportDryRun {
    const item = this.getKnowledge(knowledgeId);
    if (!item) throw new Error('km_knowledge_not_found');
    const allowed = item.state === 'approved' && item.targetLayer !== 'reviewed-only';
    return {
      knowledgeId: item.knowledgeId,
      targetLayer: item.targetLayer,
      allowed,
      requiredApprovalGrade: 'G2',
      ...(!allowed ? { reason: item.targetLayer === 'reviewed-only' ? 'reviewed_only_not_exportable' : 'knowledge_not_approved' } : {}),
      action: { kind: 'knowledge-export', targetLayer: item.targetLayer, claimKey: item.claimKey, title: item.title },
      risk: { mutatesWorkspace: true, automaticExecution: false },
    };
  }

  upsertMemory(input: MemoryUpsertInput): { item: MemoryItem; created: boolean; conflicted: boolean } {
    const subject = requireText(input.subject, 'memory_subject');
    const claimKey = requireText(input.claimKey, 'memory_claim_key');
    const claimText = requireText(input.claimText, 'memory_claim_text');
    if (input.sourceRefs.length === 0) throw new Error('km_memory_source_refs_required');
    const existing = this.db.prepare('SELECT * FROM memory_items WHERE scope=? AND subject=? AND claim_key=?')
      .get(input.scope, subject, claimKey) as any;
    if (existing && existing.claim_text === claimText) {
      return { item: this.memoryFromRow(existing), created: false, conflicted: false };
    }

    const now = new Date().toISOString();
    if (existing) {
      this.db.exec('BEGIN IMMEDIATE;');
      try {
        this.db.prepare(`UPDATE memory_items SET state='conflicted',updated_at=? WHERE memory_id=?`).run(now, existing.memory_id);
        this.db.prepare(`
          INSERT INTO memory_state_history(history_id,memory_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
          VALUES(?,?,?,?,?,?,?,?)
        `).run(kmId('hist'), existing.memory_id, existing.state, 'conflicted', 'claim_conflict', 'system', input.evidenceEventId ?? null, now);
        this.db.exec('COMMIT;');
      } catch (error) {
        try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
        throw error;
      }
      return { item: this.getMemory(existing.memory_id)!, created: false, conflicted: true };
    }

    const state = input.state ?? 'proposed';
    if (state === 'active' && input.confidence === 'inferred') throw new Error('km_memory_inferred_cannot_activate_directly');
    const memoryId = input.memoryId ?? kmId('mem');
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`
        INSERT INTO memory_items(
          memory_id,state,scope,subject,claim_key,claim_text,confidence,source_refs_json,
          ttl_expires_at,review_after,sync_policy,privacy_class,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      `).run(memoryId, state, input.scope, subject, claimKey, claimText, input.confidence,
        JSON.stringify(input.sourceRefs), input.ttlExpiresAt ?? null, input.reviewAfter ?? null,
        input.syncPolicy ?? 'local-only', input.privacyClass, now, now);
      this.db.prepare(`
        INSERT INTO memory_state_history(history_id,memory_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
        VALUES(?,?,?,?,?,?,?,?)
      `).run(kmId('hist'), memoryId, null, state, 'memory_upserted', 'system', input.evidenceEventId ?? null, now);
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
    return { item: this.getMemory(memoryId)!, created: true, conflicted: false };
  }

  getMemory(memoryId: string): MemoryItem | null {
    const row = this.db.prepare('SELECT * FROM memory_items WHERE memory_id=?').get(memoryId) as any;
    return row ? this.memoryFromRow(row) : null;
  }

  listMemory(input: { limit: number; state?: MemoryState; scope?: MemoryScope; subject?: string }): MemoryItem[] {
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (input.state) { where.push('state=?'); args.push(input.state); }
    if (input.scope) { where.push('scope=?'); args.push(input.scope); }
    if (input.subject) { where.push('subject=?'); args.push(input.subject); }
    const clause = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const rows = this.db.prepare(`SELECT * FROM memory_items ${clause} ORDER BY updated_at DESC,memory_id DESC LIMIT ?`)
      .all(...args, Math.max(1, Math.min(input.limit, 500))) as any[];
    return rows.map(row => this.memoryFromRow(row));
  }

  retrieve(input: RetrievalQuery): RetrievalItem[] {
    const limit = Math.max(1, Math.min(input.limit, 100));
    const terms = input.text.toLowerCase().split(/\s+/u).map(term => term.trim()).filter(Boolean);
    const score = (text: string): number => terms.length === 0
      ? 1
      : terms.reduce((sum, term) => sum + (text.toLowerCase().includes(term) ? 1 : 0), 0) / terms.length;
    const items: RetrievalItem[] = [];

    for (const item of this.listKnowledge({ limit: 500, state: 'approved' })) {
      if (input.targetLayers?.length && !input.targetLayers.includes(item.targetLayer)) continue;
      if (item.freshness === 'stale' || item.freshness === 'purged') continue;
      const itemScore = score(`${item.title} ${item.claimKey} ${item.claimText}`);
      if (terms.length && itemScore === 0) continue;
      items.push({ id: item.knowledgeId, kind: 'knowledge', title: item.title, text: item.claimText,
        score: itemScore, sourceRefs: item.sourceRefs, privacyClass: item.privacyClass, freshness: item.freshness });
    }

    for (const item of this.listMemory({ limit: 500, state: 'active' })) {
      if (input.scopes?.length && !input.scopes.includes(item.scope)) continue;
      if (input.subject && input.subject !== item.subject) continue;
      if (item.ttlExpiresAt && Date.parse(item.ttlExpiresAt) <= Date.now()) continue;
      const itemScore = score(`${item.claimKey} ${item.claimText}`);
      if (terms.length && itemScore === 0) continue;
      items.push({ id: item.memoryId, kind: 'memory', title: item.claimKey, text: item.claimText,
        score: itemScore, sourceRefs: item.sourceRefs, privacyClass: item.privacyClass, freshness: 'fresh' });
    }
    return items.sort((a, b) => b.score - a.score || a.id.localeCompare(b.id)).slice(0, limit);
  }

  private knowledgeFromRow(row: any): KnowledgeItem {
    return {
      knowledgeId: row.knowledge_id, state: row.state, targetLayer: row.target_layer,
      category: row.category, title: row.title, claimKey: row.claim_key, claimText: row.claim_text,
      confidence: row.confidence, freshness: row.freshness, privacyClass: row.privacy_class,
      sourceRefs: parseJsonArray(row.source_refs_json), ...(row.review_after ? { reviewAfter: row.review_after } : {}),
      createdAt: row.created_at, updatedAt: row.updated_at,
    };
  }

  private memoryFromRow(row: any): MemoryItem {
    return {
      memoryId: row.memory_id, state: row.state, scope: row.scope, subject: row.subject,
      claimKey: row.claim_key, claimText: row.claim_text, confidence: row.confidence,
      sourceRefs: parseJsonArray(row.source_refs_json), ...(row.ttl_expires_at ? { ttlExpiresAt: row.ttl_expires_at } : {}),
      ...(row.review_after ? { reviewAfter: row.review_after } : {}), syncPolicy: row.sync_policy,
      privacyClass: row.privacy_class, createdAt: row.created_at, updatedAt: row.updated_at,
    };
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
      this.db.exec('PRAGMA user_version=1;');
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  private migrateToPhase2(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 2) {
        this.db.exec('COMMIT;');
        return;
      }
      this.db.exec(PHASE2_SCHEMA);
      this.db.exec('PRAGMA user_version=2;');
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
      'knowledge_items',
      'knowledge_state_history',
      'memory_items',
      'memory_state_history',
    ];
    for (const table of required) {
      const row = this.db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(table) as { name: string } | undefined;
      if (!row) throw new Error(`km_observation_schema_invalid:missing_${table}`);
    }
  }
}
