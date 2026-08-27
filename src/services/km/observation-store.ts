import { createHash, randomUUID } from 'node:crypto';
import { lstatSync, mkdirSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import type { DatabaseSync as DatabaseSyncType } from 'node:sqlite';
import { KmMemoryProviderConfigSchema, KmPipelineProfileSchema, KmProviderDescriptorSchema, type KmMemoryProviderConfig, type KmPipelineProfile, type KmProviderDescriptor } from './provider-spi.js';
import {
  ObservationEventSchema,
  type ObservationEvent,
  type ObservationEventType,
} from './observation-schema.js';

const SCHEMA_VERSION = 12;
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

const PHASE9_SCHEMA = `
  ALTER TABLE distillation_jobs ADD COLUMN evidence_context_json TEXT NOT NULL DEFAULT '{}';
  CREATE TABLE IF NOT EXISTS retrieval_runs (
    retrieval_run_id TEXT PRIMARY KEY, bot_app_id TEXT NOT NULL, session_id TEXT NOT NULL, turn_id TEXT,
    query_hash TEXT NOT NULL, mode TEXT NOT NULL CHECK(mode IN ('off','shadow','canary','active')),
    candidate_count INTEGER NOT NULL, eligible_count INTEGER NOT NULL, latency_ms INTEGER NOT NULL,
    warnings_json TEXT NOT NULL CHECK(json_valid(warnings_json)), created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS retrieval_results (
    retrieval_run_id TEXT NOT NULL REFERENCES retrieval_runs(retrieval_run_id) ON DELETE CASCADE,
    item_id TEXT NOT NULL, item_kind TEXT NOT NULL, provider_ids_json TEXT NOT NULL CHECK(json_valid(provider_ids_json)),
    score REAL NOT NULL, eligible INTEGER NOT NULL CHECK(eligible IN (0,1)), filter_reason TEXT,
    PRIMARY KEY(retrieval_run_id,item_id,item_kind)
  );
  CREATE TABLE IF NOT EXISTS prompt_injection_snapshots (
    snapshot_id TEXT PRIMARY KEY, retrieval_run_id TEXT NOT NULL REFERENCES retrieval_runs(retrieval_run_id) ON DELETE CASCADE,
    bot_app_id TEXT NOT NULL, mode TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN ('off','would_inject','injected','skipped')),
    item_ids_json TEXT NOT NULL CHECK(json_valid(item_ids_json)), prompt_hash TEXT, prompt_bytes INTEGER NOT NULL,
    reason TEXT, created_at TEXT NOT NULL
  );
`;

const PHASE8_SCHEMA = `
  CREATE TABLE IF NOT EXISTS memory_backend_outbox (
    outbox_id TEXT PRIMARY KEY, memory_id TEXT NOT NULL REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL, operation TEXT NOT NULL CHECK(operation IN ('put','revoke','verify')),
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), payload_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','inflight','delivered','failed','quarantined')),
    attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER NOT NULL,
    claimed_at INTEGER, claim_token TEXT, last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    UNIQUE(memory_id,provider_id,operation,payload_hash)
  );
  CREATE INDEX IF NOT EXISTS memory_backend_outbox_due ON memory_backend_outbox(status,next_attempt_at,created_at);
  CREATE TABLE IF NOT EXISTS memory_backend_migrations (
    migration_id TEXT PRIMARY KEY, bot_app_id TEXT NOT NULL, from_profile_json TEXT NOT NULL CHECK(json_valid(from_profile_json)),
    to_profile_json TEXT NOT NULL CHECK(json_valid(to_profile_json)), state TEXT NOT NULL CHECK(state IN ('draft','backfilling','comparing','ready','cutover','rolled_back','failed')),
    checkpoint TEXT, stats_json TEXT NOT NULL CHECK(json_valid(stats_json)), created_at TEXT NOT NULL, updated_at TEXT NOT NULL
  );
`;

const PHASE12_SCHEMA = `
  UPDATE km_pipeline_profiles SET state='retired'
    WHERE state='shadow' AND rowid NOT IN (SELECT MAX(rowid) FROM km_pipeline_profiles WHERE state='shadow' GROUP BY bot_app_id);
  CREATE UNIQUE INDEX IF NOT EXISTS km_pipeline_profiles_one_shadow_bot
    ON km_pipeline_profiles(bot_app_id) WHERE state='shadow';
  CREATE TABLE IF NOT EXISTS km_runtime_leases (
    lease_name TEXT PRIMARY KEY, holder_id TEXT NOT NULL, expires_at INTEGER NOT NULL, updated_at TEXT NOT NULL
  );
`;

const PHASE11_SCHEMA = `
  CREATE TABLE IF NOT EXISTS km_mutation_idempotency (
    actor_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, route TEXT NOT NULL, request_hash TEXT NOT NULL,
    status_code INTEGER NOT NULL, response_json TEXT NOT NULL CHECK(json_valid(response_json)), created_at TEXT NOT NULL,
    PRIMARY KEY(actor_id,idempotency_key)
  );
  CREATE TABLE IF NOT EXISTS km_config_audit (
    audit_id TEXT PRIMARY KEY, actor_id TEXT NOT NULL, action TEXT NOT NULL, target_ref TEXT NOT NULL,
    before_hash TEXT, after_hash TEXT, request_hash TEXT NOT NULL, idempotency_key TEXT NOT NULL, created_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS km_config_audit_created ON km_config_audit(created_at DESC,audit_id);
  CREATE TABLE IF NOT EXISTS km_memory_policy_decisions (
    decision_id TEXT PRIMARY KEY, source_event_id TEXT NOT NULL, memory_id TEXT,
    policy_version TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN ('reject','propose','activate')),
    reason_codes_json TEXT NOT NULL CHECK(json_valid(reason_codes_json)), evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    created_at TEXT NOT NULL, UNIQUE(source_event_id,policy_version,evidence_json)
  );
  CREATE INDEX IF NOT EXISTS km_memory_policy_decisions_created ON km_memory_policy_decisions(created_at DESC,decision_id);
`;

const PHASE10_SCHEMA = `
  CREATE TABLE IF NOT EXISTS km_memory_provider_configs (
    provider_id TEXT PRIMARY KEY, config_json TEXT NOT NULL CHECK(json_valid(config_json)),
    config_hash TEXT NOT NULL, updated_at TEXT NOT NULL
  );
`;

const PHASE7_SCHEMA = `
  CREATE TABLE IF NOT EXISTS memory_backend_bindings (
    memory_id TEXT NOT NULL REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL, provider_version TEXT NOT NULL, backend_ref TEXT,
    write_state TEXT NOT NULL CHECK(write_state IN ('pending','active','failed','revoked','shadow')),
    content_hash TEXT NOT NULL, last_verified_at TEXT, last_error TEXT, updated_at TEXT NOT NULL,
    PRIMARY KEY(memory_id,provider_id)
  );
  CREATE INDEX IF NOT EXISTS memory_backend_bindings_provider ON memory_backend_bindings(provider_id,write_state,updated_at);
`;

const PHASE6_SCHEMA = `
  CREATE TABLE IF NOT EXISTS km_provider_registry (
    provider_id TEXT NOT NULL, provider_kind TEXT NOT NULL, provider_version TEXT NOT NULL,
    descriptor_json TEXT NOT NULL CHECK(json_valid(descriptor_json)), status TEXT NOT NULL,
    last_health_json TEXT CHECK(last_health_json IS NULL OR json_valid(last_health_json)), updated_at TEXT NOT NULL,
    PRIMARY KEY(provider_id,provider_version)
  );
  CREATE TABLE IF NOT EXISTS km_pipeline_profiles (
    profile_id TEXT NOT NULL, revision INTEGER NOT NULL, bot_app_id TEXT NOT NULL,
    profile_json TEXT NOT NULL CHECK(json_valid(profile_json)), profile_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('draft','shadow','active','retired')), created_at TEXT NOT NULL,
    PRIMARY KEY(profile_id,revision)
  );
  CREATE UNIQUE INDEX IF NOT EXISTS km_pipeline_profiles_one_active_bot
    ON km_pipeline_profiles(bot_app_id) WHERE state='active';
  CREATE TABLE IF NOT EXISTS distillation_jobs (
    job_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, source_event_id TEXT NOT NULL,
    bot_app_id TEXT NOT NULL, profile_id TEXT NOT NULL, profile_revision INTEGER NOT NULL,
    profile_snapshot_json TEXT NOT NULL CHECK(json_valid(profile_snapshot_json)),
    state TEXT NOT NULL CHECK(state IN ('queued','resolving','extracting','normalizing','gating','persisted','completed','retry_wait','inconclusive','quarantined','failed','cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER NOT NULL,
    claimed_at INTEGER, claim_token TEXT, last_error TEXT, output_hash TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS distillation_jobs_due ON distillation_jobs(state,next_attempt_at,created_at);
`;

const PHASE5_SCHEMA = `
  ALTER TABLE sync_outbox ADD COLUMN payload_json TEXT;
  ALTER TABLE sync_outbox ADD COLUMN payload_hash TEXT;
`;

const PHASE4_SCHEMA = `
  CREATE TABLE IF NOT EXISTS sync_sinks (
    sink_id TEXT PRIMARY KEY,
    protocol_version INTEGER NOT NULL CHECK(protocol_version>=1),
    endpoint_ref TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0 CHECK(enabled IN (0,1)),
    redaction_policy_json TEXT NOT NULL CHECK(json_valid(redaction_policy_json)),
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sync_cursors (
    sink_id TEXT PRIMARY KEY REFERENCES sync_sinks(sink_id) ON DELETE CASCADE,
    last_local_seq INTEGER NOT NULL DEFAULT 0 CHECK(last_local_seq>=0),
    last_batch_id TEXT, last_ack_at TEXT, central_cursor TEXT,
    status TEXT NOT NULL CHECK(status IN ('idle','syncing','degraded','blocked')),
    updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sync_quarantine (
    quarantine_id TEXT PRIMARY KEY, sink_id TEXT NOT NULL,
    event_id TEXT, reason TEXT NOT NULL, payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL, resolved_at TEXT
  );
`;

const PHASE3_SCHEMA = `
  CREATE TABLE IF NOT EXISTS trace_edges (
    edge_id TEXT PRIMARY KEY,
    from_type TEXT NOT NULL, from_id TEXT NOT NULL,
    to_type TEXT NOT NULL, to_id TEXT NOT NULL,
    edge_type TEXT NOT NULL CHECK(edge_type IN ('caused','used','produced','evaluated','superseded','conflicted','approved','synced','purged')),
    evidence_event_id TEXT REFERENCES observation_events(event_id), created_at TEXT NOT NULL,
    UNIQUE(from_type,from_id,to_type,to_id,edge_type)
  );
  CREATE INDEX IF NOT EXISTS trace_edges_from ON trace_edges(from_type,from_id,edge_type);
  CREATE INDEX IF NOT EXISTS trace_edges_to ON trace_edges(to_type,to_id,edge_type);
  CREATE TABLE IF NOT EXISTS eval_runs (
    eval_run_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('queued','running','scored','accepted','skipped','failed','inconclusive','superseded')),
    evaluator_name TEXT NOT NULL, evaluator_version TEXT NOT NULL,
    target_type TEXT NOT NULL CHECK(target_type IN ('turn','workflow-artifact','knowledge','memory','skill','sync-batch','proposal')),
    target_id TEXT NOT NULL, started_at TEXT, completed_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    UNIQUE(evaluator_name,evaluator_version,target_type,target_id)
  );
  CREATE TABLE IF NOT EXISTS eval_results (
    eval_result_id TEXT PRIMARY KEY, eval_run_id TEXT NOT NULL REFERENCES eval_runs(eval_run_id) ON DELETE CASCADE,
    metric_key TEXT NOT NULL, score REAL,
    verdict TEXT NOT NULL CHECK(verdict IN ('pass','warn','fail','not_applicable','inconclusive')),
    confidence TEXT NOT NULL CHECK(confidence IN ('observed','inferred')),
    details_json TEXT NOT NULL CHECK(json_valid(details_json)), source_refs_json TEXT NOT NULL CHECK(json_valid(source_refs_json)),
    created_at TEXT NOT NULL, UNIQUE(eval_run_id,metric_key)
  );
  CREATE TABLE IF NOT EXISTS evolution_proposals (
    proposal_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('draft','review_pending','approved','executing','applied','verified','failed','rejected','abandoned','expired','reverted','superseded')),
    proposal_type TEXT NOT NULL CHECK(proposal_type IN ('skill-route','skill-edit','knowledge-promotion','memory-policy','dashboard-warning','workflow-revision','cleanup-action','external-action')),
    target_ref TEXT NOT NULL, approval_grade TEXT NOT NULL CHECK(approval_grade IN ('G0','G1','G2','G3','G4')),
    summary TEXT NOT NULL, evidence_refs_json TEXT NOT NULL CHECK(json_valid(evidence_refs_json)),
    proposed_action_json TEXT NOT NULL CHECK(json_valid(proposed_action_json)), risk_json TEXT NOT NULL CHECK(json_valid(risk_json)),
    rollback_json TEXT NOT NULL CHECK(json_valid(rollback_json)), created_by TEXT NOT NULL, approved_by TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS approval_decisions (
    approval_id TEXT PRIMARY KEY, proposal_id TEXT NOT NULL REFERENCES evolution_proposals(proposal_id) ON DELETE CASCADE,
    grade TEXT NOT NULL CHECK(grade IN ('G0','G1','G2','G3','G4')),
    decision TEXT NOT NULL CHECK(decision IN ('approved','rejected','request_changes','expired','revoked')),
    actor_id TEXT NOT NULL, scope_json TEXT NOT NULL CHECK(json_valid(scope_json)), risk_ack_json TEXT NOT NULL CHECK(json_valid(risk_ack_json)),
    created_at TEXT NOT NULL
  );
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

export interface MemoryBackendBinding {
  memoryId: string; providerId: string; providerVersion: string; backendRef?: string;
  writeState: 'pending' | 'active' | 'failed' | 'revoked' | 'shadow';
  contentHash: string; lastVerifiedAt?: string; lastError?: string; updatedAt: string;
}
export type MemoryBackendOutboxOperation = 'put' | 'revoke' | 'verify';
export interface MemoryBackendOutboxItem {
  outboxId: string; memoryId: string; providerId: string; operation: MemoryBackendOutboxOperation;
  payload: Record<string, unknown>; payloadHash: string; attempts: number;
}
export interface MemoryBackendOutboxClaim {
  claimToken: string;
  items: MemoryBackendOutboxItem[];
}
export interface MemoryBackendOutboxRow extends MemoryBackendOutboxItem {
  status: 'pending' | 'inflight' | 'delivered' | 'failed' | 'quarantined';
  nextAttemptAt: number; claimedAt?: number; lastError?: string; createdAt: string; updatedAt: string;
}
export interface MemoryBackendMigrationSnapshot {
  migrationId: string; botAppId: string; fromProfile: Record<string, unknown>; toProfile: Record<string, unknown>;
  state: 'draft' | 'backfilling' | 'comparing' | 'ready' | 'cutover' | 'rolled_back' | 'failed';
  checkpoint?: string; stats: Record<string, unknown>; createdAt: string; updatedAt: string;
}
export interface MemoryBackendBindingCompareReport {
  fromProviderId: string; toProviderId: string; compared: number; matched: number; missing: number; mismatched: number;
  samples: Array<{ memoryId: string; reason: 'missing' | 'content_hash_mismatch' | 'state_not_active'; fromContentHash?: string; toContentHash?: string; toState?: string }>;
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

export type ApprovalGrade = 'G0' | 'G1' | 'G2' | 'G3' | 'G4';
export interface TraceEdgeInput {
  fromType: string; fromId: string; toType: string; toId: string;
  edgeType: 'caused' | 'used' | 'produced' | 'evaluated' | 'superseded' | 'conflicted' | 'approved' | 'synced' | 'purged';
  evidenceEventId?: string;
}
export interface EvalResultInput {
  metricKey: string; score?: number;
  verdict: 'pass' | 'warn' | 'fail' | 'not_applicable' | 'inconclusive';
  confidence: KmConfidence; details?: Record<string, unknown>; sourceRefs: unknown[];
}
export interface SyncSinkInput {
  sinkId: string; protocolVersion: number; endpointRef: string; enabled?: boolean; redactionPolicy?: Record<string, unknown>;
}
export interface SyncStatus {
  sinkId: string; endpointRef: string; enabled: boolean; status: string;
  lastLocalSeq: number; lastBatchId?: string; lastAckAt?: string; centralCursor?: string;
  pending: number; quarantined: number;
}

export interface EvolutionProposalInput {
  proposalType: 'skill-route' | 'skill-edit' | 'knowledge-promotion' | 'memory-policy' | 'dashboard-warning' | 'workflow-revision' | 'cleanup-action' | 'external-action';
  targetRef: string; approvalGrade: ApprovalGrade; summary: string; evidenceRefs: unknown[];
  proposedAction: Record<string, unknown>; risk: Record<string, unknown>; rollback: Record<string, unknown>; createdBy: string;
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

function kmId(prefix: 'kn' | 'mem' | 'hist' | 'edge' | 'eval' | 'result' | 'evo' | 'approval'): string {
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

const MEMORY_TRANSITIONS: Readonly<Record<MemoryState, readonly MemoryState[]>> = {
  proposed: ['active', 'revoked', 'shadowed'],
  active: ['stale', 'conflicted', 'revoked', 'expired'],
  stale: ['active', 'revoked', 'purged_local'],
  conflicted: ['proposed', 'active', 'revoked'],
  shadowed: ['proposed', 'revoked', 'purged_local'],
  expired: ['active', 'purged_local'],
  revoked: ['purged_local'],
  purged_local: [],
};

const BUILTIN_KM_PROVIDER_DESCRIPTORS: readonly KmProviderDescriptor[] = [
  {
    id: 'observation-source-v1',
    kind: 'source',
    version: '1',
    contractVersion: 1,
    capabilities: ['turn-completed', 'skill-telemetry', 'workflow-artifacts', 'idempotent-source-keys'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 100,
  },
  {
    id: 'bounded-transcript-window-v1',
    kind: 'window-resolver',
    version: '1',
    contractVersion: 1,
    capabilities: ['tail-window', 'transcript-path-resolution', 'metadata-fallback', 'sha256-content-hash'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 1,
  },
  {
    id: 'builtin.rules-v1',
    kind: 'extractor',
    version: '1',
    contractVersion: 1,
    capabilities: ['workflow-artifact-candidates', 'explicit-user-preferences', 'mechanical-attribution-only'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 1,
  },
  {
    id: 'builtin.layer-router-v1',
    kind: 'knowledge-router',
    version: '1',
    contractVersion: 1,
    capabilities: ['reviewed-only', 'l2-l3-routing', 'privacy-preserving-source-refs'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 100,
  },
  {
    id: 'safe-auto-activation-v1',
    kind: 'memory-policy',
    version: '1',
    contractVersion: 1,
    capabilities: ['explicit-observed-low-risk-auto-active', 'sensitive-reject', 'broad-scope-propose'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 100,
  },
  {
    id: 'sqlite',
    kind: 'memory-backend',
    version: '1',
    contractVersion: 1,
    capabilities: ['local-durable', 'retrieve', 'state-history', 'no-network'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 100,
  },
  {
    id: 'mem0',
    kind: 'memory-backend',
    version: '1',
    contractVersion: 1,
    capabilities: ['configured-only', 'transport-disabled', 'credential-reference'],
    execution: 'service',
    deterministic: false,
    supportsShadow: true,
    maxBatchSize: 50,
  },
  {
    id: 'hindsight',
    kind: 'memory-backend',
    version: '1',
    contractVersion: 1,
    capabilities: ['configured-only', 'transport-disabled', 'credential-reference'],
    execution: 'service',
    deterministic: false,
    supportsShadow: true,
    maxBatchSize: 50,
  },
  {
    id: 'openviking',
    kind: 'memory-backend',
    version: '1',
    contractVersion: 1,
    capabilities: ['configured-only', 'transport-disabled', 'credential-reference'],
    execution: 'service',
    deterministic: false,
    supportsShadow: true,
    maxBatchSize: 50,
  },
  {
    id: 'prompt-memory-v1',
    kind: 'prompt-composer',
    version: '1',
    contractVersion: 1,
    capabilities: ['shadow-would-inject', 'token-budget', 'privacy-filter'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 50,
  },
];

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
    if (this.schemaVersion() < 3) this.migrateToPhase3();
    if (this.schemaVersion() < 4) this.migrateToPhase4();
    if (this.schemaVersion() < 5) this.migrateToPhase5();
    if (this.schemaVersion() < 6) this.migrateToPhase6();
    if (this.schemaVersion() < 7) this.migrateToPhase7();
    if (this.schemaVersion() < 8) this.migrateToPhase8();
    if (this.schemaVersion() < 9) this.migrateToPhase9();
    if (this.schemaVersion() < 10) this.migrateToPhase10();
    if (this.schemaVersion() < 11) this.migrateToPhase11();
    if (this.schemaVersion() < 12) this.migrateToPhase12();
    this.validateSchema();
    this.seedBuiltinKmProvidersBestEffort();
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

  acquireRuntimeLease(input: { leaseName: string; holderId: string; now?: number; ttlMs?: number }): boolean {
    const now = input.now ?? Date.now(); const expiresAt = now + Math.max(1_000,input.ttlMs ?? 45_000);
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const row = this.db.prepare(`SELECT holder_id,expires_at FROM km_runtime_leases WHERE lease_name=?`).get(input.leaseName) as any;
      if (row && row.holder_id !== input.holderId && Number(row.expires_at) > now) { this.db.exec('COMMIT;'); return false; }
      this.db.prepare(`INSERT INTO km_runtime_leases(lease_name,holder_id,expires_at,updated_at) VALUES(?,?,?,?)
        ON CONFLICT(lease_name) DO UPDATE SET holder_id=excluded.holder_id,expires_at=excluded.expires_at,updated_at=excluded.updated_at`)
        .run(input.leaseName,input.holderId,expiresAt,new Date(now).toISOString());
      this.db.exec('COMMIT;'); return true;
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  releaseRuntimeLease(input: { leaseName: string; holderId: string }): void {
    this.db.prepare(`DELETE FROM km_runtime_leases WHERE lease_name=? AND holder_id=?`).run(input.leaseName,input.holderId);
  }

  distillationBacklogStatus(now = Date.now()): { queued: number; retryWait: number; oldestAgeMs: number; claimed: number } {
    const row = this.db.prepare(`SELECT
      SUM(CASE WHEN state='queued' THEN 1 ELSE 0 END) queued,
      SUM(CASE WHEN state='retry_wait' THEN 1 ELSE 0 END) retry_wait,
      SUM(CASE WHEN claim_token IS NOT NULL THEN 1 ELSE 0 END) claimed,
      MIN(CASE WHEN state IN ('queued','retry_wait') THEN created_at END) oldest
      FROM distillation_jobs`).get() as any;
    const oldestMs = row.oldest ? Date.parse(row.oldest) : now;
    return { queued: Number(row.queued ?? 0), retryWait: Number(row.retry_wait ?? 0), claimed: Number(row.claimed ?? 0),
      oldestAgeMs: Math.max(0, now - oldestMs) };
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
    this.db.exec('SAVEPOINT km_knowledge_transition;');
    try {
      this.db.prepare('UPDATE knowledge_items SET state=?,updated_at=? WHERE knowledge_id=?')
        .run(input.toState, now, input.knowledgeId);
      this.db.prepare(`
        INSERT INTO knowledge_state_history(history_id,knowledge_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
        VALUES(?,?,?,?,?,?,?,?)
      `).run(kmId('hist'), input.knowledgeId, current.state, input.toState,
        requireText(input.reasonCode, 'knowledge_reason'), requireText(input.actorId, 'actor_id'), input.evidenceEventId ?? null, now);
      this.db.exec('RELEASE km_knowledge_transition;');
    } catch (error) {
      try { this.db.exec('ROLLBACK TO km_knowledge_transition; RELEASE km_knowledge_transition;'); } catch { /* savepoint may already be closed */ }
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

  upsertMemoryBackendBinding(input: Omit<MemoryBackendBinding, 'updatedAt'>): MemoryBackendBinding {
    if (!this.getMemory(input.memoryId)) throw new Error('km_memory_not_found');
    const now = new Date().toISOString();
    this.db.prepare(`INSERT INTO memory_backend_bindings(
      memory_id,provider_id,provider_version,backend_ref,write_state,content_hash,last_verified_at,last_error,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(memory_id,provider_id) DO UPDATE SET
      provider_version=excluded.provider_version,backend_ref=excluded.backend_ref,write_state=excluded.write_state,
      content_hash=excluded.content_hash,last_verified_at=excluded.last_verified_at,last_error=excluded.last_error,updated_at=excluded.updated_at`)
      .run(input.memoryId, requireText(input.providerId, 'memory_provider_id'), requireText(input.providerVersion, 'memory_provider_version'),
        input.backendRef ?? null, input.writeState, input.contentHash, input.lastVerifiedAt ?? null, input.lastError ?? null, now);
    return this.listMemoryBackendBindings(input.memoryId).find(item => item.providerId === input.providerId)!;
  }

  listMemoryBackendBindings(memoryId: string): MemoryBackendBinding[] {
    const rows = this.db.prepare(`SELECT * FROM memory_backend_bindings WHERE memory_id=? ORDER BY provider_id`).all(memoryId) as any[];
    return rows.map(row => ({ memoryId: row.memory_id, providerId: row.provider_id, providerVersion: row.provider_version,
      ...(row.backend_ref ? { backendRef: row.backend_ref } : {}), writeState: row.write_state, contentHash: row.content_hash,
      ...(row.last_verified_at ? { lastVerifiedAt: row.last_verified_at } : {}), ...(row.last_error ? { lastError: row.last_error } : {}),
      updatedAt: row.updated_at }));
  }

  enqueueMemoryBackendOperation(input: { memoryId: string; providerId: string; operation: MemoryBackendOutboxOperation; payload: Record<string, unknown>; now?: number }): { outboxId: string; created: boolean } {
    if (!this.getMemory(input.memoryId)) throw new Error('km_memory_not_found');
    const payloadJson = JSON.stringify(input.payload);
    const hash = sha256(payloadJson);
    const outboxId = `mout_${createHash('sha256').update(`${input.memoryId}|${input.providerId}|${input.operation}|${hash}`).digest('hex')}`;
    const nowMs = input.now ?? Date.now(); const now = new Date(nowMs).toISOString();
    const result = this.db.prepare(`INSERT OR IGNORE INTO memory_backend_outbox(
      outbox_id,memory_id,provider_id,operation,payload_json,payload_hash,status,next_attempt_at,created_at,updated_at
    ) VALUES(?,?,?,?,?,?,'pending',?,?,?)`).run(outboxId, input.memoryId, input.providerId, input.operation, payloadJson, hash, nowMs, now, now);
    return { outboxId, created: Number(result.changes) === 1 };
  }

  claimMemoryBackendOutboxBatch(input: { providerId?: string; limit: number; now?: number; leaseMs?: number }): MemoryBackendOutboxClaim {
    const now = input.now ?? Date.now();
    const lease = Math.max(1_000, input.leaseMs ?? 60_000);
    const limit = Math.max(1, Math.min(input.limit, 100));
    const token = `mclaim_${randomUUID().replaceAll('-', '')}`;
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`UPDATE memory_backend_outbox SET status='failed',claim_token=NULL,claimed_at=NULL,last_error='claim_lease_expired',next_attempt_at=?,updated_at=?
        WHERE status='inflight' AND claimed_at<?`).run(now, new Date(now).toISOString(), now - lease);
      const rows = input.providerId
        ? this.db.prepare(`SELECT outbox_id FROM memory_backend_outbox WHERE provider_id=? AND status IN ('pending','failed') AND next_attempt_at<=?
            ORDER BY created_at,outbox_id LIMIT ?`).all(input.providerId, now, limit) as any[]
        : this.db.prepare(`SELECT outbox_id FROM memory_backend_outbox WHERE status IN ('pending','failed') AND next_attempt_at<=?
            ORDER BY created_at,outbox_id LIMIT ?`).all(now, limit) as any[];
      const claim = this.db.prepare(`UPDATE memory_backend_outbox SET status='inflight',attempts=attempts+1,claimed_at=?,claim_token=?,updated_at=? WHERE outbox_id=?`);
      for (const row of rows) claim.run(now, token, new Date(now).toISOString(), row.outbox_id);
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch {}
      throw error;
    }
    const items = this.db.prepare(`SELECT outbox_id,memory_id,provider_id,operation,payload_json,payload_hash,attempts
      FROM memory_backend_outbox WHERE claim_token=? ORDER BY created_at,outbox_id`).all(token) as any[];
    return { claimToken: token, items: items.map(row => ({
      outboxId: row.outbox_id, memoryId: row.memory_id, providerId: row.provider_id, operation: row.operation,
      payload: JSON.parse(row.payload_json), payloadHash: row.payload_hash, attempts: row.attempts,
    })) };
  }

  settleMemoryBackendOutboxItem(input: {
    outboxId: string; claimToken: string; providerVersion: string; writeState: MemoryBackendBinding['writeState'];
    contentHash: string; backendRef?: string; now?: number;
  }): void {
    const nowMs = input.now ?? Date.now(); const now = new Date(nowMs).toISOString();
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const row = this.db.prepare(`SELECT memory_id,provider_id FROM memory_backend_outbox WHERE outbox_id=? AND claim_token=? AND status='inflight'`)
        .get(input.outboxId, input.claimToken) as { memory_id: string; provider_id: string } | undefined;
      if (!row) throw new Error('km_memory_backend_outbox_claim_lost');
      this.db.prepare(`UPDATE memory_backend_outbox SET status='delivered',claim_token=NULL,claimed_at=NULL,last_error=NULL,updated_at=? WHERE outbox_id=?`)
        .run(now, input.outboxId);
      this.db.prepare(`INSERT INTO memory_backend_bindings(
        memory_id,provider_id,provider_version,backend_ref,write_state,content_hash,last_verified_at,last_error,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(memory_id,provider_id) DO UPDATE SET
        provider_version=excluded.provider_version,backend_ref=excluded.backend_ref,write_state=excluded.write_state,
        content_hash=excluded.content_hash,last_verified_at=excluded.last_verified_at,last_error=NULL,updated_at=excluded.updated_at`)
        .run(row.memory_id, row.provider_id, input.providerVersion, input.backendRef ?? null, input.writeState,
          input.contentHash, input.writeState === 'active' || input.writeState === 'shadow' ? now : null, null, now);
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch {}
      throw error;
    }
  }

  failMemoryBackendOutboxItem(input: { outboxId: string; claimToken: string; error: string; retry: boolean; now?: number; maxAttempts?: number }): void {
    const nowMs = input.now ?? Date.now(); const now = new Date(nowMs).toISOString();
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const row = this.db.prepare(`SELECT memory_id,provider_id,attempts,payload_json FROM memory_backend_outbox WHERE outbox_id=? AND claim_token=? AND status='inflight'`)
        .get(input.outboxId, input.claimToken) as { memory_id: string; provider_id: string; attempts: number; payload_json: string } | undefined;
      if (!row) throw new Error('km_memory_backend_outbox_claim_lost');
      const maxAttempts = Math.max(1, input.maxAttempts ?? 5);
      const retry = input.retry && row.attempts < maxAttempts;
      const status = retry ? 'failed' : 'quarantined';
      const delay = Math.min(15 * 60_000, 1_000 * 2 ** Math.max(0, row.attempts - 1));
      const message = input.error.slice(0, 500);
      this.db.prepare(`UPDATE memory_backend_outbox SET status=?,claim_token=NULL,claimed_at=NULL,last_error=?,next_attempt_at=?,updated_at=? WHERE outbox_id=?`)
        .run(status, message, retry ? nowMs + delay : Number.MAX_SAFE_INTEGER, now, input.outboxId);
      let contentHash = sha256(row.payload_json);
      try {
        const payload = JSON.parse(row.payload_json) as { contentHash?: unknown };
        if (typeof payload.contentHash === 'string' && payload.contentHash.trim()) contentHash = payload.contentHash;
      } catch { /* keep payload hash fallback */ }
      this.db.prepare(`INSERT INTO memory_backend_bindings(
        memory_id,provider_id,provider_version,backend_ref,write_state,content_hash,last_verified_at,last_error,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(memory_id,provider_id) DO UPDATE SET
        write_state='failed',last_error=excluded.last_error,updated_at=excluded.updated_at`)
        .run(row.memory_id, row.provider_id, 'unknown', null, 'failed', contentHash, null, message, now);
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch {}
      throw error;
    }
  }

  listMemoryBackendOutbox(limit: number): MemoryBackendOutboxRow[] {
    return (this.db.prepare(`SELECT * FROM memory_backend_outbox ORDER BY created_at DESC,outbox_id DESC LIMIT ?`)
      .all(Math.max(1, Math.min(limit, 500))) as any[]).map(row => ({
        outboxId: row.outbox_id, memoryId: row.memory_id, providerId: row.provider_id, operation: row.operation,
        payload: JSON.parse(row.payload_json), payloadHash: row.payload_hash, status: row.status, attempts: row.attempts,
        nextAttemptAt: row.next_attempt_at, ...(row.claimed_at ? { claimedAt: row.claimed_at } : {}),
        ...(row.last_error ? { lastError: row.last_error } : {}), createdAt: row.created_at, updatedAt: row.updated_at,
      }));
  }

  listMemoryForBackendMigration(input: { afterMemoryId?: string; limit: number }): MemoryItem[] {
    const limit = Math.max(1, Math.min(input.limit, 500));
    const rows = input.afterMemoryId
      ? this.db.prepare(`SELECT * FROM memory_items WHERE memory_id>? AND state IN ('proposed','active','stale','conflicted','shadowed','expired')
          ORDER BY memory_id ASC LIMIT ?`).all(input.afterMemoryId, limit) as any[]
      : this.db.prepare(`SELECT * FROM memory_items WHERE state IN ('proposed','active','stale','conflicted','shadowed','expired')
          ORDER BY memory_id ASC LIMIT ?`).all(limit) as any[];
    return rows.map(row => this.memoryFromRow(row));
  }

  getMemoryBackendMigration(migrationId: string): MemoryBackendMigrationSnapshot | null {
    const row = this.db.prepare(`SELECT * FROM memory_backend_migrations WHERE migration_id=?`).get(migrationId) as any;
    return row ? this.memoryBackendMigrationFromRow(row) : null;
  }

  listMemoryBackendMigrations(limit: number): MemoryBackendMigrationSnapshot[] {
    return (this.db.prepare(`SELECT * FROM memory_backend_migrations ORDER BY created_at DESC,migration_id DESC LIMIT ?`)
      .all(Math.max(1, Math.min(limit, 500))) as any[]).map(row => this.memoryBackendMigrationFromRow(row));
  }

  compareMemoryBackendBindings(input: { fromProviderId: string; toProviderId: string; sampleLimit?: number }): MemoryBackendBindingCompareReport {
    const rows = this.db.prepare(`SELECT f.memory_id,f.content_hash from_hash,t.content_hash to_hash,t.write_state to_state
      FROM memory_backend_bindings f LEFT JOIN memory_backend_bindings t ON t.memory_id=f.memory_id AND t.provider_id=?
      WHERE f.provider_id=? AND f.write_state IN ('active','shadow') ORDER BY f.memory_id ASC`).all(input.toProviderId, input.fromProviderId) as any[];
    const samples: MemoryBackendBindingCompareReport['samples'] = [];
    let matched = 0; let missing = 0; let mismatched = 0;
    const sampleLimit = Math.max(0, Math.min(input.sampleLimit ?? 20, 100));
    for (const row of rows) {
      let reason: MemoryBackendBindingCompareReport['samples'][number]['reason'] | undefined;
      if (!row.to_state) { missing += 1; reason = 'missing'; }
      else if (row.to_state !== 'active' && row.to_state !== 'shadow') { mismatched += 1; reason = 'state_not_active'; }
      else if (row.from_hash !== row.to_hash) { mismatched += 1; reason = 'content_hash_mismatch'; }
      else matched += 1;
      if (reason && samples.length < sampleLimit) samples.push({ memoryId: row.memory_id, reason,
        ...(row.from_hash ? { fromContentHash: row.from_hash } : {}), ...(row.to_hash ? { toContentHash: row.to_hash } : {}),
        ...(row.to_state ? { toState: row.to_state } : {}) });
    }
    return { fromProviderId: input.fromProviderId, toProviderId: input.toProviderId,
      compared: rows.length, matched, missing, mismatched, samples };
  }

  createMemoryBackendMigration(input: { botAppId: string; fromProfile: Record<string, unknown>; toProfile: Record<string, unknown> }): string {
    const migrationId = `mmig_${randomUUID().replaceAll('-', '')}`; const now = new Date().toISOString();
    this.db.prepare(`INSERT INTO memory_backend_migrations(migration_id,bot_app_id,from_profile_json,to_profile_json,state,stats_json,created_at,updated_at)
      VALUES(?,?,?,?,'draft','{}',?,?)`).run(migrationId, requireText(input.botAppId, 'migration_bot'), JSON.stringify(input.fromProfile), JSON.stringify(input.toProfile), now, now);
    return migrationId;
  }

  transitionMemoryBackendMigration(input: { migrationId: string; toState: 'backfilling' | 'comparing' | 'ready' | 'cutover' | 'rolled_back' | 'failed'; checkpoint?: string; stats?: Record<string, unknown> }): void {
    const row = this.db.prepare('SELECT state FROM memory_backend_migrations WHERE migration_id=?').get(input.migrationId) as { state: string } | undefined;
    if (!row) throw new Error('km_memory_migration_not_found');
    const allowed: Record<string, string[]> = { draft: ['backfilling','failed'], backfilling: ['backfilling','comparing','failed'], comparing: ['ready','failed'], ready: ['cutover','failed'], cutover: ['rolled_back'], rolled_back: [], failed: ['backfilling'] };
    if (!allowed[row.state]?.includes(input.toState)) throw new Error(`km_memory_migration_invalid_transition:${row.state}:${input.toState}`);
    this.db.prepare(`UPDATE memory_backend_migrations SET state=?,checkpoint=?,stats_json=?,updated_at=? WHERE migration_id=?`)
      .run(input.toState, input.checkpoint ?? null, JSON.stringify(input.stats ?? {}), new Date().toISOString(), input.migrationId);
  }

  recordRetrievalAudit(input: {
    botAppId: string; sessionId: string; turnId?: string; queryHash: string;
    mode: 'off' | 'shadow' | 'canary' | 'active'; candidateCount: number; eligibleCount: number;
    latencyMs: number; warnings: string[];
    results: Array<{ itemId: string; itemKind: string; providerIds: string[]; score: number; eligible: boolean; filterReason?: string }>;
  }): string {
    const id = `retr_${randomUUID().replaceAll('-', '')}`; const now = new Date().toISOString();
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`INSERT INTO retrieval_runs(retrieval_run_id,bot_app_id,session_id,turn_id,query_hash,mode,candidate_count,eligible_count,latency_ms,warnings_json,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)`).run(id, input.botAppId, input.sessionId, input.turnId ?? null, input.queryHash, input.mode,
        input.candidateCount, input.eligibleCount, input.latencyMs, JSON.stringify(input.warnings), now);
      const insert = this.db.prepare(`INSERT INTO retrieval_results(retrieval_run_id,item_id,item_kind,provider_ids_json,score,eligible,filter_reason)
        VALUES(?,?,?,?,?,?,?)`);
      for (const result of input.results) insert.run(id, result.itemId, result.itemKind, JSON.stringify(result.providerIds), result.score,
        result.eligible ? 1 : 0, result.filterReason ?? null);
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
    return id;
  }

  recordPromptInjectionSnapshot(input: {
    retrievalRunId: string; botAppId: string; mode: string;
    disposition: 'off' | 'would_inject' | 'injected' | 'skipped'; itemIds: string[];
    prompt?: string; reason?: string;
  }): string {
    const id = `inject_${randomUUID().replaceAll('-', '')}`; const prompt = input.prompt ?? '';
    this.db.prepare(`INSERT INTO prompt_injection_snapshots(snapshot_id,retrieval_run_id,bot_app_id,mode,disposition,item_ids_json,prompt_hash,prompt_bytes,reason,created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?)`).run(id, input.retrievalRunId, input.botAppId, input.mode, input.disposition,
      JSON.stringify(input.itemIds), prompt ? sha256(prompt) : null, Buffer.byteLength(prompt), input.reason ?? null, new Date().toISOString());
    return id;
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

  transitionMemory(input: {
    memoryId: string;
    toState: MemoryState;
    reasonCode: string;
    actorId: string;
    evidenceEventId?: string;
  }): MemoryItem {
    const current = this.getMemory(input.memoryId);
    if (!current) throw new Error('km_memory_not_found');
    if (!MEMORY_TRANSITIONS[current.state].includes(input.toState)) {
      throw new Error(`km_memory_invalid_transition:${current.state}:${input.toState}`);
    }
    if (input.toState === 'active') {
      if (!input.actorId.trim() || input.actorId === 'system') throw new Error('km_memory_activation_requires_human_review');
      if (current.confidence === 'inferred') throw new Error('km_memory_inferred_requires_human_review');
      if (current.privacyClass === 'sensitive' || current.privacyClass === 'secret-reference-only') {
        throw new Error('km_memory_privacy_not_activatable');
      }
    }
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_memory_transition;');
    try {
      this.db.prepare('UPDATE memory_items SET state=?,updated_at=? WHERE memory_id=?')
        .run(input.toState, now, input.memoryId);
      this.db.prepare(`
        INSERT INTO memory_state_history(history_id,memory_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
        VALUES(?,?,?,?,?,?,?,?)
      `).run(kmId('hist'), input.memoryId, current.state, input.toState,
        requireText(input.reasonCode, 'memory_reason'), requireText(input.actorId, 'actor_id'), input.evidenceEventId ?? null, now);
      this.db.exec('RELEASE km_memory_transition;');
    } catch (error) {
      try { this.db.exec('ROLLBACK TO km_memory_transition; RELEASE km_memory_transition;'); } catch { /* savepoint may already be closed */ }
      throw error;
    }
    return this.getMemory(input.memoryId)!;
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

  addTraceEdge(input: TraceEdgeInput): { edgeId: string; created: boolean } {
    const edgeId = kmId('edge');
    const result = this.db.prepare(`
      INSERT OR IGNORE INTO trace_edges(edge_id,from_type,from_id,to_type,to_id,edge_type,evidence_event_id,created_at)
      VALUES(?,?,?,?,?,?,?,?)
    `).run(edgeId, requireText(input.fromType, 'trace_from_type'), requireText(input.fromId, 'trace_from_id'),
      requireText(input.toType, 'trace_to_type'), requireText(input.toId, 'trace_to_id'), input.edgeType,
      input.evidenceEventId ?? null, new Date().toISOString());
    if (Number(result.changes) === 1) return { edgeId, created: true };
    const existing = this.db.prepare(`SELECT edge_id FROM trace_edges WHERE from_type=? AND from_id=? AND to_type=? AND to_id=? AND edge_type=?`)
      .get(input.fromType, input.fromId, input.toType, input.toId, input.edgeType) as { edge_id: string };
    return { edgeId: existing.edge_id, created: false };
  }

  listTrace(input: { type: string; id: string; limit: number }): Array<Record<string, unknown>> {
    const rows = this.db.prepare(`
      SELECT * FROM trace_edges WHERE (from_type=? AND from_id=?) OR (to_type=? AND to_id=?)
      ORDER BY created_at DESC,edge_id DESC LIMIT ?
    `).all(input.type, input.id, input.type, input.id, Math.max(1, Math.min(input.limit, 500))) as any[];
    return rows.map(row => ({ edgeId: row.edge_id, fromType: row.from_type, fromId: row.from_id,
      toType: row.to_type, toId: row.to_id, edgeType: row.edge_type,
      ...(row.evidence_event_id ? { evidenceEventId: row.evidence_event_id } : {}), createdAt: row.created_at }));
  }

  recordEval(input: {
    evaluatorName: string; evaluatorVersion: string;
    targetType: 'turn' | 'workflow-artifact' | 'knowledge' | 'memory' | 'skill' | 'sync-batch' | 'proposal';
    targetId: string; results: EvalResultInput[];
  }): { evalRunId: string; created: boolean } {
    if (input.results.length === 0) throw new Error('km_eval_results_required');
    if (input.results.some(result => result.sourceRefs.length === 0)) throw new Error('km_eval_source_refs_required');
    const existing = this.db.prepare(`SELECT eval_run_id FROM eval_runs WHERE evaluator_name=? AND evaluator_version=? AND target_type=? AND target_id=?`)
      .get(input.evaluatorName, input.evaluatorVersion, input.targetType, input.targetId) as { eval_run_id: string } | undefined;
    if (existing) return { evalRunId: existing.eval_run_id, created: false };
    const now = new Date().toISOString();
    const evalRunId = kmId('eval');
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`INSERT INTO eval_runs(eval_run_id,state,evaluator_name,evaluator_version,target_type,target_id,started_at,completed_at,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)`).run(evalRunId, 'accepted', requireText(input.evaluatorName, 'evaluator_name'),
        requireText(input.evaluatorVersion, 'evaluator_version'), input.targetType, requireText(input.targetId, 'eval_target_id'), now, now, now, now);
      const insert = this.db.prepare(`INSERT INTO eval_results(eval_result_id,eval_run_id,metric_key,score,verdict,confidence,details_json,source_refs_json,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)`);
      for (const result of input.results) insert.run(kmId('result'), evalRunId, requireText(result.metricKey, 'metric_key'), result.score ?? null,
        result.verdict, result.confidence, JSON.stringify(result.details ?? {}), JSON.stringify(result.sourceRefs), now);
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch { /* closed */ } throw error; }
    return { evalRunId, created: true };
  }

  listEvalRuns(limit: number): Array<Record<string, unknown>> {
    const rows = this.db.prepare(`
      SELECT r.*,COUNT(x.eval_result_id) result_count,
        SUM(CASE WHEN x.verdict='pass' THEN 1 ELSE 0 END) pass_count,
        SUM(CASE WHEN x.verdict='warn' THEN 1 ELSE 0 END) warn_count,
        SUM(CASE WHEN x.verdict='fail' THEN 1 ELSE 0 END) fail_count
      FROM eval_runs r LEFT JOIN eval_results x ON x.eval_run_id=r.eval_run_id
      GROUP BY r.eval_run_id ORDER BY r.updated_at DESC,r.eval_run_id DESC LIMIT ?
    `).all(Math.max(1, Math.min(limit, 500))) as any[];
    return rows.map(row => ({ evalRunId: row.eval_run_id, state: row.state,
      evaluatorName: row.evaluator_name, evaluatorVersion: row.evaluator_version,
      targetType: row.target_type, targetId: row.target_id,
      resultCount: Number(row.result_count), passCount: Number(row.pass_count ?? 0),
      warnCount: Number(row.warn_count ?? 0), failCount: Number(row.fail_count ?? 0),
      createdAt: row.created_at, updatedAt: row.updated_at }));
  }

  createEvolutionProposal(input: EvolutionProposalInput): string {
    if (input.evidenceRefs.length === 0) throw new Error('km_evolution_evidence_required');
    const now = new Date().toISOString();
    const proposalId = kmId('evo');
    this.db.prepare(`INSERT INTO evolution_proposals(
      proposal_id,state,proposal_type,target_ref,approval_grade,summary,evidence_refs_json,
      proposed_action_json,risk_json,rollback_json,created_by,created_at,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(proposalId, 'review_pending', input.proposalType,
      requireText(input.targetRef, 'proposal_target'), input.approvalGrade, requireText(input.summary, 'proposal_summary'),
      JSON.stringify(input.evidenceRefs), JSON.stringify(input.proposedAction), JSON.stringify(input.risk),
      JSON.stringify(input.rollback), requireText(input.createdBy, 'proposal_creator'), now, now);
    return proposalId;
  }

  decideProposal(input: {
    proposalId: string; decision: 'approved' | 'rejected' | 'request_changes';
    actorId: string; grade: ApprovalGrade; scope: Record<string, unknown>; riskAck?: Record<string, unknown>;
  }): { approvalId: string; state: string } {
    const proposal = this.db.prepare('SELECT * FROM evolution_proposals WHERE proposal_id=?').get(input.proposalId) as any;
    if (!proposal) throw new Error('km_evolution_proposal_not_found');
    if (proposal.state !== 'review_pending') throw new Error(`km_evolution_invalid_state:${proposal.state}`);
    const rank = (grade: ApprovalGrade) => Number(grade.slice(1));
    if (input.decision === 'approved' && rank(input.grade) < rank(proposal.approval_grade)) throw new Error('km_approval_grade_insufficient');
    const approvalId = kmId('approval');
    const now = new Date().toISOString();
    const state = input.decision === 'approved' ? 'approved' : input.decision === 'rejected' ? 'rejected' : 'draft';
    this.db.exec('SAVEPOINT km_proposal_decision;');
    try {
      this.db.prepare(`INSERT INTO approval_decisions(approval_id,proposal_id,grade,decision,actor_id,scope_json,risk_ack_json,created_at)
        VALUES(?,?,?,?,?,?,?,?)`).run(approvalId, input.proposalId, input.grade, input.decision,
        requireText(input.actorId, 'approval_actor'), JSON.stringify(input.scope), JSON.stringify(input.riskAck ?? {}), now);
      this.db.prepare('UPDATE evolution_proposals SET state=?,approved_by=?,updated_at=? WHERE proposal_id=?')
        .run(state, input.decision === 'approved' ? input.actorId : null, now, input.proposalId);
      this.db.exec('RELEASE km_proposal_decision;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_proposal_decision; RELEASE km_proposal_decision;'); } catch { /* closed */ } throw error; }
    return { approvalId, state };
  }

  listEvolution(limit: number): Array<Record<string, unknown>> {
    const rows = this.db.prepare('SELECT * FROM evolution_proposals ORDER BY updated_at DESC,proposal_id DESC LIMIT ?')
      .all(Math.max(1, Math.min(limit, 500))) as any[];
    return rows.map(row => ({ proposalId: row.proposal_id, state: row.state, proposalType: row.proposal_type,
      targetRef: row.target_ref, approvalGrade: row.approval_grade, summary: row.summary,
      evidenceRefs: parseJsonArray(row.evidence_refs_json), proposedAction: JSON.parse(row.proposed_action_json),
      risk: JSON.parse(row.risk_json), rollback: JSON.parse(row.rollback_json), createdBy: row.created_by,
      ...(row.approved_by ? { approvedBy: row.approved_by } : {}), createdAt: row.created_at, updatedAt: row.updated_at }));
  }

  configureSyncSink(input: SyncSinkInput): SyncStatus {
    const now = new Date().toISOString();
    if (!input.endpointRef.startsWith('mock://') && input.enabled) throw new Error('km_sync_real_sink_requires_explicit_external_approval');
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`INSERT INTO sync_sinks(sink_id,protocol_version,endpoint_ref,enabled,redaction_policy_json,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?) ON CONFLICT(sink_id) DO UPDATE SET protocol_version=excluded.protocol_version,
        endpoint_ref=excluded.endpoint_ref,enabled=excluded.enabled,redaction_policy_json=excluded.redaction_policy_json,updated_at=excluded.updated_at`)
        .run(requireText(input.sinkId, 'sink_id'), input.protocolVersion, requireText(input.endpointRef, 'sink_endpoint'), input.enabled ? 1 : 0,
          JSON.stringify(input.redactionPolicy ?? {}), now, now);
      this.db.prepare(`INSERT OR IGNORE INTO sync_cursors(sink_id,last_local_seq,status,updated_at) VALUES(?,0,'idle',?)`).run(input.sinkId, now);
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
    return this.listSyncStatus().find(item => item.sinkId === input.sinkId)!;
  }

  enqueueSync(input: { sinkId: string; eventId: string; payload: Record<string, unknown>; payloadHash: string; now?: number }): { outboxId: string; created: boolean } {
    const sink = this.db.prepare('SELECT enabled FROM sync_sinks WHERE sink_id=?').get(input.sinkId) as { enabled: number } | undefined;
    if (!sink) throw new Error('km_sync_sink_not_found');
    if (!sink.enabled) throw new Error('km_sync_sink_disabled');
    const outboxId = `outbox_${createHash('sha256').update(`${input.sinkId}:${input.eventId}`).digest('hex')}`;
    const now = new Date().toISOString();
    const result = this.db.prepare(`INSERT OR IGNORE INTO sync_outbox(outbox_id,event_id,sink_id,status,attempts,next_attempt_at,created_at)
      VALUES(?,?,?,'pending',0,?,?)`).run(outboxId, input.eventId, input.sinkId, input.now ?? Date.now(), now);
    if (Number(result.changes) === 1) this.db.prepare('UPDATE sync_outbox SET payload_json=?,payload_hash=? WHERE outbox_id=?')
      .run(JSON.stringify(input.payload), input.payloadHash, outboxId);
    return { outboxId, created: Number(result.changes) === 1 };
  }

  claimSyncBatch(input: { sinkId: string; limit: number; now?: number; leaseMs?: number }): { claimToken: string; items: Array<{ outboxId: string; eventId: string; payload: Record<string, unknown>; payloadHash: string }> } {
    const now = input.now ?? Date.now();
    const token = `claim_${randomUUID().replaceAll('-', '')}`;
    const leaseMs = Math.max(1_000, input.leaseMs ?? 30_000);
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`UPDATE sync_outbox SET status='failed',claim_token=NULL,claimed_at=NULL,last_error='claim_lease_expired',next_attempt_at=?
        WHERE sink_id=? AND status='inflight' AND claimed_at<?`).run(now, input.sinkId, now - leaseMs);
      const rows = this.db.prepare(`SELECT outbox_id FROM sync_outbox WHERE sink_id=? AND status IN ('pending','failed') AND next_attempt_at<=?
        ORDER BY created_at,outbox_id LIMIT ?`).all(input.sinkId, now, Math.max(1, Math.min(input.limit, 100))) as any[];
      const claim = this.db.prepare(`UPDATE sync_outbox SET status='inflight',attempts=attempts+1,claimed_at=?,claim_token=? WHERE outbox_id=?`);
      for (const row of rows) claim.run(now, token, row.outbox_id);
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
    const items = this.db.prepare(`SELECT outbox_id,event_id,payload_json,payload_hash FROM sync_outbox WHERE claim_token=? ORDER BY created_at,outbox_id`)
      .all(token) as any[];
    return { claimToken: token, items: items.map(row => ({ outboxId: row.outbox_id, eventId: row.event_id,
      payload: JSON.parse(row.payload_json ?? '{}'), payloadHash: String(row.payload_hash ?? '') })) };
  }

  failSyncClaim(input: { claimToken: string; error: string; now?: number; baseDelayMs?: number }): void {
    const now = input.now ?? Date.now();
    const rows = this.db.prepare(`SELECT outbox_id,attempts FROM sync_outbox WHERE claim_token=? AND status='inflight'`).all(input.claimToken) as any[];
    const update = this.db.prepare(`UPDATE sync_outbox SET status='failed',claim_token=NULL,claimed_at=NULL,last_error=?,next_attempt_at=? WHERE outbox_id=?`);
    for (const row of rows) {
      const delay = Math.min(300_000, (input.baseDelayMs ?? 1_000) * 2 ** Math.max(0, Number(row.attempts) - 1));
      update.run(input.error.slice(0, 500), now + delay, row.outbox_id);
    }
  }

  acknowledgeSync(input: { sinkId: string; batchId: string; acceptedEventIds: string[]; centralCursor?: string }): void {
    const now = new Date().toISOString();
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const delivered = this.db.prepare(`UPDATE sync_outbox SET status='delivered',delivered_at=? WHERE sink_id=? AND event_id=?`);
      for (const eventId of input.acceptedEventIds) delivered.run(now, input.sinkId, eventId);
      const row = this.db.prepare(`SELECT COALESCE(MAX(o.local_seq),0) max_seq FROM observation_events o JOIN sync_outbox x ON x.event_id=o.event_id WHERE x.sink_id=? AND x.status='delivered'`)
        .get(input.sinkId) as { max_seq: number };
      this.db.prepare(`UPDATE sync_cursors SET last_local_seq=?,last_batch_id=?,last_ack_at=?,central_cursor=?,status='idle',updated_at=? WHERE sink_id=?`)
        .run(Number(row.max_seq), input.batchId, now, input.centralCursor ?? null, now, input.sinkId);
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  quarantineSync(input: { sinkId: string; eventId?: string; reason: string; payloadHash: string }): string {
    const id = `syncq_${randomUUID().replaceAll('-', '')}`;
    this.db.prepare(`INSERT INTO sync_quarantine(quarantine_id,sink_id,event_id,reason,payload_hash,created_at) VALUES(?,?,?,?,?,?)`)
      .run(id, input.sinkId, input.eventId ?? null, requireText(input.reason, 'sync_quarantine_reason'), input.payloadHash, new Date().toISOString());
    if (input.eventId) this.db.prepare(`UPDATE sync_outbox SET status='quarantined',last_error=? WHERE sink_id=? AND event_id=?`)
      .run(input.reason, input.sinkId, input.eventId);
    return id;
  }

  listSyncStatus(): SyncStatus[] {
    const rows = this.db.prepare(`SELECT s.*,c.last_local_seq,c.last_batch_id,c.last_ack_at,c.central_cursor,c.status,
      (SELECT COUNT(*) FROM sync_outbox x WHERE x.sink_id=s.sink_id AND x.status IN ('pending','failed')) pending,
      (SELECT COUNT(*) FROM sync_quarantine q WHERE q.sink_id=s.sink_id AND q.resolved_at IS NULL) quarantined
      FROM sync_sinks s JOIN sync_cursors c ON c.sink_id=s.sink_id ORDER BY s.sink_id`).all() as any[];
    return rows.map(row => ({ sinkId: row.sink_id, endpointRef: row.endpoint_ref, enabled: Boolean(row.enabled), status: row.status,
      lastLocalSeq: Number(row.last_local_seq), ...(row.last_batch_id ? { lastBatchId: row.last_batch_id } : {}),
      ...(row.last_ack_at ? { lastAckAt: row.last_ack_at } : {}), ...(row.central_cursor ? { centralCursor: row.central_cursor } : {}),
      pending: Number(row.pending), quarantined: Number(row.quarantined) }));
  }

  registerKmProvider(descriptorInput: KmProviderDescriptor): void {
    const descriptor = KmProviderDescriptorSchema.parse(descriptorInput);
    this.db.prepare(`INSERT INTO km_provider_registry(provider_id,provider_kind,provider_version,descriptor_json,status,updated_at)
      VALUES(?,?,?,?,?,?) ON CONFLICT(provider_id,provider_version) DO UPDATE SET descriptor_json=excluded.descriptor_json,
      provider_kind=excluded.provider_kind,status=excluded.status,updated_at=excluded.updated_at
      WHERE descriptor_json<>excluded.descriptor_json OR provider_kind<>excluded.provider_kind OR status<>excluded.status`)
      .run(descriptor.id, descriptor.kind, descriptor.version, JSON.stringify(descriptor), 'validated', new Date().toISOString());
  }

  executeKmMutation<T>(input: { actorId: string; idempotencyKey: string; route: string; requestHash: string; statusCode: number;
    action: string; targetRef: string; beforeHash?: string; afterHash?: (response: T) => string | undefined }, operation: () => T): { statusCode: number; response: T; replayed: boolean } {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const row = this.db.prepare(`SELECT route,request_hash,status_code,response_json FROM km_mutation_idempotency WHERE actor_id=? AND idempotency_key=?`)
        .get(input.actorId, input.idempotencyKey) as any;
      if (row) {
        if (row.route !== input.route || row.request_hash !== input.requestHash) throw new Error('km_idempotency_conflict');
        this.db.exec('COMMIT;');
        return { statusCode: Number(row.status_code), response: JSON.parse(row.response_json) as T, replayed: true };
      }
      const response = operation(); const now = new Date().toISOString();
      this.db.prepare(`INSERT INTO km_mutation_idempotency(actor_id,idempotency_key,route,request_hash,status_code,response_json,created_at)
        VALUES(?,?,?,?,?,?,?)`).run(input.actorId, input.idempotencyKey, input.route, input.requestHash, input.statusCode, JSON.stringify(response), now);
      this.db.prepare(`INSERT INTO km_config_audit(audit_id,actor_id,action,target_ref,before_hash,after_hash,request_hash,idempotency_key,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(`kma_${randomUUID().replaceAll('-', '')}`, input.actorId, input.action, input.targetRef,
        input.beforeHash ?? null, input.afterHash?.(response) ?? null, input.requestHash, input.idempotencyKey, now);
      this.db.exec('COMMIT;');
      return { statusCode: input.statusCode, response, replayed: false };
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  listKmConfigAudit(limit: number): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT * FROM km_config_audit ORDER BY created_at DESC,audit_id DESC LIMIT ?`).all(Math.max(1,Math.min(limit,500))) as any[])
      .map(row => ({ auditId: row.audit_id, actorId: row.actor_id, action: row.action, targetRef: row.target_ref,
        ...(row.before_hash ? { beforeHash: row.before_hash } : {}), ...(row.after_hash ? { afterHash: row.after_hash } : {}), createdAt: row.created_at }));
  }

  putPipelineProfile(profileInput: KmPipelineProfile, state: 'draft' | 'shadow' | 'active' = 'draft'): string {
    const profile = KmPipelineProfileSchema.parse(profileInput);
    const json = JSON.stringify(profile);
    const hash = sha256(json);
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_profile_put;');
    try {
      if (state === 'active') this.db.prepare(`UPDATE km_pipeline_profiles SET state='retired' WHERE bot_app_id=? AND state='active'`).run(profile.botAppId);
      if (state === 'shadow') this.db.prepare(`UPDATE km_pipeline_profiles SET state='retired' WHERE bot_app_id=? AND state='shadow'`).run(profile.botAppId);
      this.db.prepare(`INSERT INTO km_pipeline_profiles(profile_id,revision,bot_app_id,profile_json,profile_hash,state,created_at)
        VALUES(?,?,?,?,?,?,?)`).run(profile.profileId, profile.revision, profile.botAppId, json, hash, state, now);
      this.db.exec('RELEASE km_profile_put;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_profile_put; RELEASE km_profile_put;'); } catch {} throw error; }
    return hash;
  }

  listPipelineProfiles(botAppId?: string): Array<Record<string, unknown>> {
    const rows = (botAppId
      ? this.db.prepare(`SELECT * FROM km_pipeline_profiles WHERE bot_app_id=? ORDER BY created_at DESC`).all(botAppId)
      : this.db.prepare(`SELECT * FROM km_pipeline_profiles ORDER BY created_at DESC`).all()) as any[];
    return rows.map(row => { const profile = KmPipelineProfileSchema.parse(JSON.parse(row.profile_json)); return {
      profile, profileHash: row.profile_hash, state: row.state, requestedMode: profile.injectionMode,
      effectiveMode: profile.injectionMode === 'off' ? 'off' : 'shadow', createdAt: row.created_at,
    }; });
  }

  setPipelineProfileState(input: { profileId: string; revision: number; state: 'draft' | 'shadow' | 'active' | 'retired'; expectedHash?: string }): Record<string, unknown> {
    const row = this.db.prepare(`SELECT bot_app_id,state,profile_hash FROM km_pipeline_profiles WHERE profile_id=? AND revision=?`).get(input.profileId, input.revision) as any;
    if (!row) throw new Error('km_pipeline_profile_not_found');
    if (input.expectedHash && input.expectedHash !== row.profile_hash) throw new Error('km_pipeline_profile_version_conflict');
    const allowed: Record<string, string[]> = { draft: ['shadow','retired'], shadow: ['draft','retired'], active: ['retired'], retired: [] };
    if (row.state !== input.state && !allowed[row.state]?.includes(input.state)) throw new Error(`km_pipeline_profile_invalid_transition:${row.state}:${input.state}`);
    this.db.exec('SAVEPOINT km_profile_state;');
    try {
      if (input.state === 'shadow') this.db.prepare(`UPDATE km_pipeline_profiles SET state='retired' WHERE bot_app_id=? AND state='shadow' AND NOT (profile_id=? AND revision=?)`)
        .run(row.bot_app_id,input.profileId,input.revision);
      this.db.prepare(`UPDATE km_pipeline_profiles SET state=? WHERE profile_id=? AND revision=? AND profile_hash=?`).run(input.state,input.profileId,input.revision,row.profile_hash);
      this.db.exec('RELEASE km_profile_state;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_profile_state; RELEASE km_profile_state;'); } catch {} throw error; }
    return this.listPipelineProfiles(row.bot_app_id).find(value => (value.profile as KmPipelineProfile).profileId === input.profileId
      && (value.profile as KmPipelineProfile).revision === input.revision)!;
  }

  getEffectivePipelineProfile(botAppId: string): KmPipelineProfile | undefined {
    const row = this.db.prepare(`SELECT profile_json FROM km_pipeline_profiles WHERE bot_app_id=? AND state IN ('active','shadow')
      ORDER BY CASE state WHEN 'active' THEN 0 ELSE 1 END,created_at DESC LIMIT 1`).get(botAppId) as any;
    return row ? KmPipelineProfileSchema.parse(JSON.parse(row.profile_json)) : undefined;
  }

  putMemoryProviderConfig(input: KmMemoryProviderConfig): string {
    const config = KmMemoryProviderConfigSchema.parse(input);
    const json = JSON.stringify(config); const hash = sha256(json);
    this.db.prepare(`INSERT INTO km_memory_provider_configs(provider_id,config_json,config_hash,updated_at) VALUES(?,?,?,?)
      ON CONFLICT(provider_id) DO UPDATE SET config_json=excluded.config_json,config_hash=excluded.config_hash,updated_at=excluded.updated_at`)
      .run(config.providerId, json, hash, new Date().toISOString());
    return hash;
  }

  listMemoryProviderConfigs(): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT provider_id,config_json,config_hash,updated_at FROM km_memory_provider_configs ORDER BY provider_id`).all() as any[])
      .map(row => { const config = KmMemoryProviderConfigSchema.parse(JSON.parse(row.config_json)); return {
        ...config, credentialRef: config.credentialRef.replace(/^(env|file):(.+)$/, (_m, kind) => `${kind}:***`),
        configHash: row.config_hash, updatedAt: row.updated_at,
      }; });
  }

  memoryProviderConfigurationHealth(providerId: string, env: NodeJS.ProcessEnv = process.env): Record<string, unknown> {
    const row = this.db.prepare(`SELECT config_json,updated_at FROM km_memory_provider_configs WHERE provider_id=?`).get(providerId) as any;
    if (!row) throw new Error('km_memory_provider_config_not_found');
    const config = KmMemoryProviderConfigSchema.parse(JSON.parse(row.config_json));
    const kind = config.credentialRef.startsWith('env:') ? 'env' : 'file';
    const value = config.credentialRef.slice(kind.length + 1);
    let credentialAvailable = false;
    if (kind === 'env') credentialAvailable = Boolean(env[value]?.trim());
    else try {
      const allowedRoot = resolve(env.BOTMUX_KM_SECRET_DIR?.trim() || join(homedir(), '.botmux', 'secrets'));
      const candidate = resolve(value); const insideAllowedRoot = candidate === allowedRoot || candidate.startsWith(`${allowedRoot}/`);
      const stat = lstatSync(candidate); credentialAvailable = insideAllowedRoot && stat.isFile() && !stat.isSymbolicLink() && (stat.mode & 0o077) === 0;
    } catch { credentialAvailable = false; }
    return { providerId: config.providerId, status: !config.enabled ? 'disabled' : credentialAvailable ? 'configuration_ready' : 'credential_missing',
      endpointValid: true, credentialAvailable, transportChecked: false, realTransportEnabled: false, updatedAt: row.updated_at };
  }

  recordMemoryPolicyDecision(input: { sourceEventId: string; memoryId?: string; policyVersion: string;
    disposition: 'reject' | 'propose' | 'activate'; reasonCodes: string[]; evidence: Record<string, unknown> }): string {
    const evidenceJson = JSON.stringify(input.evidence); const id = `mpd_${createHash('sha256').update(`${input.sourceEventId}|${input.policyVersion}|${evidenceJson}`).digest('hex')}`;
    this.db.prepare(`INSERT OR IGNORE INTO km_memory_policy_decisions(decision_id,source_event_id,memory_id,policy_version,disposition,reason_codes_json,evidence_json,created_at)
      VALUES(?,?,?,?,?,?,?,?)`).run(id,input.sourceEventId,input.memoryId ?? null,input.policyVersion,input.disposition,JSON.stringify(input.reasonCodes),evidenceJson,new Date().toISOString());
    return id;
  }

  listMemoryPolicyDecisions(limit: number): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT * FROM km_memory_policy_decisions ORDER BY created_at DESC,decision_id DESC LIMIT ?`).all(Math.max(1,Math.min(limit,500))) as any[])
      .map(row => ({ decisionId: row.decision_id, sourceEventId: row.source_event_id, ...(row.memory_id ? { memoryId: row.memory_id } : {}),
        policyVersion: row.policy_version, disposition: row.disposition, reasonCodes: JSON.parse(row.reason_codes_json), evidence: JSON.parse(row.evidence_json), createdAt: row.created_at }));
  }

  createDistillationJob(input: { sourceEventId: string; profile: KmPipelineProfile; evidenceContext?: Record<string, unknown>; now?: number }): { jobId: string; created: boolean } {
    const profile = KmPipelineProfileSchema.parse(input.profile);
    const profileJson = JSON.stringify(profile);
    const key = `${input.sourceEventId}|${profile.profileId}|${profile.revision}|${profile.primaryExtractor}`;
    const jobId = `distill_${createHash('sha256').update(key).digest('hex')}`;
    const nowMs = input.now ?? Date.now();
    const now = new Date(nowMs).toISOString();
    const result = this.db.prepare(`INSERT OR IGNORE INTO distillation_jobs(
      job_id,idempotency_key,source_event_id,bot_app_id,profile_id,profile_revision,profile_snapshot_json,evidence_context_json,state,next_attempt_at,created_at,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,'queued',?,?,?)`).run(jobId, key, input.sourceEventId, profile.botAppId, profile.profileId, profile.revision,
      profileJson, JSON.stringify(input.evidenceContext ?? {}), nowMs, now, now);
    return { jobId, created: Number(result.changes) === 1 };
  }

  claimDistillationJob(input: { now?: number; leaseMs?: number }): null | { jobId: string; claimToken: string; sourceEventId: string; profile: KmPipelineProfile; evidenceContext: Record<string, unknown> } {
    const now = input.now ?? Date.now();
    const lease = Math.max(1_000, input.leaseMs ?? 60_000);
    const token = `dclaim_${randomUUID().replaceAll('-', '')}`;
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`UPDATE distillation_jobs SET state='retry_wait',claim_token=NULL,claimed_at=NULL,last_error='claim_lease_expired',next_attempt_at=?,updated_at=?
        WHERE claim_token IS NOT NULL AND claimed_at<? AND state NOT IN ('completed','failed','quarantined','cancelled')`)
        .run(now, new Date(now).toISOString(), now - lease);
      const row = this.db.prepare(`SELECT job_id FROM distillation_jobs WHERE state IN ('queued','retry_wait') AND next_attempt_at<=?
        ORDER BY created_at,job_id LIMIT 1`).get(now) as { job_id: string } | undefined;
      if (!row) { this.db.exec('COMMIT;'); return null; }
      this.db.prepare(`UPDATE distillation_jobs SET state='resolving',attempts=attempts+1,claimed_at=?,claim_token=?,updated_at=? WHERE job_id=?`)
        .run(now, token, new Date(now).toISOString(), row.job_id);
      this.db.exec('COMMIT;');
      const claimed = this.db.prepare(`SELECT source_event_id,profile_snapshot_json,evidence_context_json FROM distillation_jobs WHERE job_id=?`).get(row.job_id) as any;
      return { jobId: row.job_id, claimToken: token, sourceEventId: claimed.source_event_id,
        profile: KmPipelineProfileSchema.parse(JSON.parse(claimed.profile_snapshot_json)), evidenceContext: JSON.parse(claimed.evidence_context_json ?? '{}') };
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  finishDistillationJob(input: { jobId: string; claimToken: string; outputHash: string; state?: 'completed' | 'inconclusive' }): void {
    const result = this.db.prepare(`UPDATE distillation_jobs SET state=?,output_hash=?,claim_token=NULL,claimed_at=NULL,updated_at=?
      WHERE job_id=? AND claim_token=?`).run(input.state ?? 'completed', input.outputHash, new Date().toISOString(), input.jobId, input.claimToken);
    if (Number(result.changes) !== 1) throw new Error('km_distillation_claim_lost');
  }

  failDistillationJob(input: { jobId: string; claimToken: string; error: string; retry: boolean; now?: number }): void {
    const now = input.now ?? Date.now();
    const row = this.db.prepare(`SELECT attempts FROM distillation_jobs WHERE job_id=? AND claim_token=?`).get(input.jobId, input.claimToken) as { attempts: number } | undefined;
    if (!row) throw new Error('km_distillation_claim_lost');
    const state = input.retry && row.attempts < 3 ? 'retry_wait' : 'failed';
    const delay = Math.min(300_000, 1_000 * 2 ** Math.max(0, row.attempts - 1));
    this.db.prepare(`UPDATE distillation_jobs SET state=?,next_attempt_at=?,claim_token=NULL,claimed_at=NULL,last_error=?,updated_at=? WHERE job_id=?`)
      .run(state, now + delay, input.error.slice(0, 500), new Date(now).toISOString(), input.jobId);
  }

  listKmProviders(): Array<Record<string, unknown>> {
    this.seedBuiltinKmProvidersBestEffort();
    return (this.db.prepare(`SELECT provider_id,provider_kind,provider_version,descriptor_json,status,last_health_json,updated_at FROM km_provider_registry ORDER BY provider_kind,provider_id`).all() as any[])
      .map(row => ({ providerId: row.provider_id, kind: row.provider_kind, version: row.provider_version,
        descriptor: KmProviderDescriptorSchema.parse(JSON.parse(row.descriptor_json)), status: row.status,
        ...(row.last_health_json ? { health: JSON.parse(row.last_health_json) } : {}), updatedAt: row.updated_at }));
  }

  listDistillationJobs(limit: number): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT job_id,source_event_id,bot_app_id,profile_id,profile_revision,state,attempts,next_attempt_at,last_error,output_hash,created_at,updated_at
      FROM distillation_jobs ORDER BY created_at DESC,job_id DESC LIMIT ?`).all(Math.max(1, Math.min(limit, 500))) as any[])
      .map(row => ({ jobId: row.job_id, sourceEventId: row.source_event_id, botAppId: row.bot_app_id,
        profileId: row.profile_id, profileRevision: row.profile_revision, state: row.state, attempts: row.attempts,
        nextAttemptAt: row.next_attempt_at, ...(row.last_error ? { lastError: row.last_error } : {}),
        ...(row.output_hash ? { outputHash: row.output_hash } : {}), createdAt: row.created_at, updatedAt: row.updated_at }));
  }

  listRetrievalAudits(limit: number): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT retrieval_run_id,bot_app_id,session_id,turn_id,mode,candidate_count,eligible_count,latency_ms,warnings_json,created_at
      FROM retrieval_runs ORDER BY created_at DESC,retrieval_run_id DESC LIMIT ?`).all(Math.max(1, Math.min(limit, 500))) as any[])
      .map(row => ({ retrievalRunId: row.retrieval_run_id, botAppId: row.bot_app_id, sessionId: row.session_id,
        ...(row.turn_id ? { turnId: row.turn_id } : {}), mode: row.mode, candidateCount: row.candidate_count,
        eligibleCount: row.eligible_count, latencyMs: row.latency_ms, warnings: JSON.parse(row.warnings_json), createdAt: row.created_at }));
  }

  listInjectionSnapshots(limit: number): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT snapshot_id,retrieval_run_id,bot_app_id,mode,disposition,item_ids_json,prompt_bytes,reason,created_at
      FROM prompt_injection_snapshots ORDER BY created_at DESC,snapshot_id DESC LIMIT ?`).all(Math.max(1, Math.min(limit, 500))) as any[])
      .map(row => ({ snapshotId: row.snapshot_id, retrievalRunId: row.retrieval_run_id, botAppId: row.bot_app_id,
        mode: row.mode, disposition: row.disposition, itemIds: JSON.parse(row.item_ids_json), promptBytes: row.prompt_bytes,
        ...(row.reason ? { reason: row.reason } : {}), createdAt: row.created_at }));
  }

  retrievalQualitySummary(): Record<string, number> {
    const row = this.db.prepare(`SELECT COUNT(*) runs,
      SUM(CASE WHEN candidate_count=0 THEN 1 ELSE 0 END) zero_hits,
      SUM(candidate_count) candidates,SUM(eligible_count) eligible,
      COALESCE(AVG(latency_ms),0) avg_latency_ms FROM retrieval_runs`).get() as any;
    return { runs: Number(row.runs ?? 0), zeroHits: Number(row.zero_hits ?? 0), candidates: Number(row.candidates ?? 0),
      eligible: Number(row.eligible ?? 0), avgLatencyMs: Math.round(Number(row.avg_latency_ms ?? 0)) };
  }

  retrievalRetentionPreview(cutoffIso: string): { cutoff: string; eligibleRuns: number } {
    const cutoff = new Date(cutoffIso); if (!Number.isFinite(cutoff.getTime())) throw new Error('km_retention_cutoff_invalid');
    const normalized = cutoff.toISOString();
    const row = this.db.prepare(`SELECT COUNT(*) count FROM retrieval_runs WHERE created_at<?`).get(normalized) as any;
    return { cutoff: normalized, eligibleRuns: Number(row.count ?? 0) };
  }

  purgeRetrievalAudit(input: { cutoffIso: string; expectedEligibleRuns: number; actorId: string; reason: string }): number {
    const preview = this.retrievalRetentionPreview(input.cutoffIso);
    if (preview.eligibleRuns !== input.expectedEligibleRuns) throw new Error('km_retention_preview_conflict');
    requireText(input.actorId,'retention_actor'); requireText(input.reason,'retention_reason');
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const changes = Number(this.db.prepare(`DELETE FROM retrieval_runs WHERE created_at<?`).run(preview.cutoff).changes);
      this.db.prepare(`INSERT INTO km_config_audit(audit_id,actor_id,action,target_ref,before_hash,after_hash,request_hash,idempotency_key,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(`kma_${randomUUID().replaceAll('-', '')}`,input.actorId,'retrieval.retention_purge',preview.cutoff,null,null,
        sha256(JSON.stringify({ expectedEligibleRuns: input.expectedEligibleRuns, reason: input.reason })),'retention-worker',new Date().toISOString());
      this.db.exec('COMMIT;'); return changes;
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
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

  private memoryBackendMigrationFromRow(row: any): MemoryBackendMigrationSnapshot {
    return {
      migrationId: row.migration_id,
      botAppId: row.bot_app_id,
      fromProfile: JSON.parse(row.from_profile_json),
      toProfile: JSON.parse(row.to_profile_json),
      state: row.state,
      ...(row.checkpoint ? { checkpoint: row.checkpoint } : {}),
      stats: JSON.parse(row.stats_json),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
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

  private migrateToPhase3(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 3) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE3_SCHEMA);
      this.db.exec('PRAGMA user_version=3;');
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  private migrateToPhase12(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 12) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE12_SCHEMA);
      this.db.exec('PRAGMA user_version=12;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase11(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 11) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE11_SCHEMA);
      this.db.exec('PRAGMA user_version=11;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase10(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 10) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE10_SCHEMA);
      this.db.exec('PRAGMA user_version=10;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase9(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 9) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE9_SCHEMA);
      this.db.exec('PRAGMA user_version=9;');
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  private migrateToPhase8(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 8) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE8_SCHEMA);
      this.db.exec('PRAGMA user_version=8;');
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  private migrateToPhase7(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 7) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE7_SCHEMA);
      this.db.exec('PRAGMA user_version=7;');
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  private migrateToPhase6(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 6) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE6_SCHEMA);
      this.db.exec('PRAGMA user_version=6;');
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  private migrateToPhase5(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 5) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE5_SCHEMA);
      this.db.exec('PRAGMA user_version=5;');
      this.db.exec('COMMIT;');
    } catch (error) {
      try { this.db.exec('ROLLBACK;'); } catch { /* transaction may already be closed */ }
      throw error;
    }
  }

  private migrateToPhase4(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 4) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE4_SCHEMA);
      this.db.exec('PRAGMA user_version=4;');
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
      'trace_edges',
      'eval_runs',
      'eval_results',
      'evolution_proposals',
      'approval_decisions',
      'sync_sinks',
      'sync_cursors',
      'sync_quarantine',
      'km_provider_registry',
      'km_pipeline_profiles',
      'distillation_jobs',
      'memory_backend_bindings',
      'memory_backend_outbox',
      'memory_backend_migrations',
      'retrieval_runs',
      'retrieval_results',
      'prompt_injection_snapshots',
      'km_memory_provider_configs',
      'km_mutation_idempotency',
      'km_config_audit',
      'km_memory_policy_decisions',
      'km_runtime_leases',
    ];
    for (const table of required) {
      const row = this.db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(table) as { name: string } | undefined;
      if (!row) throw new Error(`km_observation_schema_invalid:missing_${table}`);
    }
  }

  private seedBuiltinKmProviders(): void {
    for (const descriptor of BUILTIN_KM_PROVIDER_DESCRIPTORS) this.registerKmProvider(descriptor);
  }

  private seedBuiltinKmProvidersBestEffort(): void {
    const previousTimeout = Number((this.db.prepare('PRAGMA busy_timeout').get() as any)?.timeout ?? BUSY_TIMEOUT_MS);
    this.db.exec('PRAGMA busy_timeout=0;');
    try {
      this.seedBuiltinKmProviders();
    } catch (error) {
      if (!isSqliteBusyError(error)) throw error;
    } finally {
      this.db.exec(`PRAGMA busy_timeout=${previousTimeout};`);
    }
  }
}
