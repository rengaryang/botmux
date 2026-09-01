import { createHash, randomUUID } from 'node:crypto';
import { lstatSync, mkdirSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import type { DatabaseSync as DatabaseSyncType } from 'node:sqlite';
import { canonicalJsonStringify } from '../../utils/canonical-json.js';
import { KmMemoryProviderConfigSchema, KmPipelineProfileSchema, KmProviderDescriptorSchema, type KmMemoryProviderConfig, type KmPipelineProfile, type KmProviderDescriptor } from './provider-spi.js';
import {
  ObservationEventSchema,
  type ObservationEvent,
  type ObservationEventType,
} from './observation-schema.js';
import { normalizeRetrievalQuery, scoreNormalizedQuery } from './retrieval-quality.js';
import {
  DEFAULT_RETENTION_POLICIES,
  KM_RETENTION_POLICY_VERSION,
  ageDays,
  buildRetentionSloMetrics,
  finalizeRetentionPlan,
  totalEligible,
  worstSloState,
  type KmRetentionDomain,
  type KmRetentionDomainPreview,
  type KmRetentionPlan,
  type KmRetentionReportSummary,
  type KmRetentionRuntimeStatus,
} from './retention-policy.js';
import { assertKmProductionGateTransition } from './production-gate.js';

const SCHEMA_VERSION = 19;
const BUSY_TIMEOUT_MS = 5_000;

const KNOWLEDGE_STATES = [
  'observed', 'candidate', 'deduped', 'review_pending', 'approved', 'exported',
  'stale', 'conflict', 'rejected', 'deprecated', 'purged_local',
] as const;
const MEMORY_STATES = [
  'proposed', 'active', 'stale', 'conflicted', 'shadowed', 'expired', 'revoked', 'purged_local',
] as const;
const KNOWLEDGE_TO_MEMORY_IMPORT_JOB_STATES = [
  'preview', 'review_pending', 'running', 'completed', 'partial', 'failed',
] as const;
const KNOWLEDGE_TO_MEMORY_IMPORT_ITEM_STATES = [
  'pending', 'imported', 'deduped', 'conflicted', 'skipped', 'failed',
] as const;
const KM_INGEST_TARGET_STATES = ['disabled', 'ready'] as const;
const KM_INGEST_RUN_STATES = [
  'planned', 'approved', 'running', 'partial', 'completed', 'blocked', 'failed', 'rolled_back',
] as const;
const KM_INGEST_ITEM_STATES = ['pending', 'ingested', 'deduped', 'skipped', 'failed', 'rolled_back'] as const;

export type KnowledgeState = typeof KNOWLEDGE_STATES[number];
export type MemoryState = typeof MEMORY_STATES[number];
export type KnowledgeToMemoryImportJobState = typeof KNOWLEDGE_TO_MEMORY_IMPORT_JOB_STATES[number];
export type KnowledgeToMemoryImportItemState = typeof KNOWLEDGE_TO_MEMORY_IMPORT_ITEM_STATES[number];
export type KmIngestTargetState = typeof KM_INGEST_TARGET_STATES[number];
export type KmIngestRunState = typeof KM_INGEST_RUN_STATES[number];
export type KmIngestItemState = typeof KM_INGEST_ITEM_STATES[number];
export type KnowledgeLayer = 'L1' | 'L2' | 'L3' | 'L4' | 'reviewed-only';
export type MemoryScope = 'user' | 'bot' | 'workspace' | 'project' | 'skill' | 'environment' | 'team';
export type ImportableMemoryScope = Exclude<MemoryScope, 'user' | 'bot'>;
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
    warnings_json TEXT NOT NULL CHECK(json_valid(warnings_json)), created_at TEXT NOT NULL,
    direct_hit_count INTEGER NOT NULL DEFAULT 0,
    normalized_hit_count INTEGER NOT NULL DEFAULT 0,
    no_hit_count INTEGER NOT NULL DEFAULT 0,
    filtered_scope_count INTEGER NOT NULL DEFAULT 0,
    filtered_privacy_count INTEGER NOT NULL DEFAULT 0,
    filtered_state_count INTEGER NOT NULL DEFAULT 0
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

const PHASE13_SCHEMA = `
  UPDATE prompt_injection_snapshots
    SET requested_mode=COALESCE(requested_mode,mode), effective_mode=COALESCE(effective_mode,mode);
`;

const PHASE14_SCHEMA = `
  CREATE INDEX IF NOT EXISTS eval_results_run_verdict ON eval_results(eval_run_id,verdict);
  CREATE INDEX IF NOT EXISTS evolution_proposals_review_dedupe ON evolution_proposals(
    state,proposal_type,target_ref,evidence_refs_json,proposed_action_json
  );
`;

const PHASE15_SCHEMA = `
  CREATE TABLE IF NOT EXISTS km_retention_reports (
    report_id TEXT PRIMARY KEY,
    policy_version TEXT NOT NULL,
    holder_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    plan_json TEXT NOT NULL CHECK(json_valid(plan_json)),
    report_hash TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS km_retention_reports_completed ON km_retention_reports(completed_at DESC,report_id DESC);
  CREATE TABLE IF NOT EXISTS km_golden_cases (
    case_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=1),
    state TEXT NOT NULL CHECK(state IN ('reviewed','retired')),
    title TEXT NOT NULL,
    query_hash TEXT NOT NULL,
    query_redacted TEXT NOT NULL,
    expected_claims_json TEXT NOT NULL CHECK(json_valid(expected_claims_json)),
    source_refs_json TEXT NOT NULL CHECK(json_valid(source_refs_json)),
    provenance_json TEXT NOT NULL CHECK(json_valid(provenance_json)),
    privacy_class TEXT NOT NULL CHECK(privacy_class IN ('public-to-team','internal')),
    content_hash TEXT NOT NULL,
    created_by TEXT NOT NULL,
    reviewed_by TEXT NOT NULL,
    retired_by TEXT,
    created_at TEXT NOT NULL,
    reviewed_at TEXT NOT NULL,
    retired_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(case_id,revision),
    UNIQUE(content_hash)
  );
  CREATE INDEX IF NOT EXISTS km_golden_cases_state_updated ON km_golden_cases(state,updated_at DESC,case_id,revision);
  CREATE TABLE IF NOT EXISTS km_shadow_comparisons (
    comparison_id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    rules_snapshot_hash TEXT NOT NULL,
    pi_snapshot_hash TEXT NOT NULL,
    rules_claims_json TEXT NOT NULL CHECK(json_valid(rules_claims_json)),
    pi_claims_json TEXT NOT NULL CHECK(json_valid(pi_claims_json)),
    metrics_json TEXT NOT NULL CHECK(json_valid(metrics_json)),
    latency_json TEXT NOT NULL CHECK(json_valid(latency_json)),
    cost_json TEXT NOT NULL CHECK(json_valid(cost_json)),
    created_at TEXT NOT NULL,
    UNIQUE(case_id,revision,rules_snapshot_hash,pi_snapshot_hash),
    FOREIGN KEY(case_id,revision) REFERENCES km_golden_cases(case_id,revision) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS km_shadow_comparisons_case_created ON km_shadow_comparisons(case_id,revision,created_at DESC);
  CREATE TABLE IF NOT EXISTS km_shadow_review_labels (
    label_id TEXT PRIMARY KEY,
    comparison_id TEXT NOT NULL REFERENCES km_shadow_comparisons(comparison_id) ON DELETE CASCADE,
    case_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    claim_key TEXT NOT NULL,
    extractor TEXT NOT NULL CHECK(extractor IN ('rules','pi')),
    label TEXT NOT NULL CHECK(label IN ('true_positive','false_positive','false_negative','true_negative','needs_review')),
    actor_id TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(comparison_id,claim_key,extractor,label,actor_id)
  );
  CREATE INDEX IF NOT EXISTS km_shadow_review_labels_case ON km_shadow_review_labels(case_id,revision,created_at DESC);
  CREATE TABLE IF NOT EXISTS km_shadow_readiness_reports (
    report_id TEXT PRIMARY KEY,
    window_hash TEXT NOT NULL UNIQUE,
    thresholds_json TEXT NOT NULL CHECK(json_valid(thresholds_json)),
    metrics_json TEXT NOT NULL CHECK(json_valid(metrics_json)),
    ready INTEGER NOT NULL CHECK(ready IN (0,1)),
    reason_codes_json TEXT NOT NULL CHECK(json_valid(reason_codes_json)),
    created_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS km_shadow_readiness_reports_created ON km_shadow_readiness_reports(created_at DESC,report_id DESC);
`;

const PHASE16_SCHEMA = `
  CREATE TABLE IF NOT EXISTS km_import_jobs (
    job_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('preview','review_pending','running','completed','partial','failed')),
    config_json TEXT NOT NULL CHECK(json_valid(config_json)),
    config_hash TEXT NOT NULL,
    checkpoint TEXT,
    source_count INTEGER NOT NULL DEFAULT 0,
    eligible_count INTEGER NOT NULL DEFAULT 0,
    imported_count INTEGER NOT NULL DEFAULT 0,
    deduped_count INTEGER NOT NULL DEFAULT 0,
    conflict_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    outbox_enqueued_count INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    approved_by TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT
  );
  CREATE INDEX IF NOT EXISTS km_import_jobs_state_updated ON km_import_jobs(state,updated_at DESC,job_id DESC);

  CREATE TABLE IF NOT EXISTS km_import_items (
    import_item_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES km_import_jobs(job_id) ON DELETE CASCADE,
    source_kind TEXT NOT NULL CHECK(source_kind IN ('knowledge_item','markdown_file')),
    source_id TEXT NOT NULL,
    source_ref_json TEXT NOT NULL CHECK(json_valid(source_ref_json)),
    source_hash TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','imported','deduped','conflicted','skipped','failed')),
    reason_code TEXT,
    scope TEXT NOT NULL CHECK(scope IN ('workspace','project','skill','environment','team')),
    subject TEXT NOT NULL,
    claim_key TEXT NOT NULL,
    claim_text TEXT NOT NULL,
    confidence TEXT NOT NULL CHECK(confidence IN ('observed','inferred')),
    privacy_class TEXT NOT NULL CHECK(privacy_class IN ('public-to-team','internal','sensitive','secret-reference-only')),
    freshness TEXT NOT NULL CHECK(freshness IN ('fresh','stale','purged','unknown')),
    memory_id TEXT REFERENCES memory_items(memory_id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(job_id,source_kind,source_id,content_hash)
  );
  CREATE INDEX IF NOT EXISTS km_import_items_job_state ON km_import_items(job_id,state,import_item_id);
  CREATE INDEX IF NOT EXISTS km_import_items_target ON km_import_items(scope,subject,claim_key,state);

  CREATE TABLE IF NOT EXISTS km_import_audit (
    audit_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES km_import_jobs(job_id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    details_json TEXT NOT NULL CHECK(json_valid(details_json)),
    created_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS km_import_audit_job_created ON km_import_audit(job_id,created_at,audit_id);
`;

const PHASE18_SCHEMA = `
  CREATE TABLE IF NOT EXISTS km_production_gate_plans (
    plan_id TEXT PRIMARY KEY,
    action_kind TEXT NOT NULL CHECK(action_kind IN ('real-memory-transport','real-central-sink','formal-knowledge-export','prompt-canary','retention-purge')),
    state TEXT NOT NULL CHECK(state IN ('draft','ready','approved','executing','completed','failed','rolled_back','expired')),
    target_json TEXT NOT NULL CHECK(json_valid(target_json)),
    scope_json TEXT NOT NULL CHECK(json_valid(scope_json)),
    preview_json TEXT NOT NULL CHECK(json_valid(preview_json)),
    preview_hash TEXT NOT NULL,
    required_approval_grade TEXT NOT NULL CHECK(required_approval_grade IN ('G0','G1','G2','G3','G4')),
    actor_id TEXT NOT NULL,
    risk_ack_json TEXT NOT NULL CHECK(json_valid(risk_ack_json)),
    expires_at TEXT NOT NULL,
    confirmation_token_hash TEXT NOT NULL,
    confirmation_token_used_at TEXT,
    preflight_json TEXT NOT NULL CHECK(json_valid(preflight_json)),
    rollback_json TEXT NOT NULL CHECK(json_valid(rollback_json)),
    intent_json TEXT CHECK(intent_json IS NULL OR json_valid(intent_json)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS km_production_gate_plans_kind_state ON km_production_gate_plans(action_kind,state,updated_at DESC);
  CREATE UNIQUE INDEX IF NOT EXISTS km_production_gate_plans_token_hash ON km_production_gate_plans(confirmation_token_hash);

  CREATE TABLE IF NOT EXISTS km_production_gate_audit (
    audit_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES km_production_gate_plans(plan_id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    details_json TEXT NOT NULL CHECK(json_valid(details_json)),
    created_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS km_production_gate_audit_plan_created ON km_production_gate_audit(plan_id,created_at,audit_id);

  CREATE TABLE IF NOT EXISTS km_production_gate_kill_state (
    scope TEXT PRIMARY KEY CHECK(scope='global'),
    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
    reason TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
`;

const PHASE19_SCHEMA = `
  CREATE TABLE IF NOT EXISTS km_ingest_targets (
    target_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('disabled','ready')),
    target_json TEXT NOT NULL CHECK(json_valid(target_json)),
    target_hash TEXT NOT NULL,
    credential_ref TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS km_ingest_targets_state_updated ON km_ingest_targets(state,updated_at DESC,target_id);

  CREATE TABLE IF NOT EXISTS km_ingest_runs (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('planned','approved','running','partial','completed','blocked','failed','rolled_back')),
    target_id TEXT NOT NULL,
    plan_json TEXT NOT NULL CHECK(json_valid(plan_json)),
    plan_hash TEXT NOT NULL,
    canonical_key_set_hash TEXT NOT NULL,
    confirmation_token_hash TEXT NOT NULL,
    external_ack_json TEXT CHECK(external_ack_json IS NULL OR json_valid(external_ack_json)),
    source_count INTEGER NOT NULL DEFAULT 0,
    eligible_count INTEGER NOT NULL DEFAULT 0,
    ingested_count INTEGER NOT NULL DEFAULT 0,
    deduped_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    rollback_count INTEGER NOT NULL DEFAULT 0,
    mark_ingested_planned_count INTEGER NOT NULL DEFAULT 0,
    checkpoint TEXT,
    created_by TEXT NOT NULL,
    approved_by TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    rolled_back_at TEXT
  );
  CREATE INDEX IF NOT EXISTS km_ingest_runs_state_updated ON km_ingest_runs(state,updated_at DESC,run_id DESC);
  CREATE INDEX IF NOT EXISTS km_ingest_runs_target_updated ON km_ingest_runs(target_id,updated_at DESC,run_id DESC);

  CREATE TABLE IF NOT EXISTS km_ingest_items (
    ingest_item_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES km_ingest_runs(run_id) ON DELETE CASCADE,
    canonical_key TEXT NOT NULL,
    candidate_json TEXT NOT NULL CHECK(json_valid(candidate_json)),
    candidate_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','ingested','deduped','skipped','failed','rolled_back')),
    reason_code TEXT,
    knowledge_id TEXT REFERENCES knowledge_items(knowledge_id) ON DELETE SET NULL,
    mark_ingested_plan_json TEXT CHECK(mark_ingested_plan_json IS NULL OR json_valid(mark_ingested_plan_json)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(run_id,canonical_key)
  );
  CREATE INDEX IF NOT EXISTS km_ingest_items_run_state ON km_ingest_items(run_id,state,ingest_item_id);

  CREATE TABLE IF NOT EXISTS km_ingest_audit (
    audit_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES km_ingest_runs(run_id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    details_json TEXT NOT NULL CHECK(json_valid(details_json)),
    created_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS km_ingest_audit_run_created ON km_ingest_audit(run_id,created_at,audit_id);
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
    batch_limit INTEGER NOT NULL DEFAULT 25,
    timeout_ms INTEGER NOT NULL DEFAULT 5000,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    credential_ref TEXT,
    allowlist_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(allowlist_json)),
    tls_policy TEXT NOT NULL DEFAULT 'https-required-for-future-real-transport',
    payload_max_bytes INTEGER NOT NULL DEFAULT 65536,
    rollback_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(rollback_json)),
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

export interface KnowledgeToMemoryImportConfig {
  source: 'knowledge-items' | 'markdown-files' | 'mixed';
  allowlistedRoots: string[];
  markdownFiles?: string[];
  defaultScope: ImportableMemoryScope;
  defaultSubject: string;
  scopeByLayer?: Partial<Record<KnowledgeLayer, ImportableMemoryScope>>;
  subjectByLayer?: Partial<Record<KnowledgeLayer, string>>;
  enqueueBackendOutbox?: boolean;
  backendProviderIds?: string[];
  batchSize?: number;
}

export interface KnowledgeToMemoryImportJob {
  jobId: string;
  idempotencyKey: string;
  state: KnowledgeToMemoryImportJobState;
  config: KnowledgeToMemoryImportConfig;
  configHash: string;
  checkpoint?: string;
  sourceCount: number;
  eligibleCount: number;
  importedCount: number;
  dedupedCount: number;
  conflictCount: number;
  skippedCount: number;
  failedCount: number;
  outboxEnqueuedCount: number;
  createdBy: string;
  approvedBy?: string;
  lastError?: string;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  completedAt?: string;
}

export interface KnowledgeToMemoryImportItem {
  importItemId: string;
  jobId: string;
  sourceKind: 'knowledge_item' | 'markdown_file';
  sourceId: string;
  sourceRef: Record<string, unknown>;
  sourceHash: string;
  contentHash: string;
  state: KnowledgeToMemoryImportItemState;
  reasonCode?: string;
  scope: ImportableMemoryScope;
  subject: string;
  claimKey: string;
  claimText: string;
  confidence: KmConfidence;
  privacyClass: KmPrivacyClass;
  freshness: KnowledgeItem['freshness'];
  memoryId?: string;
  createdAt: string;
  updatedAt: string;
}

export interface KnowledgeToMemoryImportItemInput {
  sourceKind: KnowledgeToMemoryImportItem['sourceKind'];
  sourceId: string;
  sourceRef: Record<string, unknown>;
  sourceHash: string;
  contentHash: string;
  state?: KnowledgeToMemoryImportItemState;
  reasonCode?: string;
  scope: ImportableMemoryScope;
  subject: string;
  claimKey: string;
  claimText: string;
  confidence: KmConfidence;
  privacyClass: KmPrivacyClass;
  freshness: KnowledgeItem['freshness'];
}

export interface KnowledgeToMemoryImportStats {
  sourceCount: number;
  eligibleCount: number;
  importedCount: number;
  dedupedCount: number;
  conflictCount: number;
  skippedCount: number;
  failedCount: number;
  outboxEnqueuedCount: number;
}

export interface KnowledgeToMemoryImportPreviewInput {
  idempotencyKey: string;
  actorId: string;
  config: KnowledgeToMemoryImportConfig;
  items: KnowledgeToMemoryImportItemInput[];
}

export interface KnowledgeToMemoryImportRunInput {
  jobId: string;
  actorId: string;
  maxItems?: number;
}

export interface KnowledgeToMemoryImportReport {
  job: KnowledgeToMemoryImportJob;
  items: KnowledgeToMemoryImportItem[];
  audit: Array<{ auditId: string; action: string; actorId: string; details: Record<string, unknown>; createdAt: string }>;
}

export interface KmIngestTargetConfig {
  targetId: string;
  endpointRef: string;
  credentialRef: string;
  enabled?: boolean;
  dryRunOnly?: boolean;
  allowedProviderIds?: string[];
  markIngestedCommand?: string;
}

export interface KmIngestTargetRecord {
  targetId: string;
  state: KmIngestTargetState;
  target: {
    endpointRef: string;
    dryRunOnly: boolean;
    allowedProviderIds: string[];
    markIngestedCommand?: string;
  };
  targetHash: string;
  credentialRef: string;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

export interface KmIngestCandidateInput extends KnowledgeCandidateInput {
  canonicalKey?: string;
  providerId?: string;
  sourceRunId?: string;
}

export interface KmIngestItemInput {
  canonicalKey: string;
  candidate: KnowledgeCandidateInput & { providerId?: string; sourceRunId?: string };
  candidateHash: string;
  state?: KmIngestItemState;
  reasonCode?: string;
}

export interface KmIngestRunPlan {
  schemaVersion: 1;
  targetId: string;
  targetHash: string;
  sourceRunId: string;
  extractorRunState: string;
  extractorProviderId: string;
  mode: 'offline';
  dryRun: boolean;
  planCalls: {
    markIngested: boolean;
  };
  canonicalKeys: string[];
}

export interface KmIngestRunRecord {
  runId: string;
  idempotencyKey: string;
  state: KmIngestRunState;
  targetId: string;
  plan: KmIngestRunPlan;
  planHash: string;
  canonicalKeySetHash: string;
  confirmationTokenHash: string;
  externalAck?: Record<string, unknown>;
  sourceCount: number;
  eligibleCount: number;
  ingestedCount: number;
  dedupedCount: number;
  skippedCount: number;
  failedCount: number;
  rollbackCount: number;
  markIngestedPlannedCount: number;
  checkpoint?: string;
  createdBy: string;
  approvedBy?: string;
  lastError?: string;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  completedAt?: string;
  rolledBackAt?: string;
}

export interface KmIngestItemRecord {
  ingestItemId: string;
  runId: string;
  canonicalKey: string;
  candidate: KnowledgeCandidateInput & { providerId?: string; sourceRunId?: string };
  candidateHash: string;
  state: KmIngestItemState;
  reasonCode?: string;
  knowledgeId?: string;
  markIngestedPlan?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface KmIngestRunStats {
  sourceCount: number;
  eligibleCount: number;
  ingestedCount: number;
  dedupedCount: number;
  skippedCount: number;
  failedCount: number;
  rollbackCount: number;
  markIngestedPlannedCount: number;
}

export interface KmIngestRunCreateInput {
  idempotencyKey: string;
  actorId: string;
  targetId: string;
  confirmationTokenHash: string;
  plan: KmIngestRunPlan;
  items: KmIngestItemInput[];
}

export interface KmIngestRunReport {
  run: KmIngestRunRecord;
  items: KmIngestItemRecord[];
  audit: Array<{ auditId: string; action: string; actorId: string; details: Record<string, unknown>; createdAt: string }>;
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
  subjects?: Partial<Record<MemoryScope, string>>;
  targetLayers?: KnowledgeLayer[];
  limit: number;
}

export interface RetrievalQualityCounters {
  directHitCount: number;
  normalizedHitCount: number;
  noHitCount: number;
  filteredScopeCount: number;
  filteredPrivacyCount: number;
  filteredStateCount: number;
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
  scope?: MemoryScope;
  subject?: string;
  matchKind?: 'direct' | 'normalized';
  matchedGroups?: number;
}

export interface RetrievalResultSet {
  items: RetrievalItem[];
  metrics: RetrievalQualityCounters;
}

function retrievalExpectedSubject(input: RetrievalQuery, scope: MemoryScope): string | undefined {
  if (input.subjects) return input.subjects[scope];
  if (input.subject && input.scopes?.length === 1 && input.scopes[0] === scope) return input.subject;
  return undefined;
}

export type ApprovalGrade = 'G0' | 'G1' | 'G2' | 'G3' | 'G4';
export type KmProductionGateActionKind = 'real-memory-transport' | 'real-central-sink' | 'formal-knowledge-export' | 'prompt-canary' | 'retention-purge';
export type KmProductionGateState = 'draft' | 'ready' | 'approved' | 'executing' | 'completed' | 'failed' | 'rolled_back' | 'expired';
export interface KmProductionGatePlanRecord {
  planId: string;
  actionKind: KmProductionGateActionKind;
  state: KmProductionGateState;
  target: Record<string, unknown>;
  scope: Record<string, unknown>;
  preview: Record<string, unknown>;
  previewHash: string;
  requiredApprovalGrade: ApprovalGrade;
  actorId: string;
  riskAck: Record<string, unknown>;
  expiresAt: string;
  confirmationTokenHash: string;
  confirmationTokenUsedAt?: string;
  preflight: Array<Record<string, unknown>>;
  rollback: Record<string, unknown>;
  intent?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}
export interface KmProductionGateAuditRecord {
  auditId: string;
  planId: string;
  action: string;
  fromState?: KmProductionGateState;
  toState: KmProductionGateState;
  actorId: string;
  details: Record<string, unknown>;
  createdAt: string;
}
export interface KmProductionGateKillState {
  enabled: boolean;
  reason: string;
  actorId: string;
  updatedAt: string;
}
export interface KmProductionGatePlanInsertInput {
  planId: string;
  actionKind: KmProductionGateActionKind;
  state: KmProductionGateState;
  target: Record<string, unknown>;
  scope: Record<string, unknown>;
  preview: Record<string, unknown>;
  previewHash: string;
  requiredApprovalGrade: ApprovalGrade;
  actorId: string;
  riskAck: Record<string, unknown>;
  expiresAt: string;
  confirmationTokenHash: string;
  preflight: Array<Record<string, unknown>>;
  rollback: Record<string, unknown>;
  now?: string;
}
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
  batchLimit?: number; timeoutMs?: number; maxAttempts?: number; credentialRef?: string; allowlist?: string[];
  payloadMaxBytes?: number; rollback?: Record<string, unknown>;
}
export interface SyncStatus {
  sinkId: string; endpointRef: string; enabled: boolean; status: string;
  lastLocalSeq: number; lastBatchId?: string; lastAckAt?: string; centralCursor?: string;
  pending: number; inflight: number; failed: number; delivered: number; quarantined: number;
  endpointPolicy?: { ok: boolean; mode: 'offline' | 'blocked-real' | 'invalid'; reason?: string };
  protocolVersion?: number;
  batchLimit?: number; timeoutMs?: number; maxAttempts?: number; payloadMaxBytes?: number;
  credentialRef?: string; allowlist?: string[]; tlsPolicy?: string; rollback?: Record<string, unknown>;
}
export interface SyncOutboxRow {
  outboxId: string; eventId: string; sinkId: string; status: 'pending' | 'inflight' | 'delivered' | 'failed' | 'quarantined';
  attempts: number; nextAttemptAt: number; claimedAt?: number; claimToken?: string; lastError?: string; deliveredAt?: string;
  payload: Record<string, unknown>; payloadHash: string; createdAt: string;
}

export interface EvolutionProposalInput {
  proposalType: 'skill-route' | 'skill-edit' | 'knowledge-promotion' | 'memory-policy' | 'dashboard-warning' | 'workflow-revision' | 'cleanup-action' | 'external-action';
  targetRef: string; approvalGrade: ApprovalGrade; summary: string; evidenceRefs: unknown[];
  proposedAction: Record<string, unknown>; risk: Record<string, unknown>; rollback: Record<string, unknown>; createdBy: string;
}

export interface KmEvalEvolutionStatus {
  evalRuns: number;
  failingEvalRuns: number;
  reviewPendingProposals: number;
  latestEvalAt?: string;
  latestProposalAt?: string;
}

export interface KmEvalTarget {
  sourceKind: 'distillation-job' | 'retrieval-run' | 'prompt-injection' | 'memory-policy-decision' | 'workflow-artifact';
  targetType: 'turn' | 'workflow-artifact' | 'knowledge' | 'memory' | 'skill' | 'sync-batch' | 'proposal';
  targetId: string;
  sourceRef: { kind: 'sqlite-row' | 'workflow-artifact'; ref: string; sha256?: string | null };
  payload: Record<string, unknown>;
}

export interface KmEvalMetricWindow {
  metricKey: string;
  totalCount: number;
  passCount: number;
  warnCount: number;
  failCount: number;
  failRatio: number;
  failedTargetIds: string[];
  evidenceRefs: unknown[];
  windowHash: string;
}

export type KmGoldenCaseState = 'reviewed' | 'retired';
export type KmShadowExtractor = 'rules' | 'pi';
export type KmShadowReviewLabel = 'true_positive' | 'false_positive' | 'false_negative' | 'true_negative' | 'needs_review';
export interface KmGoldenExpectedClaim {
  claimKey: string;
  claimTextHash: string;
  category?: string;
}
export interface KmGoldenCase {
  caseId: string;
  revision: number;
  state: KmGoldenCaseState;
  title: string;
  queryHash: string;
  queryRedacted: string;
  expectedClaims: KmGoldenExpectedClaim[];
  sourceRefs: unknown[];
  provenance: Record<string, unknown>;
  privacyClass: 'public-to-team' | 'internal';
  contentHash: string;
  createdBy: string;
  reviewedBy: string;
  retiredBy?: string;
  createdAt: string;
  reviewedAt: string;
  retiredAt?: string;
  updatedAt: string;
}
export interface KmGoldenCaseInput {
  caseId?: string;
  title: string;
  queryRedacted: string;
  expectedClaims: KmGoldenExpectedClaim[];
  sourceRefs: unknown[];
  provenance: Record<string, unknown>;
  privacyClass?: 'public-to-team' | 'internal';
  actorId: string;
}
export interface KmShadowComparisonMetrics {
  expectedCount: number;
  rulesClaimCount: number;
  piClaimCount: number;
  rulesTruePositive: number;
  rulesFalsePositive: number;
  rulesFalseNegative: number;
  piTruePositive: number;
  piFalsePositive: number;
  piFalseNegative: number;
  rulesFalsePositiveRate: number;
  rulesFalseNegativeRate: number;
  piFalsePositiveRate: number;
  piFalseNegativeRate: number;
  claimOverlap: number;
  rulesUnique: number;
  piUnique: number;
  routingDisagreement: number;
  extractorDisagreement: number;
  evidenceCoverage: number;
  privacyBlocks: number;
  schemaFailures: number;
  falsePositiveLabels: number;
  falseNegativeLabels: number;
}
export interface KmShadowComparisonInput {
  caseId: string;
  revision?: number;
  rulesClaims: Array<{ claimKey: string; route?: string; evidenceRefs?: unknown[]; privacyBlocked?: boolean; schemaFailure?: boolean }>;
  piClaims: Array<{ claimKey: string; route?: string; evidenceRefs?: unknown[]; privacyBlocked?: boolean; schemaFailure?: boolean }>;
  latency?: Record<string, unknown>;
  cost?: Record<string, unknown>;
}
export interface KmShadowComparison {
  comparisonId: string;
  caseId: string;
  revision: number;
  rulesSnapshotHash: string;
  piSnapshotHash: string;
  rulesClaims: KmShadowComparisonInput['rulesClaims'];
  piClaims: KmShadowComparisonInput['piClaims'];
  metrics: KmShadowComparisonMetrics;
  latency: Record<string, unknown>;
  cost: Record<string, unknown>;
  createdAt: string;
}
export interface KmShadowReadinessReport {
  reportId: string;
  windowHash: string;
  thresholds: Record<string, number>;
  metrics: Record<string, number>;
  ready: boolean;
  reasonCodes: string[];
  createdAt: string;
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

export interface KmOpsBucket {
  key: string;
  count: number;
}

export interface KmOpsItemRank {
  itemId: string;
  itemKind: 'knowledge' | 'memory' | 'unknown';
  title: string;
  count: number;
  lastSeenAt: string;
  state?: string;
  targetLayer?: KnowledgeLayer;
  category?: string;
  scope?: MemoryScope;
}

export interface KmOpsAttentionItem {
  itemId: string;
  itemKind: 'knowledge' | 'memory';
  title: string;
  state: string;
  updatedAt: string;
  ageDays: number;
  targetLayer?: KnowledgeLayer;
  category?: string;
  scope?: MemoryScope;
}

export interface KmOpsTrendPoint {
  date: string;
  knowledgeCreated: number;
  memoryCreated: number;
  retrievalRuns: number;
  wouldInject: number;
  actualInject: number;
}

export interface KmOpsEmptyState {
  key: string;
  empty: boolean;
  title: string;
  detail: string;
}

export interface KmOpsMetricsRaw {
  schemaVersion: 1;
  source: 'sqlite';
  generatedAt: string;
  windows: {
    last7dSince: string;
    last30dSince: string;
  };
  kpis: {
    totalKnowledge: number;
    activeMemory: number;
    healthPercent: number;
    retrievalRuns: number;
    auditEvents: number;
  };
  totals: {
    knowledgeTotal: number;
    knowledgeUsable: number;
    memoryTotal: number;
    memoryActive: number;
    memoryUsable: number;
    retrievalTotal: number;
    retrievalLast7d: number;
    retrievalLast30d: number;
    wouldInjectTotal: number;
    wouldInjectLast7d: number;
    wouldInjectLast30d: number;
    actualInjectTotal: number;
    actualInjectLast7d: number;
    actualInjectLast30d: number;
    auditEventsTotal: number;
    pendingReviewTotal: number;
    conflictTotal: number;
    staleKnowledge: number;
    staleMemory: number;
    overallHealthRate: number;
    knowledgeUsableRate: number;
    memoryActiveRate: number;
    freshnessRate: number;
  };
  distributions: {
    knowledgeByLayer: KmOpsBucket[];
    knowledgeByState: KmOpsBucket[];
    memoryByState: KmOpsBucket[];
    memoryByScope: KmOpsBucket[];
    knowledgeByFreshness: KmOpsBucket[];
    knowledgeByCategory: KmOpsBucket[];
    observationBySource: Array<KmOpsBucket & { adapter: string }>;
    observationByType: KmOpsBucket[];
    operationalHealth: KmOpsBucket[];
  };
  trends: {
    last7d: KmOpsTrendPoint[];
    last30d: KmOpsTrendPoint[];
  };
  rankings: {
    recallHot: KmOpsItemRank[];
    readHot: KmOpsItemRank[];
    pendingReview: KmOpsAttentionItem[];
    conflicts: KmOpsAttentionItem[];
    stale: KmOpsAttentionItem[];
  };
  emptyStates: KmOpsEmptyState[];
}

interface ExistingIdentityRow {
  event_id: string;
  payload_hash: string;
  local_seq: number;
}

function sha256(value: string): string {
  return `sha256:${createHash('sha256').update(value).digest('hex')}`;
}

function fileBytes(path: string): number {
  try { return lstatSync(path).size; }
  catch { return 0; }
}

function payloadHash(event: ObservationEvent): string {
  return sha256(JSON.stringify(event.payload));
}

function quarantineId(): string {
  return `q_${randomUUID().replaceAll('-', '')}`;
}

function kmId(prefix: 'kn' | 'mem' | 'hist' | 'edge' | 'eval' | 'result' | 'evo' | 'approval' | 'gold' | 'cmp' | 'label' | 'ready' | 'kmi' | 'kmii' | 'kmia' | 'pg' | 'pga' | 'kmt' | 'kmir' | 'kmiri' | 'kmira'): string {
  return `${prefix}_${randomUUID().replaceAll('-', '')}`;
}

function requireText(value: string, field: string): string {
  const result = value.trim();
  if (!result) throw new Error(`km_${field}_required`);
  return result;
}

function boundedInt(value: number | undefined, fallback: number, min: number, max: number): number {
  if (value === undefined) return fallback;
  if (!Number.isFinite(value)) throw new Error('km_invalid_integer');
  return Math.max(min, Math.min(Math.trunc(value), max));
}

function parseJsonArray(value: string): unknown[] {
  const parsed = JSON.parse(value);
  return Array.isArray(parsed) ? parsed : [];
}

function parseJsonObject(value: string): Record<string, unknown> {
  const parsed = JSON.parse(value);
  return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
}

function syncEndpointPolicy(endpointRef: string): { ok: boolean; mode: 'offline' | 'blocked-real' | 'invalid'; reason?: string } {
  try {
    const url = new URL(endpointRef);
    if (url.protocol === 'mock:' || url.protocol === 'inmemory:') return { ok: true, mode: 'offline' };
    if (url.protocol === 'https:') return { ok: false, mode: 'blocked-real', reason: 'offline_runtime_allows_mock_or_inmemory_only' };
    if (url.protocol === 'http:') return { ok: false, mode: 'invalid', reason: 'tls_required_for_future_real_transport' };
    return { ok: false, mode: 'invalid', reason: 'unsupported_protocol' };
  } catch {
    return { ok: false, mode: 'invalid', reason: 'invalid_url' };
  }
}

function ratioPercent(part: number, total: number): number {
  if (total <= 0) return 0;
  return Math.round((part / total) * 1000) / 10;
}

function startOfUtcDay(ms: number): number {
  const date = new Date(ms);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

function isoDay(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

function ageDaysFrom(updatedAt: string, nowMs: number): number {
  const parsed = Date.parse(updatedAt);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.floor((nowMs - parsed) / 86_400_000));
}

interface RetentionDomainSpec {
  table: string;
  idColumn: string;
  ageColumn: string;
  protectedReasons: Record<string, string>;
}

interface OutboxStatusCounts {
  pending: number;
  inflight: number;
  failed: number;
}

function retentionSpec(domain: KmRetentionDomain): RetentionDomainSpec {
  const legalHold = (column: string) => `(${column} LIKE '%\"legalHold\":true%' OR ${column} LIKE '%\"legal_hold\"%' OR ${column} LIKE '%legal-hold%')`;
  switch (domain) {
    case 'observations':
      return {
        table: 'observation_events',
        idColumn: 'event_id',
        ageColumn: 'created_at',
        protectedReasons: {
          legal_hold: legalHold('event_json'),
          referenced_evidence: `event_id IN (SELECT parent_event_id FROM observation_parents)
            OR event_id IN (SELECT source_event_id FROM distillation_jobs)
            OR event_id IN (SELECT evidence_event_id FROM trace_edges WHERE evidence_event_id IS NOT NULL)`,
          quarantine_evidence: 'event_id IN (SELECT event_id FROM quarantine_events)',
        },
      };
    case 'knowledge':
      return {
        table: 'knowledge_items',
        idColumn: 'knowledge_id',
        ageColumn: 'updated_at',
        protectedReasons: {
          active_review_state: `state IN ('approved','exported','review_pending','candidate','conflict')`,
          legal_hold: legalHold('source_refs_json'),
        },
      };
    case 'memory':
      return {
        table: 'memory_items',
        idColumn: 'memory_id',
        ageColumn: 'updated_at',
        protectedReasons: {
          active_or_pending: `state IN ('active','proposed','conflicted')`,
          legal_hold: legalHold('source_refs_json'),
        },
      };
    case 'retrieval':
      return {
        table: 'retrieval_runs',
        idColumn: 'retrieval_run_id',
        ageColumn: 'created_at',
        protectedReasons: {
          referenced_by_injection: 'retrieval_run_id IN (SELECT retrieval_run_id FROM prompt_injection_snapshots)',
          referenced_by_eval: `retrieval_run_id IN (SELECT target_id FROM eval_runs
            WHERE evaluator_name='km.retrieval-quality' AND target_type='turn')`,
        },
      };
    case 'injection':
      return {
        table: 'prompt_injection_snapshots',
        idColumn: 'snapshot_id',
        ageColumn: 'created_at',
        protectedReasons: {
          injected_live: `disposition='injected'`,
          referenced_by_eval: `snapshot_id IN (SELECT target_id FROM eval_runs
            WHERE evaluator_name='km.injection-safety' AND target_type='turn')`,
        },
      };
    case 'trace':
      return {
        table: 'trace_edges',
        idColumn: 'edge_id',
        ageColumn: 'created_at',
        protectedReasons: {
          causal_evidence: 'evidence_event_id IS NOT NULL',
          active_decision: `edge_type IN ('approved','conflicted','synced')`,
        },
      };
    case 'eval':
      return {
        table: 'eval_runs',
        idColumn: 'eval_run_id',
        ageColumn: 'updated_at',
        protectedReasons: {
          active_or_quality_evidence: `state IN ('queued','running','accepted')`,
        },
      };
    case 'evolution':
      return {
        table: 'evolution_proposals',
        idColumn: 'proposal_id',
        ageColumn: 'updated_at',
        protectedReasons: {
          active_or_approved: `state IN ('review_pending','approved','executing','applied','verified')`,
          legal_hold: legalHold('evidence_refs_json') + ` OR ${legalHold('risk_json')} OR ${legalHold('rollback_json')}`,
        },
      };
    case 'distillation':
      return {
        table: 'distillation_jobs',
        idColumn: 'job_id',
        ageColumn: 'updated_at',
        protectedReasons: {
          active_pending_or_retry: `state IN ('queued','resolving','extracting','normalizing','gating','persisted','retry_wait','quarantined')`,
          source_evidence: 'source_event_id IN (SELECT event_id FROM observation_events)',
          legal_hold: legalHold('evidence_context_json'),
        },
      };
    case 'sync-outbox':
      return {
        table: 'sync_outbox',
        idColumn: 'outbox_id',
        ageColumn: 'created_at',
        protectedReasons: {
          pending_inflight_retry_or_quarantine: `status IN ('pending','inflight','failed','quarantined')`,
        },
      };
    case 'backend-outbox':
      return {
        table: 'memory_backend_outbox',
        idColumn: 'outbox_id',
        ageColumn: 'updated_at',
        protectedReasons: {
          pending_inflight_retry_or_quarantine: `status IN ('pending','inflight','failed','quarantined')`,
        },
      };
    case 'quarantine-evidence':
      return {
        table: 'quarantine_events',
        idColumn: 'quarantine_id',
        ageColumn: 'created_at',
        protectedReasons: {
          all_quarantine_evidence: '1=1',
        },
      };
  }
}

function parseJsonRecord(value: string): Record<string, unknown> {
  const parsed = JSON.parse(value);
  return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
}

function assertNoRawText(value: unknown, field: string): void {
  if (typeof value === 'string') {
    if (/<raw_transcript>|<\/raw_transcript>/iu.test(value)) throw new Error(`km_${field}_raw_transcript_forbidden`);
    return;
  }
  if (!value || typeof value !== 'object') return;
  if (Array.isArray(value)) {
    for (const item of value) assertNoRawText(item, field);
    return;
  }
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (/^(raw|text|transcript|rawTranscript|content)$/u.test(key)) throw new Error(`km_${field}_raw_text_field_forbidden`);
    assertNoRawText(child, field);
  }
}

function isReviewedRedactedProvenance(value: Record<string, unknown>): boolean {
  const reviewed = value.explicitlyReviewed === true || value.reviewed === true;
  const redactionStatus = typeof value.redactionStatus === 'string' ? value.redactionStatus : '';
  const redacted = value.redacted === true || redactionStatus === 'redacted' || redactionStatus === 'not_needed';
  return reviewed && redacted;
}

function assertReviewedDistillationSourceRefs(sourceRefs: unknown[]): void {
  for (const ref of sourceRefs) {
    if (!ref || typeof ref !== 'object' || Array.isArray(ref)) throw new Error('km_golden_source_ref_invalid');
    const record = ref as Record<string, unknown>;
    const kind = typeof record.kind === 'string' ? record.kind : '';
    const sourceRef = typeof record.ref === 'string' ? record.ref.trim() : '';
    if (kind !== 'distillation-example' && kind !== 'reviewed-distillation-example') {
      throw new Error('km_golden_source_ref_not_reviewed_distillation_example');
    }
    if (!sourceRef) throw new Error('km_golden_source_ref_required');
  }
}

function normalizeImportScope(value: unknown, field: string): ImportableMemoryScope {
  const scope = requireText(String(value ?? ''), field);
  if (!['workspace','project','skill','environment','team'].includes(scope)) throw new Error(`km_${field}_invalid`);
  return scope as ImportableMemoryScope;
}

function normalizeKnowledgeToMemoryImportConfig(input: KnowledgeToMemoryImportConfig): KnowledgeToMemoryImportConfig {
  const source = input.source;
  if (!['knowledge-items','markdown-files','mixed'].includes(source)) throw new Error('km_import_source_invalid');
  const allowlistedRoots = [...new Set(input.allowlistedRoots.map(root => resolve(requireText(root, 'import_allowlisted_root'))))]
    .sort((a, b) => a.localeCompare(b));
  if (allowlistedRoots.length === 0) throw new Error('km_import_allowlist_required');
  const markdownFiles = input.markdownFiles?.map(file => resolve(requireText(file, 'import_markdown_file')))
    .filter((value, index, values) => values.indexOf(value) === index)
    .sort((a, b) => a.localeCompare(b));
  const scopeByLayer: KnowledgeToMemoryImportConfig['scopeByLayer'] = {};
  for (const [layer, scope] of Object.entries(input.scopeByLayer ?? {})) {
    if (!['L1','L2','L3','L4','reviewed-only'].includes(layer)) throw new Error('km_import_layer_invalid');
    scopeByLayer[layer as KnowledgeLayer] = normalizeImportScope(scope, 'import_scope_by_layer');
  }
  const subjectByLayer: KnowledgeToMemoryImportConfig['subjectByLayer'] = {};
  for (const [layer, subject] of Object.entries(input.subjectByLayer ?? {})) {
    if (!['L1','L2','L3','L4','reviewed-only'].includes(layer)) throw new Error('km_import_layer_invalid');
    subjectByLayer[layer as KnowledgeLayer] = requireText(String(subject), 'import_subject_by_layer');
  }
  const backendProviderIds = input.backendProviderIds?.map(provider => requireText(provider, 'import_backend_provider'))
    .filter((value, index, values) => values.indexOf(value) === index)
    .sort((a, b) => a.localeCompare(b));
  return {
    source,
    allowlistedRoots,
    ...(markdownFiles ? { markdownFiles } : {}),
    defaultScope: normalizeImportScope(input.defaultScope, 'import_default_scope'),
    defaultSubject: requireText(input.defaultSubject, 'import_default_subject'),
    ...(Object.keys(scopeByLayer).length ? { scopeByLayer } : {}),
    ...(Object.keys(subjectByLayer).length ? { subjectByLayer } : {}),
    enqueueBackendOutbox: input.enqueueBackendOutbox === true,
    ...(backendProviderIds ? { backendProviderIds } : {}),
    batchSize: Math.max(1, Math.min(input.batchSize ?? 50, 100)),
  };
}

function normalizeKnowledgeToMemoryImportItems(
  jobId: string,
  items: KnowledgeToMemoryImportItemInput[],
): Array<KnowledgeToMemoryImportItem & { jobId: string }> {
  const seen = new Set<string>();
  const now = new Date().toISOString();
  const normalized: Array<KnowledgeToMemoryImportItem & { jobId: string }> = [];
  for (const raw of items) {
    const sourceKind = raw.sourceKind;
    if (sourceKind !== 'knowledge_item' && sourceKind !== 'markdown_file') throw new Error('km_import_source_kind_invalid');
    const sourceId = requireText(raw.sourceId, 'import_source_id');
    const sourceHash = requireText(raw.sourceHash, 'import_source_hash');
    const contentHash = requireText(raw.contentHash, 'import_content_hash');
    const claimKey = requireText(raw.claimKey, 'import_claim_key');
    const claimText = requireText(raw.claimText, 'import_claim_text');
    const dedupeKey = `${sourceKind}|${sourceId}|${contentHash}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    const inputState = raw.state ?? 'pending';
    if (!KNOWLEDGE_TO_MEMORY_IMPORT_ITEM_STATES.includes(inputState)) throw new Error('km_import_item_state_invalid');
    const state = inputState === 'skipped' || raw.confidence === 'inferred'
      || raw.privacyClass === 'sensitive' || raw.privacyClass === 'secret-reference-only'
      || raw.freshness !== 'fresh'
      ? 'skipped'
      : inputState;
    const reasonCode = raw.reasonCode ?? (state === 'skipped'
      ? raw.confidence === 'inferred' ? 'inferred_not_auto_imported'
        : raw.privacyClass === 'sensitive' || raw.privacyClass === 'secret-reference-only' ? 'privacy_not_auto_imported'
          : raw.freshness !== 'fresh' ? 'freshness_not_importable'
            : 'source_not_importable'
      : undefined);
    const importItemId = `kmii_${createHash('sha256').update(`${jobId}|${dedupeKey}`).digest('hex')}`;
    normalized.push({
      importItemId,
      jobId,
      sourceKind,
      sourceId,
      sourceRef: raw.sourceRef,
      sourceHash,
      contentHash,
      state,
      ...(reasonCode ? { reasonCode } : {}),
      scope: normalizeImportScope(raw.scope, 'import_item_scope'),
      subject: requireText(raw.subject, 'import_item_subject'),
      claimKey,
      claimText,
      confidence: raw.confidence,
      privacyClass: raw.privacyClass,
      freshness: raw.freshness,
      createdAt: now,
      updatedAt: now,
    });
  }
  return normalized;
}

function normalizeKmIngestItems(runId: string, items: KmIngestItemInput[]): Array<KmIngestItemRecord> {
  const seen = new Set<string>();
  const now = new Date().toISOString();
  const normalized: KmIngestItemRecord[] = [];
  for (const raw of items) {
    const canonicalKey = requireText(raw.canonicalKey, 'ingest_canonical_key');
    if (seen.has(canonicalKey)) throw new Error('km_ingest_canonical_key_duplicate');
    seen.add(canonicalKey);
    if (!KM_INGEST_ITEM_STATES.includes(raw.state ?? 'pending')) throw new Error('km_ingest_item_state_invalid');
    if (raw.candidate.sourceRefs.length === 0) throw new Error('km_ingest_candidate_source_refs_required');
    normalized.push({
      ingestItemId: `kmiri_${createHash('sha256').update(`${runId}|${canonicalKey}`).digest('hex')}`,
      runId,
      canonicalKey,
      candidate: JSON.parse(canonicalJsonStringify(raw.candidate)) as KmIngestItemRecord['candidate'],
      candidateHash: requireText(raw.candidateHash, 'ingest_candidate_hash'),
      state: raw.state ?? 'pending',
      ...(raw.reasonCode ? { reasonCode: raw.reasonCode } : {}),
      createdAt: now,
      updatedAt: now,
    });
  }
  return normalized;
}

const KM_INGEST_TRANSITIONS: Readonly<Record<KmIngestRunState, readonly KmIngestRunState[]>> = {
  planned: ['approved', 'blocked', 'failed'],
  approved: ['running', 'blocked', 'failed'],
  running: ['partial', 'completed', 'blocked', 'failed'],
  partial: ['running', 'rolled_back', 'failed'],
  completed: ['rolled_back'],
  blocked: ['approved', 'failed', 'rolled_back'],
  failed: ['running', 'rolled_back'],
  rolled_back: [],
};

function assertKmIngestRunTransition(from: KmIngestRunState, to: KmIngestRunState): void {
  if (!KM_INGEST_TRANSITIONS[from]?.includes(to)) throw new Error(`km_ingest_invalid_transition:${from}:${to}`);
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
    capabilities: ['configured-only', 'fixture-transport-only', 'credential-reference', 'request-codec', 'response-schema-validation', 'idempotency-key', 'bounded-pagination', 'privacy-gate', 'redacted-telemetry'],
    execution: 'service',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 50,
  },
  {
    id: 'hindsight',
    kind: 'memory-backend',
    version: '1',
    contractVersion: 1,
    capabilities: ['configured-only', 'fixture-transport-only', 'credential-reference', 'request-codec', 'response-schema-validation', 'idempotency-key', 'bounded-pagination', 'privacy-gate', 'redacted-telemetry'],
    execution: 'service',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 50,
  },
  {
    id: 'openviking',
    kind: 'memory-backend',
    version: '1',
    contractVersion: 1,
    capabilities: ['configured-only', 'fixture-transport-only', 'credential-reference', 'request-codec', 'response-schema-validation', 'idempotency-key', 'bounded-pagination', 'privacy-gate', 'redacted-telemetry', 'snapshot-capable'],
    execution: 'service',
    deterministic: true,
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
  {
    id: 'km-quality-evaluators-v1',
    kind: 'evaluator',
    version: '1',
    contractVersion: 1,
    capabilities: ['artifact-completeness', 'distillation-quality', 'retrieval-quality', 'memory-policy-quality', 'local-only'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 100,
  },
  {
    id: 'km-evolution-planner-v1',
    kind: 'evolution-planner',
    version: '1',
    contractVersion: 1,
    capabilities: ['review-pending-only', 'threshold-gated', 'dedupe-by-evidence-window', 'no-auto-apply'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 25,
  },
  {
    id: 'km-shadow-quality-v1',
    kind: 'evaluator',
    version: '1',
    contractVersion: 1,
    capabilities: ['golden-set-governance', 'rules-pi-shadow-comparison', 'readiness-report', 'local-only', 'no-external-execution'],
    execution: 'in-process',
    deterministic: true,
    supportsShadow: true,
    maxBatchSize: 100,
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
    if (this.schemaVersion() < 13) this.migrateToPhase13();
    if (this.schemaVersion() < 14) this.migrateToPhase14();
    if (this.schemaVersion() < 15) this.migrateToPhase15();
    if (this.schemaVersion() < 16) this.migrateToPhase16();
    if (this.schemaVersion() < 17) this.migrateToPhase17();
    if (this.schemaVersion() < 18) this.migrateToPhase18();
    if (this.schemaVersion() < 19) this.migrateToPhase19();
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

  createKnowledgeToMemoryImportPreview(input: KnowledgeToMemoryImportPreviewInput): KnowledgeToMemoryImportReport {
    const actorId = requireText(input.actorId, 'import_actor');
    const idempotencyKey = requireText(input.idempotencyKey, 'import_idempotency_key');
    const config = normalizeKnowledgeToMemoryImportConfig(input.config);
    const configJson = canonicalJsonStringify(config);
    const configHash = sha256(configJson);
    const existing = this.db.prepare('SELECT * FROM km_import_jobs WHERE idempotency_key=?').get(idempotencyKey) as any;
    if (existing) {
      if (existing.config_hash !== configHash) throw new Error('km_import_idempotency_conflict');
      return this.getKnowledgeToMemoryImportReport(existing.job_id)!;
    }

    const now = new Date().toISOString();
    const jobId = `kmi_${createHash('sha256').update(`${idempotencyKey}|${configHash}`).digest('hex')}`;
    const normalizedItems = normalizeKnowledgeToMemoryImportItems(jobId, input.items);
    const stats = this.computeKnowledgeToMemoryImportStats(normalizedItems);
    this.db.exec('SAVEPOINT km_import_preview;');
    try {
      this.db.prepare(`INSERT INTO km_import_jobs(
        job_id,idempotency_key,state,config_json,config_hash,checkpoint,source_count,eligible_count,imported_count,
        deduped_count,conflict_count,skipped_count,failed_count,outbox_enqueued_count,created_by,created_at,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(jobId, idempotencyKey, 'preview', configJson, configHash, null,
        stats.sourceCount, stats.eligibleCount, 0, 0, 0, stats.skippedCount, stats.failedCount, 0, actorId, now, now);
      const insert = this.db.prepare(`INSERT INTO km_import_items(
        import_item_id,job_id,source_kind,source_id,source_ref_json,source_hash,content_hash,state,reason_code,
        scope,subject,claim_key,claim_text,confidence,privacy_class,freshness,created_at,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
      for (const item of normalizedItems) insert.run(item.importItemId, jobId, item.sourceKind, item.sourceId,
        JSON.stringify(item.sourceRef), item.sourceHash, item.contentHash, item.state, item.reasonCode ?? null,
        item.scope, item.subject, item.claimKey, item.claimText, item.confidence, item.privacyClass, item.freshness, now, now);
      this.insertKnowledgeToMemoryImportAudit(jobId, 'preview.created', actorId, {
        configHash,
        sourceCount: stats.sourceCount,
        eligibleCount: stats.eligibleCount,
        skippedCount: stats.skippedCount,
      }, now);
      this.db.exec('RELEASE km_import_preview;');
    } catch (error) {
      try { this.db.exec('ROLLBACK TO km_import_preview; RELEASE km_import_preview;'); } catch {}
      throw error;
    }
    return this.getKnowledgeToMemoryImportReport(jobId)!;
  }

  submitKnowledgeToMemoryImportReview(input: { jobId: string; actorId: string }): KnowledgeToMemoryImportJob {
    const actorId = requireText(input.actorId, 'import_actor');
    const job = this.getKnowledgeToMemoryImportJob(input.jobId);
    if (!job) throw new Error('km_import_job_not_found');
    if (job.state === 'review_pending' || job.state === 'completed' || job.state === 'partial') return job;
    if (job.state !== 'preview') throw new Error(`km_import_invalid_review_state:${job.state}`);
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_import_review;');
    try {
      this.db.prepare(`UPDATE km_import_jobs SET state='review_pending',approved_by=?,updated_at=? WHERE job_id=?`)
        .run(actorId, now, input.jobId);
      this.insertKnowledgeToMemoryImportAudit(input.jobId, 'review.approved', actorId, {}, now);
      this.db.exec('RELEASE km_import_review;');
    } catch (error) {
      try { this.db.exec('ROLLBACK TO km_import_review; RELEASE km_import_review;'); } catch {}
      throw error;
    }
    return this.getKnowledgeToMemoryImportJob(input.jobId)!;
  }

  runKnowledgeToMemoryImport(input: KnowledgeToMemoryImportRunInput): KnowledgeToMemoryImportReport {
    const actorId = requireText(input.actorId, 'import_actor');
    const job = this.getKnowledgeToMemoryImportJob(input.jobId);
    if (!job) throw new Error('km_import_job_not_found');
    if (job.state === 'completed') return this.getKnowledgeToMemoryImportReport(input.jobId)!;
    if (!['review_pending','partial','failed','running'].includes(job.state)) throw new Error(`km_import_execution_requires_review:${job.state}`);
    const config = normalizeKnowledgeToMemoryImportConfig(job.config);
    const limit = Math.max(1, Math.min(input.maxItems ?? config.batchSize ?? 50, 100));
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_import_run;');
    try {
      this.db.prepare(`UPDATE km_import_jobs SET state='running',approved_by=COALESCE(approved_by,?),started_at=COALESCE(started_at,?),updated_at=?,last_error=NULL WHERE job_id=?`)
        .run(actorId, now, now, input.jobId);
      this.insertKnowledgeToMemoryImportAudit(input.jobId, 'execution.started', actorId, { limit }, now);
      const rows = this.db.prepare(`SELECT * FROM km_import_items
        WHERE job_id=? AND state IN ('pending','failed')
        ORDER BY import_item_id ASC LIMIT ?`).all(input.jobId, limit) as any[];
      for (const row of rows) {
        try {
          this.applyKnowledgeToMemoryImportItem(row, config, actorId, now);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          this.markKnowledgeToMemoryImportItem(String(row.import_item_id), 'failed', message.slice(0, 200), undefined, now);
        }
      }
      this.refreshKnowledgeToMemoryImportJob(input.jobId, rows.at(-1)?.import_item_id ? String(rows.at(-1).import_item_id) : job.checkpoint, now);
      this.insertKnowledgeToMemoryImportAudit(input.jobId, 'execution.finished', actorId, { processed: rows.length }, now);
      this.db.exec('RELEASE km_import_run;');
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      try {
        this.db.exec('ROLLBACK TO km_import_run;');
        this.db.prepare(`UPDATE km_import_jobs SET state='failed',last_error=?,updated_at=? WHERE job_id=?`)
          .run(message.slice(0, 500), new Date().toISOString(), input.jobId);
        this.db.exec('RELEASE km_import_run;');
      } catch {
        try { this.db.exec('ROLLBACK TO km_import_run; RELEASE km_import_run;'); } catch {}
      }
      throw error;
    }
    return this.getKnowledgeToMemoryImportReport(input.jobId)!;
  }

  getKnowledgeToMemoryImportJob(jobId: string): KnowledgeToMemoryImportJob | null {
    const row = this.db.prepare('SELECT * FROM km_import_jobs WHERE job_id=?').get(jobId) as any;
    return row ? this.knowledgeToMemoryImportJobFromRow(row) : null;
  }

  listKnowledgeToMemoryImportJobs(limit: number): KnowledgeToMemoryImportJob[] {
    const rows = this.db.prepare('SELECT * FROM km_import_jobs ORDER BY updated_at DESC,job_id DESC LIMIT ?')
      .all(Math.max(1, Math.min(limit, 100))) as any[];
    return rows.map(row => this.knowledgeToMemoryImportJobFromRow(row));
  }

  listKnowledgeToMemoryImportItems(input: { jobId: string; limit?: number; state?: KnowledgeToMemoryImportItemState }): KnowledgeToMemoryImportItem[] {
    const limit = Math.max(1, Math.min(input.limit ?? 100, 500));
    const rows = input.state
      ? this.db.prepare(`SELECT * FROM km_import_items WHERE job_id=? AND state=? ORDER BY import_item_id ASC LIMIT ?`).all(input.jobId, input.state, limit) as any[]
      : this.db.prepare(`SELECT * FROM km_import_items WHERE job_id=? ORDER BY import_item_id ASC LIMIT ?`).all(input.jobId, limit) as any[];
    return rows.map(row => this.knowledgeToMemoryImportItemFromRow(row));
  }

  getKnowledgeToMemoryImportReport(jobId: string): KnowledgeToMemoryImportReport | null {
    const job = this.getKnowledgeToMemoryImportJob(jobId);
    if (!job) return null;
    const auditRows = this.db.prepare(`SELECT * FROM km_import_audit WHERE job_id=? ORDER BY created_at ASC,audit_id ASC`).all(jobId) as any[];
    return {
      job,
      items: this.listKnowledgeToMemoryImportItems({ jobId, limit: 500 }),
      audit: auditRows.map(row => ({
        auditId: row.audit_id,
        action: row.action,
        actorId: row.actor_id,
        details: parseJsonRecord(row.details_json),
        createdAt: row.created_at,
      })),
    };
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
    latencyMs: number; warnings: string[]; metrics?: Partial<RetrievalQualityCounters>;
    results: Array<{ itemId: string; itemKind: string; providerIds: string[]; score: number; eligible: boolean; filterReason?: string }>;
  }): string {
    const id = `retr_${randomUUID().replaceAll('-', '')}`; const now = new Date().toISOString();
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const metrics = input.metrics ?? {};
      this.db.prepare(`INSERT INTO retrieval_runs(retrieval_run_id,bot_app_id,session_id,turn_id,query_hash,mode,candidate_count,eligible_count,latency_ms,warnings_json,created_at,
          direct_hit_count,normalized_hit_count,no_hit_count,filtered_scope_count,filtered_privacy_count,filtered_state_count)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(id, input.botAppId, input.sessionId, input.turnId ?? null, input.queryHash, input.mode,
        input.candidateCount, input.eligibleCount, input.latencyMs, JSON.stringify(input.warnings), now,
        metrics.directHitCount ?? 0, metrics.normalizedHitCount ?? 0, metrics.noHitCount ?? 0, metrics.filteredScopeCount ?? 0,
        metrics.filteredPrivacyCount ?? 0, metrics.filteredStateCount ?? 0);
      const insert = this.db.prepare(`INSERT INTO retrieval_results(retrieval_run_id,item_id,item_kind,provider_ids_json,score,eligible,filter_reason)
        VALUES(?,?,?,?,?,?,?)`);
      for (const result of input.results) insert.run(id, result.itemId, result.itemKind, JSON.stringify(result.providerIds), result.score,
        result.eligible ? 1 : 0, result.filterReason ?? null);
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
    return id;
  }

  recordPromptInjectionSnapshot(input: {
    retrievalRunId: string; botAppId: string; mode: string; requestedMode?: string; effectiveMode?: string;
    disposition: 'off' | 'would_inject' | 'injected' | 'skipped'; itemIds: string[];
    prompt?: string; reason?: string;
  }): string {
    const id = `inject_${randomUUID().replaceAll('-', '')}`; const prompt = input.prompt ?? '';
    this.db.prepare(`INSERT INTO prompt_injection_snapshots(snapshot_id,retrieval_run_id,bot_app_id,mode,requested_mode,effective_mode,disposition,item_ids_json,prompt_hash,prompt_bytes,reason,created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`).run(id, input.retrievalRunId, input.botAppId, input.mode, input.requestedMode ?? input.mode,
      input.effectiveMode ?? input.mode, input.disposition, JSON.stringify(input.itemIds), prompt ? sha256(prompt) : null,
      Buffer.byteLength(prompt), input.reason ?? null, new Date().toISOString());
    return id;
  }

  upsertGoldenCase(input: KmGoldenCaseInput): { item: KmGoldenCase; created: boolean } {
    const actorId = requireText(input.actorId, 'golden_actor');
    const title = requireText(input.title, 'golden_title');
    const queryRedacted = requireText(input.queryRedacted, 'golden_query_redacted');
    if (/<raw_transcript>|<\/raw_transcript>/iu.test(queryRedacted)) throw new Error('km_golden_raw_transcript_forbidden');
    if (input.expectedClaims.length === 0) throw new Error('km_golden_expected_claims_required');
    if (input.sourceRefs.length === 0) throw new Error('km_golden_source_refs_required');
    assertNoRawText(input.sourceRefs, 'golden_source_refs');
    assertReviewedDistillationSourceRefs(input.sourceRefs);
    assertNoRawText(input.provenance, 'golden_provenance');
    if (!isReviewedRedactedProvenance(input.provenance)) throw new Error('km_golden_requires_reviewed_redacted_provenance');
    const normalizedClaims = input.expectedClaims.map(claim => ({
      claimKey: requireText(claim.claimKey, 'golden_claim_key'),
      claimTextHash: requireText(claim.claimTextHash, 'golden_claim_hash'),
      ...(claim.category?.trim() ? { category: claim.category.trim() } : {}),
    })).sort((a, b) => a.claimKey.localeCompare(b.claimKey));
    if (normalizedClaims.some(claim => !/^sha256:[a-f0-9]{64}$/u.test(claim.claimTextHash))) throw new Error('km_golden_claim_hash_invalid');
    const queryHash = sha256(queryRedacted);
    const caseId = input.caseId?.trim() || `gold_${createHash('sha256').update(`${title}|${queryHash}`).digest('hex').slice(0, 32)}`;
    const provenance = { ...input.provenance, reviewedOnly: true, redacted: true };
    const canonicalContent = canonicalJsonStringify({
      caseId, title, queryHash, queryRedacted, expectedClaims: normalizedClaims,
      sourceRefs: input.sourceRefs, provenance, privacyClass: input.privacyClass ?? 'internal',
    });
    const contentHash = sha256(canonicalContent);
    const existing = this.db.prepare('SELECT * FROM km_golden_cases WHERE content_hash=?').get(contentHash) as any;
    if (existing) return { item: this.goldenCaseFromRow(existing), created: false };
    const latest = this.db.prepare('SELECT MAX(revision) revision FROM km_golden_cases WHERE case_id=?').get(caseId) as { revision: number | null } | undefined;
    const revision = Number(latest?.revision ?? 0) + 1;
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_golden_upsert;');
    try {
      this.db.prepare(`INSERT INTO km_golden_cases(
        case_id,revision,state,title,query_hash,query_redacted,expected_claims_json,source_refs_json,provenance_json,
        privacy_class,content_hash,created_by,reviewed_by,created_at,reviewed_at,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(caseId, revision, 'reviewed', title, queryHash, queryRedacted,
        JSON.stringify(normalizedClaims), JSON.stringify(input.sourceRefs), JSON.stringify(provenance),
        input.privacyClass ?? 'internal', contentHash, actorId, actorId, now, now, now);
      this.db.prepare(`INSERT INTO km_config_audit(audit_id,actor_id,action,target_ref,before_hash,after_hash,request_hash,idempotency_key,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(`kma_${randomUUID().replaceAll('-', '')}`, actorId, 'golden.created', `${caseId}@${revision}`,
        null, contentHash, sha256(canonicalContent), `golden:${contentHash}`, now);
      this.db.exec('RELEASE km_golden_upsert;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_golden_upsert; RELEASE km_golden_upsert;'); } catch {} throw error; }
    return { item: this.getGoldenCase(caseId, revision)!, created: true };
  }

  getGoldenCase(caseId: string, revision?: number): KmGoldenCase | null {
    const row = revision
      ? this.db.prepare('SELECT * FROM km_golden_cases WHERE case_id=? AND revision=?').get(caseId, revision) as any
      : this.db.prepare('SELECT * FROM km_golden_cases WHERE case_id=? ORDER BY revision DESC LIMIT 1').get(caseId) as any;
    return row ? this.goldenCaseFromRow(row) : null;
  }

  listGoldenCases(input: { limit: number; state?: KmGoldenCaseState } = { limit: 50 }): KmGoldenCase[] {
    const limit = Math.max(1, Math.min(input.limit, 500));
    const rows = input.state
      ? this.db.prepare('SELECT * FROM km_golden_cases WHERE state=? ORDER BY updated_at DESC,case_id DESC,revision DESC LIMIT ?').all(input.state, limit) as any[]
      : this.db.prepare('SELECT * FROM km_golden_cases ORDER BY updated_at DESC,case_id DESC,revision DESC LIMIT ?').all(limit) as any[];
    return rows.map(row => this.goldenCaseFromRow(row));
  }

  retireGoldenCase(input: { caseId: string; revision?: number; actorId: string; reasonCode: string }): KmGoldenCase {
    const current = this.getGoldenCase(input.caseId, input.revision);
    if (!current) throw new Error('km_golden_case_not_found');
    if (current.state === 'retired') return current;
    const actorId = requireText(input.actorId, 'golden_actor');
    requireText(input.reasonCode, 'golden_reason');
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_golden_retire;');
    try {
      this.db.prepare('UPDATE km_golden_cases SET state=?,retired_by=?,retired_at=?,updated_at=? WHERE case_id=? AND revision=?')
        .run('retired', actorId, now, now, current.caseId, current.revision);
      this.db.prepare(`INSERT INTO km_config_audit(audit_id,actor_id,action,target_ref,before_hash,after_hash,request_hash,idempotency_key,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(`kma_${randomUUID().replaceAll('-', '')}`, actorId, 'golden.retired', `${current.caseId}@${current.revision}`,
        current.contentHash, null, sha256(JSON.stringify({ reasonCode: input.reasonCode })), `golden-retire:${current.caseId}:${current.revision}`, now);
      this.db.exec('RELEASE km_golden_retire;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_golden_retire; RELEASE km_golden_retire;'); } catch {} throw error; }
    return this.getGoldenCase(current.caseId, current.revision)!;
  }

  recordShadowComparison(input: KmShadowComparisonInput): { item: KmShadowComparison; created: boolean } {
    const golden = this.getGoldenCase(input.caseId, input.revision);
    if (!golden) throw new Error('km_golden_case_not_found');
    if (golden.state !== 'reviewed') throw new Error('km_golden_case_not_reviewed');
    const normalizeClaims = (claims: KmShadowComparisonInput['rulesClaims']) => claims.map(claim => ({
      claimKey: requireText(claim.claimKey, 'shadow_claim_key'),
      ...(claim.route?.trim() ? { route: claim.route.trim() } : {}),
      ...(claim.evidenceRefs ? { evidenceRefs: claim.evidenceRefs } : {}),
      ...(claim.privacyBlocked ? { privacyBlocked: true } : {}),
      ...(claim.schemaFailure ? { schemaFailure: true } : {}),
    })).sort((a, b) => a.claimKey.localeCompare(b.claimKey));
    const rulesClaims = normalizeClaims(input.rulesClaims);
    const piClaims = normalizeClaims(input.piClaims);
    assertNoRawText(rulesClaims, 'shadow_rules_claims');
    assertNoRawText(piClaims, 'shadow_pi_claims');
    assertNoRawText(input.latency ?? {}, 'shadow_latency');
    assertNoRawText(input.cost ?? {}, 'shadow_cost');
    const rulesSnapshotHash = sha256(canonicalJsonStringify(rulesClaims));
    const piSnapshotHash = sha256(canonicalJsonStringify(piClaims));
    const metrics = this.computeShadowComparisonMetrics(golden, rulesClaims, piClaims, undefined);
    const comparisonId = `cmp_${createHash('sha256').update(`${golden.caseId}|${golden.revision}|${rulesSnapshotHash}|${piSnapshotHash}`).digest('hex')}`;
    const now = new Date().toISOString();
    const result = this.db.prepare(`INSERT OR IGNORE INTO km_shadow_comparisons(
      comparison_id,case_id,revision,rules_snapshot_hash,pi_snapshot_hash,rules_claims_json,pi_claims_json,metrics_json,latency_json,cost_json,created_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)`).run(comparisonId, golden.caseId, golden.revision, rulesSnapshotHash, piSnapshotHash,
      JSON.stringify(rulesClaims), JSON.stringify(piClaims), JSON.stringify(metrics), JSON.stringify(input.latency ?? {}), JSON.stringify(input.cost ?? {}), now);
    return { item: this.getShadowComparison(comparisonId)!, created: Number(result.changes) === 1 };
  }

  getShadowComparison(comparisonId: string): KmShadowComparison | null {
    const row = this.db.prepare('SELECT * FROM km_shadow_comparisons WHERE comparison_id=?').get(comparisonId) as any;
    return row ? this.shadowComparisonFromRow(row) : null;
  }

  listShadowComparisons(input: { limit: number; caseId?: string } = { limit: 50 }): KmShadowComparison[] {
    const limit = Math.max(1, Math.min(input.limit, 500));
    const rows = input.caseId
      ? this.db.prepare('SELECT * FROM km_shadow_comparisons WHERE case_id=? ORDER BY created_at DESC,comparison_id DESC LIMIT ?').all(input.caseId, limit) as any[]
      : this.db.prepare('SELECT * FROM km_shadow_comparisons ORDER BY created_at DESC,comparison_id DESC LIMIT ?').all(limit) as any[];
    return rows.map(row => this.shadowComparisonFromRow(row));
  }

  addShadowReviewLabel(input: {
    comparisonId: string; claimKey: string; extractor: KmShadowExtractor; label: KmShadowReviewLabel; actorId: string; reasonCode: string;
  }): { labelId: string; created: boolean } {
    const comparison = this.getShadowComparison(input.comparisonId);
    if (!comparison) throw new Error('km_shadow_comparison_not_found');
    const claimKey = requireText(input.claimKey, 'shadow_label_claim_key');
    const actorId = requireText(input.actorId, 'shadow_label_actor');
    const reasonCode = requireText(input.reasonCode, 'shadow_label_reason');
    if (!['rules','pi'].includes(input.extractor)) throw new Error('km_shadow_label_extractor_invalid');
    if (!['true_positive','false_positive','false_negative','true_negative','needs_review'].includes(input.label)) throw new Error('km_shadow_label_invalid');
    const labelId = `label_${createHash('sha256').update(`${comparison.comparisonId}|${claimKey}|${input.extractor}|${input.label}|${actorId}`).digest('hex')}`;
    const result = this.db.prepare(`INSERT OR IGNORE INTO km_shadow_review_labels(
      label_id,comparison_id,case_id,revision,claim_key,extractor,label,actor_id,reason_code,created_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?)`).run(labelId, comparison.comparisonId, comparison.caseId, comparison.revision, claimKey,
      input.extractor, input.label, actorId, reasonCode, new Date().toISOString());
    this.refreshComparisonMetrics(comparison.comparisonId);
    return { labelId, created: Number(result.changes) === 1 };
  }

  listShadowReviewLabels(limit: number): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT * FROM km_shadow_review_labels ORDER BY created_at DESC,label_id DESC LIMIT ?`).all(Math.max(1, Math.min(limit, 500))) as any[])
      .map(row => ({ labelId: row.label_id, comparisonId: row.comparison_id, caseId: row.case_id, revision: Number(row.revision),
        claimKey: row.claim_key, extractor: row.extractor, label: row.label, actorId: row.actor_id,
        reasonCode: row.reason_code, createdAt: row.created_at }));
  }

  shadowReadinessReport(input: { thresholds?: Record<string, number> } = {}): KmShadowReadinessReport {
    const thresholds = {
      minReviewedCases: input.thresholds?.minReviewedCases ?? 1,
      minComparisons: input.thresholds?.minComparisons ?? 1,
      maxSchemaFailures: input.thresholds?.maxSchemaFailures ?? 0,
      maxPrivacyBlocks: input.thresholds?.maxPrivacyBlocks ?? 0,
      maxRoutingDisagreementRate: input.thresholds?.maxRoutingDisagreementRate ?? 0.2,
      minEvidenceCoverage: input.thresholds?.minEvidenceCoverage ?? 0.8,
      maxFalsePositiveLabels: input.thresholds?.maxFalsePositiveLabels ?? 0,
      maxFalseNegativeLabels: input.thresholds?.maxFalseNegativeLabels ?? 0,
    };
    const goldenRow = this.db.prepare("SELECT COUNT(*) reviewed_cases FROM km_golden_cases WHERE state='reviewed'").get() as any;
    const comparisonRow = this.db.prepare('SELECT COUNT(*) comparisons FROM km_shadow_comparisons').get() as any;
    const rows = this.db.prepare('SELECT metrics_json FROM km_shadow_comparisons').all() as any[];
    const aggregate: Record<string, number> = { reviewedCases: Number(goldenRow.reviewed_cases ?? 0), comparisons: Number(comparisonRow.comparisons ?? 0),
      expectedCount: 0, rulesClaimCount: 0, piClaimCount: 0,
      rulesTruePositive: 0, rulesFalsePositive: 0, rulesFalseNegative: 0,
      piTruePositive: 0, piFalsePositive: 0, piFalseNegative: 0,
      claimOverlap: 0, rulesUnique: 0, piUnique: 0, routingDisagreement: 0, extractorDisagreement: 0,
      privacyBlocks: 0, schemaFailures: 0,
      falsePositiveLabels: 0, falseNegativeLabels: 0, avgEvidenceCoverage: 0 };
    let coverageTotal = 0;
    for (const row of rows) {
      const metrics = JSON.parse(row.metrics_json) as KmShadowComparisonMetrics;
      aggregate.expectedCount += Number(metrics.expectedCount ?? 0);
      aggregate.rulesClaimCount += Number(metrics.rulesClaimCount ?? 0);
      aggregate.piClaimCount += Number(metrics.piClaimCount ?? 0);
      aggregate.rulesTruePositive += Number(metrics.rulesTruePositive ?? 0);
      aggregate.rulesFalsePositive += Number(metrics.rulesFalsePositive ?? 0);
      aggregate.rulesFalseNegative += Number(metrics.rulesFalseNegative ?? 0);
      aggregate.piTruePositive += Number(metrics.piTruePositive ?? 0);
      aggregate.piFalsePositive += Number(metrics.piFalsePositive ?? 0);
      aggregate.piFalseNegative += Number(metrics.piFalseNegative ?? 0);
      aggregate.claimOverlap += Number(metrics.claimOverlap ?? 0);
      aggregate.rulesUnique += Number(metrics.rulesUnique ?? 0);
      aggregate.piUnique += Number(metrics.piUnique ?? 0);
      aggregate.routingDisagreement += Number(metrics.routingDisagreement ?? 0);
      aggregate.extractorDisagreement += Number(metrics.extractorDisagreement ?? (Number(metrics.rulesUnique ?? 0) + Number(metrics.piUnique ?? 0) + Number(metrics.routingDisagreement ?? 0)));
      aggregate.privacyBlocks += Number(metrics.privacyBlocks ?? 0);
      aggregate.schemaFailures += Number(metrics.schemaFailures ?? 0);
      aggregate.falsePositiveLabels += Number(metrics.falsePositiveLabels ?? 0);
      aggregate.falseNegativeLabels += Number(metrics.falseNegativeLabels ?? 0);
      coverageTotal += Number(metrics.evidenceCoverage ?? 0);
    }
    aggregate.avgEvidenceCoverage = rows.length ? Number((coverageTotal / rows.length).toFixed(4)) : 0;
    const denominator = Math.max(1, aggregate.claimOverlap + aggregate.rulesUnique + aggregate.piUnique);
    aggregate.routingDisagreementRate = Number((aggregate.routingDisagreement / denominator).toFixed(4));
    aggregate.extractorDisagreementRate = Number((aggregate.extractorDisagreement / denominator).toFixed(4));
    aggregate.rulesFalsePositiveRate = Number((aggregate.rulesFalsePositive / Math.max(1, aggregate.rulesClaimCount)).toFixed(4));
    aggregate.rulesFalseNegativeRate = Number((aggregate.rulesFalseNegative / Math.max(1, aggregate.expectedCount)).toFixed(4));
    aggregate.piFalsePositiveRate = Number((aggregate.piFalsePositive / Math.max(1, aggregate.piClaimCount)).toFixed(4));
    aggregate.piFalseNegativeRate = Number((aggregate.piFalseNegative / Math.max(1, aggregate.expectedCount)).toFixed(4));
    const reasonCodes: string[] = [];
    if (aggregate.reviewedCases < thresholds.minReviewedCases) reasonCodes.push('insufficient_reviewed_golden_cases');
    if (aggregate.comparisons < thresholds.minComparisons) reasonCodes.push('insufficient_shadow_comparisons');
    if (aggregate.schemaFailures > thresholds.maxSchemaFailures) reasonCodes.push('schema_failures_above_threshold');
    if (aggregate.privacyBlocks > thresholds.maxPrivacyBlocks) reasonCodes.push('privacy_blocks_above_threshold');
    if (aggregate.routingDisagreementRate > thresholds.maxRoutingDisagreementRate) reasonCodes.push('routing_disagreement_above_threshold');
    if (aggregate.avgEvidenceCoverage < thresholds.minEvidenceCoverage) reasonCodes.push('evidence_coverage_below_threshold');
    if (aggregate.falsePositiveLabels > thresholds.maxFalsePositiveLabels) reasonCodes.push('false_positive_labels_above_threshold');
    if (aggregate.falseNegativeLabels > thresholds.maxFalseNegativeLabels) reasonCodes.push('false_negative_labels_above_threshold');
    const windowHash = sha256(canonicalJsonStringify({ thresholds, aggregate }));
    const existing = this.db.prepare('SELECT * FROM km_shadow_readiness_reports WHERE window_hash=?').get(windowHash) as any;
    if (existing) return this.shadowReadinessFromRow(existing);
    const reportId = kmId('ready'); const now = new Date().toISOString();
    this.db.prepare(`INSERT INTO km_shadow_readiness_reports(report_id,window_hash,thresholds_json,metrics_json,ready,reason_codes_json,created_at)
      VALUES(?,?,?,?,?,?,?)`).run(reportId, windowHash, JSON.stringify(thresholds), JSON.stringify(aggregate), reasonCodes.length === 0 ? 1 : 0,
      JSON.stringify(reasonCodes), now);
    return this.shadowReadinessReportLatest()!;
  }

  shadowReadinessReportLatest(): KmShadowReadinessReport | null {
    const row = this.db.prepare('SELECT * FROM km_shadow_readiness_reports ORDER BY created_at DESC,report_id DESC LIMIT 1').get() as any;
    return row ? this.shadowReadinessFromRow(row) : null;
  }

  listPendingEvalTargets(input: { limit: number }): KmEvalTarget[] {
    const limit = Math.max(1, Math.min(input.limit, 500));
    const targets: KmEvalTarget[] = [];
    const push = (target: KmEvalTarget): void => {
      if (targets.length < limit) targets.push(target);
    };

    const distillationRows = this.db.prepare(`
      SELECT job_id,source_event_id,bot_app_id,profile_id,profile_revision,state,attempts,last_error,output_hash,created_at,updated_at
      FROM distillation_jobs j
      WHERE state IN ('completed','inconclusive','failed','quarantined')
        AND NOT EXISTS (
          SELECT 1 FROM eval_runs r
          WHERE r.evaluator_name='km.distillation-quality' AND r.evaluator_version='v1'
            AND r.target_type='turn' AND r.target_id=j.job_id
        )
      ORDER BY updated_at ASC,job_id ASC LIMIT ?
    `).all(limit) as any[];
    for (const row of distillationRows) push({
      sourceKind: 'distillation-job',
      targetType: 'turn',
      targetId: row.job_id,
      sourceRef: { kind: 'sqlite-row', ref: `distillation_jobs:${row.job_id}` },
      payload: {
        jobId: row.job_id,
        sourceEventId: row.source_event_id,
        botAppId: row.bot_app_id,
        profileId: row.profile_id,
        profileRevision: row.profile_revision,
        state: row.state,
        attempts: Number(row.attempts),
        ...(row.last_error ? { lastError: row.last_error } : {}),
        ...(row.output_hash ? { outputHash: row.output_hash } : {}),
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      },
    });

    if (targets.length >= limit) return targets;
    const retrievalRows = this.db.prepare(`
      SELECT retrieval_run_id,bot_app_id,session_id,turn_id,mode,candidate_count,eligible_count,latency_ms,warnings_json,created_at
      FROM retrieval_runs q
      WHERE NOT EXISTS (
        SELECT 1 FROM eval_runs r
        WHERE r.evaluator_name='km.retrieval-quality' AND r.evaluator_version='v1'
          AND r.target_type='turn' AND r.target_id=q.retrieval_run_id
      )
      ORDER BY created_at ASC,retrieval_run_id ASC LIMIT ?
    `).all(limit - targets.length) as any[];
    for (const row of retrievalRows) push({
      sourceKind: 'retrieval-run',
      targetType: 'turn',
      targetId: row.retrieval_run_id,
      sourceRef: { kind: 'sqlite-row', ref: `retrieval_runs:${row.retrieval_run_id}` },
      payload: {
        retrievalRunId: row.retrieval_run_id,
        botAppId: row.bot_app_id,
        sessionId: row.session_id,
        ...(row.turn_id ? { turnId: row.turn_id } : {}),
        mode: row.mode,
        candidateCount: Number(row.candidate_count),
        eligibleCount: Number(row.eligible_count),
        latencyMs: Number(row.latency_ms),
        warnings: JSON.parse(row.warnings_json),
        createdAt: row.created_at,
      },
    });

    if (targets.length >= limit) return targets;
    const injectionRows = this.db.prepare(`
      SELECT snapshot_id,retrieval_run_id,bot_app_id,mode,requested_mode,effective_mode,disposition,item_ids_json,prompt_hash,prompt_bytes,reason,created_at
      FROM prompt_injection_snapshots s
      WHERE NOT EXISTS (
        SELECT 1 FROM eval_runs r
        WHERE r.evaluator_name='km.injection-safety' AND r.evaluator_version='v1'
          AND r.target_type='turn' AND r.target_id=s.snapshot_id
      )
      ORDER BY created_at ASC,snapshot_id ASC LIMIT ?
    `).all(limit - targets.length) as any[];
    for (const row of injectionRows) push({
      sourceKind: 'prompt-injection',
      targetType: 'turn',
      targetId: row.snapshot_id,
      sourceRef: { kind: 'sqlite-row', ref: `prompt_injection_snapshots:${row.snapshot_id}` },
      payload: {
        snapshotId: row.snapshot_id,
        retrievalRunId: row.retrieval_run_id,
        botAppId: row.bot_app_id,
        mode: row.mode,
        requestedMode: row.requested_mode ?? row.mode,
        effectiveMode: row.effective_mode ?? row.mode,
        disposition: row.disposition,
        itemIds: JSON.parse(row.item_ids_json),
        ...(row.prompt_hash ? { promptHash: row.prompt_hash } : {}),
        promptBytes: Number(row.prompt_bytes),
        ...(row.reason ? { reason: row.reason } : {}),
        createdAt: row.created_at,
      },
    });

    if (targets.length >= limit) return targets;
    const memoryRows = this.db.prepare(`
      SELECT decision_id,source_event_id,memory_id,policy_version,disposition,reason_codes_json,evidence_json,created_at
      FROM km_memory_policy_decisions d
      WHERE NOT EXISTS (
        SELECT 1 FROM eval_runs r
        WHERE r.evaluator_name='km.memory-policy-quality' AND r.evaluator_version='v1'
          AND r.target_type='memory' AND r.target_id=d.decision_id
      )
      ORDER BY created_at ASC,decision_id ASC LIMIT ?
    `).all(limit - targets.length) as any[];
    for (const row of memoryRows) push({
      sourceKind: 'memory-policy-decision',
      targetType: 'memory',
      targetId: row.decision_id,
      sourceRef: { kind: 'sqlite-row', ref: `km_memory_policy_decisions:${row.decision_id}` },
      payload: {
        decisionId: row.decision_id,
        sourceEventId: row.source_event_id,
        ...(row.memory_id ? { memoryId: row.memory_id } : {}),
        policyVersion: row.policy_version,
        disposition: row.disposition,
        reasonCodes: JSON.parse(row.reason_codes_json),
        evidence: JSON.parse(row.evidence_json),
        createdAt: row.created_at,
      },
    });

    if (targets.length >= limit) return targets;
    const artifactRows = this.db.prepare(`
      SELECT event_id,event_json
      FROM observation_events o
      WHERE event_type='workflow.artifact.produced'
        AND NOT EXISTS (
          SELECT 1 FROM eval_runs r
          WHERE r.evaluator_name='km.workflow-artifact-quality' AND r.evaluator_version='v1'
            AND r.target_type='workflow-artifact' AND r.target_id=o.event_id
        )
      ORDER BY local_seq ASC,event_id ASC LIMIT ?
    `).all(limit - targets.length) as any[];
    for (const row of artifactRows) {
      const event = ObservationEventSchema.parse(JSON.parse(row.event_json));
      push({
        sourceKind: 'workflow-artifact',
        targetType: 'workflow-artifact',
        targetId: row.event_id,
        sourceRef: {
          kind: 'workflow-artifact',
          ref: String(event.provenance.sourceRefs[0]?.ref ?? row.event_id),
          sha256: event.content.hash,
        },
        payload: {
          eventId: event.eventId,
          botAppId: event.identity.botAppId,
          sessionId: event.identity.sessionId,
          workflowId: event.identity.workflowId,
          nodeId: event.identity.nodeId,
          attemptId: event.identity.attemptId,
          outputKey: event.payload.outputKey,
          path: event.payload.path,
          kind: event.payload.kind,
          bytes: event.payload.bytes,
          sha256: typeof event.content.hash === 'string' ? event.content.hash.replace(/^sha256:/u, '') : undefined,
          promptRequirements: Array.isArray(event.payload.promptRequirements) ? event.payload.promptRequirements : [],
          coveredRequirements: Array.isArray(event.payload.coveredRequirements) ? event.payload.coveredRequirements : [],
        },
      });
    }
    return targets;
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
    return this.retrieveWithMetrics(input).items;
  }

  retrieveWithMetrics(input: RetrievalQuery): RetrievalResultSet {
    const limit = Math.max(1, Math.min(input.limit, 100));
    const query = normalizeRetrievalQuery(input.text);
    const items: RetrievalItem[] = [];
    const metrics: RetrievalQualityCounters = {
      directHitCount: 0,
      normalizedHitCount: 0,
      noHitCount: 0,
      filteredScopeCount: 0,
      filteredPrivacyCount: 0,
      filteredStateCount: 0,
    };

    for (const item of this.listKnowledge({ limit: 500 })) {
      if (item.state !== 'approved') { metrics.filteredStateCount += 1; continue; }
      if (input.targetLayers?.length && !input.targetLayers.includes(item.targetLayer)) { metrics.filteredScopeCount += 1; continue; }
      if (item.freshness === 'stale' || item.freshness === 'purged') { metrics.filteredStateCount += 1; continue; }
      if (item.privacyClass === 'sensitive' || item.privacyClass === 'secret-reference-only') { metrics.filteredPrivacyCount += 1; continue; }
      const scored = scoreNormalizedQuery(query, `${item.title} ${item.claimKey} ${item.claimText}`);
      if (query.groups.length && scored.score === 0) { metrics.noHitCount += 1; continue; }
      if (scored.matchKind === 'normalized') metrics.normalizedHitCount += 1; else metrics.directHitCount += 1;
      items.push({ id: item.knowledgeId, kind: 'knowledge', title: item.title, text: item.claimText,
        score: scored.score, sourceRefs: item.sourceRefs, privacyClass: item.privacyClass, freshness: item.freshness,
        matchKind: scored.matchKind, matchedGroups: scored.matchedGroups });
    }

    for (const item of this.listMemory({ limit: 500 })) {
      if (input.scopes && !input.scopes.includes(item.scope)) { metrics.filteredScopeCount += 1; continue; }
      const expectedSubject = retrievalExpectedSubject(input, item.scope);
      if (!expectedSubject || expectedSubject !== item.subject) { metrics.filteredScopeCount += 1; continue; }
      if (item.state !== 'active') { metrics.filteredStateCount += 1; continue; }
      if (item.ttlExpiresAt && Date.parse(item.ttlExpiresAt) <= Date.now()) { metrics.filteredStateCount += 1; continue; }
      if (item.privacyClass === 'sensitive' || item.privacyClass === 'secret-reference-only') { metrics.filteredPrivacyCount += 1; continue; }
      const scored = scoreNormalizedQuery(query, `${item.claimKey} ${item.claimText}`);
      if (query.groups.length && scored.score === 0) { metrics.noHitCount += 1; continue; }
      if (scored.matchKind === 'normalized') metrics.normalizedHitCount += 1; else metrics.directHitCount += 1;
      items.push({ id: item.memoryId, kind: 'memory', title: item.claimKey, text: item.claimText,
        score: scored.score, sourceRefs: item.sourceRefs, privacyClass: item.privacyClass, freshness: 'fresh',
        scope: item.scope, subject: item.subject, matchKind: scored.matchKind, matchedGroups: scored.matchedGroups });
    }
    return { items: items.sort((a, b) => b.score - a.score || (b.matchedGroups ?? 0) - (a.matchedGroups ?? 0) || a.id.localeCompare(b.id)).slice(0, limit),
      metrics };
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

  evalEvolutionStatus(): KmEvalEvolutionStatus {
    const evalRow = this.db.prepare(`
      SELECT COUNT(*) eval_runs,
        SUM(CASE WHEN EXISTS (
          SELECT 1 FROM eval_results x WHERE x.eval_run_id=r.eval_run_id AND x.verdict='fail'
        ) THEN 1 ELSE 0 END) failing_eval_runs,
        MAX(r.updated_at) latest_eval_at
      FROM eval_runs r
    `).get() as any;
    const proposalRow = this.db.prepare(`
      SELECT COUNT(*) review_pending_proposals,MAX(updated_at) latest_proposal_at
      FROM evolution_proposals WHERE state='review_pending'
    `).get() as any;
    return {
      evalRuns: Number(evalRow.eval_runs ?? 0),
      failingEvalRuns: Number(evalRow.failing_eval_runs ?? 0),
      reviewPendingProposals: Number(proposalRow.review_pending_proposals ?? 0),
      ...(evalRow.latest_eval_at ? { latestEvalAt: String(evalRow.latest_eval_at) } : {}),
      ...(proposalRow.latest_proposal_at ? { latestProposalAt: String(proposalRow.latest_proposal_at) } : {}),
    };
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

  findReviewPendingEvolutionProposal(input: { proposalType: EvolutionProposalInput['proposalType']; targetRef: string; evidenceRefs: unknown[]; proposedAction: Record<string, unknown> }): string | undefined {
    const evidence = JSON.stringify(input.evidenceRefs);
    const action = JSON.stringify(input.proposedAction);
    const row = this.db.prepare(`SELECT proposal_id FROM evolution_proposals
      WHERE state='review_pending' AND proposal_type=? AND target_ref=? AND evidence_refs_json=? AND proposed_action_json=?
      ORDER BY created_at DESC,proposal_id DESC LIMIT 1`)
      .get(input.proposalType, input.targetRef, evidence, action) as { proposal_id: string } | undefined;
    return row?.proposal_id;
  }

  createEvolutionProposalOnce(input: EvolutionProposalInput): { proposalId: string; created: boolean } {
    const existing = this.findReviewPendingEvolutionProposal(input);
    if (existing) return { proposalId: existing, created: false };
    return { proposalId: this.createEvolutionProposal(input), created: true };
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
    const endpointRef = requireText(input.endpointRef, 'sink_endpoint');
    const endpointPolicy = syncEndpointPolicy(endpointRef);
    if (input.enabled && !endpointPolicy.ok) throw new Error('km_sync_real_sink_blocked_offline_runtime');
    const sinkId = requireText(input.sinkId, 'sink_id');
    const batchLimit = boundedInt(input.batchLimit, 25, 1, 100);
    const timeoutMs = boundedInt(input.timeoutMs, 5_000, 100, 30_000);
    const maxAttempts = boundedInt(input.maxAttempts, 5, 1, 50);
    const payloadMaxBytes = boundedInt(input.payloadMaxBytes, 64 * 1024, 1_024, 256 * 1024);
    const allowlist = (input.allowlist ?? []).map(item => item.trim()).filter(Boolean);
    const credentialRef = input.credentialRef?.trim();
    if (credentialRef && !/^env:[A-Z_][A-Z0-9_]*$/.test(credentialRef) && !/^file:\/[^\s]+$/.test(credentialRef)) {
      throw new Error('km_sync_credential_ref_required');
    }
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      this.db.prepare(`INSERT INTO sync_sinks(
          sink_id,protocol_version,endpoint_ref,enabled,redaction_policy_json,batch_limit,timeout_ms,max_attempts,
          credential_ref,allowlist_json,tls_policy,payload_max_bytes,rollback_json,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(sink_id) DO UPDATE SET protocol_version=excluded.protocol_version,
          endpoint_ref=excluded.endpoint_ref,enabled=excluded.enabled,redaction_policy_json=excluded.redaction_policy_json,
          batch_limit=excluded.batch_limit,timeout_ms=excluded.timeout_ms,max_attempts=excluded.max_attempts,
          credential_ref=excluded.credential_ref,allowlist_json=excluded.allowlist_json,tls_policy=excluded.tls_policy,
          payload_max_bytes=excluded.payload_max_bytes,rollback_json=excluded.rollback_json,updated_at=excluded.updated_at`)
        .run(sinkId, input.protocolVersion, endpointRef, input.enabled ? 1 : 0,
          JSON.stringify(input.redactionPolicy ?? {}), batchLimit, timeoutMs, maxAttempts, credentialRef ?? null,
          JSON.stringify(allowlist), 'https-required-for-future-real-transport', payloadMaxBytes, JSON.stringify(input.rollback ?? {}), now, now);
      this.db.prepare(`INSERT OR IGNORE INTO sync_cursors(sink_id,last_local_seq,status,updated_at) VALUES(?,0,'idle',?)`).run(sinkId, now);
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
    return this.listSyncStatus().find(item => item.sinkId === sinkId)!;
  }

  enqueueSync(input: { sinkId: string; eventId: string; payload: Record<string, unknown>; payloadHash: string; now?: number }): { outboxId: string; created: boolean } {
    const sink = this.db.prepare('SELECT enabled FROM sync_sinks WHERE sink_id=?').get(input.sinkId) as { enabled: number } | undefined;
    if (!sink) throw new Error('km_sync_sink_not_found');
    if (!sink.enabled) throw new Error('km_sync_sink_disabled');
    const outboxId = `outbox_${createHash('sha256').update(`${input.sinkId}:${input.eventId}`).digest('hex')}`;
    const now = new Date().toISOString();
    const bytes = Buffer.byteLength(JSON.stringify(input.payload));
    const cfg = this.db.prepare('SELECT payload_max_bytes FROM sync_sinks WHERE sink_id=?').get(input.sinkId) as { payload_max_bytes: number } | undefined;
    if (cfg && bytes > Number(cfg.payload_max_bytes)) throw new Error('km_sync_payload_too_large');
    const result = this.db.prepare(`INSERT OR IGNORE INTO sync_outbox(outbox_id,event_id,sink_id,status,attempts,next_attempt_at,created_at)
      VALUES(?,?,?,'pending',0,?,?)`).run(outboxId, input.eventId, input.sinkId, input.now ?? Date.now(), now);
    if (Number(result.changes) === 1) this.db.prepare('UPDATE sync_outbox SET payload_json=?,payload_hash=? WHERE outbox_id=?')
      .run(JSON.stringify(input.payload), input.payloadHash, outboxId);
    return { outboxId, created: Number(result.changes) === 1 };
  }

  enqueueSyncFromCursor(input: { sinkId: string; limit: number; now?: number; redact: (event: ObservationEvent) => { ok: true; envelope: Record<string, unknown>; payloadHash: string } | { ok: false; reason: string } }):
    { scanned: number; enqueued: number; skipped: number; quarantined: number } {
    const status = this.listSyncStatus().find(item => item.sinkId === input.sinkId);
    if (!status) throw new Error('km_sync_sink_not_found');
    if (!status.enabled) return { scanned: 0, enqueued: 0, skipped: 0, quarantined: 0 };
    const limit = Math.max(1, Math.min(input.limit, 500));
    const rows = this.db.prepare(`SELECT local_seq,event_id,event_json FROM observation_events WHERE local_seq>?
      ORDER BY local_seq ASC LIMIT ?`).all(status.lastLocalSeq, limit) as any[];
    let enqueued = 0; let skipped = 0; let quarantined = 0;
    for (const row of rows) {
      const event = ObservationEventSchema.parse(JSON.parse(row.event_json));
      const redacted = input.redact(event);
      if (!redacted.ok) {
        this.quarantineSync({ sinkId: input.sinkId, eventId: event.eventId, reason: redacted.reason, payloadHash: `sha256:${'0'.repeat(64)}` });
        this.db.prepare(`UPDATE sync_cursors SET last_local_seq=?,status='idle',updated_at=? WHERE sink_id=? AND last_local_seq<?`)
          .run(Number(row.local_seq), new Date(input.now ?? Date.now()).toISOString(), input.sinkId, Number(row.local_seq));
        quarantined += 1;
        continue;
      }
      try {
        if (this.enqueueSync({ sinkId: input.sinkId, eventId: event.eventId, payload: redacted.envelope, payloadHash: redacted.payloadHash, now: input.now }).created) enqueued += 1;
        else skipped += 1;
      } catch {
        this.quarantineSync({ sinkId: input.sinkId, eventId: event.eventId, reason: 'enqueue_failed', payloadHash: redacted.payloadHash });
        this.db.prepare(`UPDATE sync_cursors SET last_local_seq=?,status='idle',updated_at=? WHERE sink_id=? AND last_local_seq<?`)
          .run(Number(row.local_seq), new Date(input.now ?? Date.now()).toISOString(), input.sinkId, Number(row.local_seq));
        quarantined += 1;
      }
    }
    return { scanned: rows.length, enqueued, skipped, quarantined };
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

  failSyncClaim(input: { claimToken: string; error: string; now?: number; baseDelayMs?: number; maxAttempts?: number }): void {
    const now = input.now ?? Date.now();
    const rows = this.db.prepare(`SELECT outbox_id,sink_id,event_id,attempts,payload_hash FROM sync_outbox WHERE claim_token=? AND status='inflight'`).all(input.claimToken) as any[];
    const update = this.db.prepare(`UPDATE sync_outbox SET status=?,claim_token=NULL,claimed_at=NULL,last_error=?,next_attempt_at=? WHERE outbox_id=?`);
    for (const row of rows) {
      const terminal = Number(row.attempts) >= Math.max(1, input.maxAttempts ?? 5);
      const delay = Math.min(300_000, (input.baseDelayMs ?? 1_000) * 2 ** Math.max(0, Number(row.attempts) - 1));
      update.run(terminal ? 'quarantined' : 'failed', input.error.slice(0, 500), terminal ? now : now + delay, row.outbox_id);
      if (terminal) {
        this.quarantineSync({ sinkId: String(row.sink_id), eventId: String(row.event_id), reason: `max_attempts_exceeded:${input.error}`.slice(0, 500), payloadHash: String(row.payload_hash ?? 'unknown') });
      }
    }
  }

  acknowledgeSync(input: { sinkId: string; batchId: string; acceptedEventIds: string[]; centralCursor?: string }): void {
    const now = new Date().toISOString();
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const delivered = this.db.prepare(`UPDATE sync_outbox SET status='delivered',claim_token=NULL,claimed_at=NULL,last_error=NULL,delivered_at=? WHERE sink_id=? AND event_id=?`);
      for (const eventId of input.acceptedEventIds) delivered.run(now, input.sinkId, eventId);
      const row = this.db.prepare(`SELECT COALESCE(MAX(o.local_seq),0) max_seq
        FROM observation_events o JOIN sync_outbox x ON x.event_id=o.event_id
        WHERE x.sink_id=? AND x.status IN ('delivered','quarantined')`)
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
    if (input.eventId) {
      this.db.prepare(`UPDATE sync_outbox SET status='quarantined',claim_token=NULL,claimed_at=NULL,last_error=? WHERE sink_id=? AND event_id=?`)
        .run(input.reason, input.sinkId, input.eventId);
      const row = this.db.prepare(`SELECT COALESCE(MAX(o.local_seq),0) max_seq
        FROM observation_events o JOIN sync_outbox x ON x.event_id=o.event_id
        WHERE x.sink_id=? AND x.status IN ('delivered','quarantined')`).get(input.sinkId) as { max_seq: number };
      this.db.prepare(`UPDATE sync_cursors SET last_local_seq=?,status='idle',updated_at=? WHERE sink_id=? AND last_local_seq<?`)
        .run(Number(row.max_seq), new Date().toISOString(), input.sinkId, Number(row.max_seq));
    }
    return id;
  }

  listSyncStatus(input: { redactCredentials?: boolean } = {}): SyncStatus[] {
    const redactCredentials = input.redactCredentials ?? true;
    const rows = this.db.prepare(`SELECT s.*,c.last_local_seq,c.last_batch_id,c.last_ack_at,c.central_cursor,c.status,
      (SELECT COUNT(*) FROM sync_outbox x WHERE x.sink_id=s.sink_id AND x.status IN ('pending','failed')) pending,
      (SELECT COUNT(*) FROM sync_outbox x WHERE x.sink_id=s.sink_id AND x.status='inflight') inflight,
      (SELECT COUNT(*) FROM sync_outbox x WHERE x.sink_id=s.sink_id AND x.status='failed') failed,
      (SELECT COUNT(*) FROM sync_outbox x WHERE x.sink_id=s.sink_id AND x.status='delivered') delivered,
      (SELECT COUNT(*) FROM sync_quarantine q WHERE q.sink_id=s.sink_id AND q.resolved_at IS NULL) quarantined
      FROM sync_sinks s JOIN sync_cursors c ON c.sink_id=s.sink_id ORDER BY s.sink_id`).all() as any[];
    return rows.map(row => ({ sinkId: row.sink_id, endpointRef: row.endpoint_ref, enabled: Boolean(row.enabled), status: row.status,
      lastLocalSeq: Number(row.last_local_seq), ...(row.last_batch_id ? { lastBatchId: row.last_batch_id } : {}),
      ...(row.last_ack_at ? { lastAckAt: row.last_ack_at } : {}), ...(row.central_cursor ? { centralCursor: row.central_cursor } : {}),
      pending: Number(row.pending), inflight: Number(row.inflight), failed: Number(row.failed), delivered: Number(row.delivered),
      quarantined: Number(row.quarantined), endpointPolicy: syncEndpointPolicy(String(row.endpoint_ref)),
      protocolVersion: Number(row.protocol_version), batchLimit: Number(row.batch_limit ?? 25),
      timeoutMs: Number(row.timeout_ms ?? 5000), maxAttempts: Number(row.max_attempts ?? 5),
      payloadMaxBytes: Number(row.payload_max_bytes ?? 65536),
      ...(row.credential_ref ? { credentialRef: redactCredentials
        ? String(row.credential_ref).replace(/^(env|file):(.+)$/, (_m: string, kind: string) => `${kind}:***`)
        : String(row.credential_ref) } : {}),
      allowlist: parseJsonArray(row.allowlist_json ?? '[]').map(String),
      tlsPolicy: String(row.tls_policy ?? 'https-required-for-future-real-transport'),
      rollback: parseJsonObject(row.rollback_json ?? '{}') }));
  }

  listSyncOutbox(input: { sinkId?: string; limit: number }): SyncOutboxRow[] {
    const limit = Math.max(1, Math.min(input.limit, 500));
    const rows = (input.sinkId
      ? this.db.prepare(`SELECT * FROM sync_outbox WHERE sink_id=? ORDER BY created_at DESC,outbox_id DESC LIMIT ?`).all(input.sinkId, limit)
      : this.db.prepare(`SELECT * FROM sync_outbox ORDER BY created_at DESC,outbox_id DESC LIMIT ?`).all(limit)) as any[];
    return rows.map(row => ({
      outboxId: row.outbox_id,
      eventId: row.event_id,
      sinkId: row.sink_id,
      status: row.status,
      attempts: Number(row.attempts),
      nextAttemptAt: Number(row.next_attempt_at),
      ...(row.claimed_at ? { claimedAt: Number(row.claimed_at) } : {}),
      ...(row.claim_token ? { claimToken: String(row.claim_token) } : {}),
      ...(row.last_error ? { lastError: String(row.last_error) } : {}),
      ...(row.delivered_at ? { deliveredAt: String(row.delivered_at) } : {}),
      payload: parseJsonObject(row.payload_json ?? '{}'),
      payloadHash: String(row.payload_hash ?? ''),
      createdAt: row.created_at,
    }));
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

  getKmMutationReplay<T>(input: { actorId: string; idempotencyKey: string; route: string; requestHash: string }): { statusCode: number; response: T; replayed: true } | null {
    const row = this.db.prepare(`SELECT route,request_hash,status_code,response_json FROM km_mutation_idempotency WHERE actor_id=? AND idempotency_key=?`)
      .get(input.actorId, input.idempotencyKey) as any;
    if (!row) return null;
    if (row.route !== input.route || row.request_hash !== input.requestHash) throw new Error('km_idempotency_conflict');
    return { statusCode: Number(row.status_code), response: JSON.parse(row.response_json) as T, replayed: true };
  }

  recordKmMutation<T>(input: { actorId: string; idempotencyKey: string; route: string; requestHash: string; statusCode: number;
    action: string; targetRef: string; response: T; beforeHash?: string; afterHash?: string }): { statusCode: number; response: T; replayed: false } {
    const now = new Date().toISOString();
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const existing = this.db.prepare(`SELECT route,request_hash,status_code,response_json FROM km_mutation_idempotency WHERE actor_id=? AND idempotency_key=?`)
        .get(input.actorId, input.idempotencyKey) as any;
      if (existing) {
        if (existing.route !== input.route || existing.request_hash !== input.requestHash) throw new Error('km_idempotency_conflict');
        this.db.exec('COMMIT;');
        return { statusCode: Number(existing.status_code), response: JSON.parse(existing.response_json) as T, replayed: false };
      }
      this.db.prepare(`INSERT INTO km_mutation_idempotency(actor_id,idempotency_key,route,request_hash,status_code,response_json,created_at)
        VALUES(?,?,?,?,?,?,?)`).run(input.actorId, input.idempotencyKey, input.route, input.requestHash, input.statusCode, JSON.stringify(input.response), now);
      this.db.prepare(`INSERT INTO km_config_audit(audit_id,actor_id,action,target_ref,before_hash,after_hash,request_hash,idempotency_key,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(`kma_${randomUUID().replaceAll('-', '')}`, input.actorId, input.action, input.targetRef,
        input.beforeHash ?? null, input.afterHash ?? null, input.requestHash, input.idempotencyKey, now);
      this.db.exec('COMMIT;');
      return { statusCode: input.statusCode, response: input.response, replayed: false };
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  putKmIngestTarget(input: { config: KmIngestTargetConfig; actorId: string }): KmIngestTargetRecord {
    const actorId = requireText(input.actorId, 'ingest_actor');
    const targetId = requireText(input.config.targetId, 'ingest_target_id');
    const endpointRef = requireText(input.config.endpointRef, 'ingest_target_endpoint');
    if (!endpointRef.startsWith('mock:') && !endpointRef.startsWith('file:/')) throw new Error('km_ingest_target_offline_endpoint_required');
    const credentialRef = requireText(input.config.credentialRef, 'ingest_target_credential_ref');
    if (!credentialRef.startsWith('mock:') && !credentialRef.startsWith('file:/') && !credentialRef.startsWith('env:')) throw new Error('km_ingest_target_credential_ref_invalid');
    const allowedProviderIds = [...new Set((input.config.allowedProviderIds ?? []).map(value => requireText(value, 'ingest_target_provider')))]
      .sort((a, b) => a.localeCompare(b));
    const target = {
      endpointRef,
      dryRunOnly: input.config.dryRunOnly !== false,
      allowedProviderIds,
      ...(input.config.markIngestedCommand?.trim() ? { markIngestedCommand: input.config.markIngestedCommand.trim() } : {}),
    };
    const targetHash = sha256(canonicalJsonStringify({ ...target, credentialRef }));
    const now = new Date().toISOString();
    this.db.prepare(`INSERT INTO km_ingest_targets(target_id,state,target_json,target_hash,credential_ref,created_by,created_at,updated_at)
      VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(target_id) DO UPDATE SET
      state=excluded.state,target_json=excluded.target_json,target_hash=excluded.target_hash,
      credential_ref=excluded.credential_ref,updated_at=excluded.updated_at`)
      .run(targetId, input.config.enabled === true ? 'ready' : 'disabled',
        canonicalJsonStringify(target), targetHash, credentialRef, actorId, now, now);
    return this.getKmIngestTarget(targetId)!;
  }

  getKmIngestTarget(targetId: string): KmIngestTargetRecord | null {
    const row = this.db.prepare('SELECT * FROM km_ingest_targets WHERE target_id=?').get(targetId) as any;
    return row ? this.kmIngestTargetFromRow(row) : null;
  }

  listKmIngestTargets(limit = 50): KmIngestTargetRecord[] {
    const rows = this.db.prepare('SELECT * FROM km_ingest_targets ORDER BY updated_at DESC,target_id ASC LIMIT ?')
      .all(Math.max(1, Math.min(limit, 200))) as any[];
    return rows.map(row => this.kmIngestTargetFromRow(row));
  }

  createKmIngestRun(input: KmIngestRunCreateInput): KmIngestRunReport {
    const actorId = requireText(input.actorId, 'ingest_actor');
    const idempotencyKey = requireText(input.idempotencyKey, 'ingest_idempotency_key');
    const targetId = requireText(input.targetId, 'ingest_target_id');
    if (input.plan.schemaVersion !== 1 || input.plan.targetId !== targetId || input.plan.mode !== 'offline' || input.plan.dryRun !== true) {
      throw new Error('km_ingest_plan_contract_invalid');
    }
    if (input.plan.planCalls.markIngested !== false) throw new Error('km_ingest_mark_ingested_requires_external_ack');
    const planJson = canonicalJsonStringify(input.plan);
    const planHash = sha256(planJson);
    const keySetHash = sha256(canonicalJsonStringify(input.plan.canonicalKeys));
    const existing = this.db.prepare('SELECT * FROM km_ingest_runs WHERE idempotency_key=?').get(idempotencyKey) as any;
    if (existing) {
      if (existing.plan_hash !== planHash) throw new Error('km_ingest_idempotency_conflict');
      return this.getKmIngestRunReport(existing.run_id)!;
    }
    const now = new Date().toISOString();
    const runId = `kmir_${createHash('sha256').update(`${idempotencyKey}|${targetId}|${planHash}`).digest('hex')}`;
    const normalizedItems = normalizeKmIngestItems(runId, input.items);
    const itemKeys = normalizedItems.map(item => item.canonicalKey).sort((a, b) => a.localeCompare(b));
    if (canonicalJsonStringify(itemKeys) !== canonicalJsonStringify(input.plan.canonicalKeys)) throw new Error('km_ingest_canonical_key_set_mismatch');
    const stats = this.computeKmIngestStats(normalizedItems);
    this.db.exec('SAVEPOINT km_ingest_plan;');
    try {
      this.db.prepare(`INSERT INTO km_ingest_runs(
        run_id,idempotency_key,state,target_id,plan_json,plan_hash,canonical_key_set_hash,confirmation_token_hash,
        source_count,eligible_count,ingested_count,deduped_count,skipped_count,failed_count,rollback_count,
        mark_ingested_planned_count,checkpoint,created_by,created_at,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(runId, idempotencyKey, 'planned', targetId,
        planJson, planHash, keySetHash, requireText(input.confirmationTokenHash, 'ingest_confirmation_hash'),
        stats.sourceCount, stats.eligibleCount, 0, 0, stats.skippedCount, stats.failedCount, 0,
        stats.markIngestedPlannedCount, null, actorId, now, now);
      const insert = this.db.prepare(`INSERT INTO km_ingest_items(
        ingest_item_id,run_id,canonical_key,candidate_json,candidate_hash,state,reason_code,knowledge_id,mark_ingested_plan_json,created_at,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?)`);
      for (const item of normalizedItems) insert.run(item.ingestItemId, runId, item.canonicalKey,
        canonicalJsonStringify(item.candidate), item.candidateHash, item.state, item.reasonCode ?? null, null,
        item.markIngestedPlan ? canonicalJsonStringify(item.markIngestedPlan) : null, now, now);
      this.insertKmIngestAudit(runId, 'plan.created', actorId, {
        targetId, planHash, canonicalKeySetHash: keySetHash,
        sourceCount: stats.sourceCount, eligibleCount: stats.eligibleCount,
      }, now);
      this.db.exec('RELEASE km_ingest_plan;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_ingest_plan; RELEASE km_ingest_plan;'); } catch {} throw error; }
    return this.getKmIngestRunReport(runId)!;
  }

  transitionKmIngestRun(input: {
    runId: string; toState: KmIngestRunState; actorId: string; action: string;
    details?: Record<string, unknown>; expectedPlanHash?: string; externalAck?: Record<string, unknown>; lastError?: string;
  }): KmIngestRunRecord {
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_ingest_transition;');
    try {
      const row = this.db.prepare('SELECT * FROM km_ingest_runs WHERE run_id=?').get(input.runId) as any;
      if (!row) throw new Error('km_ingest_run_not_found');
      if (input.expectedPlanHash && input.expectedPlanHash !== row.plan_hash) throw new Error('km_ingest_plan_hash_mismatch');
      assertKmIngestRunTransition(row.state, input.toState);
      this.db.prepare(`UPDATE km_ingest_runs SET state=?,external_ack_json=COALESCE(?,external_ack_json),
        approved_by=CASE WHEN ?='approved' THEN ? ELSE approved_by END,
        last_error=?,updated_at=?,started_at=CASE WHEN ?='running' THEN COALESCE(started_at,?) ELSE started_at END,
        completed_at=CASE WHEN ? IN ('completed','partial','blocked','failed') THEN ? ELSE completed_at END,
        rolled_back_at=CASE WHEN ?='rolled_back' THEN ? ELSE rolled_back_at END
        WHERE run_id=?`)
        .run(input.toState, input.externalAck ? canonicalJsonStringify(input.externalAck) : null,
          input.toState, input.actorId, input.lastError ?? null, now,
          input.toState, now, input.toState, now, input.toState, now, input.runId);
      this.insertKmIngestAudit(input.runId, input.action, input.actorId, input.details ?? {}, now);
      this.db.exec('RELEASE km_ingest_transition;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_ingest_transition; RELEASE km_ingest_transition;'); } catch {} throw error; }
    return this.getKmIngestRun(input.runId)!;
  }

  runKmIngestOffline(input: { runId: string; actorId: string; maxItems?: number }): KmIngestRunReport {
    const actorId = requireText(input.actorId, 'ingest_actor');
    const run = this.getKmIngestRun(input.runId);
    if (!run) throw new Error('km_ingest_run_not_found');
    if (!['approved','partial','failed'].includes(run.state)) throw new Error(`km_ingest_execution_requires_approval:${run.state}`);
    const limit = Math.max(1, Math.min(input.maxItems ?? 50, 100));
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_ingest_run;');
    try {
      this.db.prepare(`UPDATE km_ingest_runs SET state='running',started_at=COALESCE(started_at,?),updated_at=?,last_error=NULL WHERE run_id=?`)
        .run(now, now, input.runId);
      this.insertKmIngestAudit(input.runId, 'execution.started', actorId, { limit, offline: true }, now);
      const rows = this.db.prepare(`SELECT * FROM km_ingest_items WHERE run_id=? AND state IN ('pending','failed')
        ORDER BY ingest_item_id ASC LIMIT ?`).all(input.runId, limit) as any[];
      for (const row of rows) {
        try { this.applyKmIngestItem(row, run, actorId, now); }
        catch (error) { this.markKmIngestItem(String(row.ingest_item_id), 'failed', error instanceof Error ? error.message : String(error), undefined, undefined, now); }
      }
      this.refreshKmIngestRun(input.runId, rows.at(-1)?.ingest_item_id ? String(rows.at(-1).ingest_item_id) : run.checkpoint, now);
      this.insertKmIngestAudit(input.runId, 'execution.finished', actorId, { processed: rows.length, offline: true }, now);
      this.db.exec('RELEASE km_ingest_run;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_ingest_run; RELEASE km_ingest_run;'); } catch {} throw error; }
    return this.getKmIngestRunReport(input.runId)!;
  }

  rollbackKmIngestRun(input: { runId: string; actorId: string; expectedPlanHash?: string; reasonCode: string }): KmIngestRunReport {
    const actorId = requireText(input.actorId, 'ingest_actor');
    const run = this.getKmIngestRun(input.runId);
    if (!run) throw new Error('km_ingest_run_not_found');
    if (input.expectedPlanHash && input.expectedPlanHash !== run.planHash) throw new Error('km_ingest_plan_hash_mismatch');
    if (!['partial','completed','failed'].includes(run.state)) throw new Error(`km_ingest_rollback_not_allowed:${run.state}`);
    const reasonCode = requireText(input.reasonCode, 'ingest_rollback_reason');
    const now = new Date().toISOString();
    this.db.exec('SAVEPOINT km_ingest_rollback;');
    try {
      const rows = this.db.prepare(`SELECT * FROM km_ingest_items WHERE run_id=? AND state='ingested' ORDER BY ingest_item_id ASC`)
        .all(input.runId) as any[];
      for (const row of rows) {
        if (row.knowledge_id) {
          const knowledge = this.db.prepare('SELECT state FROM knowledge_items WHERE knowledge_id=?').get(row.knowledge_id) as { state: string } | undefined;
          if (knowledge && knowledge.state === 'candidate') {
            this.db.prepare('UPDATE knowledge_items SET state=?,updated_at=? WHERE knowledge_id=?').run('rejected', now, row.knowledge_id);
            this.db.prepare(`INSERT INTO knowledge_state_history(history_id,knowledge_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
              VALUES(?,?,?,?,?,?,?,?)`).run(kmId('hist'), row.knowledge_id, 'candidate', 'rejected', `km_ingest_rollback:${reasonCode}`, actorId, null, now);
          }
        }
        this.markKmIngestItem(String(row.ingest_item_id), 'rolled_back', reasonCode, row.knowledge_id ? String(row.knowledge_id) : undefined, undefined, now);
      }
      this.refreshKmIngestRun(input.runId, run.checkpoint, now, 'rolled_back');
      this.insertKmIngestAudit(input.runId, 'rollback.finished', actorId, { reasonCode, rolledBack: rows.length }, now);
      this.db.exec('RELEASE km_ingest_rollback;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_ingest_rollback; RELEASE km_ingest_rollback;'); } catch {} throw error; }
    return this.getKmIngestRunReport(input.runId)!;
  }

  getKmIngestRun(runId: string): KmIngestRunRecord | null {
    const row = this.db.prepare('SELECT * FROM km_ingest_runs WHERE run_id=?').get(runId) as any;
    return row ? this.kmIngestRunFromRow(row) : null;
  }

  listKmIngestRuns(input: { limit?: number; targetId?: string; state?: KmIngestRunState } = {}): KmIngestRunRecord[] {
    const limit = Math.max(1, Math.min(input.limit ?? 50, 200));
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (input.targetId) { where.push('target_id=?'); args.push(input.targetId); }
    if (input.state) { where.push('state=?'); args.push(input.state); }
    const clause = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const rows = this.db.prepare(`SELECT * FROM km_ingest_runs ${clause} ORDER BY updated_at DESC,run_id DESC LIMIT ?`)
      .all(...args, limit) as any[];
    return rows.map(row => this.kmIngestRunFromRow(row));
  }

  listKmIngestItems(input: { runId: string; limit?: number; state?: KmIngestItemState }): KmIngestItemRecord[] {
    const limit = Math.max(1, Math.min(input.limit ?? 100, 500));
    const rows = input.state
      ? this.db.prepare(`SELECT * FROM km_ingest_items WHERE run_id=? AND state=? ORDER BY ingest_item_id ASC LIMIT ?`).all(input.runId, input.state, limit) as any[]
      : this.db.prepare(`SELECT * FROM km_ingest_items WHERE run_id=? ORDER BY ingest_item_id ASC LIMIT ?`).all(input.runId, limit) as any[];
    return rows.map(row => this.kmIngestItemFromRow(row));
  }

  getKmIngestRunReport(runId: string): KmIngestRunReport | null {
    const run = this.getKmIngestRun(runId);
    if (!run) return null;
    const auditRows = this.db.prepare(`SELECT * FROM km_ingest_audit WHERE run_id=? ORDER BY created_at ASC,audit_id ASC`).all(runId) as any[];
    return {
      run,
      items: this.listKmIngestItems({ runId, limit: 500 }),
      audit: auditRows.map(row => ({
        auditId: row.audit_id,
        action: row.action,
        actorId: row.actor_id,
        details: parseJsonRecord(row.details_json),
        createdAt: row.created_at,
      })),
    };
  }

  createProductionGatePlan(input: KmProductionGatePlanInsertInput): KmProductionGatePlanRecord {
    const now = input.now ?? new Date().toISOString();
    this.db.exec('SAVEPOINT km_production_gate_create;');
    try {
      this.db.prepare(`INSERT INTO km_production_gate_plans(
        plan_id,action_kind,state,target_json,scope_json,preview_json,preview_hash,required_approval_grade,
        actor_id,risk_ack_json,expires_at,confirmation_token_hash,confirmation_token_used_at,preflight_json,rollback_json,intent_json,created_at,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
        requireText(input.planId, 'production_gate_plan_id'),
        input.actionKind,
        input.state,
        canonicalJsonStringify(input.target),
        canonicalJsonStringify(input.scope),
        canonicalJsonStringify(input.preview),
        requireText(input.previewHash, 'production_gate_preview_hash'),
        input.requiredApprovalGrade,
        requireText(input.actorId, 'production_gate_actor'),
        canonicalJsonStringify(input.riskAck),
        requireText(input.expiresAt, 'production_gate_expires_at'),
        requireText(input.confirmationTokenHash, 'production_gate_confirmation_token_hash'),
        null,
        canonicalJsonStringify(input.preflight),
        canonicalJsonStringify(input.rollback),
        null,
        now,
        now,
      );
      this.insertProductionGateAudit(input.planId, 'plan.created', null, input.state, input.actorId, {
        previewHash: input.previewHash,
        requiredApprovalGrade: input.requiredApprovalGrade,
        effective: false,
        sideEffectsExecuted: false,
      }, now);
      this.db.exec('RELEASE km_production_gate_create;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_production_gate_create; RELEASE km_production_gate_create;'); } catch {} throw error; }
    return this.getProductionGatePlan(input.planId)!;
  }

  getProductionGatePlan(planId: string): KmProductionGatePlanRecord | null {
    const row = this.db.prepare('SELECT * FROM km_production_gate_plans WHERE plan_id=?').get(planId) as any;
    return row ? this.productionGatePlanFromRow(row) : null;
  }

  listProductionGatePlans(input: { limit?: number; actionKind?: KmProductionGateActionKind; state?: KmProductionGateState } = {}): KmProductionGatePlanRecord[] {
    const limit = Math.max(1, Math.min(input.limit ?? 50, 200));
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (input.actionKind) { where.push('action_kind=?'); args.push(input.actionKind); }
    if (input.state) { where.push('state=?'); args.push(input.state); }
    const clause = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const rows = this.db.prepare(`SELECT * FROM km_production_gate_plans ${clause} ORDER BY updated_at DESC,plan_id DESC LIMIT ?`)
      .all(...args, limit) as any[];
    return rows.map(row => this.productionGatePlanFromRow(row));
  }

  listProductionGateAudit(planId: string, limit = 100): KmProductionGateAuditRecord[] {
    const rows = this.db.prepare(`SELECT * FROM km_production_gate_audit WHERE plan_id=? ORDER BY created_at ASC,audit_id ASC LIMIT ?`)
      .all(planId, Math.max(1, Math.min(limit, 500))) as any[];
    return rows.map(row => this.productionGateAuditFromRow(row));
  }

  transitionProductionGatePlan(input: {
    planId: string;
    toState: KmProductionGateState;
    actorId: string;
    action: string;
    details?: Record<string, unknown>;
    expectedPreviewHash?: string;
    intent?: Record<string, unknown>;
    now?: string;
  }): KmProductionGatePlanRecord {
    const now = input.now ?? new Date().toISOString();
    this.db.exec('SAVEPOINT km_production_gate_transition;');
    try {
      const row = this.db.prepare('SELECT * FROM km_production_gate_plans WHERE plan_id=?').get(input.planId) as any;
      if (!row) throw new Error('km_production_gate_plan_not_found');
      const fromState = row.state as KmProductionGateState;
      if (input.expectedPreviewHash && input.expectedPreviewHash !== row.preview_hash) throw new Error('km_production_gate_preview_stale');
      assertKmProductionGateTransition(fromState, input.toState);
      this.db.prepare(`UPDATE km_production_gate_plans SET state=?,intent_json=COALESCE(?,intent_json),
        confirmation_token_used_at=CASE WHEN ? IS NOT NULL THEN ? ELSE confirmation_token_used_at END,
        updated_at=? WHERE plan_id=?`)
        .run(input.toState, input.intent ? canonicalJsonStringify(input.intent) : null,
          input.intent ? now : null, now, now, input.planId);
      this.insertProductionGateAudit(input.planId, requireText(input.action, 'production_gate_action'), fromState, input.toState,
        requireText(input.actorId, 'production_gate_actor'), input.details ?? {}, now);
      this.db.exec('RELEASE km_production_gate_transition;');
    } catch (error) { try { this.db.exec('ROLLBACK TO km_production_gate_transition; RELEASE km_production_gate_transition;'); } catch {} throw error; }
    return this.getProductionGatePlan(input.planId)!;
  }

  getProductionGateKillState(): KmProductionGateKillState {
    const row = this.db.prepare(`SELECT enabled,reason,actor_id,updated_at FROM km_production_gate_kill_state WHERE scope='global'`).get() as any;
    return row ? { enabled: Boolean(row.enabled), reason: row.reason, actorId: row.actor_id, updatedAt: row.updated_at }
      : { enabled: false, reason: 'unset', actorId: 'system', updatedAt: '1970-01-01T00:00:00.000Z' };
  }

  setProductionGateKillState(input: { enabled: boolean; reason: string; actorId: string; now?: string }): KmProductionGateKillState {
    const now = input.now ?? new Date().toISOString();
    this.db.prepare(`INSERT INTO km_production_gate_kill_state(scope,enabled,reason,actor_id,updated_at)
      VALUES('global',?,?,?,?)
      ON CONFLICT(scope) DO UPDATE SET enabled=excluded.enabled,reason=excluded.reason,actor_id=excluded.actor_id,updated_at=excluded.updated_at`)
      .run(input.enabled ? 1 : 0, requireText(input.reason, 'production_gate_kill_reason'), requireText(input.actorId, 'production_gate_actor'), now);
    return this.getProductionGateKillState();
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

  listMemoryProviderConfigs(input: { redactCredentials?: boolean } = {}): Array<KmMemoryProviderConfig & { configHash: string; updatedAt: string }> {
    const redactCredentials = input.redactCredentials ?? true;
    return (this.db.prepare(`SELECT provider_id,config_json,config_hash,updated_at FROM km_memory_provider_configs ORDER BY provider_id`).all() as any[])
      .map(row => { const config = KmMemoryProviderConfigSchema.parse(JSON.parse(row.config_json)); return {
        ...config, credentialRef: redactCredentials ? config.credentialRef.replace(/^(env|file):(.+)$/, (_m, kind) => `${kind}:***`) : config.credentialRef,
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

  getDistillationJob(jobId: string): Record<string, unknown> | null {
    const row = this.db.prepare(`SELECT job_id,source_event_id,bot_app_id,profile_id,profile_revision,state,attempts,next_attempt_at,last_error,output_hash,created_at,updated_at
      FROM distillation_jobs WHERE job_id=?`).get(jobId) as any;
    return row ? { jobId: row.job_id, sourceEventId: row.source_event_id, botAppId: row.bot_app_id,
      profileId: row.profile_id, profileRevision: row.profile_revision, state: row.state, attempts: row.attempts,
      nextAttemptAt: row.next_attempt_at, ...(row.last_error ? { lastError: row.last_error } : {}),
      ...(row.output_hash ? { outputHash: row.output_hash } : {}), createdAt: row.created_at, updatedAt: row.updated_at } : null;
  }

  listRetrievalAudits(limit: number): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT retrieval_run_id,bot_app_id,session_id,turn_id,mode,candidate_count,eligible_count,latency_ms,warnings_json,created_at,
        direct_hit_count,normalized_hit_count,no_hit_count,filtered_scope_count,filtered_privacy_count,filtered_state_count
      FROM retrieval_runs ORDER BY created_at DESC,retrieval_run_id DESC LIMIT ?`).all(Math.max(1, Math.min(limit, 500))) as any[])
      .map(row => ({ retrievalRunId: row.retrieval_run_id, botAppId: row.bot_app_id, sessionId: row.session_id,
        ...(row.turn_id ? { turnId: row.turn_id } : {}), mode: row.mode, candidateCount: row.candidate_count,
        eligibleCount: row.eligible_count, latencyMs: row.latency_ms, warnings: JSON.parse(row.warnings_json),
        directHitCount: Number(row.direct_hit_count ?? 0), normalizedHitCount: Number(row.normalized_hit_count ?? 0), noHitCount: Number(row.no_hit_count ?? 0),
        filteredScopeCount: Number(row.filtered_scope_count ?? 0), filteredPrivacyCount: Number(row.filtered_privacy_count ?? 0),
        filteredStateCount: Number(row.filtered_state_count ?? 0), createdAt: row.created_at }));
  }

  listInjectionSnapshots(limit: number): Array<Record<string, unknown>> {
    return (this.db.prepare(`SELECT snapshot_id,retrieval_run_id,bot_app_id,mode,requested_mode,effective_mode,disposition,item_ids_json,prompt_hash,prompt_bytes,reason,created_at
      FROM prompt_injection_snapshots ORDER BY created_at DESC,snapshot_id DESC LIMIT ?`).all(Math.max(1, Math.min(limit, 500))) as any[])
      .map(row => ({ snapshotId: row.snapshot_id, retrievalRunId: row.retrieval_run_id, botAppId: row.bot_app_id,
        mode: row.mode, requestedMode: row.requested_mode ?? row.mode, effectiveMode: row.effective_mode ?? row.mode,
        disposition: row.disposition, itemIds: JSON.parse(row.item_ids_json), ...(row.prompt_hash ? { promptHash: row.prompt_hash } : {}),
        promptBytes: row.prompt_bytes,
        ...(row.reason ? { reason: row.reason } : {}), createdAt: row.created_at }));
  }

  retrievalQualitySummary(): Record<string, number> {
    const row = this.db.prepare(`SELECT COUNT(*) runs,
      SUM(CASE WHEN candidate_count=0 THEN 1 ELSE 0 END) zero_hits,
      SUM(candidate_count) candidates,SUM(eligible_count) eligible,
      SUM(direct_hit_count) direct_hits,SUM(normalized_hit_count) normalized_hits,SUM(no_hit_count) no_hits,
      SUM(filtered_scope_count) filtered_scope,SUM(filtered_privacy_count) filtered_privacy,SUM(filtered_state_count) filtered_state,
      COALESCE(AVG(latency_ms),0) avg_latency_ms FROM retrieval_runs`).get() as any;
    return { runs: Number(row.runs ?? 0), zeroHits: Number(row.zero_hits ?? 0), candidates: Number(row.candidates ?? 0),
      eligible: Number(row.eligible ?? 0), directHits: Number(row.direct_hits ?? 0), normalizedHits: Number(row.normalized_hits ?? 0), noHits: Number(row.no_hits ?? 0),
      filteredScope: Number(row.filtered_scope ?? 0), filteredPrivacy: Number(row.filtered_privacy ?? 0),
      filteredState: Number(row.filtered_state ?? 0), avgLatencyMs: Math.round(Number(row.avg_latency_ms ?? 0)) };
  }

  dashboardMetrics(input: { now?: number; rankingLimit?: number } = {}): KmOpsMetricsRaw {
    const nowMs = input.now ?? Date.now();
    const generatedAt = new Date(nowMs).toISOString();
    const dayMs = 86_400_000;
    const todayStart = startOfUtcDay(nowMs);
    const last7Start = todayStart - (6 * dayMs);
    const last30Start = todayStart - (29 * dayMs);
    const last7dSince = new Date(last7Start).toISOString();
    const last30dSince = new Date(last30Start).toISOString();
    const rankLimit = Math.max(1, Math.min(input.rankingLimit ?? 10, 50));
    const count = (sql: string, ...args: Array<string | number>): number =>
      Number((this.db.prepare(sql).get(...args) as any)?.count ?? 0);
    const buckets = (sql: string, ...args: Array<string | number>): KmOpsBucket[] =>
      (this.db.prepare(sql).all(...args) as any[])
        .map(row => ({ key: String(row.key ?? 'unknown'), count: Number(row.count ?? 0) }))
        .filter(row => row.count > 0);
    const attentionFromRows = (rows: any[]): KmOpsAttentionItem[] => rows.map(row => ({
      itemId: String(row.item_id),
      itemKind: row.item_kind,
      title: String(row.title),
      state: String(row.state),
      updatedAt: String(row.updated_at),
      ageDays: ageDaysFrom(String(row.updated_at), nowMs),
      ...(row.target_layer ? { targetLayer: row.target_layer as KnowledgeLayer } : {}),
      ...(row.category ? { category: String(row.category) } : {}),
      ...(row.scope ? { scope: row.scope as MemoryScope } : {}),
    }));
    const rankFromRows = (rows: any[]): KmOpsItemRank[] => rows.map(row => ({
      itemId: String(row.item_id),
      itemKind: row.item_kind === 'knowledge' || row.item_kind === 'memory' ? row.item_kind : 'unknown',
      title: String(row.title ?? row.item_id),
      count: Number(row.count ?? 0),
      lastSeenAt: String(row.last_seen_at ?? generatedAt),
      ...(row.state ? { state: String(row.state) } : {}),
      ...(row.target_layer ? { targetLayer: row.target_layer as KnowledgeLayer } : {}),
      ...(row.category ? { category: String(row.category) } : {}),
      ...(row.scope ? { scope: row.scope as MemoryScope } : {}),
    }));
    const trend = (days: number, startMs: number): KmOpsTrendPoint[] => {
      const points = new Map<string, KmOpsTrendPoint>();
      for (let index = 0; index < days; index += 1) {
        const date = isoDay(startMs + (index * dayMs));
        points.set(date, { date, knowledgeCreated: 0, memoryCreated: 0, retrievalRuns: 0, wouldInject: 0, actualInject: 0 });
      }
      const fill = (sql: string, field: keyof Omit<KmOpsTrendPoint, 'date'>): void => {
        const rows = this.db.prepare(sql).all(new Date(startMs).toISOString()) as any[];
        for (const row of rows) {
          const point = points.get(String(row.date));
          if (point) point[field] = Number(row.count ?? 0);
        }
      };
      fill(`SELECT substr(created_at,1,10) date,COUNT(*) count FROM knowledge_items WHERE created_at>=? GROUP BY substr(created_at,1,10)`, 'knowledgeCreated');
      fill(`SELECT substr(created_at,1,10) date,COUNT(*) count FROM memory_items WHERE created_at>=? GROUP BY substr(created_at,1,10)`, 'memoryCreated');
      fill(`SELECT substr(created_at,1,10) date,COUNT(*) count FROM retrieval_runs WHERE created_at>=? GROUP BY substr(created_at,1,10)`, 'retrievalRuns');
      fill(`SELECT substr(created_at,1,10) date,COUNT(*) count FROM prompt_injection_snapshots WHERE created_at>=? AND disposition='would_inject' GROUP BY substr(created_at,1,10)`, 'wouldInject');
      fill(`SELECT substr(created_at,1,10) date,COUNT(*) count FROM prompt_injection_snapshots WHERE created_at>=? AND disposition='injected' GROUP BY substr(created_at,1,10)`, 'actualInject');
      return [...points.values()];
    };

    const knowledgeTotal = count(`SELECT COUNT(*) count FROM knowledge_items`);
    const memoryTotal = count(`SELECT COUNT(*) count FROM memory_items`);
    const knowledgeUsable = count(`SELECT COUNT(*) count FROM knowledge_items
      WHERE state IN ('approved','exported') AND freshness NOT IN ('stale','purged') AND privacy_class NOT IN ('sensitive','secret-reference-only')`);
    const memoryActive = count(`SELECT COUNT(*) count FROM memory_items
      WHERE state='active' AND (ttl_expires_at IS NULL OR ttl_expires_at>?)`, generatedAt);
    const memoryUsable = count(`SELECT COUNT(*) count FROM memory_items
      WHERE state='active' AND privacy_class NOT IN ('sensitive','secret-reference-only') AND (ttl_expires_at IS NULL OR ttl_expires_at>?)`, generatedAt);
    const staleKnowledge = count(`SELECT COUNT(*) count FROM knowledge_items
      WHERE state IN ('stale','deprecated') OR freshness IN ('stale','purged') OR (review_after IS NOT NULL AND review_after<=?)`, generatedAt);
    const staleMemory = count(`SELECT COUNT(*) count FROM memory_items
      WHERE state IN ('stale','expired','revoked') OR (ttl_expires_at IS NOT NULL AND ttl_expires_at<=?) OR (review_after IS NOT NULL AND review_after<=?)`, generatedAt, generatedAt);
    const retrievalTotal = count(`SELECT COUNT(*) count FROM retrieval_runs`);
    const retrievalLast7d = count(`SELECT COUNT(*) count FROM retrieval_runs WHERE created_at>=?`, last7dSince);
    const retrievalLast30d = count(`SELECT COUNT(*) count FROM retrieval_runs WHERE created_at>=?`, last30dSince);
    const wouldInjectTotal = count(`SELECT COUNT(*) count FROM prompt_injection_snapshots WHERE disposition='would_inject'`);
    const wouldInjectLast7d = count(`SELECT COUNT(*) count FROM prompt_injection_snapshots WHERE disposition='would_inject' AND created_at>=?`, last7dSince);
    const wouldInjectLast30d = count(`SELECT COUNT(*) count FROM prompt_injection_snapshots WHERE disposition='would_inject' AND created_at>=?`, last30dSince);
    const actualInjectTotal = count(`SELECT COUNT(*) count FROM prompt_injection_snapshots WHERE disposition='injected'`);
    const actualInjectLast7d = count(`SELECT COUNT(*) count FROM prompt_injection_snapshots WHERE disposition='injected' AND created_at>=?`, last7dSince);
    const actualInjectLast30d = count(`SELECT COUNT(*) count FROM prompt_injection_snapshots WHERE disposition='injected' AND created_at>=?`, last30dSince);
    const pendingReviewTotal = count(`SELECT COUNT(*) count FROM knowledge_items WHERE state IN ('candidate','review_pending')`)
      + count(`SELECT COUNT(*) count FROM memory_items WHERE state='proposed'`);
    const conflictTotal = count(`SELECT COUNT(*) count FROM knowledge_items WHERE state='conflict'`)
      + count(`SELECT COUNT(*) count FROM memory_items WHERE state='conflicted'`)
      + count(`SELECT COUNT(*) count FROM quarantine_events`);
    const auditEventsTotal = count(`SELECT COUNT(*) count FROM observation_events`)
      + count(`SELECT COUNT(*) count FROM km_config_audit`)
      + count(`SELECT COUNT(*) count FROM km_import_audit`)
      + count(`SELECT COUNT(*) count FROM km_production_gate_audit`)
      + count(`SELECT COUNT(*) count FROM km_ingest_audit`);

    const recallRows = this.db.prepare(`
      SELECT r.item_id,r.item_kind,COUNT(*) count,MAX(q.created_at) last_seen_at,
        COALESCE(k.title,m.claim_key,r.item_id) title,
        COALESCE(k.state,m.state) state,k.target_layer,k.category,m.scope
      FROM retrieval_results r
      JOIN retrieval_runs q ON q.retrieval_run_id=r.retrieval_run_id
      LEFT JOIN knowledge_items k ON r.item_kind='knowledge' AND r.item_id=k.knowledge_id
      LEFT JOIN memory_items m ON r.item_kind='memory' AND r.item_id=m.memory_id
      WHERE r.eligible=1
      GROUP BY r.item_id,r.item_kind
      ORDER BY count DESC,last_seen_at DESC,r.item_id
      LIMIT ?`).all(rankLimit) as any[];
    const readRows = this.db.prepare(`
      WITH used AS (
        SELECT CAST(value AS TEXT) item_id,COUNT(*) count,MAX(s.created_at) last_seen_at
        FROM prompt_injection_snapshots s,json_each(s.item_ids_json)
        WHERE s.disposition IN ('would_inject','injected')
        GROUP BY CAST(value AS TEXT)
      )
      SELECT u.item_id,
        CASE WHEN k.knowledge_id IS NOT NULL THEN 'knowledge' WHEN m.memory_id IS NOT NULL THEN 'memory' ELSE 'unknown' END item_kind,
        u.count,u.last_seen_at,COALESCE(k.title,m.claim_key,u.item_id) title,
        COALESCE(k.state,m.state) state,k.target_layer,k.category,m.scope
      FROM used u
      LEFT JOIN knowledge_items k ON u.item_id=k.knowledge_id
      LEFT JOIN memory_items m ON u.item_id=m.memory_id
      ORDER BY u.count DESC,u.last_seen_at DESC,u.item_id
      LIMIT ?`).all(rankLimit) as any[];

    const pendingRows = this.db.prepare(`
      SELECT * FROM (
        SELECT knowledge_id item_id,'knowledge' item_kind,title,state,updated_at,target_layer,category,NULL scope
        FROM knowledge_items WHERE state IN ('candidate','review_pending')
        UNION ALL
        SELECT memory_id item_id,'memory' item_kind,claim_key title,state,updated_at,NULL target_layer,NULL category,scope
        FROM memory_items WHERE state='proposed'
      )
      ORDER BY updated_at DESC,item_id DESC
      LIMIT ?`).all(rankLimit) as any[];
    const conflictRows = this.db.prepare(`
      SELECT * FROM (
        SELECT knowledge_id item_id,'knowledge' item_kind,title,state,updated_at,target_layer,category,NULL scope
        FROM knowledge_items WHERE state='conflict'
        UNION ALL
        SELECT memory_id item_id,'memory' item_kind,claim_key title,state,updated_at,NULL target_layer,NULL category,scope
        FROM memory_items WHERE state='conflicted'
      )
      ORDER BY updated_at DESC,item_id DESC
      LIMIT ?`).all(rankLimit) as any[];
    const staleRows = this.db.prepare(`
      SELECT * FROM (
        SELECT knowledge_id item_id,'knowledge' item_kind,title,state,updated_at,target_layer,category,NULL scope
        FROM knowledge_items WHERE state IN ('stale','deprecated') OR freshness IN ('stale','purged') OR (review_after IS NOT NULL AND review_after<=?)
        UNION ALL
        SELECT memory_id item_id,'memory' item_kind,claim_key title,state,updated_at,NULL target_layer,NULL category,scope
        FROM memory_items WHERE state IN ('stale','expired','revoked') OR (ttl_expires_at IS NOT NULL AND ttl_expires_at<=?) OR (review_after IS NOT NULL AND review_after<=?)
      )
      ORDER BY updated_at DESC,item_id DESC
      LIMIT ?`).all(generatedAt, generatedAt, generatedAt, rankLimit) as any[];

    return {
      schemaVersion: 1,
      source: 'sqlite',
      generatedAt,
      windows: { last7dSince, last30dSince },
      kpis: {
        totalKnowledge: knowledgeTotal,
        activeMemory: memoryActive,
        healthPercent: ratioPercent(knowledgeUsable + memoryUsable, knowledgeTotal + memoryTotal),
        retrievalRuns: retrievalLast30d,
        auditEvents: auditEventsTotal,
      },
      totals: {
        knowledgeTotal,
        knowledgeUsable,
        memoryTotal,
        memoryActive,
        memoryUsable,
        retrievalTotal,
        retrievalLast7d,
        retrievalLast30d,
        wouldInjectTotal,
        wouldInjectLast7d,
        wouldInjectLast30d,
        actualInjectTotal,
        actualInjectLast7d,
        actualInjectLast30d,
        auditEventsTotal,
        pendingReviewTotal,
        conflictTotal,
        staleKnowledge,
        staleMemory,
        overallHealthRate: ratioPercent(knowledgeUsable + memoryUsable, knowledgeTotal + memoryTotal),
        knowledgeUsableRate: ratioPercent(knowledgeUsable, knowledgeTotal),
        memoryActiveRate: ratioPercent(memoryActive, memoryTotal),
        freshnessRate: ratioPercent(Math.max(0, knowledgeTotal - staleKnowledge), knowledgeTotal),
      },
      distributions: {
        knowledgeByLayer: buckets(`SELECT target_layer key,COUNT(*) count FROM knowledge_items GROUP BY target_layer ORDER BY count DESC,key`),
        knowledgeByState: buckets(`SELECT state key,COUNT(*) count FROM knowledge_items GROUP BY state ORDER BY count DESC,key`),
        memoryByState: buckets(`SELECT state key,COUNT(*) count FROM memory_items GROUP BY state ORDER BY count DESC,key`),
        memoryByScope: buckets(`SELECT scope key,COUNT(*) count FROM memory_items GROUP BY scope ORDER BY count DESC,key`),
        knowledgeByFreshness: buckets(`SELECT freshness key,COUNT(*) count FROM knowledge_items GROUP BY freshness ORDER BY count DESC,key`),
        knowledgeByCategory: buckets(`SELECT category key,COUNT(*) count FROM knowledge_items GROUP BY category ORDER BY count DESC,key LIMIT 20`),
        observationBySource: (this.db.prepare(`
          SELECT json_extract(event_json,'$.source.producer') key,json_extract(event_json,'$.source.adapter') adapter,COUNT(*) count
          FROM observation_events GROUP BY key,adapter ORDER BY count DESC,key,adapter LIMIT 20`).all() as any[])
          .map(row => ({ key: String(row.key ?? 'unknown'), adapter: String(row.adapter ?? 'unknown'), count: Number(row.count ?? 0) }))
          .filter(row => row.count > 0),
        observationByType: buckets(`SELECT event_type key,COUNT(*) count FROM observation_events GROUP BY event_type ORDER BY count DESC,key LIMIT 20`),
        operationalHealth: [
          { key: 'usable_items', count: knowledgeUsable + memoryUsable },
          { key: 'pending_review', count: pendingReviewTotal },
          { key: 'conflicts', count: conflictTotal },
          { key: 'stale', count: staleKnowledge + staleMemory },
          { key: 'would_inject', count: wouldInjectTotal },
          { key: 'actual_inject', count: actualInjectTotal },
        ].filter(row => row.count > 0),
      },
      trends: {
        last7d: trend(7, last7Start),
        last30d: trend(30, last30Start),
      },
      rankings: {
        recallHot: rankFromRows(recallRows),
        readHot: rankFromRows(readRows),
        pendingReview: attentionFromRows(pendingRows),
        conflicts: attentionFromRows(conflictRows),
        stale: attentionFromRows(staleRows),
      },
      emptyStates: [
        { key: 'knowledge', empty: knowledgeTotal === 0, title: 'No knowledge items', detail: 'Create or import reviewed knowledge before the dashboard can show layer and freshness health.' },
        { key: 'memory', empty: memoryTotal === 0, title: 'No memory items', detail: 'Activate reviewed memories before the dashboard can show scope and state health.' },
        { key: 'retrieval', empty: retrievalTotal === 0, title: 'No retrieval runs', detail: 'Run KM retrieval in shadow or active mode before rankings and retrieval trends appear.' },
        { key: 'injection', empty: wouldInjectTotal + actualInjectTotal === 0, title: 'No prompt injection snapshots', detail: 'Prompt memory snapshots are required before read rankings can be populated.' },
        { key: 'observations', empty: auditEventsTotal === 0, title: 'No observation events', detail: 'The observation journal is empty or disabled for this data directory.' },
      ],
    };
  }

  evalMetricWindows(input: { metricKeys: string[]; minCount: number; sinceEvalRunId?: string }): KmEvalMetricWindow[] {
    if (input.metricKeys.length === 0) return [];
    const placeholders = input.metricKeys.map(() => '?').join(',');
    const rows = this.db.prepare(`
      SELECT r.eval_run_id,r.target_type,r.target_id,x.metric_key,x.verdict,x.source_refs_json
      FROM eval_runs r JOIN eval_results x ON x.eval_run_id=r.eval_run_id
      WHERE r.state='accepted' AND x.metric_key IN (${placeholders})
      ORDER BY r.updated_at DESC,r.eval_run_id DESC
      LIMIT 1000
    `).all(...input.metricKeys) as any[];
    const grouped = new Map<string, { totalCount: number; passCount: number; warnCount: number; failCount: number; failedTargetIds: string[]; evidenceRefs: unknown[] }>();
    for (const row of rows) {
      const group = grouped.get(row.metric_key) ?? { totalCount: 0, passCount: 0, warnCount: 0, failCount: 0, failedTargetIds: [], evidenceRefs: [] };
      group.totalCount += 1;
      if (row.verdict === 'pass') group.passCount += 1;
      else if (row.verdict === 'warn') group.warnCount += 1;
      else if (row.verdict === 'fail') {
        group.failCount += 1;
        if (group.failedTargetIds.length < 20) group.failedTargetIds.push(String(row.target_id));
      }
      const refs = parseJsonArray(row.source_refs_json);
      for (const ref of refs) {
        if (group.evidenceRefs.length < 20) group.evidenceRefs.push(ref);
      }
      grouped.set(row.metric_key, group);
    }
    return [...grouped.entries()]
      .filter(([, group]) => group.totalCount >= Math.max(1, input.minCount))
      .map(([metricKey, group]) => {
        const windowHash = sha256(JSON.stringify({
          metricKey,
          totalCount: group.totalCount,
          failCount: group.failCount,
          failedTargetIds: group.failedTargetIds,
          evidenceRefs: group.evidenceRefs,
        }));
        return {
          metricKey,
          totalCount: group.totalCount,
          passCount: group.passCount,
          warnCount: group.warnCount,
          failCount: group.failCount,
          failRatio: group.totalCount === 0 ? 0 : group.failCount / group.totalCount,
          failedTargetIds: group.failedTargetIds,
          evidenceRefs: group.evidenceRefs,
          windowHash,
        };
      });
  }

  retrievalRetentionPreview(cutoffIso: string): { cutoff: string; eligibleRuns: number } {
    const cutoff = new Date(cutoffIso); if (!Number.isFinite(cutoff.getTime())) throw new Error('km_retention_cutoff_invalid');
    const normalized = cutoff.toISOString();
    const spec = retentionSpec('retrieval');
    const protectedSql = this.retentionProtectedSql(spec);
    const row = this.db.prepare(`SELECT COUNT(*) count FROM retrieval_runs WHERE created_at<? AND NOT (${protectedSql})`).get(normalized) as any;
    return { cutoff: normalized, eligibleRuns: Number(row.count ?? 0) };
  }

  kmRetentionPreview(input: { now?: number; sampleLimit?: number } = {}): KmRetentionPlan {
    const now = input.now ?? Date.now();
    const generatedAt = new Date(now).toISOString();
    const sampleLimit = Math.max(0, Math.min(input.sampleLimit ?? 10, 50));
    const domains = Object.keys(DEFAULT_RETENTION_POLICIES)
      .map(domain => this.retentionDomainPreview(domain as KmRetentionDomain, now, sampleLimit));
    const dbBytes = fileBytes(this.path);
    const walBytes = fileBytes(`${this.path}-wal`);
    const backlog = this.distillationBacklogStatus(now);
    const syncOutbox = this.outboxStatusCounts('sync_outbox');
    const backendOutbox = this.outboxStatusCounts('memory_backend_outbox');
    const quarantine = this.quarantineCounts();
    const providerQuality = this.providerQuality();
    const retrievalQuality = this.retrievalQualitySummary();
    const slo = buildRetentionSloMetrics({
      dbBytes,
      walBytes,
      distillationOldestAgeMs: backlog.oldestAgeMs,
      syncPending: syncOutbox.pending,
      syncInflight: syncOutbox.inflight,
      syncFailed: syncOutbox.failed,
      backendPending: backendOutbox.pending,
      backendInflight: backendOutbox.inflight,
      backendFailed: backendOutbox.failed,
      observationQuarantine: quarantine.observations,
      syncQuarantine: quarantine.sync,
      backendQuarantine: quarantine.backend,
      unavailableProviders: providerQuality.unavailableProviders,
      retrievalRuns: retrievalQuality.runs,
      retrievalZeroHits: retrievalQuality.zeroHits,
      retrievalAvgLatencyMs: retrievalQuality.avgLatencyMs,
    });
    return finalizeRetentionPlan({
      policyVersion: KM_RETENTION_POLICY_VERSION,
      generatedAt,
      dryRunOnly: true,
      destructiveActionsAvailable: false,
      domains,
      db: { dbBytes, walBytes, totalBytes: dbBytes + walBytes },
      operational: {
        backlog: {
          distillationQueued: backlog.queued,
          distillationRetryWait: backlog.retryWait,
          distillationOldestAgeMs: backlog.oldestAgeMs,
          distillationClaimed: backlog.claimed,
          syncPending: syncOutbox.pending,
          syncInflight: syncOutbox.inflight,
          syncFailed: syncOutbox.failed,
          backendPending: backendOutbox.pending,
          backendInflight: backendOutbox.inflight,
          backendFailed: backendOutbox.failed,
        },
        quarantine,
        retry: {
          distillationRetryWait: backlog.retryWait,
          syncFailed: syncOutbox.failed,
          backendFailed: backendOutbox.failed,
        },
        providerQuality,
        retrievalQuality,
      },
      slo,
    });
  }

  recordKmRetentionShadowReport(input: { holderId: string; now?: number; sampleLimit?: number }): KmRetentionReportSummary {
    const startedAt = new Date(input.now ?? Date.now()).toISOString();
    const plan = this.kmRetentionPreview({ now: input.now, sampleLimit: input.sampleLimit });
    const completedAt = plan.generatedAt;
    const reportId = `kmret_${createHash('sha256').update(`${input.holderId}|${plan.planHash}|${completedAt}`).digest('hex')}`;
    this.db.prepare(`INSERT OR IGNORE INTO km_retention_reports(report_id,policy_version,holder_id,started_at,completed_at,plan_json,report_hash)
      VALUES(?,?,?,?,?,?,?)`).run(reportId, plan.policyVersion, requireText(input.holderId, 'retention_holder'),
      startedAt, completedAt, JSON.stringify(plan), plan.planHash);
    return {
      reportId,
      policyVersion: plan.policyVersion,
      holderId: input.holderId,
      startedAt,
      completedAt,
      reportHash: plan.planHash,
      totalEligible: totalEligible(plan),
      worstSloState: worstSloState(plan.slo),
    };
  }

  kmRetentionStatus(input: { enabled?: boolean; leaseName?: string; now?: number; reportLimit?: number; sampleLimit?: number } = {}): KmRetentionRuntimeStatus {
    const reports = this.listKmRetentionReports(input.reportLimit ?? 30);
    return {
      enabled: Boolean(input.enabled),
      leaseName: input.leaseName ?? 'km-retention-shadow',
      latestPlan: this.kmRetentionPreview({ now: input.now, sampleLimit: input.sampleLimit }),
      reports,
      trend: reports.map(report => {
        const row = this.db.prepare('SELECT plan_json FROM km_retention_reports WHERE report_id=?').get(report.reportId) as { plan_json: string } | undefined;
        const plan = row ? JSON.parse(row.plan_json) as KmRetentionPlan : undefined;
        return {
          reportId: report.reportId,
          completedAt: report.completedAt,
          totalEligible: report.totalEligible,
          worstSloState: report.worstSloState,
          dbBytes: plan?.db.dbBytes ?? 0,
          walBytes: plan?.db.walBytes ?? 0,
        };
      }),
    };
  }

  listKmRetentionReports(limit: number): KmRetentionReportSummary[] {
    const rows = this.db.prepare(`SELECT report_id,policy_version,holder_id,started_at,completed_at,plan_json,report_hash
      FROM km_retention_reports ORDER BY completed_at DESC,report_id DESC LIMIT ?`).all(Math.max(1, Math.min(limit, 100))) as any[];
    return rows.map(row => {
      const plan = JSON.parse(row.plan_json) as KmRetentionPlan;
      return {
        reportId: row.report_id,
        policyVersion: row.policy_version,
        holderId: row.holder_id,
        startedAt: row.started_at,
        completedAt: row.completed_at,
        reportHash: row.report_hash,
        totalEligible: totalEligible(plan),
        worstSloState: worstSloState(plan.slo),
      };
    });
  }

  private retentionDomainPreview(domain: KmRetentionDomain, nowMs: number, sampleLimit: number): KmRetentionDomainPreview {
    const policy = DEFAULT_RETENTION_POLICIES[domain];
    const spec = retentionSpec(domain);
    const cutoff = new Date(nowMs - policy.retentionDays * 86_400_000).toISOString();
    const protectedSql = this.retentionProtectedSql(spec);
    const total = this.db.prepare(`SELECT COUNT(*) count FROM ${spec.table}`).get() as any;
    const oldest = this.db.prepare(`SELECT MIN(${spec.ageColumn}) oldest FROM ${spec.table}`).get() as any;
    const protectedCount = this.db.prepare(`SELECT COUNT(*) count FROM ${spec.table} WHERE ${protectedSql}`).get() as any;
    const eligible = this.db.prepare(`SELECT COUNT(*) count FROM ${spec.table} WHERE ${spec.ageColumn}<? AND NOT (${protectedSql})`).get(cutoff) as any;
    const eligibleRows = this.db.prepare(`SELECT ${spec.idColumn} id,${spec.ageColumn} created_at FROM ${spec.table}
      WHERE ${spec.ageColumn}<? AND NOT (${protectedSql}) ORDER BY ${spec.ageColumn} ASC,${spec.idColumn} ASC LIMIT ?`)
      .all(cutoff, sampleLimit) as any[];
    const oldestEligible = this.db.prepare(`SELECT MIN(${spec.ageColumn}) oldest FROM ${spec.table}
      WHERE ${spec.ageColumn}<? AND NOT (${protectedSql})`).get(cutoff) as any;
    return {
      domain,
      table: spec.table,
      tier: policy.tier,
      retentionDays: policy.retentionDays,
      cutoff,
      totalCount: Number(total.count ?? 0),
      eligibleCount: Number(eligible.count ?? 0),
      protectedCount: Number(protectedCount.count ?? 0),
      oldestRecordAgeDays: ageDays(nowMs, oldest.oldest ? String(oldest.oldest) : undefined),
      oldestEligibleAgeDays: ageDays(nowMs, oldestEligible.oldest ? String(oldestEligible.oldest) : undefined),
      protectedReasonCounts: this.retentionProtectedReasonCounts(spec),
      eligibleSamples: eligibleRows.map(row => ({
        id: String(row.id),
        createdAt: String(row.created_at),
        ageDays: ageDays(nowMs, String(row.created_at)),
        reason: `older_than_${policy.retentionDays}d_and_unprotected`,
      })),
    };
  }

  private retentionProtectedSql(spec: RetentionDomainSpec): string {
    const clauses = Object.values(spec.protectedReasons);
    return clauses.length > 0 ? clauses.map(clause => `(${clause})`).join(' OR ') : '0=1';
  }

  private retentionProtectedReasonCounts(spec: RetentionDomainSpec): Record<string, number> {
    const counts: Record<string, number> = {};
    for (const [reason, sql] of Object.entries(spec.protectedReasons)) {
      const row = this.db.prepare(`SELECT COUNT(*) count FROM ${spec.table} WHERE ${sql}`).get() as any;
      counts[reason] = Number(row.count ?? 0);
    }
    return counts;
  }

  private outboxStatusCounts(table: 'sync_outbox' | 'memory_backend_outbox'): OutboxStatusCounts {
    const row = this.db.prepare(`SELECT
      SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) pending,
      SUM(CASE WHEN status='inflight' THEN 1 ELSE 0 END) inflight,
      SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) failed
      FROM ${table}`).get() as any;
    return {
      pending: Number(row.pending ?? 0),
      inflight: Number(row.inflight ?? 0),
      failed: Number(row.failed ?? 0),
    };
  }

  private quarantineCounts(): KmRetentionPlan['operational']['quarantine'] {
    const observations = this.db.prepare('SELECT COUNT(*) count FROM quarantine_events').get() as any;
    const sync = this.db.prepare('SELECT COUNT(*) count FROM sync_quarantine WHERE resolved_at IS NULL').get() as any;
    const backend = this.db.prepare(`SELECT COUNT(*) count FROM memory_backend_outbox WHERE status='quarantined'`).get() as any;
    return {
      observations: Number(observations.count ?? 0),
      sync: Number(sync.count ?? 0),
      backend: Number(backend.count ?? 0),
    };
  }

  private providerQuality(): KmRetentionPlan['operational']['providerQuality'] {
    const row = this.db.prepare(`SELECT COUNT(*) configured,
      SUM(CASE WHEN status NOT IN ('validated','ready','ok') THEN 1 ELSE 0 END) unavailable
      FROM km_provider_registry`).get() as any;
    const backend = this.db.prepare(`SELECT COUNT(*) quarantined FROM memory_backend_outbox WHERE status='quarantined'`).get() as any;
    return {
      configuredProviders: Number(row.configured ?? 0),
      unavailableProviders: Number(row.unavailable ?? 0),
      quarantinedBackendOutbox: Number(backend.quarantined ?? 0),
    };
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

  private productionGatePlanFromRow(row: any): KmProductionGatePlanRecord {
    return {
      planId: row.plan_id,
      actionKind: row.action_kind,
      state: row.state,
      target: parseJsonRecord(row.target_json),
      scope: parseJsonRecord(row.scope_json),
      preview: parseJsonRecord(row.preview_json),
      previewHash: row.preview_hash,
      requiredApprovalGrade: row.required_approval_grade,
      actorId: row.actor_id,
      riskAck: parseJsonRecord(row.risk_ack_json),
      expiresAt: row.expires_at,
      confirmationTokenHash: row.confirmation_token_hash,
      ...(row.confirmation_token_used_at ? { confirmationTokenUsedAt: row.confirmation_token_used_at } : {}),
      preflight: parseJsonArray(row.preflight_json) as Array<Record<string, unknown>>,
      rollback: parseJsonRecord(row.rollback_json),
      ...(row.intent_json ? { intent: parseJsonRecord(row.intent_json) } : {}),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  private productionGateAuditFromRow(row: any): KmProductionGateAuditRecord {
    return {
      auditId: row.audit_id,
      planId: row.plan_id,
      action: row.action,
      ...(row.from_state ? { fromState: row.from_state } : {}),
      toState: row.to_state,
      actorId: row.actor_id,
      details: parseJsonRecord(row.details_json),
      createdAt: row.created_at,
    };
  }

  private kmIngestTargetFromRow(row: any): KmIngestTargetRecord {
    return {
      targetId: row.target_id,
      state: row.state,
      target: parseJsonRecord(row.target_json) as KmIngestTargetRecord['target'],
      targetHash: row.target_hash,
      credentialRef: row.credential_ref,
      createdBy: row.created_by,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  private kmIngestRunFromRow(row: any): KmIngestRunRecord {
    return {
      runId: row.run_id,
      idempotencyKey: row.idempotency_key,
      state: row.state,
      targetId: row.target_id,
      plan: parseJsonRecord(row.plan_json) as unknown as KmIngestRunPlan,
      planHash: row.plan_hash,
      canonicalKeySetHash: row.canonical_key_set_hash,
      confirmationTokenHash: row.confirmation_token_hash,
      ...(row.external_ack_json ? { externalAck: parseJsonRecord(row.external_ack_json) } : {}),
      sourceCount: Number(row.source_count),
      eligibleCount: Number(row.eligible_count),
      ingestedCount: Number(row.ingested_count),
      dedupedCount: Number(row.deduped_count),
      skippedCount: Number(row.skipped_count),
      failedCount: Number(row.failed_count),
      rollbackCount: Number(row.rollback_count),
      markIngestedPlannedCount: Number(row.mark_ingested_planned_count),
      ...(row.checkpoint ? { checkpoint: row.checkpoint } : {}),
      createdBy: row.created_by,
      ...(row.approved_by ? { approvedBy: row.approved_by } : {}),
      ...(row.last_error ? { lastError: row.last_error } : {}),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      ...(row.started_at ? { startedAt: row.started_at } : {}),
      ...(row.completed_at ? { completedAt: row.completed_at } : {}),
      ...(row.rolled_back_at ? { rolledBackAt: row.rolled_back_at } : {}),
    };
  }

  private kmIngestItemFromRow(row: any): KmIngestItemRecord {
    return {
      ingestItemId: row.ingest_item_id,
      runId: row.run_id,
      canonicalKey: row.canonical_key,
      candidate: parseJsonRecord(row.candidate_json) as unknown as KmIngestItemRecord['candidate'],
      candidateHash: row.candidate_hash,
      state: row.state,
      ...(row.reason_code ? { reasonCode: row.reason_code } : {}),
      ...(row.knowledge_id ? { knowledgeId: row.knowledge_id } : {}),
      ...(row.mark_ingested_plan_json ? { markIngestedPlan: parseJsonRecord(row.mark_ingested_plan_json) } : {}),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  private computeKmIngestStats(items: Array<Pick<KmIngestItemRecord, 'state' | 'markIngestedPlan'>>): KmIngestRunStats {
    return {
      sourceCount: items.length,
      eligibleCount: items.filter(item => item.state === 'pending').length,
      ingestedCount: items.filter(item => item.state === 'ingested').length,
      dedupedCount: items.filter(item => item.state === 'deduped').length,
      skippedCount: items.filter(item => item.state === 'skipped').length,
      failedCount: items.filter(item => item.state === 'failed').length,
      rollbackCount: items.filter(item => item.state === 'rolled_back').length,
      markIngestedPlannedCount: items.filter(item => item.markIngestedPlan).length,
    };
  }

  private insertKmIngestAudit(runId: string, action: string, actorId: string, details: Record<string, unknown>, now: string): void {
    this.db.prepare(`INSERT INTO km_ingest_audit(audit_id,run_id,action,actor_id,details_json,created_at)
      VALUES(?,?,?,?,?,?)`).run(kmId('kmira'), runId, action, actorId, canonicalJsonStringify(details), now);
  }

  private applyKmIngestItem(row: any, run: KmIngestRunRecord, actorId: string, now: string): void {
    const item = this.kmIngestItemFromRow(row);
    if (item.state !== 'pending' && item.state !== 'failed') return;
    const candidate = item.candidate;
    const markIngestedPlan = run.externalAck ? {
      command: 'mark-ingested',
      dryRun: true,
      runId: run.runId,
      targetId: run.targetId,
      canonicalKey: item.canonicalKey,
      planHash: run.planHash,
      externalAckHash: sha256(canonicalJsonStringify(run.externalAck)),
      sideEffectsExecuted: false,
    } : undefined;
    if (candidate.confidence === 'inferred') {
      this.markKmIngestItem(item.ingestItemId, 'skipped', 'inferred_not_ingested', undefined, undefined, now);
      return;
    }
    if (candidate.privacyClass === 'sensitive' || candidate.privacyClass === 'secret-reference-only') {
      this.markKmIngestItem(item.ingestItemId, 'skipped', 'privacy_not_ingested', undefined, undefined, now);
      return;
    }
    const existing = this.db.prepare('SELECT * FROM knowledge_items WHERE target_layer=? AND claim_key=? AND claim_text=?')
      .get(candidate.targetLayer, candidate.claimKey, candidate.claimText) as any;
    if (existing) {
      this.markKmIngestItem(item.ingestItemId, 'deduped', 'identical_knowledge_exists', existing.knowledge_id, markIngestedPlan, now);
      this.insertTraceEdgeInTransaction({ fromType: 'km-ingest-candidate', fromId: item.canonicalKey, toType: 'knowledge', toId: existing.knowledge_id, edgeType: 'superseded' }, now);
      return;
    }
    const knowledgeId = candidate.knowledgeId ?? `kn_${createHash('sha256').update(`${item.canonicalKey}|${item.candidateHash}`).digest('hex')}`;
    this.db.prepare(`
      INSERT INTO knowledge_items(
        knowledge_id,state,target_layer,category,title,claim_key,claim_text,confidence,
        freshness,privacy_class,source_refs_json,review_after,created_at,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      knowledgeId, 'candidate', candidate.targetLayer, requireText(candidate.category, 'knowledge_category'),
      requireText(candidate.title, 'knowledge_title'), requireText(candidate.claimKey, 'knowledge_claim_key'),
      requireText(candidate.claimText, 'knowledge_claim_text'), candidate.confidence, candidate.freshness ?? 'unknown',
      candidate.privacyClass, JSON.stringify(candidate.sourceRefs), candidate.reviewAfter ?? null, now, now,
    );
    this.db.prepare(`
      INSERT INTO knowledge_state_history(history_id,knowledge_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
      VALUES(?,?,?,?,?,?,?,?)
    `).run(kmId('hist'), knowledgeId, null, 'candidate', 'km_ingest_candidate_created', actorId, candidate.evidenceEventId ?? null, now);
    this.markKmIngestItem(item.ingestItemId, 'ingested', 'knowledge_candidate_created', knowledgeId, markIngestedPlan, now);
    this.insertTraceEdgeInTransaction({ fromType: 'km-ingest-candidate', fromId: item.canonicalKey, toType: 'knowledge', toId: knowledgeId, edgeType: 'produced' }, now);
  }

  private markKmIngestItem(
    ingestItemId: string,
    state: KmIngestItemState,
    reasonCode: string,
    knowledgeId: string | undefined,
    markIngestedPlan: Record<string, unknown> | undefined,
    now: string,
  ): void {
    this.db.prepare(`UPDATE km_ingest_items SET state=?,reason_code=?,knowledge_id=COALESCE(?,knowledge_id),
      mark_ingested_plan_json=COALESCE(?,mark_ingested_plan_json),updated_at=? WHERE ingest_item_id=?`)
      .run(state, reasonCode, knowledgeId ?? null, markIngestedPlan ? canonicalJsonStringify(markIngestedPlan) : null, now, ingestItemId);
  }

  private refreshKmIngestRun(
    runId: string,
    checkpoint: string | undefined,
    now: string,
    forcedState?: KmIngestRunState,
  ): void {
    const row = this.db.prepare(`SELECT
      COUNT(*) source_count,
      SUM(CASE WHEN state IN ('pending','ingested','deduped') THEN 1 ELSE 0 END) eligible_count,
      SUM(CASE WHEN state='ingested' THEN 1 ELSE 0 END) ingested_count,
      SUM(CASE WHEN state='deduped' THEN 1 ELSE 0 END) deduped_count,
      SUM(CASE WHEN state='skipped' THEN 1 ELSE 0 END) skipped_count,
      SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) failed_count,
      SUM(CASE WHEN state='rolled_back' THEN 1 ELSE 0 END) rollback_count,
      SUM(CASE WHEN mark_ingested_plan_json IS NOT NULL THEN 1 ELSE 0 END) mark_ingested_planned_count,
      SUM(CASE WHEN state='pending' THEN 1 ELSE 0 END) pending_count
      FROM km_ingest_items WHERE run_id=?`).get(runId) as any;
    const pending = Number(row.pending_count ?? 0);
    const failed = Number(row.failed_count ?? 0);
    const finalState: KmIngestRunState = forcedState ?? (pending > 0 || failed > 0 ? 'partial' : 'completed');
    this.db.prepare(`UPDATE km_ingest_runs SET state=?,checkpoint=?,source_count=?,eligible_count=?,ingested_count=?,
      deduped_count=?,skipped_count=?,failed_count=?,rollback_count=?,mark_ingested_planned_count=?,updated_at=?,
      completed_at=CASE WHEN ? IN ('completed','partial','blocked','failed') THEN ? ELSE completed_at END,
      rolled_back_at=CASE WHEN ?='rolled_back' THEN ? ELSE rolled_back_at END WHERE run_id=?`)
      .run(finalState, checkpoint ?? null, Number(row.source_count ?? 0), Number(row.eligible_count ?? 0),
        Number(row.ingested_count ?? 0), Number(row.deduped_count ?? 0), Number(row.skipped_count ?? 0),
        failed, Number(row.rollback_count ?? 0), Number(row.mark_ingested_planned_count ?? 0), now,
        finalState, now, finalState, now, runId);
  }

  private insertProductionGateAudit(
    planId: string,
    action: string,
    fromState: KmProductionGateState | null,
    toState: KmProductionGateState,
    actorId: string,
    details: Record<string, unknown>,
    now: string,
  ): void {
    this.db.prepare(`INSERT INTO km_production_gate_audit(audit_id,plan_id,action,from_state,to_state,actor_id,details_json,created_at)
      VALUES(?,?,?,?,?,?,?,?)`).run(kmId('pga'), planId, action, fromState, toState, actorId, canonicalJsonStringify(details), now);
  }

  private knowledgeToMemoryImportJobFromRow(row: any): KnowledgeToMemoryImportJob {
    return {
      jobId: row.job_id,
      idempotencyKey: row.idempotency_key,
      state: row.state,
      config: normalizeKnowledgeToMemoryImportConfig(JSON.parse(row.config_json)),
      configHash: row.config_hash,
      ...(row.checkpoint ? { checkpoint: row.checkpoint } : {}),
      sourceCount: Number(row.source_count),
      eligibleCount: Number(row.eligible_count),
      importedCount: Number(row.imported_count),
      dedupedCount: Number(row.deduped_count),
      conflictCount: Number(row.conflict_count),
      skippedCount: Number(row.skipped_count),
      failedCount: Number(row.failed_count),
      outboxEnqueuedCount: Number(row.outbox_enqueued_count),
      createdBy: row.created_by,
      ...(row.approved_by ? { approvedBy: row.approved_by } : {}),
      ...(row.last_error ? { lastError: row.last_error } : {}),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      ...(row.started_at ? { startedAt: row.started_at } : {}),
      ...(row.completed_at ? { completedAt: row.completed_at } : {}),
    };
  }

  private knowledgeToMemoryImportItemFromRow(row: any): KnowledgeToMemoryImportItem {
    return {
      importItemId: row.import_item_id,
      jobId: row.job_id,
      sourceKind: row.source_kind,
      sourceId: row.source_id,
      sourceRef: parseJsonRecord(row.source_ref_json),
      sourceHash: row.source_hash,
      contentHash: row.content_hash,
      state: row.state,
      ...(row.reason_code ? { reasonCode: row.reason_code } : {}),
      scope: row.scope,
      subject: row.subject,
      claimKey: row.claim_key,
      claimText: row.claim_text,
      confidence: row.confidence,
      privacyClass: row.privacy_class,
      freshness: row.freshness,
      ...(row.memory_id ? { memoryId: row.memory_id } : {}),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  private computeKnowledgeToMemoryImportStats(items: Array<Pick<KnowledgeToMemoryImportItem, 'state'>>): KnowledgeToMemoryImportStats {
    return {
      sourceCount: items.length,
      eligibleCount: items.filter(item => item.state === 'pending').length,
      importedCount: items.filter(item => item.state === 'imported').length,
      dedupedCount: items.filter(item => item.state === 'deduped').length,
      conflictCount: items.filter(item => item.state === 'conflicted').length,
      skippedCount: items.filter(item => item.state === 'skipped').length,
      failedCount: items.filter(item => item.state === 'failed').length,
      outboxEnqueuedCount: 0,
    };
  }

  private insertKnowledgeToMemoryImportAudit(jobId: string, action: string, actorId: string, details: Record<string, unknown>, now: string): void {
    this.db.prepare(`INSERT INTO km_import_audit(audit_id,job_id,action,actor_id,details_json,created_at)
      VALUES(?,?,?,?,?,?)`).run(kmId('kmia'), jobId, action, actorId, JSON.stringify(details), now);
  }

  private applyKnowledgeToMemoryImportItem(row: any, config: KnowledgeToMemoryImportConfig, actorId: string, now: string): void {
    const item = this.knowledgeToMemoryImportItemFromRow(row);
    if (item.state !== 'pending' && item.state !== 'failed') return;
    if (item.confidence === 'inferred') {
      this.markKnowledgeToMemoryImportItem(item.importItemId, 'skipped', 'inferred_not_auto_imported', undefined, now);
      return;
    }
    if (item.privacyClass === 'sensitive' || item.privacyClass === 'secret-reference-only') {
      this.markKnowledgeToMemoryImportItem(item.importItemId, 'skipped', 'privacy_not_auto_imported', undefined, now);
      return;
    }
    if (item.freshness !== 'fresh') {
      this.markKnowledgeToMemoryImportItem(item.importItemId, 'skipped', 'freshness_not_importable', undefined, now);
      return;
    }
    const existing = this.db.prepare('SELECT * FROM memory_items WHERE scope=? AND subject=? AND claim_key=?')
      .get(item.scope, item.subject, item.claimKey) as any;
    if (existing && existing.claim_text === item.claimText) {
      this.markKnowledgeToMemoryImportItem(item.importItemId, 'deduped', 'identical_claim_exists', existing.memory_id, now);
      this.insertTraceEdgeInTransaction({ fromType: item.sourceKind, fromId: item.sourceId, toType: 'memory', toId: existing.memory_id, edgeType: 'superseded' }, now);
      return;
    }
    if (existing) {
      this.markKnowledgeToMemoryImportItem(item.importItemId, 'conflicted', 'active_claim_conflict', existing.memory_id, now);
      this.insertTraceEdgeInTransaction({ fromType: item.sourceKind, fromId: item.sourceId, toType: 'memory', toId: existing.memory_id, edgeType: 'conflicted' }, now);
      return;
    }
    const memoryId = `mem_${createHash('sha256').update(`${item.scope}|${item.subject}|${item.claimKey}|${item.contentHash}`).digest('hex')}`;
    const sourceRefs = [{
      kind: 'km-import',
      ref: `${item.sourceKind}:${item.sourceId}`,
      jobId: item.jobId,
      sourceHash: item.sourceHash,
      contentHash: item.contentHash,
      sourceRef: item.sourceRef,
    }];
    this.db.prepare(`INSERT INTO memory_items(
      memory_id,state,scope,subject,claim_key,claim_text,confidence,source_refs_json,
      ttl_expires_at,review_after,sync_policy,privacy_class,created_at,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(memoryId, 'active', item.scope, item.subject, item.claimKey, item.claimText,
      item.confidence, JSON.stringify(sourceRefs), null, null, 'local-only', item.privacyClass, now, now);
    this.db.prepare(`INSERT INTO memory_state_history(history_id,memory_id,from_state,to_state,reason_code,actor_id,evidence_event_id,created_at)
      VALUES(?,?,?,?,?,?,?,?)`).run(kmId('hist'), memoryId, null, 'active', 'km_import_active', actorId, null, now);
    this.markKnowledgeToMemoryImportItem(item.importItemId, 'imported', 'imported_active', memoryId, now);
    this.insertTraceEdgeInTransaction({ fromType: item.sourceKind, fromId: item.sourceId, toType: 'memory', toId: memoryId, edgeType: 'produced' }, now);
    if (config.enqueueBackendOutbox === true) {
      for (const providerId of config.backendProviderIds ?? []) {
        const payload = {
          memoryId,
          scope: item.scope,
          subject: item.subject,
          claimKey: item.claimKey,
          claimText: item.claimText,
          contentHash: item.contentHash,
        };
        const payloadJson = JSON.stringify(payload);
        const payloadHash = sha256(payloadJson);
        const outboxId = `mout_${createHash('sha256').update(`${memoryId}|${providerId}|put|${payloadHash}`).digest('hex')}`;
        this.db.prepare(`INSERT OR IGNORE INTO memory_backend_outbox(
          outbox_id,memory_id,provider_id,operation,payload_json,payload_hash,status,next_attempt_at,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,'pending',?,?,?)`).run(outboxId, memoryId, providerId, 'put', payloadJson, payloadHash, Date.now(), now, now);
      }
    }
  }

  private markKnowledgeToMemoryImportItem(
    importItemId: string,
    state: KnowledgeToMemoryImportItemState,
    reasonCode: string,
    memoryId: string | undefined,
    now: string,
  ): void {
    this.db.prepare(`UPDATE km_import_items SET state=?,reason_code=?,memory_id=?,updated_at=? WHERE import_item_id=?`)
      .run(state, reasonCode, memoryId ?? null, now, importItemId);
  }

  private insertTraceEdgeInTransaction(input: TraceEdgeInput, now: string): void {
    this.db.prepare(`
      INSERT OR IGNORE INTO trace_edges(edge_id,from_type,from_id,to_type,to_id,edge_type,evidence_event_id,created_at)
      VALUES(?,?,?,?,?,?,?,?)
    `).run(kmId('edge'), requireText(input.fromType, 'trace_from_type'), requireText(input.fromId, 'trace_from_id'),
      requireText(input.toType, 'trace_to_type'), requireText(input.toId, 'trace_to_id'), input.edgeType, input.evidenceEventId ?? null, now);
  }

  private refreshKnowledgeToMemoryImportJob(jobId: string, checkpoint: string | undefined, now: string): void {
    const row = this.db.prepare(`SELECT
      COUNT(*) source_count,
      SUM(CASE WHEN state IN ('pending','imported','deduped','conflicted') THEN 1 ELSE 0 END) eligible_count,
      SUM(CASE WHEN state='imported' THEN 1 ELSE 0 END) imported_count,
      SUM(CASE WHEN state='deduped' THEN 1 ELSE 0 END) deduped_count,
      SUM(CASE WHEN state='conflicted' THEN 1 ELSE 0 END) conflict_count,
      SUM(CASE WHEN state='skipped' THEN 1 ELSE 0 END) skipped_count,
      SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) failed_count,
      SUM(CASE WHEN state='pending' THEN 1 ELSE 0 END) pending_count
      FROM km_import_items WHERE job_id=?`).get(jobId) as any;
    const outbox = this.db.prepare(`SELECT COUNT(*) count FROM memory_backend_outbox
      WHERE memory_id IN (SELECT memory_id FROM km_import_items WHERE job_id=? AND memory_id IS NOT NULL)`).get(jobId) as any;
    const pending = Number(row.pending_count ?? 0);
    const failed = Number(row.failed_count ?? 0);
    const imported = Number(row.imported_count ?? 0);
    const deduped = Number(row.deduped_count ?? 0);
    const conflicted = Number(row.conflict_count ?? 0);
    const skipped = Number(row.skipped_count ?? 0);
    const finalState: KnowledgeToMemoryImportJobState = pending > 0 || failed > 0 ? 'partial' : 'completed';
    const anyOutcome = imported + deduped + conflicted + skipped + failed > 0;
    this.db.prepare(`UPDATE km_import_jobs SET state=?,checkpoint=?,source_count=?,eligible_count=?,imported_count=?,deduped_count=?,
      conflict_count=?,skipped_count=?,failed_count=?,outbox_enqueued_count=?,updated_at=?,completed_at=? WHERE job_id=?`)
      .run(finalState, checkpoint ?? null, Number(row.source_count ?? 0), Number(row.eligible_count ?? 0), imported, deduped,
        conflicted, skipped, failed, Number(outbox.count ?? 0), now, finalState === 'completed' && anyOutcome ? now : null, jobId);
  }

  private goldenCaseFromRow(row: any): KmGoldenCase {
    return {
      caseId: row.case_id,
      revision: Number(row.revision),
      state: row.state,
      title: row.title,
      queryHash: row.query_hash,
      queryRedacted: row.query_redacted,
      expectedClaims: parseJsonArray(row.expected_claims_json) as KmGoldenExpectedClaim[],
      sourceRefs: parseJsonArray(row.source_refs_json),
      provenance: parseJsonRecord(row.provenance_json),
      privacyClass: row.privacy_class,
      contentHash: row.content_hash,
      createdBy: row.created_by,
      reviewedBy: row.reviewed_by,
      ...(row.retired_by ? { retiredBy: row.retired_by } : {}),
      createdAt: row.created_at,
      reviewedAt: row.reviewed_at,
      ...(row.retired_at ? { retiredAt: row.retired_at } : {}),
      updatedAt: row.updated_at,
    };
  }

  private shadowComparisonFromRow(row: any): KmShadowComparison {
    return {
      comparisonId: row.comparison_id,
      caseId: row.case_id,
      revision: Number(row.revision),
      rulesSnapshotHash: row.rules_snapshot_hash,
      piSnapshotHash: row.pi_snapshot_hash,
      rulesClaims: parseJsonArray(row.rules_claims_json) as KmShadowComparison['rulesClaims'],
      piClaims: parseJsonArray(row.pi_claims_json) as KmShadowComparison['piClaims'],
      metrics: parseJsonRecord(row.metrics_json) as unknown as KmShadowComparisonMetrics,
      latency: parseJsonRecord(row.latency_json),
      cost: parseJsonRecord(row.cost_json),
      createdAt: row.created_at,
    };
  }

  private shadowReadinessFromRow(row: any): KmShadowReadinessReport {
    return {
      reportId: row.report_id,
      windowHash: row.window_hash,
      thresholds: parseJsonRecord(row.thresholds_json) as Record<string, number>,
      metrics: parseJsonRecord(row.metrics_json) as Record<string, number>,
      ready: Boolean(row.ready),
      reasonCodes: parseJsonArray(row.reason_codes_json).map(String),
      createdAt: row.created_at,
    };
  }

  private computeShadowComparisonMetrics(
    golden: KmGoldenCase,
    rulesClaims: KmShadowComparison['rulesClaims'],
    piClaims: KmShadowComparison['piClaims'],
    labelCounts?: { falsePositiveLabels: number; falseNegativeLabels: number },
  ): KmShadowComparisonMetrics {
    const rulesKeys = new Set(rulesClaims.map(claim => claim.claimKey));
    const piKeys = new Set(piClaims.map(claim => claim.claimKey));
    const expectedKeys = new Set(golden.expectedClaims.map(claim => claim.claimKey));
    const overlap = [...rulesKeys].filter(key => piKeys.has(key)).length;
    const routeByKey = (claims: KmShadowComparison['rulesClaims']) => new Map(claims.map(claim => [claim.claimKey, claim.route ?? '']));
    const rulesRoutes = routeByKey(rulesClaims);
    const piRoutes = routeByKey(piClaims);
    let routingDisagreement = 0;
    for (const key of rulesKeys) {
      if (piKeys.has(key) && rulesRoutes.get(key) !== piRoutes.get(key)) routingDisagreement += 1;
    }
    const allClaims = [...rulesClaims, ...piClaims];
    const claimsWithEvidence = allClaims.filter(claim => Array.isArray(claim.evidenceRefs) && claim.evidenceRefs.length > 0).length;
    return {
      expectedCount: golden.expectedClaims.length,
      rulesClaimCount: rulesKeys.size,
      piClaimCount: piKeys.size,
      rulesTruePositive: [...rulesKeys].filter(key => expectedKeys.has(key)).length,
      rulesFalsePositive: [...rulesKeys].filter(key => !expectedKeys.has(key)).length,
      rulesFalseNegative: [...expectedKeys].filter(key => !rulesKeys.has(key)).length,
      piTruePositive: [...piKeys].filter(key => expectedKeys.has(key)).length,
      piFalsePositive: [...piKeys].filter(key => !expectedKeys.has(key)).length,
      piFalseNegative: [...expectedKeys].filter(key => !piKeys.has(key)).length,
      rulesFalsePositiveRate: Number(([...rulesKeys].filter(key => !expectedKeys.has(key)).length / Math.max(1, rulesKeys.size)).toFixed(4)),
      rulesFalseNegativeRate: Number(([...expectedKeys].filter(key => !rulesKeys.has(key)).length / Math.max(1, expectedKeys.size)).toFixed(4)),
      piFalsePositiveRate: Number(([...piKeys].filter(key => !expectedKeys.has(key)).length / Math.max(1, piKeys.size)).toFixed(4)),
      piFalseNegativeRate: Number(([...expectedKeys].filter(key => !piKeys.has(key)).length / Math.max(1, expectedKeys.size)).toFixed(4)),
      claimOverlap: overlap,
      rulesUnique: [...rulesKeys].filter(key => !piKeys.has(key)).length,
      piUnique: [...piKeys].filter(key => !rulesKeys.has(key)).length,
      routingDisagreement,
      extractorDisagreement: [...rulesKeys].filter(key => !piKeys.has(key)).length + [...piKeys].filter(key => !rulesKeys.has(key)).length + routingDisagreement,
      evidenceCoverage: allClaims.length === 0 ? 0 : Number((claimsWithEvidence / allClaims.length).toFixed(4)),
      privacyBlocks: allClaims.filter(claim => claim.privacyBlocked).length,
      schemaFailures: allClaims.filter(claim => claim.schemaFailure).length,
      falsePositiveLabels: labelCounts?.falsePositiveLabels ?? 0,
      falseNegativeLabels: labelCounts?.falseNegativeLabels ?? 0,
    };
  }

  private refreshComparisonMetrics(comparisonId: string): void {
    const comparison = this.getShadowComparison(comparisonId);
    if (!comparison) return;
    const golden = this.getGoldenCase(comparison.caseId, comparison.revision);
    if (!golden) return;
    const labelRow = this.db.prepare(`SELECT
      SUM(CASE WHEN label='false_positive' THEN 1 ELSE 0 END) false_positive,
      SUM(CASE WHEN label='false_negative' THEN 1 ELSE 0 END) false_negative
      FROM km_shadow_review_labels WHERE comparison_id=?`).get(comparisonId) as any;
    const metrics = this.computeShadowComparisonMetrics(golden, comparison.rulesClaims, comparison.piClaims, {
      falsePositiveLabels: Number(labelRow.false_positive ?? 0),
      falseNegativeLabels: Number(labelRow.false_negative ?? 0),
    });
    this.db.prepare('UPDATE km_shadow_comparisons SET metrics_json=? WHERE comparison_id=?')
      .run(JSON.stringify(metrics), comparisonId);
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

  private migrateToPhase13(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 13) { this.db.exec('COMMIT;'); return; }
      const columns = new Set((this.db.prepare('PRAGMA table_info(prompt_injection_snapshots)').all() as Array<{ name: string }>).map(row => row.name));
      if (!columns.has('requested_mode')) this.db.exec('ALTER TABLE prompt_injection_snapshots ADD COLUMN requested_mode TEXT;');
      if (!columns.has('effective_mode')) this.db.exec('ALTER TABLE prompt_injection_snapshots ADD COLUMN effective_mode TEXT;');
      this.db.exec(PHASE13_SCHEMA);
      this.db.exec('PRAGMA user_version=13;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase14(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 14) { this.db.exec('COMMIT;'); return; }
      const columns = new Set((this.db.prepare('PRAGMA table_info(retrieval_runs)').all() as Array<{ name: string }>).map(row => row.name));
      const add = (name: string, sql: string) => { if (!columns.has(name)) this.db.exec(sql); };
      add('direct_hit_count', 'ALTER TABLE retrieval_runs ADD COLUMN direct_hit_count INTEGER NOT NULL DEFAULT 0;');
      add('normalized_hit_count', 'ALTER TABLE retrieval_runs ADD COLUMN normalized_hit_count INTEGER NOT NULL DEFAULT 0;');
      add('no_hit_count', 'ALTER TABLE retrieval_runs ADD COLUMN no_hit_count INTEGER NOT NULL DEFAULT 0;');
      add('filtered_scope_count', 'ALTER TABLE retrieval_runs ADD COLUMN filtered_scope_count INTEGER NOT NULL DEFAULT 0;');
      add('filtered_privacy_count', 'ALTER TABLE retrieval_runs ADD COLUMN filtered_privacy_count INTEGER NOT NULL DEFAULT 0;');
      add('filtered_state_count', 'ALTER TABLE retrieval_runs ADD COLUMN filtered_state_count INTEGER NOT NULL DEFAULT 0;');
      this.db.exec(PHASE14_SCHEMA);
      this.db.exec('PRAGMA user_version=14;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase15(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 15) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE15_SCHEMA);
      this.db.exec('PRAGMA user_version=15;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase16(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 16) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE16_SCHEMA);
      this.db.exec('PRAGMA user_version=16;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase17(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 17) { this.db.exec('COMMIT;'); return; }
      const columns = new Set((this.db.prepare('PRAGMA table_info(sync_sinks)').all() as Array<{ name: string }>).map(row => row.name));
      if (!columns.has('batch_limit')) this.db.exec('ALTER TABLE sync_sinks ADD COLUMN batch_limit INTEGER NOT NULL DEFAULT 25;');
      if (!columns.has('timeout_ms')) this.db.exec('ALTER TABLE sync_sinks ADD COLUMN timeout_ms INTEGER NOT NULL DEFAULT 5000;');
      if (!columns.has('max_attempts')) this.db.exec('ALTER TABLE sync_sinks ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 5;');
      if (!columns.has('credential_ref')) this.db.exec('ALTER TABLE sync_sinks ADD COLUMN credential_ref TEXT;');
      if (!columns.has('allowlist_json')) this.db.exec(`ALTER TABLE sync_sinks ADD COLUMN allowlist_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(allowlist_json));`);
      if (!columns.has('tls_policy')) this.db.exec(`ALTER TABLE sync_sinks ADD COLUMN tls_policy TEXT NOT NULL DEFAULT 'https-required-for-future-real-transport';`);
      if (!columns.has('payload_max_bytes')) this.db.exec('ALTER TABLE sync_sinks ADD COLUMN payload_max_bytes INTEGER NOT NULL DEFAULT 65536;');
      if (!columns.has('rollback_json')) this.db.exec(`ALTER TABLE sync_sinks ADD COLUMN rollback_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(rollback_json));`);
      this.db.exec('CREATE INDEX IF NOT EXISTS sync_outbox_status_counts ON sync_outbox(sink_id,status,next_attempt_at);');
      this.db.exec('PRAGMA user_version=17;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase18(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 18) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE18_SCHEMA);
      const columns = new Set((this.db.prepare('PRAGMA table_info(km_production_gate_plans)').all() as Array<{ name: string }>).map(row => row.name));
      if (!columns.has('confirmation_token_used_at')) this.db.exec('ALTER TABLE km_production_gate_plans ADD COLUMN confirmation_token_used_at TEXT;');
      this.db.exec('PRAGMA user_version=18;');
      this.db.exec('COMMIT;');
    } catch (error) { try { this.db.exec('ROLLBACK;'); } catch {} throw error; }
  }

  private migrateToPhase19(): void {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      if (this.schemaVersion() >= 19) { this.db.exec('COMMIT;'); return; }
      this.db.exec(PHASE19_SCHEMA);
      this.db.exec('PRAGMA user_version=19;');
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
      'km_retention_reports',
      'km_golden_cases',
      'km_shadow_comparisons',
      'km_shadow_review_labels',
      'km_shadow_readiness_reports',
      'km_import_jobs',
      'km_import_items',
      'km_import_audit',
      'km_production_gate_plans',
      'km_production_gate_audit',
      'km_production_gate_kill_state',
      'km_ingest_targets',
      'km_ingest_runs',
      'km_ingest_items',
      'km_ingest_audit',
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
