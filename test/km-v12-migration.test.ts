import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { defaultShadowProfile } from '../src/services/km/runtime-orchestrator.js';

const dirs: string[] = [];
function tempDir() { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-v12-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

describe('KM v12 migration', () => {
  it('retires duplicate shadows before adding the uniqueness index', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    const first = { ...defaultShadowProfile('bot'), profileId: 'first', revision: 1 };
    const second = { ...defaultShadowProfile('bot'), profileId: 'second', revision: 2 };
    store.putPipelineProfile(first, 'draft'); store.putPipelineProfile(second, 'draft'); store.close();
    const { DatabaseSync } = await import('node:sqlite'); const db = new DatabaseSync(join(dir, 'botmux-km.sqlite'));
    db.exec(`DROP INDEX km_pipeline_profiles_one_shadow_bot; UPDATE km_pipeline_profiles SET state='shadow'; DROP TABLE km_runtime_leases; PRAGMA user_version=11;`); db.close();
    const migrated = await ObservationStore.open(dir);
    expect(migrated.schemaVersion()).toBe(17);
    const rows = migrated.listPipelineProfiles('bot');
    expect(rows.filter(row => row.state === 'shadow')).toHaveLength(1);
    expect(rows.filter(row => row.state === 'retired')).toHaveLength(1);
    migrated.close();
  });

  it('backfills v13 prompt injection mode columns without storing prompt content', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    const runId = store.recordRetrievalAudit({ botAppId: 'bot', sessionId: 's1', queryHash: 'sha256:abc', mode: 'shadow',
      candidateCount: 1, eligibleCount: 1, latencyMs: 1, warnings: [], results: [] });
    store.recordPromptInjectionSnapshot({ retrievalRunId: runId, botAppId: 'bot', mode: 'shadow', disposition: 'would_inject',
      itemIds: ['mem-1'], prompt: 'secret prompt body' });
    store.close();

    const { DatabaseSync } = await import('node:sqlite'); const db = new DatabaseSync(join(dir, 'botmux-km.sqlite'));
    db.exec(`CREATE TABLE prompt_injection_snapshots_old AS SELECT snapshot_id,retrieval_run_id,bot_app_id,mode,disposition,item_ids_json,prompt_hash,prompt_bytes,reason,created_at FROM prompt_injection_snapshots;
      DROP TABLE prompt_injection_snapshots;
      CREATE TABLE prompt_injection_snapshots (
        snapshot_id TEXT PRIMARY KEY, retrieval_run_id TEXT NOT NULL REFERENCES retrieval_runs(retrieval_run_id) ON DELETE CASCADE,
        bot_app_id TEXT NOT NULL, mode TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN ('off','would_inject','injected','skipped')),
        item_ids_json TEXT NOT NULL CHECK(json_valid(item_ids_json)), prompt_hash TEXT, prompt_bytes INTEGER NOT NULL,
        reason TEXT, created_at TEXT NOT NULL
      );
      INSERT INTO prompt_injection_snapshots SELECT * FROM prompt_injection_snapshots_old;
      DROP TABLE prompt_injection_snapshots_old;
      PRAGMA user_version=12;`);
    db.close();

    const migrated = await ObservationStore.open(dir);
    expect(migrated.schemaVersion()).toBe(17);
    expect(migrated.listInjectionSnapshots(1)).toEqual([expect.objectContaining({
      requestedMode: 'shadow',
      effectiveMode: 'shadow',
      promptHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
    })]);
    expect(JSON.stringify(migrated.listInjectionSnapshots(1))).not.toContain('secret prompt body');
    migrated.close();
  });

  it('adds v14 retrieval quality counters to existing audit rows', async () => {
    const dir = tempDir(); const store = await ObservationStore.open(dir);
    const runId = store.recordRetrievalAudit({ botAppId: 'bot', sessionId: 's1', queryHash: 'sha256:abc', mode: 'shadow',
      candidateCount: 1, eligibleCount: 1, latencyMs: 1, warnings: [], results: [] });
    expect(runId).toMatch(/^retr_/);
    store.close();

    const { DatabaseSync } = await import('node:sqlite'); const db = new DatabaseSync(join(dir, 'botmux-km.sqlite'));
    db.exec(`CREATE TABLE retrieval_runs_old AS SELECT retrieval_run_id,bot_app_id,session_id,turn_id,query_hash,mode,candidate_count,eligible_count,latency_ms,warnings_json,created_at FROM retrieval_runs;
      DROP TABLE retrieval_runs;
      CREATE TABLE retrieval_runs (
        retrieval_run_id TEXT PRIMARY KEY, bot_app_id TEXT NOT NULL, session_id TEXT NOT NULL, turn_id TEXT,
        query_hash TEXT NOT NULL, mode TEXT NOT NULL CHECK(mode IN ('off','shadow','canary','active')),
        candidate_count INTEGER NOT NULL, eligible_count INTEGER NOT NULL, latency_ms INTEGER NOT NULL,
        warnings_json TEXT NOT NULL CHECK(json_valid(warnings_json)), created_at TEXT NOT NULL
      );
      INSERT INTO retrieval_runs SELECT * FROM retrieval_runs_old;
      DROP TABLE retrieval_runs_old;
      PRAGMA user_version=13;`);
    db.close();

    const migrated = await ObservationStore.open(dir);
    expect(migrated.schemaVersion()).toBe(17);
    expect(migrated.listRetrievalAudits(1)).toEqual([expect.objectContaining({
      directHitCount: 0,
      normalizedHitCount: 0,
      noHitCount: 0,
      filteredScopeCount: 0,
      filteredPrivacyCount: 0,
      filteredStateCount: 0,
    })]);
    migrated.close();
  });
});
