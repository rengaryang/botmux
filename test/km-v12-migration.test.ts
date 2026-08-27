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
    expect(migrated.schemaVersion()).toBe(13);
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
    expect(migrated.schemaVersion()).toBe(13);
    expect(migrated.listInjectionSnapshots(1)).toEqual([expect.objectContaining({
      requestedMode: 'shadow',
      effectiveMode: 'shadow',
      promptHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
    })]);
    expect(JSON.stringify(migrated.listInjectionSnapshots(1))).not.toContain('secret prompt body');
    migrated.close();
  });
});
