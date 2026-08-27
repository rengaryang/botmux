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
    expect(migrated.schemaVersion()).toBe(12);
    const rows = migrated.listPipelineProfiles('bot');
    expect(rows.filter(row => row.state === 'shadow')).toHaveLength(1);
    expect(rows.filter(row => row.state === 'retired')).toHaveLength(1);
    migrated.close();
  });
});
