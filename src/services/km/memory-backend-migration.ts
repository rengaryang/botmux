import { createHash } from 'node:crypto';
import type { ObservationStore, MemoryBackendMigrationSnapshot, MemoryItem } from './observation-store.js';

function contentHash(memory: MemoryItem): string {
  return `sha256:${createHash('sha256').update(JSON.stringify({
    scope: memory.scope,
    subject: memory.subject,
    claimKey: memory.claimKey,
    claimText: memory.claimText,
    privacyClass: memory.privacyClass,
    ttlExpiresAt: memory.ttlExpiresAt ?? null,
  })).digest('hex')}`;
}

export interface MemoryBackendBackfillReport {
  migration: MemoryBackendMigrationSnapshot;
  scanned: number;
  enqueued: number;
  checkpoint: string | null;
  done: boolean;
}

type MemoryBackendMigrationBackfillStore = Pick<ObservationStore,
  | 'getMemoryBackendMigration'
  | 'transitionMemoryBackendMigration'
  | 'listMemoryForBackendMigration'
  | 'enqueueMemoryBackendOperation'
>;
type MemoryBackendMigrationCompareStore = Pick<ObservationStore,
  | 'getMemoryBackendMigration'
  | 'transitionMemoryBackendMigration'
  | 'compareMemoryBackendBindings'
>;

export function enqueueMemoryBackendMigrationBackfill(input: {
  store: MemoryBackendMigrationBackfillStore;
  migrationId: string;
  toProviderId: string;
  limit?: number;
  now?: number;
}): MemoryBackendBackfillReport {
  const migration = input.store.getMemoryBackendMigration(input.migrationId);
  if (!migration) throw new Error('km_memory_migration_not_found');
  if (migration.state !== 'draft' && migration.state !== 'backfilling') {
    throw new Error(`km_memory_migration_backfill_invalid_state:${migration.state}`);
  }
  if (migration.state === 'draft') {
    input.store.transitionMemoryBackendMigration({ migrationId: input.migrationId, toState: 'backfilling', checkpoint: migration.checkpoint, stats: migration.stats });
  }
  const rows = input.store.listMemoryForBackendMigration({ afterMemoryId: migration.checkpoint, limit: input.limit ?? 100 });
  let enqueued = 0;
  for (const memory of rows) {
    const result = input.store.enqueueMemoryBackendOperation({
      memoryId: memory.memoryId,
      providerId: input.toProviderId,
      operation: 'put',
      now: input.now,
      payload: {
        memoryId: memory.memoryId,
        scope: memory.scope,
        subject: memory.subject,
        claimKey: memory.claimKey,
        claimText: memory.claimText,
        privacyClass: memory.privacyClass,
        ttlExpiresAt: memory.ttlExpiresAt,
        sourceRefs: memory.sourceRefs,
        contentHash: contentHash(memory),
      },
    });
    if (result.created) enqueued += 1;
  }
  const checkpoint = rows.at(-1)?.memoryId ?? migration.checkpoint ?? null;
  const done = rows.length < Math.max(1, Math.min(input.limit ?? 100, 500));
  input.store.transitionMemoryBackendMigration({ migrationId: input.migrationId, toState: done ? 'comparing' : 'backfilling',
    checkpoint: checkpoint ?? undefined, stats: { ...migration.stats, scanned: Number(migration.stats.scanned ?? 0) + rows.length,
      enqueued: Number(migration.stats.enqueued ?? 0) + enqueued } });
  return { migration: input.store.getMemoryBackendMigration(input.migrationId)!, scanned: rows.length, enqueued, checkpoint, done };
}

export function compareMemoryBackendMigration(input: {
  store: MemoryBackendMigrationCompareStore;
  migrationId: string;
  fromProviderId: string;
  toProviderId: string;
  sampleLimit?: number;
}): ReturnType<ObservationStore['compareMemoryBackendBindings']> {
  const migration = input.store.getMemoryBackendMigration(input.migrationId);
  if (!migration) throw new Error('km_memory_migration_not_found');
  if (migration.state !== 'comparing') throw new Error(`km_memory_migration_compare_invalid_state:${migration.state}`);
  const report = input.store.compareMemoryBackendBindings({ fromProviderId: input.fromProviderId, toProviderId: input.toProviderId,
    sampleLimit: input.sampleLimit });
  input.store.transitionMemoryBackendMigration({ migrationId: input.migrationId, toState: report.missing === 0 && report.mismatched === 0 ? 'ready' : 'failed',
    checkpoint: migration.checkpoint, stats: { ...migration.stats, compare: report } });
  return report;
}
