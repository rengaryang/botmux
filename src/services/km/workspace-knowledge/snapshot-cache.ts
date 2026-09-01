import { discoverWorkspaceKnowledgeRoots, scanWorkspaceKnowledge } from './scanner.js';
import type { WorkspaceKnowledgeSnapshotV2 } from './types.js';

export class WorkspaceKnowledgeSnapshotCache {
  private snapshot?: WorkspaceKnowledgeSnapshotV2;
  private timer?: NodeJS.Timeout;

  constructor(private readonly candidates: () => Array<string | undefined | null>, private readonly intervalMs = 300_000) {}

  start(): void {
    // Never put filesystem discovery on the Dashboard startup critical path.
    const initial = setImmediate(() => this.refresh());
    initial.unref?.();
    this.timer = setInterval(() => this.refresh(), this.intervalMs);
    this.timer.unref?.();
  }

  stop(): void { if (this.timer) clearInterval(this.timer); this.timer = undefined; }

  get(): WorkspaceKnowledgeSnapshotV2 {
    return this.snapshot ?? unavailableSnapshot('workspace_scan_not_started');
  }

  refresh(): WorkspaceKnowledgeSnapshotV2 {
    try {
      const roots = discoverWorkspaceKnowledgeRoots(this.candidates());
      const next = scanWorkspaceKnowledge({ roots });
      this.snapshot = next;
      return next;
    } catch (error) {
      if (this.snapshot) {
        this.snapshot = { ...this.snapshot, state: 'stale', errors: [...this.snapshot.errors, `scan_failed:${safeError(error)}`] };
        return this.snapshot;
      }
      return this.snapshot = unavailableSnapshot(`scan_failed:${safeError(error)}`);
    }
  }
}

function unavailableSnapshot(error: string): WorkspaceKnowledgeSnapshotV2 {
  return {
    schemaVersion: 2, generatedAt: new Date().toISOString(), state: 'unavailable', hash: 'sha256:unavailable', durationMs: 0,
    roots: [], assets: [], errors: [error],
    health: { totalsByLayer: { L0: 0, L1: 0, L2: 0, L3: 0, L4: 0 }, totalAssets: 0, contractValidRate: null,
      indexConsistencyRate: null, retrievableRate: null, linkageCoverageRate: null, lifecycle: {}, freshness: {}, contractErrors: 0, legacyAssets: 0 },
    retrievalQuality: { indexQueries: 0, entryRecallEvents: 0, neverRecalledAssets: 0, markdownReads: 0, zeroReadQueries: null,
      zeroReadRate: null, effectivenessRate: null, fallbackSuccessRate: null, queryFeedbackRate: null, evidenceState: 'cold_start' },
    attention: { contractErrors: [], pendingIngest: [], staleOrPurged: [], neverRecalled: [], orphaned: [] },
  };
}
function safeError(error: unknown): string { return String(error instanceof Error ? error.message : error).slice(0, 200); }
