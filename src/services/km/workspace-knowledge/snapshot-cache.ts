import { discoverWorkspaceKnowledgeRoots, scanWorkspaceKnowledge } from './scanner.js';
import type { WorkspaceKnowledgeSnapshotV2 } from './types.js';
import { scanKmReviewQueue, unavailableKmReviewQueue, type KmReviewQueueV2 } from './review-queue.js';

export class WorkspaceKnowledgeSnapshotCache {
  private snapshot?: WorkspaceKnowledgeSnapshotV2;
  private reviewQueue?: KmReviewQueueV2;
  private timer?: NodeJS.Timeout;
  private initial?: NodeJS.Immediate;

  constructor(private readonly candidates: () => Array<string | undefined | null>, private readonly intervalMs = 300_000) {}

  start(): void {
    // Never put filesystem discovery on the Dashboard startup critical path.
    this.stop();
    this.initial = setImmediate(() => { this.initial = undefined; this.refresh(); });
    this.initial.unref?.();
    this.timer = setInterval(() => this.refresh(), this.intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.initial) clearImmediate(this.initial);
    if (this.timer) clearInterval(this.timer);
    this.initial = undefined;
    this.timer = undefined;
  }

  get(): WorkspaceKnowledgeSnapshotV2 {
    return this.snapshot ?? unavailableSnapshot('workspace_scan_not_started');
  }

  getReviewQueue(): KmReviewQueueV2 {
    return this.reviewQueue ?? unavailableKmReviewQueue('workspace_scan_not_started');
  }

  refresh(): WorkspaceKnowledgeSnapshotV2 {
    try {
      const roots = discoverWorkspaceKnowledgeRoots(this.candidates());
      const next = scanWorkspaceKnowledge({ roots });
      this.reviewQueue = scanKmReviewQueue({ roots });
      this.snapshot = next;
      return next;
    } catch (error) {
      if (this.snapshot) {
        this.snapshot = { ...this.snapshot, state: 'stale', errors: [...this.snapshot.errors, `scan_failed:${safeError(error)}`] };
        this.reviewQueue = { ...this.getReviewQueue(), state: 'partial', errors: [...this.getReviewQueue().errors, `scan_failed:${safeError(error)}`] };
        return this.snapshot;
      }
      this.reviewQueue = unavailableKmReviewQueue(`scan_failed:${safeError(error)}`);
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
      zeroReadRate: null, effectivenessRate: null, fallbackSuccessRate: null, queryFeedbackRate: null, evidenceState: 'cold_start', evidenceQueries: 0,
      useLabels: { direct_apply: 0, context_guided: 0, pitfall_avoided: 0, not_used: 0, misleading: 0 }, invalidEvidenceEvents: 0 },
    attention: { contractErrors: [], pendingIngest: [], staleOrPurged: [], neverRecalled: [], orphaned: [] },
  };
}
function safeError(error: unknown): string { return String(error instanceof Error ? error.message : error).slice(0, 200); }
