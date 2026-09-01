export type KnowledgeAssetLayer = 'L0' | 'L1' | 'L2' | 'L3' | 'L4';
export type WorkspaceSnapshotState = 'complete' | 'partial' | 'stale' | 'unavailable';
export type AssetLifecycle = 'pending-ingest' | 'ingested' | 'ingested_purged' | 'rejected' | 'deprecated' | 'legacy' | 'not-applicable';
export type AssetFreshness = 'fresh' | 'stale' | 'purged' | 'unknown' | 'not-applicable';

export interface KnowledgeAssetV2 {
  assetId: string;
  workspaceId: string;
  layer: KnowledgeAssetLayer;
  kind: 'policy' | 'wiki' | 'l2-entry' | 'skill' | 'reference';
  title: string;
  relativePath: string;
  lifecycle: AssetLifecycle;
  freshness: AssetFreshness;
  contract: { version: 'legacy' | 'v3' | 'unknown'; valid: boolean; errors: string[]; warnings: string[] };
  retrieval: { recallCount: number; lastRecalledAt?: string };
  linkage: { relatedCount: number; canonicalKey?: string; source?: string; ingestRunId?: string };
  updatedAt?: string;
}

export interface WorkspaceKnowledgeSnapshotV2 {
  schemaVersion: 2;
  generatedAt: string;
  state: WorkspaceSnapshotState;
  hash: string;
  durationMs: number;
  roots: Array<{ workspaceId: string; displayRoot: string; state: 'complete' | 'partial'; errors: string[] }>;
  assets: KnowledgeAssetV2[];
  health: {
    totalsByLayer: Record<KnowledgeAssetLayer, number>;
    totalAssets: number;
    contractValidRate: number | null;
    indexConsistencyRate: number | null;
    retrievableRate: number | null;
    linkageCoverageRate: number | null;
    lifecycle: Record<string, number>;
    freshness: Record<string, number>;
    contractErrors: number;
    legacyAssets: number;
  };
  retrievalQuality: {
    indexQueries: number;
    entryRecallEvents: number;
    neverRecalledAssets: number;
    markdownReads: number;
    zeroReadQueries: number | null;
    zeroReadRate: number | null;
    effectivenessRate: number | null;
    fallbackSuccessRate: number | null;
    queryFeedbackRate: number | null;
    evidenceState: 'available' | 'cold_start' | 'partial';
    evidenceQueries: number;
    useLabels: Record<'direct_apply' | 'context_guided' | 'pitfall_avoided' | 'not_used' | 'misleading', number>;
    invalidEvidenceEvents: number;
  };
  attention: {
    contractErrors: KnowledgeAssetV2[];
    pendingIngest: KnowledgeAssetV2[];
    staleOrPurged: KnowledgeAssetV2[];
    neverRecalled: KnowledgeAssetV2[];
    orphaned: KnowledgeAssetV2[];
  };
  errors: string[];
}
