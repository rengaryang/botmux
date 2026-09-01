import { createHash } from 'node:crypto';
import { existsSync, lstatSync, readFileSync, readdirSync, realpathSync, statSync } from 'node:fs';
import { basename, join, relative, resolve, sep } from 'node:path';
import type { AssetFreshness, AssetLifecycle, KnowledgeAssetLayer, KnowledgeAssetV2, WorkspaceKnowledgeSnapshotV2 } from './types.js';

export interface ScanWorkspaceKnowledgeInput {
  roots: string[];
  now?: number;
  maxFiles?: number;
  maxFileBytes?: number;
}

type Json = Record<string, any>;
const DEFAULT_MAX_FILES = 20_000;
const DEFAULT_MAX_BYTES = 2 * 1024 * 1024;

export function discoverWorkspaceKnowledgeRoots(candidates: Array<string | undefined | null>): string[] {
  const out = new Set<string>();
  for (const candidate of candidates) {
    if (!candidate?.trim()) continue;
    let cursor = resolve(candidate.trim());
    try { cursor = realpathSync(cursor); } catch { continue; }
    for (;;) {
      if (isWorkspaceKnowledgeRoot(cursor)) { out.add(cursor); break; }
      const parent = resolve(cursor, '..');
      if (parent === cursor) break;
      cursor = parent;
    }
  }
  return [...out].sort();
}

export function scanWorkspaceKnowledge(input: ScanWorkspaceKnowledgeInput): WorkspaceKnowledgeSnapshotV2 {
  const started = Date.now();
  const now = input.now ?? started;
  const maxFiles = input.maxFiles ?? DEFAULT_MAX_FILES;
  const maxBytes = input.maxFileBytes ?? DEFAULT_MAX_BYTES;
  const assets: KnowledgeAssetV2[] = [];
  const errors: string[] = [];
  const roots: WorkspaceKnowledgeSnapshotV2['roots'] = [];
  let visited = 0;
  let budgetExhausted = false;

  for (const configuredRoot of [...new Set(input.roots.map(root => resolve(root)))]) {
    const rootErrors: string[] = [];
    let root: string;
    try { root = realpathSync(configuredRoot); } catch { errors.push('workspace_root_unavailable'); continue; }
    const workspaceId = workspaceIdFor(root);
    const add = (asset: KnowledgeAssetV2) => {
      if (visited >= maxFiles) { budgetExhausted = true; if (!rootErrors.includes('scan_file_budget_exceeded')) rootErrors.push('scan_file_budget_exceeded'); return false; }
      visited += 1; assets.push(asset); return true;
    };
    scanSimpleLayer(root, workspaceId, 'L0', 'policy', ['AGENTS.md'], add, maxBytes, rootErrors);
    if (!budgetExhausted) scanMarkdownDirectory(root, workspaceId, 'L1', 'wiki', 'docs/wiki', add, maxBytes, rootErrors);
    if (!budgetExhausted) scanL2(root, workspaceId, add, maxBytes, now, rootErrors);
    if (!budgetExhausted) scanSkills(root, workspaceId, add, maxBytes, rootErrors);
    roots.push({ workspaceId, displayRoot: basename(root), state: rootErrors.length ? 'partial' : 'complete', errors: [...new Set(rootErrors)] });
    errors.push(...rootErrors.map(error => `${workspaceId}:${error}`));
  }

  assets.sort((a, b) => a.layer.localeCompare(b.layer) || a.assetId.localeCompare(b.assetId));
  const snapshotBody = JSON.stringify(assets.map(asset => [asset.assetId, asset.lifecycle, asset.freshness, asset.contract.valid, asset.retrieval.recallCount]));
  const state = roots.length === 0 ? 'unavailable' : errors.length ? 'partial' : 'complete';
  return {
    schemaVersion: 2,
    generatedAt: new Date(now).toISOString(),
    state,
    hash: `sha256:${createHash('sha256').update(snapshotBody).digest('hex')}`,
    durationMs: Date.now() - started,
    roots,
    assets,
    health: health(assets),
    retrievalQuality: retrievalQuality(assets, input.roots, maxBytes),
    attention: {
      contractErrors: assets.filter(asset => !asset.contract.valid).slice(0, 50),
      pendingIngest: assets.filter(asset => asset.lifecycle === 'pending-ingest').slice(0, 50),
      staleOrPurged: assets.filter(asset => asset.freshness === 'stale' || asset.freshness === 'purged').slice(0, 50),
      neverRecalled: assets.filter(asset => asset.layer === 'L2' && asset.retrieval.recallCount === 0).slice(0, 50),
      orphaned: assets.filter(asset => asset.layer === 'L2' && asset.linkage.relatedCount === 0).slice(0, 50),
    },
    errors: [...new Set(errors)],
  };
}

function isWorkspaceKnowledgeRoot(root: string): boolean {
  return existsSync(join(root, 'AGENTS.md')) || existsSync(join(root, 'l2-knowledge', 'INDEX.json'))
    || existsSync(join(root, 'docs', 'wiki')) || existsSync(join(root, '.agents', 'skills'));
}

function workspaceIdFor(root: string): string {
  return `${basename(root).replace(/[^a-z0-9_-]+/gi, '-').toLowerCase() || 'workspace'}-${createHash('sha256').update(root).digest('hex').slice(0, 8)}`;
}

function safeFile(root: string, path: string, maxBytes: number): string | null {
  try {
    const resolved = realpathSync(path);
    if (resolved !== root && !resolved.startsWith(`${root}${sep}`)) return null;
    const stat = statSync(resolved);
    if (!stat.isFile() || stat.size > maxBytes) return null;
    return resolved;
  } catch { return null; }
}

function titleFor(path: string, text: string): string {
  const heading = text.match(/^#\s+(.+)$/m)?.[1]?.trim();
  return heading || basename(path).replace(/\.md$/i, '').replace(/[-_]+/g, ' ');
}

function baseAsset(root: string, workspaceId: string, layer: KnowledgeAssetLayer, kind: KnowledgeAssetV2['kind'], path: string, text: string): KnowledgeAssetV2 {
  const rel = relative(root, path).split(sep).join('/');
  return {
    assetId: `${workspaceId}:${layer}:path:${rel}`,
    workspaceId, layer, kind, title: titleFor(path, text), relativePath: rel,
    lifecycle: 'not-applicable', freshness: 'not-applicable',
    contract: { version: 'unknown', valid: true, errors: [], warnings: [] },
    retrieval: { recallCount: 0 }, linkage: { relatedCount: 0 },
    updatedAt: new Date(statSync(path).mtimeMs).toISOString(),
  };
}

type AddAsset = (asset: KnowledgeAssetV2) => boolean;
function scanSimpleLayer(root: string, workspaceId: string, layer: KnowledgeAssetLayer, kind: KnowledgeAssetV2['kind'], paths: string[], add: AddAsset, maxBytes: number, errors: string[]): void {
  for (const rel of paths) {
    const path = safeFile(root, join(root, rel), maxBytes);
    if (!path) continue;
    try { add(baseAsset(root, workspaceId, layer, kind, path, readFileSync(path, 'utf8'))); } catch { errors.push(`${layer.toLowerCase()}_read_failed`); }
  }
}

function scanMarkdownDirectory(root: string, workspaceId: string, layer: KnowledgeAssetLayer, kind: KnowledgeAssetV2['kind'], relDir: string, add: AddAsset, maxBytes: number, errors: string[]): void {
  const dir = join(root, relDir);
  walkMarkdown(root, dir, path => {
    const safe = safeFile(root, path, maxBytes); if (!safe) return;
    try { return add(baseAsset(root, workspaceId, layer, kind, safe, readFileSync(safe, 'utf8'))); } catch { errors.push(`${layer.toLowerCase()}_read_failed`); }
    return true;
  });
}

function scanSkills(root: string, workspaceId: string, add: AddAsset, maxBytes: number, errors: string[]): void {
  const skills = join(root, '.agents', 'skills');
  if (!existsSync(skills)) return;
  for (const entry of safeEntries(skills)) {
    const skillRoot = join(skills, entry);
    const skill = safeFile(root, join(skillRoot, 'SKILL.md'), maxBytes);
    if (skill) {
      try { const asset = baseAsset(root, workspaceId, 'L3', 'skill', skill, readFileSync(skill, 'utf8')); asset.assetId = `${workspaceId}:L3:skill:${entry}`; if (!add(asset)) return; }
      catch { errors.push('l3_read_failed'); }
    }
    scanReferenceDirectory(root, workspaceId, relative(root, join(skillRoot, 'references')), add, maxBytes, errors);
  }
}

function scanReferenceDirectory(root: string, workspaceId: string, relDir: string, add: AddAsset, maxBytes: number, errors: string[]): void {
  const allowed = new Set(['.md', '.json', '.yaml', '.yml', '.txt', '.sh', '.py']);
  walkFiles(root, join(root, relDir), path => {
    if (![...allowed].some(extension => path.toLowerCase().endsWith(extension))) return;
    const safe = safeFile(root, path, maxBytes); if (!safe) return;
    try { return add(baseAsset(root, workspaceId, 'L4', 'reference', safe, readFileSync(safe, 'utf8'))); }
    catch { errors.push('l4_read_failed'); }
    return true;
  });
}

function scanL2(root: string, workspaceId: string, add: AddAsset, maxBytes: number, now: number, errors: string[]): void {
  const indexPath = safeFile(root, join(root, 'l2-knowledge', 'INDEX.json'), maxBytes);
  if (!indexPath) return;
  let index: Json;
  try { index = JSON.parse(readFileSync(indexPath, 'utf8')); } catch { errors.push('l2_index_invalid_json'); return; }
  if (!Array.isArray(index.entries)) { errors.push('l2_index_entries_missing'); return; }
  const recalls = readRecallLog(root, maxBytes);
  for (const raw of index.entries as Json[]) {
    const id = typeof raw.id === 'string' && raw.id.trim() ? raw.id.trim() : `invalid-${createHash('sha256').update(JSON.stringify(raw)).digest('hex').slice(0, 8)}`;
    const rel = typeof raw.file === 'string' ? raw.file : typeof raw.path === 'string' ? raw.path : '';
    const path = rel ? safeFile(root, join(root, 'l2-knowledge', rel), maxBytes) : null;
    const legacy = !raw.file && Boolean(raw.path);
    const contractErrors: string[] = [];
    const warnings: string[] = [];
    if (!rel) contractErrors.push('path_missing');
    if (!path) contractErrors.push('content_file_missing');
    let frontmatter: Json = {};
    let text = '';
    if (path) {
      try { text = readFileSync(path, 'utf8'); frontmatter = parseSimpleFrontmatter(text); }
      catch { contractErrors.push('content_read_failed'); }
    }
    if (!legacy) {
      for (const field of ['id', 'status', 'created_at', 'title', 'tags', 'trigger_scenarios', 'category', 'knowledge_type']) {
        if (frontmatter[field] == null) contractErrors.push(`frontmatter_${field}_missing`);
      }
      if (frontmatter.id && frontmatter.id !== id) contractErrors.push('index_frontmatter_id_drift');
      if (raw.content_hash && frontmatter.content_hash && !isPlaceholderMetadata(frontmatter.content_hash)
        && raw.content_hash !== frontmatter.content_hash) contractErrors.push('index_frontmatter_hash_drift');
      if (isPlaceholderMetadata(frontmatter.content_hash) || String(frontmatter.size_bytes ?? '').startsWith('0')) warnings.push('frontmatter_metadata_placeholder');
    } else warnings.push('legacy_contract');
    const lifecycle = lifecycleFor(legacy ? 'legacy' : (frontmatter.status ?? raw.status));
    const freshness = freshnessFor(lifecycle, frontmatter.ingested_at ?? raw.ingested_at, now);
    const recall = recalls.get(id);
    if (!add({
      assetId: `${workspaceId}:L2:id:${id}`, workspaceId, layer: 'L2', kind: 'l2-entry',
      title: String(frontmatter.title ?? raw.title ?? titleFor(rel, text)), relativePath: `l2-knowledge/${rel}`,
      lifecycle, freshness,
      contract: { version: legacy ? 'legacy' : 'v3', valid: contractErrors.length === 0 && !legacy, errors: contractErrors, warnings },
      retrieval: { recallCount: recall?.count ?? Number(raw.recall_count ?? 0), ...(recall?.last ? { lastRecalledAt: recall.last } : {}) },
      linkage: { relatedCount: Array.isArray(raw.related_entries) ? raw.related_entries.length : 0,
        ...(frontmatter.canonical_key ?? raw.canonical_key ? { canonicalKey: String(frontmatter.canonical_key ?? raw.canonical_key) } : {}),
        ...(frontmatter.source ?? raw.source ? { source: String(frontmatter.source ?? raw.source) } : {}),
        ...(frontmatter.ingest_run_id ?? raw.ingest_run_id ? { ingestRunId: String(frontmatter.ingest_run_id ?? raw.ingest_run_id) } : {}) },
      ...(path ? { updatedAt: new Date(statSync(path).mtimeMs).toISOString() } : {}),
    })) return;
  }
}

function parseSimpleFrontmatter(text: string): Json {
  if (!text.startsWith('---\n')) return {};
  const end = text.indexOf('\n---\n', 4); if (end < 0) return {};
  const out: Json = {}; let currentList: string | null = null;
  for (const line of text.slice(4, end).split(/\r?\n/)) {
    const item = line.match(/^\s*-\s+(.+)$/); if (item && currentList) { out[currentList].push(unquote(item[1])); continue; }
    const pair = line.match(/^([a-zA-Z0-9_]+):\s*(.*)$/); if (!pair) continue;
    const [, key, raw] = pair; if (!raw) { out[key] = []; currentList = key; } else { out[key] = unquote(raw); currentList = null; }
  }
  return out;
}
function unquote(raw: string): any { const value = raw.replace(/\s+#.*$/u, '').trim(); if (value === 'null') return null; return value.replace(/^['"]|['"]$/g, ''); }
function isPlaceholderMetadata(value: unknown): boolean { return value == null || value === '' || value === 'sha256:0' || value === 0 || value === '0'; }
function lifecycleFor(value: unknown): AssetLifecycle { return ['pending-ingest','ingested','ingested_purged','rejected','deprecated','legacy'].includes(String(value)) ? String(value) as AssetLifecycle : 'legacy'; }
function freshnessFor(lifecycle: AssetLifecycle, ingestedAt: unknown, now: number): AssetFreshness {
  if (lifecycle === 'pending-ingest') return 'fresh'; if (lifecycle === 'ingested_purged') return 'purged';
  if (lifecycle !== 'ingested') return lifecycle === 'legacy' ? 'unknown' : 'not-applicable';
  const parsed = Date.parse(String(ingestedAt ?? '')); return Number.isFinite(parsed) ? (now - parsed >= 7 * 86_400_000 ? 'stale' : 'fresh') : 'unknown';
}
function readRecallLog(root: string, maxBytes: number): Map<string, { count: number; last: string }> {
  const result = new Map<string, { count: number; last: string }>();
  for (const row of readRecallRows(root, maxBytes)) {
    if (typeof row.entry_id !== 'string' || !row.entry_id) continue;
    if (row.event_type && row.event_type !== 'entry_recalled') continue;
    const old = result.get(row.entry_id); result.set(row.entry_id, { count: (old?.count ?? 0) + 1, last: String(row.recalled_at ?? row.observed_at ?? old?.last ?? '') });
  }
  return result;
}
function readRecallRows(root: string, maxBytes: number): Json[] {
  const path = safeFile(root, join(root, 'l2-knowledge', '.recall_log.jsonl'), maxBytes); if (!path) return [];
  const rows: Json[] = []; for (const line of readFileSync(path, 'utf8').split(/\r?\n/)) try { const row = JSON.parse(line); if (row && typeof row === 'object') rows.push(row); } catch { /* tolerant */ }
  return rows;
}
const USE_LABELS = ['direct_apply', 'context_guided', 'pitfall_avoided', 'not_used', 'misleading'] as const;
function retrievalQuality(assets: KnowledgeAssetV2[], roots: string[], maxBytes = DEFAULT_MAX_BYTES): WorkspaceKnowledgeSnapshotV2['retrievalQuality'] {
  let indexQueries = 0; let markdownReads = 0; let invalidEvidenceEvents = 0; const queryIds = new Set<string>(); const indexQueryIds = new Set<string>(); const readQueryIds = new Set<string>(); const feedbackQueryIds = new Set<string>(); const fallbackAttempts = new Set<string>(); const fallbackSuccesses = new Set<string>();
  const useLabels: Record<(typeof USE_LABELS)[number], number> = { direct_apply: 0, context_guided: 0, pitfall_avoided: 0, not_used: 0, misleading: 0 };
  for (const root of roots) for (const row of readRecallRows(root, maxBytes)) {
    const eventType = String(row.event_type ?? (row.entry_id ? 'entry_recalled' : ''));
    if (eventType === 'index_query') { indexQueries += 1; if (validEvidenceHash(row.query_hash)) { queryIds.add(row.query_hash); indexQueryIds.add(row.query_hash); } else if (row.query_hash != null) invalidEvidenceEvents += 1; continue; }
    if (eventType === 'entry_read') { if (!validEvidenceHash(row.query_hash) || typeof row.entry_id !== 'string') { invalidEvidenceEvents += 1; continue; } markdownReads += 1; readQueryIds.add(row.query_hash); queryIds.add(row.query_hash); continue; }
    if (eventType === 'entry_used') { const label = String(row.use_label); if (!validEvidenceHash(row.query_hash) || !USE_LABELS.includes(label as any)) { invalidEvidenceEvents += 1; continue; } useLabels[label as keyof typeof useLabels] += 1; queryIds.add(row.query_hash); continue; }
    if (eventType === 'fallback') { if (!validEvidenceHash(row.query_hash) || !['success', 'no_hit', 'error'].includes(String(row.outcome))) { invalidEvidenceEvents += 1; continue; } fallbackAttempts.add(row.query_hash); if (row.outcome === 'success') fallbackSuccesses.add(row.query_hash); queryIds.add(row.query_hash); continue; }
    if (eventType === 'query_feedback') { if (!validEvidenceHash(row.query_hash) || !['helpful', 'not_helpful'].includes(String(row.feedback))) { invalidEvidenceEvents += 1; continue; } feedbackQueryIds.add(row.query_hash); queryIds.add(row.query_hash); }
  }
  const l2 = assets.filter(asset => asset.layer === 'L2'); const entryRecallEvents = l2.reduce((sum, asset) => sum + asset.retrieval.recallCount, 0);
  const effectiveUses = useLabels.direct_apply + useLabels.context_guided + useLabels.pitfall_avoided; const totalUses = effectiveUses + useLabels.not_used + useLabels.misleading;
  const rate = (part: number, total: number) => total ? Math.round(part / total * 1000) / 10 : null;
  const evidenceState = queryIds.size === 0 && markdownReads === 0 && totalUses === 0 && fallbackAttempts.size === 0 && feedbackQueryIds.size === 0 ? 'cold_start' : invalidEvidenceEvents ? 'partial' : 'available';
  return { indexQueries, entryRecallEvents, neverRecalledAssets: l2.filter(asset => !asset.retrieval.recallCount).length, markdownReads,
    zeroReadQueries: indexQueryIds.size ? [...indexQueryIds].filter(hash => !readQueryIds.has(hash)).length : null,
    zeroReadRate: indexQueryIds.size ? rate([...indexQueryIds].filter(hash => !readQueryIds.has(hash)).length, indexQueryIds.size) : null,
    effectivenessRate: rate(effectiveUses, totalUses), fallbackSuccessRate: rate(fallbackSuccesses.size, fallbackAttempts.size), queryFeedbackRate: rate(feedbackQueryIds.size, queryIds.size), evidenceState,
    evidenceQueries: queryIds.size, useLabels, invalidEvidenceEvents };
}
function validEvidenceHash(value: unknown): value is string { return typeof value === 'string' && /^sha256:[a-f0-9]{64}$/i.test(value); }
function health(assets: KnowledgeAssetV2[]): WorkspaceKnowledgeSnapshotV2['health'] {
  const layers = { L0: 0, L1: 0, L2: 0, L3: 0, L4: 0 }; const lifecycle: Record<string, number> = {}; const freshness: Record<string, number> = {};
  for (const asset of assets) { layers[asset.layer] += 1; lifecycle[asset.lifecycle] = (lifecycle[asset.lifecycle] ?? 0) + 1; freshness[asset.freshness] = (freshness[asset.freshness] ?? 0) + 1; }
  const l2 = assets.filter(asset => asset.layer === 'L2'); const v3 = l2.filter(asset => asset.contract.version === 'v3'); const eligible = l2.filter(asset => !['rejected','deprecated'].includes(asset.lifecycle));
  const rate = (part: number, total: number) => total ? Math.round(part / total * 1000) / 10 : null;
  return { totalsByLayer: layers, totalAssets: assets.length, contractValidRate: rate(v3.filter(asset => asset.contract.valid).length, v3.length), indexConsistencyRate: rate(l2.filter(asset => !asset.contract.errors.some(error => error.includes('drift') || error.includes('missing'))).length, l2.length), retrievableRate: rate(eligible.filter(asset => asset.freshness !== 'purged' || Boolean(asset.linkage.canonicalKey)).length, eligible.length), linkageCoverageRate: rate(l2.filter(asset => asset.linkage.relatedCount > 0).length, l2.length), lifecycle, freshness, contractErrors: assets.filter(asset => !asset.contract.valid).length, legacyAssets: assets.filter(asset => asset.contract.version === 'legacy').length };
}
function walkMarkdown(root: string, dir: string, visit: (path: string) => boolean | void): void { walkFiles(root, dir, path => path.endsWith('.md') ? visit(path) : true); }
function walkFiles(root: string, dir: string, visit: (path: string) => boolean | void): boolean { try { const real = realpathSync(dir); if (real !== root && !real.startsWith(`${root}${sep}`)) return true; for (const entry of readdirSync(real, { withFileTypes: true })) { if (entry.name === '.git' || entry.name === 'node_modules') continue; const path = join(real, entry.name); if (entry.isDirectory()) { if (!walkFiles(root, path, visit)) return false; } else if (entry.isFile() && visit(path) === false) return false; } } catch {} return true; }
function safeEntries(dir: string): string[] { try { return readdirSync(dir).filter(entry => { try { return lstatSync(join(dir, entry)).isDirectory(); } catch { return false; } }); } catch { return []; } }
