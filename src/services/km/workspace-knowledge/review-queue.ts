import { createHash } from 'node:crypto';
import { existsSync, readFileSync, realpathSync, statSync } from 'node:fs';
import { basename, join, relative, resolve, sep } from 'node:path';

export type KmReviewQueueState = 'available' | 'partial' | 'unavailable';

export interface KmReviewQueueItemV2 {
  itemId: string;
  title: string;
  batch: string | null;
  route: string | null;
  decision: string | null;
  blockers: string[];
  planHash: string | null;
  auditTime: string | null;
  sourceRef: string | null;
  manifest: {
    state: 'available' | 'unavailable';
    kind: string | null;
    relativePath: string | null;
    checksum: string | null;
  };
}

export interface KmReviewQueueV2 {
  schemaVersion: 2;
  generatedAt: string;
  state: KmReviewQueueState;
  sources: Array<{
    workspaceId: string;
    kind: string;
    state: 'available' | 'unavailable';
    relativePath: string | null;
    checksum: string | null;
    error?: string;
  }>;
  summary: {
    total: number;
    unavailableManifests: number;
    byBatch: Record<string, number>;
    byRoute: Record<string, number>;
    byDecision: Record<string, number>;
  };
  items: KmReviewQueueItemV2[];
  errors: string[];
}

export interface ScanKmReviewQueueInput {
  roots: string[];
  now?: number;
  maxFileBytes?: number;
}

type Json = Record<string, any>;

const DEFAULT_MAX_BYTES = 2 * 1024 * 1024;
const REVIEW_REGISTRY_PATHS = [
  '.distilled/review-queue-v2.json',
  '.distilled/review-queue.json',
  '.distilled/review-registry.json',
  '.distilled/review-registry/pending-ingest-batch-review-matrix.json',
  '.distilled/review-queue/pending-ingest-batch-review-matrix.json',
] as const;
const DISTILLED_INDEX_PATH = '.distilled/INDEX.json';
const SECRET_VALUE_PATTERNS = [
  /\b((?:AK|SK|API[_-]?KEY|TOKEN|PASSWORD|PASSWD|SECRET)\s*[:=]\s*)['"]?[^\s'",;]+/giu,
  /\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}/giu,
] as const;

export function unavailableKmReviewQueue(error = 'review_registry_not_found'): KmReviewQueueV2 {
  return {
    schemaVersion: 2,
    generatedAt: new Date().toISOString(),
    state: 'unavailable',
    sources: [],
    summary: {
      total: 0,
      unavailableManifests: 0,
      byBatch: {},
      byRoute: {},
      byDecision: {},
    },
    items: [],
    errors: [error],
  };
}

export function scanKmReviewQueue(input: ScanKmReviewQueueInput): KmReviewQueueV2 {
  const now = input.now ?? Date.now();
  const maxFileBytes = input.maxFileBytes ?? DEFAULT_MAX_BYTES;
  const roots = [...new Set(input.roots.map(root => resolve(root)))];
  const items: KmReviewQueueItemV2[] = [];
  const sources: KmReviewQueueV2['sources'] = [];
  const errors: string[] = [];

  for (const configuredRoot of roots) {
    let root: string;
    try { root = realpathSync(configuredRoot); } catch { errors.push('workspace_root_unavailable'); continue; }
    const workspaceId = workspaceIdFor(root);

    for (const rel of REVIEW_REGISTRY_PATHS) {
      readReviewSource({ root, workspaceId, rel, maxFileBytes, items, sources, errors, sourceKind: 'review-registry' });
    }
    readReviewSource({ root, workspaceId, rel: DISTILLED_INDEX_PATH, maxFileBytes, items, sources, errors, sourceKind: 'distilled-index-compat' });
  }

  const unique = dedupeItems(items);
  unique.sort((a, b) => (Date.parse(b.auditTime ?? '') || 0) - (Date.parse(a.auditTime ?? '') || 0)
    || a.itemId.localeCompare(b.itemId));
  return {
    schemaVersion: 2,
    generatedAt: new Date(now).toISOString(),
    state: sources.some(source => source.state === 'available') ? errors.length ? 'partial' : 'available' : 'unavailable',
    sources,
    summary: summarize(unique),
    items: unique,
    errors: [...new Set(errors)],
  };
}

function readReviewSource(input: {
  root: string;
  workspaceId: string;
  rel: string;
  maxFileBytes: number;
  sourceKind: string;
  items: KmReviewQueueItemV2[];
  sources: KmReviewQueueV2['sources'];
  errors: string[];
}): void {
  const file = safeFile(input.root, join(input.root, input.rel), input.maxFileBytes);
  if (!file) return;
  const relativePath = toPosix(input.rel);
  try {
    const text = readFileSync(file, 'utf8');
    const checksum = sha256(text);
    const parsed = JSON.parse(text) as Json | Json[];
    const sourceTime = isoFromUnknown((Array.isArray(parsed) ? undefined : parsed.generated_at ?? parsed.generatedAt ?? parsed.created_at ?? parsed.createdAt)
      ?? statSync(file).mtimeMs);
    const kind = String(Array.isArray(parsed) ? input.sourceKind : parsed.kind ?? input.sourceKind);
    input.sources.push({ workspaceId: input.workspaceId, kind, state: 'available', relativePath, checksum });
    const rows = rowsFromSource(parsed);
    rows.forEach((row, index) => {
      input.items.push(projectReviewRow(row, {
        workspaceId: input.workspaceId,
        sourceKind: kind,
        relativePath,
        checksum,
        sourceTime,
        fallbackId: `${input.workspaceId}:${relativePath}:${index}`,
        root: input.root,
      }));
    });
  } catch (error) {
    input.sources.push({ workspaceId: input.workspaceId, kind: input.sourceKind, state: 'unavailable', relativePath, checksum: null, error: 'review_source_unreadable' });
    input.errors.push(`review_source_unreadable:${relativePath}:${safeError(error)}`);
  }
}

function rowsFromSource(parsed: Json | Json[]): Json[] {
  if (Array.isArray(parsed)) return parsed.filter(isRecord);
  for (const key of ['items', 'entries', 'sessions', 'queue', 'reviewQueue']) {
    if (Array.isArray(parsed[key])) return parsed[key].filter(isRecord);
  }
  return [];
}

function projectReviewRow(row: Json, source: {
  workspaceId: string;
  sourceKind: string;
  relativePath: string;
  checksum: string;
  sourceTime: string | null;
  fallbackId: string;
  root: string;
}): KmReviewQueueItemV2 {
  const recommendation = isRecord(row.archive_recommendation) ? row.archive_recommendation : {};
  const manifest = manifestFromRow(row, source.root);
  const batch = firstText(row.review_batch, row.batch, row.batchId, row.batch_id, manifest.batch);
  const route = firstText(row.route, row.recommended_business_space, row.targetRoute, row.target_route, row.target, recommendation.target, manifest.route);
  const explicitDecision = firstText(row.decision, row.reviewDecision, row.review_decision, row.archive_decision, row.action);
  const decision = source.sourceKind === 'distilled-index-compat'
    ? normalizeDecision(manifest.status ?? null)
    : normalizeDecision(manifest.status ?? explicitDecision ?? null);
  return {
    itemId: stableText(firstText(row.id, row.session_id, row.sessionId, row.knowledgeId, row.assetId), source.fallbackId),
    title: stableText(firstText(row.title, row.name), 'Untitled review item', 180),
    batch,
    route,
    decision,
    blockers: stringArray(row.blockers ?? row.blocker ?? row.reasonCodes ?? row.reason_codes ?? manifest.blockers),
    planHash: firstHash(row.planHash, row.plan_hash, row.previewHash, row.preview_hash, row.manifest_checksum, row.manifestChecksum, manifest.planHash, manifest.checksum)
      ?? (source.sourceKind === 'distilled-index-compat' ? null : source.checksum),
    auditTime: isoFromUnknown(row.auditTime ?? row.audit_time ?? row.reviewedAt ?? row.reviewed_at ?? row.updatedAt ?? row.updated_at ?? row.migrated_at ?? manifest.auditTime)
      ?? source.sourceTime,
    sourceRef: source.relativePath,
    manifest: {
      state: manifest.state,
      kind: manifest.kind,
      relativePath: manifest.relativePath,
      checksum: manifest.checksum,
    },
  };
}

function manifestFromRow(row: Json, root: string): {
  state: 'available' | 'unavailable';
  kind: string | null;
  relativePath: string | null;
  checksum: string | null;
  status?: string;
  batch?: string;
  route?: string;
  blockers?: unknown;
  planHash?: string;
  auditTime?: unknown;
} {
  const manifestPath = firstRawString(row.manifestPath, row.manifest_path, row.registryPath, row.registry_path, row.archive_write_evidence);
  if (!manifestPath) return { state: 'unavailable', kind: null, relativePath: null, checksum: null };
  const safe = safePath(root, manifestPath);
  if (!safe || !existsSync(safe.absolute)) return { state: 'unavailable', kind: null, relativePath: null, checksum: null };
  try {
    const text = readFileSync(safe.absolute, 'utf8');
    const parsed = JSON.parse(text) as Json;
    const status = firstText(parsed.decision, parsed.status);
    const recommendation = isRecord(parsed.archive_recommendation) ? parsed.archive_recommendation : {};
    const batch = firstText(parsed.review_batch, parsed.batch, parsed.batchId, parsed.batch_id);
    const route = firstText(parsed.route, parsed.recommended_business_space, parsed.targetRoute, parsed.target_route, parsed.target, recommendation.target);
    const planHash = firstHash(parsed.planHash, parsed.plan_hash, parsed.previewHash, parsed.preview_hash, parsed.manifest_checksum, parsed.manifestChecksum);
    const auditTime = parsed.auditTime ?? parsed.audit_time ?? parsed.reviewedAt ?? parsed.reviewed_at ?? parsed.updatedAt ?? parsed.updated_at ?? parsed.migrated_at ?? parsed.created_at ?? parsed.createdAt;
    return {
      state: 'available',
      kind: firstText(parsed.kind, parsed.type) ?? 'manifest',
      relativePath: safe.relative,
      checksum: sha256(text),
      ...(status ? { status } : {}),
      ...(batch ? { batch } : {}),
      ...(route ? { route } : {}),
      ...(parsed.blockers ?? parsed.blocker ?? parsed.reasonCodes ?? parsed.reason_codes ? { blockers: parsed.blockers ?? parsed.blocker ?? parsed.reasonCodes ?? parsed.reason_codes } : {}),
      ...(planHash ? { planHash } : {}),
      ...(auditTime ? { auditTime } : {}),
    };
  } catch {
    return { state: 'unavailable', kind: null, relativePath: null, checksum: null };
  }
}

function safeFile(root: string, path: string, maxBytes: number): string | null {
  try {
    const resolvedRoot = realpathSync(root);
    const resolved = realpathSync(path);
    if (resolved !== resolvedRoot && !resolved.startsWith(`${resolvedRoot}${sep}`)) return null;
    const stat = statSync(resolved);
    if (!stat.isFile() || stat.size > maxBytes) return null;
    return resolved;
  } catch {
    return null;
  }
}

function safePath(root: string, value: string): { absolute: string; relative: string } | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  try {
    const resolvedRoot = realpathSync(root);
    const candidate = trimmed.startsWith('/') ? resolve(trimmed) : resolve(root, trimmed);
    const resolved = existsSync(candidate) ? realpathSync(candidate) : candidate;
    if (resolved !== resolvedRoot && !resolved.startsWith(`${resolvedRoot}${sep}`)) return null;
    const rel = toPosix(relative(resolvedRoot, resolved));
    if (!rel || rel.startsWith('../')) return null;
    return { absolute: resolved, relative: rel };
  } catch {
    return null;
  }
}

function dedupeItems(items: KmReviewQueueItemV2[]): KmReviewQueueItemV2[] {
  const seen = new Set<string>();
  const out: KmReviewQueueItemV2[] = [];
  for (const item of items) {
    const key = `${item.sourceRef ?? 'source'}:${item.itemId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function summarize(items: KmReviewQueueItemV2[]): KmReviewQueueV2['summary'] {
  const byBatch: Record<string, number> = {};
  const byRoute: Record<string, number> = {};
  const byDecision: Record<string, number> = {};
  let unavailableManifests = 0;
  for (const item of items) {
    byBatch[item.batch ?? 'unavailable'] = (byBatch[item.batch ?? 'unavailable'] ?? 0) + 1;
    byRoute[item.route ?? 'unavailable'] = (byRoute[item.route ?? 'unavailable'] ?? 0) + 1;
    byDecision[item.decision ?? 'unavailable'] = (byDecision[item.decision ?? 'unavailable'] ?? 0) + 1;
    if (item.manifest.state === 'unavailable') unavailableManifests += 1;
  }
  return { total: items.length, unavailableManifests, byBatch, byRoute, byDecision };
}

function workspaceIdFor(root: string): string {
  return `${basename(root).replace(/[^a-z0-9_-]+/gi, '-').toLowerCase() || 'workspace'}-${createHash('sha256').update(root).digest('hex').slice(0, 8)}`;
}

function firstText(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return redactText(value.trim());
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  }
  return null;
}

function firstRawString(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return null;
}

function firstHash(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === 'string' && /^sha256:[a-f0-9]{64}$/iu.test(value.trim())) return value.trim();
    if (typeof value === 'string' && /^[a-f0-9]{64}$/iu.test(value.trim())) return `sha256:${value.trim()}`;
  }
  return null;
}

function safeError(error: unknown): string {
  return redactText(String(error instanceof Error ? error.message : error)).slice(0, 160);
}

function normalizeDecision(value: string | null): string | null {
  if (!value) return null;
  return redactText(value).replaceAll('_', '-').toLowerCase();
}

function stringArray(value: unknown): string[] {
  const source = Array.isArray(value) ? value : typeof value === 'string' ? value.split(',') : [];
  return source.map(item => redactText(String(item).trim())).filter(Boolean).slice(0, 12);
}

function stableText(value: string | null, fallback: string, maxLength = 120): string {
  const text = redactText(value ?? fallback).replace(/\s+/gu, ' ').trim();
  return text.length > maxLength ? `${text.slice(0, maxLength - 3)}...` : text;
}

function redactText(value: string): string {
  return SECRET_VALUE_PATTERNS.reduce((text, pattern) => text.replace(pattern, '$1***'), value)
    .replace(/\/(?:Users|home|root|data\d*|tmp|var|private|opt)\/[^\s'",;]+/giu, '[absolute-path-redacted]')
    .replace(/[A-Z]:(?:\\|\/)[^\s'",;]+/gu, '[absolute-path-redacted]');
}

function isoFromUnknown(value: unknown): string | null {
  if (typeof value === 'number' && Number.isFinite(value)) return new Date(value).toISOString();
  if (typeof value !== 'string' || !value.trim()) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

function sha256(value: string): string {
  return `sha256:${createHash('sha256').update(value).digest('hex')}`;
}

function isRecord(value: unknown): value is Json {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function toPosix(path: string): string {
  return path.split(sep).join('/');
}
