import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, posix, resolve, sep } from 'node:path';
import { atomicWriteFileSync } from '../../utils/atomic-write.js';
import type { KnowledgeItem, KnowledgeLayer } from './observation-store.js';

export const KM_EXPORT_STAGING_SCHEMA_VERSION = 1;
export const KM_EXPORT_STAGING_DIR = 'km-export-staging';
export const KM_EXPORTER_VERSION = 'km-approved-exporter-v1';

export type KmKnowledgeExportJobState = 'review_pending' | 'rejected' | 'staged';
export type KmExportActionKind = 'create' | 'approve' | 'reject';

export interface KmExportDestination {
  layer: KnowledgeLayer;
  root: string;
  relativePath: string;
  formalPath: string;
  writeMode: 'staging-only' | 'reviewed-only';
}

export interface KmExportPlannedFile {
  relativePath: string;
  content: string;
  contentHash: string;
  bytes: number;
}

export interface KmExportConflict {
  targetPath: string;
  existingJobId: string;
  existingContentHash: string;
  newContentHash: string;
}

export interface KmExportDiffPreview {
  status: 'new' | 'unchanged' | 'changed' | 'blocked';
  lines: string[];
}

export interface KmKnowledgeExportPlan {
  schemaVersion: 1;
  exporterVersion: string;
  knowledgeId: string;
  targetLayer: KnowledgeLayer;
  allowed: boolean;
  requiredApprovalGrade: 'G2';
  reasonCodes: string[];
  destination: KmExportDestination;
  file: KmExportPlannedFile;
  provenance: {
    claimKey: string;
    title: string;
    sourceRefs: unknown[];
    privacyClass: KnowledgeItem['privacyClass'];
    freshness: KnowledgeItem['freshness'];
    state: KnowledgeItem['state'];
    updatedAt: string;
  };
  conflicts: KmExportConflict[];
  diff: KmExportDiffPreview;
  rollbackPlan: string[];
  gates: Array<{ name: string; passed: boolean; reason?: string }>;
  risk: { mutatesFormalDestination: false; stagingOnly: true; automaticExecution: false };
}

export interface KmKnowledgeExportManifest {
  schemaVersion: 1;
  exporterVersion: string;
  jobId: string;
  state: KmKnowledgeExportJobState;
  knowledgeId: string;
  targetLayer: KnowledgeLayer;
  destination: KmExportDestination;
  contentHash: string;
  stagedFile?: string;
  provenance: KmKnowledgeExportPlan['provenance'];
  rollbackPlan: string[];
  approvals: Array<{ action: KmExportActionKind; actorId: string; reasonCode: string; idempotencyKey: string; createdAt: string }>;
  createdAt: string;
  updatedAt: string;
}

export interface KmKnowledgeExportJob {
  schemaVersion: 1;
  jobId: string;
  state: KmKnowledgeExportJobState;
  plan: KmKnowledgeExportPlan;
  manifest?: KmKnowledgeExportManifest;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateKmKnowledgeExportJobInput {
  dataDir: string;
  knowledge: KnowledgeItem;
  actorId: string;
  idempotencyKey: string;
  now?: string;
}

export interface ReviewKmKnowledgeExportJobInput {
  dataDir: string;
  jobId: string;
  decision: 'approved' | 'rejected';
  actorId: string;
  idempotencyKey: string;
  reasonCode: string;
  now?: string;
}

const LAYER_ROOTS: Readonly<Record<KnowledgeLayer, string>> = {
  L1: 'l1-wiki',
  L2: 'l2-staging',
  L3: 'l3-skills',
  L4: 'l4-references',
  'reviewed-only': 'reviewed-only',
};

const CONTENT_HEADER = '<!-- botmux:km-export schema=1; generated for review staging only -->';

function sha256(value: string): string {
  return `sha256:${createHash('sha256').update(value).digest('hex')}`;
}

function sha256Hex(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map(key => `${JSON.stringify(key)}:${stableJson(record[key])}`).join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

function slugPart(value: string): string {
  const slug = value.trim().toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/\.{2,}/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  return slug || 'knowledge';
}

function normalizeRelativePath(input: string): string | null {
  const normalized = posix.normalize(input.replaceAll('\\', '/'));
  if (!normalized || normalized === '.' || normalized.startsWith('../') || normalized.includes('/../') || posix.isAbsolute(normalized)) {
    return null;
  }
  return normalized;
}

function assertUnder(parent: string, child: string): void {
  const root = resolve(parent);
  const target = resolve(child);
  if (target !== root && !target.startsWith(`${root}${sep}`)) throw new Error('km_export_path_escape');
}

export function kmExportRoot(dataDir: string): string {
  return join(dataDir, KM_EXPORT_STAGING_DIR);
}

function jobsRoot(dataDir: string): string {
  return join(kmExportRoot(dataDir), 'jobs');
}

function stagedRoot(dataDir: string): string {
  return join(kmExportRoot(dataDir), 'staged');
}

function jobPath(dataDir: string, jobId: string): string {
  return join(jobsRoot(dataDir), `${jobId}.json`);
}

function idempotencyPath(dataDir: string, key: string): string {
  return join(kmExportRoot(dataDir), 'idempotency', `${sha256Hex(key)}.json`);
}

function targetPathIndex(dataDir: string, targetPath: string): string {
  return join(kmExportRoot(dataDir), 'targets', `${sha256Hex(targetPath)}.json`);
}

function renderKnowledgeContent(item: KnowledgeItem): string {
  return [
    CONTENT_HEADER,
    `# ${item.title}`,
    '',
    `- Knowledge ID: ${item.knowledgeId}`,
    `- Claim key: ${item.claimKey}`,
    `- Layer: ${item.targetLayer}`,
    `- Privacy: ${item.privacyClass}`,
    `- Freshness: ${item.freshness}`,
    `- Confidence: ${item.confidence}`,
    '',
    item.claimText.trim(),
    '',
    '## Provenance',
    '',
    '```json',
    stableJson(item.sourceRefs),
    '```',
    '',
  ].join('\n');
}

function destinationFor(item: KnowledgeItem): KmExportDestination {
  const root = LAYER_ROOTS[item.targetLayer];
  const base = `${slugPart(item.claimKey)}-${sha256Hex(item.knowledgeId).slice(0, 10)}.md`;
  const relative = normalizeRelativePath(`${root}/${base}`);
  if (!relative || !relative.startsWith(`${root}/`)) throw new Error('km_export_invalid_target_path');
  return {
    layer: item.targetLayer,
    root,
    relativePath: relative,
    formalPath: posix.join('knowledge', relative),
    writeMode: item.targetLayer === 'reviewed-only' ? 'reviewed-only' : 'staging-only',
  };
}

function diffLines(existing: string | undefined, next: string): KmExportDiffPreview {
  if (existing === undefined) return { status: 'new', lines: next.split('\n').slice(0, 12).map(line => `+${line}`) };
  if (existing === next) return { status: 'unchanged', lines: [] };
  const before = existing.split('\n');
  const after = next.split('\n');
  const lines: string[] = [];
  const max = Math.max(before.length, after.length);
  for (let i = 0; i < max && lines.length < 40; i += 1) {
    if (before[i] === after[i]) continue;
    if (before[i] !== undefined) lines.push(`-${before[i]}`);
    if (after[i] !== undefined) lines.push(`+${after[i]}`);
  }
  return { status: 'changed', lines };
}

function readJson<T>(path: string): T | null {
  try { return JSON.parse(readFileSync(path, 'utf8')) as T; }
  catch { return null; }
}

function writeJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  atomicWriteFileSync(path, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
}

function readTargetIndex(dataDir: string, targetPath: string): Array<{ jobId: string; contentHash: string }> {
  return readJson<Array<{ jobId: string; contentHash: string }>>(targetPathIndex(dataDir, targetPath)) ?? [];
}

function writeTargetIndex(dataDir: string, targetPath: string, entries: Array<{ jobId: string; contentHash: string }>): void {
  writeJson(targetPathIndex(dataDir, targetPath), entries);
}

function existingStagedContent(dataDir: string, targetPath: string): string | undefined {
  const path = join(stagedRoot(dataDir), targetPath);
  try { return readFileSync(path, 'utf8'); }
  catch { return undefined; }
}

function jobIdFor(plan: KmKnowledgeExportPlan): string {
  return `kmx_${sha256Hex(stableJson({
    knowledgeId: plan.knowledgeId,
    targetPath: plan.destination.relativePath,
    contentHash: plan.file.contentHash,
  })).slice(0, 32)}`;
}

export function planKnowledgeExport(dataDir: string, item: KnowledgeItem): KmKnowledgeExportPlan {
  const destination = destinationFor(item);
  const content = renderKnowledgeContent(item);
  const contentHash = sha256(content);
  const stagedContent = existingStagedContent(dataDir, destination.relativePath);
  const conflicts = readTargetIndex(dataDir, destination.relativePath)
    .filter(entry => entry.contentHash !== contentHash)
    .map(entry => ({
      targetPath: destination.relativePath,
      existingJobId: entry.jobId,
      existingContentHash: entry.contentHash,
      newContentHash: contentHash,
    }));
  const gates = [
    { name: 'state', passed: item.state === 'approved', reason: item.state === 'approved' ? undefined : 'knowledge_not_approved' },
    { name: 'targetLayer', passed: item.targetLayer !== 'reviewed-only', reason: item.targetLayer === 'reviewed-only' ? 'reviewed_only_not_exportable' : undefined },
    { name: 'privacy', passed: item.privacyClass !== 'sensitive' && item.privacyClass !== 'secret-reference-only',
      reason: (item.privacyClass === 'sensitive' || item.privacyClass === 'secret-reference-only') ? 'privacy_not_exportable' : undefined },
    { name: 'freshness', passed: item.freshness === 'fresh', reason: item.freshness === 'fresh' ? undefined : 'knowledge_not_fresh' },
    { name: 'evidence', passed: item.sourceRefs.length > 0, reason: item.sourceRefs.length > 0 ? undefined : 'evidence_required' },
    { name: 'targetPath', passed: normalizeRelativePath(destination.relativePath) === destination.relativePath, reason: 'target_path_invalid' },
    { name: 'conflicts', passed: conflicts.length === 0, reason: conflicts.length ? 'target_conflict' : undefined },
  ];
  const reasonCodes = gates.filter(gate => !gate.passed).map(gate => gate.reason ?? gate.name);
  const diff = diffLines(stagedContent, content);
  return {
    schemaVersion: 1,
    exporterVersion: KM_EXPORTER_VERSION,
    knowledgeId: item.knowledgeId,
    targetLayer: item.targetLayer,
    allowed: reasonCodes.length === 0,
    requiredApprovalGrade: 'G2',
    reasonCodes,
    destination,
    file: { relativePath: destination.relativePath, content, contentHash, bytes: Buffer.byteLength(content) },
    provenance: {
      claimKey: item.claimKey,
      title: item.title,
      sourceRefs: item.sourceRefs,
      privacyClass: item.privacyClass,
      freshness: item.freshness,
      state: item.state,
      updatedAt: item.updatedAt,
    },
    conflicts,
    diff: reasonCodes.length ? { status: 'blocked', lines: [] } : diff,
    rollbackPlan: [
      'Delete only the staged job directory and target index entry created by this job.',
      'Do not delete or mutate formal knowledge destinations from Botmux automation.',
      'If a reviewer copied staged content manually, revert that manual destination change in the destination repository.',
    ],
    gates,
    risk: { mutatesFormalDestination: false, stagingOnly: true, automaticExecution: false },
  };
}

export function createKnowledgeExportJob(input: CreateKmKnowledgeExportJobInput): KmKnowledgeExportJob {
  const keyFile = idempotencyPath(input.dataDir, `create:${input.idempotencyKey}`);
  const replay = readJson<{ jobId: string }>(keyFile);
  if (replay) {
    const existing = getKnowledgeExportJob(input.dataDir, replay.jobId);
    if (existing) return existing;
  }

  const plan = planKnowledgeExport(input.dataDir, input.knowledge);
  if (!plan.allowed) throw new Error(`km_export_gate_rejected:${plan.reasonCodes.join(',')}`);
  const now = input.now ?? new Date().toISOString();
  const job: KmKnowledgeExportJob = {
    schemaVersion: 1,
    jobId: jobIdFor(plan),
    state: 'review_pending',
    plan,
    createdBy: input.actorId,
    createdAt: now,
    updatedAt: now,
  };
  mkdirSync(jobsRoot(input.dataDir), { recursive: true });
  writeJson(jobPath(input.dataDir, job.jobId), job);
  writeJson(keyFile, { jobId: job.jobId, action: 'create', createdAt: now });
  return job;
}

export function getKnowledgeExportJob(dataDir: string, jobId: string): KmKnowledgeExportJob | null {
  if (!/^kmx_[a-f0-9]{32}$/.test(jobId)) return null;
  return readJson<KmKnowledgeExportJob>(jobPath(dataDir, jobId));
}

export function listKnowledgeExportJobs(dataDir: string): KmKnowledgeExportJob[] {
  const root = jobsRoot(dataDir);
  if (!existsSync(root)) return [];
  return readdirSync(root)
    .filter(name => /^kmx_[a-f0-9]{32}\.json$/.test(name))
    .map(name => readJson<KmKnowledgeExportJob>(join(root, name)))
    .filter((job): job is KmKnowledgeExportJob => !!job)
    .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || b.jobId.localeCompare(a.jobId));
}

export function reviewKnowledgeExportJob(input: ReviewKmKnowledgeExportJobInput): KmKnowledgeExportJob {
  const keyFile = idempotencyPath(input.dataDir, `review:${input.jobId}:${input.idempotencyKey}`);
  const replay = readJson<{ jobId: string }>(keyFile);
  if (replay) {
    const existing = getKnowledgeExportJob(input.dataDir, replay.jobId);
    if (existing) return existing;
  }
  const job = getKnowledgeExportJob(input.dataDir, input.jobId);
  if (!job) throw new Error('km_export_job_not_found');
  if (job.state !== 'review_pending') throw new Error(`km_export_job_not_review_pending:${job.state}`);
  const now = input.now ?? new Date().toISOString();
  const approval = {
    action: input.decision === 'approved' ? 'approve' as const : 'reject' as const,
    actorId: input.actorId,
    reasonCode: input.reasonCode.trim() || (input.decision === 'approved' ? 'review_approved' : 'review_rejected'),
    idempotencyKey: input.idempotencyKey,
    createdAt: now,
  };
  const next: KmKnowledgeExportJob = { ...job, state: input.decision === 'approved' ? 'staged' : 'rejected', updatedAt: now };
  if (input.decision === 'approved') {
    const stagedFile = join(stagedRoot(input.dataDir), job.plan.file.relativePath);
    assertUnder(stagedRoot(input.dataDir), stagedFile);
    mkdirSync(dirname(stagedFile), { recursive: true });
    atomicWriteFileSync(stagedFile, job.plan.file.content, { mode: 0o600 });
    const manifest: KmKnowledgeExportManifest = {
      schemaVersion: 1,
      exporterVersion: KM_EXPORTER_VERSION,
      jobId: job.jobId,
      state: 'staged',
      knowledgeId: job.plan.knowledgeId,
      targetLayer: job.plan.targetLayer,
      destination: job.plan.destination,
      contentHash: job.plan.file.contentHash,
      stagedFile: job.plan.file.relativePath,
      provenance: job.plan.provenance,
      rollbackPlan: job.plan.rollbackPlan,
      approvals: [{ action: 'create', actorId: job.createdBy, reasonCode: 'export_job_created', idempotencyKey: '', createdAt: job.createdAt }, approval],
      createdAt: job.createdAt,
      updatedAt: now,
    };
    next.manifest = manifest;
    const manifestPath = join(stagedRoot(input.dataDir), `${job.jobId}.manifest.json`);
    assertUnder(stagedRoot(input.dataDir), manifestPath);
    atomicWriteFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });
    const index = readTargetIndex(input.dataDir, job.plan.destination.relativePath);
    if (!index.some(entry => entry.jobId === job.jobId)) {
      writeTargetIndex(input.dataDir, job.plan.destination.relativePath, [...index, { jobId: job.jobId, contentHash: job.plan.file.contentHash }]);
    }
  }
  writeJson(jobPath(input.dataDir, job.jobId), next);
  writeJson(keyFile, { jobId: job.jobId, action: approval.action, createdAt: now });
  return next;
}
