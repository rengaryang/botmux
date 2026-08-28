import { createHash } from 'node:crypto';
import { appendFileSync, existsSync, lstatSync, mkdirSync, readdirSync, readFileSync, realpathSync, unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { delimiter, dirname, join, posix, relative, resolve, sep } from 'node:path';
import { atomicWriteFileSync } from '../../utils/atomic-write.js';
import type { KnowledgeItem, KnowledgeLayer } from './observation-store.js';

export const KM_EXPORT_STAGING_SCHEMA_VERSION = 1;
export const KM_EXPORT_STAGING_DIR = 'km-export-staging';
export const KM_EXPORTER_VERSION = 'km-approved-exporter-v1';
export const KM_FORMAL_EXPORT_EXECUTOR_VERSION = 'km-formal-export-executor-v1';

export type KmKnowledgeExportJobState = 'review_pending' | 'rejected' | 'staged' | 'executing' | 'applied' | 'conflict' | 'failed' | 'rolled_back';
export type KmExportActionKind = 'create' | 'approve' | 'reject' | 'execute' | 'rollback' | 'resume';
export type KmDestinationAdapterKind = 'plain-markdown' | 'command-plan';

export interface KmExportDestination {
  layer: KnowledgeLayer;
  root: string;
  relativePath: string;
  formalPath: string;
  writeMode: 'staging-only' | 'reviewed-only';
  adapterId: string;
  adapterKind: KmDestinationAdapterKind;
  commandPlan: string[];
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

export interface KmFormalExportPrecondition {
  destinationRoot: string;
  targetRelativePath: string;
  expectedTargetHash: string | null;
  currentTargetHash: string | null;
  stagedContentHash: string;
  destinationVersion: string;
}

export interface KmFormalExportPatch {
  deterministicPatchHash: string;
  status: 'new' | 'unchanged' | 'changed' | 'blocked';
  lines: string[];
}

export interface KmFormalExportExecutionPreview {
  schemaVersion: 1;
  executorVersion: string;
  jobId: string;
  state: KmKnowledgeExportJobState;
  adapter: { adapterId: string; kind: KmDestinationAdapterKind; commandPlan: string[] };
  destination: { root: string; relativePath: string; absolutePath: string };
  allowed: boolean;
  reasonCodes: string[];
  precondition: KmFormalExportPrecondition;
  patch: KmFormalExportPatch;
  requiredApprovalGrade: 'G2';
  confirmationToken: string;
  risk: { mutatesWorkspace: boolean; network: false; gitPush: false; fixtureOnly: boolean };
}

export interface KmFormalExportExecutionManifest {
  schemaVersion: 1;
  executorVersion: string;
  jobId: string;
  executionId: string;
  state: 'applied' | 'rolled_back' | 'conflict' | 'failed';
  adapterId: string;
  destination: { root: string; relativePath: string; absolutePath: string };
  precondition: KmFormalExportPrecondition;
  patchHash: string;
  contentHash: string;
  beforeHash: string | null;
  afterHash: string | null;
  backupFile?: string;
  committedAt?: string;
  rolledBackAt?: string;
  rolledBackExecutionId?: string;
  rollbackPlan: string[];
}

export interface KmFormalExportAttempt {
  executionId: string;
  action: 'execute' | 'rollback';
  state: 'prepared' | 'applied' | 'rolled_back' | 'conflict' | 'failed';
  idempotencyKey: string;
  actorId: string;
  approvalGrade: 'G2' | 'G3' | 'G4';
  createdAt: string;
  updatedAt: string;
  error?: string;
  manifestPath?: string;
}

export interface KmKnowledgeExportJob {
  schemaVersion: 1;
  jobId: string;
  state: KmKnowledgeExportJobState;
  plan: KmKnowledgeExportPlan;
  manifest?: KmKnowledgeExportManifest;
  execution?: KmFormalExportExecutionManifest;
  attempts?: KmFormalExportAttempt[];
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

export interface KmFormalExportEnv {
  BOTMUX_KM_FORMAL_EXPORT_ENABLED?: string;
  BOTMUX_KM_FORMAL_EXPORT_ALLOWED_ROOTS?: string;
}

export interface PreviewKmFormalExportInput {
  dataDir: string;
  jobId: string;
  workspaceRoot?: string;
  env?: KmFormalExportEnv;
}

export interface ExecuteKmFormalExportInput extends PreviewKmFormalExportInput {
  actorId: string;
  idempotencyKey: string;
  approvalGrade: 'G2' | 'G3' | 'G4';
  confirmationToken: string;
  expectedTargetHash: string | null;
  destinationVersion: string;
  maxAttempts?: number;
  now?: string;
  simulateCrashAfterPrepare?: boolean;
}

export interface RollbackKmFormalExportInput extends PreviewKmFormalExportInput {
  actorId: string;
  idempotencyKey: string;
  approvalGrade: 'G2' | 'G3' | 'G4';
  confirmationToken: string;
  expectedTargetHash?: string | null;
  destinationVersion?: string;
  now?: string;
}

const LAYER_ROOTS: Readonly<Record<KnowledgeLayer, string>> = {
  L1: 'l1-wiki',
  L2: 'l2-staging',
  L3: 'l3-skills',
  L4: 'l4-references',
  'reviewed-only': 'reviewed-only',
};

const LAYER_ADAPTERS: Readonly<Record<KnowledgeLayer, { adapterId: string; adapterKind: KmDestinationAdapterKind; commandPlan: string[] }>> = {
  L1: {
    adapterId: 'l1-wiki-command-plan-v1',
    adapterKind: 'command-plan',
    commandPlan: [
      'Prepare reviewed Markdown payload for the future Lark/wiki writer.',
      'Do not invoke Lark APIs, network, git push, or external tools from this executor.',
    ],
  },
  L2: {
    adapterId: 'l2-staging-plain-markdown-v1',
    adapterKind: 'plain-markdown',
    commandPlan: [
      'Atomically replace the allowlisted local Markdown destination.',
      'Do not run git commands, network calls, or background schedulers.',
    ],
  },
  L3: {
    adapterId: 'l3-skill-command-plan-v1',
    adapterKind: 'command-plan',
    commandPlan: [
      'Prepare reviewed Markdown payload and patch metadata for a future skill/reference tool.',
      'Do not install skills, edit package registries, invoke network, or push git refs.',
    ],
  },
  L4: {
    adapterId: 'l4-reference-command-plan-v1',
    adapterKind: 'command-plan',
    commandPlan: [
      'Prepare reviewed Markdown payload and patch metadata for a future reference writer.',
      'Do not invoke network, external CLIs, or git push.',
    ],
  },
  'reviewed-only': {
    adapterId: 'reviewed-only-disabled-v1',
    adapterKind: 'command-plan',
    commandPlan: ['Reviewed-only knowledge is intentionally not exportable.'],
  },
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

function appendAudit(dataDir: string, event: Record<string, unknown>): void {
  const path = join(kmExportRoot(dataDir), 'audit.jsonl');
  mkdirSync(dirname(path), { recursive: true });
  appendFileSync(path, `${JSON.stringify(event)}\n`, { mode: 0o600 });
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

function executionRoot(dataDir: string): string {
  return join(kmExportRoot(dataDir), 'executions');
}

function jobPath(dataDir: string, jobId: string): string {
  return join(jobsRoot(dataDir), `${jobId}.json`);
}

function executionDir(dataDir: string, executionId: string): string {
  return join(executionRoot(dataDir), executionId);
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
  const adapter = LAYER_ADAPTERS[item.targetLayer];
  const base = `${slugPart(item.claimKey)}-${sha256Hex(item.knowledgeId).slice(0, 10)}.md`;
  const relative = normalizeRelativePath(`${root}/${base}`);
  if (!relative || !relative.startsWith(`${root}/`)) throw new Error('km_export_invalid_target_path');
  return {
    layer: item.targetLayer,
    root,
    relativePath: relative,
    formalPath: posix.join('knowledge', relative),
    writeMode: item.targetLayer === 'reviewed-only' ? 'reviewed-only' : 'staging-only',
    adapterId: adapter.adapterId,
    adapterKind: adapter.adapterKind,
    commandPlan: [...adapter.commandPlan],
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

function existingDestinationContent(root: string, targetPath: string): string | undefined {
  const path = join(root, targetPath);
  try { return readFileSync(path, 'utf8'); }
  catch { return undefined; }
}

function hashContentOrNull(content: string | undefined): string | null {
  return content === undefined ? null : sha256(content);
}

function fileHashOrNull(path: string): string | null {
  try { return sha256(readFileSync(path, 'utf8')); }
  catch { return null; }
}

function canonicalExistingRoot(root: string): string {
  const resolved = resolve(root);
  const stat = lstatSync(resolved);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('km_export_destination_root_invalid');
  return realpathSync(resolved);
}

function assertNoSymlinkAncestors(root: string, relativePath: string): string {
  const normalized = normalizeRelativePath(relativePath);
  if (!normalized) throw new Error('km_export_path_escape');
  const rootReal = canonicalExistingRoot(root);
  let cursor = rootReal;
  const parts = normalized.split('/');
  for (let i = 0; i < parts.length; i += 1) {
    cursor = join(cursor, parts[i]);
    if (!existsSync(cursor)) continue;
    const stat = lstatSync(cursor);
    if (stat.isSymbolicLink()) throw new Error('km_export_symlink_rejected');
    if (i < parts.length - 1 && !stat.isDirectory()) throw new Error('km_export_destination_parent_not_directory');
    if (i === parts.length - 1 && !stat.isFile()) throw new Error('km_export_destination_not_plain_file');
    const real = realpathSync(cursor);
    assertUnder(rootReal, real);
  }
  return join(rootReal, normalized);
}

function destinationVersion(root: string): string {
  const real = canonicalExistingRoot(root);
  const marker = join(real, '.botmux-km-destination.json');
  try {
    const parsed = JSON.parse(readFileSync(marker, 'utf8')) as { version?: unknown };
    return typeof parsed.version === 'string' && parsed.version.trim() ? parsed.version : sha256(real);
  } catch {
    return sha256(real);
  }
}

function parseAllowedRoots(env: KmFormalExportEnv): string[] {
  const raw = env.BOTMUX_KM_FORMAL_EXPORT_ALLOWED_ROOTS?.trim();
  if (!raw) return [];
  return raw.split(delimiter).map(value => value.trim()).filter(Boolean).map(value => canonicalExistingRoot(value));
}

function exactRootAllowed(root: string, env: KmFormalExportEnv): boolean {
  if (env.BOTMUX_KM_FORMAL_EXPORT_ENABLED !== 'true') return false;
  const canonical = canonicalExistingRoot(root);
  return parseAllowedRoots(env).some(allowed => allowed === canonical);
}

function isFixtureRoot(root: string): boolean {
  const canonical = canonicalExistingRoot(root);
  const rel = relative(canonicalExistingRoot(tmpdir()), canonical);
  return !!rel && !rel.startsWith('..') && !posix.isAbsolute(rel.replaceAll('\\', '/')) && canonical.includes('botmux-km-export-');
}

function defaultWorkspaceRoot(dataDir: string): string {
  return join(tmpdir(), `botmux-km-export-${sha256Hex(resolve(dataDir)).slice(0, 12)}`);
}

function executionIdFor(jobId: string, idempotencyKey: string): string {
  return `kmxe_${sha256Hex(`${jobId}:${idempotencyKey}`).slice(0, 32)}`;
}

function confirmationTokenFor(job: KmKnowledgeExportJob, root: string): string {
  return `kmx-confirm:${sha256Hex(stableJson({
    jobId: job.jobId,
    root: canonicalExistingRoot(root),
    targetPath: job.plan.destination.relativePath,
    contentHash: job.plan.file.contentHash,
    adapterId: job.plan.destination.adapterId,
  })).slice(0, 32)}`;
}

function appendAttempt(job: KmKnowledgeExportJob, attempt: KmFormalExportAttempt): KmKnowledgeExportJob {
  const attempts = job.attempts ?? [];
  const index = attempts.findIndex(item => item.executionId === attempt.executionId);
  const nextAttempts = index >= 0
    ? attempts.map(item => item.executionId === attempt.executionId ? attempt : item)
    : [...attempts, attempt];
  return { ...job, attempts: nextAttempts, updatedAt: attempt.updatedAt };
}

function manifestPathForExecution(dataDir: string, executionId: string): string {
  return join(executionDir(dataDir, executionId), 'manifest.json');
}

function backupPathForExecution(dataDir: string, executionId: string): string {
  return join(executionDir(dataDir, executionId), 'before.md');
}

function commitManifest(dataDir: string, executionId: string, manifest: KmFormalExportExecutionManifest): string {
  const path = manifestPathForExecution(dataDir, executionId);
  assertUnder(executionRoot(dataDir), path);
  mkdirSync(dirname(path), { recursive: true });
  atomicWriteFileSync(path, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600, durable: true, followTargetSymlink: false });
  return path;
}

function relativeToKmRoot(dataDir: string, path: string): string {
  return relative(kmExportRoot(dataDir), path).replaceAll('\\', '/');
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
  appendAudit(input.dataDir, { action: 'create', state: job.state, jobId: job.jobId, actorId: input.actorId, at: now });
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
  appendAudit(input.dataDir, { action: approval.action, state: next.state, jobId: job.jobId, actorId: input.actorId, at: now });
  return next;
}

function resolveWorkspaceRoot(input: PreviewKmFormalExportInput): string {
  const root = input.workspaceRoot?.trim() || defaultWorkspaceRoot(input.dataDir);
  if (!input.workspaceRoot) mkdirSync(root, { recursive: true });
  return canonicalExistingRoot(root);
}

function stagedContentForJob(dataDir: string, job: KmKnowledgeExportJob): string {
  if (!job.manifest?.stagedFile) throw new Error('km_export_staged_manifest_required');
  const stagedFile = join(stagedRoot(dataDir), job.manifest.stagedFile);
  assertUnder(stagedRoot(dataDir), stagedFile);
  const content = readFileSync(stagedFile, 'utf8');
  if (sha256(content) !== job.plan.file.contentHash) throw new Error('km_export_staged_content_mismatch');
  return content;
}

function previewContentForJob(dataDir: string, job: KmKnowledgeExportJob): string {
  if (!job.manifest?.stagedFile) return job.plan.file.content;
  return stagedContentForJob(dataDir, job);
}

function formalPatch(existing: string | undefined, next: string): KmFormalExportPatch {
  const preview = diffLines(existing, next);
  return {
    deterministicPatchHash: sha256(stableJson({ before: existing ?? null, after: next })),
    status: preview.status,
    lines: preview.lines,
  };
}

function writePreparedAttempt(dataDir: string, job: KmKnowledgeExportJob, attempt: KmFormalExportAttempt): KmKnowledgeExportJob {
  const next = appendAttempt(job, attempt);
  writeJson(jobPath(dataDir, job.jobId), next);
  return next;
}

function writeExecutionResult(
  dataDir: string,
  job: KmKnowledgeExportJob,
  attempt: KmFormalExportAttempt,
  manifest: KmFormalExportExecutionManifest,
  jobExecution: KmFormalExportExecutionManifest = manifest,
): KmKnowledgeExportJob {
  const manifestPath = commitManifest(dataDir, attempt.executionId, manifest);
  const finalAttempt: KmFormalExportAttempt = {
    ...attempt,
    state: manifest.state,
    updatedAt: manifest.committedAt ?? manifest.rolledBackAt ?? attempt.updatedAt,
    manifestPath: relativeToKmRoot(dataDir, manifestPath),
  };
  const next = appendAttempt({ ...job, state: manifest.state, execution: jobExecution }, finalAttempt);
  writeJson(jobPath(dataDir, job.jobId), next);
  return next;
}

export function previewKmFormalExport(input: PreviewKmFormalExportInput): KmFormalExportExecutionPreview {
  const job = getKnowledgeExportJob(input.dataDir, input.jobId);
  if (!job) throw new Error('km_export_job_not_found');
  const root = resolveWorkspaceRoot(input);
  const content = previewContentForJob(input.dataDir, job);
  const targetPath = assertNoSymlinkAncestors(root, job.plan.destination.relativePath);
  const current = existingDestinationContent(root, job.plan.destination.relativePath);
  const currentTargetHash = hashContentOrNull(current);
  const destVersion = destinationVersion(root);
  const env = input.env ?? process.env;
  const fixtureOnly = isFixtureRoot(root);
  const formalAllowed = exactRootAllowed(root, env);
  const reasonCodes: string[] = [];
  if (!['staged', 'executing', 'conflict', 'failed'].includes(job.state)) reasonCodes.push(`job_not_staged:${job.state}`);
  if (!fixtureOnly && !formalAllowed) reasonCodes.push('formal_export_not_allowlisted');
  if (job.plan.destination.adapterKind === 'command-plan') reasonCodes.push('command_plan_adapter_no_direct_write');
  const patch = formalPatch(current, content);
  return {
    schemaVersion: 1,
    executorVersion: KM_FORMAL_EXPORT_EXECUTOR_VERSION,
    jobId: job.jobId,
    state: job.state,
    adapter: {
      adapterId: job.plan.destination.adapterId,
      kind: job.plan.destination.adapterKind,
      commandPlan: [...job.plan.destination.commandPlan],
    },
    destination: { root, relativePath: job.plan.destination.relativePath, absolutePath: targetPath },
    allowed: reasonCodes.length === 0,
    reasonCodes,
    precondition: {
      destinationRoot: root,
      targetRelativePath: job.plan.destination.relativePath,
      expectedTargetHash: currentTargetHash,
      currentTargetHash,
      stagedContentHash: sha256(content),
      destinationVersion: destVersion,
    },
    patch,
    requiredApprovalGrade: 'G2',
    confirmationToken: confirmationTokenFor(job, root),
    risk: { mutatesWorkspace: job.plan.destination.adapterKind === 'plain-markdown', network: false, gitPush: false, fixtureOnly },
  };
}

export function executeKmFormalExport(input: ExecuteKmFormalExportInput): KmKnowledgeExportJob {
  const keyFile = idempotencyPath(input.dataDir, `execute:${input.jobId}:${input.idempotencyKey}`);
  const replay = readJson<{ jobId: string }>(keyFile);
  if (replay) {
    const existing = getKnowledgeExportJob(input.dataDir, replay.jobId);
    if (existing) return existing;
  }
  let job = getKnowledgeExportJob(input.dataDir, input.jobId);
  if (!job) throw new Error('km_export_job_not_found');
  if (input.approvalGrade !== 'G2') throw new Error('km_export_approval_grade_required');
  const maxAttempts = input.maxAttempts ?? 3;
  const executionAttempts = (job.attempts ?? []).filter(attempt => attempt.action === 'execute' && attempt.idempotencyKey !== input.idempotencyKey);
  const existingAttempt = (job.attempts ?? []).find(attempt => attempt.action === 'execute' && attempt.idempotencyKey === input.idempotencyKey);
  if (!existingAttempt && executionAttempts.length >= maxAttempts) throw new Error('km_export_retry_exhausted');

  const preview = previewKmFormalExport(input);
  if (input.confirmationToken !== preview.confirmationToken) throw new Error('km_export_confirmation_token_invalid');
  if (!preview.allowed) throw new Error(`km_export_execution_blocked:${preview.reasonCodes.join(',')}`);
  if (input.expectedTargetHash !== preview.precondition.currentTargetHash || input.destinationVersion !== preview.precondition.destinationVersion) {
    const now = input.now ?? new Date().toISOString();
    const executionId = executionIdFor(job.jobId, input.idempotencyKey);
    const attempt: KmFormalExportAttempt = {
      executionId,
      action: 'execute',
      state: 'conflict',
      idempotencyKey: input.idempotencyKey,
      actorId: input.actorId,
      approvalGrade: input.approvalGrade,
      createdAt: existingAttempt?.createdAt ?? now,
      updatedAt: now,
      error: 'km_export_precondition_stale',
    };
    const manifest: KmFormalExportExecutionManifest = {
      schemaVersion: 1,
      executorVersion: KM_FORMAL_EXPORT_EXECUTOR_VERSION,
      jobId: job.jobId,
      executionId,
      state: 'conflict',
      adapterId: job.plan.destination.adapterId,
      destination: preview.destination,
      precondition: { ...preview.precondition, expectedTargetHash: input.expectedTargetHash, destinationVersion: input.destinationVersion },
      patchHash: preview.patch.deterministicPatchHash,
      contentHash: job.plan.file.contentHash,
      beforeHash: preview.precondition.currentTargetHash,
      afterHash: preview.precondition.currentTargetHash,
      rollbackPlan: ['No destination mutation was performed because the precondition was stale. Refresh preview and retry with a new token.'],
    };
    const next = writeExecutionResult(input.dataDir, job, attempt, manifest);
    writeJson(keyFile, { jobId: job.jobId, action: 'execute', executionId, createdAt: now });
    appendAudit(input.dataDir, { action: 'execute', state: 'conflict', jobId: job.jobId, executionId, actorId: input.actorId, at: now });
    return next;
  }

  const now = input.now ?? new Date().toISOString();
  const executionId = executionIdFor(job.jobId, input.idempotencyKey);
  const attempt: KmFormalExportAttempt = {
    executionId,
    action: 'execute',
    state: 'prepared',
    idempotencyKey: input.idempotencyKey,
    actorId: input.actorId,
    approvalGrade: input.approvalGrade,
    createdAt: existingAttempt?.createdAt ?? now,
    updatedAt: now,
  };
  job = writePreparedAttempt(input.dataDir, { ...job, state: 'executing' }, attempt);
  const content = stagedContentForJob(input.dataDir, job);
  const preparedContent = join(executionDir(input.dataDir, executionId), 'content.md');
  mkdirSync(dirname(preparedContent), { recursive: true });
  atomicWriteFileSync(preparedContent, content, { mode: 0o600, durable: true, followTargetSymlink: false });
  const targetPath = assertNoSymlinkAncestors(preview.destination.root, job.plan.destination.relativePath);
  const beforeContent = existingDestinationContent(preview.destination.root, job.plan.destination.relativePath);
  const beforeHash = hashContentOrNull(beforeContent);
  let backupFile: string | undefined;
  if (beforeContent !== undefined) {
    const backup = backupPathForExecution(input.dataDir, executionId);
    mkdirSync(dirname(backup), { recursive: true });
    atomicWriteFileSync(backup, beforeContent, { mode: 0o600, durable: true, followTargetSymlink: false });
    backupFile = relativeToKmRoot(input.dataDir, backup);
  }
  appendAudit(input.dataDir, { action: 'execute', state: 'prepared', jobId: job.jobId, executionId, actorId: input.actorId, at: now });
  if (input.simulateCrashAfterPrepare) throw new Error('km_export_simulated_crash_after_prepare');

  mkdirSync(dirname(targetPath), { recursive: true });
  const commitTargetPath = assertNoSymlinkAncestors(preview.destination.root, job.plan.destination.relativePath);
  atomicWriteFileSync(commitTargetPath, content, { mode: 0o600, durable: true, followTargetSymlink: false });
  const afterHash = fileHashOrNull(commitTargetPath);
  const manifest: KmFormalExportExecutionManifest = {
    schemaVersion: 1,
    executorVersion: KM_FORMAL_EXPORT_EXECUTOR_VERSION,
    jobId: job.jobId,
    executionId,
    state: 'applied',
    adapterId: job.plan.destination.adapterId,
    destination: preview.destination,
    precondition: preview.precondition,
    patchHash: preview.patch.deterministicPatchHash,
    contentHash: job.plan.file.contentHash,
    beforeHash,
    afterHash,
    ...(backupFile ? { backupFile } : {}),
    committedAt: now,
    rollbackPlan: beforeHash
      ? ['Restore the captured backup file through rollbackKmFormalExport after verifying the current target hash.']
      : ['Remove the newly created destination file through rollbackKmFormalExport after verifying the current target hash.'],
  };
  const next = writeExecutionResult(input.dataDir, job, attempt, manifest);
  writeJson(keyFile, { jobId: job.jobId, action: 'execute', executionId, createdAt: now });
  appendAudit(input.dataDir, { action: 'execute', state: 'applied', jobId: job.jobId, executionId, actorId: input.actorId, at: now });
  return next;
}

export function rollbackKmFormalExport(input: RollbackKmFormalExportInput): KmKnowledgeExportJob {
  const keyFile = idempotencyPath(input.dataDir, `rollback:${input.jobId}:${input.idempotencyKey}`);
  const replay = readJson<{ jobId: string }>(keyFile);
  if (replay) {
    const existing = getKnowledgeExportJob(input.dataDir, replay.jobId);
    if (existing) return existing;
  }
  const job = getKnowledgeExportJob(input.dataDir, input.jobId);
  if (!job?.execution || job.execution.state !== 'applied') throw new Error('km_export_applied_execution_required');
  if (input.approvalGrade !== 'G2') throw new Error('km_export_approval_grade_required');
  const root = resolveWorkspaceRoot(input);
  const expectedToken = confirmationTokenFor(job, root);
  if (input.confirmationToken !== expectedToken) throw new Error('km_export_confirmation_token_invalid');
  if (!isFixtureRoot(root) && !exactRootAllowed(root, input.env ?? process.env)) throw new Error('km_export_destination_not_allowlisted');
  const targetPath = assertNoSymlinkAncestors(root, job.plan.destination.relativePath);
  const currentHash = fileHashOrNull(targetPath);
  const expectedHash = input.expectedTargetHash ?? job.execution.afterHash;
  if (currentHash !== expectedHash) throw new Error('km_export_rollback_precondition_stale');
  if (input.destinationVersion && input.destinationVersion !== destinationVersion(root)) throw new Error('km_export_destination_version_stale');

  const now = input.now ?? new Date().toISOString();
  const executionId = executionIdFor(job.jobId, input.idempotencyKey);
  const attempt: KmFormalExportAttempt = {
    executionId,
    action: 'rollback',
    state: 'prepared',
    idempotencyKey: input.idempotencyKey,
    actorId: input.actorId,
    approvalGrade: input.approvalGrade,
    createdAt: now,
    updatedAt: now,
  };
  writePreparedAttempt(input.dataDir, job, attempt);
  if (job.execution.beforeHash && !job.execution.backupFile) throw new Error('km_export_rollback_backup_missing');
  if (job.execution.beforeHash && job.execution.backupFile) {
    const backup = join(kmExportRoot(input.dataDir), job.execution.backupFile);
    assertUnder(kmExportRoot(input.dataDir), backup);
    atomicWriteFileSync(targetPath, readFileSync(backup, 'utf8'), { mode: 0o600, durable: true, followTargetSymlink: false });
  } else if (existsSync(targetPath)) {
    unlinkSync(targetPath);
  }
  const afterHash = fileHashOrNull(targetPath);
  const manifest: KmFormalExportExecutionManifest = {
    ...job.execution,
    executionId,
    state: 'rolled_back',
    beforeHash: currentHash,
    afterHash,
    rolledBackAt: now,
    rolledBackExecutionId: job.execution.executionId,
    rollbackPlan: ['Rollback already applied; reruns with the same idempotency key are no-ops.'],
  };
  const next = writeExecutionResult(input.dataDir, job, attempt, manifest, { ...job.execution, state: 'rolled_back', rolledBackAt: now, rolledBackExecutionId: executionId });
  writeJson(keyFile, { jobId: job.jobId, action: 'rollback', executionId, createdAt: now });
  appendAudit(input.dataDir, { action: 'rollback', state: 'rolled_back', jobId: job.jobId, executionId, actorId: input.actorId, at: now });
  return next;
}
