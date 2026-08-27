import { createHash } from 'node:crypto';
import { lstatSync, readFileSync, realpathSync } from 'node:fs';
import { relative, resolve, sep } from 'node:path';
import { canonicalJsonStringify } from '../../utils/canonical-json.js';
import type {
  ImportableMemoryScope,
  KnowledgeItem,
  KnowledgeLayer,
  KnowledgeToMemoryImportConfig,
  KnowledgeToMemoryImportItemInput,
  KnowledgeToMemoryImportReport,
  KnowledgeToMemoryImportRunInput,
  ObservationStore,
} from './observation-store.js';

const MAX_MARKDOWN_BYTES = 256 * 1024;
const SECRET_PATTERNS = [
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/u,
  /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/u,
  /\b(?:password|passwd|secret|token|api[_-]?key)\s*[:=]\s*['"]?[^\s'"]{8,}/iu,
];

export interface KnowledgeToMemoryImportScanInput {
  store: Pick<ObservationStore, 'listKnowledge' | 'createKnowledgeToMemoryImportPreview'>;
  config: KnowledgeToMemoryImportConfig;
  actorId: string;
  idempotencyKey: string;
}

export interface KnowledgeToMemoryImportExecuteInput {
  store: Pick<ObservationStore, 'submitKnowledgeToMemoryImportReview' | 'runKnowledgeToMemoryImport'>;
  jobId: string;
  actorId: string;
  idempotencyKey: string;
  approvalToken?: string;
  maxItems?: number;
}

export interface KnowledgeToMemoryImportStatusStore extends Pick<ObservationStore,
  'listKnowledgeToMemoryImportJobs' | 'getKnowledgeToMemoryImportReport'> {}

function sha256(value: string): string {
  return `sha256:${createHash('sha256').update(value).digest('hex')}`;
}

function stableHash(value: unknown): string {
  return sha256(canonicalJsonStringify(value));
}

function isSafeMarkdownText(value: string): boolean {
  if (/<raw_transcript>|<\/raw_transcript>/iu.test(value)) return false;
  const normalized = value.normalize('NFKC').toLowerCase();
  if (['password', 'passwd', 'secret', 'token', 'api_key', 'api-key'].some(key => normalized.includes(key))) return false;
  return !SECRET_PATTERNS.some(pattern => pattern.test(value));
}

function normalizeRoots(roots: string[]): string[] {
  const resolved = roots.map(root => realpathSync(resolve(root)));
  return [...new Set(resolved)].sort((a, b) => a.localeCompare(b));
}

function assertUnderAllowlist(file: string, roots: string[]): string {
  const resolvedFile = realpathSync(resolve(file));
  if (!roots.some(root => resolvedFile === root || resolvedFile.startsWith(`${root}${sep}`))) {
    throw new Error('km_import_file_not_allowlisted');
  }
  return resolvedFile;
}

function targetForKnowledge(item: KnowledgeItem, config: KnowledgeToMemoryImportConfig): { scope: ImportableMemoryScope; subject: string } {
  return {
    scope: config.scopeByLayer?.[item.targetLayer] ?? config.defaultScope,
    subject: config.subjectByLayer?.[item.targetLayer] ?? config.defaultSubject,
  };
}

function knowledgeItemToImport(item: KnowledgeItem, config: KnowledgeToMemoryImportConfig): KnowledgeToMemoryImportItemInput {
  const target = targetForKnowledge(item, config);
  const sourceRef = {
    kind: 'knowledge_item',
    knowledgeId: item.knowledgeId,
    targetLayer: item.targetLayer,
    title: item.title,
    sourceRefs: item.sourceRefs,
  };
  const content = {
    scope: target.scope,
    subject: target.subject,
    claimKey: item.claimKey,
    claimText: item.claimText,
    confidence: item.confidence,
    privacyClass: item.privacyClass,
    freshness: item.freshness,
    sourceRef,
  };
  return {
    sourceKind: 'knowledge_item',
    sourceId: item.knowledgeId,
    sourceRef,
    sourceHash: stableHash(sourceRef),
    contentHash: stableHash(content),
    scope: target.scope,
    subject: target.subject,
    claimKey: item.claimKey,
    claimText: item.claimText,
    confidence: item.confidence,
    privacyClass: item.privacyClass,
    freshness: item.freshness,
  };
}

function parseMarkdownClaim(path: string, text: string, config: KnowledgeToMemoryImportConfig, roots: string[]): KnowledgeToMemoryImportItemInput {
  const root = roots.find(candidate => path === candidate || path.startsWith(`${candidate}${sep}`)) ?? roots[0];
  const rel = relative(root, path).replaceAll('\\', '/');
  const title = text.match(/^#\s+(.+)$/mu)?.[1]?.trim() || rel;
  const sourceRef = { kind: 'markdown_file', path, relativePath: rel, title };
  const content = {
    scope: config.defaultScope,
    subject: config.defaultSubject,
    claimKey: `markdown.${rel}`,
    claimText: text.trim(),
    confidence: 'observed',
    privacyClass: 'internal',
    freshness: 'fresh',
    sourceRef,
  };
  return {
    sourceKind: 'markdown_file',
    sourceId: path,
    sourceRef,
    sourceHash: stableHash({ path, bytes: Buffer.byteLength(text), sha256: sha256(text) }),
    contentHash: stableHash(content),
    scope: config.defaultScope,
    subject: config.defaultSubject,
    claimKey: `markdown.${rel}`,
    claimText: text.trim(),
    confidence: 'observed',
    privacyClass: 'internal',
    freshness: 'fresh',
  };
}

function markdownImportCandidates(config: KnowledgeToMemoryImportConfig): KnowledgeToMemoryImportItemInput[] {
  if (config.source === 'knowledge-items') return [];
  const files = config.markdownFiles ?? [];
  if (files.length === 0) return [];
  const roots = normalizeRoots(config.allowlistedRoots);
  const items: KnowledgeToMemoryImportItemInput[] = [];
  for (const file of files) {
    const path = assertUnderAllowlist(file, roots);
    const stat = lstatSync(path);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error('km_import_markdown_file_invalid');
    if (!path.endsWith('.md')) throw new Error('km_import_markdown_file_extension_invalid');
    if (stat.size > MAX_MARKDOWN_BYTES) throw new Error('km_import_markdown_file_too_large');
    const text = readFileSync(path, 'utf8');
    if (!text.trim()) continue;
    if (!isSafeMarkdownText(text)) {
      items.push({
        ...parseMarkdownClaim(path, text.replaceAll(/./gsu, '[redacted]'), config, roots),
        claimText: '[redacted]',
        privacyClass: 'sensitive',
        contentHash: stableHash({ path, redacted: true }),
        state: 'skipped',
        reasonCode: 'markdown_sensitive_pattern',
      });
      continue;
    }
    items.push(parseMarkdownClaim(path, text, config, roots));
  }
  return items;
}

function knowledgeImportCandidates(store: Pick<ObservationStore, 'listKnowledge'>, config: KnowledgeToMemoryImportConfig): KnowledgeToMemoryImportItemInput[] {
  if (config.source === 'markdown-files') return [];
  return store.listKnowledge({ limit: 500, state: 'approved' })
    .filter(item => item.freshness === 'fresh')
    .filter(item => item.confidence === 'observed')
    .filter(item => item.privacyClass !== 'sensitive' && item.privacyClass !== 'secret-reference-only')
    .map(item => knowledgeItemToImport(item, config));
}

export function createKnowledgeToMemoryImportPreview(input: KnowledgeToMemoryImportScanInput): KnowledgeToMemoryImportReport {
  const items = [
    ...knowledgeImportCandidates(input.store, input.config),
    ...markdownImportCandidates(input.config),
  ];
  return input.store.createKnowledgeToMemoryImportPreview({
    idempotencyKey: input.idempotencyKey,
    actorId: input.actorId,
    config: input.config,
    items,
  });
}

export function executeKnowledgeToMemoryImport(input: KnowledgeToMemoryImportExecuteInput): KnowledgeToMemoryImportReport {
  if (input.approvalToken !== input.jobId) throw new Error('km_import_explicit_approval_required');
  input.store.submitKnowledgeToMemoryImportReview({ jobId: input.jobId, actorId: input.actorId });
  const runInput: KnowledgeToMemoryImportRunInput = {
    jobId: input.jobId,
    actorId: input.actorId,
    ...(input.maxItems ? { maxItems: input.maxItems } : {}),
  };
  return input.store.runKnowledgeToMemoryImport(runInput);
}

export function listKnowledgeToMemoryImportJobs(store: KnowledgeToMemoryImportStatusStore, limit: number) {
  return store.listKnowledgeToMemoryImportJobs(limit);
}

export function getKnowledgeToMemoryImportReport(store: KnowledgeToMemoryImportStatusStore, jobId: string) {
  return store.getKnowledgeToMemoryImportReport(jobId);
}
