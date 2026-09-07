import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { handleKmObservationApi } from '../src/dashboard/km-observation-api.js';
import { createKnowledgeToMemoryImportPreview, executeKnowledgeToMemoryImport } from '../src/services/km/knowledge-to-memory-import.js';
import { ObservationStore } from '../src/services/km/observation-store.js';

const dirs: string[] = [];
function tempDir(prefix = 'botmux-km-import-'): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  dirs.push(dir);
  return dir;
}
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const sourceRefs = [{ kind: 'api', ref: 'test/import-evidence' }];

function config(root: string) {
  return {
    source: 'knowledge-items' as const,
    allowlistedRoots: [root],
    defaultScope: 'workspace' as const,
    defaultSubject: 'repo-a',
    enqueueBackendOutbox: false,
    batchSize: 50,
  };
}

async function approvedKnowledge(store: ObservationStore, overrides: Partial<Parameters<ObservationStore['proposeKnowledge']>[0]> = {}) {
  const item = store.proposeKnowledge({
    targetLayer: 'L2',
    category: 'runbook',
    title: 'Retry failed imports',
    claimKey: 'imports.retry',
    claimText: 'Resume a partial import by running the same reviewed job again.',
    confidence: 'observed',
    freshness: 'fresh',
    privacyClass: 'internal',
    sourceRefs,
    ...overrides,
  }).item;
  store.transitionKnowledge({ knowledgeId: item.knowledgeId, toState: 'review_pending', reasonCode: 'ready', actorId: 'reviewer' });
  return store.transitionKnowledge({ knowledgeId: item.knowledgeId, toState: 'approved', reasonCode: 'approved', actorId: 'reviewer' });
}

describe('knowledge-to-memory import', () => {
  it('creates a deterministic preview from approved fresh non-sensitive knowledge and replays exactly', async () => {
    const root = tempDir();
    const store = await ObservationStore.open(tempDir());
    await approvedKnowledge(store);
    store.proposeKnowledge({
      targetLayer: 'L2',
      category: 'secret',
      title: 'Secret',
      claimKey: 'secret',
      claimText: 'Do not import.',
      confidence: 'observed',
      freshness: 'fresh',
      privacyClass: 'sensitive',
      sourceRefs,
    });

    const first = createKnowledgeToMemoryImportPreview({ store, config: config(root), actorId: 'user-1', idempotencyKey: 'scan-1' });
    const replay = createKnowledgeToMemoryImportPreview({ store, config: config(root), actorId: 'user-1', idempotencyKey: 'scan-1' });

    expect(first.job.jobId).toBe(replay.job.jobId);
    expect(first.job.state).toBe('preview');
    expect(first.job.sourceCount).toBe(1);
    expect(first.items).toHaveLength(1);
    expect(first.items[0]).toEqual(expect.objectContaining({
      state: 'pending',
      scope: 'workspace',
      subject: 'repo-a',
      sourceKind: 'knowledge_item',
    }));
    expect(() => createKnowledgeToMemoryImportPreview({
      store,
      config: { ...config(root), defaultSubject: 'repo-b' },
      actorId: 'user-1',
      idempotencyKey: 'scan-1',
    })).toThrow(/idempotency_conflict/);
    store.close();
  });

  it('requires explicit approval before execution, imports active memory, preserves provenance, and emits no outbox by default', async () => {
    const root = tempDir();
    const store = await ObservationStore.open(tempDir());
    await approvedKnowledge(store);
    const preview = createKnowledgeToMemoryImportPreview({ store, config: config(root), actorId: 'user-1', idempotencyKey: 'scan-exec' });

    expect(() => store.runKnowledgeToMemoryImport({ jobId: preview.job.jobId, actorId: 'user-1' })).toThrow(/execution_requires_review/);
    const result = executeKnowledgeToMemoryImport({ store, jobId: preview.job.jobId, actorId: 'user-1', idempotencyKey: 'exec-1', approvalToken: preview.job.jobId });

    expect(result.job.state).toBe('completed');
    expect(result.job.importedCount).toBe(1);
    expect(result.job.outboxEnqueuedCount).toBe(0);
    const imported = result.items.find(item => item.state === 'imported');
    expect(imported?.memoryId).toMatch(/^mem_/);
    const memory = store.getMemory(imported!.memoryId!);
    expect(memory).toEqual(expect.objectContaining({ state: 'active', scope: 'workspace', subject: 'repo-a', claimKey: 'imports.retry' }));
    expect(memory?.sourceRefs[0]).toEqual(expect.objectContaining({
      kind: 'km-import',
      jobId: preview.job.jobId,
      sourceRef: expect.objectContaining({ kind: 'knowledge_item' }),
    }));
    expect(store.retrieve({ text: 'partial import', scopes: ['workspace'], subject: 'repo-a', limit: 10 })
      .map(item => item.id)).toContain(imported!.memoryId);
    expect(store.listMemoryBackendOutbox(10)).toEqual([]);
    store.close();
  });

  it('dedupes identical claims and reports conflicts without overwriting active memory', async () => {
    const root = tempDir();
    const store = await ObservationStore.open(tempDir());
    store.upsertMemory({
      state: 'active',
      scope: 'workspace',
      subject: 'repo-a',
      claimKey: 'imports.retry',
      claimText: 'Resume a partial import by running the same reviewed job again.',
      confidence: 'observed',
      privacyClass: 'internal',
      sourceRefs,
    });
    await approvedKnowledge(store);
    await approvedKnowledge(store, {
      knowledgeId: 'kn-conflict',
      claimKey: 'imports.batch',
      claimText: 'Use a single unbounded batch.',
      title: 'Conflicting batch policy',
    });
    store.upsertMemory({
      state: 'active',
      scope: 'workspace',
      subject: 'repo-a',
      claimKey: 'imports.batch',
      claimText: 'Use bounded batches for import execution.',
      confidence: 'observed',
      privacyClass: 'internal',
      sourceRefs,
    });

    const preview = createKnowledgeToMemoryImportPreview({ store, config: config(root), actorId: 'user-1', idempotencyKey: 'scan-conflict' });
    const result = executeKnowledgeToMemoryImport({ store, jobId: preview.job.jobId, actorId: 'user-1', idempotencyKey: 'exec-conflict', approvalToken: preview.job.jobId });

    expect(result.job.dedupedCount).toBe(1);
    expect(result.job.conflictCount).toBe(1);
    expect(store.listMemory({ limit: 10, scope: 'workspace', subject: 'repo-a' })
      .find(item => item.claimKey === 'imports.batch')?.claimText).toBe('Use bounded batches for import execution.');
    store.close();
  });

  it('resumes partial jobs in bounded batches', async () => {
    const root = tempDir();
    const store = await ObservationStore.open(tempDir());
    await approvedKnowledge(store, { knowledgeId: 'kn-1', claimKey: 'one', claimText: 'First resumable import item.' });
    await approvedKnowledge(store, { knowledgeId: 'kn-2', claimKey: 'two', claimText: 'Second resumable import item.' });
    const preview = createKnowledgeToMemoryImportPreview({ store, config: { ...config(root), batchSize: 1 }, actorId: 'user-1', idempotencyKey: 'scan-resume' });

    const first = executeKnowledgeToMemoryImport({ store, jobId: preview.job.jobId, actorId: 'user-1', idempotencyKey: 'exec-resume-1', approvalToken: preview.job.jobId, maxItems: 1 });
    expect(first.job.state).toBe('partial');
    expect(first.job.importedCount).toBe(1);
    const second = executeKnowledgeToMemoryImport({ store, jobId: preview.job.jobId, actorId: 'user-1', idempotencyKey: 'exec-resume-2', approvalToken: preview.job.jobId, maxItems: 1 });
    expect(second.job.state).toBe('completed');
    expect(second.job.importedCount).toBe(2);
    store.close();
  });

  it('imports only explicitly selected markdown under allowlisted roots and redacts unsafe files', async () => {
    const root = tempDir();
    const outside = tempDir('botmux-km-import-outside-');
    const docs = join(root, 'docs');
    mkdirSync(docs, { recursive: true });
    const safeFile = join(docs, 'guide.md');
    const secretFile = join(docs, 'secret.md');
    const outsideFile = join(outside, 'other.md');
    writeFileSync(safeFile, '# Reviewed Guide\\n\\nUse dry-run before importing memory.\\n');
    writeFileSync(secretFile, '# Secret\\n\\napi_key = \"supersecretvalue\"\\n');
    writeFileSync(outsideFile, '# Outside\\n\\nNot allowlisted.\\n');
    const store = await ObservationStore.open(tempDir());

    expect(() => createKnowledgeToMemoryImportPreview({
      store,
      config: { ...config(root), source: 'markdown-files', markdownFiles: [outsideFile] },
      actorId: 'user-1',
      idempotencyKey: 'scan-outside',
    })).toThrow(/file_not_allowlisted/);

    const preview = createKnowledgeToMemoryImportPreview({
      store,
      config: { ...config(root), source: 'markdown-files', markdownFiles: [safeFile, secretFile] },
      actorId: 'user-1',
      idempotencyKey: 'scan-md',
    });
    expect(preview.items.map(item => [item.sourceKind, item.state, item.reasonCode])).toEqual(expect.arrayContaining([
      ['markdown_file', 'pending', undefined],
      ['markdown_file', 'skipped', 'markdown_sensitive_pattern'],
    ]));
    const result = executeKnowledgeToMemoryImport({ store, jobId: preview.job.jobId, actorId: 'user-1', idempotencyKey: 'exec-md', approvalToken: preview.job.jobId });
    expect(result.job.importedCount).toBe(1);
    expect(result.job.skippedCount).toBe(1);
    expect(store.retrieve({ text: 'dry-run importing memory', scopes: ['workspace'], subject: 'repo-a', limit: 10 })
      .some(item => item.title.includes('markdown.docs/guide.md'))).toBe(true);
    store.close();
  });

  it('exposes read-only status and an authenticated explicit execution boundary through the KM API', async () => {
    const root = tempDir();
    const store = await ObservationStore.open(tempDir());
    await approvedKnowledge(store);
    const dataDir = tempDir();
    const seed = await ObservationStore.open(dataDir);
    await approvedKnowledge(seed);
    seed.close();
    const deps = {
      enabled: true,
      actorId: 'user-1',
      openStore: async () => ObservationStore.open(dataDir),
    };
    const makeRes = () => {
      const res: any = { statusCode: 0, headers: {}, body: '', writeHead(status: number, headers: Record<string, string>) { this.statusCode = status; Object.assign(this.headers, headers); }, setHeader(k: string, v: string) { this.headers[k] = v; }, end(v: string) { this.body = v; } };
      return res;
    };
    const req = (method: string, body?: unknown, key = 'api-key') => ({
      method,
      headers: { 'idempotency-key': key },
      async *[Symbol.asyncIterator]() {
        if (body !== undefined) yield Buffer.from(JSON.stringify(body));
      },
    });

    const create = makeRes();
    await handleKmObservationApi(req('POST', { config: config(root) }, 'api-scan') as any, create, new URL('http://localhost/api/km/imports'), deps);
    expect(create.statusCode).toBe(201);
    const created = JSON.parse(create.body);
    const missingApproval = makeRes();
    await handleKmObservationApi(req('POST', {}, 'api-exec-missing') as any, missingApproval, new URL(`http://localhost/api/km/imports/${created.job.jobId}/execute`), deps);
    expect(missingApproval.statusCode).toBe(422);
    const execute = makeRes();
    await handleKmObservationApi(req('POST', { approvalToken: created.job.jobId }, 'api-exec') as any, execute, new URL(`http://localhost/api/km/imports/${created.job.jobId}/execute`), deps);
    expect(execute.statusCode).toBe(200);
    expect(JSON.parse(execute.body).job.state).toBe('completed');
    const status = makeRes();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, status, new URL(`http://localhost/api/km/imports/${created.job.jobId}`), deps);
    expect(status.statusCode).toBe(200);
    expect(JSON.parse(status.body).items).toHaveLength(1);
    store.close();
  });
});
