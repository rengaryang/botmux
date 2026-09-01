import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { handleKmObservationApi } from '../src/dashboard/km-observation-api.js';
import { scanKmReviewQueue } from '../src/services/km/workspace-knowledge/review-queue.js';

const dirs: string[] = [];
function fixture(): string { const root = mkdtempSync(join(tmpdir(), 'km-review-queue-')); dirs.push(root); return root; }
afterEach(() => dirs.splice(0).forEach(dir => rmSync(dir, { recursive: true, force: true })));

function response() {
  const bodies: unknown[] = [];
  const res = {
    writeHead: vi.fn(),
    end: vi.fn(value => bodies.push(JSON.parse(String(value)))),
  } as any;
  return { res, bodies };
}

describe('KM review queue v2', () => {
  it('projects review decisions from a read-only registry without raw content, secrets, or absolute paths', () => {
    const root = fixture();
    writeFileSync(join(root, 'AGENTS.md'), '# Workspace');
    mkdirSync(join(root, '.distilled/review-registry'), { recursive: true });
    writeFileSync(join(root, '.distilled/review-registry/decision-manifest.json'), JSON.stringify({
      kind: 'km-review-decision',
      status: 'approved',
      secret: 'TOKEN=should-not-leak',
    }));
    writeFileSync(join(root, '.distilled/review-registry/pending-ingest-batch-review-matrix.json'), JSON.stringify({
      schema_version: 1,
      kind: 'pending-ingest-batch-review-matrix',
      generated_at: '2026-09-01T12:00:00.000Z',
      entries: [{
        id: 'l2k-1',
        title: 'Rotate API_KEY=abc123456789 before publishing /root/secret.txt',
        review_batch: 'B0-sensitive-review',
        recommended_business_space: 'database-platform',
        decision: 'pending-review',
        blockers: ['sensitive_content_review_required'],
        planHash: `sha256:${'a'.repeat(64)}`,
        audit_time: '2026-09-01T12:01:00.000Z',
        manifest_path: '.distilled/review-registry/decision-manifest.json',
        claimText: 'raw body must not be returned',
      }],
    }));

    const queue = scanKmReviewQueue({ roots: [root], now: Date.parse('2026-09-01T13:00:00.000Z') });
    expect(queue).toMatchObject({
      schemaVersion: 2,
      generatedAt: '2026-09-01T13:00:00.000Z',
      state: 'available',
      summary: {
        total: 1,
        unavailableManifests: 0,
        byBatch: { 'B0-sensitive-review': 1 },
        byRoute: { 'database-platform': 1 },
        byDecision: { approved: 1 },
      },
    });
    expect(queue.items[0]).toMatchObject({
      itemId: 'l2k-1',
      batch: 'B0-sensitive-review',
      route: 'database-platform',
      decision: 'approved',
      blockers: ['sensitive_content_review_required'],
      planHash: `sha256:${'a'.repeat(64)}`,
      auditTime: '2026-09-01T12:01:00.000Z',
      sourceRef: '.distilled/review-registry/pending-ingest-batch-review-matrix.json',
      manifest: {
        state: 'available',
        kind: 'km-review-decision',
        relativePath: '.distilled/review-registry/decision-manifest.json',
      },
    });
    const wire = JSON.stringify(queue);
    expect(wire).not.toContain('raw body must not be returned');
    expect(wire).not.toContain('/root/secret.txt');
    expect(wire).not.toContain('abc123456789');
    expect(wire).toContain('API_KEY=***');
  });

  it('uses distilled INDEX only as compatibility metadata and leaves missing manifest decisions null', () => {
    const root = fixture();
    writeFileSync(join(root, 'AGENTS.md'), '# Workspace');
    mkdirSync(join(root, '.distilled'), { recursive: true });
    writeFileSync(join(root, '.distilled/INDEX.json'), JSON.stringify({
      version: 1,
      sessions: [{
        sessionId: 'session-1',
        status: 'distilled',
        title: 'Distilled candidate',
        archive_recommendation: { target: 'L2' },
        migrated: false,
      }],
    }));

    const queue = scanKmReviewQueue({ roots: [root] });
    expect(queue.summary).toMatchObject({
      total: 1,
      unavailableManifests: 1,
      byRoute: { L2: 1 },
      byDecision: { unavailable: 1 },
    });
    expect(queue.items[0]).toMatchObject({
      itemId: 'session-1',
      route: 'L2',
      decision: null,
      manifest: { state: 'unavailable', relativePath: null, checksum: null },
    });
  });

  it('serves the read-only dashboard API without opening the SQLite store', async () => {
    const queue = scanKmReviewQueue({ roots: [] });
    const openStore = vi.fn();
    const { res, bodies } = response();
    const handled = await handleKmObservationApi(
      Object.assign(Readable.from([]), { method: 'GET', headers: {} }) as any,
      res,
      new URL('http://localhost/api/km/review-queue-v2'),
      {
        enabled: true,
        openStore,
        kmReviewQueueSnapshot: () => queue,
      },
    );

    expect(handled).toBe(true);
    expect(openStore).not.toHaveBeenCalled();
    expect(res.writeHead).toHaveBeenCalledWith(200, expect.anything());
    expect(bodies).toEqual([queue]);
  });
});
