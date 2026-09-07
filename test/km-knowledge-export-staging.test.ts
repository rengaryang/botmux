import { mkdirSync, mkdtempSync, readFileSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { describe, expect, it } from 'vitest';
import type { KnowledgeItem } from '../src/services/km/observation-store.js';
import {
  createKnowledgeExportJob,
  executeKmFormalExport,
  getKnowledgeExportJob,
  listKnowledgeExportJobs,
  planKnowledgeExport,
  previewKmFormalExport,
  reviewKnowledgeExportJob,
  rollbackKmFormalExport,
} from '../src/services/km/knowledge-export-staging.js';

function tmpDataDir(): string {
  return mkdtempSync(join(tmpdir(), 'botmux-km-export-'));
}

function item(overrides: Partial<KnowledgeItem> = {}): KnowledgeItem {
  const now = '2026-08-27T00:00:00.000Z';
  return {
    knowledgeId: 'kn_abc',
    state: 'approved',
    targetLayer: 'L2',
    category: 'ops',
    title: 'Hybrid DB deploy guard',
    claimKey: 'ops.deploy.guard',
    claimText: 'Require approval before environment writes.',
    confidence: 'observed',
    freshness: 'fresh',
    privacyClass: 'internal',
    sourceRefs: [{ kind: 'turn', ref: 'session/turn-1', sha256: 'abc' }],
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

describe('KM approved knowledge export staging', () => {
  it('maps approved knowledge layers to deterministic staged destinations', () => {
    const dataDir = tmpDataDir();
    const layers = [
      ['L1', 'l1-wiki'],
      ['L2', 'l2-staging'],
      ['L3', 'l3-skills'],
      ['L4', 'l4-references'],
    ] as const;
    for (const [layer, root] of layers) {
      const plan = planKnowledgeExport(dataDir, item({ knowledgeId: `kn_${layer}`, targetLayer: layer }));
      expect(plan.allowed).toBe(true);
      expect(plan.destination.relativePath).toMatch(new RegExp(`^${root}/ops\\.deploy\\.guard-[a-f0-9]{10}\\.md$`));
      expect(plan.destination.formalPath).toBe(`knowledge/${plan.destination.relativePath}`);
      expect(plan.file.contentHash).toMatch(/^sha256:[a-f0-9]{64}$/);
      expect(plan.risk).toEqual({ mutatesFormalDestination: false, stagingOnly: true, automaticExecution: false });
    }
  });

  it('rejects non-approved, reviewed-only, private, stale, and missing-evidence exports', () => {
    const dataDir = tmpDataDir();
    expect(planKnowledgeExport(dataDir, item({ state: 'candidate' })).reasonCodes).toContain('knowledge_not_approved');
    expect(planKnowledgeExport(dataDir, item({ targetLayer: 'reviewed-only' })).reasonCodes).toContain('reviewed_only_not_exportable');
    expect(planKnowledgeExport(dataDir, item({ privacyClass: 'sensitive' })).reasonCodes).toContain('privacy_not_exportable');
    expect(planKnowledgeExport(dataDir, item({ privacyClass: 'secret-reference-only' })).reasonCodes).toContain('privacy_not_exportable');
    expect(planKnowledgeExport(dataDir, item({ freshness: 'stale' })).reasonCodes).toContain('knowledge_not_fresh');
    expect(planKnowledgeExport(dataDir, item({ sourceRefs: [] })).reasonCodes).toContain('evidence_required');
  });

  it('normalizes unsafe claim keys into allowlisted relative paths', () => {
    const dataDir = tmpDataDir();
    const plan = planKnowledgeExport(dataDir, item({ claimKey: '../../secrets\\token/../deploy guard' }));
    expect(plan.destination.relativePath).toMatch(/^l2-staging\/secrets-token-deploy-guard-[a-f0-9]{10}\.md$/);
    expect(plan.destination.relativePath).not.toContain('..');
    expect(plan.destination.formalPath).toBe(`knowledge/${plan.destination.relativePath}`);
    expect(plan.allowed).toBe(true);
  });

  it('creates review jobs idempotently without writing staged files before approval', () => {
    const dataDir = tmpDataDir();
    const first = createKnowledgeExportJob({ dataDir, knowledge: item(), actorId: 'reviewer', idempotencyKey: 'create-1',
      now: '2026-08-27T01:00:00.000Z' });
    const second = createKnowledgeExportJob({ dataDir, knowledge: item(), actorId: 'reviewer', idempotencyKey: 'create-1',
      now: '2026-08-27T01:05:00.000Z' });
    expect(second.jobId).toBe(first.jobId);
    expect(first.state).toBe('review_pending');
    expect(() => statSync(join(dataDir, 'km-export-staging', 'staged', first.plan.file.relativePath))).toThrow();
    expect(listKnowledgeExportJobs(dataDir).map(job => job.jobId)).toEqual([first.jobId]);
  });

  it('keeps export plans and manifests deterministic for identical knowledge', () => {
    const dataDir = tmpDataDir();
    const firstPlan = planKnowledgeExport(dataDir, item());
    const secondPlan = planKnowledgeExport(dataDir, item());
    expect(secondPlan.destination.relativePath).toBe(firstPlan.destination.relativePath);
    expect(secondPlan.file.contentHash).toBe(firstPlan.file.contentHash);

    const firstJob = createKnowledgeExportJob({ dataDir, knowledge: item(), actorId: 'reviewer', idempotencyKey: 'create-1',
      now: '2026-08-27T01:00:00.000Z' });
    const secondJob = createKnowledgeExportJob({ dataDir, knowledge: item(), actorId: 'reviewer', idempotencyKey: 'create-2',
      now: '2026-08-27T01:05:00.000Z' });
    expect(secondJob.jobId).toBe(firstJob.jobId);

    const reviewed = reviewKnowledgeExportJob({ dataDir, jobId: firstJob.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved', now: '2026-08-27T01:10:00.000Z' });
    expect(reviewed.manifest?.contentHash).toBe(firstPlan.file.contentHash);
    expect(reviewed.manifest?.provenance).toEqual(firstPlan.provenance);
  });

  it('stages approved jobs only under the dataDir outbox with manifest provenance', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item(), actorId: 'reviewer', idempotencyKey: 'create-1',
      now: '2026-08-27T01:00:00.000Z' });
    const reviewed = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved', now: '2026-08-27T01:10:00.000Z' });
    const replay = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved', now: '2026-08-27T01:20:00.000Z' });

    expect(reviewed.state).toBe('staged');
    expect(replay.updatedAt).toBe(reviewed.updatedAt);
    expect(reviewed.manifest).toMatchObject({
      schemaVersion: 1,
      jobId: created.jobId,
      state: 'staged',
      contentHash: created.plan.file.contentHash,
      stagedFile: created.plan.file.relativePath,
    });
    expect(reviewed.manifest?.destination.writeMode).toBe('staging-only');
    expect(reviewed.manifest?.approvals.map(item => item.action)).toEqual(['create', 'approve']);

    const staged = readFileSync(join(dataDir, 'km-export-staging', 'staged', created.plan.file.relativePath), 'utf8');
    expect(staged).toContain('Require approval before environment writes.');
    const formalPath = join(dataDir, 'knowledge', created.plan.file.relativePath);
    expect(() => statSync(formalPath)).toThrow();
    const unchanged = planKnowledgeExport(dataDir, item());
    expect(unchanged.diff.status).toBe('unchanged');
    expect(unchanged.diff.lines).toEqual([]);
  });

  it('blocks conflicting staged destinations with different content hashes', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ knowledgeId: 'kn_same' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });

    const conflicting = planKnowledgeExport(dataDir, item({ knowledgeId: 'kn_same', claimText: 'A different approved claim.' }));
    expect(conflicting.allowed).toBe(false);
    expect(conflicting.reasonCodes).toContain('target_conflict');
    expect(conflicting.conflicts[0]).toMatchObject({ existingJobId: created.jobId, targetPath: created.plan.file.relativePath });
  });

  it('keeps rejected jobs from staging content', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ knowledgeId: 'kn_rejected' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const rejected = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'rejected', actorId: 'reviewer',
      idempotencyKey: 'reject-1', reasonCode: 'not_needed' });
    expect(rejected.state).toBe('rejected');
    expect(getKnowledgeExportJob(dataDir, created.jobId)?.manifest).toBeUndefined();
    expect(() => statSync(join(dataDir, 'km-export-staging', 'staged', created.plan.file.relativePath))).toThrow();
  });

  it('previews and executes approved L2 exports in fixture workspaces only with exact preconditions', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ targetLayer: 'L2' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });
    const preview = previewKmFormalExport({ dataDir, jobId: staged.jobId });
    expect(preview.allowed).toBe(true);
    expect(preview.adapter.kind).toBe('plain-markdown');
    expect(preview.risk).toEqual({ mutatesWorkspace: true, network: false, gitPush: false, fixtureOnly: true });
    expect(preview.precondition.currentTargetHash).toBeNull();
    expect(preview.patch.status).toBe('new');

    const applied = executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'execute-1',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: preview.precondition.currentTargetHash,
      destinationVersion: preview.precondition.destinationVersion,
      now: '2026-08-27T02:00:00.000Z',
    });
    expect(applied.state).toBe('applied');
    expect(applied.execution).toMatchObject({ state: 'applied', beforeHash: null, afterHash: staged.plan.file.contentHash });
    expect(readFileSync(preview.destination.absolutePath, 'utf8')).toContain('Require approval before environment writes.');

    const replay = executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'execute-1',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: preview.precondition.currentTargetHash,
      destinationVersion: preview.precondition.destinationVersion,
      now: '2026-08-27T02:05:00.000Z',
    });
    expect(replay.updatedAt).toBe(applied.updatedAt);
  });

  it('exposes every formal destination adapter and blocks command-plan adapters from direct writes', () => {
    const dataDir = tmpDataDir();
    const expected = {
      L1: { kind: 'command-plan', adapterId: 'l1-wiki-command-plan-v1' },
      L2: { kind: 'plain-markdown', adapterId: 'l2-staging-plain-markdown-v1' },
      L3: { kind: 'command-plan', adapterId: 'l3-skill-command-plan-v1' },
      L4: { kind: 'command-plan', adapterId: 'l4-reference-command-plan-v1' },
    } as const;
    for (const [layer, adapter] of Object.entries(expected)) {
      const created = createKnowledgeExportJob({ dataDir, knowledge: item({ knowledgeId: `kn_adapter_${layer}`, targetLayer: layer as KnowledgeItem['targetLayer'] }), actorId: 'reviewer',
        idempotencyKey: `create-${layer}` });
      const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
        idempotencyKey: `review-${layer}`, reasonCode: 'manual_review_approved' });
      const preview = previewKmFormalExport({ dataDir, jobId: staged.jobId });
      expect(preview.adapter).toMatchObject(adapter);
      expect(preview.adapter.commandPlan.length).toBeGreaterThan(0);
      if (layer === 'L2') expect(preview.allowed).toBe(true);
      else expect(preview.reasonCodes).toContain('command_plan_adapter_no_direct_write');
    }
  });

  it('keeps production roots blocked until enabled and exactly allowlisted', () => {
    const dataDir = tmpDataDir();
    const workspaceRoot = mkdtempSync(join(tmpdir(), 'botmux-prod-root-'));
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ targetLayer: 'L2' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });

    const blocked = previewKmFormalExport({ dataDir, jobId: staged.jobId, workspaceRoot, env: { BOTMUX_KM_FORMAL_EXPORT_ENABLED: 'false',
      BOTMUX_KM_FORMAL_EXPORT_ALLOWED_ROOTS: workspaceRoot } });
    expect(blocked.allowed).toBe(false);
    expect(blocked.reasonCodes).toContain('formal_export_not_allowlisted');

    const allowed = previewKmFormalExport({ dataDir, jobId: staged.jobId, workspaceRoot, env: { BOTMUX_KM_FORMAL_EXPORT_ENABLED: 'true',
      BOTMUX_KM_FORMAL_EXPORT_ALLOWED_ROOTS: workspaceRoot } });
    expect(allowed.allowed).toBe(true);
    expect(allowed.risk.fixtureOnly).toBe(false);
  });

  it('detects stale destination preconditions before mutation', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ targetLayer: 'L2' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });
    const preview = previewKmFormalExport({ dataDir, jobId: staged.jobId });
    const target = preview.destination.absolutePath;
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, 'manual edit');

    const conflict = executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'execute-conflict',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: preview.precondition.currentTargetHash,
      destinationVersion: preview.precondition.destinationVersion,
    });
    expect(conflict.state).toBe('conflict');
    expect(readFileSync(target, 'utf8')).toBe('manual edit');

    const refreshed = previewKmFormalExport({ dataDir, jobId: staged.jobId });
    const applied = executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'execute-after-conflict',
      approvalGrade: 'G2',
      confirmationToken: refreshed.confirmationToken,
      expectedTargetHash: refreshed.precondition.currentTargetHash,
      destinationVersion: refreshed.precondition.destinationVersion,
    });
    expect(applied.state).toBe('applied');
  });

  it('detects stale destination versions before mutation', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ targetLayer: 'L2' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });
    const preview = previewKmFormalExport({ dataDir, jobId: staged.jobId });
    writeFileSync(join(preview.destination.root, '.botmux-km-destination.json'), JSON.stringify({ version: 'next' }));

    const conflict = executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'execute-version-conflict',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: preview.precondition.currentTargetHash,
      destinationVersion: preview.precondition.destinationVersion,
    });
    expect(conflict.state).toBe('conflict');
    expect(() => statSync(preview.destination.absolutePath)).toThrow();
  });

  it('rejects tampered traversal target paths before destination writes', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ targetLayer: 'L2' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });
    const jobFile = join(dataDir, 'km-export-staging', 'jobs', `${staged.jobId}.json`);
    const tampered = JSON.parse(readFileSync(jobFile, 'utf8')) as any;
    tampered.plan.destination.relativePath = '../escape.md';
    writeFileSync(jobFile, JSON.stringify(tampered, null, 2));
    expect(() => previewKmFormalExport({ dataDir, jobId: staged.jobId })).toThrow('km_export_path_escape');
    expect(() => statSync(join(dataDir, 'escape.md'))).toThrow();
  });

  it('rejects symlink ancestors in the destination path', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ targetLayer: 'L2' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });
    const workspaceRoot = mkdtempSync(join(tmpdir(), 'botmux-km-export-symlink-'));
    mkdirSync(workspaceRoot, { recursive: true });
    symlinkSync(tmpdir(), join(workspaceRoot, 'l2-staging'));
    expect(() => previewKmFormalExport({ dataDir, jobId: staged.jobId, workspaceRoot })).toThrow('km_export_symlink_rejected');
  });

  it('resumes a prepared execution and rolls back newly created files', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ targetLayer: 'L2' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });
    const preview = previewKmFormalExport({ dataDir, jobId: staged.jobId });
    expect(() => executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'execute-resume',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: preview.precondition.currentTargetHash,
      destinationVersion: preview.precondition.destinationVersion,
      simulateCrashAfterPrepare: true,
    })).toThrow('km_export_simulated_crash_after_prepare');
    expect(getKnowledgeExportJob(dataDir, staged.jobId)?.state).toBe('executing');

    const applied = executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'execute-resume',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: preview.precondition.currentTargetHash,
      destinationVersion: preview.precondition.destinationVersion,
    });
    expect(applied.state).toBe('applied');
    const target = preview.destination.absolutePath;
    expect(readFileSync(target, 'utf8')).toContain('Require approval before environment writes.');

    const rolledBack = rollbackKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'rollback-1',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
    });
    expect(rolledBack.state).toBe('rolled_back');
    expect(() => statSync(target)).toThrow();
  });

  it('restores previous destination content during rollback', () => {
    const dataDir = tmpDataDir();
    const created = createKnowledgeExportJob({ dataDir, knowledge: item({ targetLayer: 'L2' }), actorId: 'reviewer',
      idempotencyKey: 'create-1' });
    const staged = reviewKnowledgeExportJob({ dataDir, jobId: created.jobId, decision: 'approved', actorId: 'reviewer',
      idempotencyKey: 'review-1', reasonCode: 'manual_review_approved' });
    const previewRoot = previewKmFormalExport({ dataDir, jobId: staged.jobId });
    const target = previewRoot.destination.absolutePath;
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, 'previous content');
    const preview = previewKmFormalExport({ dataDir, jobId: staged.jobId });
    const applied = executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'execute-restore',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: preview.precondition.currentTargetHash,
      destinationVersion: preview.precondition.destinationVersion,
    });
    expect(applied.execution?.backupFile).toBeTruthy();
    rollbackKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'reviewer',
      idempotencyKey: 'rollback-restore',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
    });
    expect(readFileSync(target, 'utf8')).toBe('previous content');
  });
});
