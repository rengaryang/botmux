import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';

const dirs: string[] = [];
const sourceRefs = [{ kind: 'api', ref: 'observation/evt-1' }];
function tempDir(): string { const dir = mkdtempSync(join(tmpdir(), 'botmux-km-p3-')); dirs.push(dir); return dir; }
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

describe('KM Phase 3 trace, eval and evolution', () => {
  it('migrates additively through the latest schema and keeps trace edges idempotent', async () => {
    const store = await ObservationStore.open(tempDir());
    expect(store.schemaVersion()).toBe(9);
    const input = { fromType: 'turn', fromId: 'turn-1', toType: 'skill', toId: 'skill-1', edgeType: 'used' as const };
    expect(store.addTraceEdge(input).created).toBe(true);
    expect(store.addTraceEdge(input).created).toBe(false);
    expect(store.listTrace({ type: 'turn', id: 'turn-1', limit: 10 })).toEqual([
      expect.objectContaining({ fromType: 'turn', fromId: 'turn-1', toType: 'skill', toId: 'skill-1', edgeType: 'used' }),
    ]);
    store.close();
  });

  it('records versioned eval results with evidence and deduplicates reruns', async () => {
    const store = await ObservationStore.open(tempDir());
    const input = {
      evaluatorName: 'artifact-completeness', evaluatorVersion: '1', targetType: 'workflow-artifact' as const, targetId: 'artifact-1',
      results: [{ metricKey: 'sha256.present', score: 1, verdict: 'pass' as const, confidence: 'observed' as const, sourceRefs }],
    };
    expect(store.recordEval(input).created).toBe(true);
    expect(store.recordEval(input).created).toBe(false);
    expect(store.listEvalRuns(10)[0]).toEqual(expect.objectContaining({ evaluatorName: 'artifact-completeness', resultCount: 1, passCount: 1, failCount: 0 }));
    expect(() => store.recordEval({ ...input, targetId: 'artifact-2', results: [{ ...input.results[0], sourceRefs: [] }] })).toThrow(/source_refs_required/);
    store.close();
  });

  it('requires matching approval grade before approving a proposal', async () => {
    const store = await ObservationStore.open(tempDir());
    const proposalId = store.createEvolutionProposal({
      proposalType: 'skill-edit', targetRef: 'skill:test', approvalGrade: 'G2', summary: 'Improve route matching',
      evidenceRefs: sourceRefs, proposedAction: { patch: 'ref' }, risk: { level: 'medium' }, rollback: { action: 'revert' }, createdBy: 'eval-worker',
    });
    expect(() => store.decideProposal({ proposalId, decision: 'approved', actorId: 'reviewer', grade: 'G1', scope: {}, riskAck: {} }))
      .toThrow(/grade_insufficient/);
    expect(store.decideProposal({ proposalId, decision: 'approved', actorId: 'reviewer', grade: 'G2', scope: { target: 'skill:test' }, riskAck: { accepted: true } }))
      .toEqual(expect.objectContaining({ state: 'approved' }));
    expect(store.listEvolution(10)[0]).toEqual(expect.objectContaining({ proposalId, state: 'approved', approvedBy: 'reviewer' }));
    store.close();
  });

  it('does not execute proposals; it only persists review decisions', async () => {
    const store = await ObservationStore.open(tempDir());
    const proposalId = store.createEvolutionProposal({
      proposalType: 'external-action', targetRef: 'cluster:prod', approvalGrade: 'G4', summary: 'External action draft',
      evidenceRefs: sourceRefs, proposedAction: { command: 'blocked' }, risk: { critical: true }, rollback: { action: 'manual' }, createdBy: 'eval-worker',
    });
    expect(store.listEvolution(10)[0]).toEqual(expect.objectContaining({ proposalId, state: 'review_pending', approvalGrade: 'G4' }));
    store.close();
  });
});
