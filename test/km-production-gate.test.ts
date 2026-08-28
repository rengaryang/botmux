import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import {
  approveKmProductionGatePlan,
  assertKmProductionGateTokenMatchesOnlyActionKind,
  buildKmProductionGateHandoff,
  buildKmProductionGatePlan,
  createKmProductionGateIntent,
  hashKmProductionGateToken,
  stableKmProductionGateHash,
  type KmProductionGatePlanRequest,
} from '../src/services/km/production-gate.js';

const dirs: string[] = [];
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-production-gate-'));
  dirs.push(dir);
  return dir;
}

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const base = {
  actorId: 'operator-1',
  riskAck: { acknowledged: true, ticket: 'KM-GATE-1' },
  now: '2026-08-28T00:00:00.000Z',
  confirmationToken: 'confirm-token-1',
};

const cases: Array<Pick<KmProductionGatePlanRequest, 'actionKind' | 'target' | 'scope'> & { grade: string }> = [
  {
    actionKind: 'real-memory-transport',
    target: { provider: 'mem0', endpoint: 'https://memory.example.test/v1', credentialRef: 'env:MEM0_API_KEY' },
    scope: { provider: 'mem0', botAppId: 'cli_a' },
    grade: 'G3',
  },
  {
    actionKind: 'real-central-sink',
    target: { provider: 'central', endpoint: 'https://central.example.test/ingest', credentialRef: 'env:BOTMUX_KM_CENTRAL_SINK_SECRET' },
    scope: { sinkId: 'central-prod', tenant: 'internal' },
    grade: 'G3',
  },
  {
    actionKind: 'formal-knowledge-export',
    target: { destinationRoot: '/tmp/km-export-fixture', manifestHash: `sha256:${'a'.repeat(64)}` },
    scope: { targetLayer: 'L3', knowledgeId: 'kn_1' },
    grade: 'G2',
  },
  {
    actionKind: 'prompt-canary',
    target: { botAppId: 'cli_a', window: { start: '2026-08-28T01:00:00.000Z', end: '2026-08-28T02:00:00.000Z' } },
    scope: { botAppId: 'cli_a', sessionClass: 'manual-canary' },
    grade: 'G2',
  },
  {
    actionKind: 'retention-purge',
    target: { cutoff: '2026-08-01T00:00:00.000Z', expectedCounts: { observations: 3, memory: 0 } },
    scope: { database: 'km-local', domain: 'observations' },
    grade: 'G4',
  },
];

describe('KM production gate orchestrator', () => {
  it('builds and persists deterministic plans for all production action kinds', async () => {
    const store = await ObservationStore.open(tempDir());
    for (const item of cases) {
      const built = buildKmProductionGatePlan({ ...base, ...item, ttlSeconds: 600 });
      const saved = store.createProductionGatePlan(built.plan);
      expect(saved).toEqual(expect.objectContaining({
        actionKind: item.actionKind,
        state: 'ready',
        requiredApprovalGrade: item.grade,
        actorId: base.actorId,
        previewHash: built.plan.previewHash,
      }));
      expect(saved.previewHash).toBe(stableKmProductionGateHash(saved.preview));
      const rebuilt = buildKmProductionGatePlan({ ...base, ...item, ttlSeconds: 600 });
      expect(rebuilt.plan.planId).toBe(built.plan.planId);
      expect(rebuilt.plan.previewHash).toBe(built.plan.previewHash);
      expect(saved.preflight).toEqual(expect.arrayContaining([
        expect.objectContaining({ name: 'side_effect_executors_absent', passed: true, effective: false }),
      ]));
      expect(saved.preview).toEqual(expect.objectContaining({
        rollbackHash: stableKmProductionGateHash(saved.rollback),
        impact: expect.objectContaining({
          effective: false,
          sideEffectsExecuted: false,
          networkCalls: 0,
          filesWrittenOutsideTestDirs: 0,
          providerCalls: 0,
          daemonSchedulerChanges: 0,
        }),
      }));
      expect(buildKmProductionGateHandoff(saved)).toEqual(expect.objectContaining({
        planId: saved.planId,
        rollback: expect.objectContaining({ operatorRollbackOnly: true, automaticRollback: false }),
        effective: false,
        sideEffectsExecuted: false,
      }));
      expect(store.listProductionGateAudit(saved.planId)).toEqual([
        expect.objectContaining({ action: 'plan.created', toState: 'ready' }),
      ]);
    }
    expect(store.listProductionGatePlans({ limit: 10 })).toHaveLength(5);
    store.close();
  });

  it('rejects missing exact parameters, wildcard scope and unsupported network targets', () => {
    expect(() => buildKmProductionGatePlan({ ...base, actionKind: 'real-memory-transport', target: { provider: 'mem0', endpoint: 'https://x.test' }, scope: { botAppId: 'cli_a' } }))
      .toThrow('km_production_gate_target_credential_ref_required');
    expect(() => buildKmProductionGatePlan({ ...base, actionKind: 'prompt-canary', target: { botAppId: 'cli_a', window: { start: base.now, end: base.now } }, scope: { botAppId: 'cli_a' } }))
      .toThrow('km_production_gate_canary_window_invalid');
    expect(() => buildKmProductionGatePlan({ ...base, actionKind: 'formal-knowledge-export', target: { destinationRoot: 'relative', manifestHash: `sha256:${'a'.repeat(64)}` }, scope: { targetLayer: 'L3' } }))
      .toThrow('km_production_gate_destination_root_absolute_required');
    expect(() => buildKmProductionGatePlan({ ...base, actionKind: 'retention-purge', target: { cutoff: base.now, expectedCounts: {} }, scope: { database: 'km-local' } }))
      .toThrow('km_production_gate_expected_counts_required');
    expect(() => buildKmProductionGatePlan({ ...base, actionKind: 'real-central-sink', target: { provider: 'central', endpoint: 'http://central.test', credentialRef: 'env:CENTRAL' }, scope: { sinkId: 's1' } }))
      .toThrow('km_production_gate_endpoint_https_required');
    expect(() => buildKmProductionGatePlan({ ...base, actionKind: 'prompt-canary', target: { botAppId: 'cli_a', window: { start: base.now, end: '2026-08-28T01:00:00.000Z' } }, scope: { botAppId: '*' } }))
      .toThrow('km_production_gate_scope_wildcard_rejected');
  });

  it('enforces approval grade, TTL, stale preview and one-time action-scoped token use', async () => {
    const store = await ObservationStore.open(tempDir());
    const built = buildKmProductionGatePlan({ ...base, ...cases[0], ttlSeconds: 120 });
    const saved = store.createProductionGatePlan(built.plan);

    expect(() => approveKmProductionGatePlan(store, {
      planId: saved.planId,
      actorId: 'operator-2',
      approvalGrade: 'G2',
      confirmationToken: built.confirmationToken,
      previewHash: saved.previewHash,
      riskAck: saved.riskAck,
      now: '2026-08-28T00:00:30.000Z',
    })).toThrow('km_production_gate_approval_grade_insufficient');
    expect(() => approveKmProductionGatePlan(store, {
      planId: saved.planId,
      actorId: 'operator-2',
      approvalGrade: 'G3',
      confirmationToken: built.confirmationToken,
      previewHash: `sha256:${'b'.repeat(64)}`,
      riskAck: saved.riskAck,
      now: '2026-08-28T00:00:30.000Z',
    })).toThrow('km_production_gate_preview_stale');
    expect(() => approveKmProductionGatePlan(store, {
      planId: saved.planId,
      actorId: 'operator-2',
      approvalGrade: 'G3',
      confirmationToken: built.confirmationToken,
      previewHash: saved.previewHash,
      riskAck: saved.riskAck,
      now: '2026-08-28T00:03:00.000Z',
    })).toThrow('km_production_gate_plan_expired');

    const approved = approveKmProductionGatePlan(store, {
      planId: saved.planId,
      actorId: 'operator-2',
      approvalGrade: 'G3',
      confirmationToken: built.confirmationToken,
      previewHash: saved.previewHash,
      riskAck: saved.riskAck,
      now: '2026-08-28T00:01:00.000Z',
    });
    expect(approved.state).toBe('approved');

    const intent = createKmProductionGateIntent(store, {
      planId: saved.planId,
      actorId: 'operator-2',
      confirmationToken: built.confirmationToken,
      previewHash: saved.previewHash,
      now: '2026-08-28T00:01:10.000Z',
    });
    expect(intent.intent).toEqual(expect.objectContaining({
      effective: false,
      sideEffectsExecuted: false,
      executorAvailable: false,
      safety: {
        noNetwork: true,
        noFileWritesOutsideTestDirs: true,
        noExport: true,
        noInjection: true,
        noDeletion: true,
        noProviderCalls: true,
        noDaemonScheduler: true,
      },
    }));
    expect(store.getProductionGatePlan(saved.planId)).toEqual(expect.objectContaining({
      state: 'executing',
      confirmationTokenUsedAt: '2026-08-28T00:01:10.000Z',
    }));
    expect(() => createKmProductionGateIntent(store, {
      planId: saved.planId,
      actorId: 'operator-2',
      confirmationToken: built.confirmationToken,
      previewHash: saved.previewHash,
      now: '2026-08-28T00:01:20.000Z',
    })).toThrow('km_production_gate_intent_requires_approved:executing');

    expect(hashKmProductionGateToken('real-memory-transport', built.confirmationToken))
      .not.toBe(hashKmProductionGateToken('real-central-sink', built.confirmationToken));
    expect(() => assertKmProductionGateTokenMatchesOnlyActionKind('real-central-sink', built.confirmationToken, saved.confirmationTokenHash))
      .toThrow('km_production_gate_confirmation_token_invalid');
    expect(store.listProductionGateAudit(saved.planId).map(item => item.action)).toEqual([
      'plan.created',
      'approved',
      'intent.created',
    ]);
    store.close();
  });

  it('global kill switch blocks new intents without mutating existing runtime gate records', async () => {
    const store = await ObservationStore.open(tempDir());
    const built = buildKmProductionGatePlan({ ...base, ...cases[3], ttlSeconds: 120 });
    const saved = store.createProductionGatePlan(built.plan);
    approveKmProductionGatePlan(store, {
      planId: saved.planId,
      actorId: 'operator-2',
      approvalGrade: 'G2',
      confirmationToken: built.confirmationToken,
      previewHash: saved.previewHash,
      riskAck: saved.riskAck,
      now: '2026-08-28T00:00:30.000Z',
    });
    const before = store.getProductionGatePlan(saved.planId);
    expect(store.setProductionGateKillState({ enabled: true, reason: 'incident-freeze', actorId: 'operator-3', now: '2026-08-28T00:00:40.000Z' }))
      .toEqual({ enabled: true, reason: 'incident-freeze', actorId: 'operator-3', updatedAt: '2026-08-28T00:00:40.000Z' });
    expect(() => createKmProductionGateIntent(store, {
      planId: saved.planId,
      actorId: 'operator-2',
      confirmationToken: built.confirmationToken,
      previewHash: saved.previewHash,
      now: '2026-08-28T00:00:50.000Z',
    })).toThrow('km_production_gate_kill_switch_enabled');
    expect(store.getProductionGatePlan(saved.planId)).toEqual(before);
    store.close();
  });
});
