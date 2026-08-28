import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { buildKmProductionGatePlan } from '../src/services/km/production-gate.js';
import { activateKmCanaryRelease, resolveKmCanaryRuntimeAuthorization, rollbackKmCanaryRelease } from '../src/services/km/canary-release.js';

const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

async function fixture() {
  const dir = mkdtempSync(join(tmpdir(), 'botmux-km-canary-release-')); dirs.push(dir);
  const store = await ObservationStore.open(dir);
  const now = '2026-08-28T00:00:00.000Z';
  const riskAck = { acknowledged: true, rollback: 'shadow' };
  const built = buildKmProductionGatePlan({
    actionKind: 'prompt-canary', actorId: 'operator-1', confirmationToken: 'exact-token', ttlSeconds: 604800, now,
    target: { botAppId: 'cli_exact', window: { start: now, end: '2026-09-04T00:00:00.000Z' } },
    scope: { botAppId: 'cli_exact', sessionClass: 'wizard' }, riskAck,
  });
  store.createProductionGatePlan(built.plan);
  return { store, built, riskAck, now };
}

describe('KM Canary release executor', () => {
  it('activates an exact bot without env or restart and auto-falls back outside the window', async () => {
    const { store, built, riskAck, now } = await fixture();
    const active = activateKmCanaryRelease(store, {
      planId: built.plan.planId, actorId: 'operator-2', approvalGrade: 'G2', confirmationToken: 'exact-token',
      previewHash: built.plan.previewHash, riskAck, now: '2026-08-28T00:00:01.000Z',
    });
    expect(active.state).toBe('executing');
    expect(active.intent).toMatchObject({ kind: 'prompt-canary-runtime-v1', effective: true, exactBotAppId: 'cli_exact', restartRequired: false });
    expect(resolveKmCanaryRuntimeAuthorization(store, 'cli_exact', '2026-08-29T00:00:00.000Z')).toMatchObject({ active: true, planId: built.plan.planId });
    expect(resolveKmCanaryRuntimeAuthorization(store, 'cli_other', '2026-08-29T00:00:00.000Z')).toMatchObject({ active: false, reason: 'not_found' });
    expect(resolveKmCanaryRuntimeAuthorization(store, 'cli_exact', '2026-09-05T00:00:00.000Z')).toMatchObject({ active: false, reason: 'expired' });
    store.close();
  });

  it('rolls an active canary back to shadow and rejects stale or wrong tokens', async () => {
    const { store, built, riskAck } = await fixture();
    expect(() => activateKmCanaryRelease(store, {
      planId: built.plan.planId, actorId: 'operator-2', approvalGrade: 'G2', confirmationToken: 'wrong',
      previewHash: built.plan.previewHash, riskAck, now: '2026-08-28T00:00:01.000Z',
    })).toThrow('km_production_gate_confirmation_token_invalid');
    activateKmCanaryRelease(store, {
      planId: built.plan.planId, actorId: 'operator-2', approvalGrade: 'G2', confirmationToken: 'exact-token',
      previewHash: built.plan.previewHash, riskAck, now: '2026-08-28T00:00:01.000Z',
    });
    const rolled = rollbackKmCanaryRelease(store, { planId: built.plan.planId, actorId: 'operator-2', reason: 'latency regression' });
    expect(rolled.state).toBe('rolled_back');
    expect(resolveKmCanaryRuntimeAuthorization(store, 'cli_exact', '2026-08-29T00:00:00.000Z')).toMatchObject({ active: false, reason: 'not_found' });
    store.close();
  });
});
