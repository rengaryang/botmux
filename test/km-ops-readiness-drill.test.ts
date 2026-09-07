import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { afterEach, describe, expect, it } from 'vitest';
import { runKmOpsReadinessDrill } from '../src/services/km/ops-readiness-drill.js';

const dirs: string[] = [];
function tempDir(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  dirs.push(dir);
  return dir;
}
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe('KM ops readiness drill', () => {
  it('produces a deterministic offline report for all readiness tracks', async () => {
    const first = await runKmOpsReadinessDrill({ now: '2026-08-28T00:00:00.000Z' });
    const second = await runKmOpsReadinessDrill({ now: '2026-08-28T00:00:00.000Z' });

    expect(second.drillHash).toBe(first.drillHash);
    expect(first.checks.every(check => check.passed)).toBe(true);
    expect(first.safety).toEqual({
      fixtureOnly: true,
      realMemoryProviderCanary: false,
      realTransportEnabled: false,
      formalDestinationWrites: false,
      promptMutation: false,
      deletionExecutorAvailable: false,
    });
    expect(first.centralSink.partialAck).toEqual(expect.objectContaining({
      scanned: 2,
      enqueued: 2,
      delivered: 1,
      quarantined: 1,
      failures: 0,
    }));
    expect(first.centralSink.replay).toEqual(expect.objectContaining({
      replayable: true,
      rows: expect.any(Number),
      outboxHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
    }));
    expect(first.centralSink.replay.rows).toBeGreaterThanOrEqual(2);
    expect(first.centralSink.conflict).toEqual({ quarantineCreated: true, rollbackLocalDisableOnly: true });
    expect(first.knowledgeExport).toEqual(expect.objectContaining({
      targetLayer: 'L2',
      manifestState: 'staged',
      diffStatus: 'new',
      executionPreviewAllowed: true,
      executionFixtureOnly: true,
      appliedState: 'applied',
      rollbackState: 'rolled_back',
      destinationAfterRollback: 'absent',
    }));
    expect(first.retention).toEqual(expect.objectContaining({
      dryRunOnly: true,
      destructiveActionsAvailable: false,
    }));
    expect(first.retention.observations.legalHoldProtected).toBeGreaterThanOrEqual(1);
    expect(first.retention.knowledge.legalHoldProtected).toBeGreaterThanOrEqual(1);
    expect(first.retention.memory.legalHoldProtected).toBeGreaterThanOrEqual(1);
    expect(first.productionGates).toHaveLength(5);
    expect(first.productionGates.every(item => item.effective === false && item.sideEffectsExecuted === false)).toBe(true);
    expect(first.localDefaultProfile.memoryBackends).toEqual({ writePolicy: 'single', primary: 'sqlite', mirrors: [] });
    expect(first.localDefaultProfile.externalProvidersConfigured).toBe(0);
    expect(first.localDefaultProfile.backendWorkerEnabled).toBe(false);
  });

  it('exposes the drill through botmux km ops-readiness without using the live data dir by default', () => {
    const outDir = tempDir('botmux-km-readiness-output-');
    const output = join(outDir, 'report.json');
    const env = { ...process.env, SESSION_DATA_DIR: tempDir('botmux-km-live-unused-') };
    for (const key of Object.keys(env)) {
      if (key.startsWith('BOTMUX_GOAL_') || key.startsWith('BOTMUX_WORKFLOW')) delete env[key];
    }
    delete env.BOTMUX_V3_GOAL;
    execFileSync('pnpm', [
      'tsx',
      'src/cli.ts',
      'km',
      'ops-readiness',
      '--now',
      '2026-08-28T00:00:00.000Z',
      '--output',
      output,
    ], { cwd: process.cwd(), env, stdio: 'pipe' });
    const report = JSON.parse(readFileSync(output, 'utf8')) as Awaited<ReturnType<typeof runKmOpsReadinessDrill>>;
    expect(report.scratch).toEqual({ usedTemporaryDataDir: true, retained: false });
    expect(report.checks.every(check => check.passed)).toBe(true);
    expect(report.localDefaultProfile.memoryBackends).toEqual({ writePolicy: 'single', primary: 'sqlite', mirrors: [] });
  });
});
