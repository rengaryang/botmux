import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { execFileSync } from 'node:child_process';

const dirs: string[] = [];

beforeEach(() => {
  vi.stubEnv('BOTMUX_KM_OBSERVATION_ENABLED', 'true');
});

afterEach(async () => {
  const { drainObservationQueue, __testOnly_closeObservationStores, __testOnly_reopenObservationAdmission } =
    await import('../src/services/km/observation-queue.js');
  await drainObservationQueue(3_000);
  await __testOnly_closeObservationStores();
  __testOnly_reopenObservationAdmission();
  vi.unstubAllEnvs();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe('skill command KM invocation telemetry', () => {
  it('records an observed skill.invoked event when a session runs skill show', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'botmux-km-skillcmd-'));
    dirs.push(dataDir);
    const out = execFileSync(
      process.execPath,
      [
        'dist/cli.js', 'skill', 'show', 'botmux-send',
      ],
      {
        env: {
          ...process.env,
          SESSION_DATA_DIR: dataDir,
          BOTMUX_SESSION_ID: 'session-km-test',
          BOTMUX_LARK_APP_ID: 'cli_km_test',
          BOTMUX_TURN_ID: 'turn-km-test',
          BOTMUX_KM_OBSERVATION_ENABLED: 'true',
        },
        // The telemetry enqueue is fire-and-forget in the CLI process; give it a
        // moment before the process would otherwise exit with pending work.
        timeout: 30_000,
      },
    );
    expect(out.length).toBeGreaterThan(0);

    const { ObservationStore } = await import('../src/services/km/observation-store.js');
    const store = await ObservationStore.open(dataDir);
    const invoked = store.list({ limit: 10, eventType: 'skill.invoked' });
    expect(invoked).toHaveLength(1);
    expect(invoked[0]).toMatchObject({
      identity: {
        botAppId: 'cli_km_test',
        sessionId: 'session-km-test',
        turnId: 'turn-km-test',
        skillName: 'botmux-send',
      },
      payload: { subcommand: 'show', exitCode: 0 },
    });
    store.close();
  });
});
