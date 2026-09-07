import { describe, expect, it } from 'vitest';
import {
  observationFromSkillCommand,
} from '../src/services/km/observation-producers.js';

describe('KM skill command observation producer', () => {
  it('emits an observed skill.invoked event for a successful show', () => {
    const event = observationFromSkillCommand({
      botAppId: 'cli_test',
      sessionId: 'session-1',
      turnId: 'turn-1',
      subcommand: 'show',
      skillName: 'deploy',
      exitCode: 0,
      bytes: 1234,
      at: '2026-08-26T08:00:00.000Z',
      invocationId: 'inv-1',
    });
    expect(event).toMatchObject({
      eventType: 'skill.invoked',
      source: { producer: 'skill-cli', adapter: 'unknown', confidence: 'observed' },
      identity: {
        botAppId: 'cli_test',
        sessionId: 'session-1',
        turnId: 'turn-1',
        skillName: 'deploy',
      },
      payload: { subcommand: 'show', exitCode: 0, bytes: 1234 },
    });
  });

  it('emits a skill.failed event when the skill lookup fails', () => {
    const event = observationFromSkillCommand({
      botAppId: 'cli_test',
      sessionId: 'session-1',
      turnId: 'turn-1',
      subcommand: 'show',
      skillName: 'missing-skill',
      exitCode: 2,
      at: '2026-08-26T08:00:00.000Z',
      invocationId: 'inv-2',
    });
    expect(event).toMatchObject({
      eventType: 'skill.failed',
      payload: { subcommand: 'show', exitCode: 2 },
    });
    // No error message detail is copied — the error text may contain local paths.
    expect(JSON.stringify(event)).not.toContain('manifest not found');
    expect(JSON.stringify(event)).not.toContain('usage:');
  });

  it('includes the invocation id in the idempotency key so distinct reads are kept', () => {
    const base = {
      botAppId: 'cli_test', sessionId: 'session-1', turnId: 'turn-1',
      subcommand: 'show' as const, skillName: 'deploy', at: '2026-08-26T08:00:00.000Z',
    };
    const first = observationFromSkillCommand({ ...base, exitCode: 0, bytes: 10, invocationId: 'inv-a' });
    const second = observationFromSkillCommand({ ...base, exitCode: 0, bytes: 10, invocationId: 'inv-b' });
    expect(first.ordering.idempotencyKey).not.toBe(second.ordering.idempotencyKey);
  });
});
