import { describe, expect, it } from 'vitest';
import type { SessionSkillManifest } from '../src/core/skills/types.js';
import type { TurnCompletionEventPayload } from '../src/services/skill-feedback-store.js';
import {
  observationFromSessionSkillManifest,
  observationFromTurnCompletion,
  observationFromWorkflowArtifact,
} from '../src/services/km/observation-producers.js';

const turn: TurnCompletionEventPayload = {
  type: 'turn.completed',
  version: 1,
  eventId: 'turn-event-1',
  time: '2026-08-26T01:00:00.000Z',
  status: 'completed',
  deliveryId: 'delivery-1',
  contentHash: 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  platform: 'lark',
  platformMessageId: 'om_1',
  platformAppId: 'cli_test',
  botAppId: 'cli_test',
  sessionId: 'session-1',
  turnId: 'turn-1',
  dispatchAttempt: 2,
  cliId: 'traex',
  model: 'GPT-5.5',
};

const manifest: SessionSkillManifest = {
  sessionId: 'session-1',
  cliId: 'traex',
  workingDir: '/private/workspace',
  policyMode: 'priority',
  delivery: 'prompt',
  prioritySkills: [{
    id: 'skill-1',
    name: 'deploy',
    version: '1.2.0',
    tags: ['ops'],
    rootDir: '/private/skills/deploy',
    entrypoint: '/private/skills/deploy/SKILL.md',
    source: { type: 'git', url: 'https://example.invalid/private.git', path: 'skills/deploy', commit: 'abc' },
    checksum: 'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    priorityReason: 'bot-policy',
  }],
  diagnostics: [],
  generatedAt: '2026-08-26T00:59:59.000Z',
};

describe('KM observation producers', () => {
  it('normalizes a durable turn completion without copying answer content', () => {
    const event = observationFromTurnCompletion(turn);
    expect(event).toMatchObject({
      eventType: 'turn.completed',
      eventId: 'km_turn-event-1',
      source: { producer: 'turn-completion-events', confidence: 'observed' },
      identity: { botAppId: 'cli_test', sessionId: 'session-1', turnId: 'turn-1', dispatchAttempt: 2 },
      content: { hash: turn.contentHash, storageMode: 'external_ref' },
      payload: { status: 'completed', cliId: 'traex', model: 'GPT-5.5' },
    });
    expect(JSON.stringify(event)).not.toContain('answer');
  });

  it('normalizes selected and loaded skills without leaking local paths or git URLs', () => {
    const event = observationFromSessionSkillManifest({
      botAppId: 'cli_test',
      manifest,
    });
    expect(event).toMatchObject({
      eventType: 'skill.manifest.resolved',
      identity: { botAppId: 'cli_test', sessionId: 'session-1' },
      payload: {
        cliId: 'traex',
        delivery: 'prompt',
        skills: [{
          name: 'deploy',
          version: '1.2.0',
          checksum: manifest.prioritySkills[0].checksum,
          sourceType: 'git',
          priorityReason: 'bot-policy',
        }],
      },
    });
    const serialized = JSON.stringify(event);
    expect(serialized).not.toContain('/private/');
    expect(serialized).not.toContain('private.git');
  });

  it('emits one observed artifact event from a validated workflow manifest entry', () => {
    const event = observationFromWorkflowArtifact({
      botAppId: 'cli_test',
      sessionId: 'session-1',
      runId: 'run-1',
      nodeId: 'audit',
      attemptId: 'audit#001/attempts/001',
      outputKey: 'report',
      artifact: {
        path: 'report.md',
        kind: 'markdown',
        bytes: 123,
        sha256: 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
      },
      producedAt: '2026-08-26T01:01:00.000Z',
    });
    expect(event).toMatchObject({
      eventType: 'workflow.artifact.produced',
      identity: { workflowId: 'run-1', nodeId: 'audit', attemptId: 'audit#001/attempts/001' },
      provenance: { evidenceLevel: 'workflow-artifact' },
      content: {
        hash: 'sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
        storageMode: 'external_ref',
        ref: 'workflow://run-1/audit/audit%23001%2Fattempts%2F001/report',
      },
      payload: { outputKey: 'report', path: 'report.md', kind: 'markdown', bytes: 123 },
    });
  });
});
