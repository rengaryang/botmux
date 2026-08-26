import { createHash } from 'node:crypto';
import type { SessionSkillManifest, SkillPackage, SkillSource } from '../../core/skills/types.js';
import type { TurnCompletionEventPayload } from '../skill-feedback-store.js';
import {
  ObservationEventSchema,
  type ObservationEvent,
} from './observation-schema.js';

function stableId(prefix: string, ...parts: Array<string | number | undefined>): string {
  return `${prefix}_${createHash('sha256').update(JSON.stringify(parts)).digest('hex')}`;
}

function sourceType(source: SkillSource): SkillSource['type'] {
  return source.type;
}

function safeSkill(skill: SkillPackage & { priorityReason: string }): Record<string, unknown> {
  return {
    id: skill.id,
    name: skill.name,
    ...(skill.version ? { version: skill.version } : {}),
    ...(skill.checksum ? { checksum: skill.checksum } : {}),
    sourceType: sourceType(skill.source),
    tags: skill.tags,
    priorityReason: skill.priorityReason,
  };
}

export function observationFromTurnCompletion(input: TurnCompletionEventPayload): ObservationEvent {
  return ObservationEventSchema.parse({
    schemaVersion: 1,
    eventId: `km_${input.eventId}`,
    eventType: 'turn.completed',
    source: {
      producer: 'turn-completion-events',
      adapter: input.cliId ?? input.platform,
      nativeSessionId: input.nativeSessionId ?? null,
      resolverStatus: 'resolved',
      confidence: 'observed',
    },
    identity: {
      botAppId: input.botAppId,
      sessionId: input.sessionId,
      turnId: input.turnId,
      nativeSessionId: input.nativeSessionId ?? null,
      dispatchAttempt: input.dispatchAttempt ?? null,
      workflowId: input.workflowId ?? null,
      taskId: input.taskId ?? null,
      parentTaskId: input.parentTaskId ?? null,
      skillName: input.skillName ?? null,
      skillVersion: input.skillVersion ?? null,
      chatId: input.chatId ?? null,
      topicRootId: input.topicRootId ?? null,
    },
    ordering: {
      sourceKey: `turn-completion:${input.botAppId}`,
      idempotencyKey: `${input.sessionId}|${input.turnId}|${input.dispatchAttempt ?? 0}`,
      parentEventIds: [],
      observedAt: input.time,
    },
    provenance: {
      evidenceLevel: 'runtime',
      parserVersion: 'turn-completion-events/v1',
      sourceRefs: [{ kind: 'sqlite-row', ref: `turn_completion_events/${input.eventId}` }],
      privacyClass: 'internal',
      redactionStatus: 'not_needed',
    },
    content: {
      hash: input.contentHash,
      storageMode: input.contentRef ? 'external_ref' : 'external_ref',
      ref: input.contentRef ?? `delivery://${encodeURIComponent(input.deliveryId)}`,
    },
    payload: {
      status: input.status,
      platform: input.platform,
      platformMessageId: input.platformMessageId,
      deliveryId: input.deliveryId,
      ...(input.durationMs !== undefined ? { durationMs: input.durationMs } : {}),
      ...(input.usage ? { usage: input.usage } : {}),
      ...(input.cliId ? { cliId: input.cliId } : {}),
      ...(input.cliVersion ? { cliVersion: input.cliVersion } : {}),
      ...(input.model ? { model: input.model } : {}),
      ...(input.reasoningEffort ? { reasoningEffort: input.reasoningEffort } : {}),
    },
    createdAt: input.time,
  });
}

export function observationFromSessionSkillManifest(input: {
  botAppId: string;
  manifest: SessionSkillManifest;
}): ObservationEvent {
  const { manifest } = input;
  const skills = manifest.prioritySkills.map(safeSkill);
  const manifestHash = `sha256:${createHash('sha256').update(JSON.stringify({
    sessionId: manifest.sessionId,
    cliId: manifest.cliId,
    policyMode: manifest.policyMode,
    delivery: manifest.delivery ?? 'auto',
    skills,
    diagnostics: manifest.diagnostics,
    generatedAt: manifest.generatedAt,
  })).digest('hex')}`;

  return ObservationEventSchema.parse({
    schemaVersion: 1,
    eventId: stableId('km_skill_manifest', input.botAppId, manifest.sessionId, manifestHash),
    eventType: 'skill.manifest.resolved',
    source: {
      producer: 'session-skill-manifest',
      adapter: manifest.cliId,
      resolverStatus: 'resolved',
      confidence: 'observed',
    },
    identity: {
      botAppId: input.botAppId,
      sessionId: manifest.sessionId,
    },
    ordering: {
      sourceKey: `skill-manifest:${input.botAppId}`,
      idempotencyKey: `${manifest.sessionId}|${manifestHash}`,
      parentEventIds: [],
      observedAt: manifest.generatedAt,
    },
    provenance: {
      evidenceLevel: 'runtime',
      parserVersion: 'session-skill-manifest/v1',
      sourceRefs: [{ kind: 'file', ref: `skill-manifests/${encodeURIComponent(manifest.sessionId)}.json`, sha256: manifestHash }],
      privacyClass: 'internal',
      redactionStatus: 'redacted',
    },
    content: {
      hash: manifestHash,
      storageMode: 'external_ref',
      ref: `skill-manifest://${encodeURIComponent(manifest.sessionId)}`,
    },
    payload: {
      cliId: manifest.cliId,
      policyMode: manifest.policyMode,
      delivery: manifest.delivery ?? 'auto',
      skills,
      diagnostics: manifest.diagnostics,
    },
    createdAt: manifest.generatedAt,
  });
}

/** An actually-executed `botmux skill show|read` from inside a live CLI
 *  session — the strongest available evidence that the model pulled skill
 *  content (stronger than manifest-resolved, which only proves delivery).
 *  Runs in the CLI subprocess, so identity comes from BOTMUX_* env vars. */
export interface SkillCommandObservationInput {
  botAppId: string;
  sessionId: string;
  turnId?: string | null;
  subcommand: 'show' | 'read';
  skillName: string;
  exitCode: number;
  bytes?: number;
  at: string;
  /** Unique per execution (uuid) so two reads of the same skill stay two events. */
  invocationId: string;
}

export function observationFromSkillCommand(input: SkillCommandObservationInput): ObservationEvent {
  const failed = input.exitCode !== 0;
  return ObservationEventSchema.parse({
    schemaVersion: 1,
    eventId: stableId('km_skill_cmd', input.botAppId, input.sessionId, input.skillName, input.at, input.invocationId),
    eventType: failed ? 'skill.failed' : 'skill.invoked',
    source: {
      producer: 'skill-cli',
      adapter: 'unknown',
      nativeSessionId: null,
      resolverStatus: 'resolved',
      confidence: 'observed',
    },
    identity: {
      botAppId: input.botAppId,
      sessionId: input.sessionId,
      turnId: input.turnId ?? null,
      skillName: input.skillName,
    },
    ordering: {
      sourceKey: `skill-command:${input.botAppId}`,
      idempotencyKey: `${input.sessionId}|${input.skillName}|${input.at}|${input.invocationId}`,
      parentEventIds: [],
      observedAt: input.at,
    },
    provenance: {
      evidenceLevel: 'runtime',
      parserVersion: 'skill-cli/v1',
      sourceRefs: [{ kind: 'api', ref: `skill-command/${input.invocationId}` }],
      privacyClass: 'internal',
      redactionStatus: 'not_needed',
    },
    content: { hash: null, storageMode: 'none' },
    payload: {
      subcommand: input.subcommand,
      exitCode: input.exitCode,
      ...(input.bytes !== undefined ? { bytes: input.bytes } : {}),
    },
    createdAt: input.at,
  });
}

export interface WorkflowArtifactObservationInput {
  botAppId: string;
  sessionId: string;
  runId: string;
  nodeId: string;
  attemptId: string;
  outputKey: string;
  artifact: {
    path: string;
    kind: string;
    bytes: number;
    sha256: string;
  };
  producedAt: string;
}

export function observationFromWorkflowArtifact(input: WorkflowArtifactObservationInput): ObservationEvent {
  const contentHash = input.artifact.sha256.startsWith('sha256:')
    ? input.artifact.sha256
    : `sha256:${input.artifact.sha256}`;
  const ref = `workflow://${encodeURIComponent(input.runId)}/${encodeURIComponent(input.nodeId)}/${encodeURIComponent(input.attemptId)}/${encodeURIComponent(input.outputKey)}`;
  return ObservationEventSchema.parse({
    schemaVersion: 1,
    eventId: stableId('km_workflow_artifact', input.runId, input.nodeId, input.attemptId, input.outputKey, contentHash),
    eventType: 'workflow.artifact.produced',
    source: {
      producer: 'workflow-v3-manifest',
      adapter: 'workflow',
      resolverStatus: 'resolved',
      confidence: 'observed',
    },
    identity: {
      botAppId: input.botAppId,
      sessionId: input.sessionId,
      workflowId: input.runId,
      nodeId: input.nodeId,
      attemptId: input.attemptId,
    },
    ordering: {
      sourceKey: `workflow-artifact:${input.runId}`,
      idempotencyKey: `${input.nodeId}|${input.attemptId}|${input.outputKey}|${contentHash}`,
      parentEventIds: [],
      observedAt: input.producedAt,
    },
    provenance: {
      evidenceLevel: 'workflow-artifact',
      parserVersion: 'workflow-v3-manifest/v1',
      sourceRefs: [{ kind: 'workflow-artifact', ref, sha256: contentHash }],
      privacyClass: 'internal',
      redactionStatus: 'not_needed',
    },
    content: {
      hash: contentHash,
      storageMode: 'external_ref',
      ref,
    },
    payload: {
      outputKey: input.outputKey,
      path: input.artifact.path,
      kind: input.artifact.kind,
      bytes: input.artifact.bytes,
    },
    createdAt: input.producedAt,
  });
}
