import { z } from 'zod';

export const OBSERVATION_EVENT_SCHEMA_VERSION = 1 as const;

const nonEmpty = z.string().trim().min(1);
const nullableNonEmpty = nonEmpty.nullable().optional();
const isoDateTime = z.string().datetime({ offset: true });
const sha256 = z.string().regex(/^sha256:[a-f0-9]{64}$/);

export const ObservationEventTypeSchema = z.enum([
  'turn.started',
  'turn.completed',
  'feedback.revised',
  'transcript.window.resolved',
  'workflow.artifact.produced',
  'skill.manifest.resolved',
  'skill.invoked',
  'skill.completed',
  'skill.failed',
  'plugin.manifest.resolved',
  'knowledge.candidate.proposed',
  'knowledge.state.changed',
  'memory.item.upserted',
  'memory.state.changed',
  'eval.run.started',
  'eval.result.recorded',
  'evolution.proposal.created',
  'approval.decision.recorded',
  'sync.batch.acknowledged',
  'gc.item.purged',
]);

export const SourceRefSchema = z.object({
  kind: z.enum([
    'sqlite-row',
    'transcript-window',
    'workflow-artifact',
    'file',
    'dashboard-action',
    'feedback',
    'api',
  ]),
  ref: nonEmpty,
  sha256: sha256.nullable().optional(),
  span: z.object({
    start: z.number().int().nonnegative().nullable().optional(),
    end: z.number().int().nonnegative().nullable().optional(),
  }).nullable().optional(),
}).strict();

export const ObservationEventSchema = z.object({
  schemaVersion: z.literal(OBSERVATION_EVENT_SCHEMA_VERSION),
  eventId: nonEmpty,
  eventType: ObservationEventTypeSchema,
  source: z.object({
    producer: nonEmpty,
    adapter: nonEmpty,
    transcriptKind: nullableNonEmpty,
    nativeSessionId: nullableNonEmpty,
    resolverStatus: z.enum(['resolved', 'missing', 'stale', 'partial', 'unsupported', 'not_applicable']),
    confidence: z.enum(['observed', 'inferred']),
    inferenceReason: nullableNonEmpty,
  }).strict(),
  identity: z.object({
    botAppId: nonEmpty,
    botId: nullableNonEmpty,
    sessionId: nonEmpty,
    turnId: nullableNonEmpty,
    nativeSessionId: nullableNonEmpty,
    dispatchAttempt: z.number().int().nonnegative().nullable().optional(),
    workflowId: nullableNonEmpty,
    nodeId: nullableNonEmpty,
    attemptId: nullableNonEmpty,
    taskId: nullableNonEmpty,
    parentTaskId: nullableNonEmpty,
    skillName: nullableNonEmpty,
    skillVersion: nullableNonEmpty,
    pluginId: nullableNonEmpty,
    chatId: nullableNonEmpty,
    topicRootId: nullableNonEmpty,
  }).strict(),
  ordering: z.object({
    sourceKey: nonEmpty,
    idempotencyKey: nonEmpty,
    sourceSeq: z.number().int().nonnegative().nullable().optional(),
    parentEventIds: z.array(nonEmpty).default([]),
    observedAt: isoDateTime,
  }).strict(),
  provenance: z.object({
    evidenceLevel: z.enum([
      'runtime',
      'dist-contract',
      'readme',
      'source',
      'workflow-artifact',
      'user-feedback',
      'inference',
    ]),
    parserVersion: nonEmpty,
    sourceRefs: z.array(SourceRefSchema),
    privacyClass: z.enum(['public-to-team', 'internal', 'sensitive', 'secret-reference-only']),
    redactionStatus: z.enum(['not_needed', 'redacted', 'blocked', 'pending_review']),
  }).strict(),
  content: z.object({
    hash: sha256.nullable(),
    storageMode: z.enum([
      'none',
      'inline_preview_only',
      'local_blob',
      'transcript_ref',
      'external_ref',
      'redacted',
    ]),
    ref: nullableNonEmpty,
    inlinePreview: z.string().max(2_048).nullable().optional(),
    encryption: z.object({
      algorithm: z.enum(['none', 'aes-256-gcm']),
      keyRef: nullableNonEmpty,
      nonceRef: nullableNonEmpty,
      aad: z.string().nullable().optional(),
    }).nullable().optional(),
  }).strict(),
  payload: z.record(z.unknown()),
  createdAt: isoDateTime,
}).strict().superRefine((event, ctx) => {
  if (event.source.confidence === 'inferred' && !event.source.inferenceReason) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['source', 'inferenceReason'],
      message: 'inferenceReason is required when confidence is inferred',
    });
  }
  if (event.source.confidence === 'observed' && event.provenance.sourceRefs.length === 0) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['provenance', 'sourceRefs'],
      message: 'observed events require at least one sourceRef',
    });
  }
  if (event.provenance.privacyClass === 'secret-reference-only') {
    if (event.content.storageMode !== 'none' && event.content.storageMode !== 'redacted') {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['content', 'storageMode'],
        message: 'secret-reference-only content must use none or redacted storage',
      });
    }
    if (event.content.inlinePreview) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['content', 'inlinePreview'],
        message: 'secret-reference-only content cannot include an inline preview',
      });
    }
  }
});

export type ObservationEventType = z.infer<typeof ObservationEventTypeSchema>;
export type SourceRef = z.infer<typeof SourceRefSchema>;
export type ObservationEvent = z.infer<typeof ObservationEventSchema>;
