import { createHash } from 'node:crypto';
import type { ObservationEvent } from './observation-schema.js';

const SECRET_PATTERNS = [
  /\b(?:AK|SK|API[_-]?KEY|TOKEN|PASSWORD|SECRET)\s*[:=]\s*[^\s,;]+/i,
  /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,
  /\b(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}/i,
];

export interface RedactedSyncEnvelope {
  eventId: string;
  eventType: string;
  idempotencyKey: string;
  payloadHash: string;
  localMetadata: {
    botAppId: string; sessionId: string; turnId?: string | null;
    skillName?: string | null; workflowId?: string | null;
    observedAt: string; confidence: string; privacyClass: string;
  };
  payload: Record<string, unknown>;
}

function containsSecret(value: unknown): boolean {
  const text = typeof value === 'string' ? value : JSON.stringify(value);
  return SECRET_PATTERNS.some(pattern => pattern.test(text));
}

function metadataPayload(event: ObservationEvent): Record<string, unknown> {
  const safeKeys = ['status', 'exitCode', 'revision', 'semantic', 'outputKey', 'kind', 'bytes', 'resolverStatus'];
  return Object.fromEntries(safeKeys.filter(key => key in event.payload).map(key => [key, event.payload[key]]));
}

/** Fail-closed redaction: secret/sensitive material never enters a sync payload. */
export function redactObservationForSync(event: ObservationEvent): { ok: true; envelope: RedactedSyncEnvelope } | { ok: false; reason: string } {
  if (event.provenance.privacyClass === 'secret-reference-only') return { ok: false, reason: 'secret_reference_only' };
  if (event.provenance.redactionStatus === 'blocked' || event.provenance.redactionStatus === 'pending_review') {
    return { ok: false, reason: `redaction_${event.provenance.redactionStatus}` };
  }
  if (containsSecret(event.payload) || containsSecret(event.content.inlinePreview ?? '')) return { ok: false, reason: 'secret_detected' };
  const payload = event.provenance.privacyClass === 'sensitive' ? {} : metadataPayload(event);
  const stable = JSON.stringify({ eventId: event.eventId, eventType: event.eventType, payload });
  return { ok: true, envelope: {
    eventId: event.eventId, eventType: event.eventType, idempotencyKey: event.ordering.idempotencyKey,
    payloadHash: `sha256:${createHash('sha256').update(stable).digest('hex')}`,
    localMetadata: {
      botAppId: event.identity.botAppId, sessionId: event.identity.sessionId,
      turnId: event.identity.turnId, skillName: event.identity.skillName, workflowId: event.identity.workflowId,
      observedAt: event.ordering.observedAt, confidence: event.source.confidence,
      privacyClass: event.provenance.privacyClass,
    }, payload,
  } };
}
