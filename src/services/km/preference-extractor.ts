import type { ObservationEvent } from './observation-schema.js';
import type { ExplicitPreferenceEvidence } from './safe-memory-policy.js';

export interface ExtractedPreference extends ExplicitPreferenceEvidence {
  evidenceText: string;
  evidenceStart: number;
  evidenceEnd: number;
}
export interface AttributedUserEvidence { text: string; start: number; end: number }

function contentText(value: unknown): string {
  if (typeof value === 'string') return value;
  if (!Array.isArray(value)) return '';
  return value.map(item => typeof item === 'string' ? item : item && typeof item === 'object' && typeof (item as any).text === 'string' ? (item as any).text : '').join('\n');
}

/** Extract only evidence mechanically attributed to a user role. */
export function extractAttributedUserEvidence(source: string): AttributedUserEvidence[] {
  const regions: AttributedUserEvidence[] = [];
  const tagged = /<user_message>([\s\S]*?)<\/user_message>/giu;
  for (const match of source.matchAll(tagged)) {
    const text = match[1].trim(); const inner = match[0].indexOf(match[1]);
    if (text && match.index !== undefined) regions.push({ text, start: match.index + inner, end: match.index + inner + match[1].length });
  }
  let offset = 0;
  for (const line of source.split('\n')) {
    try {
      const row = JSON.parse(line); const message = row?.message ?? row?.payload ?? row;
      const role = message?.role ?? (message?.type === 'user_message' ? 'user' : undefined);
      const text = role === 'user' ? contentText(message?.content ?? message?.text ?? message?.message) : '';
      if (text.trim()) regions.push({ text, start: offset, end: offset + line.length });
    } catch { /* non-JSON transcript line */ }
    offset += Buffer.byteLength(`${line}\n`, 'utf8');
  }
  return regions;
}

const RULES: Array<{ key: string; patterns: RegExp[] }> = [
  { key: 'response.language', patterns: [
    /(?:请|以后请|后续请|我希望你|我偏好你|我喜欢你)(?:都|一直)?(?:使用|用)(中文|英文)(?:回复|回答|交流)?/u,
    /(?:please\s+)?(?:always\s+)?(?:reply|respond|answer)\s+in\s+(english|chinese)/iu,
  ] },
  { key: 'response.style', patterns: [
    /(?:请|以后请|后续请)(?:保持)?(简洁|详细)(?:回复|回答)?/u,
    /(?:please\s+)?(?:keep\s+(?:responses?|answers?)\s+)?(concise|detailed)/iu,
  ] },
];

function normalizeValue(value: string): string {
  const normalized = value.toLowerCase();
  if (normalized === '中文' || normalized === 'chinese') return 'Chinese';
  if (normalized === '英文' || normalized === 'english') return 'English';
  if (normalized === '简洁' || normalized === 'concise') return 'concise';
  if (normalized === '详细' || normalized === 'detailed') return 'detailed';
  return value;
}

/** Deterministic and deliberately narrow: only explicit user preference phrases. */
export function extractExplicitPreferences(input: {
  event: ObservationEvent;
  evidenceText: string;
  userId?: string;
  evidenceOffset?: number;
  roleAttributed?: boolean;
}): ExtractedPreference[] {
  const { event, evidenceText } = input;
  if (event.eventType !== 'turn.completed' || event.source.confidence !== 'observed' || !input.userId?.trim() || !input.roleAttributed) return [];
  const sourceRef = { kind: 'transcript-window' as const, ref: `observation/${event.eventId}` };
  const found: ExtractedPreference[] = [];
  for (const rule of RULES) {
    for (const pattern of rule.patterns) {
      const match = pattern.exec(evidenceText);
      if (!match || match.index === undefined) continue;
      const value = normalizeValue(match[1]);
      found.push({ confidence: 'observed', explicitUserStatement: true, scope: 'user', subject: input.userId,
        claimKey: rule.key, claimText: `${rule.key}=${value}`, privacyClass: event.provenance.privacyClass,
        sourceRefs: [{ ...sourceRef, span: { start: (input.evidenceOffset ?? 0) + match.index, end: (input.evidenceOffset ?? 0) + match.index + match[0].length } }],
        evidenceSpanComplete: true, policyTags: ['preference'], evidenceText: match[0],
        evidenceStart: (input.evidenceOffset ?? 0) + match.index, evidenceEnd: (input.evidenceOffset ?? 0) + match.index + match[0].length });
      break;
    }
  }
  return found;
}
