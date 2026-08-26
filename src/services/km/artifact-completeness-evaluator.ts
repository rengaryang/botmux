import { createHash } from 'node:crypto';
import type { EvalResultInput } from './observation-store.js';

export interface ArtifactEvidence {
  outputKey?: string;
  relativePath?: string;
  kind?: string;
  bytes?: number;
  sha256?: string;
  promptRequirements?: string[];
  coveredRequirements?: string[];
  sourceRef: unknown;
}

function verdict(condition: boolean): 'pass' | 'fail' { return condition ? 'pass' : 'fail'; }
function metric(metricKey: string, ok: boolean, details: Record<string, unknown>, sourceRef: unknown): EvalResultInput {
  return { metricKey, score: ok ? 1 : 0, verdict: verdict(ok), confidence: 'observed', details, sourceRefs: [sourceRef] };
}

/** Pure evaluator: checks durable artifact metadata without reading future human labels. */
export function evaluateArtifactCompleteness(input: ArtifactEvidence): EvalResultInput[] {
  const pathSafe = typeof input.relativePath === 'string'
    && input.relativePath.length > 0
    && !input.relativePath.startsWith('/')
    && !input.relativePath.split('/').includes('..');
  const shaValid = typeof input.sha256 === 'string' && /^[a-f0-9]{64}$/.test(input.sha256);
  const required = [...new Set(input.promptRequirements ?? [])];
  const covered = new Set(input.coveredRequirements ?? []);
  const missing = required.filter(item => !covered.has(item));
  return [
    metric('artifact.output_key.present', Boolean(input.outputKey?.trim()), { outputKey: input.outputKey ?? null }, input.sourceRef),
    metric('artifact.relative_path.safe', pathSafe, { relativePath: input.relativePath ?? null }, input.sourceRef),
    metric('artifact.kind.present', Boolean(input.kind?.trim()), { kind: input.kind ?? null }, input.sourceRef),
    metric('artifact.bytes.valid', Number.isInteger(input.bytes) && Number(input.bytes) >= 0, { bytes: input.bytes ?? null }, input.sourceRef),
    metric('artifact.sha256.valid', shaValid, { sha256: input.sha256 ?? null }, input.sourceRef),
    metric('artifact.prompt_coverage.complete', missing.length === 0, { required, missing }, input.sourceRef),
  ];
}

export function artifactEvalTargetId(input: ArtifactEvidence): string {
  return `artifact_${createHash('sha256').update(JSON.stringify({
    outputKey: input.outputKey, relativePath: input.relativePath, kind: input.kind, sha256: input.sha256,
  })).digest('hex')}`;
}
