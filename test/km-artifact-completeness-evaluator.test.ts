import { describe, expect, it } from 'vitest';
import { artifactEvalTargetId, evaluateArtifactCompleteness } from '../src/services/km/artifact-completeness-evaluator.js';

const valid = {
  outputKey: 'report', relativePath: 'out/report.md', kind: 'file', bytes: 120,
  sha256: 'a'.repeat(64), promptRequirements: ['summary', 'risks'], coveredRequirements: ['summary', 'risks'],
  sourceRef: { kind: 'workflow-artifact', ref: 'wf/node/report' },
};

describe('artifact completeness evaluator', () => {
  it('checks key, safe path, kind, bytes, sha256 and prompt coverage', () => {
    const results = evaluateArtifactCompleteness(valid);
    expect(results).toHaveLength(6);
    expect(results.every(result => result.verdict === 'pass')).toBe(true);
    expect(results.every(result => result.sourceRefs.length === 1)).toBe(true);
  });

  it('fails unsafe and incomplete artifact evidence', () => {
    const results = evaluateArtifactCompleteness({ ...valid, relativePath: '../secret', bytes: -1, sha256: 'bad', coveredRequirements: ['summary'] });
    expect(results.filter(result => result.verdict === 'fail').map(result => result.metricKey)).toEqual([
      'artifact.relative_path.safe', 'artifact.bytes.valid', 'artifact.sha256.valid', 'artifact.prompt_coverage.complete',
    ]);
  });

  it('builds a stable target id from artifact identity', () => {
    expect(artifactEvalTargetId(valid)).toBe(artifactEvalTargetId({ ...valid, bytes: 999 }));
  });
});
