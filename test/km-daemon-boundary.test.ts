import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const daemonSource = readFileSync(join(process.cwd(), 'src', 'daemon.ts'), 'utf8');

describe('KM live prompt-memory daemon boundary', () => {
  it('routes ordinary Lark turns through one composition boundary only', () => {
    expect(daemonSource).toContain('composePromptMemoryForTurn');
    expect(daemonSource.match(/composePromptMemoryForTurn\(/g)).toHaveLength(1);
    expect(daemonSource).not.toContain('runRetrievalShadow');
  });

  it('composes before ordinary prompt builders and after command early returns', () => {
    const helper = daemonSource.indexOf('const composePromptMemoryAtBoundary');
    const call = daemonSource.indexOf('await composePromptMemoryAtBoundary(ds)');
    const firstFollowUp = daemonSource.indexOf('buildFollowUpCliInput(promptContent', call);
    const firstOpening = daemonSource.indexOf('buildNewTopicCliInput(', call);
    const commandBoundary = daemonSource.indexOf('// Intercept daemon commands');
    expect(helper).toBeGreaterThan(0);
    expect(call).toBeGreaterThan(helper);
    expect(call).toBeGreaterThan(commandBoundary);
    expect(firstFollowUp).toBeGreaterThan(call);
    expect(firstOpening).toBeGreaterThan(call);
  });
});
