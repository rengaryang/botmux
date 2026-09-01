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

  it('starts backend outbox runtime only behind explicit observation and worker gates', () => {
    expect(daemonSource).toContain('runKmBackendWorkerOnce');
    expect(daemonSource.match(/runKmBackendWorkerOnce\(/g)).toHaveLength(1);
    const gate = 'if (kmBackendWorkerRunning || !isKmObservationEnabled() || !isKmBackendWorkerEnabled()) return;';
    expect(daemonSource).toContain(gate);
    expect(daemonSource).toContain('clearInterval(kmBackendWorkerTimer)');
  });

  it('starts shadow quality only behind explicit observation and shadow-quality gates', () => {
    expect(daemonSource).toContain('runKmShadowQualityOnce');
    expect(daemonSource.match(/runKmShadowQualityOnce\(/g)).toHaveLength(1);
    const gate = 'if (kmShadowQualityRunning || !isKmObservationEnabled() || !isKmShadowQualityEnabled()) return;';
    expect(daemonSource).toContain(gate);
    expect(daemonSource).not.toContain('BOTMUX_KM_PI_SHADOW_ENABLED =');
    expect(daemonSource).not.toContain('BOTMUX_KM_AUTO_EVAL_ENABLED =');
    expect(daemonSource).not.toContain('BOTMUX_KM_AUTO_EVOLUTION_ENABLED =');
  });

  it('composes before ordinary prompt builders and after command early returns', () => {
    const helper = daemonSource.indexOf('const composePromptMemoryAtBoundary');
    const existingCall = daemonSource.indexOf('await composePromptMemoryAtBoundary(ds)');
    const newSessionCall = daemonSource.indexOf('await composePromptMemoryAtBoundary(newDs)');
    const firstFollowUp = daemonSource.indexOf('buildFollowUpCliInput(promptContent', existingCall);
    const firstOpening = daemonSource.indexOf('buildNewTopicCliInput(', existingCall);
    const registrationCheck = daemonSource.indexOf('if (!registration.accepted)');
    const newSessionPromptPersist = daemonSource.indexOf('newDs.pendingPrompt = promptContent', newSessionCall);
    const commandBoundary = daemonSource.indexOf('// Intercept daemon commands');
    expect(helper).toBeGreaterThan(0);
    expect(existingCall).toBeGreaterThan(helper);
    expect(existingCall).toBeGreaterThan(commandBoundary);
    expect(firstFollowUp).toBeGreaterThan(existingCall);
    expect(firstOpening).toBeGreaterThan(existingCall);
    expect(newSessionCall).toBeGreaterThan(registrationCheck);
    expect(newSessionPromptPersist).toBeGreaterThan(newSessionCall);
  });
});
