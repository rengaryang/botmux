import { mkdtempSync, rmSync, writeFileSync, chmodSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { PiDistillationExecutor } from '../src/services/km/pi-distillation-executor.js';
import type { CliDistillationInvocation } from '../src/services/km/cli-distillation-runner.js';

const dirs: string[] = [];
function fakePi(script: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'fake-pi-distill-')); dirs.push(dir);
  const path = join(dir, 'pi'); writeFileSync(path, `#!/bin/sh\n${script}\n`); chmodSync(path, 0o755); return path;
}
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
const invocation: CliDistillationInvocation = {
  workload: 'km-distillation', cliId: 'pi', systemPrompt: 'system', userPrompt: 'user', timeoutMs: 5_000,
  env: { BOTMUX_KM_WORKLOAD: 'distillation' }, maxOutputBytes: 1024,
};

describe('Pi distillation executor', () => {
  it('uses non-interactive no-tool no-customization flags and scrubs session identity', async () => {
    const bin = fakePi(`
      all=" $* "
      for flag in --print --mode --no-session --no-tools --no-extensions --no-skills --no-context-files; do case "$all" in *" $flag "*) ;; *) exit 42;; esac; done
      [ "$BOTMUX_KM_WORKLOAD" = distillation ] || exit 43
      [ -z "$BOTMUX_SESSION_ID" ] || exit 44
      printf '%s' '{"knowledge":[],"memories":[],"discarded":[],"warnings":[]}'
    `);
    const output = await new PiDistillationExecutor({ piBin: bin, baseEnv: { BOTMUX_SESSION_ID: 'parent-session' } }).invoke(invocation);
    expect(JSON.parse(output)).toEqual({ knowledge: [], memories: [], discarded: [], warnings: [] });
  });

  it('accepts a JSON fence but rejects empty output', async () => {
    const fenced = fakePi(`printf '%s' '\`\`\`json\n{"knowledge":[],"memories":[],"discarded":[],"warnings":[]}\n\`\`\`'`);
    expect(JSON.parse(await new PiDistillationExecutor({ piBin: fenced }).invoke(invocation))).toEqual(expect.objectContaining({ knowledge: [] }));
    const empty = fakePi('exit 0');
    await expect(new PiDistillationExecutor({ piBin: empty }).invoke(invocation)).rejects.toThrow(/empty_output/);
  });

  it('fails closed for unsupported CLI, nonzero exit and oversized output', async () => {
    await expect(new PiDistillationExecutor().invoke({ ...invocation, cliId: 'codex' })).rejects.toThrow(/cli_unsupported/);
    const failed = fakePi('echo bad >&2; exit 7');
    await expect(new PiDistillationExecutor({ piBin: failed }).invoke(invocation)).rejects.toThrow(/cli_failed:7/);
    const large = fakePi(`python3 -c 'print("x"*2048)'`);
    await expect(new PiDistillationExecutor({ piBin: large }).invoke(invocation)).rejects.toThrow(/output_too_large/);
  });
});
