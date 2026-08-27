import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import type { CliDistillationExecutor, CliDistillationInvocation } from './cli-distillation-runner.js';

export interface PiDistillationExecutorOptions {
  piBin?: string;
  cwd?: string;
  baseEnv?: NodeJS.ProcessEnv;
}

function extractText(stdout: string): string {
  const value = stdout.trim();
  if (!value) throw new Error('km_distillation_empty_output');
  // Pi text mode is preferred because the system prompt already requires JSON.
  // Accept a fenced JSON block defensively, but reject surrounding prose.
  const fence = value.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  return fence ? fence[1].trim() : value;
}

export class PiDistillationExecutor implements CliDistillationExecutor {
  constructor(private readonly options: PiDistillationExecutorOptions = {}) {}

  async invoke(input: CliDistillationInvocation): Promise<string> {
    if (input.cliId !== 'pi') throw new Error(`km_distillation_cli_unsupported:${input.cliId}`);
    const args = [
      '--print', '--mode', 'text', '--no-session', '--no-tools', '--no-extensions', '--no-skills',
      '--no-prompt-templates', '--no-context-files', '--no-approve',
      '--system-prompt', input.systemPrompt,
    ];
    if (input.model?.trim()) args.push('--model', input.model.trim());
    args.push(input.userPrompt);
    const bin = this.options.piBin ?? 'pi';
    const env: NodeJS.ProcessEnv = {
      ...(this.options.baseEnv ?? process.env),
      ...input.env,
      BOTMUX_SESSION_ID: '', BOTMUX_TURN_ID: '', BOTMUX_CHAT_ID: '', BOTMUX_LARK_APP_ID: '',
      BOTMUX_KM_DISTILLATION_RUN_ID: randomUUID(),
    };
    return new Promise<string>((resolve, reject) => {
      const child = spawn(bin, args, {
        cwd: this.options.cwd,
        env,
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: false,
      });
      let stdout = Buffer.alloc(0);
      let stderr = Buffer.alloc(0);
      let settled = false;
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (error) reject(error);
        else {
          try { resolve(extractText(stdout.toString('utf8'))); }
          catch (parseError) { reject(parseError); }
        }
      };
      child.stdout.on('data', chunk => {
        stdout = Buffer.concat([stdout, Buffer.from(chunk)]);
        if (stdout.length > input.maxOutputBytes) {
          child.kill('SIGKILL');
          finish(new Error('km_distillation_output_too_large'));
        }
      });
      child.stderr.on('data', chunk => {
        stderr = Buffer.concat([stderr, Buffer.from(chunk)]);
        if (stderr.length > 64 * 1024) stderr = stderr.subarray(stderr.length - 64 * 1024);
      });
      child.once('error', error => finish(new Error(`km_distillation_spawn_failed:${error.message}`)));
      child.once('close', (code, signal) => {
        if (code !== 0) return finish(new Error(`km_distillation_cli_failed:${code ?? signal ?? 'unknown'}:${stderr.toString('utf8').slice(-500)}`));
        finish();
      });
      const timer = setTimeout(() => {
        child.kill('SIGTERM');
        setTimeout(() => { if (!settled) child.kill('SIGKILL'); }, 1_000).unref();
        finish(new Error('km_distillation_timeout'));
      }, input.timeoutMs);
      timer.unref();
    });
  }
}
