import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { isAbsolute, join, relative, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { canonicalJson } from '../../utils/canonical-input-hash.js';
import { loadBotConfigs } from '../../bot-registry.js';
import { readGlobalConfig } from '../../global-config.js';
import { getInstalledPlugin } from '../../services/plugin-registry-store.js';
import { resolveEffectivePluginIds } from './effective.js';
import { assertJsonSchemaValue, assertSafePluginDto } from './json-contract.js';
import { assertPluginProtocolCompatibility } from './manifest.js';
import { pluginRuntimeDir, pluginWorkflowExecutorPrivatePath, resolvePluginPath } from './paths.js';

export interface PluginWorkflowPrincipal {
  subject: string;
  exactBotId: string;
  scopes: readonly string[];
}

export interface PluginQuerySpec {
  pluginId: string;
  query: string;
  input: Record<string, unknown>;
}

export interface FrozenPluginWorkflowDispatch {
  schemaVersion: 1;
  plugin: { id: string; version: string; protocolVersion: 1 };
  querySpec: PluginQuerySpec;
  inputDigest: string;
  principal: PluginWorkflowPrincipal;
  limits: { outputBytes: number; timeoutMs: number };
}

export interface PluginWorkflowExecutionResult {
  frozen: FrozenPluginWorkflowDispatch;
  output: unknown;
  outputDigest: string;
}

function sha256(value: string | Buffer): string {
  return `sha256:${createHash('sha256').update(value).digest('hex')}`;
}

function commandDescriptor(pluginId: string): { command: string[] } {
  const path = pluginWorkflowExecutorPrivatePath(pluginId);
  if (!existsSync(path)) throw new Error('plugin_workflow_private_descriptor_missing');
  const raw = JSON.parse(readFileSync(path, 'utf8')) as { command?: unknown };
  if (!Array.isArray(raw.command) || raw.command.length === 0 || raw.command.some(part => typeof part !== 'string' || !part)) {
    throw new Error('plugin_workflow_command_invalid');
  }
  if (raw.command.some(part => /(?:^|\/)(?:sh|bash|zsh|fish|cmd(?:\.exe)?|powershell(?:\.exe)?)$/i.test(part))) {
    throw new Error('plugin_workflow_shell_forbidden');
  }
  return { command: raw.command as string[] };
}

function assertPluginEnabledForExactBot(pluginId: string, exactBotId: string): void {
  let bots: ReturnType<typeof loadBotConfigs>;
  try { bots = loadBotConfigs(); } catch { throw new Error('plugin_workflow_exact_bot_unknown'); }
  const bot = bots.find(candidate => candidate.larkAppId === exactBotId);
  if (!bot) throw new Error('plugin_workflow_exact_bot_unknown');
  if (!resolveEffectivePluginIds(bot, { plugins: readGlobalConfig().plugins }).includes(pluginId)) {
    throw new Error('plugin_workflow_cross_bot_forbidden');
  }
}

function assertReadOnlyQuery(spec: PluginQuerySpec): void {
  if (!/^[a-z][a-z0-9._:-]{0,63}$/.test(spec.query)) throw new Error('plugin_workflow_query_invalid');
  const serialized = canonicalJson(spec.input);
  if (/\b(select|insert|update|delete|drop|alter|create|grant|revoke)\b/i.test(serialized)) {
    throw new Error('plugin_workflow_arbitrary_sql_forbidden');
  }
  assertSafePluginDto(spec.input);
}

export function freezePluginWorkflowDispatch(input: {
  spec: PluginQuerySpec;
  principal: PluginWorkflowPrincipal;
  timeoutMs?: number;
}): FrozenPluginWorkflowDispatch {
  assertReadOnlyQuery(input.spec);
  const record = getInstalledPlugin(input.spec.pluginId);
  const contribution = record?.contributions?.workflowExecutor;
  if (!record || !contribution) throw new Error('plugin_workflow_executor_not_installed');
  assertPluginProtocolCompatibility(record);
  if (contribution.mode !== 'read-only') throw new Error('plugin_workflow_executor_not_read_only');
  if (!input.principal.subject || !input.principal.exactBotId || input.principal.scopes.length === 0) throw new Error('plugin_workflow_principal_missing');
  if (!input.principal.scopes.includes('workflow.read')) throw new Error('plugin_workflow_scope_forbidden');
  assertPluginEnabledForExactBot(record.id, input.principal.exactBotId);
  assertJsonSchemaValue(contribution.inputSchema, input.spec.input);
  const querySpec = JSON.parse(canonicalJson(input.spec)) as PluginQuerySpec;
  return Object.freeze({
    schemaVersion: 1,
    plugin: Object.freeze({ id: record.id, version: record.version, protocolVersion: 1 as const }),
    querySpec: Object.freeze(querySpec),
    inputDigest: sha256(canonicalJson(querySpec)),
    principal: Object.freeze({ ...input.principal, scopes: Object.freeze([...input.principal.scopes]) }),
    limits: Object.freeze({ outputBytes: contribution.outputMaxBytes, timeoutMs: Math.min(Math.max(input.timeoutMs ?? 30_000, 1), 300_000) }),
  });
}

/** Execute one already-frozen read query. Callers persist `frozen` beside the
 * v3 attempt/journal before invoking this function; retries must reuse it. */
export async function executeFrozenPluginWorkflow(
  frozen: FrozenPluginWorkflowDispatch,
  options: { signal?: AbortSignal } = {},
): Promise<PluginWorkflowExecutionResult> {
  const record = getInstalledPlugin(frozen.plugin.id);
  if (!record || record.version !== frozen.plugin.version || record.manifest.compatibility?.protocolVersion !== frozen.plugin.protocolVersion) {
    throw new Error('plugin_workflow_version_drift');
  }
  const contribution = record.contributions?.workflowExecutor;
  if (!contribution || contribution.mode !== 'read-only') throw new Error('plugin_workflow_executor_disabled');
  assertPluginProtocolCompatibility(record);
  assertPluginEnabledForExactBot(record.id, frozen.principal.exactBotId);
  if (!frozen.principal.scopes.includes('workflow.read')) throw new Error('plugin_workflow_scope_forbidden');
  if (sha256(canonicalJson(frozen.querySpec)) !== frozen.inputDigest) throw new Error('plugin_workflow_dispatch_digest_mismatch');
  assertReadOnlyQuery(frozen.querySpec);
  assertJsonSchemaValue(contribution.inputSchema, frozen.querySpec.input);
  const descriptor = commandDescriptor(record.id);
  const runtime = pluginRuntimeDir(record.id);
  const [rawProgram, ...rawArgs] = descriptor.command;
  const program = rawProgram.startsWith('./') ? resolvePluginPath(runtime, rawProgram, 'workflow_executor_command') : rawProgram;
  const args = rawArgs.map(part => part.startsWith('./') ? resolvePluginPath(runtime, part, 'workflow_executor_command') : part);
  const payload = `${canonicalJson({
    schemaVersion: 1,
    plugin: frozen.plugin,
    querySpec: frozen.querySpec,
    inputDigest: frozen.inputDigest,
    principal: frozen.principal,
    limits: frozen.limits,
  })}\n`;
  const output = await new Promise<Buffer>((resolve, reject) => {
    const child = spawn(program, args, {
      cwd: runtime,
      stdio: ['pipe', 'pipe', 'pipe'],
      shell: false,
      env: { PATH: process.env.PATH ?? '', HOME: process.env.HOME ?? '', LANG: process.env.LANG ?? 'C.UTF-8' },
    });
    const chunks: Buffer[] = [];
    const errors: Buffer[] = [];
    let bytes = 0;
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error('plugin_workflow_timeout')); }, frozen.limits.timeoutMs);
    const abort = () => { child.kill('SIGKILL'); reject(new Error('plugin_workflow_cancelled')); };
    options.signal?.addEventListener('abort', abort, { once: true });
    child.stdout.on('data', (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > frozen.limits.outputBytes) { child.kill('SIGKILL'); reject(new Error('plugin_workflow_output_too_large')); }
      else chunks.push(chunk);
    });
    child.stderr.on('data', (chunk: Buffer) => { if (errors.reduce((n, item) => n + item.length, 0) < 8192) errors.push(chunk); });
    child.once('error', reject);
    child.once('exit', (code) => {
      clearTimeout(timer);
      options.signal?.removeEventListener('abort', abort);
      if (code !== 0) reject(new Error(`plugin_workflow_failed:${code}:${Buffer.concat(errors).toString('utf8').slice(0, 512)}`));
      else resolve(Buffer.concat(chunks));
    });
    child.stdin.end(payload);
  });
  let parsed: unknown;
  try { parsed = JSON.parse(output.toString('utf8')); } catch { throw new Error('plugin_workflow_output_invalid_json'); }
  assertSafePluginDto(parsed);
  assertJsonSchemaValue(contribution.outputSchema, parsed);
  return { frozen, output: parsed, outputDigest: sha256(output) };
}

function containedAttemptDir(runDir: string, attemptId: string): string {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(attemptId)) throw new Error('plugin_workflow_attempt_id_invalid');
  const root = resolve(runDir);
  const target = resolve(root, 'plugin-attempts', attemptId);
  const rel = relative(root, target);
  if (!rel || rel.startsWith('..') || isAbsolute(rel)) throw new Error('plugin_workflow_attempt_escapes_run');
  return target;
}

function writeCreateOrVerify(path: string, value: string, label: string): void {
  try { writeFileSync(path, value, { encoding: 'utf8', mode: 0o600, flag: 'wx' }); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
    if (readFileSync(path, 'utf8') !== value) throw new Error(`plugin_workflow_${label}_conflict`);
  }
}

/** v3-compatible durable attempt seam. The frozen sidecar is the dispatch
 * commit; exact retries reuse a validated terminal artifact, while a changed
 * plugin/version/input fails closed. Artifacts are run-relative and contained. */
export async function runPluginWorkflowAttempt(input: {
  runDir: string;
  attemptId: string;
  frozen: FrozenPluginWorkflowDispatch;
  signal?: AbortSignal;
}): Promise<PluginWorkflowExecutionResult> {
  const attemptDir = containedAttemptDir(input.runDir, input.attemptId);
  mkdirSync(attemptDir, { recursive: true, mode: 0o700 });
  const frozenPath = join(attemptDir, 'plugin-input.json');
  const frozenBytes = `${canonicalJson(input.frozen)}\n`;
  writeCreateOrVerify(frozenPath, frozenBytes, 'frozen_input');
  const resultPath = join(attemptDir, 'plugin-result.json');
  if (existsSync(resultPath)) {
    const recovered = JSON.parse(readFileSync(resultPath, 'utf8')) as PluginWorkflowExecutionResult;
    if (canonicalJson(recovered.frozen) !== canonicalJson(input.frozen)) throw new Error('plugin_workflow_recovery_identity_mismatch');
    if (sha256(canonicalJson(recovered.output)) !== recovered.outputDigest) throw new Error('plugin_workflow_recovery_digest_mismatch');
    return recovered;
  }
  if (input.signal?.aborted) throw new Error('plugin_workflow_cancelled');
  const result = await executeFrozenPluginWorkflow(input.frozen, { signal: input.signal });
  // Normalize the digest over canonical output so recovery can verify it without
  // trusting original stdout formatting.
  const durable = { ...result, outputDigest: sha256(canonicalJson(result.output)) };
  writeCreateOrVerify(resultPath, `${canonicalJson(durable)}\n`, 'result');
  return durable;
}
