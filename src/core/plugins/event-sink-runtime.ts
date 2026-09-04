import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { basename, dirname, join, relative, resolve } from 'node:path';
import type { BotConfig } from '../../bot-registry.js';
import type { DaemonSession } from '../types.js';
import type { WorkerToDaemon } from '../../types.js';
import { logger } from '../../utils/logger.js';
import { readPluginRegistry } from '../../services/plugin-registry-store.js';
import { pluginEventSinkPrivatePath, pluginPrivateDir, pluginRuntimeDir } from './paths.js';

export const EVENT_SINK_PROTOCOL_VERSION = 1 as const;
export const EVENT_SINK_DEFAULT_MAX_ITEMS = 10_000;
export const EVENT_SINK_DEFAULT_MAX_BYTES = 64 * 1024 * 1024;
export const EVENT_SINK_MAX_ITEMS = 10_000;
export const EVENT_SINK_MAX_BYTES = 256 * 1024 * 1024;
export const EVENT_SINK_MAX_BATCH = 100;
export const EVENT_SINK_DEFAULT_FLUSH_MS = 250;
export const EVENT_SINK_MAX_LINE_BYTES = 2 * 1024 * 1024;

export interface TrustedPluginEventEnvelope {
  schemaVersion: 1;
  eventId: string;
  eventType: 'turn.completed' | 'turn.failed';
  occurredAt: string;
  scope: Readonly<{
    larkAppId: string;
    botId: string;
    sessionId: string;
    turnId: string;
    workflowRunId: string | null;
  }>;
  projection: Readonly<{
    status: 'completed' | 'failed' | 'ambiguous';
    model?: string;
    errorCode?: string;
  }>;
}

interface PrivateDescriptor {
  protocolVersion: 1;
  transport: 'private-stdio';
  command: string[];
  env?: Record<string, string>;
  queue?: { maxItems?: number; maxBytes?: number; batchSize?: number; maxBatchBytes?: number; flushMs?: number };
  ackTimeoutMs?: number;
}

interface PendingEvent { envelope: TrustedPluginEventEnvelope; bytes: number; spoolPath?: string }
interface Batch { generation: number; events: PendingEvent[]; timer: NodeJS.Timeout }
export type EventSinkAdmission = 'queued' | 'spooling' | 'disabled';

type SpawnChild = (command: string, args: string[], options: Parameters<typeof spawn>[2]) => ChildProcessWithoutNullStreams;

export interface DurablePluginEventSinkOptions {
  pluginId: string;
  larkAppId: string;
  botId: string;
  descriptor: PrivateDescriptor;
  runtimeDir: string;
  privateDir: string;
  spawnChild?: SpawnChild;
  onDiagnostic?: (message: string, error?: unknown) => void;
}

function bounded(raw: unknown, fallback: number, maximum: number): number {
  return Number.isInteger(raw) && Number(raw) > 0 ? Math.min(Number(raw), maximum) : fallback;
}

function assertContained(root: string, target: string): void {
  const rel = relative(resolve(root), resolve(target));
  if (!rel || rel.startsWith('..') || rel.includes(`..${process.platform === 'win32' ? '\\' : '/'}`)) {
    throw new Error('plugin_event_sink_path_escape');
  }
}

function safeBotDirectory(larkAppId: string, botId: string): string {
  return createHash('sha256').update(`${larkAppId}\0${botId}`).digest('hex');
}

/** A per-Bot, private-stdio durable event sink. enqueue() never waits for child
 * IO or filesystem IO. On pressure, persistence is scheduled off the caller's
 * stack and every failure is surfaced through diagnostics. */
export class DurablePluginEventSink {
  private readonly maxItems: number;
  private readonly maxBytes: number;
  private readonly batchSize: number;
  private readonly maxBatchBytes: number;
  private readonly flushMs: number;
  private readonly ackTimeoutMs: number;
  private readonly spoolDir: string;
  private readonly spawnChild: SpawnChild;
  private readonly diagnostic: (message: string, error?: unknown) => void;
  private queue: PendingEvent[] = [];
  private queuedBytes = 0;
  private child?: ChildProcessWithoutNullStreams;
  private generation = 0;
  private inFlight?: Batch;
  private flushTimer?: NodeJS.Timeout;
  private restartTimer?: NodeJS.Timeout;
  private restartAttempt = 0;
  private stdoutBuffer = '';
  private accepting = true;
  private pumping = false;

  constructor(private readonly options: DurablePluginEventSinkOptions) {
    const q = options.descriptor.queue ?? {};
    this.maxItems = bounded(q.maxItems, EVENT_SINK_DEFAULT_MAX_ITEMS, EVENT_SINK_MAX_ITEMS);
    this.maxBytes = bounded(q.maxBytes, EVENT_SINK_DEFAULT_MAX_BYTES, EVENT_SINK_MAX_BYTES);
    this.batchSize = bounded(q.batchSize, EVENT_SINK_MAX_BATCH, EVENT_SINK_MAX_BATCH);
    this.maxBatchBytes = bounded((q as { maxBatchBytes?: number }).maxBatchBytes, 256 * 1024, 1024 * 1024);
    this.flushMs = bounded(q.flushMs, EVENT_SINK_DEFAULT_FLUSH_MS, 5_000);
    this.ackTimeoutMs = bounded(options.descriptor.ackTimeoutMs, 10_000, 60_000);
    this.spawnChild = options.spawnChild ?? ((command, args, spawnOptions) => spawn(command, args, spawnOptions) as ChildProcessWithoutNullStreams);
    this.diagnostic = options.onDiagnostic ?? ((message, error) => logger.error(`[plugin-event-sink:${options.pluginId}:${options.botId}] ${message}${error ? `: ${String(error)}` : ''}`));
    this.spoolDir = join(options.privateDir, 'spool', safeBotDirectory(options.larkAppId, options.botId));
    assertContained(options.privateDir, this.spoolDir);
    mkdirSync(this.spoolDir, { recursive: true, mode: 0o700 });
    chmodSync(this.spoolDir, 0o700);
    this.replaySpool();
  }

  enqueue(envelope: TrustedPluginEventEnvelope): EventSinkAdmission {
    if (!this.accepting) {
      this.deferSpool({ envelope, bytes: Buffer.byteLength(JSON.stringify(envelope)) });
      return 'spooling';
    }
    const bytes = Buffer.byteLength(JSON.stringify(envelope));
    const pending = { envelope, bytes };
    if (bytes > EVENT_SINK_MAX_LINE_BYTES || this.queue.length >= this.maxItems || this.queuedBytes + bytes > this.maxBytes) {
      this.diagnostic(`queue overflow; event ${envelope.eventId} moved to durable spool`);
      this.deferSpool(pending);
      return 'spooling';
    }
    this.queue.push(pending);
    this.queuedBytes += bytes;
    this.schedulePump();
    return 'queued';
  }

  private deferSpool(event: PendingEvent): void {
    queueMicrotask(() => {
      try {
        this.writeSpool(event);
        if (this.accepting) this.replaySpool();
      } catch (error) {
        this.diagnostic(`FAILED to spool unacknowledged event ${event.envelope.eventId}; event retained for retry`, error);
        if (this.accepting) {
          setTimeout(() => this.deferSpool(event), 250).unref();
        }
      }
    });
  }

  private spoolName(event: PendingEvent): string {
    const time = Date.parse(event.envelope.occurredAt) || Date.now();
    return `${String(time).padStart(16, '0')}-${event.envelope.eventId.replace(/[^a-zA-Z0-9_-]/g, '_')}.json`;
  }

  private writeSpool(event: PendingEvent): string {
    mkdirSync(this.spoolDir, { recursive: true, mode: 0o700 });
    chmodSync(this.spoolDir, 0o700);
    const finalPath = join(this.spoolDir, this.spoolName(event));
    assertContained(this.spoolDir, finalPath);
    if (existsSync(finalPath)) return finalPath;
    const temp = join(this.spoolDir, `.${basename(finalPath)}.${process.pid}.${randomUUID()}.tmp`);
    assertContained(this.spoolDir, temp);
    writeFileSync(temp, `${JSON.stringify(event.envelope)}\n`, { mode: 0o600, flag: 'wx' });
    renameSync(temp, finalPath);
    return finalPath;
  }

  private replaySpool(): void {
    if (!this.accepting) return;
    let names: string[];
    try { names = readdirSync(this.spoolDir).filter(name => name.endsWith('.json')).sort(); }
    catch (error) { this.diagnostic('failed to list spool', error); return; }
    const already = new Set([...this.queue, ...(this.inFlight?.events ?? [])].map(item => item.envelope.eventId));
    for (const name of names) {
      if (this.queue.length >= this.maxItems) break;
      const path = join(this.spoolDir, name);
      try {
        assertContained(this.spoolDir, path);
        const envelope = JSON.parse(readFileSync(path, 'utf8')) as TrustedPluginEventEnvelope;
        if (already.has(envelope.eventId)) continue;
        const bytes = Buffer.byteLength(JSON.stringify(envelope));
        if (this.queuedBytes + bytes > this.maxBytes) break;
        this.queue.push({ envelope, bytes, spoolPath: path });
        this.queuedBytes += bytes;
        already.add(envelope.eventId);
      } catch (error) { this.diagnostic(`invalid spool entry retained: ${name}`, error); }
    }
    if (this.queue.length) this.schedulePump();
  }

  private ensureChild(): void {
    if (this.child || !this.accepting) return;
    const [command, ...args] = this.options.descriptor.command;
    if (!command) throw new Error('plugin_event_sink_command_missing');
    const child = this.spawnChild(command, args, {
      cwd: this.options.runtimeDir,
      // Do not leak the daemon's provider/Lark credentials. Plugins receive
      // only process-launch essentials plus their own 0600 private descriptor.
      env: {
        ...(process.env.PATH ? { PATH: process.env.PATH } : {}),
        ...(process.env.HOME ? { HOME: process.env.HOME } : {}),
        ...(process.env.TMPDIR ? { TMPDIR: process.env.TMPDIR } : {}),
        ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
        ...(this.options.descriptor.env ?? {}),
      },
      stdio: ['pipe', 'pipe', 'pipe'],
      shell: false,
    });
    this.child = child;
    const generation = ++this.generation;
    this.stdoutBuffer = '';
    child.stdout.on('data', data => this.onStdout(generation, data));
    child.stderr.on('data', data => this.diagnostic(`child stderr: ${String(data).slice(0, 1024)}`));
    child.once('error', error => this.onChildExit(generation, error));
    child.once('exit', (code, signal) => this.onChildExit(generation, new Error(`exit ${code ?? signal}`)));
  }

  private onStdout(generation: number, data: Buffer | string): void {
    if (generation !== this.generation) return;
    this.stdoutBuffer += String(data);
    if (Buffer.byteLength(this.stdoutBuffer) > EVENT_SINK_MAX_LINE_BYTES) {
      this.diagnostic('child emitted oversized/unterminated protocol line; restarting');
      this.child?.kill('SIGKILL');
      return;
    }
    for (;;) {
      const newline = this.stdoutBuffer.indexOf('\n');
      if (newline < 0) break;
      const line = this.stdoutBuffer.slice(0, newline);
      this.stdoutBuffer = this.stdoutBuffer.slice(newline + 1);
      if (!line.trim()) continue;
      try {
        const ack = JSON.parse(line) as { type?: string; generation?: number; eventIds?: unknown };
        if (ack.type !== 'ack' || ack.generation !== generation || !Array.isArray(ack.eventIds)) continue;
        this.commitAck(generation, ack.eventIds.filter((id): id is string => typeof id === 'string'));
      } catch (error) { this.diagnostic('child emitted invalid/partial NDJSON line', error); }
    }
  }

  private commitAck(generation: number, eventIds: string[]): void {
    const batch = this.inFlight;
    if (!batch || batch.generation !== generation) return; // duplicate or stale ACK
    const expected = new Set(batch.events.map(event => event.envelope.eventId));
    if (eventIds.length !== expected.size || eventIds.some(id => !expected.delete(id)) || expected.size) {
      this.diagnostic('child ACK did not exactly commit the in-flight batch; ignored');
      return;
    }
    clearTimeout(batch.timer);
    for (const event of batch.events) {
      if (event.spoolPath) {
        try { rmSync(event.spoolPath, { force: true }); } catch (error) { this.diagnostic('failed to remove committed spool event', error); }
      }
    }
    this.inFlight = undefined;
    this.restartAttempt = 0;
    this.schedulePump(true);
  }

  private onChildExit(generation: number, error: unknown): void {
    if (generation !== this.generation) return;
    this.child = undefined;
    if (this.inFlight?.generation === generation) {
      clearTimeout(this.inFlight.timer);
      const retry = this.inFlight.events;
      this.inFlight = undefined;
      this.queue.unshift(...retry);
      this.queuedBytes += retry.reduce((sum, item) => sum + item.bytes, 0);
    }
    if (!this.accepting) return;
    this.diagnostic('child unavailable; unacknowledged events retained', error);
    const delay = Math.min(30_000, 100 * 2 ** Math.min(this.restartAttempt++, 8));
    clearTimeout(this.restartTimer);
    this.restartTimer = setTimeout(() => this.schedulePump(true), delay);
    this.restartTimer.unref();
  }

  private schedulePump(immediate = false): void {
    if (this.pumping || this.inFlight || !this.queue.length || !this.accepting) return;
    if (immediate || this.queue.length >= this.batchSize) queueMicrotask(() => this.pump());
    else if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => { this.flushTimer = undefined; this.pump(); }, this.flushMs);
      this.flushTimer.unref();
    }
  }

  private pump(): void {
    if (this.pumping || this.inFlight || !this.queue.length || !this.accepting) return;
    this.pumping = true;
    try {
      this.ensureChild();
      if (!this.child?.stdin.writable) throw new Error('plugin_event_sink_child_not_writable');
      const events: PendingEvent[] = [];
      let batchBytes = 0;
      while (events.length < this.batchSize && this.queue.length > 0) {
        const next = this.queue[0];
        if (events.length > 0 && batchBytes + next.bytes > this.maxBatchBytes) break;
        events.push(this.queue.shift()!);
        batchBytes += next.bytes;
      }
      this.queuedBytes -= batchBytes;
      const generation = this.generation;
      const line = `${JSON.stringify({ type: 'events', protocolVersion: 1, generation, events: events.map(event => event.envelope) })}\n`;
      if (Buffer.byteLength(line) > EVENT_SINK_MAX_LINE_BYTES) {
        for (const event of events) this.deferSpool(event);
        this.diagnostic('batch exceeded protocol line bound and was spooled');
        return;
      }
      const timer = setTimeout(() => {
        if (this.inFlight?.generation !== generation) return;
        this.diagnostic('ACK timeout; restarting child with batch retained');
        this.child?.kill('SIGKILL');
      }, this.ackTimeoutMs);
      timer.unref();
      this.inFlight = { generation, events, timer };
      this.child.stdin.write(line, error => {
        if (error && generation === this.generation) this.child?.kill('SIGKILL');
      });
    } catch (error) {
      this.diagnostic('failed to dispatch event batch; retained for retry', error);
      this.onChildExit(this.generation, error);
    } finally { this.pumping = false; }
  }

  pendingCount(): number { return this.queue.length + (this.inFlight?.events.length ?? 0); }

  async drain(timeoutMs: number): Promise<number> {
    this.schedulePump(true);
    const deadline = Date.now() + Math.max(0, timeoutMs);
    while (this.pendingCount() > 0 && Date.now() < deadline) await new Promise(resolveWait => setTimeout(resolveWait, 10));
    if (this.pendingCount() > 0) this.spoolAll();
    return this.pendingCount();
  }

  private spoolAll(): void {
    const pending = [...(this.inFlight?.events ?? []), ...this.queue];
    if (this.inFlight) clearTimeout(this.inFlight.timer);
    this.inFlight = undefined;
    this.queue = [];
    this.queuedBytes = 0;
    for (const event of pending) {
      try { event.spoolPath ??= this.writeSpool(event); }
      catch (error) { this.diagnostic(`FAILED shutdown spool for ${event.envelope.eventId}; event could be lost`, error); }
    }
  }

  async shutdown(timeoutMs = 2_000): Promise<void> {
    const deadline = Date.now() + Math.max(0, timeoutMs);
    await this.drain(Math.max(0, deadline - Date.now()));
    this.accepting = false;
    clearTimeout(this.flushTimer); clearTimeout(this.restartTimer);
    this.spoolAll();
    this.child?.kill('SIGTERM');
    this.child = undefined;
  }
}

function readDescriptor(pluginId: string): PrivateDescriptor {
  const path = pluginEventSinkPrivatePath(pluginId);
  const root = pluginPrivateDir(pluginId);
  assertContained(root, path);
  const descriptor = JSON.parse(readFileSync(path, 'utf8')) as PrivateDescriptor;
  if (descriptor.protocolVersion !== 1 || descriptor.transport !== 'private-stdio' || !Array.isArray(descriptor.command)) {
    throw new Error(`invalid_plugin_event_sink_private_descriptor:${pluginId}`);
  }
  return descriptor;
}

/** Core-owned exact-Bot binding manager. Deliberately ignores machine/global
 * plugin defaults: a canary sink runs only where bot.plugins names it. */
export class PluginEventSinkManager {
  private readonly sinks = new Map<string, DurablePluginEventSink>();
  private readonly starting = new Map<string, TrustedPluginEventEnvelope[]>();

  emitTurnTerminal(
    bot: BotConfig,
    ds: DaemonSession,
    terminal: Extract<WorkerToDaemon, { type: 'turn_terminal' }>,
    context: { workerGeneration: number; workflowRunId?: string },
  ): void {
    if (ds.workerGeneration !== undefined && context.workerGeneration !== ds.workerGeneration) return;
    const pluginIds = bot.plugins ?? []; // exact binding; no global fallback/union
    if (!pluginIds.length) return;
    const eventType = terminal.status === 'completed' ? 'turn.completed' : 'turn.failed';
    const registry = readPluginRegistry();
    for (const pluginId of pluginIds) {
      const contribution = registry.plugins[pluginId]?.contributions?.eventSink;
      if (!contribution?.events.includes(eventType)) continue;
      const key = `${pluginId}\0${bot.larkAppId}`;
      const envelope: TrustedPluginEventEnvelope = Object.freeze({
        schemaVersion: 1,
        eventId: randomUUID(),
        eventType,
        occurredAt: new Date().toISOString(),
        scope: Object.freeze({
          larkAppId: bot.larkAppId,
          botId: bot.name ?? bot.larkAppId,
          sessionId: ds.session.sessionId,
          turnId: terminal.turnId,
          workflowRunId: context.workflowRunId ?? null,
        }),
        projection: Object.freeze({
          status: terminal.status === 'cancelled' ? 'failed' : terminal.status,
          ...(ds.activeModel ? { model: ds.activeModel } : {}),
          ...(terminal.errorCode ? { errorCode: terminal.errorCode } : {}),
        }),
      });
      const sink = this.sinks.get(key);
      if (sink) {
        sink.enqueue(envelope);
        continue;
      }
      // Initialization performs private descriptor/spool filesystem reads, so
      // never do it on the turn-terminal hot stack. A small bounded bootstrap
      // queue bridges the single microtask until the real per-Bot queue exists.
      const pending = this.starting.get(key);
      if (pending) {
        if (pending.length < EVENT_SINK_MAX_ITEMS) pending.push(envelope);
        else logger.error(`[plugin-event-sink:${pluginId}:${bot.larkAppId}] bootstrap queue overflow; event was not silently discarded`);
        continue;
      }
      this.starting.set(key, [envelope]);
      queueMicrotask(() => {
        const buffered = this.starting.get(key) ?? [];
        this.starting.delete(key);
        try {
          const started = new DurablePluginEventSink({
            pluginId,
            larkAppId: bot.larkAppId,
            botId: bot.name ?? bot.larkAppId,
            descriptor: readDescriptor(pluginId),
            runtimeDir: pluginRuntimeDir(pluginId),
            privateDir: pluginPrivateDir(pluginId),
          });
          this.sinks.set(key, started);
          for (const item of buffered) started.enqueue(item);
        } catch (error) {
          // Initialization failure is loud. The plugin is not allowed to break
          // the conversation; no untrusted code or network endpoint is used.
          logger.error(`[plugin-event-sink:${pluginId}:${bot.larkAppId}] initialization failed with ${buffered.length} retained-in-process event(s): ${String(error)}`);
        }
      });
    }
  }

  async shutdown(timeoutMs = 2_000): Promise<void> {
    // Let scheduled initializers transfer their bootstrap queues first.
    await Promise.resolve();
    const sinks = [...this.sinks.values()];
    this.sinks.clear();
    await Promise.allSettled(sinks.map(sink => sink.shutdown(timeoutMs)));
  }
}
