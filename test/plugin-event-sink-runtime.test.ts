import { mkdtempSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  DurablePluginEventSink,
  EVENT_SINK_DEFAULT_MAX_BYTES,
  EVENT_SINK_DEFAULT_MAX_ITEMS,
  PluginEventSinkManager,
  type TrustedPluginEventEnvelope,
} from '../src/core/plugins/event-sink-runtime.js';

const fixture = join(process.cwd(), 'test/fixtures/fake-plugin-event-sink.mjs');
const roots: string[] = [];
const waitFor = async (fn: () => boolean, timeout = 3000) => {
  const end = Date.now() + timeout;
  while (!fn() && Date.now() < end) await new Promise(resolve => setTimeout(resolve, 10));
  expect(fn()).toBe(true);
};
function envelope(id: string, bot = 'bot-a'): TrustedPluginEventEnvelope {
  return Object.freeze({
    schemaVersion: 1, eventId: id, eventType: 'turn.completed', occurredAt: new Date().toISOString(),
    scope: Object.freeze({ larkAppId: `app-${bot}`, botId: bot, sessionId: 'session', turnId: `turn-${id}`, workflowRunId: null }),
    projection: Object.freeze({ status: 'completed' }),
  });
}
function create(mode = 'ack', queue: Record<string, number> = {}) {
  const root = mkdtempSync(join(tmpdir(), 'botmux-event-sink-')); roots.push(root);
  const output = join(root, 'events.ndjson'); writeFileSync(output, '');
  const sink = new DurablePluginEventSink({
    pluginId: 'sink', larkAppId: 'app-a', botId: 'bot-a', runtimeDir: root, privateDir: join(root, 'private'),
    descriptor: { protocolVersion: 1, transport: 'private-stdio', command: [process.execPath, fixture], queue,
      env: { FAKE_SINK_MODE: mode, FAKE_SINK_OUTPUT: output, FAKE_SINK_MARKER: join(root, 'marker') }, ackTimeoutMs: 100 },
  });
  return { root, output, sink };
}
afterEach(async () => { vi.restoreAllMocks(); });

describe('DurablePluginEventSink', () => {
  it('uses required bounded defaults and accepts without awaiting child IO', async () => {
    expect(EVENT_SINK_DEFAULT_MAX_ITEMS).toBe(10_000);
    expect(EVENT_SINK_DEFAULT_MAX_BYTES).toBe(64 * 1024 * 1024);
    const { output, sink } = create('ack', { flushMs: 1 });
    expect(sink.enqueue(envelope('one'))).toBe('queued');
    await waitFor(() => sink.pendingCount() === 0);
    const sent = JSON.parse(readFileSync(output, 'utf8').trim());
    expect(sent.events[0].scope).toEqual(expect.objectContaining({ larkAppId: 'app-bot-a', botId: 'bot-a', sessionId: 'session', turnId: 'turn-one', workflowRunId: null }));
    await sink.shutdown();
  });

  it.each(['partial', 'duplicate'])('handles %s ACK framing idempotently', async mode => {
    const { sink } = create(mode, { flushMs: 1 });
    sink.enqueue(envelope(mode));
    await waitFor(() => sink.pendingCount() === 0);
    await sink.shutdown();
  });

  it('retains a killed batch and replays it after backoff/restart', async () => {
    const { sink, output } = create('exit-once', { flushMs: 1 });
    sink.enqueue(envelope('restart'));
    await waitFor(() => sink.pendingCount() === 0, 5000);
    const lines = readFileSync(output, 'utf8').trim().split('\n');
    expect(lines.length).toBeGreaterThanOrEqual(2);
    expect(JSON.parse(lines.at(-1)!).events[0].eventId).toBe('restart');
    await sink.shutdown();
  });

  it('spools overflow atomically with private modes and replays in filename order', async () => {
    const { sink, root } = create('no-ack', { maxItems: 1, maxBytes: 1024, batchSize: 1 });
    sink.enqueue(envelope('first'));
    expect(sink.enqueue(envelope('second'))).toBe('spooling');
    await waitFor(() => readdirSync(join(root, 'private', 'spool')).length > 0);
    const botDir = join(root, 'private', 'spool', readdirSync(join(root, 'private', 'spool'))[0]);
    await waitFor(() => readdirSync(botDir).some(name => name.endsWith('.json')));
    expect(readdirSync(botDir).every(name => !name.endsWith('.tmp'))).toBe(true);
    await sink.shutdown(10);
  });

  it('keeps different Bot private spools isolated', async () => {
    const a = create('no-ack', { maxItems: 1, batchSize: 1 });
    const root = a.root;
    const b = new DurablePluginEventSink({ pluginId: 'sink', larkAppId: 'app-b', botId: 'bot-b', runtimeDir: root, privateDir: join(root, 'private'),
      descriptor: { protocolVersion: 1, transport: 'private-stdio', command: [process.execPath, fixture], queue: { maxItems: 1, batchSize: 1 }, env: { FAKE_SINK_MODE: 'no-ack' }, ackTimeoutMs: 100 } });
    a.sink.enqueue(envelope('a1', 'a')); a.sink.enqueue(envelope('a2', 'a'));
    b.enqueue(envelope('b1', 'b')); b.enqueue(envelope('b2', 'b'));
    await waitFor(() => readdirSync(join(root, 'private', 'spool')).length === 2);
    await Promise.all([a.sink.shutdown(10), b.shutdown(10)]);
  });
});

describe('PluginEventSinkManager trust boundary', () => {
  it('drops stale generations and does not expand an absent exact bot binding', () => {
    const manager = new PluginEventSinkManager();
    const readSpy = vi.spyOn(manager as any, 'emitTurnTerminal');
    const ds = { workerGeneration: 2, session: { sessionId: 's' } } as any;
    manager.emitTurnTerminal({ larkAppId: 'app', name: 'bot', plugins: [] } as any, ds,
      { type: 'turn_terminal', sessionId: 's', turnId: 't', status: 'completed' } as any, { workerGeneration: 1 });
    expect(readSpy).toHaveBeenCalledOnce();
    expect((manager as any).sinks.size).toBe(0);
  });
});
