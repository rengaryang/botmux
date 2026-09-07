import type { IncomingMessage, ServerResponse } from 'node:http';
import { existsSync, readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { loadBotConfigs } from '../../bot-registry.js';
import { readGlobalConfig } from '../../global-config.js';
import { getInstalledPlugin } from '../../services/plugin-registry-store.js';
import { resolveEffectivePluginIds } from './effective.js';
import { assertJsonSchemaValue, assertSafePluginDto } from './json-contract.js';
import { assertPluginProtocolCompatibility } from './manifest.js';
import { pluginRuntimeDir, resolvePluginPath } from './paths.js';

export interface PluginDashboardPrincipal {
  kind: 'legacy-dashboard' | 'platform-dashboard' | 'feishu-h5';
  subject: string;
  authSessionId: string;
  exactBotIds: readonly string[];
  capabilities: readonly string[];
}

export interface PluginDashboardHandlerContext {
  plugin: Readonly<{ id: string; version: string; protocolVersion: 1 }>;
  principal: Readonly<PluginDashboardPrincipal>;
  request: Readonly<{ method: 'GET' | 'POST'; path: string; query: Record<string, string[]>; body: unknown }>;
}

export type PluginDashboardHandler = (context: PluginDashboardHandlerContext) => unknown | Promise<unknown>;

export type PluginDashboardDispatchResult =
  | { handled: false }
  | { handled: true; status: number; body: unknown };

function response(status: number, body: unknown): PluginDashboardDispatchResult {
  return { handled: true, status, body };
}

function exactEnabledBotIds(pluginId: string): string[] {
  const global = readGlobalConfig().plugins ?? [];
  let bots: ReturnType<typeof loadBotConfigs> = [];
  try { bots = loadBotConfigs(); } catch { return []; }
  return bots
    .filter(bot => resolveEffectivePluginIds(bot, { plugins: global }).includes(pluginId))
    .map(bot => bot.larkAppId);
}

async function readBoundedJson(req: IncomingMessage, maxBytes: number): Promise<unknown> {
  const declared = Number(req.headers['content-length'] ?? 0);
  if (Number.isFinite(declared) && declared > maxBytes) throw new Error('plugin_request_too_large');
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > maxBytes) throw new Error('plugin_request_too_large');
    chunks.push(buffer);
  }
  if (bytes === 0) return {};
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { throw new Error('plugin_request_invalid_json'); }
}

function queryDto(url: URL): Record<string, string[]> {
  const out: Record<string, string[]> = {};
  for (const key of new Set(url.searchParams.keys())) out[key] = url.searchParams.getAll(key);
  return out;
}

/** Dispatch after the Dashboard's normal authentication and CSRF gates. The
 * plugin receives a DTO only: no request object, response object, cookie or
 * management token crosses this boundary. */
export async function dispatchPluginDashboardApi(input: {
  req: IncomingMessage;
  url: URL;
  identity: { kind: PluginDashboardPrincipal['kind']; userId: string; authSessionId: string } | null;
  csrfAllowed: () => boolean;
}): Promise<PluginDashboardDispatchResult> {
  const match = input.url.pathname.match(/^\/api\/plugins\/([^/]+)\/(.+)$/);
  if (!match) return { handled: false };
  let pluginId: string;
  try { pluginId = decodeURIComponent(match[1]); } catch { return response(400, { ok: false, error: 'invalid_plugin_id' }); }
  const record = getInstalledPlugin(pluginId);
  const contribution = record?.contributions?.dashboardApi;
  if (!record || !contribution) return { handled: false }; // management routes keep ownership of their namespace
  try { assertPluginProtocolCompatibility(record); } catch { return response(409, { ok: false, error: 'plugin_protocol_incompatible' }); }
  if (!input.identity) return response(401, { ok: false, error: 'authentication_required' });
  const method = (input.req.method ?? 'GET').toUpperCase();
  const route = contribution.routes.find(candidate => candidate.method === method && candidate.path === input.url.pathname);
  if (!route) return response(405, { ok: false, error: 'plugin_route_not_allowed' });
  if (method === 'POST' && !input.csrfAllowed()) return response(403, { ok: false, error: 'control_csrf_invalid' });
  const exactBotIds = exactEnabledBotIds(pluginId);
  if (exactBotIds.length === 0) return response(403, { ok: false, error: 'plugin_not_enabled' });
  const runtime = pluginRuntimeDir(pluginId);
  const entry = resolvePluginPath(runtime, 'dashboard-api/index.js', 'dashboard_api_entry');
  if (!existsSync(entry)) return response(503, { ok: false, error: 'plugin_dashboard_handler_missing' });
  let body: unknown = {};
  try {
    body = method === 'POST' ? await readBoundedJson(input.req, contribution.limits.requestBytes) : {};
    assertSafePluginDto(body);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return response(message === 'plugin_request_too_large' ? 413 : 400, { ok: false, error: message });
  }
  // Optional requestSchema/responseSchema are private descriptor details and
  // never become public registry metadata.
  const descriptor = JSON.parse(readFileSync(resolvePluginPath(runtime, 'dashboard-api/index.json'), 'utf8')) as Record<string, unknown>;
  try {
    if (descriptor.requestSchema && typeof descriptor.requestSchema === 'object') assertJsonSchemaValue(descriptor.requestSchema as Record<string, unknown>, body);
    const mod = await import(`${pathToFileURL(entry).href}?v=${encodeURIComponent(record.version)}`);
    const handler = (mod.default ?? mod.handle) as PluginDashboardHandler | undefined;
    if (typeof handler !== 'function') throw new Error('plugin_dashboard_handler_invalid');
    const principal: PluginDashboardPrincipal = Object.freeze({
      kind: input.identity.kind,
      subject: input.identity.userId,
      authSessionId: input.identity.authSessionId,
      exactBotIds: Object.freeze(exactBotIds),
      capabilities: Object.freeze([contribution.capability]),
    });
    const output = await handler(Object.freeze({
      plugin: Object.freeze({ id: record.id, version: record.version, protocolVersion: 1 as const }),
      principal,
      request: Object.freeze({ method: method as 'GET' | 'POST', path: input.url.pathname, query: queryDto(input.url), body }),
    }));
    assertSafePluginDto(output);
    if (descriptor.responseSchema && typeof descriptor.responseSchema === 'object') assertJsonSchemaValue(descriptor.responseSchema as Record<string, unknown>, output);
    if (Buffer.byteLength(JSON.stringify(output), 'utf8') > contribution.limits.responseBytes) {
      return response(502, { ok: false, error: 'plugin_response_too_large' });
    }
    return response(200, output);
  } catch (error) {
    return response(502, { ok: false, error: error instanceof Error ? error.message : String(error) });
  }
}

export function sendPluginDashboardResult(res: ServerResponse, result: Exclude<PluginDashboardDispatchResult, { handled: false }>): void {
  res.writeHead(result.status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(JSON.stringify(result.body));
}
