import { assertValidPluginId, isValidPluginId } from './ids.js';
import type {
  BotmuxPluginManifest,
  InstalledPluginRecord,
  PluginPackageManifest,
  PluginProtocolCompatibility,
  PluginServiceConfig,
} from './types.js';

const PACKAGE_VERSION_RE = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
function asRecord(value: unknown, field: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`invalid_${field}`);
  return value as Record<string, unknown>;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

function readDependencies(raw: unknown): BotmuxPluginManifest['dependencies'] {
  if (raw === undefined) return undefined;
  const record = asRecord(raw, 'botmux_dependencies');
  const out: BotmuxPluginManifest['dependencies'] = {};
  if (record.plugins !== undefined) {
    if (!Array.isArray(record.plugins)) throw new Error('invalid_botmux_dependencies_plugins');
    const deps: string[] = [];
    for (const id of record.plugins) {
      if (!isValidPluginId(id)) throw new Error(`invalid_plugin_dependency:${id}`);
      if (!deps.includes(id)) deps.push(id);
    }
    if (deps.length > 0) out.plugins = deps;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

function readCompatibility(raw: unknown): PluginProtocolCompatibility | undefined {
  if (raw === undefined) return undefined;
  const record = asRecord(raw, 'botmux_compatibility');
  if (record.protocolVersion !== 1) throw new Error('unsupported_plugin_protocol_version');
  const declared = asRecord(record.contributions, 'botmux_compatibility_contributions');
  const contributions: PluginProtocolCompatibility['contributions'] = {};
  for (const key of ['eventSink', 'dashboardApi', 'workflowExecutor'] as const) {
    if (declared[key] === undefined) continue;
    if (declared[key] !== 1) throw new Error(`unsupported_plugin_${key}_protocol`);
    contributions[key] = 1;
  }
  if (Object.keys(contributions).length === 0) throw new Error('plugin_compatibility_contributions_required');
  return { protocolVersion: 1, contributions };
}

function readService(raw: unknown): PluginServiceConfig | undefined {
  if (raw === undefined) return undefined;
  const record = asRecord(raw, 'botmux_service');
  const mode = record.mode === undefined ? 'manual' : record.mode;
  if (mode !== 'manual' && mode !== 'auto') throw new Error('invalid_plugin_service_mode');
  return { mode };
}

export function parseBotmuxManifest(raw: unknown): BotmuxPluginManifest {
  const record = asRecord(raw, 'botmux_manifest');
  const id = assertValidPluginId(record.id, 'botmux_plugin_id');
  const displayName = optionalString(record.displayName);
  const dependencies = readDependencies(record.dependencies);
  const service = readService(record.service);
  const compatibility = readCompatibility(record.compatibility);
  return {
    schemaVersion: 1,
    id,
    ...(displayName ? { displayName } : {}),
    ...(dependencies ? { dependencies } : {}),
    ...(service ? { service } : {}),
    ...(compatibility ? { compatibility } : {}),
  };
}

export function assertPluginProtocolCompatibility(record: InstalledPluginRecord): void {
  const compatibility = record.manifest.compatibility;
  const contributions = record.contributions;
  const checks = [
    ['eventSink', contributions?.eventSink, compatibility?.contributions.eventSink],
    ['dashboardApi', contributions?.dashboardApi, compatibility?.contributions.dashboardApi],
    ['workflowExecutor', contributions?.workflowExecutor, compatibility?.contributions.workflowExecutor],
  ] as const;
  for (const [name, contribution, declared] of checks) {
    if (!contribution && !declared) continue; // manifest v1 legacy plugin
    if (!compatibility || compatibility.protocolVersion !== 1 || declared !== 1 || contribution?.protocolVersion !== 1) {
      throw new Error(`plugin_protocol_incompatible:${record.id}:${name}`);
    }
  }
}

export function parsePluginPackageManifest(raw: unknown): PluginPackageManifest {
  const record = asRecord(raw, 'package_json');
  const name = optionalString(record.name);
  const version = optionalString(record.version);
  if (!name) throw new Error('plugin_package_missing_name');
  if (!version) throw new Error('plugin_package_missing_version');
  if (!PACKAGE_VERSION_RE.test(version)) throw new Error('invalid_plugin_package_version');
  const botmux = parseBotmuxManifest(record.botmux);
  const keywords = Array.isArray(record.keywords)
    ? record.keywords.filter((value): value is string => typeof value === 'string')
    : undefined;
  if (!keywords?.includes('botmux-plugin')) throw new Error('plugin_package_missing_keyword');
  return {
    name,
    version,
    ...(typeof record.type === 'string' ? { type: record.type } : {}),
    keywords,
    botmux,
  };
}
