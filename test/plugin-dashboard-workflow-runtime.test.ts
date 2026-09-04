import { afterEach, describe, expect, it, vi } from 'vitest';
import { assertJsonSchemaValue, assertSafePluginDto } from '../src/core/plugins/json-contract.js';
import { freezePluginWorkflowDispatch } from '../src/core/plugins/workflow-executor.js';

vi.mock('../src/services/plugin-registry-store.js', () => ({
  getInstalledPlugin: vi.fn(() => ({
    id: 'era', version: '1.2.3', packageName: '@botmux-ai/plugin-era', source: { type: 'local', spec: '.' },
    installedAt: '', updatedAt: '',
    manifest: { schemaVersion: 1, id: 'era', compatibility: { protocolVersion: 1, contributions: { workflowExecutor: 1 } } },
    contributions: { workflowExecutor: {
      protocolVersion: 1, mode: 'read-only', privateRef: 'private/workflow-executor.json', outputMaxBytes: 1024,
      inputSchema: { type: 'object', properties: { traceId: { type: 'string' } }, required: ['traceId'], additionalProperties: false },
      outputSchema: { type: 'object', properties: {}, additionalProperties: true },
    } },
  })),
}));
vi.mock('../src/bot-registry.js', () => ({ loadBotConfigs: vi.fn(() => [{ larkAppId: 'bot-a', plugins: ['era'] }, { larkAppId: 'bot-b', plugins: [] }]) }));
vi.mock('../src/global-config.js', () => ({ readGlobalConfig: vi.fn(() => ({ plugins: [] })) }));

const priorHome = process.env.HOME;
afterEach(() => { process.env.HOME = priorHome; vi.restoreAllMocks(); });

describe('plugin public DTO boundary', () => {
  it('rejects secrets and absolute host paths recursively', () => {
    expect(() => assertSafePluginDto({ nested: { api_token: 'x' } })).toThrow(/secret_forbidden/);
    expect(() => assertSafePluginDto({ resultPath: '/etc/passwd' })).toThrow(/absolute_path_forbidden/);
  });

  it('fails closed for schema-invalid and unsupported schema contracts', () => {
    expect(() => assertJsonSchemaValue({ type: 'object', properties: { n: { type: 'integer' } }, required: ['n'], additionalProperties: false }, { n: '1' }))
      .toThrow(/schema_invalid/);
    expect(() => assertJsonSchemaValue({ type: 'object', oneOf: [] }, {})).toThrow(/keyword_unsupported/);
  });
});

describe('plugin workflow dispatch freeze', () => {
  it('pins version, query digest, identity, exact Bot, scopes, and limits', () => {
    const frozen = freezePluginWorkflowDispatch({
      spec: { pluginId: 'era', query: 'trace.get', input: { traceId: 't-1' } },
      principal: { subject: 'user-1', exactBotId: 'bot-a', scopes: ['workflow.read'] }, timeoutMs: 500,
    });
    expect(frozen).toMatchObject({ plugin: { id: 'era', version: '1.2.3', protocolVersion: 1 }, principal: { exactBotId: 'bot-a' }, limits: { timeoutMs: 500, outputBytes: 1024 } });
    expect(frozen.inputDigest).toMatch(/^sha256:[0-9a-f]{64}$/);
    expect(Object.isFrozen(frozen)).toBe(true);
  });

  it('rejects missing identity, cross-Bot dispatch, bad schema and arbitrary SQL', () => {
    expect(() => freezePluginWorkflowDispatch({ spec: { pluginId: 'era', query: 'trace.get', input: { traceId: 'x' } }, principal: { subject: '', exactBotId: 'bot-a', scopes: ['workflow.read'] } })).toThrow(/principal_missing/);
    expect(() => freezePluginWorkflowDispatch({ spec: { pluginId: 'era', query: 'trace.get', input: { traceId: 'x' } }, principal: { subject: 'u', exactBotId: 'bot-b', scopes: ['workflow.read'] } })).toThrow(/cross_bot/);
    expect(() => freezePluginWorkflowDispatch({ spec: { pluginId: 'era', query: 'trace.get', input: {} }, principal: { subject: 'u', exactBotId: 'bot-a', scopes: ['workflow.read'] } })).toThrow(/schema_invalid/);
    expect(() => freezePluginWorkflowDispatch({ spec: { pluginId: 'era', query: 'trace.get', input: { traceId: 'SELECT * FROM secrets' } }, principal: { subject: 'u', exactBotId: 'bot-a', scopes: ['workflow.read'] } })).toThrow(/sql_forbidden/);
  });
});
