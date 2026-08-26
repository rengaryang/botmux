import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { tmpdir } from 'node:os';
import type { CliAdapter } from '../src/adapters/cli/types.js';
import { installLocalPlugin } from '../src/core/plugins/install.js';
import { prepareCliPluginGeneration } from '../src/core/plugins/cli-generation.js';
import {
  readSessionMcpRuntimeManifest,
  sessionMcpRuntimeHostOnlyPaths,
  sessionMcpRuntimeManifestPath,
} from '../src/core/plugins/mcp/session-runtime.js';
import { pluginMcpPrivatePath } from '../src/core/plugins/paths.js';
import { readSessionPluginManifest } from '../src/core/plugins/session-manifest.js';
import { readSessionSkillManifest } from '../src/core/skills/manifest-store.js';
import {
  __testOnly_closeObservationStores,
  __testOnly_reopenObservationAdmission,
  drainObservationQueue,
} from '../src/services/km/observation-queue.js';
import { ObservationStore } from '../src/services/km/observation-store.js';

function write(file: string, content: string): void {
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, content);
}

describe('CLI plugin generation', () => {
  let home: string;
  let dataDir: string;

  beforeEach(() => {
    home = mkdtempSync(join(tmpdir(), 'botmux-plugin-generation-'));
    dataDir = join(home, '.botmux', 'data');
    vi.stubEnv('HOME', home);
    vi.stubEnv('SESSION_DATA_DIR', dataDir);
  });

  afterEach(async () => {
    await drainObservationQueue(3_000);
    await __testOnly_closeObservationStores();
    __testOnly_reopenObservationAdmission();
    vi.unstubAllEnvs();
    rmSync(home, { recursive: true, force: true });
  });

  it('replaces Skills and MCP plugin bindings when the same session starts a new CLI process', () => {
    const source = join(home, 'demo-source');
    write(join(source, 'package.json'), JSON.stringify({
      name: '@botmux-ai/plugin-demo',
      version: '0.1.0',
      keywords: ['botmux-plugin'],
      botmux: { schemaVersion: 1, id: 'demo' },
    }));
    write(join(source, 'dist', 'skills', 'browser', 'SKILL.md'), [
      '---',
      'name: browser',
      'description: Browser tools',
      '---',
      '# Browser',
    ].join('\n'));
    write(join(source, 'dist', 'mcp', 'index.json'), JSON.stringify({
      transport: 'stdio',
      command: ['./mcp/server.mjs'],
    }));
    write(join(source, 'dist', 'mcp', 'server.mjs'), 'process.exit(0);\n');
    installLocalPlugin(source);
    const adapter = { id: 'codex' } as CliAdapter;

    const first = prepareCliPluginGeneration({
      sessionId: 'same-session',
      bot: { larkAppId: 'app-1', plugins: ['demo'] },
      global: { plugins: [] },
      dataDir,
      cliId: 'codex',
      adapter,
      workingDir: '/repo',
      prompt: 'first turn',
      replacesPriorGeneration: false,
      now: () => '2026-07-12T00:00:00.000Z',
    });
    expect(first.pluginManifest.pluginIds).toEqual(['demo']);
    expect(first.prompt).toContain('botmux skill show browser');
    expect(first.skillCatalog).toContain('botmux skill show browser');
    expect(readSessionSkillManifest('same-session')?.prioritySkills.map(skill => skill.name)).toEqual(['browser']);
    const firstMcpRuntime = readSessionMcpRuntimeManifest('same-session', dataDir);
    expect(firstMcpRuntime).toMatchObject({
      sessionId: 'same-session',
      pluginIds: ['demo'],
      entries: [{
        pluginId: 'demo',
        server: { transport: 'stdio', command: ['./mcp/server.mjs'] },
      }],
    });
    if (!firstMcpRuntime) throw new Error('expected session MCP runtime manifest');
    expect(sessionMcpRuntimeHostOnlyPaths(firstMcpRuntime, dataDir)).toEqual([
      sessionMcpRuntimeManifestPath('same-session', dataDir),
      pluginMcpPrivatePath('demo'),
      join(home, '.botmux', 'plugins', 'demo', 'dist', 'mcp', 'index.json'),
    ]);
    expect('mcpReadonlyRoots' in first).toBe(false);
    expect('mcpHidePaths' in first).toBe(false);

    const refreshed = prepareCliPluginGeneration({
      sessionId: 'same-session',
      bot: { larkAppId: 'app-1', plugins: [] },
      global: { plugins: [] },
      dataDir,
      cliId: 'codex',
      adapter,
      workingDir: '/repo',
      prompt: 'after restart',
      replacesPriorGeneration: true,
      now: () => '2026-07-12T01:00:00.000Z',
    });
    expect(refreshed.pluginManifest.pluginIds).toEqual([]);
    expect(refreshed.prompt).toContain('<botmux_skills_refresh>');
    expect(refreshed.skillCatalog).toContain('<botmux_skills_refresh>');
    expect(refreshed.prompt).toContain('Skills not listed here are no longer available');
    expect(readSessionPluginManifest('same-session', dataDir)?.pluginIds).toEqual([]);
    expect(readSessionSkillManifest('same-session')).toBeNull();
    expect(readSessionMcpRuntimeManifest('same-session', dataDir)).toMatchObject({
      pluginIds: [],
      entries: [],
    });
    const refreshedMcpRuntime = readSessionMcpRuntimeManifest('same-session', dataDir);
    if (!refreshedMcpRuntime) throw new Error('expected refreshed session MCP runtime manifest');
    expect(sessionMcpRuntimeHostOnlyPaths(refreshedMcpRuntime, dataDir)).toEqual([
      sessionMcpRuntimeManifestPath('same-session', dataDir),
    ]);
  });

  it('emits a redacted skill manifest observation only when the feature flag is enabled', async () => {
    vi.stubEnv('BOTMUX_KM_OBSERVATION_ENABLED', 'true');
    const source = join(home, 'observed-source');
    write(join(source, 'package.json'), JSON.stringify({
      name: '@botmux-ai/plugin-observed', version: '0.1.0', keywords: ['botmux-plugin'],
      botmux: { schemaVersion: 1, id: 'observed' },
    }));
    write(join(source, 'dist', 'skills', 'browser', 'SKILL.md'), [
      '---', 'name: browser', 'description: Browser tools', '---', '# Browser',
    ].join('\n'));
    installLocalPlugin(source);

    prepareCliPluginGeneration({
      sessionId: 'observed-session',
      bot: { larkAppId: 'app-observed', plugins: ['observed'] },
      global: { plugins: [] },
      dataDir,
      cliId: 'codex',
      adapter: { id: 'codex' } as CliAdapter,
      workingDir: '/private/repo',
      prompt: 'hello',
      replacesPriorGeneration: false,
      now: () => '2026-07-12T00:00:00.000Z',
    });
    await drainObservationQueue(3_000);

    const store = await ObservationStore.open(dataDir);
    const events = store.list({ limit: 10, eventType: 'skill.manifest.resolved' });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      identity: { botAppId: 'app-observed', sessionId: 'observed-session' },
      payload: { skills: [{ name: 'browser', sourceType: 'plugin' }] },
    });
    expect(JSON.stringify(events[0])).not.toContain('/private/repo');
    expect(JSON.stringify(events[0])).not.toContain(source);
    store.close();
  });
});
