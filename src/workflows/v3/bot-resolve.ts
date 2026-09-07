/**
 * Shared v3 bot resolution — selector → BotConfig → BotSnapshot.
 *
 * The daemon-driven run path (`daemon-run.ts`) and the standalone CLI path
 * (`cli-run.ts`) both turn a `--bot`/node.bot selector into the `BotSnapshot`
 * the runtime freezes per node.  This module is the single source of that
 * matching + working-dir fallback so the two entries can't drift.
 *
 * (cli-run.ts currently keeps equivalent local copies — codex's runtime file,
 * which this feature doesn't touch; dedupe it to this module next time that
 * file is edited.)
 */

import { effectiveDefaultWorkingDir, type BotConfig } from '../../bot-registry.js';
import { executionSelector, isGoalNode, isLoopNode, type V3Dag } from './dag.js';
import type { WorkflowExecutionProfile } from './execution-profile-store.js';
import {
  V3_SUPPORTED_CLIS,
  isV3SupportedCli,
  type BotSnapshot,
} from './contract.js';

export function resolveBotConfig(selector: string | undefined, bots: BotConfig[]): BotConfig {
  if (!selector) {
    if (bots.length === 0) throw new Error('v3: bots.json has no bots — run `botmux setup` first');
    return bots[0]!;
  }
  // Stable identity always wins over a display-name collision. Saved Workflow
  // templates persist larkAppId; a bot named like another bot's app id must
  // never hijack that selector.
  const match = bots.find((b) => b.larkAppId === selector)
    ?? bots.find((b) => b.name === selector);
  if (!match) {
    const known = bots.map((b) => b.name ?? b.larkAppId).join(', ') || '(none)';
    throw new Error(`v3: no bot matches "${selector}" (known: ${known})`);
  }
  return match;
}

/** The configured working dir for a bot, before `~` expansion (the pool
 *  expands).  An explicit override wins over the bot's configured value. */
export function botWorkingDir(bot: BotConfig, override?: string): string {
  return override
    ?? effectiveDefaultWorkingDir(bot)
    ?? bot.workingDir
    ?? bot.workingDirs?.[0]
    ?? '~';
}

/** Freeze the three-tier sandboxPaths into a snapshot-safe copy, or undefined
 *  when the bot has no non-empty tier (so the field is omitted from the frozen
 *  snapshot and the worker's legacy fallback stays intact). */
function sandboxPathsSnapshot(
  sp: { readWrite?: string[]; readOnly?: string[]; deny?: string[] } | undefined,
): { readWrite?: string[]; readOnly?: string[]; deny?: string[] } | undefined {
  if (!sp) return undefined;
  const out: { readWrite?: string[]; readOnly?: string[]; deny?: string[] } = {};
  if (sp.readWrite?.length) out.readWrite = [...sp.readWrite];
  if (sp.readOnly?.length) out.readOnly = [...sp.readOnly];
  if (sp.deny?.length) out.deny = [...sp.deny];
  return Object.keys(out).length ? out : undefined;
}

/** BotConfig → BotSnapshot (the runtime-frozen, secret-free per-node identity). */
export function botToSnapshot(bot: BotConfig, workingDirOverride?: string): BotSnapshot {
  if (bot.disableCliBypass === true) {
    throw new Error(
      `v3 workflow requires CLI bypass permissions, but bot "${bot.name ?? bot.larkAppId}" ` +
      'has disableCliBypass=true. Use a workflow bot with bypass enabled, or remove/set this bot option to false',
    );
  }
  return {
    larkAppId: bot.larkAppId,
    cliId: bot.cliId,
    ...(bot.cliPathOverride ? { cliPathOverride: bot.cliPathOverride } : {}),
    ...(bot.model ? { model: bot.model } : {}),
    ...(bot.sandbox === true ? { sandbox: true } : {}),
    ...(sandboxPathsSnapshot(bot.sandboxPaths) ? { sandboxPaths: sandboxPathsSnapshot(bot.sandboxPaths)! } : {}),
    ...(bot.sandboxHidePaths?.length ? { sandboxHidePaths: [...bot.sandboxHidePaths] } : {}),
    ...(bot.sandboxReadonlyPaths?.length ? { sandboxReadonlyPaths: [...bot.sandboxReadonlyPaths] } : {}),
    ...(bot.sandboxNetwork === false ? { sandboxNetwork: false } : {}),
    workingDir: botWorkingDir(bot, workingDirOverride),
  };
}

/** Direct CLI profile → legacy-shaped execution snapshot. The synthetic
 * profile identity is intentionally not a Lark app id; EphemeralPool runs it
 * apiOnly and never resolves or injects a Feishu secret. */
export function executionProfileToSnapshot(profile: WorkflowExecutionProfile): BotSnapshot {
  if (!profile.enabled) throw new Error(`v3 workflow execution profile "${profile.profileId}" is disabled`);
  if (!isV3SupportedCli(profile.cli)) throw new Error(`v3 workflow execution profile "${profile.profileId}" uses unsupported CLI "${profile.cli}"`);
  return {
    larkAppId: `profile:${profile.profileId}`,
    executionProfileId: profile.profileId,
    directCli: true,
    cliId: profile.cli,
    ...(profile.provider ? { provider: profile.provider } : {}),
    ...(profile.model ? { model: profile.model } : {}),
    workingDir: profile.workingDir,
    sandbox: profile.sandbox.enabled,
    sandboxNetwork: profile.sandbox.network,
    sandboxPaths: {
      ...(profile.sandbox.readWrite.length ? { readWrite: [...profile.sandbox.readWrite] } : {}),
      ...(profile.sandbox.readOnly.length ? { readOnly: [...profile.sandbox.readOnly] } : {}),
      ...(profile.sandbox.deny.length ? { deny: [...profile.sandbox.deny] } : {}),
    },
    timeoutDefaultSec: profile.timeoutPolicy.defaultSec,
    costTier: profile.costTier,
    timeoutMaxSec: profile.timeoutPolicy.maxSec,
    envAllowlist: [...profile.envPolicy.allow],
    envDenylist: [...profile.envPolicy.deny],
  };
}

/** Freeze all execution selectors. New executionProfile selectors resolve from
 * the independent catalog; legacy bot selectors keep their historical path. */
export function freezeDagExecutionSnapshots(
  dag: V3Dag,
  bots: BotConfig[],
  profiles: readonly WorkflowExecutionProfile[],
  opts: { defaultSelector?: string; workingDirOverride?: string } = {},
): Map<string, BotSnapshot> {
  const profileById = new Map(profiles.map(profile => [profile.profileId, profile]));
  const snapshots = new Map<string, BotSnapshot>();
  const freeze = (selector: string | undefined): void => {
    const key = selector ?? '';
    if (snapshots.has(key)) return;
    const profile = selector ? profileById.get(selector) : undefined;
    snapshots.set(key, profile
      ? executionProfileToSnapshot(profile)
      : botToSnapshot(resolveBotConfig(selector ?? opts.defaultSelector, bots), opts.workingDirOverride));
  };
  for (const node of dag.nodes) {
    if (isGoalNode(node)) freeze(executionSelector(node));
    if (isLoopNode(node)) for (const body of node.body.nodes) freeze(executionSelector(body, executionSelector(node)));
  }
  return snapshots;
}

/**
 * Resolve every selector used by a DAG exactly once. Keys intentionally mirror
 * the runtime contract (`''` means the DAG's default bot); loop-body nodes use
 * their own selector or inherit the loop selector.
 */
export function freezeDagBotSnapshots(
  dag: V3Dag,
  bots: BotConfig[],
  opts: { defaultSelector?: string; workingDirOverride?: string; executionProfiles?: readonly WorkflowExecutionProfile[] } = {},
): Map<string, BotSnapshot> {
  const snapshots = new Map<string, BotSnapshot>();
  const profileById = new Map((opts.executionProfiles ?? []).map(profile => [profile.profileId, profile]));
  const freeze = (selector: string | undefined): void => {
    const key = selector ?? '';
    if (snapshots.has(key)) return;
    const directProfile = selector ? profileById.get(selector) : undefined;
    const bot = directProfile ? undefined : resolveBotConfig(selector ?? opts.defaultSelector, bots);
    const snapshot = directProfile ? executionProfileToSnapshot(directProfile) : botToSnapshot(bot!, opts.workingDirOverride);
    if (!isV3SupportedCli(snapshot.cliId)) {
      throw new Error(
        `v3 workflow executor "${directProfile?.displayName ?? bot?.name ?? bot?.larkAppId}" uses unsupported CLI "${snapshot.cliId}" ` +
        `(supported: ${V3_SUPPORTED_CLIS.join(', ')})`,
      );
    }
    snapshots.set(key, snapshot);
  };

  for (const node of dag.nodes) {
    if (isGoalNode(node)) freeze(executionSelector(node));
    if (isLoopNode(node)) {
      for (const bodyNode of node.body.nodes) freeze(executionSelector(bodyNode, executionSelector(node)));
    }
  }
  return snapshots;
}

/** JSON-safe persisted shape used by `bots.snapshot.json`. */
export function serializeFrozenBotSnapshots(
  snapshots: ReadonlyMap<string, BotSnapshot>,
): Record<string, BotSnapshot> {
  return Object.fromEntries(snapshots);
}

/**
 * Validate the secret-free pinned bot snapshot artifact before a retry/resume
 * can hand it to the runtime. Unknown keys fail closed so credentials or
 * mutable permission flags cannot quietly creep into this run contract.
 */
export function parseFrozenBotSnapshots(raw: unknown, dag?: V3Dag): Map<string, BotSnapshot> {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new Error('bots.snapshot.json must be an object keyed by DAG bot selector');
  }
  const allowed = new Set([
    'larkAppId',
    'cliId',
    'executionProfileId',
    'directCli',
    'cliPathOverride',
    'provider',
    'model',
    'sandbox',
    'sandboxPaths',
    'sandboxHidePaths',
    'sandboxReadonlyPaths',
    'sandboxNetwork',
    'workingDir',
    'timeoutDefaultSec',
    'timeoutMaxSec',
    'costTier',
    'envAllowlist',
    'envDenylist',
  ]);
  const snapshots = new Map<string, BotSnapshot>();
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw new Error(`bots.snapshot.json[${JSON.stringify(key)}] must be an object`);
    }
    const obj = value as Record<string, unknown>;
    const extra = Object.keys(obj).filter((field) => !allowed.has(field));
    if (extra.length > 0) {
      throw new Error(`bots.snapshot.json[${JSON.stringify(key)}] has unsupported key(s): ${extra.join(', ')}`);
    }
    if (typeof obj.larkAppId !== 'string' || obj.larkAppId.length === 0) {
      throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].larkAppId must be a non-empty string`);
    }
    if (typeof obj.cliId !== 'string' || !isV3SupportedCli(obj.cliId as BotSnapshot['cliId'])) {
      throw new Error(
        `bots.snapshot.json[${JSON.stringify(key)}].cliId must be one of ${V3_SUPPORTED_CLIS.join(', ')}`,
      );
    }
    if (typeof obj.workingDir !== 'string' || obj.workingDir.length === 0) {
      throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].workingDir must be a non-empty string`);
    }
    for (const field of ['cliPathOverride', 'provider', 'model', 'executionProfileId', 'costTier'] as const) {
      if (obj[field] !== undefined && typeof obj[field] !== 'string') {
        throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].${field} must be a string`);
      }
    }
    for (const field of ['sandbox', 'sandboxNetwork', 'directCli'] as const) {
      if (obj[field] !== undefined && typeof obj[field] !== 'boolean') {
        throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].${field} must be a boolean`);
      }
    }
    for (const field of ['sandboxHidePaths', 'sandboxReadonlyPaths', 'envAllowlist', 'envDenylist'] as const) {
      if (
        obj[field] !== undefined &&
        (!Array.isArray(obj[field]) || !(obj[field] as unknown[]).every((item) => typeof item === 'string'))
      ) {
        throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].${field} must be a string array`);
      }
    }
    for (const field of ['timeoutDefaultSec', 'timeoutMaxSec'] as const) {
      if (obj[field] !== undefined && (!Number.isInteger(obj[field]) || Number(obj[field]) < 1)) {
        throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].${field} must be a positive integer`);
      }
    }
    if (obj.sandboxPaths !== undefined) {
      const sp = obj.sandboxPaths;
      if (!sp || typeof sp !== 'object' || Array.isArray(sp)) {
        throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].sandboxPaths must be an object`);
      }
      const spObj = sp as Record<string, unknown>;
      const spExtra = Object.keys(spObj).filter((f) => !['readWrite', 'readOnly', 'deny'].includes(f));
      if (spExtra.length > 0) {
        throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].sandboxPaths has unsupported key(s): ${spExtra.join(', ')}`);
      }
      for (const tier of ['readWrite', 'readOnly', 'deny'] as const) {
        if (
          spObj[tier] !== undefined &&
          (!Array.isArray(spObj[tier]) || !(spObj[tier] as unknown[]).every((item) => typeof item === 'string'))
        ) {
          throw new Error(`bots.snapshot.json[${JSON.stringify(key)}].sandboxPaths.${tier} must be a string array`);
        }
      }
    }
    const parsedSandboxPaths = sandboxPathsSnapshot(obj.sandboxPaths as BotSnapshot['sandboxPaths']);
    snapshots.set(key, {
      larkAppId: obj.larkAppId,
      cliId: obj.cliId as BotSnapshot['cliId'],
      ...(obj.cliPathOverride !== undefined ? { cliPathOverride: obj.cliPathOverride as string } : {}),
      ...(obj.executionProfileId !== undefined ? { executionProfileId: obj.executionProfileId as string } : {}),
      ...(obj.directCli !== undefined ? { directCli: obj.directCli as boolean } : {}),
      ...(obj.provider !== undefined ? { provider: obj.provider as string } : {}),
      ...(obj.model !== undefined ? { model: obj.model as string } : {}),
      ...(obj.sandbox !== undefined ? { sandbox: obj.sandbox as boolean } : {}),
      ...(parsedSandboxPaths ? { sandboxPaths: parsedSandboxPaths } : {}),
      ...(obj.sandboxHidePaths !== undefined ? { sandboxHidePaths: [...obj.sandboxHidePaths as string[]] } : {}),
      ...(obj.sandboxReadonlyPaths !== undefined ? { sandboxReadonlyPaths: [...obj.sandboxReadonlyPaths as string[]] } : {}),
      ...(obj.sandboxNetwork !== undefined ? { sandboxNetwork: obj.sandboxNetwork as boolean } : {}),
      workingDir: obj.workingDir,
      ...(typeof obj.timeoutDefaultSec === 'number' ? { timeoutDefaultSec: obj.timeoutDefaultSec } : {}),
      ...(obj.costTier === 'low' || obj.costTier === 'medium' || obj.costTier === 'high' ? { costTier: obj.costTier } : {}),
      ...(typeof obj.timeoutMaxSec === 'number' ? { timeoutMaxSec: obj.timeoutMaxSec } : {}),
      ...(Array.isArray(obj.envAllowlist) ? { envAllowlist: [...obj.envAllowlist as string[]] } : {}),
      ...(Array.isArray(obj.envDenylist) ? { envDenylist: [...obj.envDenylist as string[]] } : {}),
    });
  }

  if (dag) {
    const required = new Set<string>();
    for (const node of dag.nodes) {
      if (isGoalNode(node)) required.add(executionSelector(node) ?? '');
      if (isLoopNode(node)) {
        for (const bodyNode of node.body.nodes) required.add(executionSelector(bodyNode, executionSelector(node)) ?? '');
      }
    }
    for (const key of required) {
      if (!snapshots.has(key)) {
        throw new Error(`bots.snapshot.json is missing selector ${JSON.stringify(key || '<default>')}`);
      }
    }
  }
  return snapshots;
}
