import type { CliId } from '../adapters/cli/types.js';

export type InteractiveCommandChoiceSource = 'modelCatalog';
export type InteractiveCommandSessionMode = 'raw_input' | 'unsupported';

export interface InteractiveCommandCapability {
  readonly command: '/model';
  readonly choiceSource: InteractiveCommandChoiceSource;
  readonly sessionMode: InteractiveCommandSessionMode;
  readonly requiresIdle: boolean;
  /** Card-driven model selection is a complete TUI transaction. Terminal CLIs
   * may show argument autocomplete: Enter #1 accepts the exact catalogue item,
   * Enter #2 submits it. This is deliberately scoped to the trusted card action;
   * ordinary `/model <name>` passthrough remains a single submit. */
  readonly submitCount: 1 | 2;
  readonly submitIntervalMs?: number;
  buildInvocation(value: string): string;
}

const STRUCTURED_RUNNER_CLI_IDS = new Set<string>(['codex-app', 'mira', 'mir', 'dsh']);
const REMOTE_RUNNER_CLI_IDS = new Set<string>(['riff', 'mojo']);

/** Native terminal CLIs whose `/model <value>` path may own an autocomplete
 * surface. Keep this explicit: a newly added CLI must opt in after its input
 * protocol is reviewed instead of inheriting terminal keystrokes by accident. */
const TERMINAL_MODEL_PICKER_CLI_IDS = new Set<string>([
  'claude-code', 'seed', 'relay', 'aiden', 'coco', 'codex', 'cursor', 'gemini',
  'genius', 'opencode', 'opencode2', 'antigravity', 'mtr', 'hermes', 'traex',
  'pi', 'copilot', 'oh-my-pi', 'kimi', 'grok', 'kiro-cli', 'reasonix', 'dsh-tui',
]);

function safeSlashArgument(value: string): string {
  const normalized = value.trim();
  if (!normalized || /[\r\n\0]/.test(normalized)) {
    throw new Error('invalid interactive command value');
  }
  return normalized;
}

/**
 * Adapter-independent interactive command capability registry.
 *
 * `/model` is the first consumer. Future `/effort`, `/fast`, `/mcp`, and
 * `/plugin` pickers should be added here rather than hard-coded into Lark's
 * event router. Structured runners deliberately fail closed: their stdin is a
 * framed protocol and must never receive terminal slash-command bytes.
 */
export function interactiveModelCapability(cliId: CliId | string): InteractiveCommandCapability {
  const terminalPicker = TERMINAL_MODEL_PICKER_CLI_IDS.has(cliId)
    && !STRUCTURED_RUNNER_CLI_IDS.has(cliId)
    && !REMOTE_RUNNER_CLI_IDS.has(cliId);
  return {
    command: '/model',
    choiceSource: 'modelCatalog',
    sessionMode: terminalPicker ? 'raw_input' : 'unsupported',
    requiresIdle: true,
    submitCount: terminalPicker ? 2 : 1,
    submitIntervalMs: terminalPicker ? 300 : undefined,
    buildInvocation(value: string): string {
      return `/model ${safeSlashArgument(value)}`;
    },
  };
}
