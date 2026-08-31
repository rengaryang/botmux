import type { CliId } from '../adapters/cli/types.js';

export type InteractiveCommandChoiceSource = 'modelCatalog';
export type InteractiveCommandSessionMode = 'raw_input' | 'unsupported';

export interface InteractiveCommandCapability {
  readonly command: '/model';
  readonly choiceSource: InteractiveCommandChoiceSource;
  readonly sessionMode: InteractiveCommandSessionMode;
  readonly requiresIdle: boolean;
  buildInvocation(value: string): string;
}

const STRUCTURED_RUNNER_CLI_IDS = new Set<string>(['codex-app', 'mira', 'mir', 'dsh']);

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
  return {
    command: '/model',
    choiceSource: 'modelCatalog',
    sessionMode: STRUCTURED_RUNNER_CLI_IDS.has(cliId) ? 'unsupported' : 'raw_input',
    requiresIdle: true,
    buildInvocation(value: string): string {
      return `/model ${safeSlashArgument(value)}`;
    },
  };
}
