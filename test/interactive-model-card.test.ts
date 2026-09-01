import { beforeEach, describe, expect, it } from 'vitest';
import { interactiveModelCapability } from '../src/services/interactive-cli-commands.js';
import {
  __testOnlyResetModelPickerBindings,
  claimModelPickerBinding,
  issueModelPickerBinding,
  MODEL_PICKER_TTL_MS,
} from '../src/services/model-picker-state.js';
import {
  buildModelPickerCard,
  filterModelChoices,
  MODEL_PICKER_PAGE_SIZE,
} from '../src/im/lark/model-picker-card.js';
import { stampBotmuxCallbackMarkers } from '../src/im/lark/callback-button-marker.js';
import type { ModelChoiceDetail } from '../src/services/model-catalog.js';

const binding = {
  larkAppId: 'cli_app', rootId: 'om_root', sessionId: 'sess-1', cliId: 'pi', invokerOpenId: 'ou_owner',
};

const choices: ModelChoiceDetail[] = [
  { value: 'provider-a/same', provider: 'provider-a', model: 'same', label: 'same · provider-a' },
  { value: 'provider-b/same', provider: 'provider-b', model: 'same', label: 'same · provider-b' },
  ...Array.from({ length: MODEL_PICKER_PAGE_SIZE }, (_, i) => ({
    value: `provider-a/model-${i}`,
    provider: 'provider-a',
    model: `model-${i}`,
    label: `model-${i} · provider-a`,
  })),
];

describe('interactive CLI command capability', () => {
  it('builds an exact native /model invocation for terminal CLIs', () => {
    const capability = interactiveModelCapability('pi');
    expect(capability.sessionMode).toBe('raw_input');
    expect(capability.requiresIdle).toBe(true);
    expect(capability.buildInvocation('provider-a/same')).toBe('/model provider-a/same');
  });

  it.each(['codex-app', 'mira', 'mir', 'dsh'])('fails closed for structured runner %s', cliId => {
    expect(interactiveModelCapability(cliId).sessionMode).toBe('unsupported');
  });

  it('rejects values that could inject a second terminal command', () => {
    expect(() => interactiveModelCapability('pi').buildInvocation('model-a\n/clear')).toThrow();
  });
});

describe('model picker server binding', () => {
  beforeEach(() => __testOnlyResetModelPickerBindings());

  it('is one-shot and expires', () => {
    const nonce = issueModelPickerBinding(binding, 1_000);
    expect(claimModelPickerBinding(nonce, undefined, 1_001)).toEqual(binding);
    expect(claimModelPickerBinding(nonce, undefined, 1_002)).toBeUndefined();

    const protectedNonce = issueModelPickerBinding(binding, 1_500);
    expect(claimModelPickerBinding(protectedNonce, { larkAppId: 'cli_app', invokerOpenId: 'ou_other' }, 1_501)).toBeUndefined();
    expect(claimModelPickerBinding(protectedNonce, { larkAppId: 'cli_app', invokerOpenId: 'ou_owner' }, 1_502)).toEqual(binding);

    const expired = issueModelPickerBinding(binding, 2_000);
    expect(claimModelPickerBinding(expired, undefined, 2_000 + MODEL_PICKER_TTL_MS)).toBeUndefined();
  });
});

describe('model picker card', () => {
  it('keeps provider-qualified duplicates distinct and filters by provider', () => {
    expect(filterModelChoices(choices, { provider: 'provider-b' }).map(c => c.value))
      .toEqual(['provider-b/same']);
    expect(filterModelChoices(choices, { search: 'provider-b' }).map(c => c.value))
      .toEqual(['provider-b/same']);
  });

  it('renders a bound searchable paginated card without persisting defaults', () => {
    const card = JSON.parse(buildModelPickerCard({
      cliName: 'Pi',
      activeModel: 'provider-a/same',
      choices,
      source: 'live',
      binding: { rootId: 'om_root', sessionId: 'sess-1', cliId: 'pi', invokerOpenId: 'ou_owner', nonce: 'nonce-1' },
    }));
    expect(card.schema).toBe('2.0');
    expect(card.header.title.content).toContain('Pi');
    const serialized = JSON.stringify(card);
    expect(serialized).toContain('cli_model_select');
    expect(serialized).toContain('provider-a/same');
    expect(serialized).toContain('nonce-1');
    expect(serialized).toContain('下一页');
  });

  it('uses only Card 2.0 callback structures accepted by Lark', () => {
    const card = JSON.parse(stampBotmuxCallbackMarkers(buildModelPickerCard({
      cliName: 'Pi',
      choices,
      source: 'live',
      binding: { rootId: 'om_root', sessionId: 'sess-1', cliId: 'pi', invokerOpenId: 'ou_owner', nonce: 'nonce-1' },
    })));
    const nodes: any[] = [];
    const visit = (value: unknown): void => {
      if (!value || typeof value !== 'object') return;
      if (Array.isArray(value)) { value.forEach(visit); return; }
      const node = value as Record<string, unknown>;
      if (typeof node.tag === 'string') nodes.push(node);
      Object.values(node).forEach(visit);
    };
    visit(card.body);

    expect(nodes.some(node => node.tag === 'action')).toBe(false);
    for (const node of nodes.filter(node => ['button', 'select_static'].includes(node.tag))) {
      expect(node.value).toBeUndefined();
      expect(node.behaviors).toEqual(expect.arrayContaining([
        expect.objectContaining({ type: 'callback', value: expect.objectContaining({ action: expect.any(String) }) }),
      ]));
    }
    for (const button of nodes.filter(node => node.tag === 'button')) {
      expect(button.behaviors[0].value.__bm_cb).toBe(1);
    }
  });
});
