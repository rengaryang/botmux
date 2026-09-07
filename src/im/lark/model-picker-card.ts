import type { ModelChoiceDetail, ModelSource } from '../../services/model-catalog.js';
import type { Locale } from '../../i18n/index.js';

export const MODEL_PICKER_PAGE_SIZE = 6;

export interface ModelPickerState {
  readonly search?: string;
  readonly provider?: string;
  readonly page?: number;
}

export interface ModelPickerBinding {
  readonly rootId: string;
  readonly sessionId: string;
  readonly cliId: string;
  readonly invokerOpenId: string;
  readonly nonce: string;
}

function esc(value: string): string {
  return value.replace(/([\\`*_{}\[\]()#+\-.!|>])/g, '\\$1');
}

function normalizedSearch(value: string | undefined): string {
  return (value ?? '').trim().toLowerCase();
}

export function filterModelChoices(
  choices: readonly ModelChoiceDetail[],
  state: ModelPickerState = {},
): ModelChoiceDetail[] {
  const query = normalizedSearch(state.search);
  return choices.filter(choice => {
    if (state.provider && choice.provider !== state.provider) return false;
    if (!query) return true;
    return [choice.value, choice.model, choice.provider, choice.label]
      .filter(Boolean)
      .some(part => String(part).toLowerCase().includes(query));
  });
}

export function buildModelPickerCard(args: {
  readonly cliName: string;
  readonly activeModel?: string;
  readonly choices: readonly ModelChoiceDetail[];
  readonly source: ModelSource;
  readonly binding: ModelPickerBinding;
  readonly state?: ModelPickerState;
  readonly locale?: Locale;
}): string {
  const state = args.state ?? {};
  const filtered = filterModelChoices(args.choices, state);
  const providers = [...new Set(args.choices.map(choice => choice.provider).filter((v): v is string => !!v))];
  const totalPages = Math.max(1, Math.ceil(filtered.length / MODEL_PICKER_PAGE_SIZE));
  const page = Math.min(Math.max(0, Number(state.page ?? 0) || 0), totalPages - 1);
  const visible = filtered.slice(page * MODEL_PICKER_PAGE_SIZE, (page + 1) * MODEL_PICKER_PAGE_SIZE);
  const common = {
    root_id: args.binding.rootId,
    session_id: args.binding.sessionId,
    cli_id: args.binding.cliId,
    invoker_open_id: args.binding.invokerOpenId,
    nonce: args.binding.nonce,
    search: state.search ?? '',
    provider: state.provider ?? '',
    page: String(page),
  };
  const elements: any[] = [{
    tag: 'markdown',
    content: `**当前 CLI**：${esc(args.cliName)}\n**当前模型**：${args.activeModel ? `\`${esc(args.activeModel)}\`` : '未知'}\n**目录来源**：${args.source === 'live' ? '实时探测' : '静态候选'}`,
  }];

  if (providers.length > 0) {
    elements.push({
      tag: 'select_static',
      placeholder: { tag: 'plain_text', content: '筛选 Provider' },
      initial_option: state.provider || '__all__',
      options: [
        { text: { tag: 'plain_text', content: '全部 Provider' }, value: '__all__' },
        ...providers.map(provider => ({ text: { tag: 'plain_text', content: provider }, value: provider })),
      ],
      behaviors: [{ type: 'callback', value: { action: 'cli_model_provider', ...common } }],
    });
  }

  elements.push({
    tag: 'input',
    name: 'model_search',
    placeholder: { tag: 'plain_text', content: '搜索模型或 Provider' },
    default_value: state.search ?? '',
    width: 'fill',
    behaviors: [{ type: 'callback', value: { action: 'cli_model_search', ...common } }],
  });
  elements.push({ tag: 'hr' });

  if (visible.length === 0) {
    elements.push({ tag: 'markdown', content: '没有匹配的模型。请调整搜索词或 Provider。' });
  } else {
    // Card 2.0 removed the legacy `action` container. Keep the compact
    // two-column layout with column_set, but put each button directly inside
    // a column and carry callback data only through behaviors[].value.
    for (let index = 0; index < visible.length; index += 2) {
      elements.push({
        tag: 'column_set',
        flex_mode: 'none',
        horizontal_spacing: 'small',
        columns: visible.slice(index, index + 2).map(choice => {
          const selected = args.activeModel === choice.value;
          return {
            tag: 'column', width: 'weighted', weight: 1,
            elements: [{
              tag: 'button',
              width: 'fill',
              type: selected ? 'primary_filled' : 'default',
              text: { tag: 'plain_text', content: `${selected ? '✓ ' : ''}${choice.label}` },
              behaviors: [{
                type: 'callback',
                value: { action: 'cli_model_select', model: choice.value, ...common },
              }],
            }],
          };
        }),
      });
    }
  }

  if (totalPages > 1) {
    elements.push({
      tag: 'column_set',
      flex_mode: 'none',
      columns: [
        {
          tag: 'column', width: 'weighted', weight: 1,
          elements: [{
            tag: 'button', type: 'default', disabled: page === 0,
            text: { tag: 'plain_text', content: '← 上一页' },
            behaviors: [{
              type: 'callback',
              value: { action: 'cli_model_page', ...common, page: String(Math.max(0, page - 1)) },
            }],
          }],
        },
        {
          tag: 'column', width: 'weighted', weight: 1,
          elements: [{ tag: 'markdown', text_align: 'center', content: `${page + 1} / ${totalPages}` }],
        },
        {
          tag: 'column', width: 'weighted', weight: 1,
          elements: [{
            tag: 'button', type: 'default', disabled: page >= totalPages - 1,
            text: { tag: 'plain_text', content: '下一页 →' },
            behaviors: [{
              type: 'callback',
              value: { action: 'cli_model_page', ...common, page: String(Math.min(totalPages - 1, page + 1)) },
            }],
          }],
        },
      ],
    });
  }

  return JSON.stringify({
    schema: '2.0',
    config: { wide_screen_mode: true },
    header: {
      title: { tag: 'plain_text', content: `选择 ${args.cliName} 模型` },
      template: 'blue',
    },
    body: { direction: 'vertical', elements },
  });
}

export function buildModelPickerResolvedCard(cliName: string, model: string): Record<string, unknown> {
  return {
    schema: '2.0',
    config: { wide_screen_mode: true },
    header: { title: { tag: 'plain_text', content: '模型切换指令已发送' }, template: 'green' },
    body: {
      direction: 'vertical',
      elements: [{ tag: 'markdown', content: `**CLI**：${esc(cliName)}\n**模型**：\`${esc(model)}\`\n\n已交给当前 CLI 处理；实际生效状态以 CLI 随后的输出为准。` }],
    },
  };
}
