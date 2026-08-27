import type { MemoryScope } from './observation-store.js';

export type RetrievalMatchKind = 'direct' | 'normalized';

export interface NormalizedRetrievalQuery {
  normalizedText: string;
  groups: RetrievalQueryTermGroup[];
}

export interface RetrievalVisibilityContext {
  botAppId?: string;
  userId?: string;
  projectId?: string;
  skillName?: string;
  environmentId?: string;
  teamId?: string;
  workspaceId?: string;
}

interface RetrievalQueryTermGroup {
  canonical: string;
  alternatives: string[];
  queryTerms: string[];
}

const SYNONYM_GROUPS = [
  ['chinese', '中文', '汉语', 'Chinese', 'zh', 'zh-cn'],
  ['english', '英文', '英语', 'English', 'en', 'en-us'],
  ['response', '回复', '回答', '答复', 'reply', 'answer', 'respond'],
  ['preference', '偏好', '习惯', 'prefer', 'prefers', 'preference', 'style'],
  ['markdown', 'markdown', 'md', 'Markdown'],
  ['deploy', '部署', '发布', '上线', 'deploy', 'deployment', 'rollout', 'release'],
  ['upgrade', '升级', '更新', 'upgrade', 'update'],
  ['environment', '环境', 'env', 'environment'],
  ['workspace', '工作区', 'workspace', 'worktree', 'repo'],
  ['project', '项目', 'project'],
  ['skill', '技能', 'skill'],
  ['bot', '机器人', 'bot', 'agent'],
  ['user', '用户', '成员', 'user'],
  ['team', '团队', 'team'],
  ['permission', '权限', '授权', 'permission', 'auth', 'authorization'],
  ['log', '日志', 'log', 'logs'],
  ['logid', '日志id', '日志 ID', 'logid', 'log id', 'request id', 'requestid'],
  ['error', '报错', '错误', '失败', 'error', 'failure', 'failed', 'fail'],
  ['cluster', '集群', 'cluster'],
  ['node', '节点', 'node'],
  ['nodepool', '节点池', 'node pool', 'nodepool'],
  ['database', '数据库', 'db', 'database'],
  ['instance', '实例', 'instance'],
  ['config', '配置', 'config', 'configuration'],
  ['alert', '告警', '报警', 'alert', 'alarm'],
  ['restart', '重启', 'restart', 'reboot'],
  ['build', '构建', '编译', 'build', 'compile'],
  ['retrieval', '检索', '召回', 'recall', 'retrieval'],
  ['memory', '记忆', 'memory'],
  ['knowledge', '知识', 'knowledge'],
] as const;

const PHRASE_TO_GROUP = new Map<string, string[]>();
for (const group of SYNONYM_GROUPS) {
  const normalized = [...new Set(group.map(term => normalizeText(term)).filter(Boolean))];
  for (const term of normalized) PHRASE_TO_GROUP.set(term, normalized);
}

function unique(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}

export function normalizeText(value: string): string {
  return value.normalize('NFKC')
    .toLowerCase()
    .replace(/[\u2018\u2019]/gu, "'")
    .replace(/[\u201c\u201d]/gu, '"')
    .replace(/[\s/_:;,.!?()[\]{}<>|\\+=*&^%$#@`~，。！？；：（）【】《》、]+/gu, ' ')
    .trim()
    .replace(/\s+/gu, ' ');
}

function asciiTokens(value: string): string[] {
  return value.match(/[a-z0-9][a-z0-9-]*/gu) ?? [];
}

function cjkTokens(value: string): string[] {
  return value.match(/[\p{Script=Han}]{1,8}/gu) ?? [];
}

function tokenVariants(token: string): string[] {
  const variants = [token];
  if (/^[a-z0-9-]+$/u.test(token)) {
    if (token.endsWith('ies') && token.length > 4) variants.push(`${token.slice(0, -3)}y`);
    if (token.endsWith('es') && token.length > 3) variants.push(token.slice(0, -2));
    if (token.endsWith('s') && token.length > 3) variants.push(token.slice(0, -1));
  }
  return unique(variants);
}

function containsTerm(normalizedText: string, term: string): boolean {
  if (!term) return false;
  if (/^[a-z0-9][a-z0-9-]*(?: [a-z0-9][a-z0-9-]*)*$/u.test(term)) {
    const escaped = term.split(' ').map(part => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('\\s+');
    return new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, 'u').test(normalizedText);
  }
  return normalizedText.includes(term);
}

export function normalizeRetrievalQuery(text: string): NormalizedRetrievalQuery {
  const normalizedText = normalizeText(text);
  const groups: RetrievalQueryTermGroup[] = [];
  const seenCanonicals = new Set<string>();
  for (const [phrase, alternatives] of [...PHRASE_TO_GROUP.entries()].sort((a, b) => b[0].length - a[0].length || a[0].localeCompare(b[0]))) {
    if (!containsTerm(normalizedText, phrase)) continue;
    const canonical = alternatives[0];
    if (seenCanonicals.has(canonical)) continue;
    seenCanonicals.add(canonical);
    groups.push({ canonical, alternatives, queryTerms: alternatives.filter(term => containsTerm(normalizedText, term)) });
  }
  const covered = new Set(groups.flatMap(group => group.alternatives));
  for (const token of [...asciiTokens(normalizedText), ...cjkTokens(normalizedText)]) {
    for (const variant of tokenVariants(token)) {
      if (covered.has(variant) || seenCanonicals.has(variant)) continue;
      seenCanonicals.add(variant);
      groups.push({ canonical: variant, alternatives: [variant], queryTerms: [variant] });
    }
  }
  return { normalizedText, groups };
}

export function scoreNormalizedQuery(query: NormalizedRetrievalQuery, text: string): { score: number; matchKind?: RetrievalMatchKind; matchedGroups: number } {
  if (query.groups.length === 0) return { score: 1, matchKind: 'direct', matchedGroups: 0 };
  const normalizedText = normalizeText(text);
  let matchedGroups = 0;
  let normalizedOnly = false;
  for (const group of query.groups) {
    const direct = group.queryTerms.some(term => containsTerm(normalizedText, term));
    const expanded = direct ? true : group.alternatives.some(term => containsTerm(normalizedText, term));
    if (expanded) {
      matchedGroups += 1;
      if (!direct) normalizedOnly = true;
    }
  }
  if (matchedGroups === 0) return { score: 0, matchedGroups };
  const coverage = matchedGroups / query.groups.length;
  const phraseBoost = query.normalizedText && containsTerm(normalizedText, query.normalizedText) ? 0.15 : 0;
  return { score: Math.min(1, coverage + phraseBoost), matchKind: normalizedOnly ? 'normalized' : 'direct', matchedGroups };
}

export function resolveRetrievalScopeSubjects(ctx: RetrievalVisibilityContext): Partial<Record<MemoryScope, string>> {
  const subjects: Partial<Record<MemoryScope, string>> = {};
  if (ctx.userId?.trim()) subjects.user = ctx.userId.trim();
  if (ctx.botAppId?.trim()) subjects.bot = ctx.botAppId.trim();
  if (ctx.projectId?.trim()) subjects.project = ctx.projectId.trim();
  if (ctx.skillName?.trim()) subjects.skill = ctx.skillName.trim();
  if (ctx.environmentId?.trim()) subjects.environment = ctx.environmentId.trim();
  if (ctx.teamId?.trim()) subjects.team = ctx.teamId.trim();
  if (ctx.workspaceId?.trim()) subjects.workspace = ctx.workspaceId.trim();
  return subjects;
}

export function visibleScopes(subjects: Partial<Record<MemoryScope, string>>): MemoryScope[] {
  return (['user', 'bot', 'project', 'skill', 'environment', 'team', 'workspace'] as MemoryScope[])
    .filter(scope => Boolean(subjects[scope]));
}
