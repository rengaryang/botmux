import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { readJournal } from './journal.js';
import type { WorkflowExecutionProfile } from './execution-profile-store.js';

export type WorkflowTaskType = 'code' | 'research' | 'summary' | 'review' | 'test' | 'ops' | 'general';
export interface WorkflowProfileHistory { attempts: number; successes: number; failures: number; timeouts: number; successRate: number | null }
export interface WorkflowProfileRecommendation {
  profileId: string; taskType: WorkflowTaskType; score: number; history: WorkflowProfileHistory;
  coldStart: boolean; reasons: string[];
}

const TYPE_HINTS: Record<WorkflowTaskType, RegExp> = {
  code: /实现|开发|修复|代码|refactor|implement|fix|code/iu,
  research: /调研|研究|分析|检索|research|investigate/iu,
  summary: /总结|汇总|报告|摘要|summary|report/iu,
  review: /审查|评审|review|audit/iu,
  test: /测试|验证|回归|test|verify/iu,
  ops: /部署|运维|环境|巡检|日志|deploy|ops|incident/iu,
  general: /(?:)/u,
};

export function classifyWorkflowTask(goal: string): WorkflowTaskType {
  for (const type of ['code', 'review', 'test', 'ops', 'research', 'summary'] as WorkflowTaskType[]) {
    if (TYPE_HINTS[type].test(goal)) return type;
  }
  return 'general';
}

export function collectWorkflowProfileHistory(runsDir: string): Map<string, WorkflowProfileHistory> {
  const counts = new Map<string, { attempts: number; successes: number; failures: number; timeouts: number }>();
  if (!existsSync(runsDir)) return new Map();
  for (const runId of readdirSync(runsDir).slice(-500)) {
    const runDir = join(runsDir, runId);
    const snapshots = readSnapshots(runDir);
    const selectors = readNodeSelectors(runDir);
    if (!snapshots || !selectors) continue;
    let events;
    try { events = readJournal(join(runDir, 'journal.ndjson')); } catch { continue; }
    const attempts = new Map<string, string>();
    for (const event of events) {
      if (event.type === 'nodeDispatched') {
        const profileId = selectorProfile(snapshots, selectors.get(event.nodeId) ?? '') ?? selectorProfile(snapshots, '');
        if (profileId) attempts.set(event.attemptId, profileId);
      } else if (event.type === 'nodeSucceeded' || event.type === 'nodeFailed' || event.type === 'nodeBlocked') {
        const profileId = attempts.get(event.attemptId); if (!profileId) continue;
        const item = counts.get(profileId) ?? { attempts: 0, successes: 0, failures: 0, timeouts: 0 };
        item.attempts += 1;
        if (event.type === 'nodeSucceeded') item.successes += 1;
        else item.failures += 1;
        if (event.type === 'nodeFailed' && event.errorClass === 'timeout') item.timeouts += 1;
        counts.set(profileId, item); attempts.delete(event.attemptId);
      }
    }
  }
  return new Map([...counts].map(([id, value]) => [id, { ...value, successRate: value.attempts ? value.successes / value.attempts : null }]));
}

export function recommendWorkflowProfiles(input: {
  goal: string; profiles: WorkflowExecutionProfile[]; history?: Map<string, WorkflowProfileHistory>;
}): WorkflowProfileRecommendation[] {
  const taskType = classifyWorkflowTask(input.goal);
  const history = input.history ?? new Map();
  return input.profiles.filter(profile => profile.enabled).map(profile => {
    const h = history.get(profile.profileId) ?? { attempts: 0, successes: 0, failures: 0, timeouts: 0, successRate: null };
    const typeScore = capabilityScore(profile, taskType);
    const historyScore = h.successRate === null ? 0 : Math.round(h.successRate * 35);
    const confidence = Math.min(10, h.attempts);
    const timeoutPenalty = h.attempts ? Math.round((h.timeouts / h.attempts) * 15) : 0;
    const costBonus = ['summary', 'research', 'test'].includes(taskType) && profile.costTier === 'low' ? 8 : 0;
    const score = Math.max(0, Math.min(100, 35 + typeScore + historyScore + confidence + costBonus - timeoutPenalty));
    return {
      profileId: profile.profileId, taskType, score, history: h, coldStart: h.attempts < 3,
      reasons: [
        `task_type=${taskType}:${typeScore >= 15 ? 'strong_match' : typeScore > 0 ? 'compatible' : 'neutral'}`,
        h.attempts < 3 ? `cold_start:samples=${h.attempts}` : `historical_success=${Math.round((h.successRate ?? 0) * 100)}%/n=${h.attempts}`,
        `cost_tier=${profile.costTier}`,
      ],
    };
  }).sort((a, b) => b.score - a.score || a.profileId.localeCompare(b.profileId));
}

function capabilityScore(profile: WorkflowExecutionProfile, type: WorkflowTaskType): number {
  const text = `${profile.displayName} ${profile.cli} ${profile.model ?? ''}`;
  if (type === 'code' && /claude|codex|seed|traex/iu.test(text)) return 20;
  if (type === 'review' && /claude|codex|pi/iu.test(text)) return 20;
  if (type === 'test' && /codex|traex|relay|pi/iu.test(text)) return 18;
  if (type === 'ops' && /traex|relay|pi|claude/iu.test(text)) return 18;
  if (type === 'research' && /pi|claude|seed|relay/iu.test(text)) return 16;
  if (type === 'summary') return 10;
  return 5;
}

function readSnapshots(runDir: string): Record<string, { larkAppId?: string }> | null {
  try { return JSON.parse(readFileSync(join(runDir, 'bots.snapshot.json'), 'utf8')); } catch { return null; }
}
function readNodeSelectors(runDir: string): Map<string, string> | null {
  try {
    const dag = JSON.parse(readFileSync(join(runDir, 'dag.json'), 'utf8')) as { nodes?: Array<{ id?: unknown; executionProfile?: unknown; bot?: unknown }> };
    if (!Array.isArray(dag.nodes)) return null;
    return new Map(dag.nodes.map(node => [String(node.id ?? ''), typeof node.executionProfile === 'string' ? node.executionProfile : typeof node.bot === 'string' ? node.bot : '']));
  } catch { return null; }
}
function selectorProfile(snapshots: Record<string, { larkAppId?: string }>, selector: string): string | undefined {
  const identity = snapshots[selector]?.larkAppId;
  return identity?.startsWith('profile:') ? identity.slice('profile:'.length) : undefined;
}
