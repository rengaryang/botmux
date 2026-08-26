# Knowledge / Memory / Skill Intelligence 源码校准报告（Phase 0）

> 基线：`deepcoldy/botmux` master `9c19530f4cd0d6ad328edde480a6c5945d2766ec`  
> 目标分支：`feat/km-skill-intelligence-phase1`  
> 本报告把此前基于 v3.17.0 安装产物的判断映射到真实源码。未列出的能力仍视为 open item。

## 1. 构建与测试基线

| 项 | 源码事实 |
|---|---|
| Node | `>=22` |
| 包管理器 | `pnpm@9.5.0` |
| 构建 | `pnpm build`：domain audit → clean dist → tsc → scripts typecheck → runtime build id → dashboard bundle → dist audit |
| 单测 | `pnpm test` / `vitest --project unit` |
| E2E | `pnpm test:e2e`；真实 CLI，串行执行 |
| 版本 | `package.json` 源码保持 `0.0.0`，tag CI 注入版本；禁止手改 version |

## 2. 现有能力映射

| 设计能力 | 真实源码 | 已有测试 | 稳定性判断 | Phase 1 用法 |
|---|---|---|---|---|
| Skill Registry / Package | `src/core/skills/types.ts`, `src/services/skill-registry-store.ts` | `test/skill-registry-store.test.ts` | 内部源码契约 | 复用 SkillPackage identity、version、checksum、source |
| Skill Pack | `src/services/skill-pack-store.ts` | `test/skill-pack-store.test.ts` | 内部源码契约 | 用于 pack → skill 展开和 trace 展示 |
| Session Skill Manifest | `src/core/skills/session-resolver.ts`, `manifest-store.ts`, `session-runtime.ts` | `skill-manifest-store`, `session-skill-runtime`, `session-skill-injection` | 文件态内部契约 | 可靠表示 selected/loaded；不能据此宣称 invoked |
| Skill Delivery | `src/core/skills/delivery.ts`, `prompt.ts`, `claude-plugin-delivery.ts` | `skill-claude-delivery`, `skill-prompt` | CLI 相关 | Trace 必须标记 delivery mode 和 adapter |
| Turn completion | `src/services/turn-completion-events.ts`, `skill-feedback-store.ts` | `turn-completion-events`, `turn-terminal-queue` | 已有 durable contract | 新 Store 通过 producer 读取/接收，不修改 feedback schema |
| Feedback / Outbox | `src/services/skill-feedback-store.ts`, `feedback-outbox.ts`, `feedback-webhook-dispatcher.ts` | feedback store/outbox/analytics tests | 已有成熟先例 | 复用 WAL、busy retry、outbox 和非阻塞原则，不复用表 |
| Transcript resolution | `src/services/transcript-resolver.ts` + 各 CLI transcript parser | 多个 transcript/bridge test | Adapter-specific | 每条 inferred usage 必须带 resolver status、parser version |
| Workflow artifacts | `src/workflows/v3/manifest.ts`, artifact contracts, daemon runtime | v3 workflow tests | manifest-backed | 可作为 observed artifact event 来源 |
| Plugin | `src/core/plugins/*` | plugin manifest/generation/runtime tests | 内部扩展面 | 后续官方 KM service/plugin 需 fixture 验证 |
| Dashboard route | `src/dashboard/web/dashboard-routes.ts`, `app.tsx` | dashboard route lifecycle tests | 明确 lazy route seam | Phase 1 可新增 health/trace route |
| Dashboard server API | `src/dashboard.ts` 与独立 API handler | 多类 dashboard API tests | 大文件、需谨慎修改 | API 应拆为独立 handler，减少 dashboard.ts 风险 |

## 3. 关键校准结论

### 3.1 Feedback DB 不是 KM 扩展数据库

`src/services/skill-feedback-store.ts` 当前 schema version 为 7，包含 response/delivery/feedback/turn terminal/outbox 等表，并处理共享 dataDir 下的跨进程迁移竞争。Phase 1：

- 不新增列到该 DB；
- 不把未文档化表当成外部稳定 API；
- 新建 `botmux-km.sqlite`；
- 只通过 producer/service bridge 关联事件。

### 3.2 Session Skill Manifest 只证明选择和分发

`SessionSkillManifest.prioritySkills` 可以证明 resolver 选择出的 Skill 及其版本、checksum、source、priorityReason；结合 delivery 可证明已加载/注入。它不能单独证明模型实际读取或执行了 Skill。因此：

- `skill.manifest.resolved`：observed；
- `skill.invoked`：必须来自结构化 hook/tool/transcript 证据；
- 仅根据 manifest 推断 invoked：禁止。

### 3.3 Goal/Workflow artifact 是高质量证据

Workflow v3 对 output key、相对路径、kind、bytes、sha256 和 manifest 有严格契约。Phase 1 可把 `nodeSucceeded + manifest` 归一化为 `workflow.artifact.produced`，不读取 DAG 未声明文件。

### 3.4 观测写入必须不阻塞 daemon 主事件循环

现有 `turn-completion-events.ts` 已明确说明 `node:sqlite` 同步锁可能阻塞 event loop，并采用 nonblocking try + timer retry + graceful drain。新 Store 的 daemon producer 接入必须采用相同原则：

- Store 原子事务可以同步；
- daemon hot path 不能同步等待 busy timeout；
- 必须队列化、快速失败、异步重试；
- shutdown 可有界 drain。

## 4. Adapter 能力初版矩阵

| Adapter | Turn boundary | Skill selected/loaded | Skill invoked | Phase 1 判断 |
|---|---|---|---|---|
| Claude Code | transcript/hook + plugin delivery | observed | 可通过 skill/tool/hook 证据，需 fixture | 首批候选 |
| Codex / Codex App | structured turn events + transcript | observed（manifest/prompt） | 需 transcript/hook 规则与 fixture | 首批候选 |
| Traex | structured turn + transcript/hook | observed | 当前 hooks 丰富，但需 fixture 验证 payload | 首批候选 |
| CoCo | transcript/plugin/hook 差异 | observed | 部分事件可能 inferred | 后续 |
| Pi | transcript bridge | observed（prompt catalog） | 需 parser 证据 | 后续 |
| Grok | `turn_completed` 明确 | observed | 需 skill event fixture | 后续 |
| 其它 CLI | 不统一 | manifest 可有 | unknown | 默认 unknown，不假定支持 |

## 5. Phase 1 契约冻结

### 5.1 新模块边界

```text
src/services/km/
  observation-schema.ts   # Schema v1 + TS types
  observation-store.ts    # 独立 botmux-km.sqlite
  (后续) producers/*      # turn/feedback/workflow/skill
  (后续) observation-queue.ts
```

### 5.2 首版 feature 边界

- 默认不挂 daemon hot path；先完成 Store/Schema 单测。
- 不启用中心同步。
- 不生成 Knowledge/Memory 候选。
- 不执行 Skill mutation。
- 所有内容正文默认不内联，只存 hash/ref/短 preview。

## 6. Open Items

1. daemon producer 的精确注入点与 queue 生命周期需在 MR-03 单独评审。
2. Dashboard API 应复用哪组 auth helper，需在 MR-07 校准。
3. Traex/Codex/Claude skill invocation hook payload 需构建 fixture，不能凭文档猜。
4. feature flag 的最终配置层级（global/bot/host allowlist）需 ADR 决定。
5. 中央 Sink、向量检索、Knowledge/Memory 写入均不属于 Phase 1 首个 MR。
