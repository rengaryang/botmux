# KM Phase 4 Canary And Rollback Runbook

## Safety defaults

- Central Sink is disabled unless a `sync_sinks.enabled=1` row exists.
- The current implementation only permits enabled `mock://` sinks. Real HTTP endpoints fail closed.
- Redaction blocks secrets, `secret-reference-only`, `pending_review`, and `blocked` observations.
- Sync is downstream-only; local chat, observation, review, retrieval, Trace, and Eval never wait for Central Sink.
- Prompt-memory live injection is disabled unless all gates pass at once:
  `BOTMUX_KM_LIVE_INJECTION_ENABLED=true`, `BOTMUX_KM_EFFECTIVE_MODE_AUTHORIZED=true`,
  a stored profile requests `canary` or `active`, and the current `botAppId` appears in
  `BOTMUX_KM_CANARY_BOT_APP_IDS`.
- The prompt-memory allowlist is bot-only. Do not add user, session, thread, project, or item-scoped live-injection overrides.
- Federated memory transport is not part of live prompt injection readiness. The live boundary only records that remote retrieval was skipped.

## Preflight

1. Run `pnpm exec vitest run --project unit test/km-*.test.ts` and `pnpm build`.
2. Verify `BOTMUX_KM_OBSERVATION_ENABLED=true` only on the canary host.
3. Verify `GET /api/km/health` reports WAL, foreign keys, zero quarantine regressions.
4. Keep every real sink disabled. Use `mock://local` for the first drill.
5. Record DB size, WAL size, daemon restart counters, memory, observation count, and quarantine count.
6. Dry-run prompt-memory readiness with live injection still off and confirm new audit rows have `requestedMode`, `effectiveMode`, `disposition`, selected item IDs, `promptHash`, and no raw prompt content.
7. Confirm `BOTMUX_KM_CANARY_BOT_APP_IDS` contains only the exact bot app IDs approved for the current canary window.

## Local closeout report

Use `GET /api/km/canary-closeout?botAppId=cli_aacca607f9ccdcf8` to generate the deterministic local closeout bundle for the approved canary bot. Add `format=markdown` for the operator-facing report.

The closeout report is preview-only and inert. It reads reviewed golden cases, shadow comparisons, readiness, retrieval audits, prompt snapshot summaries, import-job counts, and prompt-canary production-gate state from local SQLite. It also includes an exact-bot production-gate handoff preview and rollback checklist, but it does not persist a gate, create an approval intent, call Pi, call external memory providers, mutate prompts, write formal knowledge pages, or edit the central KM Dashboard page.

Before canary activation, the report should show reviewed/redacted golden bootstrap validation, FP/FN and disagreement calibration, `unexpectedLiveInjection=0`, and no valid action-scoped approval unless a separate operator-approved prompt-canary gate exists for exactly `cli_aacca607f9ccdcf8`.

## Prompt-Memory Canary Gates

Stop at two human gates:

1. Code deployment/restart gate: after tests pass, an operator may deploy/restart with `BOTMUX_KM_LIVE_INJECTION_ENABLED` still unset or false. This may create `would_inject`/`skipped` audit rows but must not mutate prompts.
2. Canary activation gate: only after reviewing the dry-run audit, an operator may enable live injection for explicit bot app IDs by setting all live gates and a stored profile requesting `canary` or `active`.

Readiness checks before gate 2:

- `prompt_injection_snapshots` shows no unexpected user/session/item-scoped activation.
- Non-allowlisted bots record `effectiveMode=shadow` and `reason=bot_not_allowlisted`.
- Profiles requesting `shadow` record `reason=requested_mode_not_live`.
- Failed authorization records `reason=effective_mode_not_authorized`.
- Prompt hashes are present for non-empty composed prompts; raw prompt content is absent from audit output.
- Budget truncation records `byte_budget` or `prompt_budget` and keeps output deterministic.

## Seven-day canary gates

- No P0/P1 data loss or privacy leak.
- Daemon and Dashboard restart regression: zero attributable to KM.
- Secret export block fixtures: 100%.
- Local ingest P95 <= 5 seconds.
- Mock sync pending age P95 <= 10 minutes.
- Idempotency collisions are quarantined; no source observation is overwritten.
- Draft/conflicted/stale memory remains excluded from retrieval.
- G2-G4 proposal execution remains unavailable without matching approval.

## Rollback

Rollback is disable-first and non-destructive:

1. Disable all sinks (`enabled=0`). Do not delete outbox, cursors, or quarantine evidence.
2. Stop scheduling `runSyncOnce`; inflight leases recover to `failed` after expiry.
3. Disable prompt-memory live injection first: set `BOTMUX_KM_LIVE_INJECTION_ENABLED=false` or remove the affected bot from `BOTMUX_KM_CANARY_BOT_APP_IDS`, then restart only after explicit operator confirmation.
4. Disable candidate extraction / Eval / Evolution workers independently.
5. Set `BOTMUX_KM_OBSERVATION_ENABLED=false` and restart only after explicit operator confirmation if observation itself must stop.
6. Keep `botmux-km.sqlite` read-only for forensic review. Do not downgrade or delete it automatically.
7. Switch PM2 script paths back to the released Botmux package only after recording current checkout commit and process state.

## Verification after rollback

- Dashboard and all daemons are online.
- Chat turn completion is unaffected.
- No new outbox attempts occur.
- Existing observations, approvals, and quarantine rows remain readable.
- Restore drill can reopen schema v5 without loss.

## Real Central Sink prerequisites

Not authorized by this runbook. Before enabling a real endpoint, require:

- assigned service/data owner;
- reviewed schema/auth/signature protocol;
- G3 data-scope approval;
- credential reference (never raw credential in SQLite);
- allowlisted endpoint and TLS policy;
- redaction/privacy fixture report;
- partial-ack and conflict drill;
- explicit operator confirmation for the exact endpoint and scope.
