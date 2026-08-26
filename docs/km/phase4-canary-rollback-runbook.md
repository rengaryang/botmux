# KM Phase 4 Canary And Rollback Runbook

## Safety defaults

- Central Sink is disabled unless a `sync_sinks.enabled=1` row exists.
- The current implementation only permits enabled `mock://` sinks. Real HTTP endpoints fail closed.
- Redaction blocks secrets, `secret-reference-only`, `pending_review`, and `blocked` observations.
- Sync is downstream-only; local chat, observation, review, retrieval, Trace, and Eval never wait for Central Sink.

## Preflight

1. Run `pnpm exec vitest run --project unit test/km-*.test.ts` and `pnpm build`.
2. Verify `BOTMUX_KM_OBSERVATION_ENABLED=true` only on the canary host.
3. Verify `GET /api/km/health` reports WAL, foreign keys, zero quarantine regressions.
4. Keep every real sink disabled. Use `mock://local` for the first drill.
5. Record DB size, WAL size, daemon restart counters, memory, observation count, and quarantine count.

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
3. Disable retrieval injection if runtime behavior is suspect.
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
