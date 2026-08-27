# KM Retention Shadow Runbook

KM retention is intentionally preview-only in this milestone. It computes deterministic eligibility plans and operational SLOs, then optionally stores append-only shadow reports. It does not expose purge/apply endpoints and does not delete SQLite rows or files.

## Enablement

- `BOTMUX_KM_OBSERVATION_ENABLED=true` must be enabled before KM data exists.
- `BOTMUX_KM_RETENTION_SHADOW_ENABLED=true` enables the background shadow reporter.
- Leaving `BOTMUX_KM_RETENTION_SHADOW_ENABLED` unset, `false`, `0`, or `no` keeps the scheduler off.
- `BOTMUX_KM_AUTO_GC_ENABLED` is ignored by the retention implementation and must not be treated as permission to delete data.

## Dashboard And API

- Dashboard page: `#/km`, section `Retention / GC Preview`.
- Read-only status endpoint: `GET /api/km/retention`.
- Read-only history endpoint: `GET /api/km/retention/reports?limit=30`.

The status payload includes:

- `latestPlan.dryRunOnly=true` and `latestPlan.destructiveActionsAvailable=false`.
- Per-domain retention tier, cutoff, total row count, eligible row count, protected row count, oldest age, and bounded eligible samples.
- SQLite DB/WAL size.
- Distillation, sync outbox, backend outbox, quarantine, retry, provider quality, and retrieval quality metrics.
- SLO metrics with `ok`, `warn`, or `critical` state.
- Append-only shadow report summaries and trend snapshots.

## Protected Records

Retention preview excludes protected records. Protection includes:

- Legal-hold markers in JSON evidence.
- Active, pending, inflight, retry, conflicted, approved, exported, review-pending, executing, applied, and verified states.
- Source evidence referenced by distillation, trace, eval, prompt injection, or quarantine tables.
- All quarantine evidence.

## Operator Procedure

1. Confirm the dashboard/API shows `destructiveActionsAvailable=false`.
2. Inspect `GET /api/km/retention` for domain-level eligible counts and SLO states.
3. If long-term trend is needed, enable only the shadow reporter with `BOTMUX_KM_RETENTION_SHADOW_ENABLED=true` during a controlled daemon rollout.
4. Watch `GET /api/km/retention/reports?limit=30` for append-only report cadence.
5. Treat `warn`/`critical` SLOs as investigation signals. They authorize analysis, not deletion.
6. Stop at deployment gate for any live daemon restart, PM2 change, config write, or future purge workflow.

## Forbidden In This Milestone

- No purge/apply API endpoint.
- No SQL `DELETE` for retention.
- No file deletion.
- No automatic garbage collection.
- No live prompt behavior change.
- No real network or provider transport.
- No deployment, merge, push, publish, or live DB/PM2 change without a separate human approval.
