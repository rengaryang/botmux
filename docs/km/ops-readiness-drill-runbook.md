# KM Ops Readiness Drill Runbook

This runbook is for the offline readiness milestone only. It deliberately excludes any real Memory provider G3 canary and must not be used as approval to enable production transport, formal destination writes, prompt mutation, or deletion.

## Command

```bash
botmux km ops-readiness --output /tmp/km-ops-readiness.json
```

By default the command creates a temporary `botmux-km.sqlite` fixture and removes it after the report is produced. Use `--keep-data-dir` only when an operator needs to inspect the fixture database after the drill. Use `--data-dir <dir>` only for an isolated fixture directory, not a live daemon data directory.

## What It Proves

- Central Sink fixture E2E:
  - `mock://central` only;
  - partial acknowledgement moves accepted rows to delivered and rejected rows to quarantine;
  - replay produces a stable outbox hash without sending new network traffic;
  - conflict drill creates local quarantine evidence and reports local-disable-only rollback.
- Knowledge export rehearsal:
  - approved knowledge creates a staging job and manifest;
  - formal preview is deterministic and limited to a fixture workspace;
  - execute and rollback are exercised only against the fixture destination.
- Retention preview:
  - every domain remains `dryRunOnly`;
  - protected/legal-hold rows are excluded from eligibility;
  - no purge or apply executor is exposed.
- Production-gate handoff:
  - all handoff bundles are inert;
  - `effective=false` and `sideEffectsExecuted=false` for real transport, real central sink, formal export, prompt canary, and retention purge intents.
- Local default profile:
  - absent stored provider profile resolves to a single local `sqlite` primary;
  - no mirror provider is configured by default.

## Required Green Checks

The JSON report is acceptable only when every entry in `checks` has `passed: true` and `safety` is:

```json
{
  "fixtureOnly": true,
  "realMemoryProviderCanary": false,
  "realTransportEnabled": false,
  "formalDestinationWrites": false,
  "promptMutation": false,
  "deletionExecutorAvailable": false
}
```

The report `drillHash` is a stable hash over the report body excluding `drillHash`; use it to compare repeated runs with the same `--now`.
