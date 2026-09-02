# Governed Knowledge Operations

This runbook describes the current L2 governance, retrieval-evidence, review-queue, and offline-ingest surfaces. It does not authorize external writes.

## Safety defaults

- L2 governance commands are preview-only unless `--execute`, the exact `--plan-hash`, and `--confirm apply:<plan_hash>` are all supplied.
- Governance execution creates per-entry backups and supports guarded rollback. It never runs ingest or GC.
- Retrieval evidence stores hash/count/fixed-label metadata only; query and reasoning bodies are forbidden.
- The Dashboard Review Queue is read-only. Missing independent manifests remain `unavailable`/`null`.
- The ingest executor is disabled and offline. HTTP exposes status reads only; there is no execution endpoint.
- Do not treat a preview, plan, or local `mark-ingested` plan as an external ACK.

## L2 governance CLI

Run from the `database-devops` repository:

```bash
GOV=.coco/skills/workspace-l2-knowledge/scripts/governance.py
python3 "$GOV" --working-dir /path/to/workspace review-matrix
python3 "$GOV" --working-dir /path/to/workspace routing-plan
python3 "$GOV" --working-dir /path/to/workspace redact-generalize
python3 "$GOV" --working-dir /path/to/workspace approve-links
python3 "$GOV" --working-dir /path/to/workspace legacy-migration
```

For a mutating operation, first save the preview and its plan hash. Execute only after reviewing the exact plan:

```bash
python3 "$GOV" --working-dir /path/to/workspace redact-generalize \
  --execute --plan-hash '<exact-preview-hash>' \
  --confirm 'apply:<exact-preview-hash>'
```

Exact link approval additionally requires the reviewed `--pair-id`. Rollback uses the same hash-and-confirmation gate. See `.coco/skills/workspace-l2-knowledge/SKILL.md` for the complete contract.

## Retrieval evidence

Automatic producers observe proven boundaries only:

- `index_query` / `entry_read` / `fallback` at retrieval or observable transcript boundaries;
- `entry_used` only after actual live prompt injection, never for shadow/would-inject;
- `query_feedback` only when the feedback delivery context carries matching hash-only correlation.

Manual append remains explicit:

```bash
botmux km retrieval-evidence index-query \
  --working-dir /path/to/workspace \
  --query-hash 'sha256:<64-hex>' \
  --execute
```

Without `--execute`, the command refuses the write. Evidence failures are isolated from the user interaction.

## Read-only Dashboard APIs

All endpoints remain behind normal Dashboard authentication:

- `GET /api/km/dashboard-metrics-v2`
- `GET /api/km/review-queue-v2`
- `GET /api/km/ingest`
- `GET /api/km/ingest/targets`
- `GET /api/km/ingest/:runId`

The Review Queue exposes only safe metadata such as batch, route, decision, blockers, plan hash, audit time, checksums, and relative references. The ingest status projection omits endpoint, credential, candidate, ACK, and mark-ingested payloads.

## Local ingest control plane (additive)

Botmux also exposes a separate local-only control plane under `/api/km/local-ingest/*`. It does not replace or migrate the existing `/api/km/ingest/*` read-only offline contract, Memory Provider, Central Sink, Production Gate, or Canary configuration.

- Local credential plaintext is encrypted with AES-256-GCM in a machine-local `0600` secret store; SQLite and API responses retain reference/metadata only.
- The Dashboard provides additive forms for local credential, target, extractor run, plan confirmation, execution approval, execution, and rollback.
- Target endpoints remain restricted to `mock:` or `file:/`; this control plane does not perform real network/business-space writes.
- Plan creation and execution resolve `local-secret:*` references without exposing plaintext.

## Formal ingest gate

The offline/local state machine requires all of the following before local execution can progress:

1. a registered offline target and credential reference;
2. unique canonical keys and an exact key-set hash;
3. a completed/persisted knowledge-extractor run;
4. matching plan hash and confirmation token;
5. explicit run approval;
6. an external ACK matching the plan hash.

Even then, the current implementation performs no network/business-space write and only records local dry-run evidence. Formal external execution remains blocked until separately implemented and approved.
