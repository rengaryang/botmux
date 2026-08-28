import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ObservationStore, type KnowledgeItem } from './observation-store.js';
import type { ObservationEvent } from './observation-schema.js';
import { runKmCentralSinkDrill } from './central-sink-runtime.js';
import {
  createKnowledgeExportJob,
  executeKmFormalExport,
  previewKmFormalExport,
  reviewKnowledgeExportJob,
  rollbackKmFormalExport,
} from './knowledge-export-staging.js';
import { defaultShadowProfile } from './runtime-orchestrator.js';
import { kmBackendRuntimeStatus } from './memory-backend-runtime.js';
import {
  buildKmProductionGateHandoff,
  buildKmProductionGatePlan,
  stableKmProductionGateHash,
  type KmProductionGatePlanRequest,
} from './production-gate.js';

export interface KmOpsReadinessDrillReport {
  schemaVersion: 1;
  drillVersion: 'km-ops-readiness-drill-v1';
  generatedAt: string;
  scratch: { usedTemporaryDataDir: boolean; retained: boolean };
  safety: {
    fixtureOnly: true;
    realMemoryProviderCanary: false;
    realTransportEnabled: false;
    formalDestinationWrites: false;
    promptMutation: false;
    deletionExecutorAvailable: false;
  };
  centralSink: {
    sinkId: string;
    endpointRef: 'mock://central';
    status: { enabled: boolean; endpointMode: string; delivered: number; quarantined: number };
    partialAck: { scanned: number; enqueued: number; delivered: number; quarantined: number; failures: number };
    replay: { replayable: boolean; rows: number; outboxHash: string };
    conflict: { quarantineCreated: boolean; rollbackLocalDisableOnly: boolean };
  };
  knowledgeExport: {
    jobId: string;
    targetLayer: 'L2';
    stagedFile: string;
    manifestState: string;
    diffStatus: string;
    executionPreviewAllowed: boolean;
    executionFixtureOnly: boolean;
    appliedState: string;
    rollbackState: string;
    destinationAfterRollback: 'absent' | 'restored';
  };
  retention: {
    dryRunOnly: true;
    destructiveActionsAvailable: false;
    observations: { total: number; eligible: number; protected: number; legalHoldProtected: number };
    knowledge: { total: number; eligible: number; protected: number; legalHoldProtected: number };
    memory: { total: number; eligible: number; protected: number; legalHoldProtected: number };
    eligibleTotal: number;
  };
  productionGates: Array<{
    actionKind: string;
    requiredApprovalGrade: string;
    previewHash: string;
    effective: false;
    sideEffectsExecuted: false;
    operatorChecklistItems: number;
    rollbackHash: string;
  }>;
  localDefaultProfile: {
    injectionMode: string;
    memoryBackends: { writePolicy: string; primary: string; mirrors: string[] };
    externalProvidersConfigured: number;
    backendWorkerEnabled: boolean;
  };
  checks: Array<{ name: string; passed: boolean; evidence: string }>;
  drillHash: string;
}

export interface RunKmOpsReadinessDrillInput {
  dataDir?: string;
  now?: string;
  keepDataDir?: boolean;
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map(key => `${JSON.stringify(key)}:${stableJson(record[key])}`).join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

function sha256(value: unknown): string {
  return `sha256:${createHash('sha256').update(stableJson(value)).digest('hex')}`;
}

function event(id: string, createdAt: string, extra: Partial<ObservationEvent> = {}): ObservationEvent {
  return {
    schemaVersion: 1,
    eventId: id,
    eventType: 'turn.completed',
    source: { producer: 'turn', adapter: 'fixture', resolverStatus: 'resolved', confidence: 'observed' },
    identity: { botAppId: 'bot-readiness', sessionId: 'session-readiness', turnId: id },
    ordering: { sourceKey: 'km-ops-readiness', idempotencyKey: id, parentEventIds: [], observedAt: createdAt },
    provenance: { evidenceLevel: 'runtime', parserVersion: 'km-ops-readiness-v1', sourceRefs: [{ kind: 'api', ref: id }], privacyClass: 'internal', redactionStatus: 'not_needed' },
    content: { hash: null, storageMode: 'none' },
    payload: { status: 'completed', summary: id },
    createdAt,
    ...extra,
  };
}

function knowledge(overrides: Partial<KnowledgeItem> = {}): KnowledgeItem {
  const now = '2026-08-28T00:00:00.000Z';
  return {
    knowledgeId: 'kn_ops_readiness',
    state: 'approved',
    targetLayer: 'L2',
    category: 'ops',
    title: 'KM offline operations guard',
    claimKey: 'km.ops.offline.guard',
    claimText: 'KM operational readiness drills use only local SQLite, mock transports, and fixture destinations.',
    confidence: 'observed',
    freshness: 'fresh',
    privacyClass: 'internal',
    sourceRefs: [{ kind: 'workflow-artifact', ref: 'ops-readiness/drill' }],
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

async function setFixtureAges(dataDir: string): Promise<void> {
  const { DatabaseSync } = await import('node:sqlite');
  const db = new DatabaseSync(join(dataDir, 'botmux-km.sqlite'));
  try {
    db.prepare('UPDATE knowledge_items SET updated_at=? WHERE knowledge_id IN (?,?)')
      .run('2024-01-01T00:00:00.000Z', 'kn-retention-free', 'kn-retention-hold');
    db.prepare('UPDATE memory_items SET updated_at=? WHERE memory_id IN (?,?)')
      .run('2024-01-01T00:00:00.000Z', 'mem-retention-free', 'mem-retention-hold');
  } finally {
    db.close();
  }
}

async function seedFixture(dataDir: string): Promise<void> {
  const store = await ObservationStore.open(dataDir);
  try {
    store.append(event('evt-sink-1', '2026-08-28T00:00:00.000Z'));
    store.append(event('evt-sink-2', '2026-08-28T00:00:01.000Z'));
    store.append(event('evt-retention-free', '2024-01-01T00:00:00.000Z'));
    store.append(event('evt-retention-hold', '2024-01-02T00:00:00.000Z', {
      ordering: { sourceKey: 'km-ops-readiness-retention', idempotencyKey: 'evt-retention-hold', parentEventIds: [], observedAt: '2024-01-02T00:00:00.000Z' },
      provenance: {
        evidenceLevel: 'runtime',
        parserVersion: 'km-ops-readiness-v1',
        sourceRefs: [{ kind: 'api', ref: 'legal-hold' }],
        privacyClass: 'internal',
        redactionStatus: 'not_needed',
      },
    }));
    store.append(event('evt-retention-new', '2026-08-20T00:00:00.000Z', {
      ordering: { sourceKey: 'km-ops-readiness-retention', idempotencyKey: 'evt-retention-new', parentEventIds: [], observedAt: '2026-08-20T00:00:00.000Z' },
    }));
    store.configureSyncSink({ sinkId: 'central-mock', protocolVersion: 1, endpointRef: 'mock://central', enabled: true, batchLimit: 2, maxAttempts: 2 });
    const rejected = store.proposeKnowledge({
      knowledgeId: 'kn-retention-free',
      targetLayer: 'L2',
      category: 'ops',
      title: 'Old rejected KM note',
      claimKey: 'km.retention.free',
      claimText: 'Old rejected knowledge is eligible in preview.',
      confidence: 'observed',
      freshness: 'stale',
      privacyClass: 'internal',
      sourceRefs: [{ kind: 'api', ref: 'evt-retention-free' }],
    }).item;
    store.transitionKnowledge({ knowledgeId: rejected.knowledgeId, toState: 'rejected', reasonCode: 'fixture_rejected', actorId: 'ops-readiness' });
    store.proposeKnowledge({
      knowledgeId: 'kn-retention-hold',
      targetLayer: 'L2',
      category: 'ops',
      title: 'Old held KM note',
      claimKey: 'km.retention.held',
      claimText: 'Old held knowledge is excluded by legal hold.',
      confidence: 'observed',
      freshness: 'stale',
      privacyClass: 'internal',
      sourceRefs: [{ kind: 'api', ref: 'legal-hold' }],
    });
    store.transitionKnowledge({ knowledgeId: 'kn-retention-hold', toState: 'rejected', reasonCode: 'fixture_rejected', actorId: 'ops-readiness' });
    store.upsertMemory({
      memoryId: 'mem-retention-free',
      state: 'active',
      scope: 'bot',
      subject: 'bot-readiness',
      claimKey: 'km.retention.free',
      claimText: 'Old revoked memory is eligible in preview.',
      confidence: 'observed',
      privacyClass: 'internal',
      sourceRefs: [{ kind: 'api', ref: 'evt-retention-free' }],
    });
    store.transitionMemory({ memoryId: 'mem-retention-free', toState: 'revoked', reasonCode: 'fixture_revoked', actorId: 'ops-readiness' });
    store.upsertMemory({
      memoryId: 'mem-retention-hold',
      state: 'active',
      scope: 'bot',
      subject: 'bot-readiness',
      claimKey: 'km.retention.held',
      claimText: 'Old held memory is excluded by legal hold.',
      confidence: 'observed',
      privacyClass: 'internal',
      sourceRefs: [{ kind: 'api', ref: 'legal-hold' }],
    });
    store.transitionMemory({ memoryId: 'mem-retention-hold', toState: 'revoked', reasonCode: 'fixture_revoked', actorId: 'ops-readiness' });
  } finally {
    store.close();
  }
  await setFixtureAges(dataDir);
}

function productionGateRequests(now: string): Array<Pick<KmProductionGatePlanRequest, 'actionKind' | 'target' | 'scope'> & { confirmationToken: string }> {
  const canaryEnd = new Date(Date.parse(now) + 60 * 60_000).toISOString();
  return [
    {
      actionKind: 'real-memory-transport',
      target: { provider: 'mem0', endpoint: 'https://memory.example.test/v1', credentialRef: 'env:MEM0_API_KEY' },
      scope: { provider: 'mem0', botAppId: 'bot-readiness' },
      confirmationToken: 'ops-readiness-memory',
    },
    {
      actionKind: 'real-central-sink',
      target: { provider: 'central', endpoint: 'https://central.example.test/ingest', credentialRef: 'env:BOTMUX_KM_CENTRAL_SINK_SECRET' },
      scope: { sinkId: 'central-prod', tenant: 'fixture' },
      confirmationToken: 'ops-readiness-sink',
    },
    {
      actionKind: 'formal-knowledge-export',
      target: { destinationRoot: '/tmp/km-export-fixture', manifestHash: `sha256:${'a'.repeat(64)}` },
      scope: { targetLayer: 'L2', knowledgeId: 'kn_ops_readiness' },
      confirmationToken: 'ops-readiness-export',
    },
    {
      actionKind: 'prompt-canary',
      target: { botAppId: 'bot-readiness', window: { start: now, end: canaryEnd } },
      scope: { botAppId: 'bot-readiness', sessionClass: 'manual-canary' },
      confirmationToken: 'ops-readiness-canary',
    },
    {
      actionKind: 'retention-purge',
      target: { cutoff: '2026-08-01T00:00:00.000Z', expectedCounts: { observations: 1, knowledge: 1, memory: 1 } },
      scope: { database: 'km-local', domain: 'fixture' },
      confirmationToken: 'ops-readiness-retention',
    },
  ];
}

export async function runKmOpsReadinessDrill(input: RunKmOpsReadinessDrillInput = {}): Promise<KmOpsReadinessDrillReport> {
  const generatedAt = input.now ?? '2026-08-28T00:00:00.000Z';
  const dataDir = input.dataDir ?? mkdtempSync(join(tmpdir(), 'botmux-km-ops-readiness-'));
  const usedTemporaryDataDir = !input.dataDir;
  let report: Omit<KmOpsReadinessDrillReport, 'drillHash'>;
  try {
    await seedFixture(dataDir);
    const statusDrill = await runKmCentralSinkDrill({
      dataDir,
      sinkId: 'central-mock',
      drill: 'status',
      actorId: 'ops-readiness',
      idempotencyKey: 'status',
      now: Date.parse(generatedAt),
    });
    const partialDrill = await runKmCentralSinkDrill({
      dataDir,
      sinkId: 'central-mock',
      drill: 'partial-ack',
      actorId: 'ops-readiness',
      idempotencyKey: 'partial',
      now: Date.parse(generatedAt),
    });
    const replayDrill = await runKmCentralSinkDrill({
      dataDir,
      sinkId: 'central-mock',
      drill: 'replay',
      actorId: 'ops-readiness',
      idempotencyKey: 'replay',
      now: Date.parse(generatedAt),
    });
    const conflictDrill = await runKmCentralSinkDrill({
      dataDir,
      sinkId: 'central-mock',
      drill: 'conflict',
      actorId: 'ops-readiness',
      idempotencyKey: 'conflict',
      now: Date.parse(generatedAt),
    });

    const job = createKnowledgeExportJob({
      dataDir,
      knowledge: knowledge(),
      actorId: 'ops-readiness',
      idempotencyKey: 'export-create',
      now: generatedAt,
    });
    const staged = reviewKnowledgeExportJob({
      dataDir,
      jobId: job.jobId,
      decision: 'approved',
      actorId: 'ops-readiness',
      idempotencyKey: 'export-review',
      reasonCode: 'fixture_review_approved',
      now: generatedAt,
    });
    const preview = previewKmFormalExport({ dataDir, jobId: staged.jobId });
    const applied = executeKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'ops-readiness',
      idempotencyKey: 'export-execute',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: preview.precondition.currentTargetHash,
      destinationVersion: preview.precondition.destinationVersion,
      now: generatedAt,
    });
    const rolledBack = rollbackKmFormalExport({
      dataDir,
      jobId: staged.jobId,
      actorId: 'ops-readiness',
      idempotencyKey: 'export-rollback',
      approvalGrade: 'G2',
      confirmationToken: preview.confirmationToken,
      expectedTargetHash: applied.execution?.afterHash ?? undefined,
      destinationVersion: preview.precondition.destinationVersion,
      now: generatedAt,
    });

    const store = await ObservationStore.open(dataDir);
    let retention;
    try {
      retention = store.kmRetentionPreview({ now: Date.parse(generatedAt), sampleLimit: 5 });
    } finally {
      store.close();
    }
    const retentionDomain = (domain: 'observations' | 'knowledge' | 'memory') => {
      const item = retention.domains.find(value => value.domain === domain);
      if (!item) throw new Error(`km_retention_domain_missing:${domain}`);
      return {
        total: item.totalCount,
        eligible: item.eligibleCount,
        protected: item.protectedCount,
        legalHoldProtected: item.protectedReasonCounts.legal_hold ?? 0,
      };
    };

    const productionGates = productionGateRequests(generatedAt).map(request => {
      const built = buildKmProductionGatePlan({
        actionKind: request.actionKind,
        target: request.target,
        scope: request.scope,
        actorId: 'ops-readiness',
        riskAck: { acknowledged: true, ticket: 'KM-OPS-READINESS' },
        now: generatedAt,
        ttlSeconds: 600,
        confirmationToken: request.confirmationToken,
      });
      const handoff = buildKmProductionGateHandoff({
        ...built.plan,
        createdAt: generatedAt,
        updatedAt: generatedAt,
      });
      return {
        actionKind: built.plan.actionKind,
        requiredApprovalGrade: built.plan.requiredApprovalGrade,
        previewHash: built.plan.previewHash,
        effective: handoff.effective,
        sideEffectsExecuted: handoff.sideEffectsExecuted,
        operatorChecklistItems: handoff.operatorChecklist.length,
        rollbackHash: stableKmProductionGateHash(handoff.rollback),
      };
    });

    const defaultProfile = defaultShadowProfile('bot-readiness');
    const backendStatus = await kmBackendRuntimeStatus({ dataDir, env: {}, now: Date.parse(generatedAt) });
    const partial = partialDrill.result as Record<string, unknown>;
    const statusSink = statusDrill.sink as Record<string, unknown>;
    const replay = replayDrill as Record<string, unknown>;
    report = {
      schemaVersion: 1,
      drillVersion: 'km-ops-readiness-drill-v1',
      generatedAt,
      scratch: { usedTemporaryDataDir, retained: Boolean(input.keepDataDir || input.dataDir) },
      safety: {
        fixtureOnly: true,
        realMemoryProviderCanary: false,
        realTransportEnabled: false,
        formalDestinationWrites: false,
        promptMutation: false,
        deletionExecutorAvailable: false,
      },
      centralSink: {
        sinkId: 'central-mock',
        endpointRef: 'mock://central',
        status: {
          enabled: Boolean(statusSink.enabled),
          endpointMode: String((statusSink.endpointPolicy as Record<string, unknown> | undefined)?.mode ?? 'unknown'),
          delivered: Number(statusSink.delivered ?? 0),
          quarantined: Number(statusSink.quarantined ?? 0),
        },
        partialAck: {
          scanned: Number(partial.scanned ?? 0),
          enqueued: Number(partial.enqueued ?? 0),
          delivered: Number(partial.delivered ?? 0),
          quarantined: Number(partial.quarantined ?? 0),
          failures: Array.isArray(partial.failures) ? partial.failures.length : 0,
        },
        replay: {
          replayable: Boolean(replay.replayable),
          rows: Number(replay.rows ?? 0),
          outboxHash: String(replay.outboxHash ?? ''),
        },
        conflict: {
          quarantineCreated: typeof conflictDrill.quarantineId === 'string',
          rollbackLocalDisableOnly: Boolean((conflictDrill.rollback as Record<string, unknown> | undefined)?.localDisableOnly),
        },
      },
      knowledgeExport: {
        jobId: staged.jobId,
        targetLayer: 'L2',
        stagedFile: staged.manifest?.stagedFile ?? '',
        manifestState: staged.manifest?.state ?? 'missing',
        diffStatus: job.plan.diff.status,
        executionPreviewAllowed: preview.allowed,
        executionFixtureOnly: preview.risk.fixtureOnly,
        appliedState: applied.state,
        rollbackState: rolledBack.state,
        destinationAfterRollback: existsSync(preview.destination.absolutePath) ? 'restored' : 'absent',
      },
      retention: {
        dryRunOnly: retention.dryRunOnly,
        destructiveActionsAvailable: retention.destructiveActionsAvailable,
        observations: retentionDomain('observations'),
        knowledge: retentionDomain('knowledge'),
        memory: retentionDomain('memory'),
        eligibleTotal: retention.domains.reduce((sum, domain) => sum + domain.eligibleCount, 0),
      },
      productionGates,
      localDefaultProfile: {
        injectionMode: defaultProfile.injectionMode,
        memoryBackends: defaultProfile.memoryBackends,
        externalProvidersConfigured: backendStatus.providers.length,
        backendWorkerEnabled: backendStatus.enabled,
      },
      checks: [],
    };
    report.checks = [
      { name: 'central_sink_partial_ack', passed: report.centralSink.partialAck.delivered === 1 && report.centralSink.partialAck.quarantined >= 1, evidence: JSON.stringify(report.centralSink.partialAck) },
      { name: 'central_sink_replay', passed: report.centralSink.replay.replayable && report.centralSink.replay.rows >= 2, evidence: report.centralSink.replay.outboxHash },
      { name: 'central_sink_conflict', passed: report.centralSink.conflict.quarantineCreated && report.centralSink.conflict.rollbackLocalDisableOnly, evidence: JSON.stringify(report.centralSink.conflict) },
      { name: 'knowledge_export_rehearsal', passed: report.knowledgeExport.manifestState === 'staged' && report.knowledgeExport.appliedState === 'applied' && report.knowledgeExport.rollbackState === 'rolled_back', evidence: report.knowledgeExport.jobId },
      { name: 'retention_preview_only', passed: report.retention.dryRunOnly && !report.retention.destructiveActionsAvailable && report.retention.observations.legalHoldProtected >= 1, evidence: JSON.stringify(report.retention.observations) },
      { name: 'production_gate_inert', passed: report.productionGates.length === 5 && report.productionGates.every(item => !item.effective && !item.sideEffectsExecuted), evidence: report.productionGates.map(item => item.actionKind).join(',') },
      { name: 'sqlite_only_default_profile', passed: report.localDefaultProfile.memoryBackends.writePolicy === 'single' && report.localDefaultProfile.memoryBackends.primary === 'sqlite' && report.localDefaultProfile.memoryBackends.mirrors.length === 0, evidence: JSON.stringify(report.localDefaultProfile.memoryBackends) },
      { name: 'default_off_no_external_transport', passed: !report.safety.realTransportEnabled && report.localDefaultProfile.externalProvidersConfigured === 0 && !report.localDefaultProfile.backendWorkerEnabled, evidence: JSON.stringify(report.safety) },
    ];
    return { ...report, drillHash: sha256(report) };
  } finally {
    if (usedTemporaryDataDir && !input.keepDataDir) rmSync(dataDir, { recursive: true, force: true });
  }
}
