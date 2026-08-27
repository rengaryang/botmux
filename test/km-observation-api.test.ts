import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import { handleKmObservationApi } from '../src/dashboard/km-observation-api.js';

function response() {
  const bodies: unknown[] = [];
  const res = {
    writeHead: vi.fn(),
    end: vi.fn(value => bodies.push(JSON.parse(String(value)))),
  } as any;
  return { res, bodies };
}

describe('KM observation dashboard API', () => {
  it('does not expose routes while the feature is disabled', async () => {
    const { res, bodies } = response();
    const handled = await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/health'),
      { enabled: false, openStore: vi.fn() },
    );
    expect(handled).toBe(true);
    expect(res.writeHead).toHaveBeenCalledWith(404, expect.anything());
    expect(bodies).toEqual([{ error: 'km_observation_disabled' }]);
  });

  it('returns store health and closes the request-scoped store', async () => {
    const close = vi.fn();
    const { res, bodies } = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/health'),
      {
        enabled: true,
        openStore: async () => ({
          schemaVersion: () => 1,
          pragmas: () => ({ journalMode: 'wal', foreignKeys: 1, busyTimeout: 5000 }),
          counts: () => ({ observations: 3, quarantined: 1, knowledge: 2, memory: 4 }),
          list: vi.fn(), get: vi.fn(), close,
        }),
      },
    );
    expect(bodies).toEqual([{
      enabled: true,
      schemaVersion: 1,
      pragmas: { journalMode: 'wal', foreignKeys: 1, busyTimeout: 5000 },
      counts: { observations: 3, quarantined: 1, knowledge: 2, memory: 4 },
    }]);
    expect(close).toHaveBeenCalledOnce();
  });

  it('bounds list pagination and forwards typed filters', async () => {
    const list = vi.fn(() => [{ eventId: 'evt-1' }]);
    const { res } = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      res,
      new URL('http://localhost/api/km/observations?limit=999&before=42&type=turn.completed'),
      {
        enabled: true,
        openStore: async () => ({
          schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(),
          list, get: vi.fn(), close: vi.fn(),
        }),
      },
    );
    expect(list).toHaveBeenCalledWith({ limit: 100, beforeLocalSeq: 42, eventType: 'turn.completed' });
  });

  it('serves Phase 2 knowledge, memory and retrieval reads', async () => {
    const listKnowledge = vi.fn(() => [{ knowledgeId: 'kn-1' }]);
    const listMemory = vi.fn(() => [{ memoryId: 'mem-1' }]);
    const retrieve = vi.fn(() => [{ id: 'kn-1', kind: 'knowledge' }]);
    const deps = {
      enabled: true,
      openStore: async () => ({
        schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
        listKnowledge, listMemory, retrieve,
      }),
    };
    const knowledge = response();
    await handleKmObservationApi({ method: 'GET' } as any, knowledge.res,
      new URL('http://localhost/api/km/knowledge?state=approved&targetLayer=L2'), deps);
    expect(listKnowledge).toHaveBeenCalledWith({ limit: 50, state: 'approved', targetLayer: 'L2' });
    expect(knowledge.bodies).toEqual([{ items: [{ knowledgeId: 'kn-1' }] }]);

    const memory = response();
    await handleKmObservationApi({ method: 'GET' } as any, memory.res,
      new URL('http://localhost/api/km/memory?state=active&scope=user&subject=u1'), deps);
    expect(listMemory).toHaveBeenCalledWith({ limit: 50, state: 'active', scope: 'user', subject: 'u1' });

    const retrieval = response();
    await handleKmObservationApi({ method: 'GET' } as any, retrieval.res,
      new URL('http://localhost/api/km/retrieve?q=failover&scope=user&targetLayer=L3&subject=u1'), deps);
    expect(retrieve).toHaveBeenCalledWith({ text: 'failover', limit: 20, subject: 'u1', scopes: ['user'], targetLayers: ['L3'] });
  });

  it('serves trace/evolution reads and enforces approval grade through the store', async () => {
    const listTrace = vi.fn(() => [{ edgeId: 'edge-1' }]);
    const listEvolution = vi.fn(() => [{ proposalId: 'evo-1' }]);
    const decideProposal = vi.fn(() => ({ approvalId: 'approval-1', state: 'approved' }));
    const listEvalRuns = vi.fn(() => [{ evalRunId: 'eval-1' }]);
    const listSyncStatus = vi.fn(() => [{ sinkId: 'mock', enabled: false }]);
    const deps = {
      enabled: true,
      openStore: async () => ({
        schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
        listTrace, listEvolution, decideProposal, listEvalRuns, listSyncStatus,
      }),
    };
    const trace = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, trace.res,
      new URL('http://localhost/api/km/trace?type=turn&id=turn-1&limit=999'), deps);
    expect(listTrace).toHaveBeenCalledWith({ type: 'turn', id: 'turn-1', limit: 500 });
    expect(trace.bodies).toEqual([{ items: [{ edgeId: 'edge-1' }] }]);

    const sync = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, sync.res,
      new URL('http://localhost/api/km/sync/sinks'), deps);
    expect(sync.bodies).toEqual([{ items: [{ sinkId: 'mock', enabled: false }] }]);

    const evals = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, evals.res,
      new URL('http://localhost/api/km/eval/runs'), deps);
    expect(evals.bodies).toEqual([{ items: [{ evalRunId: 'eval-1' }] }]);

    const proposals = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, proposals.res,
      new URL('http://localhost/api/km/evolution/proposals'), deps);
    expect(proposals.bodies).toEqual([{ items: [{ proposalId: 'evo-1' }] }]);

    const decision = response();
    const req = Object.assign(Readable.from([Buffer.from(JSON.stringify({ decision: 'approved', grade: 'G2', scope: { target: 'skill' }, riskAck: {} }))]), {
      method: 'POST', headers: { 'x-km-actor-id': 'reviewer-1' },
    });
    await handleKmObservationApi(req as any, decision.res,
      new URL('http://localhost/api/km/evolution/proposals/evo-1/decision'), deps);
    expect(decideProposal).toHaveBeenCalledWith({ proposalId: 'evo-1', decision: 'approved', actorId: 'reviewer-1', grade: 'G2', scope: { target: 'skill' }, riskAck: {} });
  });

  it('serves guarded pipeline profile and provider configuration mutations', async () => {
    const listPipelineProfiles = vi.fn(() => [{ state: 'draft' }]);
    const putPipelineProfile = vi.fn(() => `sha256:${'a'.repeat(64)}`);
    const setPipelineProfileState = vi.fn(() => ({ state: 'shadow' }));
    const listMemoryProviderConfigs = vi.fn(() => [{ providerId: 'mem0', credentialRef: 'env:***' }]);
    const putMemoryProviderConfig = vi.fn(() => `sha256:${'b'.repeat(64)}`);
    const deps = { enabled: true, openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get: vi.fn(), close: vi.fn(),
      listPipelineProfiles, putPipelineProfile, setPipelineProfileState, listMemoryProviderConfigs, putMemoryProviderConfig }) };
    const list = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, list.res, new URL('http://localhost/api/km/profiles?botAppId=bot-1'), deps);
    expect(listPipelineProfiles).toHaveBeenCalledWith('bot-1');

    const configReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ providerId: 'mem0', endpoint: 'https://memory.example.test',
      credentialRef: 'env:MEM0_KEY', enabled: true, realTransportEnabled: false, timeoutMs: 5000 }))]),
      { method: 'PUT', headers: { 'x-km-actor-id': 'reviewer', 'idempotency-key': 'key-1' } });
    const config = response();
    await handleKmObservationApi(configReq as any, config.res, new URL('http://localhost/api/km/provider-configs'), deps);
    expect(putMemoryProviderConfig).toHaveBeenCalledWith(expect.objectContaining({ providerId: 'mem0', realTransportEnabled: false }));

    const stateReq = Object.assign(Readable.from([Buffer.from(JSON.stringify({ state: 'shadow' }))]),
      { method: 'PATCH', headers: { 'x-km-actor-id': 'reviewer', 'idempotency-key': 'key-2' } });
    const state = response();
    await handleKmObservationApi(stateReq as any, state.res, new URL('http://localhost/api/km/profiles/default/1/state'), deps);
    expect(setPipelineProfileState).toHaveBeenCalledWith({ profileId: 'default', revision: 1, state: 'shadow' });
  });

  it('returns one event and rejects unsupported methods', async () => {
    const get = vi.fn(() => ({ eventId: 'evt-1', payload: { status: 'completed' } }));
    const first = response();
    await handleKmObservationApi(
      { method: 'GET' } as any,
      first.res,
      new URL('http://localhost/api/km/observations/evt-1'),
      {
        enabled: true,
        openStore: async () => ({ schemaVersion: vi.fn(), pragmas: vi.fn(), counts: vi.fn(), list: vi.fn(), get, close: vi.fn() }),
      },
    );
    expect(first.bodies).toEqual([{ eventId: 'evt-1', payload: { status: 'completed' } }]);

    const second = response();
    await handleKmObservationApi(
      { method: 'POST', headers: {} } as any,
      second.res,
      new URL('http://localhost/api/km/observations'),
      { enabled: true, openStore: vi.fn() },
    );
    expect(second.bodies).toEqual([{ error: 'method_not_allowed' }]);
  });
});
