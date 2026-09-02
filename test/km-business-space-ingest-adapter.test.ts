import { describe, expect, it, vi } from 'vitest';
import { executeBusinessSpaceIngest, BUSINESS_SPACE_INGEST_CONTRACT } from '../src/services/km/business-space-ingest-adapter.js';
import type { KmIngestRunReport, KmIngestTargetRecord } from '../src/services/km/observation-store.js';

const target = (overrides: Partial<KmIngestTargetRecord['target']> = {}): KmIngestTargetRecord => ({
  targetId: 'business-space', state: 'ready', targetHash: 'sha256:target', credentialRef: 'local-secret:business', createdBy: 'owner', createdAt: '2026-01-01', updatedAt: '2026-01-01',
  target: { endpointRef: 'https://knowledge.example.test/v1/ingest', dryRunOnly: false, allowedProviderIds: ['builtin.rules-v1'], transport: 'https', allowedHosts: ['knowledge.example.test'], timeoutMs: 2000, ...overrides },
});
const report = (): KmIngestRunReport => ({
  run: { runId:'run-1', idempotencyKey:'idem', state:'approved', targetId:'business-space', plan:{ schemaVersion:1,targetId:'business-space',targetHash:'sha256:target',sourceRunId:'extract-1',extractorRunState:'completed',extractorProviderId:'builtin.rules-v1',mode:'offline',dryRun:true,planCalls:{markIngested:false},canonicalKeys:['key-1'] }, planHash:'sha256:plan',canonicalKeySetHash:'sha256:keys',confirmationTokenHash:'sha256:token',externalAck:{approved:true,approvedBy:'owner',planHash:'sha256:plan'},sourceCount:1,eligibleCount:1,ingestedCount:0,dedupedCount:0,skippedCount:0,failedCount:0,rollbackCount:0,markIngestedPlannedCount:0,createdBy:'owner',createdAt:'2026-01-01',updatedAt:'2026-01-01' },
  items:[{ingestItemId:'item-1',runId:'run-1',canonicalKey:'key-1',candidate:{targetLayer:'L2',category:'runbook',title:'Title',claimKey:'claim',claimText:'body',confidence:'observed',freshness:'fresh',privacyClass:'internal',sourceRefs:[{kind:'distillation-job',ref:'extract-1'}]},candidateHash:'sha256:candidate',state:'pending',createdAt:'2026-01-01',updatedAt:'2026-01-01'}],audit:[],
});

describe('business space ingest adapter', () => {
  it('sends a bounded idempotent HTTPS contract and verifies ACK correlation', async () => {
    const post = vi.fn(async (input: any) => {
      const request = JSON.parse(input.body);
      expect(input.url).toBe('https://knowledge.example.test/v1/ingest');
      expect(input.headers.authorization).toBe('Bearer secret-value');
      expect(input.headers['idempotency-key']).toBe(request.requestId);
      expect(request.candidates[0].canonicalKey).toBe('key-1');
      return { status:200, body:JSON.stringify({contract:BUSINESS_SPACE_INGEST_CONTRACT,requestId:request.requestId,planHash:'sha256:plan',targetId:'business-space',status:'accepted',accepted:[{canonicalKey:'key-1',remoteId:'remote-1'}],rejected:[],ackId:'ack-1'}) };
    });
    const ack = await executeBusinessSpaceIngest({target:target(),report:report(),credential:'secret-value',actorId:'owner',transport:{post},resolveHost:async()=>['203.0.113.10']});
    expect(ack.ackId).toBe('ack-1'); expect(post).toHaveBeenCalledOnce();
  });
  it('fails closed for non-allowlisted/private targets and malformed ACKs', async () => {
    await expect(executeBusinessSpaceIngest({target:target({allowedHosts:['other.test']}),report:report(),credential:'x',actorId:'owner',transport:{post:vi.fn()},resolveHost:async()=>['203.0.113.10']})).rejects.toThrow('host_not_allowlisted');
    await expect(executeBusinessSpaceIngest({target:target(),report:report(),credential:'x',actorId:'owner',transport:{post:vi.fn()},resolveHost:async()=>['127.0.0.1']})).rejects.toThrow('address_forbidden');
    await expect(executeBusinessSpaceIngest({target:target(),report:report(),credential:'x',actorId:'owner',transport:{post:async()=>({status:200,body:'{"status":"accepted"}'})},resolveHost:async()=>['203.0.113.10']})).rejects.toThrow('ack_contract_mismatch');
  });
  it('accepts exact partial ACK partitions only', async () => {
    const value=report(); value.run.plan.canonicalKeys=['key-1','key-2']; value.run.sourceCount=2; value.run.eligibleCount=2; value.items.push({...value.items[0],ingestItemId:'item-2',canonicalKey:'key-2',candidateHash:'sha256:candidate2'});
    const post=async(input:any)=>{const req=JSON.parse(input.body);return{status:200,body:JSON.stringify({contract:BUSINESS_SPACE_INGEST_CONTRACT,requestId:req.requestId,planHash:'sha256:plan',targetId:'business-space',status:'partial',accepted:[{canonicalKey:'key-1',remoteId:'remote-1'}],rejected:[{canonicalKey:'key-2',errorCode:'temporary',retryable:true}],ackId:'ack-partial'})}};
    const ack=await executeBusinessSpaceIngest({target:target(),report:value,credential:'x',actorId:'owner',transport:{post},resolveHost:async()=>['203.0.113.10']});
    expect(ack.status).toBe('partial'); expect(ack.rejected[0].retryable).toBe(true);
  });
});
