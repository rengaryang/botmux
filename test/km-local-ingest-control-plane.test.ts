import { mkdtempSync, readFileSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { handleKmObservationApi } from '../src/dashboard/km-observation-api.js';
import { hasLocalIngestSecret, listLocalIngestSecrets, setLocalIngestSecret } from '../src/services/km/local-ingest-secret-store.js';
import { ObservationStore } from '../src/services/km/observation-store.js';
import { defaultShadowProfile } from '../src/services/km/runtime-orchestrator.js';

function response() {
  return { statusCode: 0, headers: {} as Record<string,string>, body: '', writeHead(status: number, headers: Record<string,string>) { this.statusCode = status; Object.assign(this.headers, headers); }, setHeader(k: string, v: string) { this.headers[k] = v; }, end(v = '') { this.body = v; } };
}
function request(method: string, body: unknown, idempotencyKey: string) {
  return Object.assign(Readable.from([Buffer.from(JSON.stringify(body))]), { method, headers: { 'idempotency-key': idempotencyKey } });
}

describe('KM local ingest control plane', () => {
  it('encrypts local credentials and exposes metadata only', () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'botmux-local-ingest-secret-'));
    const meta = setLocalIngestSecret(dataDir, 'local-secret:business-space', 'plain-value');
    expect(meta.configured).toBe(true);
    expect(hasLocalIngestSecret(dataDir, meta.ref)).toBe(true);
    expect(readFileSync(join(dataDir, 'km-ingest-local-secrets.json'), 'utf8')).not.toContain('plain-value');
    expect(statSync(join(dataDir, 'km-ingest-local-master.key')).mode & 0o777).toBe(0o600);
    expect(listLocalIngestSecrets(dataDir)).toEqual([expect.objectContaining({ ref: meta.ref, configured: true })]);
    expect(JSON.stringify(listLocalIngestSecrets(dataDir))).not.toContain('ciphertext');
  });

  it('adds local target and secret APIs without changing existing configuration surfaces', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'botmux-local-ingest-api-'));
    const store = await ObservationStore.open(dataDir);
    store.close();
    const deps = { enabled: true, actorId: 'dashboard-owner', dataDir, openStore: async () => ObservationStore.open(dataDir) };

    const secretRes = response();
    await handleKmObservationApi(request('PUT', { ref: 'local-secret:business-space', secret: 'plain-value' }, 'secret-1') as any, secretRes as any, new URL('http://localhost/api/km/local-ingest/secrets'), deps);
    expect(secretRes.statusCode).toBe(200);
    expect(secretRes.body).not.toContain('plain-value');

    const targetRes = response();
    await handleKmObservationApi(request('PUT', { targetId: 'business-space', endpointRef: 'mock://business-space', credentialRef: 'local-secret:business-space', enabled: true, dryRunOnly: true, allowedProviderIds: ['builtin.rules-v1'] }, 'target-1') as any, targetRes as any, new URL('http://localhost/api/km/local-ingest/targets'), deps);
    expect(targetRes.statusCode).toBe(200);
    const target = JSON.parse(targetRes.body).target;
    expect(target.credential).toEqual({ mode: 'local-secret', reference: 'local-secret:***' });
    expect(target.target.endpointMode).toBe('mock');
    expect(targetRes.body).not.toContain('plain-value');

    const approvalRes = response();
    await handleKmObservationApi(request('PUT', { sourceRunId: 'missing-run', extractorProviderId: 'builtin.rules-v1' }, 'approval-1') as any, approvalRes as any, new URL('http://localhost/api/km/local-ingest/extractor-approvals'), deps);
    expect(approvalRes.statusCode).toBe(422);

    const jobStore = await ObservationStore.open(dataDir);
    const job = jobStore.createDistillationJob({ sourceEventId: 'evt-local', profile: defaultShadowProfile('bot-1'), now: Date.now() - 1 });
    const claim = jobStore.claimDistillationJob({})!;
    jobStore.finishDistillationJob({ jobId: job.jobId, claimToken: claim.claimToken, outputHash: `sha256:${'a'.repeat(64)}` });
    jobStore.close();
    const approvedRes = response();
    await handleKmObservationApi(request('PUT', { sourceRunId: job.jobId, extractorProviderId: 'builtin.rules-v1' }, 'approval-2') as any, approvedRes as any, new URL('http://localhost/api/km/local-ingest/extractor-approvals'), deps);
    expect(approvedRes.statusCode).toBe(200);
    expect(JSON.parse(approvedRes.body)).toEqual(expect.objectContaining({ sourceRunId: job.jobId, state: 'approved' }));

    const httpsTargetRes = response();
    await handleKmObservationApi(request('PUT', { targetId: 'business-space-https', endpointRef: 'https://knowledge.example.test/v1/ingest', credentialRef: 'local-secret:business-space', transport: 'https', enabled: true, dryRunOnly: false, allowedHosts: ['knowledge.example.test'], timeoutMs: 5000, allowedProviderIds: ['builtin.rules-v1'] }, 'target-https-1') as any, httpsTargetRes as any, new URL('http://localhost/api/km/local-ingest/targets'), deps);
    expect(httpsTargetRes.statusCode).toBe(200);
    expect(JSON.parse(httpsTargetRes.body).target.target).toEqual(expect.objectContaining({ transport: 'https', allowedHosts: ['knowledge.example.test'], timeoutMs: 5000, dryRunOnly: false }));

    const badHttpsTargetRes = response();
    await handleKmObservationApi(request('PUT', { targetId: 'bad-https', endpointRef: 'https://knowledge.example.test/v1/ingest', credentialRef: 'local-secret:business-space', transport: 'https', enabled: true, dryRunOnly: false, allowedHosts: ['other.example.test'] }, 'target-https-bad') as any, badHttpsTargetRes as any, new URL('http://localhost/api/km/local-ingest/targets'), deps);
    expect(badHttpsTargetRes.statusCode).toBe(422);

    const listRes = response();
    await handleKmObservationApi({ method: 'GET', headers: {} } as any, listRes as any, new URL('http://localhost/api/km/local-ingest/targets'), deps);
    expect(JSON.parse(listRes.body).executor).toEqual({ enabled: true, mode: 'local', executionApiEnabled: true });

    const saved = await ObservationStore.open(dataDir);
    expect(saved.listMemoryProviderConfigs()).toEqual([]);
    expect(saved.listSyncStatus()).toEqual([]);
    expect(saved.listProductionGatePlans()).toEqual([]);
    saved.close();
  });
});
