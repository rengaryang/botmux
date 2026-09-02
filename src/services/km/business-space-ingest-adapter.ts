import { createHash, timingSafeEqual } from 'node:crypto';
import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import { request as httpsRequest } from 'node:https';
import type { KmIngestRunReport, KmIngestTargetRecord } from './observation-store.js';
import { canonicalJsonStringify } from '../../utils/canonical-json.js';

export const BUSINESS_SPACE_INGEST_CONTRACT = 'botmux.business-space.ingest.v1';
const MAX_RESPONSE_BYTES = 256 * 1024;

export interface BusinessSpaceAcceptedItem { canonicalKey: string; remoteId: string; version?: string }
export interface BusinessSpaceRejectedItem { canonicalKey: string; errorCode: string; retryable: boolean }
export interface BusinessSpaceIngestAck {
  contract: typeof BUSINESS_SPACE_INGEST_CONTRACT;
  requestId: string;
  planHash: string;
  targetId: string;
  status: 'accepted' | 'partial' | 'rejected';
  accepted: BusinessSpaceAcceptedItem[];
  rejected: BusinessSpaceRejectedItem[];
  ackId: string;
}
export interface BusinessSpaceTransport {
  post(input: { url: string; headers: Record<string,string>; body: string; timeoutMs: number; resolvedAddresses: string[] }): Promise<{ status: number; body: string }>; 
}
export interface BusinessSpaceExecuteInput {
  target: KmIngestTargetRecord;
  report: KmIngestRunReport;
  credential: string;
  actorId: string;
  transport?: BusinessSpaceTransport;
  resolveHost?: (hostname: string) => Promise<string[]>;
}

export async function executeBusinessSpaceIngest(input: BusinessSpaceExecuteInput): Promise<BusinessSpaceIngestAck> {
  const { target, report } = input;
  if (target.state !== 'ready' || target.target.transport !== 'https' || target.target.dryRunOnly) throw new Error('km_ingest_remote_target_not_enabled');
  if (target.targetHash !== report.run.plan.targetHash) throw new Error('km_ingest_target_hash_mismatch');
  if (report.run.state !== 'approved' && report.run.state !== 'partial' && report.run.state !== 'failed') throw new Error(`km_ingest_remote_execution_requires_approval:${report.run.state}`);
  const endpoint = new URL(target.target.endpointRef);
  if (endpoint.protocol !== 'https:' || !target.target.allowedHosts.includes(endpoint.hostname.toLowerCase())) throw new Error('km_ingest_remote_host_not_allowlisted');
  if (endpoint.username || endpoint.password || endpoint.hash) throw new Error('km_ingest_remote_endpoint_invalid');
  const resolvedAddresses = await assertAllowedResolution(endpoint.hostname, target.target.allowPrivateNetwork, input.resolveHost ?? resolveHost);
  if (!input.credential.trim()) throw new Error('km_ingest_remote_credential_missing');

  const candidates = report.items.filter(item => item.state === 'pending' || item.state === 'failed').map(item => ({
    canonicalKey: item.canonicalKey,
    candidateHash: item.candidateHash,
    knowledge: item.candidate,
  }));
  if (!candidates.length) throw new Error('km_ingest_remote_no_pending_items');
  const requestId = `kmri_${createHash('sha256').update(`${report.run.runId}|${report.run.planHash}|${candidates.map(item => item.canonicalKey).join('|')}`).digest('hex')}`;
  const payload = { contract: BUSINESS_SPACE_INGEST_CONTRACT, requestId, planHash: report.run.planHash, targetId: report.run.targetId,
    sourceRunId: report.run.plan.sourceRunId, actorId: input.actorId, candidates };
  const body = canonicalJsonStringify(payload);
  const transport = input.transport ?? nodeFetchTransport;
  const response = await transport.post({ url: endpoint.toString(), timeoutMs: target.target.timeoutMs, body, resolvedAddresses, headers: {
    authorization: `Bearer ${input.credential}`,
    'content-type': 'application/json',
    'idempotency-key': requestId,
    'x-botmux-plan-hash': report.run.planHash,
    'x-botmux-contract': BUSINESS_SPACE_INGEST_CONTRACT,
  } });
  if (response.status < 200 || response.status >= 300) throw new Error(`km_ingest_remote_http_${response.status}`);
  if (Buffer.byteLength(response.body) > MAX_RESPONSE_BYTES) throw new Error('km_ingest_remote_response_too_large');
  let ack: BusinessSpaceIngestAck;
  try { ack = JSON.parse(response.body) as BusinessSpaceIngestAck; } catch { throw new Error('km_ingest_remote_ack_invalid_json'); }
  validateAck(ack, { requestId, planHash: report.run.planHash, targetId: report.run.targetId, canonicalKeys: candidates.map(item => item.canonicalKey) });
  return ack;
}

function validateAck(ack: BusinessSpaceIngestAck, expected: { requestId: string; planHash: string; targetId: string; canonicalKeys: string[] }): void {
  if (ack.contract !== BUSINESS_SPACE_INGEST_CONTRACT || ack.requestId !== expected.requestId || ack.targetId !== expected.targetId) throw new Error('km_ingest_remote_ack_contract_mismatch');
  const a = Buffer.from(String(ack.planHash)); const b = Buffer.from(expected.planHash);
  if (a.length !== b.length || !timingSafeEqual(a,b)) throw new Error('km_ingest_remote_ack_plan_hash_mismatch');
  if (!['accepted','partial','rejected'].includes(ack.status) || !ack.ackId?.trim() || !Array.isArray(ack.accepted) || !Array.isArray(ack.rejected)) throw new Error('km_ingest_remote_ack_invalid');
  const expectedSet = new Set(expected.canonicalKeys); const seen = new Set<string>();
  for (const item of [...ack.accepted, ...ack.rejected]) {
    if (!expectedSet.has(item.canonicalKey) || seen.has(item.canonicalKey)) throw new Error('km_ingest_remote_ack_item_mismatch');
    seen.add(item.canonicalKey);
  }
  if (seen.size !== expectedSet.size) throw new Error('km_ingest_remote_ack_incomplete');
  if (ack.status === 'accepted' && ack.rejected.length) throw new Error('km_ingest_remote_ack_status_mismatch');
  if (ack.status === 'rejected' && ack.accepted.length) throw new Error('km_ingest_remote_ack_status_mismatch');
}

const nodeFetchTransport: BusinessSpaceTransport = {
  async post(input) {
    const endpoint = new URL(input.url); let index = 0;
    return new Promise((resolve, reject) => {
      const req = httpsRequest(endpoint, {
        method: 'POST', headers: { ...input.headers, 'content-length': String(Buffer.byteLength(input.body)) },
        servername: endpoint.hostname,
        lookup: (_hostname, options, callback) => {
          const address = input.resolvedAddresses[index++ % input.resolvedAddresses.length];
          const family = isIP(address) as 4 | 6;
          if (typeof options === 'object' && options.all) callback(null, [{ address, family }]);
          else callback(null, address, family);
        },
      }, response => {
        const chunks: Buffer[] = []; let bytes = 0;
        response.on('data', chunk => { bytes += Buffer.byteLength(chunk); if (bytes > MAX_RESPONSE_BYTES) req.destroy(new Error('km_ingest_remote_response_too_large')); else chunks.push(Buffer.from(chunk)); });
        response.on('end', () => resolve({ status: response.statusCode ?? 0, body: Buffer.concat(chunks).toString('utf8') }));
      });
      req.setTimeout(input.timeoutMs, () => req.destroy(new Error('km_ingest_remote_timeout')));
      req.on('error', reject); req.end(input.body);
    });
  },
};
async function resolveHost(hostname: string): Promise<string[]> { return (await lookup(hostname, { all: true, verbatim: true })).map(item => item.address); }
async function assertAllowedResolution(hostname: string, allowPrivateNetwork: boolean, resolver: (hostname: string) => Promise<string[]>): Promise<string[]> {
  const addresses = isIP(hostname) ? [hostname] : await resolver(hostname);
  if (!addresses.length) throw new Error('km_ingest_remote_address_unresolved');
  if (!allowPrivateNetwork && addresses.some(isPrivateAddress)) throw new Error('km_ingest_remote_address_forbidden');
  if (addresses.some(address => isLoopbackOrLinkLocal(address))) throw new Error('km_ingest_remote_address_forbidden');
  return addresses;
}
function isLoopbackOrLinkLocal(address: string): boolean {
  const value = address.toLowerCase(); const parts = value.split('.').map(Number);
  return value === '::1' || value === '::' || value.startsWith('fe80:') || parts[0] === 127 || parts[0] === 0 || (parts[0] === 169 && parts[1] === 254);
}
function isPrivateAddress(address: string): boolean {
  const value = address.toLowerCase();
  if (value === '::1' || value === '::' || value.startsWith('fe80:') || value.startsWith('fc') || value.startsWith('fd')) return true;
  const parts = value.split('.').map(Number);
  if (parts.length !== 4 || parts.some(Number.isNaN)) return false;
  return parts[0] === 10 || parts[0] === 127 || parts[0] === 0 || (parts[0] === 169 && parts[1] === 254)
    || (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) || (parts[0] === 192 && parts[1] === 168)
    || (parts[0] === 100 && parts[1] >= 64 && parts[1] <= 127) || parts[0] >= 224;
}
