import { chmodSync, existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';

const FILE = 'km-ingest-local-registry.json';
export interface LocalExtractorApproval {
  sourceRunId: string;
  extractorProviderId: string;
  approvedBy: string;
  approvedAt: string;
  state: 'approved';
}
interface Registry { version: 1; extractorApprovals: Record<string, LocalExtractorApproval> }
function pathFor(dataDir: string): string { return join(dataDir, FILE); }
function key(runId: string, providerId: string): string { return `${runId.trim()}\0${providerId.trim()}`; }
function readRegistry(dataDir: string): Registry {
  const path = pathFor(dataDir);
  if (!existsSync(path)) return { version: 1, extractorApprovals: {} };
  const value = JSON.parse(readFileSync(path, 'utf8')) as Registry;
  if (value.version !== 1 || !value.extractorApprovals || typeof value.extractorApprovals !== 'object') throw new Error('km_ingest_local_registry_invalid');
  return value;
}
function writeRegistry(dataDir: string, registry: Registry): void {
  const path = pathFor(dataDir); mkdirSync(dirname(path), { recursive: true });
  const temp = `${path}.${process.pid}.${randomUUID()}.tmp`;
  writeFileSync(temp, `${JSON.stringify(registry, null, 2)}\n`, { mode: 0o600 }); renameSync(temp, path); chmodSync(path, 0o600);
}
export function approveLocalExtractorRun(dataDir: string, input: { sourceRunId: string; extractorProviderId: string; approvedBy: string }): LocalExtractorApproval {
  const sourceRunId = input.sourceRunId.trim(); const extractorProviderId = input.extractorProviderId.trim(); const approvedBy = input.approvedBy.trim();
  if (!sourceRunId || !extractorProviderId || !approvedBy) throw new Error('km_ingest_extractor_approval_invalid');
  const registry = readRegistry(dataDir);
  const approval: LocalExtractorApproval = { sourceRunId, extractorProviderId, approvedBy, approvedAt: new Date().toISOString(), state: 'approved' };
  registry.extractorApprovals[key(sourceRunId, extractorProviderId)] = approval; writeRegistry(dataDir, registry); return approval;
}
export function hasLocalExtractorApproval(dataDir: string, sourceRunId: string, extractorProviderId: string): boolean {
  return readRegistry(dataDir).extractorApprovals[key(sourceRunId, extractorProviderId)]?.state === 'approved';
}
export function listLocalExtractorApprovals(dataDir: string): LocalExtractorApproval[] {
  return Object.values(readRegistry(dataDir).extractorApprovals).sort((a, b) => b.approvedAt.localeCompare(a.approvedAt));
}
