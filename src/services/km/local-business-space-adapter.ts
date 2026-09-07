import { createHash, randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve, sep } from 'node:path';
import { canonicalJsonStringify } from '../../utils/canonical-json.js';
import type { KmIngestRunReport, KmIngestTargetRecord } from './observation-store.js';

export const LOCAL_BUSINESS_SPACE_CONTRACT = 'botmux.local-business-space.v1';
export interface LocalBusinessSpaceAck {
  contract: typeof LOCAL_BUSINESS_SPACE_CONTRACT;
  requestId: string;
  planHash: string;
  targetId: string;
  spaceId: string;
  status: 'accepted';
  accepted: Array<{ canonicalKey: string; remoteId: string; version: string }>;
  rejected: [];
  ackId: string;
  backupId: string;
}
interface SpaceIndex { schemaVersion: 1; spaceId: string; entries: Record<string,{ file: string; contentHash: string; version: string; updatedAt: string }> }

export function executeLocalBusinessSpaceIngest(input: { dataDir: string; target: KmIngestTargetRecord; report: KmIngestRunReport; actorId: string }): LocalBusinessSpaceAck {
  const { target, report } = input;
  if (target.state !== 'ready' || target.target.transport !== 'local-space' || target.target.dryRunOnly) throw new Error('km_ingest_local_space_target_not_enabled');
  if (target.targetHash !== report.run.plan.targetHash) throw new Error('km_ingest_target_hash_mismatch');
  if (!['approved','partial','failed'].includes(report.run.state)) throw new Error(`km_ingest_local_space_requires_approval:${report.run.state}`);
  const spaceId = localSpaceId(target.target.endpointRef); const root = contained(resolve(input.dataDir, 'km-business-spaces'), resolve(input.dataDir, 'km-business-spaces', spaceId));
  mkdirSync(join(root, 'entries'), { recursive: true }); mkdirSync(join(root, '.backups'), { recursive: true });
  const indexPath = join(root, 'INDEX.json'); const index = readIndex(indexPath, spaceId); const now = new Date().toISOString();
  const pending = report.items.filter(item => item.state === 'pending' || item.state === 'failed');
  if (!pending.length) throw new Error('km_ingest_local_space_no_pending_items');
  for (const item of pending) {
    const existing = index.entries[item.canonicalKey];
    if (existing && existing.contentHash !== item.candidateHash) throw new Error(`km_ingest_local_space_canonical_conflict:${item.canonicalKey}`);
  }
  const backupId = `backup_${Date.now()}_${randomUUID().slice(0,8)}`; const backupRoot = join(root, '.backups', backupId); mkdirSync(backupRoot, { recursive: true });
  if (existsSync(indexPath)) writeAtomic(join(backupRoot, 'INDEX.json'), readFileSync(indexPath));
  const accepted: LocalBusinessSpaceAck['accepted'] = [];
  try {
    for (const item of pending) {
      const file = `entries/${createHash('sha256').update(item.canonicalKey).digest('hex')}.json`;
      const entryPath = contained(root, resolve(root, file));
      if (existsSync(entryPath)) writeAtomic(join(backupRoot, file), readFileSync(entryPath));
      const version = item.candidateHash;
      const document = { schemaVersion:1, contract:LOCAL_BUSINESS_SPACE_CONTRACT, spaceId, canonicalKey:item.canonicalKey, contentHash:item.candidateHash,
        version, planHash:report.run.planHash, sourceRunId:report.run.plan.sourceRunId, actorId:input.actorId, updatedAt:now, knowledge:item.candidate };
      writeAtomic(entryPath, Buffer.from(`${canonicalJsonStringify(document)}\n`));
      index.entries[item.canonicalKey] = { file, contentHash:item.candidateHash, version, updatedAt:now };
      accepted.push({ canonicalKey:item.canonicalKey, remoteId:`${spaceId}:${item.canonicalKey}`, version });
    }
    writeAtomic(indexPath, Buffer.from(`${JSON.stringify(index,null,2)}\n`));
    const requestId = `kmls_${createHash('sha256').update(`${report.run.runId}|${report.run.planHash}`).digest('hex')}`;
    const ack: LocalBusinessSpaceAck = { contract:LOCAL_BUSINESS_SPACE_CONTRACT, requestId,planHash:report.run.planHash,targetId:report.run.targetId,spaceId,status:'accepted',accepted,rejected:[],ackId:`ack_${randomUUID()}`,backupId };
    writeAtomic(join(backupRoot, 'ack.json'), Buffer.from(`${JSON.stringify(ack,null,2)}\n`)); return ack;
  } catch (error) { restoreBackup(root, backupRoot, accepted.map(item => item.canonicalKey)); throw error; }
}

export function rollbackLocalBusinessSpace(input:{dataDir:string;spaceId:string;backupId:string}):void {
  const root=contained(resolve(input.dataDir,'km-business-spaces'),resolve(input.dataDir,'km-business-spaces',localSpaceId(`space:${input.spaceId}`)));
  const backup=contained(join(root,'.backups'),resolve(root,'.backups',input.backupId)); if(!existsSync(backup))throw new Error('km_ingest_local_space_backup_missing');
  const ack=JSON.parse(readFileSync(join(backup,'ack.json'),'utf8')) as LocalBusinessSpaceAck; restoreBackup(root,backup,ack.accepted.map(item=>item.canonicalKey));
}
function readIndex(path:string,spaceId:string):SpaceIndex { if(!existsSync(path))return{schemaVersion:1,spaceId,entries:{}}; const x=JSON.parse(readFileSync(path,'utf8')) as SpaceIndex;if(x.schemaVersion!==1||x.spaceId!==spaceId||!x.entries)throw new Error('km_ingest_local_space_index_invalid');return x; }
function localSpaceId(ref:string):string { const id=ref.startsWith('space:')?ref.slice(6):'';if(!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(id))throw new Error('km_ingest_local_space_id_invalid');return id; }
function contained(root:string,path:string):string { if(path!==root&&!path.startsWith(`${root}${sep}`))throw new Error('km_ingest_local_space_path_escape');return path; }
function writeAtomic(path:string,data:Buffer):void { mkdirSync(dirname(path),{recursive:true});const tmp=`${path}.${process.pid}.${randomUUID()}.tmp`;writeFileSync(tmp,data,{mode:0o600});renameSync(tmp,path); }
function restoreBackup(root:string,backup:string,canonicalKeys:string[]):void {
  for(const key of canonicalKeys){const name=`${createHash('sha256').update(key).digest('hex')}.json`;rmSync(join(root,'entries',name),{force:true});}
  const entries=join(backup,'entries');if(existsSync(entries)){for(const name of readdirSync(entries)){const f=join(entries,name);if(existsSync(f))writeAtomic(join(root,'entries',name),readFileSync(f));}}
  const index=join(backup,'INDEX.json');if(existsSync(index))writeAtomic(join(root,'INDEX.json'),readFileSync(index));else rmSync(join(root,'INDEX.json'),{force:true});
}
