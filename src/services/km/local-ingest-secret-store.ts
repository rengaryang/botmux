import { createCipheriv, createDecipheriv, randomBytes, randomUUID } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

const KEY_FILE = 'km-ingest-local-master.key';
const STORE_FILE = 'km-ingest-local-secrets.json';

interface SecretRecord {
  ref: string;
  alg: 'aes-256-gcm';
  iv: string;
  tag: string;
  ciphertext: string;
  createdAt: string;
  updatedAt: string;
}
interface SecretStoreFile { version: 1; secrets: Record<string, SecretRecord> }
export interface LocalIngestSecretMetadata { ref: string; configured: boolean; createdAt: string; updatedAt: string }

function assertRef(ref: string): string {
  const value = ref.trim();
  if (!/^local-secret:[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(value)) throw new Error('km_ingest_local_secret_ref_invalid');
  return value;
}
function keyPath(dataDir: string): string { return join(dataDir, KEY_FILE); }
function storePath(dataDir: string): string { return join(dataDir, STORE_FILE); }
function readOrCreateKey(dataDir: string): Buffer {
  const path = keyPath(dataDir);
  if (existsSync(path)) {
    const key = Buffer.from(readFileSync(path, 'utf8').trim(), 'base64url');
    if (key.length !== 32) throw new Error('km_ingest_local_master_key_invalid');
    return key;
  }
  mkdirSync(dirname(path), { recursive: true });
  const key = randomBytes(32);
  writeFileSync(path, `${key.toString('base64url')}\n`, { mode: 0o600 });
  chmodSync(path, 0o600);
  return key;
}
function readStore(dataDir: string): SecretStoreFile {
  const path = storePath(dataDir);
  if (!existsSync(path)) return { version: 1, secrets: {} };
  const parsed = JSON.parse(readFileSync(path, 'utf8')) as Partial<SecretStoreFile>;
  if (parsed.version !== 1 || !parsed.secrets || typeof parsed.secrets !== 'object') throw new Error('km_ingest_local_secret_store_invalid');
  return { version: 1, secrets: parsed.secrets };
}
function writeStore(dataDir: string, store: SecretStoreFile): void {
  const path = storePath(dataDir);
  mkdirSync(dirname(path), { recursive: true });
  const temp = `${path}.${process.pid}.${randomUUID()}.tmp`;
  writeFileSync(temp, `${JSON.stringify(store, null, 2)}\n`, { mode: 0o600 });
  renameSync(temp, path);
  chmodSync(path, 0o600);
}
function encrypt(value: string, key: Buffer): Pick<SecretRecord, 'alg'|'iv'|'tag'|'ciphertext'> {
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()]);
  return { alg: 'aes-256-gcm', iv: iv.toString('base64url'), tag: cipher.getAuthTag().toString('base64url'), ciphertext: ciphertext.toString('base64url') };
}
function decrypt(record: SecretRecord, key: Buffer): string {
  const decipher = createDecipheriv('aes-256-gcm', key, Buffer.from(record.iv, 'base64url'));
  decipher.setAuthTag(Buffer.from(record.tag, 'base64url'));
  return Buffer.concat([decipher.update(Buffer.from(record.ciphertext, 'base64url')), decipher.final()]).toString('utf8');
}

export function setLocalIngestSecret(dataDir: string, ref: string, plaintext: string): LocalIngestSecretMetadata {
  const normalizedRef = assertRef(ref);
  if (!plaintext) throw new Error('km_ingest_local_secret_value_required');
  const store = readStore(dataDir);
  const now = new Date().toISOString();
  const prior = store.secrets[normalizedRef];
  store.secrets[normalizedRef] = { ref: normalizedRef, ...encrypt(plaintext, readOrCreateKey(dataDir)), createdAt: prior?.createdAt ?? now, updatedAt: now };
  writeStore(dataDir, store);
  return { ref: normalizedRef, configured: true, createdAt: store.secrets[normalizedRef].createdAt, updatedAt: now };
}
export function getLocalIngestSecret(dataDir: string, ref: string): string | null {
  const record = readStore(dataDir).secrets[assertRef(ref)];
  return record ? decrypt(record, readOrCreateKey(dataDir)) : null;
}
export function hasLocalIngestSecret(dataDir: string, ref: string): boolean {
  try { return Boolean(readStore(dataDir).secrets[assertRef(ref)]); } catch { return false; }
}
export function listLocalIngestSecrets(dataDir: string): LocalIngestSecretMetadata[] {
  return Object.values(readStore(dataDir).secrets).map(record => ({ ref: record.ref, configured: true, createdAt: record.createdAt, updatedAt: record.updatedAt }));
}
