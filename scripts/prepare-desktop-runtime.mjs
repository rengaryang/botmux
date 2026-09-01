#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import https from 'node:https';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const runtimeDir = join(root, 'build', 'desktop-runtime');
const nodeDir = join(root, 'build', 'desktop-node');
const nodeVersion = process.env.BOTMUX_DESKTOP_NODE_VERSION || '22.20.0';
const knownTargets = ['darwin-arm64', 'darwin-x64', 'linux-x64', 'linux-arm64', 'win32-x64'];
const targets = resolveTargets();
const pinnedChecksums = {
  'node-v22.20.0-darwin-arm64.tar.gz': 'cc04a76a09f79290194c0646f48fec40354d88969bec467789a5d55dd097f949',
  'node-v22.20.0-darwin-x64.tar.gz': '00df9c5df3e4ec6848c26b70fb47bf96492f342f4bed6b17f12d99b3a45eeecc',
  'node-v22.20.0-linux-arm64.tar.gz': '4181609e03dcb9880e7e5bf956061ecc0503c77a480c6631d868cb1f65a2c7dd',
  'node-v22.20.0-linux-x64.tar.gz': 'eeaccb0378b79406f2208e8b37a62479c70595e20be6b659125eb77dd1ab2a29',
  'node-v22.20.0-win-x64.zip': 'bb819d6eb8f5bfda294bbc83a7e4ec6539da67c4233d54b0d655b9248b15e29d',
};

await stageBotmuxRuntime();
await stageNodeRuntimes();

function resolveTargets() {
  const raw = process.env.BOTMUX_DESKTOP_TARGETS?.trim();
  const selected = raw ? raw.split(',').map(value => value.trim()).filter(Boolean) : defaultHostTargets();
  const unique = [...new Set(selected)];
  for (const target of unique) {
    if (!knownTargets.includes(target)) throw new Error(`Unsupported desktop target: ${target}`);
  }
  if (unique.length === 0) throw new Error('BOTMUX_DESKTOP_TARGETS resolved to an empty target list');
  return unique;
}

function defaultHostTargets() {
  const platform = process.platform === 'win32' ? 'win32' : process.platform;
  if (platform === 'darwin') return ['darwin-arm64', 'darwin-x64'];
  return [`${platform}-${process.arch}`];
}

async function stageBotmuxRuntime() {
  await rm(runtimeDir, { recursive: true, force: true });
  await mkdir(runtimeDir, { recursive: true });

  const pkg = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
  const stagedVersion = normalizeVersion(process.env.BOTMUX_DESKTOP_VERSION);
  if (stagedVersion) pkg.version = stagedVersion;
  const supportedOs = [...new Set(targets.map(target => target.split('-')[0]))];
  const supportedCpu = [...new Set(targets.map(target => target.split('-').at(-1)))];
  pkg.pnpm = { ...(pkg.pnpm ?? {}), supportedArchitectures: { os: supportedOs, cpu: supportedCpu } };
  delete pkg.scripts;
  await writeFile(join(runtimeDir, 'package.json'), `${JSON.stringify(pkg, null, 2)}\n`);
  await cp(join(root, 'pnpm-lock.yaml'), join(runtimeDir, 'pnpm-lock.yaml'));
  await writeFile(join(runtimeDir, 'pnpm-workspace.yaml'), [
    'packages:', "  - '.'", 'supportedArchitectures:', '  os:',
    ...supportedOs.map(os => `    - ${os}`), '  cpu:', ...supportedCpu.map(cpu => `    - ${cpu}`), '',
  ].join('\n'));

  run('pnpm', ['install', '--prod', '--frozen-lockfile', '--ignore-scripts'], runtimeDir);
  // node-pty has no Linux prebuild. Build it on each native Linux runner; macOS
  // and Windows use the package's architecture-qualified prebuilds.
  if (targets.some(target => target.startsWith('linux-'))) {
    run('pnpm', ['rebuild', 'node-pty'], runtimeDir);
  }
  await assertBundledCanvasTargets();
  await assertBundledPtyTargets();
  run('tar', ['-czf', 'node_modules.tar.gz', 'node_modules'], runtimeDir);
  await rm(join(runtimeDir, 'node_modules'), { recursive: true, force: true });
  const distDir = join(root, 'dist');
  await cp(distDir, join(runtimeDir, 'dist'), { recursive: true, filter: source => isRuntimeDistPath(distDir, source) });
}

async function assertBundledCanvasTargets() {
  const entries = await readdir(join(runtimeDir, 'node_modules', '.pnpm'));
  for (const target of targets) {
    const [os, arch] = target.split('-');
    const packageOs = os === 'win32' ? 'win32' : os;
    const prefix = `@napi-rs+canvas-${packageOs}-${arch}`;
    if (!entries.some(entry => entry.startsWith(prefix))) {
      throw new Error(`Bundled runtime is missing @napi-rs/canvas-${packageOs}-${arch}`);
    }
  }
}

async function assertBundledPtyTargets() {
  const ptyRoot = join(runtimeDir, 'node_modules', 'node-pty');
  for (const target of targets) {
    const [os, arch] = target.split('-');
    const candidates = os === 'linux'
      ? [join(ptyRoot, 'build', 'Release', 'pty.node')]
      : os === 'win32'
        ? [join(ptyRoot, 'prebuilds', `win32-${arch}`, 'pty.node'), join(ptyRoot, 'prebuilds', `win32-${arch}`, 'conpty.node')]
        : [join(ptyRoot, 'prebuilds', `darwin-${arch}`, 'pty.node'), join(ptyRoot, 'prebuilds', `darwin-${arch}`, 'spawn-helper')];
    for (const candidate of candidates) {
      try { await readFile(candidate); } catch { throw new Error(`Bundled runtime is missing native PTY asset: ${candidate}`); }
    }
  }
}

function normalizeVersion(value) {
  const version = String(value ?? '').trim().replace(/^v/, '');
  return /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version) ? version : null;
}

function isRuntimeDistPath(distDir, source) {
  const path = relative(distDir, source);
  if (!path) return true;
  const top = path.split(sep)[0];
  if (top === 'desktop' || top === '.icon-icns' || top.startsWith('mac') || top.startsWith('win-') || top.startsWith('linux-')) return false;
  return !/\.(?:dmg|zip|exe|AppImage|deb|rpm|tar\.gz|blockmap)$/i.test(top) && !top.startsWith('builder-');
}

async function stageNodeRuntimes() {
  await rm(nodeDir, { recursive: true, force: true });
  await mkdir(nodeDir, { recursive: true });
  const cacheDir = join(homedir(), '.cache', 'botmux-desktop-node', `v${nodeVersion}`);
  await mkdir(cacheDir, { recursive: true });
  let sums;

  for (const target of targets) {
    const nodePlatform = target.startsWith('win32-') ? target.replace('win32-', 'win-') : target;
    const extension = nodePlatform.startsWith('win-') ? 'zip' : 'tar.gz';
    const filename = `node-v${nodeVersion}-${nodePlatform}.${extension}`;
    const expected = pinnedChecksums[filename]
      ?? checksumFor(sums ??= await fetchText(`https://nodejs.org/dist/v${nodeVersion}/SHASUMS256.txt`), filename);
    const archive = join(cacheDir, filename);
    if (!(await fileMatches(archive, expected))) {
      await rm(archive, { force: true });
      await download(`https://nodejs.org/dist/v${nodeVersion}/${filename}`, archive);
      if (!(await fileMatches(archive, expected))) throw new Error(`Node checksum mismatch: ${filename}`);
    }

    const extracted = await mkdtemp(join(tmpdir(), 'botmux-node-'));
    try {
      if (extension === 'zip') {
        runPowerShell(['-NoProfile', '-Command', `Expand-Archive -LiteralPath '${escapePowerShell(archive)}' -DestinationPath '${escapePowerShell(extracted)}' -Force`]);
        const source = join(extracted, `node-v${nodeVersion}-${nodePlatform}`);
        const targetDir = join(nodeDir, target);
        await mkdir(targetDir, { recursive: true });
        await cp(join(source, 'node.exe'), join(targetDir, 'node.exe'));
        await cp(join(source, 'LICENSE'), join(targetDir, 'LICENSE'));
      } else {
        run('tar', ['-xzf', archive, '-C', extracted, '--strip-components=1'], root);
        const targetDir = join(nodeDir, target);
        await mkdir(join(targetDir, 'bin'), { recursive: true });
        await cp(join(extracted, 'bin', 'node'), join(targetDir, 'bin', 'node'));
        await cp(join(extracted, 'LICENSE'), join(targetDir, 'LICENSE'));
      }
    } finally {
      await rm(extracted, { recursive: true, force: true });
    }
  }
}

function escapePowerShell(value) { return value.replaceAll("'", "''"); }
function runPowerShell(args) { run(process.platform === 'win32' ? 'powershell.exe' : 'pwsh', args, root); }
function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, stdio: 'inherit', shell: false });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} exited with ${result.status ?? 1}`);
}
function checksumFor(sums, filename) {
  const line = sums.split(/\r?\n/).find(candidate => candidate.endsWith(`  ${filename}`));
  if (!line) throw new Error(`Checksum not found for ${filename}`);
  return line.split(/\s+/)[0];
}
async function fileMatches(path, expected) {
  try { return createHash('sha256').update(await readFile(path)).digest('hex') === expected; } catch { return false; }
}
async function fetchText(url) {
  const chunks = []; for await (const chunk of await request(url)) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString('utf8');
}
async function download(url, destination) {
  await mkdir(dirname(destination), { recursive: true });
  await pipeline(await request(url), createWriteStream(destination, { mode: 0o644 }));
}
function request(url) {
  return new Promise((resolveRequest, reject) => {
    https.get(url, response => {
      if (response.statusCode && response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
        response.resume(); resolveRequest(request(new URL(response.headers.location, url).toString())); return;
      }
      if (response.statusCode !== 200) { response.resume(); reject(new Error(`GET ${url} failed: ${response.statusCode}`)); return; }
      resolveRequest(response);
    }).on('error', reject);
  });
}
