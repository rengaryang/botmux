#!/usr/bin/env node
import { lstat, readdir } from 'node:fs/promises';
import { resolve, relative } from 'node:path';

const roots = process.argv.slice(2);
if (roots.length === 0) {
  console.error('usage: node scripts/diagnose-desktop-release.mjs <path>...');
  process.exit(2);
}

for (const raw of roots) {
  const root = resolve(raw);
  console.log(`\n## ${raw}`);
  try {
    const entries = [];
    await walk(root, root, entries, 0);
    for (const entry of entries.sort((a, b) => a.path.localeCompare(b.path))) {
      console.log(`${entry.type}\t${entry.size}\t${entry.path}`);
    }
  } catch (error) {
    console.log(`missing\t0\t${raw}\t${error instanceof Error ? error.message : String(error)}`);
  }
}

async function walk(root, current, out, depth) {
  if (depth > 7 || out.length >= 1000) return;
  const stats = await lstat(current);
  if (!stats.isDirectory()) {
    out.push({ type: stats.isSymbolicLink() ? 'link' : 'file', size: stats.size, path: relative(process.cwd(), current) });
    return;
  }
  for (const entry of await readdir(current)) {
    await walk(root, resolve(current, entry), out, depth + 1);
    if (out.length >= 1000) break;
  }
}
