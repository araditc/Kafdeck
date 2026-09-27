import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const frontendRoot = fileURLToPath(new URL('..', import.meta.url));
const sourceRoot = path.join(frontendRoot, 'src');

async function sourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(entries.map(async entry => {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(full);
    return /\.(?:ts|tsx|js|jsx|html)$/.test(entry.name) ? [full] : [];
  }));
  return nested.flat();
}

test('frontend source does not persist secrets or payloads in browser storage', async () => {
  for (const file of await sourceFiles(sourceRoot)) {
    const source = await readFile(file, 'utf8');
    assert.doesNotMatch(
      source,
      /\b(?:localStorage|sessionStorage)\b/,
      `${path.relative(frontendRoot, file)} must not use persistent browser storage`,
    );
  }
});

test('frontend entry document has no runtime CDN script or stylesheet dependency', async () => {
  const source = await readFile(path.join(frontendRoot, 'index.html'), 'utf8');
  assert.doesNotMatch(source, /<(?:script|link)\b[^>]+(?:src|href)=["']https?:\/\//i);
});
