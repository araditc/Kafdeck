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
    let guarded = source;
    if (file === path.join(sourceRoot, 'shared', 'i18n.ts')) {
      // This is a call-level exception, not an exemption for the module: any
      // alias, extra operation, alternate key or unnormalized value still fails.
      assert.match(source, /export const localePreferenceKey = 'kafdeck\.locale\.v1';/);
      assert.match(source, /const next = normalizeLocale\(value\);/);
      const permitted = [
        'window.localStorage.getItem(localePreferenceKey)',
        'window.localStorage.setItem(localePreferenceKey, next)',
      ];
      for (const call of permitted) {
        assert.equal(source.split(call).length - 1, 1, `Expected exactly one ${call}`);
        guarded = guarded.replace(call, '/* closed language preference operation */');
      }
    }
    assert.doesNotMatch(
      guarded,
      /\b(?:localStorage|sessionStorage)\b/,
      `${path.relative(frontendRoot, file)} must not use persistent browser storage`,
    );
  }
});

test('frontend entry document has no runtime CDN script or stylesheet dependency', async () => {
  const source = await readFile(path.join(frontendRoot, 'index.html'), 'utf8');
  assert.doesNotMatch(source, /<(?:script|link)\b[^>]+(?:src|href)=["']https?:\/\//i);
});
