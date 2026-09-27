import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { ReadViewsExplorer } from '../dist/test-source/features/readviews/ReadViewsExplorer.js';

test('v0.4 read-view UI exposes Consumers, Schemas and Ecosystem landmarks', () => {
  const markup = renderToStaticMarkup(
    React.createElement(ReadViewsExplorer, { clusterId: 'prod' }),
  );

  assert.match(markup, /id="consumers"/);
  assert.match(markup, /Consumer groups/);
  assert.match(markup, /id="schemas"/);
  assert.match(markup, /Schema Registry/);
  assert.match(markup, /id="ecosystem"/);
  assert.match(markup, /Ecosystem read views/);
  assert.match(markup, /no SQL or metadata-statement execution surface/);
});

test('v0.4 initial UI has no mutation controls', () => {
  const markup = renderToStaticMarkup(
    React.createElement(ReadViewsExplorer, { clusterId: 'prod' }),
  );

  for (const forbidden of [
    'Reset offsets',
    'Commit offsets',
    'Restart connector',
    'Pause connector',
    'Resume connector',
    'Delete connector',
    'Create schema',
    'Delete schema',
    'SQL editor',
    'Execute SQL',
  ]) {
    assert.doesNotMatch(markup, new RegExp(forbidden, 'i'));
  }
});

test('v0.7 Connect view starts from the bounded profile catalog instead of assuming one worker', () => {
  const markup = renderToStaticMarkup(
    React.createElement(ReadViewsExplorer, { clusterId: 'prod' }),
  );

  assert.match(markup, /Loading configured Connect profiles/);
  assert.doesNotMatch(markup, /provider URL/i);
  assert.doesNotMatch(markup, /HTTP method/i);
});


test('v0.7 Connect profile switch clears request-scoped plugin material', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );
  const start = source.indexOf('const openConnectProfile = async');
  const end = source.indexOf('const openConnector = async', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);

  const profileSwitch = source.slice(start, end);
  assert.match(profileSwitch, /setPluginConfiguration\(''\)/);
  assert.match(profileSwitch, /setPluginFieldValues\(\{\}\)/);
  assert.match(profileSwitch, /setPluginValidation\(null\)/);
});


test('v0.7 W55 Connect auto-restart UI remains governed and bounded', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /Bounded auto-restart/);
  assert.match(source, /previewConnectAutoRestartPolicy/);
  assert.match(source, /applyConnectAutoRestartPolicy/);
  assert.match(source, /Governed operation/);
  assert.match(source, /autoRestartOperation\.state !== 'ready'/);
  assert.doesNotMatch(source, /fetch\([^\n]*restart/i);
});
