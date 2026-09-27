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
  assert.match(markup, /bounded single-statement SELECT queries only/);
  assert.match(markup, /DDL, DML, persistent-query creation/);
  assert.doesNotMatch(markup, /generic provider forwarding is allowed/i);
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


test('v0.7 W55 policy lookup cannot break read-only Connect detail when mutation mode is absent', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );
  const start = source.indexOf('const openConnector = async');
  const end = source.indexOf('const previewAutoRestart = async', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);

  const openConnector = source.slice(start, end);
  assert.match(openConnector, /getConnectProfileConnector/);
  assert.match(openConnector, /getConnectAutoRestartPolicy/);
  assert.match(openConnector, /reason instanceof MutationApiProblem && reason\.status === 404/);
  assert.doesNotMatch(openConnector, /Promise\.all/);
});


test('v0.7 W56 controlled SerDe UI is bounded, local-only and clears request material', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /Controlled SerDe tooling/);
  assert.match(source, /CBOR, XML and MessagePack/);
  assert.match(source, /decodeControlledSerde/);
  assert.match(source, /encodeControlledSerde/);
  assert.match(source, /setSerdePayloadBase64\(''\)/);
  assert.match(source, /setSerdeStructuredJson\(''\)/);
  assert.doesNotMatch(source, /provider URL/i);
  assert.doesNotMatch(source, /import\([^)]*plugin/i);
  assert.doesNotMatch(source, /script engine/i);
});


test('v0.7 Connect profile loads ignore stale responses after profile switches', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /const connectLoadGeneration = useRef\(0\)/);
  assert.match(source, /const generation = \+\+connectLoadGeneration\.current/);
  assert.match(source, /if \(generation !== connectLoadGeneration\.current\) return/);
  assert.match(source, /if \(generation === connectLoadGeneration\.current\)/);
});

test('v0.7 initial Connect profile rejection is generation-fenced', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  const start = source.indexOf('void kafdeckApi.listConnectProfiles');
  const end = source.indexOf('void kafdeckApi.getKsqlInfo', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);

  const initialLoad = source.slice(start, end);
  assert.match(
    initialLoad,
    /initialConnectGeneration === connectLoadGeneration\.current/,
  );
  assert.match(
    initialLoad,
    /setConnectError\(readViewError\(reason\)\)/,
  );
});
