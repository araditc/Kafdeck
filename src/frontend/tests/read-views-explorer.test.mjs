import assert from 'node:assert/strict';
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
  assert.match(markup, /does not execute SQL or metadata statements/);
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
