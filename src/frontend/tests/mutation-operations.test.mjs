import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { MutationOperationsPanel } from '../dist/test-source/features/mutations/MutationOperationsPanel.js';

test('v0.5 mutation UI exposes governed lifecycle and independent approval landmarks', () => {
  const markup = renderToStaticMarkup(React.createElement(MutationOperationsPanel));

  assert.match(markup, /id="mutations"/);
  assert.match(markup, /Governed mutations/);
  assert.match(markup, /Operation status/);
  assert.match(markup, /Independent approval inbox/);
  assert.match(markup, /currently authorized/);
  assert.match(markup, /frozen previews/);
  assert.match(markup, /Unknown or partial outcomes are never presented as safe retries/);
});

test('v0.5 mutation UI does not expose a generic provider or execution-material console', () => {
  const markup = renderToStaticMarkup(React.createElement(MutationOperationsPanel));

  for (const forbidden of [
    'AdminClient console',
    'Kafka CLI',
    'provider command',
    'arbitrary command',
    'SQL editor',
    'shell command',
    'raw execution material',
  ]) {
    assert.doesNotMatch(markup, new RegExp(forbidden, 'i'));
  }
});
