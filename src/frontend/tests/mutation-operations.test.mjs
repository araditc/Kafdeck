import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { MutationOperationsPanel } from '../dist/test-source/features/mutations/MutationOperationsPanel.js';

function renderPanel(enabled = true) {
  return renderToStaticMarkup(React.createElement(MutationOperationsPanel, { clusterId: 'prod', enabled }));
}

test('v0.5 mutation UI exposes governed lifecycle, typed preview and independent approval landmarks', () => {
  const markup = renderPanel();

  assert.match(markup, /id="mutations"/);
  assert.match(markup, /Governed mutations/);
  assert.match(markup, /Create governed preview/);
  assert.match(markup, /Topic administration/);
  assert.match(markup, /Record production/);
  assert.match(markup, /Consumer administration/);
  assert.match(markup, /Schema Registry/);
  assert.match(markup, /Kafka Connect/);
  assert.match(markup, /Controlled purge/);
  assert.match(markup, /Operation status/);
  assert.match(markup, /Independent approval inbox/);
  assert.match(markup, /currently authorized/);
  assert.match(markup, /frozen previews/);
  assert.match(markup, /Unknown or partial outcomes are never presented as safe retries/);
});

test('v0.5 mutation UI is omitted when mutation capability is disabled', () => {
  const markup = renderPanel(false);

  assert.equal(markup, '');
});

test('v0.5 mutation preview controls have explicit labels and bounded governance guidance', () => {
  const markup = renderPanel();

  for (const label of [
    'mutation-workflow',
    'mutation-idempotency-key',
    'topic-mutation-action',
    'topic-mutation-name',
    'topic-partitions',
    'topic-replication-factor',
  ]) {
    assert.match(markup, new RegExp(`for="${label}"`));
    assert.match(markup, new RegExp(`id="${label}"`));
  }

  assert.match(markup, /Idempotency-Key/);
  assert.match(markup, /server-admitted topic configuration keys/);
});

test('v0.5 mutation UI does not expose a generic provider or raw execution-material console', () => {
  const markup = renderPanel();

  for (const forbidden of [
    'AdminClient console',
    'Kafka CLI',
    'arbitrary command',
    'SQL editor',
    'shell command',
    'raw execution material',
  ]) {
    assert.doesNotMatch(markup, new RegExp(forbidden, 'i'));
  }

  assert.match(markup, /does not provide a generic provider command console/);
});

test('v0.5 idempotency key generation never falls back to insecure randomness', () => {
  const source = readFileSync(
    new URL('../src/features/mutations/MutationPreviewWorkflows.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /crypto\.randomUUID/);
  assert.doesNotMatch(source, /Math\.random/);
});