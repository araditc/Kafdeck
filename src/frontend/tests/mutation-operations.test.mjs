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
test('v0.7 Connect mutations preserve default compatibility and use profile-scoped routes otherwise', () => {
  const workflowSource = readFileSync(
    new URL('../src/features/mutations/MutationPreviewWorkflows.tsx', import.meta.url),
    'utf8',
  );
  const apiSource = readFileSync(
    new URL('../src/features/mutations/mutationApi.ts', import.meta.url),
    'utf8',
  );
  const readViewsSource = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(workflowSource, /connectProfileId === 'default'/);
  assert.match(workflowSource, /previewConnectProfileConfiguration/);
  assert.match(workflowSource, /previewConnectProfileControl/);
  assert.match(workflowSource, /previewConnectProfileDelete/);
  assert.match(apiSource, /connect\/profiles\/\$\{encodeURIComponent\(connectProfileId\)\}\/connectors/);
  assert.doesNotMatch(apiSource, /providerUrl/i);
  assert.doesNotMatch(apiSource, /providerMethod/i);

  assert.match(readViewsSource, /setPluginConfiguration\(''\)/);
  assert.match(readViewsSource, /Values are sent only to the selected configured Connect profile/);
});

test('v0.7 Connect smart form is evidence-driven and clears in-memory values after validation', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /Smart configuration form/);
  assert.match(source, /field\.type\.toUpperCase\(\) === 'PASSWORD' \? 'password' : 'text'/);
  assert.match(source, /field\.recommendedValues\.length > 0/);
  assert.match(source, /setPluginFieldValues\(\{\}\)/);
  assert.match(source, /Smart-form values remain in browser memory only and are cleared/);
});

test('v0.7 W57 data-job UI uses finite governed routes and explicit reconciliation', () => {
  const workflowSource = readFileSync(
    new URL('../src/features/mutations/MutationPreviewWorkflows.tsx', import.meta.url),
    'utf8',
  );
  const panelSource = readFileSync(
    new URL('../src/features/mutations/MutationOperationsPanel.tsx', import.meta.url),
    'utf8',
  );
  const apiSource = readFileSync(
    new URL('../src/features/mutations/mutationApi.ts', import.meta.url),
    'utf8',
  );

  assert.match(workflowSource, /Replay \/ forward data job/);
  assert.match(workflowSource, /Ranges are frozen at preview/);
  assert.match(workflowSource, /max="1000000"/);
  assert.match(workflowSource, /max="1073741824"/);
  assert.match(workflowSource, /max="86400"/);
  assert.match(workflowSource, /MaskedStructuredProjection/);
  assert.doesNotMatch(workflowSource, /script editor/i);

  assert.match(apiSource, /\/api\/v1\/data-jobs\/\$\{encodeURIComponent\(kind\)\}\/preview/);
  assert.match(apiSource, /\/api\/v1\/data-jobs\/\$\{encodeURIComponent\(operationId\)\}\/cancel/);
  assert.match(apiSource, /provenNonApplication/);

  assert.match(panelSource, /selected\.operationKind !== 'dataJob'/);
  assert.match(panelSource, /Start governed data job/);
  assert.match(panelSource, /Unresolved external write/);
  assert.match(panelSource, /must not be blindly replayed/);
  assert.match(panelSource, /Reconcile proven non-application/);
});
