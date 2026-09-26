import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { AppShell } from '../dist/test-source/app/AppShell.js';
import { describeObservation, selectCluster, shouldAutoRefresh, visibleRefreshIntervalMs } from '../dist/test-source/app/operatorState.js';

test('AppShell exposes product identity and accessible operator landmarks', () => {
  const markup = renderToStaticMarkup(React.createElement(AppShell));

  assert.match(markup, /<main[^>]+class="page-wrapper"[^>]+aria-labelledby="kafdeck-title"/);
  assert.match(markup, /class="navbar[^"]*kafdeck-topbar/);
  assert.match(markup, /class="kafdeck-brand-logo" src="\/kafdeck-logo\.webp" alt="Kafdeck" width="42" height="42"/);
  assert.match(markup, /Open Source Kafka Control Plane/);
  assert.match(markup, /class="form-label" for="cluster-selector">Cluster<\/label>/);
  assert.match(markup, /aria-label="Kafdeck sections"/);
  assert.match(markup, /class="nav nav-pills py-2"/);
  assert.match(markup, /href="#consumers"/);
  assert.match(markup, /href="#schemas"/);
  assert.match(markup, /href="#ecosystem"/);
  assert.match(markup, /href="#fleet"/);
  assert.match(markup, /Fleet Operations — v0\.6/);
  assert.match(markup, /Loading fleet capability status/);
});

test('cluster switching clears cluster-scoped state', () => {
  const previous = {
    selectedClusterId: 'prod',
    cluster: { data: { clusterId: 'prod' } },
    topicQuery: 'payments',
    topicCursor: 'cursor-from-prod',
  };

  const next = selectCluster(previous, 'dr');
  assert.equal(next.selectedClusterId, 'dr');
  assert.equal(next.cluster, null);
  assert.equal(next.topicQuery, '');
  assert.equal(next.topicCursor, null);
});

test('observation labels never hide stale or partial state', () => {
  assert.equal(describeObservation({ freshness: 'fresh', partial: false }), 'Current');
  assert.equal(describeObservation({ freshness: 'stale', partial: false }), 'Stale');
  assert.equal(describeObservation({ freshness: 'fresh', partial: true }), 'Partial');
  assert.equal(describeObservation({ freshness: 'stale', partial: true }), 'Partial · stale');
});

test('automatic refresh is bounded and pauses while the page is hidden', () => {
  assert.equal(visibleRefreshIntervalMs, 10_000);
  assert.equal(shouldAutoRefresh('visible'), true);
  assert.equal(shouldAutoRefresh('hidden'), false);
});
