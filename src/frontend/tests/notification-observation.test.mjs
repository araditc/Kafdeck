import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { NotificationObservationPanel } from '../dist/test-source/features/notifications/NotificationObservationPanel.js';
import { AppShell } from '../dist/test-source/app/AppShell.js';

function subscriptionPage(overrides = {}) {
  return {
    items: [{
      subscriptionId: 'w66-subscription',
      destinationId: 'approved-destination',
      state: 'Active',
      revision: 3,
      updatedAtUtc: '2026-10-08T12:00:00Z',
      eventClasses: ['Operations'],
      eventTypes: ['broker.health.degraded'],
      providerRequestPayload: 'TOP_SECRET_SENTINEL',
    }],
    truncated: false,
    nextSubscriptionId: null,
    authorizationFiltered: false,
    continuationRestricted: false,
    ...overrides,
  };
}

test('W66 panel projects only authorized metadata, never an extra provider payload', () => {
  const markup = renderToStaticMarkup(React.createElement(
    NotificationObservationPanel, { initialPage: subscriptionPage() },
  ));
  assert.match(markup, /Notification observation — v0\.8/);
  assert.match(markup, /w66-subscription/);
  assert.match(markup, /approved-destination/);
  assert.match(markup, /Delivery evidence lookup/);
  assert.match(markup, /Notification UUID/);
  assert.match(markup, /Look up evidence/);
  assert.doesNotMatch(markup, /TOP_SECRET_SENTINEL/);
  assert.doesNotMatch(markup, /Load more subscriptions/);
});

test('W66 security-filtered pagination does not offer hidden cursor continuation', () => {
  const markup = renderToStaticMarkup(React.createElement(
    NotificationObservationPanel,
    { initialPage: subscriptionPage({
      truncated: true,
      authorizationFiltered: true,
      continuationRestricted: true,
      nextSubscriptionId: 'hidden-internal-id',
    }) },
  ));
  assert.match(markup, /Some subscriptions are hidden/);
  assert.match(markup, /restricted cursor/);
  assert.doesNotMatch(markup, /hidden-internal-id/);
  assert.doesNotMatch(markup, /Load more subscriptions/);
});

test('W66 UI is absent from the unverified app shell', () => {
  const markup = renderToStaticMarkup(React.createElement(AppShell));
  assert.doesNotMatch(markup, /href="#notifications"/);
  assert.doesNotMatch(markup, /Notification observation — v0\.8/);
});
