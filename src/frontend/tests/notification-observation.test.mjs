import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { NotificationObservationPanel } from '../dist/test-source/features/notifications/NotificationObservationPanel.js';
import { AppShell } from '../dist/test-source/app/AppShell.js';
import { ApiProblem, kafdeckApi, operatorSessionLostEvent } from '../dist/test-source/shared/api.js';
import { mutationApi, MutationApiProblem } from '../dist/test-source/features/mutations/mutationApi.js';

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

test('a 401 from any typed notification GET broadcasts session loss before retaining evidence', async () => {
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  const events = [];
  try {
    globalThis.window = {
      location: { hash: '', pathname: '/', search: '' },
      dispatchEvent: event => { events.push(event.type); return true; },
    };
    globalThis.fetch = async () => new Response(
      JSON.stringify({ title: 'Authentication required' }),
      { status: 401, headers: { 'Content-Type': 'application/problem+json' } },
    );
    await assert.rejects(
      kafdeckApi.getNotificationSubscription('visible-before-expiration'),
      error => error instanceof ApiProblem && error.status === 401,
    );
    assert.deepEqual(events, [operatorSessionLostEvent]);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});

test('resource-scoped 403 does not incorrectly represent the whole OIDC session as expired', async () => {
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  const events = [];
  try {
    globalThis.window = {
      location: { hash: '', pathname: '/', search: '' },
      dispatchEvent: event => { events.push(event.type); return true; },
    };
    globalThis.fetch = async () => new Response(
      JSON.stringify({ title: 'Access denied' }),
      { status: 403, headers: { 'Content-Type': 'application/problem+json' } },
    );
    await assert.rejects(
      kafdeckApi.getNotificationSubscription('not-authorized'),
      error => error instanceof ApiProblem && error.status === 403,
    );
    assert.deepEqual(events, []);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});

test('a 401 from the independently implemented mutation GET also revokes notification identity', async () => {
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  const events = [];
  try {
    globalThis.window = {
      location: { hash: '', pathname: '/', search: '' },
      dispatchEvent: event => { events.push(event.type); return true; },
    };
    globalThis.fetch = async () => new Response(
      JSON.stringify({ title: 'OIDC identity expired' }),
      { status: 401, headers: { 'Content-Type': 'application/problem+json' } },
    );
    await assert.rejects(
      mutationApi.get('existing-operation'),
      error => error instanceof MutationApiProblem && error.status === 401,
    );
    assert.deepEqual(events, [operatorSessionLostEvent]);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});

test('a mutation POST CSRF preflight 401 also revokes the operator session', async () => {
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  const events = [];
  try {
    globalThis.window = {
      location: { hash: '', pathname: '/', search: '' },
      dispatchEvent: event => { events.push(event.type); return true; },
    };
    globalThis.fetch = async () => new Response(
      JSON.stringify({ title: 'Session expired' }),
      { status: 401, headers: { 'Content-Type': 'application/problem+json' } },
    );
    await assert.rejects(
      mutationApi.confirm({ operationId: 'existing-operation', previewHash: 'bound' }, null),
      error => error instanceof MutationApiProblem && error.status === 401,
    );
    assert.deepEqual(events, [operatorSessionLostEvent]);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});
