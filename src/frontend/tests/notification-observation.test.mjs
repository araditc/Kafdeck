import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { NotificationObservationPanel } from '../dist/test-source/features/notifications/NotificationObservationPanel.js';
import { AppShell, notificationManagementSurfaces } from '../dist/test-source/app/AppShell.js';
import {
  NotificationSubscriptionManagementPanel,
  isValidNotificationDestinationId,
} from '../dist/test-source/features/notifications/NotificationSubscriptionManagementPanel.js';
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


test('W66 delivery history UI starts without privileged provider data or network history', () => {
  const markup = renderToStaticMarkup(React.createElement(
    NotificationObservationPanel, { initialPage: subscriptionPage() },
  ));
  assert.match(markup, /Delivery history by destination/);
  assert.match(markup, /Load destination history/);
  assert.doesNotMatch(markup, /Authorized destination delivery history/);
  assert.doesNotMatch(markup, /TOP_SECRET_SENTINEL/);
});

test('W66 typed history GET uses an encoded destination and a complete bounded cursor', async () => {
  const originalFetch = globalThis.fetch;
  const originalWindow = globalThis.window;
  const requests = [];
  try {
    globalThis.window = { location: { hash: '', pathname: '/', search: '' } };
    globalThis.fetch = async (url, init) => {
      requests.push({ url: String(url), method: init.method });
      return new Response(JSON.stringify({
        items: [], truncated: false, nextCreatedAtUtc: null, nextNotificationId: null,
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    };
    const response = await kafdeckApi.listNotificationDeliveryHistory(
      'approved destination',
      50,
      { createdAtUtc: '2026-10-08T12:00:00Z', notificationId: '00000000-0000-4000-8000-000000000001' },
    );
    const request = new URL(requests[0].url, 'https://kafdeck.example');
    assert.equal(requests[0].method, 'GET');
    assert.equal(request.pathname, '/api/v1/notifications/deliveries/history');
    assert.equal(request.searchParams.get('destinationId'), 'approved destination');
    assert.equal(request.searchParams.get('maxResults'), '50');
    assert.equal(request.searchParams.get('afterCreatedAtUtc'), '2026-10-08T12:00:00Z');
    assert.equal(request.searchParams.get('afterNotificationId'), '00000000-0000-4000-8000-000000000001');
    assert.deepEqual(response.items, []);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});

test('W66 destination history 401 invalidates operator session, unlike resource-only 403', async () => {
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  const events = [];
  try {
    globalThis.window = {
      location: { hash: '', pathname: '/', search: '' },
      dispatchEvent: event => { events.push(event.type); return true; },
    };
    globalThis.fetch = async () => new Response('{}',
      { status: 403, headers: { 'Content-Type': 'application/problem+json' } });
    await assert.rejects(
      kafdeckApi.listNotificationDeliveryHistory('restricted'),
      error => error instanceof ApiProblem && error.status === 403,
    );
    assert.deepEqual(events, []);
    globalThis.fetch = async () => new Response('{}',
      { status: 401, headers: { 'Content-Type': 'application/problem+json' } });
    await assert.rejects(
      kafdeckApi.listNotificationDeliveryHistory('expired'),
      error => error instanceof ApiProblem && error.status === 401,
    );
    assert.deepEqual(events, [operatorSessionLostEvent]);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});

test('W66 subscription management controls require separately admitted capability', () => {
  const readOnly = renderToStaticMarkup(React.createElement(
    NotificationObservationPanel, { initialPage: subscriptionPage() },
  ));
  assert.doesNotMatch(readOnly, /Manage notification subscription/);
  const enabled = renderToStaticMarkup(React.createElement(
    NotificationObservationPanel, { initialPage: subscriptionPage(), canManageSubscriptions: true },
  ));
  assert.match(enabled, /Manage notification subscription/);
  assert.match(enabled, /Review subscription change/);
  assert.doesNotMatch(enabled, /Confirm subscription change/);
  assert.doesNotMatch(enabled, /TOP_SECRET_SENTINEL/);
});

test('W66 management form does not render extraneous provider secret projections', () => {
  const value = { ...subscriptionPage().items[0], secretCredential: 'SENTINEL_PRIVATE_CREDENTIAL' };
  const markup = renderToStaticMarkup(React.createElement(
    NotificationSubscriptionManagementPanel, {
      referenceSubscription: value, onUpdated: () => {},
    },
  ));
  assert.match(markup, /Manage notification subscription/);
  // Every valid 32 x 128-character exact-filter list plus separators must fit
  // without browser-side truncation changing the intended subscription.
  const size = markup.match(/id="notification-manage-types"[^>]*maxlength="(\d+)"/i);
  assert.ok(size && Number(size[1]) >= 32 * 128 + 31);
  assert.doesNotMatch(markup, /SENTINEL_PRIVATE_CREDENTIAL/);
  assert.doesNotMatch(markup, /Confirm subscription change/);
});

test('W66 management capability preflight is GET-only and forbidden does not expire session', async () => {
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  const requests = [];
  const events = [];
  try {
    globalThis.window = {
      location: { hash: '', pathname: '/', search: '' },
      dispatchEvent: event => { events.push(event.type); return true; },
    };
    globalThis.fetch = async (url, init) => {
      requests.push({ url, method: init.method });
      return new Response('{}', { status: 403, headers: { 'Content-Type': 'application/problem+json' } });
    };
    await assert.rejects(kafdeckApi.getNotificationManagementCapabilities(),
      error => error instanceof ApiProblem && error.status === 403);
    assert.deepEqual(requests, [
      { url: '/api/v1/notifications/management/capabilities', method: 'GET' },
    ]);
    assert.deepEqual(events, []);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});

test('W66 typed CAS PUT obtains same-origin antiforgery and never sends provider URLs', async () => {
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  const requests = [];
  try {
    globalThis.window = { location: { hash: '', pathname: '/', search: '' } };
    globalThis.fetch = async (url, init) => {
      requests.push({ url, method: init.method, headers: init.headers, body: init.body });
      if (url === '/api/v1/auth/csrf') return new Response(
        JSON.stringify({ headerName: 'X-CSRF', requestToken: 'opaque-session-token' }),
        { status: 200, headers: { 'Content-Type': 'application/json' } });
      return new Response(JSON.stringify({
        subscriptionId: 'ops-sub', state: 'active', revision: 8,
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    };
    const request = {
      destinationId: 'ops-dest',
      state: 'active',
      eventClasses: ['operational'],
      eventTypes: ['consumer.lag.alert'],
      expectedRevision: 7,
    };
    const result = await kafdeckApi.upsertNotificationSubscription('ops-sub', request);
    assert.equal(result.revision, 8);
    assert.equal(requests[0].url, '/api/v1/auth/csrf');
    assert.equal(requests[0].method, 'GET');
    const put = requests.find(x => x.method === 'PUT');
    assert.ok(put);
    assert.equal(put.url, '/api/v1/notifications/subscriptions/ops-sub');
    assert.equal(put.headers['X-CSRF'], 'opaque-session-token');
    assert.deepEqual(JSON.parse(put.body), request);
    assert.equal(requests.filter(x => x.method === 'PUT').length, 1);
    assert.doesNotMatch(put.body, /password|credential|providerUrl|https:\/\//);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});

test('W66 subscription client rejects unbounded IDs without issuing a network request', async () => {
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  let called = false;
  try {
    globalThis.window = { location: { hash: '', pathname: '/', search: '' } };
    globalThis.fetch = async () => { called = true; throw Error('network should not be used'); };
    assert.throws(() => kafdeckApi.upsertNotificationSubscription('../admin', {
      destinationId: 'ops', state: 'active', eventClasses: ['operational'],
      eventTypes: [], expectedRevision: null,
    }), error => error instanceof ApiProblem && error.status === 400);
    assert.equal(called, false);
  } finally {
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
});

test('W66 NotificationManage alone permits the create-only surface without NotificationRead', () => {
  assert.deepEqual(notificationManagementSurfaces(false, false),
    { navigation: false, insideRead: false, standalone: false });
  assert.deepEqual(notificationManagementSurfaces(true, false),
    { navigation: true, insideRead: false, standalone: false });
  assert.deepEqual(notificationManagementSurfaces(false, true),
    { navigation: true, insideRead: false, standalone: true });
  assert.deepEqual(notificationManagementSurfaces(true, true),
    { navigation: true, insideRead: true, standalone: false });
  const ui = renderToStaticMarkup(React.createElement(
    NotificationSubscriptionManagementPanel, {
      referenceSubscription: null, onUpdated: () => {},
    },
  ));
  assert.match(ui, /Create-only/);
  assert.doesNotMatch(ui, /Delivery evidence lookup|Subscription details/);
});

test('W66 destination JSON identity admits dot-only server-valid values without path privilege', () => {
  assert.equal(isValidNotificationDestinationId('.'), true);
  assert.equal(isValidNotificationDestinationId('..'), true);
  assert.equal(isValidNotificationDestinationId('ops-destination'), true);
  assert.equal(isValidNotificationDestinationId('https://unapproved.example'), false);
  assert.equal(isValidNotificationDestinationId('../admin'), false);
  assert.equal(isValidNotificationDestinationId(''), false);
});

test('W66 dot-only destination identities survive history and evidence GET URL parsing', async () => {
  const oldWindow = globalThis.window;
  const oldFetch = globalThis.fetch;
  const requests = [];
  const notificationId = '00000000-0000-4000-8000-000000000001';
  try {
    globalThis.window = { location: { hash: '', pathname: '/', search: '' } };
    globalThis.fetch = async (url, init) => {
      requests.push({ url: String(url), method: init.method });
      return new Response(JSON.stringify({
        items: [], truncated: false,
        nextCreatedAtUtc: null, nextNotificationId: null,
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    };

    for (const destinationId of ['.', '..', 'ops-dest']) {
      await kafdeckApi.listNotificationDeliveryHistory(destinationId);
      await kafdeckApi.getNotificationDelivery(notificationId, destinationId);
    }

    assert.equal(requests.length, 6);
    for (let i = 0; i < requests.length; i += 2) {
      const destinationId = ['.', '..', 'ops-dest'][i / 2];
      const history = new URL(requests[i].url, 'https://kafdeck.example');
      const evidence = new URL(requests[i + 1].url, 'https://kafdeck.example');
      assert.equal(requests[i].method, 'GET');
      assert.equal(requests[i + 1].method, 'GET');
      assert.equal(history.pathname, '/api/v1/notifications/deliveries/history');
      assert.equal(evidence.pathname,
        '/api/v1/notifications/deliveries/evidence/' + notificationId);
      assert.equal(history.searchParams.get('destinationId'), destinationId);
      assert.equal(evidence.searchParams.get('destinationId'), destinationId);
      assert.equal(history.searchParams.get('maxResults'), '50');
    }
  } finally {
    globalThis.window = oldWindow;
    globalThis.fetch = oldFetch;
  }
});
