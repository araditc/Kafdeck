import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { AppShell, authoritativeSessionScope, canPreserveOperatorSession, verificationFocusDecision } from '../dist/test-source/app/AppShell.js';
import { describeObservation, selectCluster, shouldAutoRefresh, visibleRefreshIntervalMs } from '../dist/test-source/app/operatorState.js';

test('AppShell exposes the public sign-in landmark and withholds operator controls before verification', () => {
  const markup = renderToStaticMarkup(React.createElement(AppShell));

  assert.match(markup, /<main[^>]+aria-labelledby="kafdeck-sign-in-title"/);
  assert.match(markup, /class="kafdeck-brand-logo" src="\/kafdeck-logo\.webp" alt="" width="42" height="42"/);
  assert.match(markup, /Open Source Kafka Control Plane/);
  assert.match(markup, /Sign in to Kafdeck/);
  assert.match(markup, /Recent sign-ins/);
  assert.match(markup, /role="status"[^>]*>Checking operator session/);
  assert.doesNotMatch(markup, /kafdeck-shell|cluster-selector|Kafdeck sections|Fleet Operations|Commands|Signed in as/);
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


test('only a valid opaque server session scope preserves mounted operator state', () => {
  const scope='a'.repeat(64), other='b'.repeat(64);
  const identity={authenticated:true,authenticationMode:'oidc',email:'same@example.test',authenticatedAt:'2026-11-06T00:00:00Z'};
  assert.equal(authoritativeSessionScope({...identity,sessionScope:scope}),scope);
  assert.equal(canPreserveOperatorSession(scope,{...identity,sessionScope:scope,displayName:'Cosmetic change'}),true);
  assert.equal(canPreserveOperatorSession(scope,{...identity,sessionScope:other}),false);
  for(const value of [undefined,null,'',scope+'x','x'.repeat(64),12,{scope},'../'+scope]) {
    assert.equal(authoritativeSessionScope({...identity,sessionScope:value}),null);
    assert.equal(canPreserveOperatorSession(scope,{...identity,sessionScope:value}),false);
  }
  // Equal email/authentication time is never an authority discriminator for a
  // legacy host or a server that cannot supply its current policy binding.
  assert.equal(canPreserveOperatorSession(scope,identity),false);
  assert.equal(canPreserveOperatorSession(null,identity),false);
  assert.equal(canPreserveOperatorSession(null,{...identity,sessionScope:scope}),false);
  assert.equal(authoritativeSessionScope(Object.create({sessionScope:scope})),null);
});

test('visibility verification defers original focus until actual window return without stealing user focus', () => {
  const candidate={candidateEpoch:4,currentEpoch:4,candidateScope:'server:'+'a'.repeat(64),currentScope:'server:'+'a'.repeat(64),connected:true,documentFocused:false,bodyActive:true};
  // The first 200 response can arrive while visible but still unfocused.
  assert.equal(verificationFocusDecision(candidate),'defer');
  // The next guarded same-scope verification after actual focus can recover
  // that original connected node rather than restarting the tab sequence.
  assert.equal(verificationFocusDecision({...candidate,documentFocused:true}),'restore');
  assert.equal(verificationFocusDecision({...candidate,documentFocused:true,bodyActive:false}),'discard');
  for(const change of [{currentEpoch:5},{currentScope:null},{currentScope:'server:'+'b'.repeat(64)},{connected:false}])assert.equal(verificationFocusDecision({...candidate,...change}),'discard');
});
