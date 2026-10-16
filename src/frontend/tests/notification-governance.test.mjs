import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { NotificationDestinationManagementPanel } from '../dist/test-source/features/notifications/NotificationDestinationManagementPanel.js';
import { kafdeckApi } from '../dist/test-source/shared/api.js';

test('destination governance UI exposes approved choices and review, with no raw secret or endpoint inputs', () => {
  const markup = renderToStaticMarkup(React.createElement(NotificationDestinationManagementPanel));
  assert.match(markup, /Notification destination and credential management/);
  assert.match(markup, /Reload authorized destinations/);
  assert.match(markup, /already sent requires reconciliation/);
  assert.doesNotMatch(markup, /type="password"|name="secret"|name="endpoint"|name="url"|textarea/);
});
test('typed destination control rejects path aliases before any network request', () => {
  for (const value of ['..', '.', 'https://bad.example', 'ops/other']) {
    assert.throws(() => kafdeckApi.changeNotificationManagedDestination(value,
      { candidateId: 'v2', expectedRevision: 1, profileEnabled: true, credentialEnabled: false }));
  }
});
test('audit cursor rejects nonfinite or unsafe input before authority acquisition', () => {
  for (const cursor of [-1, NaN, Infinity, 9007199254740992])
    assert.throws(() => kafdeckApi.readNotificationManagedAudit('ops', cursor));
});
