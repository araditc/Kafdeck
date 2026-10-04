import assert from 'node:assert/strict';
import test from 'node:test';
import { uiStatusPresentation } from '../dist/test-source/app/statusPresentation.js';

test('unified W60 status vocabulary keeps critical states explicit in text', () => {
  assert.equal(uiStatusPresentation('denied').label, 'Denied');
  assert.equal(uiStatusPresentation('unsupported').label, 'Unsupported');
  assert.equal(uiStatusPresentation('blocked').label, 'Blocked');
  assert.equal(uiStatusPresentation('partial').label, 'Partial');
  assert.equal(uiStatusPresentation('stale').label, 'Stale');
  assert.equal(uiStatusPresentation('unavailable').label, 'Unavailable');
  assert.equal(uiStatusPresentation('unknown').label, 'Unknown');
  assert.match(uiStatusPresentation('externalAction').description, /must not be blindly retried/i);
});

test('all unified statuses provide non-color descriptive semantics', () => {
  for (const kind of [
    'supported',
    'current',
    'denied',
    'unsupported',
    'blocked',
    'unconfigured',
    'partial',
    'stale',
    'unavailable',
    'unknown',
    'awaitingConfirmation',
    'awaitingApproval',
    'externalAction',
  ]) {
    const item = uiStatusPresentation(kind);
    assert.ok(item.label.length > 0);
    assert.ok(item.description.length > 12);
    assert.match(item.badgeClass, /^bg-/);
  }
});


test('v0.8 operational evidence states preserve truth in status presentation', async () => {
  const module = await import('../dist/test-source/app/statusPresentation.js');

  assert.equal(module.operationalEvidenceStatusKind('available'), 'current');
  assert.equal(module.operationalEvidenceStatusKind('partial'), 'partial');
  assert.equal(module.operationalEvidenceStatusKind('stale'), 'stale');
  assert.equal(module.operationalEvidenceStatusKind('unavailable'), 'unavailable');
  assert.equal(module.operationalEvidenceStatusKind('unknown'), 'unknown');

  assert.equal(module.operationalTrendStatusKind('available'), 'current');
  assert.equal(module.operationalTrendStatusKind('partial'), 'partial');
  assert.equal(module.operationalTrendStatusKind('unavailable'), 'unavailable');
  assert.equal(module.operationalTrendStatusKind('unknown'), 'unknown');
});
