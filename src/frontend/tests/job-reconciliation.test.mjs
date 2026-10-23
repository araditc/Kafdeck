import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { mutationApi, validateDataReconciliationPreview } from '../dist/test-source/features/mutations/mutationApi.js';
import { mutationExecutionSurface } from '../dist/test-source/features/mutations/mutationExecutionSurface.js';

const id = '11111111-1111-1111-1111-111111111111';
const request = { batchId: '22222222-2222-2222-2222-222222222222', expectedProgressVersion: 2,
  proofReferenceSha256: 'a'.repeat(64), fencedWorkerEvidenceSha256: 'b'.repeat(64) };

test('reconciliation rejects missing proof, stale-shaped progress, raw authority and path input before network', () => {
  let calls = 0; const previous = globalThis.fetch;
  globalThis.fetch = () => { calls++; throw new Error('Network must not be used'); };
  try {
    for (const invalid of [{ ...request, expectedProgressVersion: -1 }, { ...request, expectedProgressVersion: NaN },
      { ...request, expectedProgressVersion: 9007199254740992 }, { ...request, proofReferenceSha256: '' },
      { ...request, fencedWorkerEvidenceSha256: 'file:unreviewed-proof' }, { ...request, disposition: 'provenNonApplication' },
      { ...request, batchId: '' }])
      assert.throws(() => mutationApi.previewDataJobReconciliation(id, invalid, 'stable-key'));
    assert.throws(() => mutationApi.previewDataGeneratorReconciliation('../another', request, 'stable-key'));
    assert.throws(() => validateDataReconciliationPreview(id, request, ''));
    assert.equal(calls, 0);
  } finally { globalThis.fetch = previous; }
});

test('job and generator create only typed CSRF-protected idempotent previews and return a new mutation', async () => {
  const previous = globalThis.fetch; const previousWindow = globalThis.window; const calls = [];
  globalThis.window = { location: { hash: '', search: '' } };
  globalThis.fetch = async (url, init) => {
    if (url === '/api/v1/auth/csrf') return Response.json({ headerName: 'X-Test-CSRF', requestToken: 'test-csrf' });
    calls.push({ url, init });
    return Response.json({ operationId: '33333333-3333-3333-3333-333333333333', operationKind: 'fleetUncertaintyDisposition',
      riskClass: 'critical', requiresIndependentApproval: true, state: 'awaitingConfirmation' });
  };
  try {
    const first = await mutationApi.previewDataJobReconciliation(id, request, 'same-review-key');
    const second = await mutationApi.previewDataGeneratorReconciliation(id, request, 'same-review-key');
    assert.equal(first.operationKind, 'fleetUncertaintyDisposition'); assert.equal(second.requiresIndependentApproval, true);
    assert.equal(calls.length, 2);
    assert.equal(calls[0].url, `/api/v1/data-jobs/${id}/reconcile/preview`);
    assert.equal(calls[1].url, `/api/v1/data-jobs/generator/${id}/reconcile/preview`);
    for (const { init } of calls) {
      assert.equal(init.method, 'POST'); assert.equal(init.credentials, 'same-origin');
      assert.equal(init.headers['Idempotency-Key'], 'same-review-key'); assert.equal(init.headers['X-Test-CSRF'], 'test-csrf');
      assert.deepEqual(JSON.parse(init.body), request); assert.doesNotMatch(init.body, /provenNonApplication|requester|observedAt|watermark/i);
    }
  } finally { globalThis.fetch = previous; globalThis.window = previousWindow; }
});

test('CRITICAL reconciliation execution requires ready no-material status and its server review', () => {
  const operation = { operationKind: 'fleetUncertaintyDisposition', state: 'ready', requiresExecutionMaterial: false };
  assert.equal(mutationExecutionSurface(operation), 'none');
  assert.equal(mutationExecutionSurface({ ...operation, reconciliationReview: {} }), 'genericNoMaterial');
  assert.equal(mutationExecutionSurface({ ...operation, reconciliationReview: {}, state: 'awaitingApproval' }), 'none');
  assert.equal(mutationExecutionSurface({ ...operation, reconciliationReview: {}, requiresExecutionMaterial: true }), 'none');
});

test('browser reconciliation reviews proof references and immutable server observations without direct batch clearing', () => {
  const panel = readFileSync(new URL('../src/features/mutations/MutationOperationsPanel.tsx', import.meta.url), 'utf8');
  const api = readFileSync(new URL('../src/features/mutations/mutationApi.ts', import.meta.url), 'utf8');
  assert.match(panel, /Reviewed reconciliation request/); assert.match(panel, /Server-bound reconciliation review/);
  assert.match(panel, /both watermarks/); assert.match(panel, /distinct authorized human approval/);
  assert.match(panel, /does not clear a batch or resume a worker/);
  assert.match(panel, /Secure randomness is required/); assert.doesNotMatch(api, /disposition: 'provenNonApplication'/);
});
