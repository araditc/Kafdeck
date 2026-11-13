import assert from 'node:assert/strict';
import test from 'node:test';
import { governancePageAvailable, governancePageKeys } from '../dist/test-source/app/governanceNavigation.js';
import { filterProductCommands } from '../dist/test-source/app/commandPaletteModel.js';
import { kafdeckApi } from '../dist/test-source/shared/api.js';

const snapshot = { sessionKey: 'session-a', value: { clusterId: 'a', ownershipCatalog: true, governancePolicy: false, serviceRequests: true, governanceAttribution: false } };
test('G05 each governance page follows only its exact current server hint', () => {
  for (const [page, key] of Object.entries(governancePageKeys))
    assert.equal(governancePageAvailable(page, 'a', 'session-a', snapshot), snapshot.value[key]);
  assert.equal(governancePageAvailable('topics', 'a', null, null), true);
});
test('G05 absent, stale cluster/session and malformed hints fail closed', () => {
  for (const [cluster, session, value] of [['a', 'session-a', null], ['b', 'session-a', snapshot], ['a', 'session-b', snapshot], ['', 'session-a', snapshot], ['a', null, snapshot]])
    assert.equal(governancePageAvailable('ownership-catalog', cluster, session, value), false);
  for (const hint of [undefined, null, false, 'true', 1])
    assert.equal(governancePageAvailable('ownership-catalog', 'a', 'session-a', { ...snapshot, value: { ...snapshot.value, ownershipCatalog: hint } }), false);
});
test('G05 palette keeps denied pages visible but unavailable and preserves allowed routes', () => {
  const commands = filterProductCommands('', ['overview', 'topics', 'ownership-catalog']);
  for (const id of ['policy', 'requests', 'attribution', 'automation', 'data-jobs'])
    assert.equal(commands.find(command => command.id === id).availability, 'unavailable');
  for (const id of ['catalog', 'topics', 'overview', 'cluster'])
    assert.equal(commands.find(command => command.id === id).availability, 'available');
  assert.equal(filterProductCommands('policy', []).find(command => command.id === 'policy').availability, 'unavailable');
});
test('G05 browser preflight is one encoded GET and a denied response does not imply a new grant', async () => {
  const previous = globalThis.fetch;
  const previousWindow = globalThis.window;
  const calls = [];
  const events = [];
  globalThis.window = { location: { hash: '' }, dispatchEvent: event => { events.push(event.type); return true; } };
  globalThis.fetch = async (url, init) => {
    calls.push({ url, method: init?.method ?? 'GET' });
    return new Response(JSON.stringify(snapshot.value), { status: 200, headers: { 'Content-Type': 'application/json' } });
  };
  try {
    assert.deepEqual(await kafdeckApi.getGovernanceNavigation('a & b'), snapshot.value);
    assert.deepEqual(calls, [{ url: '/api/v1/auth/navigation?clusterId=a+%26+b', method: 'GET' }]);
    globalThis.fetch = async (_url, init) => {
      assert.equal(init.cache, 'no-store');
      return new Response('{}', { status: 403, headers: { 'Content-Type': 'application/problem+json' } });
    };
    await assert.rejects(kafdeckApi.getGovernanceNavigation('a'), error => error.status === 403);
    assert.deepEqual(events, []);
  } finally { globalThis.fetch = previous; globalThis.window = previousWindow; }
});
