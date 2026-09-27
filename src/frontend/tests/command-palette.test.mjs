import assert from 'node:assert/strict';
import test from 'node:test';
import { filterProductCommands, productCommands } from '../dist/test-source/app/commandPalette.js';

test('command registry has stable unique product action identifiers', () => {
  const ids = productCommands.map(command => command.id);
  assert.equal(new Set(ids).size, ids.length);
  assert.ok(ids.includes('topics'));
  assert.ok(ids.includes('schemas'));
  assert.ok(ids.includes('connect'));
  assert.ok(ids.includes('data-jobs'));
  assert.ok(ids.includes('generator'));
  assert.ok(ids.includes('ksql'));
  assert.ok(ids.includes('streams'));
  assert.ok(ids.includes('lineage'));
});

test('command filtering matches labels, descriptions and registered keywords', () => {
  assert.deepEqual(filterProductCommands('DLQ').map(command => command.id), ['data-jobs']);
  assert.deepEqual(filterProductCommands('protobuf').map(command => command.id), ['schemas']);
  assert.deepEqual(filterProductCommands('rocksdb').map(command => command.id), ['streams']);
});

test('generator navigation is a registered typed product surface', () => {
  const generator = productCommands.find(command => command.id === 'generator');
  assert.ok(generator);
  assert.equal(generator.availability, 'available');
  assert.equal(generator.targetId, 'mutations');
  assert.equal(generator.focusId, 'mutation-workflow');
});
