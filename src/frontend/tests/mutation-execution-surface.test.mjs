import assert from 'node:assert/strict';
import test from 'node:test';
import { mutationExecutionSurface } from '../dist/test-source/features/mutations/mutationExecutionSurface.js';

function operation(operationKind, requiresExecutionMaterial, state = 'ready') {
  return { operationKind, requiresExecutionMaterial, state };
}

test('ConnectAlter renders exactly one admitted execution surface', () => {
  assert.equal(
    mutationExecutionSurface(operation('connectAlter', true)),
    'connectMaterial',
  );
  assert.equal(
    mutationExecutionSurface(operation('connectAlter', false)),
    'connectNoMaterial',
  );
});

test('Connect create/delete cannot cross material and no-material execution paths', () => {
  assert.equal(
    mutationExecutionSurface(operation('connectCreate', true)),
    'connectMaterial',
  );
  assert.equal(
    mutationExecutionSurface(operation('connectCreate', false)),
    'none',
  );
  assert.equal(
    mutationExecutionSurface(operation('connectDelete', false)),
    'connectNoMaterial',
  );
  assert.equal(
    mutationExecutionSurface(operation('connectDelete', true)),
    'none',
  );
});

test('material execution controls fail closed when status does not require material', () => {
  assert.equal(
    mutationExecutionSurface(operation('recordProduce', false)),
    'none',
  );
  assert.equal(
    mutationExecutionSurface(operation('schemaCreate', false)),
    'none',
  );
  assert.equal(
    mutationExecutionSurface(operation('recordProduce', true)),
    'recordMaterial',
  );
  assert.equal(
    mutationExecutionSurface(operation('schemaCreate', true)),
    'schemaMaterial',
  );
});

test('execution controls remain hidden until the operation is ready', () => {
  assert.equal(
    mutationExecutionSurface(operation('connectAlter', false, 'awaitingApproval')),
    'none',
  );
  assert.equal(
    mutationExecutionSurface(operation('topicDelete', false, 'executing')),
    'none',
  );
});
