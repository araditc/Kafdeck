import type { MutationStatus } from './mutationApi.js';

export type MutationExecutionSurface =
  | 'none'
  | 'genericNoMaterial'
  | 'connectNoMaterial'
  | 'recordMaterial'
  | 'schemaMaterial'
  | 'connectMaterial';

const genericNoMaterialKinds = new Set([
  'topicCreate',
  'topicAlter',
  'topicIncreasePartitions',
  'topicDelete',
  'consumerOffsetAlter',
  'consumerDelete',
  'schemaAlter',
  'schemaDelete',
  'recordsPurge',
]);

export function mutationExecutionSurface(
  operation: Pick<MutationStatus, 'state' | 'operationKind' | 'requiresExecutionMaterial'>,
): MutationExecutionSurface {
  if (operation.state !== 'ready') return 'none';

  if (operation.operationKind === 'recordProduce') {
    return operation.requiresExecutionMaterial ? 'recordMaterial' : 'none';
  }

  if (operation.operationKind === 'schemaCreate') {
    return operation.requiresExecutionMaterial ? 'schemaMaterial' : 'none';
  }

  if (operation.operationKind === 'connectCreate') {
    return operation.requiresExecutionMaterial ? 'connectMaterial' : 'none';
  }

  if (operation.operationKind === 'connectAlter') {
    return operation.requiresExecutionMaterial
      ? 'connectMaterial'
      : 'connectNoMaterial';
  }

  if (operation.operationKind === 'connectDelete') {
    return operation.requiresExecutionMaterial
      ? 'none'
      : 'connectNoMaterial';
  }

  if (!operation.requiresExecutionMaterial &&
      genericNoMaterialKinds.has(operation.operationKind)) {
    return 'genericNoMaterial';
  }

  return 'none';
}
