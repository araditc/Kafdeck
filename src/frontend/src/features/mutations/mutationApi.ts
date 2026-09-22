export type MutationRiskClass = 'low' | 'moderate' | 'high' | 'critical';
export type MutationConfirmationMode = 'explicit' | 'typedTarget';
export type MutationOperationState =
  | 'previewed'
  | 'awaitingConfirmation'
  | 'awaitingApproval'
  | 'ready'
  | 'executing'
  | 'appliedVerified'
  | 'appliedUnverified'
  | 'partiallyApplied'
  | 'executionUnknown'
  | 'rejected'
  | 'expired'
  | 'cancelled'
  | 'stalePreview'
  | 'failedBeforeDispatch'
  | 'failedDefinitive';

export interface MutationStatus {
  operationId: string;
  clusterId: string;
  operationKind: string;
  riskClass: MutationRiskClass;
  confirmationMode: MutationConfirmationMode;
  requiresIndependentApproval: boolean;
  requiresExecutionMaterial: boolean;
  state: MutationOperationState;
  previewHash: string;
  previewExpiresAtUtc: string;
  confirmationChallenge: string | null;
  resultCode: string | null;
  safeProviderEvidence: Record<string, string>;
  version: number;
  createdAtUtc: string;
  updatedAtUtc: string;
}

export interface MutationApprovalInbox {
  items: MutationStatus[];
}

export interface TopicCreatePreviewInput {
  partitionCount: number;
  replicationFactor: number;
  configurations?: Record<string, string>;
}

export interface TopicAlterPreviewInput {
  changes: Record<string, string | null>;
}

export interface TopicIncreasePartitionsPreviewInput {
  newPartitionCount: number;
}

export interface RecordProductionHeaderInput {
  name: string;
  value: string;
}

export interface RecordProductionRecordInput {
  key?: string | null;
  value: string;
  headers?: RecordProductionHeaderInput[];
}

export interface RecordProductionPreviewInput {
  records: RecordProductionRecordInput[];
}

export type ConsumerOffsetSelectorKind =
  | 'absolute'
  | 'earliest'
  | 'latest'
  | 'timestamp'
  | 'relativeShift';

export interface ConsumerOffsetSelectorInput {
  kind: ConsumerOffsetSelectorKind;
  value?: number | null;
  timestampUtc?: string | null;
}

export interface ConsumerOffsetTargetInput {
  topicName: string;
  partition: number;
  selector: ConsumerOffsetSelectorInput;
}

export interface ConsumerOffsetDeleteTargetInput {
  topicName: string;
  partition: number;
}

export interface SchemaReferenceInput {
  name: string;
  subject: string;
  version: number;
}

export type SchemaFormatInput = 'avro' | 'protobuf' | 'jsonSchema';

export interface SchemaRegistrationPreviewInput {
  format: SchemaFormatInput;
  schema: string;
  references?: SchemaReferenceInput[];
}

export interface ConnectConfigurationInput {
  configuration: Record<string, string>;
}

export type ConnectControlActionInput = 'pause' | 'resume' | 'restart';

export interface ConnectControlPreviewInput {
  action: ConnectControlActionInput;
  taskId?: number | null;
}

export type RecordsPurgeSelectorKind = 'absolute' | 'timestamp';

export interface RecordsPurgeTargetInput {
  topicName: string;
  partition: number;
  selector: {
    kind: RecordsPurgeSelectorKind;
    beforeOffset?: number | null;
    timestampUtc?: string | null;
  };
}

export class MutationApiProblem extends Error {
  readonly status: number;
  readonly code: string | null;

  constructor(status: number, message: string, code: string | null) {
    super(message);
    this.name = 'MutationApiProblem';
    this.status = status;
    this.code = code;
  }
}

interface CsrfToken {
  requestToken: string;
  headerName: string;
}

let csrfToken: CsrfToken | null = null;

async function parseProblem(response: Response): Promise<MutationApiProblem> {
  let problem: { detail?: string; title?: string; code?: string; type?: string } = {};
  try {
    problem = (await response.json()) as typeof problem;
  } catch {
    // Never expose raw provider/server response bodies to the operator UI.
  }

  return new MutationApiProblem(
    response.status,
    problem.detail ?? problem.title ?? 'The governed mutation request failed.',
    problem.code ?? problem.type ?? null,
  );
}

async function readJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const init: RequestInit = {
    method: 'GET',
    headers: { Accept: 'application/json' },
    credentials: 'same-origin',
  };
  if (signal !== undefined) init.signal = signal;

  const response = await fetch(path, init);
  if (!response.ok) throw await parseProblem(response);
  return (await response.json()) as T;
}

async function csrf(signal?: AbortSignal): Promise<CsrfToken> {
  if (csrfToken !== null) return csrfToken;

  const token = await readJson<CsrfToken>('/api/v1/auth/csrf', signal);
  if (!token.requestToken || !token.headerName) {
    throw new MutationApiProblem(
      500,
      'The antiforgery token response was incomplete.',
      'urn:kafdeck:problem:antiforgery-token-invalid',
    );
  }

  csrfToken = token;
  return token;
}

async function postJson<T>(
  path: string,
  body: unknown,
  options?: { idempotencyKey?: string | undefined; signal?: AbortSignal | undefined },
): Promise<T> {
  const token = await csrf(options?.signal);
  const headers: Record<string, string> = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    [token.headerName]: token.requestToken,
  };
  if (options?.idempotencyKey) {
    headers['Idempotency-Key'] = options.idempotencyKey;
  }

  const init: RequestInit = {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    credentials: 'same-origin',
  };
  if (options?.signal !== undefined) init.signal = options.signal;

  const response = await fetch(path, init);
  if (!response.ok) {
    const problem = await parseProblem(response);
    if (problem.code === 'urn:kafdeck:problem:antiforgery-validation-failed') {
      csrfToken = null;
    }
    throw problem;
  }

  return (await response.json()) as T;
}

function mutationPath(operationId: string): string {
  return `/api/v1/mutations/${encodeURIComponent(operationId)}`;
}

function clusterPath(clusterId: string): string {
  return `/api/v1/clusters/${encodeURIComponent(clusterId)}`;
}

function topicMutationPath(clusterId: string, topicName: string, action: string): string {
  return `${clusterPath(clusterId)}/topics/${encodeURIComponent(topicName)}/mutations/${action}/preview`;
}

function connectorMutationPath(clusterId: string, connectorName: string, action: string): string {
  return `${clusterPath(clusterId)}/connect/connectors/${encodeURIComponent(connectorName)}/mutations/${action}/preview`;
}

export const mutationApi = {
  get(operationId: string, signal?: AbortSignal) {
    return readJson<MutationStatus>(mutationPath(operationId), signal);
  },

  listApprovals(limit = 50, signal?: AbortSignal) {
    const bounded = Math.max(1, Math.min(100, Math.trunc(limit)));
    return readJson<MutationApprovalInbox>(
      `/api/v1/mutations/approvals?limit=${bounded}`,
      signal,
    );
  },

  confirm(
    operation: MutationStatus,
    typedTargetChallenge: string | null,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/confirm`,
      {
        previewHash: operation.previewHash,
        typedTargetChallenge,
      },
      { signal },
    );
  },

  approve(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/approve`,
      { previewHash: operation.previewHash },
      { signal },
    );
  },

  reject(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/reject`,
      { previewHash: operation.previewHash },
      { signal },
    );
  },

  cancel(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/cancel`,
      { previewHash: operation.previewHash },
      { signal },
    );
  },

  executeWithoutMaterial(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/execute`,
      {},
      { signal },
    );
  },

  previewTopicCreate(
    clusterId: string,
    topicName: string,
    request: TopicCreatePreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      topicMutationPath(clusterId, topicName, 'create'),
      request,
      { idempotencyKey, signal },
    );
  },

  previewTopicAlter(
    clusterId: string,
    topicName: string,
    request: TopicAlterPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      topicMutationPath(clusterId, topicName, 'alter'),
      request,
      { idempotencyKey, signal },
    );
  },

  previewTopicIncreasePartitions(
    clusterId: string,
    topicName: string,
    request: TopicIncreasePartitionsPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      topicMutationPath(clusterId, topicName, 'increase-partitions'),
      request,
      { idempotencyKey, signal },
    );
  },

  previewTopicDelete(
    clusterId: string,
    topicName: string,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      topicMutationPath(clusterId, topicName, 'delete'),
      {},
      { idempotencyKey, signal },
    );
  },

  previewRecordProduction(
    clusterId: string,
    topicName: string,
    request: RecordProductionPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      topicMutationPath(clusterId, topicName, 'produce'),
      request,
      { idempotencyKey, signal },
    );
  },

  executeRecordProduction(
    operation: MutationStatus,
    records: RecordProductionRecordInput[],
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/record-production/execute`,
      { records },
      { signal },
    );
  },

  previewConsumerOffsets(
    clusterId: string,
    groupId: string,
    targets: ConsumerOffsetTargetInput[],
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${clusterPath(clusterId)}/consumer-groups/${encodeURIComponent(groupId)}/mutations/offsets/preview`,
      { targets },
      { idempotencyKey, signal },
    );
  },

  previewConsumerDelete(
    clusterId: string,
    groupId: string,
    mode: 'group' | 'offsets',
    targets: ConsumerOffsetDeleteTargetInput[] | null,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${clusterPath(clusterId)}/consumer-groups/${encodeURIComponent(groupId)}/mutations/delete/preview`,
      { mode, targets },
      { idempotencyKey, signal },
    );
  },

  previewSchemaRegister(
    clusterId: string,
    subject: string,
    request: SchemaRegistrationPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${clusterPath(clusterId)}/schemas/subjects/${encodeURIComponent(subject)}/mutations/register/preview`,
      request,
      { idempotencyKey, signal },
    );
  },

  previewSchemaCompatibility(
    clusterId: string,
    subject: string | null,
    requestedMode: string,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    const path = subject === null
      ? `${clusterPath(clusterId)}/schemas/mutations/compatibility/preview`
      : `${clusterPath(clusterId)}/schemas/subjects/${encodeURIComponent(subject)}/mutations/compatibility/preview`;
    return postJson<MutationStatus>(
      path,
      { requestedMode },
      { idempotencyKey, signal },
    );
  },

  previewSchemaDelete(
    clusterId: string,
    subject: string,
    version: number | null,
    permanent: boolean,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${clusterPath(clusterId)}/schemas/subjects/${encodeURIComponent(subject)}/mutations/delete/preview`,
      { version, permanent },
      { idempotencyKey, signal },
    );
  },

  executeSchemaCreate(
    operation: MutationStatus,
    schema: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/schema-create/execute`,
      { schema },
      { signal },
    );
  },

  previewConnectConfiguration(
    clusterId: string,
    connectorName: string,
    action: 'create' | 'update',
    configuration: Record<string, string>,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      connectorMutationPath(clusterId, connectorName, action),
      { configuration },
      { idempotencyKey, signal },
    );
  },

  previewConnectControl(
    clusterId: string,
    connectorName: string,
    request: ConnectControlPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      connectorMutationPath(clusterId, connectorName, 'control'),
      request,
      { idempotencyKey, signal },
    );
  },

  previewConnectDelete(
    clusterId: string,
    connectorName: string,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      connectorMutationPath(clusterId, connectorName, 'delete'),
      {},
      { idempotencyKey, signal },
    );
  },

  executeConnectConfiguration(
    operation: MutationStatus,
    configuration: Record<string, string>,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/connect-configuration/execute`,
      { configuration },
      { signal },
    );
  },

  executeConnectWithoutMaterial(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/connect/execute`,
      {},
      { signal },
    );
  },

  previewRecordsPurge(
    clusterId: string,
    targets: RecordsPurgeTargetInput[],
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${clusterPath(clusterId)}/records/purge/preview`,
      { targets },
      { idempotencyKey, signal },
    );
  },
};