import { withDeploymentAccessToken } from '../../shared/deploymentAccess.js';
import { notifySessionLossIfUnauthorized } from '../../shared/operatorSessionSecurity.js';
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

export interface ConnectAutoRestartPolicyStatus {
  clusterId: string;
  connectProfileId: string;
  connectorName: string;
  taskId: number | null;
  deploymentEnabled: boolean;
  active: boolean;
  activationId: string | null;
  circuitState: string | null;
  attemptsUsed: number;
  maxAttempts: number;
  activatedAtUtc: string | null;
  deadlineUtc: string | null;
  nextAttemptUtc: string | null;
  terminalReason: string | null;
  hasUnresolvedDispatch: boolean;
  version: number | null;
}

export interface ConnectAutoRestartPolicyPreviewInput {
  enabled: boolean;
  taskId?: number | null;
  maxAttempts?: number | null;
  initialBackoffSeconds?: number | null;
  maxBackoffSeconds?: number | null;
  activationLifetimeSeconds?: number | null;
  maxActivePoliciesPerProfile?: number | null;
  jitterBasisPoints?: number | null;
}

export type DataJobKindInput = 'replay' | 'forward' | 'reprocess' | 'dlq-forward';

export interface DataJobRangeInput {
  sourceTopic: string;
  sourcePartition: number;
  destinationTopic: string;
  destinationPartition: number;
  startInclusive: number;
  endExclusive: number;
}

export interface DataJobBudgetInput {
  maxBatchRecords?: number | null;
  maxBatchBytes?: number | null;
  maxTotalRecords?: number | null;
  maxTotalBytes?: number | null;
  maxDurationSeconds?: number | null;
  maxRecordsPerSecond?: number | null;
  maxBytesPerSecond?: number | null;
}

export interface DataJobTransformInput {
  kind: 'BytePreserving' | 'MaskedStructuredProjection';
  serdeFormat?: 'json' | 'cbor' | 'xml' | 'messagepack' | null;
  projectedFields?: string[] | null;
}

export interface DataJobPreviewInput {
  sourceClusterId: string;
  sourceProfileVersion: string;
  destinationClusterId: string;
  destinationProfileVersion: string;
  ranges: DataJobRangeInput[];
  budget?: DataJobBudgetInput | null;
  transform?: DataJobTransformInput | null;
}

export interface DataJobRangeStatus {
  rangeIndex: number;
  sourceTopic: string;
  sourcePartition: number;
  destinationTopic: string;
  destinationPartition: number;
  startInclusive: number;
  endExclusive: number;
  nextSourceOffset: number | null;
}

export interface DataJobPendingBatchStatus {
  batchId: string;
  rangeIndex: number;
  sourceOffset: number;
  destinationPartition: number;
  rawBytes: number;
  state: string;
  reservedAtUtc: string;
  dispatchStartedAtUtc: string | null;
}

export interface DataJobStatus {
  operationId: string;
  kind: string;
  mutationState: string;
  resultCode: string | null;
  planFingerprint: string;
  sourceClusterId: string;
  destinationClusterId: string;
  progressPhase: string;
  workerGeneration: number;
  acknowledgedRecords: number;
  acknowledgedBytes: number;
  activeRuntimeMilliseconds: number;
  ranges: DataJobRangeStatus[];
  pendingBatch: DataJobPendingBatchStatus | null;
}

export type DataGeneratorSourceKindInput = 'Schema' | 'BuiltInTemplate';

export interface DataGeneratorBudgetInput {
  maxBatchRecords?: number | null;
  maxBatchBytes?: number | null;
  maxTotalRecords?: number | null;
  maxTotalBytes?: number | null;
  maxDurationSeconds?: number | null;
  maxRecordsPerSecond?: number | null;
  maxBytesPerSecond?: number | null;
}

export interface DataGeneratorSourceInput {
  kind: DataGeneratorSourceKindInput;
  schemaSubject?: string | null;
  schemaVersion?: number | null;
  template?: 'BasicJsonV1' | null;
}

export interface DataGeneratorPreviewInput {
  destinationClusterId: string;
  destinationProfileVersion: string;
  destinationTopic: string;
  destinationPartition: number;
  recordCount: number;
  seed?: number | null;
  source: DataGeneratorSourceInput;
  budget?: DataGeneratorBudgetInput | null;
}

export interface DataGeneratorPendingBatchStatus {
  batchId: string;
  recordIndex: number;
  destinationPartition: number;
  rawBytes: number;
  state: string;
  reservedAtUtc: string;
  dispatchStartedAtUtc: string | null;
}

export interface DataGeneratorStatus {
  operationId: string;
  mutationState: string;
  resultCode: string | null;
  planFingerprint: string;
  destinationClusterId: string;
  destinationTopic: string;
  destinationPartition: number;
  sourceKind: string;
  recordCount: number;
  seed: number;
  progressPhase: string;
  workerGeneration: number;
  nextRecordIndex: number;
  acknowledgedRecords: number;
  acknowledgedBytes: number;
  activeRuntimeMilliseconds: number;
  pendingBatch: DataGeneratorPendingBatchStatus | null;
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
  // Shared auth-loss boundary with read-only APIs; cover mutation GET/PUT/POST and CSRF.
  notifySessionLossIfUnauthorized(response.status);
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
    headers: withDeploymentAccessToken({ Accept: 'application/json' }),
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

async function putJson<T>(
  path: string,
  body: unknown,
  options?: { idempotencyKey?: string | undefined; signal?: AbortSignal | undefined },
): Promise<T> {
  const token = await csrf(options?.signal);
  const headers: Record<string, string> = withDeploymentAccessToken({
    Accept: 'application/json',
    'Content-Type': 'application/json',
    [token.headerName]: token.requestToken,
  });
  if (options?.idempotencyKey) {
    headers['Idempotency-Key'] = options.idempotencyKey;
  }

  const init: RequestInit = {
    method: 'PUT',
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

async function postJson<T>(
  path: string,
  body: unknown,
  options?: { idempotencyKey?: string | undefined; signal?: AbortSignal | undefined },
): Promise<T> {
  const token = await csrf(options?.signal);
  const headers: Record<string, string> = withDeploymentAccessToken({
    Accept: 'application/json',
    'Content-Type': 'application/json',
    [token.headerName]: token.requestToken,
  });
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

function connectorProfileMutationPath(
  clusterId: string,
  connectProfileId: string,
  connectorName: string,
  action: string,
): string {
  return `${clusterPath(clusterId)}/connect/profiles/${encodeURIComponent(connectProfileId)}/connectors/${encodeURIComponent(connectorName)}/mutations/${action}/preview`;
}

function connectAutoRestartPolicyPath(
  clusterId: string,
  connectProfileId: string,
  connectorName: string,
): string {
  return `${clusterPath(clusterId)}/connect/profiles/${encodeURIComponent(connectProfileId)}/connectors/${encodeURIComponent(connectorName)}/restart-policy`;
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

  previewConnectProfileConfiguration(
    clusterId: string,
    connectProfileId: string,
    connectorName: string,
    action: 'create' | 'update',
    configuration: Record<string, string>,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      connectorProfileMutationPath(clusterId, connectProfileId, connectorName, action),
      { configuration },
      { idempotencyKey, signal },
    );
  },

  previewConnectProfileControl(
    clusterId: string,
    connectProfileId: string,
    connectorName: string,
    request: ConnectControlPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      connectorProfileMutationPath(clusterId, connectProfileId, connectorName, 'control'),
      request,
      { idempotencyKey, signal },
    );
  },

  previewConnectProfileDelete(
    clusterId: string,
    connectProfileId: string,
    connectorName: string,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      connectorProfileMutationPath(clusterId, connectProfileId, connectorName, 'delete'),
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

  getConnectAutoRestartPolicy(
    clusterId: string,
    connectProfileId: string,
    connectorName: string,
    taskId?: number | null,
    signal?: AbortSignal,
  ) {
    const params = new URLSearchParams();
    if (taskId !== undefined && taskId !== null) params.set('taskId', String(taskId));
    const suffix = params.size > 0 ? `?${params}` : '';
    return readJson<ConnectAutoRestartPolicyStatus>(
      `${connectAutoRestartPolicyPath(clusterId, connectProfileId, connectorName)}${suffix}`,
      signal,
    );
  },

  previewConnectAutoRestartPolicy(
    clusterId: string,
    connectProfileId: string,
    connectorName: string,
    request: ConnectAutoRestartPolicyPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return putJson<MutationStatus>(
      `${connectAutoRestartPolicyPath(clusterId, connectProfileId, connectorName)}/preview`,
      request,
      { idempotencyKey, signal },
    );
  },

  applyConnectAutoRestartPolicy(
    clusterId: string,
    connectProfileId: string,
    connectorName: string,
    operationId: string,
    taskId?: number | null,
    signal?: AbortSignal,
  ) {
    return putJson<MutationStatus>(
      connectAutoRestartPolicyPath(clusterId, connectProfileId, connectorName),
      { operationId, taskId: taskId ?? null },
      { signal },
    );
  },

  previewDataJob(
    kind: DataJobKindInput,
    request: DataJobPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `/api/v1/data-jobs/${encodeURIComponent(kind)}/preview`,
      request,
      { idempotencyKey, signal },
    );
  },

  startDataJob(operationId: string, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `/api/v1/data-jobs/${encodeURIComponent(operationId)}/start`,
      {},
      { signal },
    );
  },

  getDataJobStatus(operationId: string, signal?: AbortSignal) {
    return readJson<DataJobStatus>(
      `/api/v1/data-jobs/${encodeURIComponent(operationId)}`,
      signal,
    );
  },

  cancelDataJob(operationId: string, signal?: AbortSignal) {
    return postJson<DataJobStatus>(
      `/api/v1/data-jobs/${encodeURIComponent(operationId)}/cancel`,
      {},
      { signal },
    );
  },

  reconcileDataJob(
    operationId: string,
    batchId: string,
    signal?: AbortSignal,
  ) {
    return postJson<DataJobStatus>(
      `/api/v1/data-jobs/${encodeURIComponent(operationId)}/reconcile`,
      { batchId, disposition: 'provenNonApplication' },
      { signal },
    );
  },

  previewDataGenerator(
    request: DataGeneratorPreviewInput,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      '/api/v1/data-jobs/generator/preview',
      request,
      { idempotencyKey, signal },
    );
  },

  startDataGenerator(operationId: string, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `/api/v1/data-jobs/generator/${encodeURIComponent(operationId)}/start`,
      {},
      { signal },
    );
  },

  getDataGeneratorStatus(operationId: string, signal?: AbortSignal) {
    return readJson<DataGeneratorStatus>(
      `/api/v1/data-jobs/generator/${encodeURIComponent(operationId)}`,
      signal,
    );
  },

  cancelDataGenerator(operationId: string, signal?: AbortSignal) {
    return postJson<DataGeneratorStatus>(
      `/api/v1/data-jobs/generator/${encodeURIComponent(operationId)}/cancel`,
      {},
      { signal },
    );
  },

  reconcileDataGenerator(
    operationId: string,
    batchId: string,
    signal?: AbortSignal,
  ) {
    return postJson<DataGeneratorStatus>(
      `/api/v1/data-jobs/generator/${encodeURIComponent(operationId)}/reconcile`,
      { batchId, disposition: 'provenNonApplication' },
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