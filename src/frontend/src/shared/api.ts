export type Freshness = 'fresh' | 'stale';

export interface ApiObservation { observedAt: string; freshness: Freshness; cacheAgeMs: number; partial: boolean; }
export interface ApiLimitation { capability: string | null; state: string; reason: string | null; }
export interface ApiEnvelope<T> { data: T; observation: ApiObservation; limitations: ApiLimitation[]; }
export interface BrokerProjection { brokerId: number; host: string; port: number; rack: string | null; isController: boolean; }
export interface ClusterData { clusterId: string; kafkaClusterId: string | null; controllerBrokerId: number | null; brokers: BrokerProjection[]; health: string | number; healthReasons: Array<{ code: string | number; description: string }>; failureCode: string | null; }
export interface TopicListItem { name: string; partitionCount: number; isInternal: boolean; offlinePartitionCount: number | null; underReplicatedPartitionCount: number | null; anomalyState: string | number; }
export interface TopicPageData { items: TopicListItem[]; nextCursor: string | null; }
export interface PartitionProjection { partitionId: number; leaderBrokerId: number | null; replicaBrokerIds: number[]; inSyncReplicaBrokerIds: number[]; outOfSyncReplicaBrokerIds: number[]; health: string | number; healthReasons: string[]; }
export interface TopicDetailData { name: string; isInternal: boolean; partitions: PartitionProjection[]; offlinePartitionCount: number; underReplicatedPartitionCount: number; anomalyState: string | number; }
export interface ConfigurationEntryData { name: string; value: string | null; isSensitive: boolean; isReadOnly: boolean; source: string | null; }
export interface OperatorSession { authenticated: true; authenticationMode: 'oidc'; displayName: string | null; email: string | null; authenticatedAt: string; }

export interface ReadViewLimitation { code: string; message: string; }
export interface ReadViewEnvelope<T> { data: T; partial: boolean; limitations: ReadViewLimitation[]; }

export interface ConsumerGroupSummary {
  groupId: string;
  state: string;
  memberCount: number | null;
  isSimpleConsumerGroup: boolean;
}
export interface ConsumerAssignment { topic: string; partition: number; }
export interface ConsumerMember {
  memberId: string;
  groupInstanceId: string | null;
  clientId: string | null;
  clientHost: string | null;
  assignments: ConsumerAssignment[];
}
export interface ConsumerGroupDetail {
  groupId: string;
  state: string;
  protocolType: string | null;
  protocol: string | null;
  coordinator: string | null;
  members: ConsumerMember[];
}
export interface ConsumerOffset {
  topic: string;
  partition: number;
  committedOffset: number | null;
  endOffset: number | null;
  lag: number | null;
  state: string;
}
export interface ConsumerLag {
  groupId: string;
  partitions: ConsumerOffset[];
  totalLag: number | null;
  isPartial: boolean;
  limitations: ReadViewLimitation[];
}
export interface ConsumerRateObservation {
  produceRecordsPerSecond: number | null;
  consumeRecordsPerSecond: number | null;
  window: string;
  observedAt: string;
  source: string;
}
export interface ConsumerDiagnosticEvidence { code: string; safeMessage: string; }
export interface ConsumerDiagnostics {
  groupId: string;
  state: string;
  groupState: string;
  totalLag: number | null;
  rates: ConsumerRateObservation | null;
  metricsAvailable: boolean;
  historyAvailable: boolean;
  evidence: ConsumerDiagnosticEvidence[];
  limitations: ReadViewLimitation[];
}

export interface SchemaReference { name: string; subject: string; version: number; }
export interface SchemaSubjectSummary { subject: string; }
export interface SchemaVersionSummary {
  subject: string;
  version: number;
  schemaId: number;
  format: string;
  references: SchemaReference[];
}
export interface SchemaDocument {
  id: number;
  format: string;
  schemaText: string;
  references: SchemaReference[];
}
export interface SchemaVersionDetail { subject: string; version: number; schema: SchemaDocument; }
export interface SchemaCompatibility {
  subject: string;
  mode: string;
  isInherited: boolean;
}
export interface SchemaDiffHunk {
  leftStartLine: number;
  leftLineCount: number;
  rightStartLine: number;
  rightLineCount: number;
  removedLines: string[];
  addedLines: string[];
}
export interface SchemaDiff { isEqual: boolean; hunks: SchemaDiffHunk[]; }

export interface ConnectClusterInfo {
  version: string | null;
  commit: string | null;
  kafkaClusterId: string | null;
}
export interface ConnectConnectorSummary { name: string; }
export interface ConnectTaskStatus {
  id: number;
  state: string;
  workerId: string | null;
  safeTrace: string | null;
}
export interface ConnectConnectorDetail {
  name: string;
  state: string;
  workerId: string | null;
  tasks: ConnectTaskStatus[];
  safeConfiguration: Record<string, string | null>;
}
export interface KsqlServerInfo {
  version: string | null;
  kafkaClusterId: string | null;
  state: string | null;
}
export interface KsqlMetadataItem {
  kind: string;
  name: string;
  kafkaTopic: string | null;
  valueFormat: string | null;
  keyFormat: string | null;
}
export interface TopicCatalogEntry {
  clusterId: string;
  topicName: string;
  description: string | null;
  owner: string | null;
  domain: string | null;
  tags: string[];
  documentationReference: string | null;
  classification: string | null;
}

export interface RecordAnchorProjection { kind: string; offset: number | null; timestampUtc: string | null; }
export interface RecordSafeHeader { name: string; value: string; isRedacted: boolean; }
export interface RecordSafeProjection {
  partition: number;
  offset: number;
  timestampUtc: string | null;
  key: string | null;
  keyRedacted: boolean;
  valueKind: 'raw' | 'structured' | 'fullyRedacted';
  rawValue: string | null;
  structuredValue: unknown | null;
  headers: RecordSafeHeader[];
  policyId: string;
  policyVersion: number;
  redactedPaths: string[];
}
export interface RecordFilterLimitation { code: string; count: number; }
export interface RecordSafePage {
  clusterId: string;
  topicName: string;
  partition: number;
  records: RecordSafeProjection[];
  lowWatermark: number;
  highWatermark: number;
  nextAnchor: RecordAnchorProjection | null;
  previousAnchor: RecordAnchorProjection | null;
  readBudgetOutcome: string;
  filterBudgetOutcome: string;
  limitations: RecordFilterLimitation[];
  policyId: string;
  policyVersion: number;
}
export interface RecordTailFrame { kind: 'records' | 'error' | 'admissionDenied' | 'completed'; page: RecordSafePage | null; failure: { category: string; code: string; message: string; retryable: boolean } | null; }
export interface RecordQuery {
  partition: number;
  anchor?: 'earliest' | 'latest';
  offset?: number;
  timestampUtc?: string;
  direction?: 'forward' | 'previous';
  maxRecords?: number;
  maxBytes?: number;
  keyEquals?: string;
  keyPrefix?: string;
  minimumOffset?: number;
  maximumOffset?: number;
  minimumTimestampUtc?: string;
  maximumTimestampUtc?: string;
  headerName?: string;
  headerEquals?: string;
  headerPrefix?: string;
  filterLanguage?: 'cel' | 'jq';
  filter?: string;
  decode?: boolean;
}

export class ApiProblem extends Error {
  readonly status: number;
  readonly code: string | null;
  constructor(status: number, message: string, code: string | null) {
    super(message); this.name = 'ApiProblem'; this.status = status; this.code = code;
  }
}

const deploymentTokenKey = 'kafdeck.deploymentAccessToken';

function bootstrapDeploymentToken(): string | null {
  const fragment = new URLSearchParams(window.location.hash.startsWith('#') ? window.location.hash.slice(1) : window.location.hash);
  const supplied = fragment.get('access_token');
  if (supplied) {
    sessionStorage.setItem(deploymentTokenKey, supplied);
    fragment.delete('access_token');
    const remainder = fragment.toString();
    history.replaceState(null, '', `${window.location.pathname}${window.location.search}${remainder ? `#${remainder}` : ''}`);
    return supplied;
  }
  return sessionStorage.getItem(deploymentTokenKey);
}

function requestHeaders(accept: string): Record<string, string> {
  const headers: Record<string, string> = { Accept: accept };
  const token = bootstrapDeploymentToken();
  if (token) headers['X-Kafdeck-Access-Token'] = token;
  return headers;
}

async function parseProblem(response: Response): Promise<ApiProblem> {
  let problem: { detail?: string; title?: string; code?: string; type?: string } = {};
  try { problem = (await response.json()) as typeof problem; } catch { /* safe generic fallback */ }
  return new ApiProblem(response.status, problem.detail ?? problem.title ?? 'Kafdeck request failed.', problem.code ?? problem.type ?? null);
}

async function readJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const init: RequestInit = { method: 'GET', headers: requestHeaders('application/json') };
  if (signal !== undefined) init.signal = signal;
  const response = await fetch(path, init);
  if (!response.ok) throw await parseProblem(response);
  return (await response.json()) as T;
}

function clusterPath(clusterId: string) { return `/api/v1/clusters/${encodeURIComponent(clusterId)}`; }
function topicPath(clusterId: string, topicName: string) { return `${clusterPath(clusterId)}/topics/${encodeURIComponent(topicName)}`; }
function recordPath(clusterId: string, topicName: string, partition: number) { return `${topicPath(clusterId, topicName)}/partitions/${partition}/records`; }

function recordParams(query: RecordQuery): URLSearchParams {
  const params = new URLSearchParams();
  if (query.offset !== undefined) params.set('offset', String(query.offset));
  else if (query.timestampUtc) params.set('timestampUtc', query.timestampUtc);
  else params.set('anchor', query.anchor ?? 'earliest');
  params.set('direction', query.direction ?? 'forward');
  if (query.maxRecords !== undefined) params.set('maxRecords', String(query.maxRecords));
  if (query.maxBytes !== undefined) params.set('maxBytes', String(query.maxBytes));
  if (query.keyEquals) params.set('keyEquals', query.keyEquals);
  if (query.keyPrefix) params.set('keyPrefix', query.keyPrefix);
  if (query.minimumOffset !== undefined) params.set('minimumOffset', String(query.minimumOffset));
  if (query.maximumOffset !== undefined) params.set('maximumOffset', String(query.maximumOffset));
  if (query.minimumTimestampUtc) params.set('minimumTimestampUtc', query.minimumTimestampUtc);
  if (query.maximumTimestampUtc) params.set('maximumTimestampUtc', query.maximumTimestampUtc);
  if (query.headerName) params.set('headerName', query.headerName);
  if (query.headerEquals) params.set('headerEquals', query.headerEquals);
  if (query.headerPrefix) params.set('headerPrefix', query.headerPrefix);
  if (query.filter) {
    params.set('filterLanguage', query.filterLanguage ?? 'cel');
    params.set('filter', query.filter);
  }
  if (query.decode !== undefined) params.set('decode', String(query.decode));
  return params;
}

export const kafdeckApi = {
  getOperatorSession(signal?: AbortSignal) { return readJson<OperatorSession>('/api/v1/auth/session', signal); },
  logout() {
    sessionStorage.removeItem(deploymentTokenKey);
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = '/api/v1/auth/logout';
    form.hidden = true;
    document.body.appendChild(form);
    form.submit();
  },
  listClusters(signal?: AbortSignal) { return readJson<{ data: ApiEnvelope<ClusterData>[] }>('/api/v1/clusters', signal); },
  getCluster(clusterId: string, signal?: AbortSignal) { return readJson<ApiEnvelope<ClusterData>>(clusterPath(clusterId), signal); },
  listTopics(clusterId: string, query: string, cursor: string | null, signal?: AbortSignal) {
    const params = new URLSearchParams({ pageSize: '50' });
    if (query.trim()) params.set('q', query.trim());
    if (cursor) params.set('cursor', cursor);
    return readJson<ApiEnvelope<TopicPageData>>(`${clusterPath(clusterId)}/topics?${params}`, signal);
  },
  getTopic(clusterId: string, topicName: string, signal?: AbortSignal) { return readJson<ApiEnvelope<TopicDetailData>>(topicPath(clusterId, topicName), signal); },
  getTopicConfiguration(clusterId: string, topicName: string, signal?: AbortSignal) { return readJson<ApiEnvelope<ConfigurationEntryData[]>>(`${topicPath(clusterId, topicName)}/configuration`, signal); },
  getBrokerConfiguration(clusterId: string, brokerId: number, signal?: AbortSignal) { return readJson<ApiEnvelope<ConfigurationEntryData[]>>(`${clusterPath(clusterId)}/brokers/${brokerId}/configuration`, signal); },
  listConsumerGroups(clusterId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<ConsumerGroupSummary[]>>(`${clusterPath(clusterId)}/consumer-groups`, signal);
  },
  getConsumerGroup(clusterId: string, groupId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<ConsumerGroupDetail>>(`${clusterPath(clusterId)}/consumer-groups/${encodeURIComponent(groupId)}`, signal);
  },
  getConsumerLag(clusterId: string, groupId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<ConsumerLag>>(`${clusterPath(clusterId)}/consumer-groups/${encodeURIComponent(groupId)}/lag`, signal);
  },
  getConsumerDiagnostics(clusterId: string, groupId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<ConsumerDiagnostics>>(`${clusterPath(clusterId)}/consumer-groups/${encodeURIComponent(groupId)}/diagnostics`, signal);
  },
  listSchemaSubjects(clusterId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<SchemaSubjectSummary[]>>(`${clusterPath(clusterId)}/schemas/subjects`, signal);
  },
  listSchemaVersions(clusterId: string, subject: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<SchemaVersionSummary[]>>(`${clusterPath(clusterId)}/schemas/subjects/${encodeURIComponent(subject)}/versions`, signal);
  },
  getSchemaVersion(clusterId: string, subject: string, version: number, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<SchemaVersionDetail>>(`${clusterPath(clusterId)}/schemas/subjects/${encodeURIComponent(subject)}/versions/${version}`, signal);
  },
  getSchemaCompatibility(clusterId: string, subject: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<SchemaCompatibility>>(`${clusterPath(clusterId)}/schemas/subjects/${encodeURIComponent(subject)}/compatibility`, signal);
  },
  diffSchemaVersions(clusterId: string, subject: string, leftVersion: number, rightVersion: number, signal?: AbortSignal) {
    const params = new URLSearchParams({ leftVersion: String(leftVersion), rightVersion: String(rightVersion) });
    return readJson<ReadViewEnvelope<SchemaDiff>>(`${clusterPath(clusterId)}/schemas/subjects/${encodeURIComponent(subject)}/diff?${params}`, signal);
  },
  getConnectInfo(clusterId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<ConnectClusterInfo>>(`${clusterPath(clusterId)}/connect`, signal);
  },
  listConnectors(clusterId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<ConnectConnectorSummary[]>>(`${clusterPath(clusterId)}/connect/connectors`, signal);
  },
  getConnector(clusterId: string, connectorName: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<ConnectConnectorDetail>>(`${clusterPath(clusterId)}/connect/connectors/${encodeURIComponent(connectorName)}`, signal);
  },
  getKsqlInfo(clusterId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<KsqlServerInfo>>(`${clusterPath(clusterId)}/ksql`, signal);
  },
  listKsqlMetadata(clusterId: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<KsqlMetadataItem[]>>(`${clusterPath(clusterId)}/ksql/metadata`, signal);
  },
  getTopicCatalog(clusterId: string, topicName: string, signal?: AbortSignal) {
    return readJson<ReadViewEnvelope<TopicCatalogEntry>>(`${clusterPath(clusterId)}/catalog/topics/${encodeURIComponent(topicName)}`, signal);
  },
  getRecords(clusterId: string, topicName: string, query: RecordQuery, signal?: AbortSignal) {
    return readJson<RecordSafePage>(`${recordPath(clusterId, topicName, query.partition)}?${recordParams(query)}`, signal);
  },
  async exportRecords(clusterId: string, topicName: string, query: RecordQuery, format: 'json' | 'ndjson' | 'csv', signal?: AbortSignal) {
    const params = recordParams(query); params.set('format', format);
    const init: RequestInit = { method: 'GET', headers: requestHeaders('*/*') };
    if (signal !== undefined) init.signal = signal;
    const response = await fetch(`${recordPath(clusterId, topicName, query.partition)}/export?${params}`, init);
    if (!response.ok) throw await parseProblem(response);
    return response.blob();
  },
  async tailRecords(clusterId: string, topicName: string, query: RecordQuery, onFrame: (frame: RecordTailFrame) => void, signal?: AbortSignal) {
    const params = recordParams({ ...query, direction: 'forward' });
    const init: RequestInit = { method: 'GET', headers: requestHeaders('text/event-stream') };
    if (signal !== undefined) init.signal = signal;
    const response = await fetch(`${recordPath(clusterId, topicName, query.partition)}/tail?${params}`, init);
    if (!response.ok) throw await parseProblem(response);
    if (!response.body) throw new Error('Live tail response body is unavailable.');
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let pending = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      pending += decoder.decode(value, { stream: true });
      let boundary = pending.indexOf('\n\n');
      while (boundary >= 0) {
        const event = pending.slice(0, boundary); pending = pending.slice(boundary + 2);
        const data = event.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).trimStart()).join('\n');
        if (data) onFrame(JSON.parse(data) as RecordTailFrame);
        boundary = pending.indexOf('\n\n');
      }
    }
  },
};
