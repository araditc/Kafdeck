export type Freshness = 'fresh' | 'stale';

export interface ApiObservation {
  observedAt: string;
  freshness: Freshness;
  cacheAgeMs: number;
  partial: boolean;
}

export interface ApiLimitation {
  capability: string | null;
  state: string;
  reason: string | null;
}

export interface ApiEnvelope<T> {
  data: T;
  observation: ApiObservation;
  limitations: ApiLimitation[];
}

export interface BrokerProjection {
  brokerId: number;
  host: string;
  port: number;
  rack: string | null;
  isController: boolean;
}

export interface ClusterData {
  clusterId: string;
  kafkaClusterId: string | null;
  controllerBrokerId: number | null;
  brokers: BrokerProjection[];
  health: string | number;
  healthReasons: Array<{ code: string | number; description: string }>;
  failureCode: string | null;
}

export interface TopicListItem {
  name: string;
  partitionCount: number;
  isInternal: boolean;
  offlinePartitionCount: number | null;
  underReplicatedPartitionCount: number | null;
  anomalyState: string | number;
}

export interface TopicPageData {
  items: TopicListItem[];
  nextCursor: string | null;
}

export interface PartitionProjection {
  partitionId: number;
  leaderBrokerId: number | null;
  replicaBrokerIds: number[];
  inSyncReplicaBrokerIds: number[];
  outOfSyncReplicaBrokerIds: number[];
  health: string | number;
  healthReasons: string[];
}

export interface TopicDetailData {
  name: string;
  isInternal: boolean;
  partitions: PartitionProjection[];
  offlinePartitionCount: number;
  underReplicatedPartitionCount: number;
  anomalyState: string | number;
}

export interface ConfigurationEntryData {
  name: string;
  value: string | null;
  isSensitive: boolean;
  isReadOnly: boolean;
  source: string | null;
}

export class ApiProblem extends Error {
  readonly status: number;
  readonly code: string | null;

  constructor(status: number, message: string, code: string | null) {
    super(message);
    this.name = 'ApiProblem';
    this.status = status;
    this.code = code;
  }
}

async function readJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const init: RequestInit = { method: 'GET', headers: { Accept: 'application/json' } };
  if (signal !== undefined) init.signal = signal;
  const response = await fetch(path, init);
  const payload = (await response.json()) as unknown;
  if (!response.ok) {
    const problem = payload as { detail?: string; title?: string; code?: string };
    throw new ApiProblem(response.status, problem.detail ?? problem.title ?? 'Kafdeck request failed.', problem.code ?? null);
  }
  return payload as T;
}

function clusterPath(clusterId: string) {
  return `/api/v1/clusters/${encodeURIComponent(clusterId)}`;
}

function topicPath(clusterId: string, topicName: string) {
  return `${clusterPath(clusterId)}/topics/${encodeURIComponent(topicName)}`;
}

export const kafdeckApi = {
  listClusters(signal?: AbortSignal) {
    return readJson<{ data: ApiEnvelope<ClusterData>[] }>('/api/v1/clusters', signal);
  },
  getCluster(clusterId: string, signal?: AbortSignal) {
    return readJson<ApiEnvelope<ClusterData>>(clusterPath(clusterId), signal);
  },
  listTopics(clusterId: string, query: string, cursor: string | null, signal?: AbortSignal) {
    const params = new URLSearchParams({ pageSize: '50' });
    if (query.trim()) params.set('q', query.trim());
    if (cursor) params.set('cursor', cursor);
    return readJson<ApiEnvelope<TopicPageData>>(`${clusterPath(clusterId)}/topics?${params}`, signal);
  },
  getTopic(clusterId: string, topicName: string, signal?: AbortSignal) {
    return readJson<ApiEnvelope<TopicDetailData>>(topicPath(clusterId, topicName), signal);
  },
  getTopicConfiguration(clusterId: string, topicName: string, signal?: AbortSignal) {
    return readJson<ApiEnvelope<ConfigurationEntryData[]>>(`${topicPath(clusterId, topicName)}/configuration`, signal);
  },
  getBrokerConfiguration(clusterId: string, brokerId: number, signal?: AbortSignal) {
    return readJson<ApiEnvelope<ConfigurationEntryData[]>>(`${clusterPath(clusterId)}/brokers/${brokerId}/configuration`, signal);
  },
};
