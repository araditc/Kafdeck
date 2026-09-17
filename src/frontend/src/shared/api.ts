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
  offlinePartitionCount: number;
  underReplicatedPartitionCount: number;
  anomalyState: string | number;
}

export interface TopicPageData {
  items: TopicListItem[];
  nextCursor: string | null;
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
  const response = await fetch(path, { method: 'GET', headers: { Accept: 'application/json' }, signal });
  const payload = (await response.json()) as unknown;
  if (!response.ok) {
    const problem = payload as { detail?: string; title?: string; code?: string };
    throw new ApiProblem(response.status, problem.detail ?? problem.title ?? 'Kafdeck request failed.', problem.code ?? null);
  }
  return payload as T;
}

export const kafdeckApi = {
  listClusters(signal?: AbortSignal) {
    return readJson<{ data: ApiEnvelope<ClusterData>[] }>('/api/v1/clusters', signal);
  },
  getCluster(clusterId: string, signal?: AbortSignal) {
    return readJson<ApiEnvelope<ClusterData>>(`/api/v1/clusters/${encodeURIComponent(clusterId)}`, signal);
  },
  listTopics(clusterId: string, query: string, cursor: string | null, signal?: AbortSignal) {
    const params = new URLSearchParams({ pageSize: '50' });
    if (query.trim()) params.set('q', query.trim());
    if (cursor) params.set('cursor', cursor);
    return readJson<ApiEnvelope<TopicPageData>>(`/api/v1/clusters/${encodeURIComponent(clusterId)}/topics?${params}`, signal);
  },
};
