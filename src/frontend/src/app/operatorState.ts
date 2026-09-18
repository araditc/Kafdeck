import type { ApiEnvelope, ClusterData } from '../shared/api.js';

export interface ClusterViewState {
  selectedClusterId: string | null;
  cluster: ApiEnvelope<ClusterData> | null;
  topicQuery: string;
  topicCursor: string | null;
}

export const initialClusterViewState: ClusterViewState = {
  selectedClusterId: null,
  cluster: null,
  topicQuery: '',
  topicCursor: null,
};

export function selectCluster(state: ClusterViewState, clusterId: string): ClusterViewState {
  if (state.selectedClusterId === clusterId) return state;

  return {
    selectedClusterId: clusterId,
    cluster: null,
    topicQuery: '',
    topicCursor: null,
  };
}

export function describeObservation(observation: { freshness: string; partial: boolean }): string {
  if (observation.partial && observation.freshness === 'stale') return 'Partial · stale';
  if (observation.partial) return 'Partial';
  if (observation.freshness === 'stale') return 'Stale';
  return 'Current';
}

export const visibleRefreshIntervalMs = 10_000;

export function shouldAutoRefresh(visibilityState: DocumentVisibilityState): boolean {
  return visibilityState === 'visible';
}
