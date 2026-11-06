import { t } from '../shared/i18n.js';
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
  if (observation.partial && observation.freshness === 'stale') return t("Partial · stale");
  if (observation.partial) return t("Partial");
  if (observation.freshness === 'stale') return t("Stale");
  return t("Current");
}

export const visibleRefreshIntervalMs = 10_000;

export function shouldAutoRefresh(visibilityState: DocumentVisibilityState): boolean {
  return visibilityState === 'visible';
}
