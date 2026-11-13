import type { DashboardPage } from './dashboardPage.js';

export const governancePageKeys = {
  'ownership-catalog': 'ownershipCatalog',
  'governance-policy': 'governancePolicy',
  'service-requests': 'serviceRequests',
  'governance-attribution': 'governanceAttribution',
} as const;

export interface GovernanceNavigation {
  clusterId: string;
  ownershipCatalog: boolean;
  governancePolicy: boolean;
  serviceRequests: boolean;
  governanceAttribution: boolean;
}

export function isGovernancePage(page: DashboardPage): page is keyof typeof governancePageKeys {
  return Object.hasOwn(governancePageKeys, page);
}

// Bind presentation to the exact verified session and selected cluster. Missing,
// stale or malformed server hints never make a governance page available.
export function governancePageAvailable(page: DashboardPage, clusterId: string, sessionKey: string | null,
  snapshot: { sessionKey: string; value: GovernanceNavigation } | null): boolean {
  if (!isGovernancePage(page)) return true;
  return !!clusterId && !!sessionKey && snapshot?.sessionKey === sessionKey &&
    snapshot.value.clusterId === clusterId && snapshot.value[governancePageKeys[page]] === true;
}
