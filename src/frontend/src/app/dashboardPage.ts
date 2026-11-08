export const dashboardPages = [
  ['overview', 'Overview'], ['brokers', 'Brokers'], ['topics', 'Topics'],
  ['consumers', 'Consumers'], ['schemas', 'Schemas'], ['ecosystem', 'Ecosystem'],
  ['fleet', 'Fleet'], ['automation', 'Automation'], ['ownership-catalog', 'Ownership catalog'],
  ['governance-policy', 'Governance policy'], ['service-requests', 'Requests'],
  ['governance-attribution', 'Measured attribution'], ['notifications', 'Notifications'],
] as const;

export type DashboardPage = typeof dashboardPages[number][0];

export function dashboardPageFor(target: string): DashboardPage | undefined {
  if (target === 'serde') return 'schemas';
  if (target === 'mutations') return 'automation';
  if (['connect-title', 'ksql-title', 'streams-title', 'lineage-title'].includes(target)) return 'ecosystem';
  return dashboardPages.find(([id]) => id === target)?.[0];
}
