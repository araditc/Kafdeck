export type CommandAvailability = 'available' | 'unavailable';

export type ProductCommand = {
  id: string;
  label: string;
  description: string;
  keywords: string[];
  targetId: string;
  focusId?: string;
  availability: CommandAvailability;
  unavailableReason?: string;
};

export const productCommands: readonly ProductCommand[] = [
  { id: 'cluster', label: 'Select cluster', description: 'Focus the active cluster selector.', keywords: ['cluster', 'environment', 'profile'], targetId: 'kafdeck-title', focusId: 'cluster-selector', availability: 'available' },
  { id: 'overview', label: 'Cluster overview', description: 'Open the current cluster overview.', keywords: ['overview', 'health', 'controller'], targetId: 'overview', availability: 'available' },
  { id: 'brokers', label: 'Brokers', description: 'Navigate to broker metadata and configuration.', keywords: ['broker', 'controller', 'configuration'], targetId: 'brokers', availability: 'available' },
  { id: 'topics', label: 'Topics', description: 'Navigate to the topic catalog and focus topic search.', keywords: ['topic', 'catalog', 'documentation', 'partition'], targetId: 'topics', focusId: 'topic-search', availability: 'available' },
  { id: 'consumers', label: 'Consumer groups', description: 'Navigate to consumer groups, offsets and lag.', keywords: ['consumer', 'group', 'lag', 'offset'], targetId: 'consumers', availability: 'available' },
  { id: 'schemas', label: 'Schema Registry', description: 'Navigate to schemas, references, diff and compatibility tooling.', keywords: ['schema', 'registry', 'avro', 'protobuf', 'json schema', 'compatibility'], targetId: 'schemas', availability: 'available' },
  { id: 'serde', label: 'Controlled SerDe', description: 'Navigate to bounded CBOR, XML and MessagePack tooling.', keywords: ['serde', 'cbor', 'xml', 'messagepack'], targetId: 'serde', focusId: 'serde-format', availability: 'available' },
  { id: 'connect', label: 'Kafka Connect', description: 'Navigate to configured Connect profiles, plugins and connectors.', keywords: ['connect', 'connector', 'plugin', 'task'], targetId: 'connect-title', focusId: 'connect-profile-selector', availability: 'available' },
  { id: 'ksql', label: 'ksqlDB', description: 'Navigate to bounded read-only ksqlDB query tooling.', keywords: ['ksql', 'sql', 'query', 'streaming'], targetId: 'ksql-title', focusId: 'ksql-query-editor', availability: 'available' },
  { id: 'streams', label: 'Kafka Streams evidence', description: 'Navigate to registered topology and state-store evidence.', keywords: ['streams', 'topology', 'state store', 'rocksdb'], targetId: 'streams-title', availability: 'available' },
  { id: 'lineage', label: 'Data lineage', description: 'Navigate to provenance-labelled observed and inferred lineage.', keywords: ['lineage', 'provenance', 'graph'], targetId: 'lineage-title', availability: 'available' },
  { id: 'mutations', label: 'Governed mutations', description: 'Navigate to authorization, preview, approval and execution workflows.', keywords: ['mutation', 'approval', 'operation'], targetId: 'mutations', focusId: 'mutation-workflow', availability: 'available' },
  { id: 'data-jobs', label: 'Replay / forward data jobs', description: 'Navigate to governed finite replay and forwarding jobs.', keywords: ['replay', 'forward', 'dlq', 'reprocess', 'job'], targetId: 'mutations', focusId: 'mutation-workflow', availability: 'available' },
  { id: 'generator', label: 'Data Generator', description: 'Navigate to bounded governed Smart Mock and Data Generator workflows.', keywords: ['generator', 'mock', 'data', 'records'], targetId: 'mutations', focusId: 'mutation-workflow', availability: 'available' },
  { id: 'fleet', label: 'Fleet Operations', description: 'Navigate to runtime capability status.', keywords: ['fleet', 'acl', 'scram', 'quota', 'maintenance'], targetId: 'fleet', availability: 'available' },
] as const;

export function filterProductCommands(query: string): ProductCommand[] {
  const normalized = query.trim().toLocaleLowerCase();
  if (!normalized) return [...productCommands];

  return productCommands.filter(command => {
    const haystack = [
      command.label,
      command.description,
      ...command.keywords,
    ].join(' ').toLocaleLowerCase();
    return haystack.includes(normalized);
  });
}
