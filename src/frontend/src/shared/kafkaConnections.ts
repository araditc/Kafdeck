export const kafkaProtocols = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'] as const;
export const saslMechanisms = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'] as const;
export type KafkaProtocol = typeof kafkaProtocols[number];
export type SaslMechanism = typeof saslMechanisms[number];

export interface KafkaConnectionProfile {
  name: string;
  bootstrapServers: string[];
  protocol: KafkaProtocol;
  mechanism: SaslMechanism | null;
}

export interface KafkaConnectionDraft extends KafkaConnectionProfile {
  username: string | null;
  password: string | null;
  caCertificate: string | null;
  clientCertificate: string | null;
  clientKey: string | null;
  clientKeyPassword: string | null;
}

export interface KafkaConnectionTarget {
  id: string;
  name?: string;
  bootstrapServers: string[];
  protocol: KafkaProtocol;
  mechanism?: SaslMechanism | null;
  registered?: boolean;
}

export interface KafkaConnectionObservation {
  clusterId: string | null;
  observedAt: string;
  brokers: { id: number; host: string; port: number }[];
  topics: { name: string; partitions: number }[];
  topicsTruncated: boolean;
  requiresRegistration?: boolean;
  savedTarget?: KafkaConnectionTarget | null;
}

const recentConnectionsPrefix = 'kafdeck.kafka-connections.v1.';

function normalize(value: unknown): KafkaConnectionProfile[] {
  if (!Array.isArray(value)) return [];
  const profiles: KafkaConnectionProfile[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== 'object') continue;
    const { name, bootstrapServers, protocol, mechanism } = entry as Partial<KafkaConnectionProfile>;
    if (typeof name !== 'string' || !name.trim() || name.length > 60 || /[\p{Cc}\u202a-\u202e\u2066-\u2069]/u.test(name) ||
        !Array.isArray(bootstrapServers) || bootstrapServers.length < 1 || bootstrapServers.length > 16 ||
        !bootstrapServers.every(server => typeof server === 'string' && /^[\w.[\]:-]{1,300}$/u.test(server)) ||
        protocol === undefined || !kafkaProtocols.includes(protocol)) continue;
    const sasl = protocol.startsWith('SASL_');
    if (sasl && (!mechanism || !saslMechanisms.includes(mechanism))) continue;
    profiles.push({ name, bootstrapServers, protocol, mechanism: sasl ? mechanism ?? null : null });
    if (profiles.length === 5) break;
  }
  return profiles;
}

export function recentKafkaConnections(scope: string): KafkaConnectionProfile[] {
  if (!/^[a-f0-9]{64}$/.test(scope)) return [];
  const storageKey = recentConnectionsPrefix + scope;
  try { return normalize(JSON.parse(window.localStorage.getItem(storageKey) ?? '[]')); }
  catch { return []; }
}

function storeProfiles(scope: string, profiles: KafkaConnectionProfile[]) {
  const safeProfiles = normalize(profiles);
  if (!/^[a-f0-9]{64}$/.test(scope)) return safeProfiles;
  const storageKey = recentConnectionsPrefix + scope;
  try { window.localStorage.setItem(storageKey, JSON.stringify(safeProfiles)); }
  catch { /* The connection remains usable with browser storage disabled. */ }
  return safeProfiles;
}

function matches(left: KafkaConnectionProfile, right: KafkaConnectionProfile) {
  return left.name === right.name && left.protocol === right.protocol && left.mechanism === right.mechanism &&
    left.bootstrapServers.join(',') === right.bootstrapServers.join(',');
}

export function rememberKafkaConnection(scope: string, profile: KafkaConnectionProfile) {
  return storeProfiles(scope, [profile, ...recentKafkaConnections(scope).filter(item => !matches(item, profile))]);
}

export function forgetKafkaConnection(scope: string, profile: KafkaConnectionProfile) {
  return storeProfiles(scope, recentKafkaConnections(scope).filter(item => !matches(item, profile)));
}
