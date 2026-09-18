import { useCallback, useEffect, useState } from 'react';
import {
  ApiProblem,
  kafdeckApi,
  type ApiEnvelope,
  type ClusterData,
  type ConfigurationEntryData,
  type TopicDetailData,
  type TopicListItem,
} from '../shared/api.js';
import { productDescription, productName } from '../shared/product.js';
import { describeObservation } from './operatorState.js';

function observationText(envelope: ApiEnvelope<unknown>) {
  const observed = new Date(envelope.observation.observedAt);
  return `${describeObservation(envelope.observation)} · observed ${observed.toLocaleString()}`;
}

function problemText(reason: unknown) {
  if (reason instanceof ApiProblem && reason.status === 403) return 'Access denied by Kafka authorization policy.';
  if (reason instanceof ApiProblem && reason.status === 501) return 'This capability is unsupported by the connected Kafka cluster.';
  return reason instanceof Error ? reason.message : 'The requested observation could not be loaded.';
}

function ConfigurationView({ entries }: { entries: ConfigurationEntryData[] }) {
  if (entries.length === 0) return <p>No configuration entries are observable.</p>;
  return <table><thead><tr><th scope="col">Name</th><th scope="col">Value</th><th scope="col">Source</th></tr></thead><tbody>{entries.map(entry => <tr key={entry.name}><th scope="row">{entry.name}</th><td>{entry.isSensitive ? 'Sensitive value redacted' : (entry.value ?? 'Not set')}</td><td>{entry.source ?? 'Unknown'}</td></tr>)}</tbody></table>;
}

export function AppShell() {
  const [clusters, setClusters] = useState<ApiEnvelope<ClusterData>[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState<string>('');
  const [selected, setSelected] = useState<ApiEnvelope<ClusterData> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [topicQuery, setTopicQuery] = useState('');
  const [topics, setTopics] = useState<TopicListItem[]>([]);
  const [topicCursor, setTopicCursor] = useState<string | null>(null);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState<string | null>(null);
  const [topicDetail, setTopicDetail] = useState<ApiEnvelope<TopicDetailData> | null>(null);
  const [topicConfiguration, setTopicConfiguration] = useState<ConfigurationEntryData[] | null>(null);
  const [topicDetailError, setTopicDetailError] = useState<string | null>(null);

  const loadClusters = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    try {
      const response = await kafdeckApi.listClusters(signal);
      setClusters(response.data);
      const nextId = selectedClusterId || response.data[0]?.data.clusterId || '';
      setSelectedClusterId(nextId);
      setSelected(response.data.find(item => item.data.clusterId === nextId) ?? null);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      setError(problemText(reason));
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [selectedClusterId]);

  const loadTopics = useCallback(async (clusterId: string, query: string, cursor: string | null, append = false, signal?: AbortSignal) => {
    if (!clusterId) return;
    setTopicsLoading(true);
    setTopicsError(null);
    try {
      const response = await kafdeckApi.listTopics(clusterId, query, cursor, signal);
      setTopics(previous => append ? [...previous, ...response.data.items] : response.data.items);
      setTopicCursor(response.data.nextCursor);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      setTopicsError(problemText(reason));
    } finally {
      if (!signal?.aborted) setTopicsLoading(false);
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void loadClusters(controller.signal);
    return () => controller.abort();
  }, [loadClusters]);

  useEffect(() => {
    if (!selectedClusterId) return;
    const controller = new AbortController();
    void loadTopics(selectedClusterId, topicQuery, null, false, controller.signal);
    return () => controller.abort();
  }, [loadTopics, selectedClusterId, topicQuery]);

  const selectCluster = (clusterId: string) => {
    setSelectedClusterId(clusterId);
    setSelected(clusters.find(item => item.data.clusterId === clusterId) ?? null);
    setError(null);
    setTopicQuery('');
    setTopics([]);
    setTopicCursor(null);
    setTopicDetail(null);
    setTopicConfiguration(null);
    setTopicDetailError(null);
  };

  const openTopic = async (topicName: string) => {
    setTopicDetail(null);
    setTopicConfiguration(null);
    setTopicDetailError(null);
    try {
      const detail = await kafdeckApi.getTopic(selectedClusterId, topicName);
      setTopicDetail(detail);
      try {
        const configuration = await kafdeckApi.getTopicConfiguration(selectedClusterId, topicName);
        setTopicConfiguration(configuration.data);
      } catch (reason) {
        setTopicDetailError(problemText(reason));
      }
    } catch (reason) {
      setTopicDetailError(problemText(reason));
    }
  };

  return <main aria-labelledby="kafdeck-title">
    <header><div><h1 id="kafdeck-title">{productName}</h1><p>{productDescription}</p></div><div><label htmlFor="cluster-selector">Cluster</label>{' '}<select id="cluster-selector" value={selectedClusterId} onChange={event => selectCluster(event.target.value)} disabled={loading || clusters.length === 0}>{clusters.map(cluster => <option key={cluster.data.clusterId} value={cluster.data.clusterId}>{cluster.data.clusterId}</option>)}</select>{' '}<button type="button" onClick={() => void loadClusters()} disabled={loading}>Refresh</button></div></header>
    <nav aria-label="Kafdeck sections"><a href="#overview">Overview</a>{' · '}<a href="#brokers">Brokers</a>{' · '}<a href="#topics">Topics</a></nav>
    {loading && <p role="status">Loading cluster observations…</p>}{error && <p role="alert">{error}</p>}{!loading && !error && clusters.length === 0 && <p role="status">No clusters are configured.</p>}
    {selected && <>
      <section id="overview" aria-labelledby="overview-title"><h2 id="overview-title">Cluster overview</h2><p><strong>{selected.data.clusterId}</strong> · {observationText(selected)}</p><dl><dt>Kafka cluster ID</dt><dd>{selected.data.kafkaClusterId ?? 'Not observed'}</dd><dt>Health</dt><dd>{String(selected.data.health)}</dd><dt>Controller</dt><dd>{selected.data.controllerBrokerId ?? 'Not observed'}</dd><dt>Brokers</dt><dd>{selected.data.brokers.length}</dd></dl>{selected.limitations.length > 0 && <aside aria-label="Cluster limitations"><h3>Limitations</h3><ul>{selected.limitations.map((item, index) => <li key={`${item.capability}-${index}`}>{item.capability ?? 'cluster'}: {item.state}{item.reason ? ` — ${item.reason}` : ''}</li>)}</ul></aside>}</section>
      <section id="brokers" aria-labelledby="brokers-title"><h2 id="brokers-title">Brokers</h2>{selected.data.brokers.length === 0 ? <p>No broker metadata is currently observable.</p> : <table><thead><tr><th scope="col">ID</th><th scope="col">Host</th><th scope="col">Port</th><th scope="col">Role</th></tr></thead><tbody>{selected.data.brokers.map(broker => <tr key={broker.brokerId}><th scope="row">{broker.brokerId}</th><td>{broker.host}</td><td>{broker.port}</td><td>{broker.isController ? 'Controller' : 'Broker'}</td></tr>)}</tbody></table>}</section>
      <section id="topics" aria-labelledby="topics-title"><h2 id="topics-title">Topics</h2><label htmlFor="topic-search">Search topics</label>{' '}<input id="topic-search" type="search" value={topicQuery} onChange={event => setTopicQuery(event.target.value)} />
        {topicsLoading && topics.length === 0 && <p role="status">Loading topics…</p>}{topicsError && <p role="alert">{topicsError}</p>}{!topicsLoading && !topicsError && topics.length === 0 && <p>No topics match the current search.</p>}
        {topics.length > 0 && <table><thead><tr><th scope="col">Topic</th><th scope="col">Partitions</th><th scope="col">Offline</th><th scope="col">Under replicated</th><th scope="col">State</th></tr></thead><tbody>{topics.map(topic => <tr key={topic.name}><th scope="row"><button type="button" onClick={() => void openTopic(topic.name)}>{topic.name}</button></th><td>{topic.partitionCount}</td><td>{topic.offlinePartitionCount ?? 'Unknown'}</td><td>{topic.underReplicatedPartitionCount ?? 'Unknown'}</td><td>{String(topic.anomalyState)}</td></tr>)}</tbody></table>}
        {topicCursor && <button type="button" disabled={topicsLoading} onClick={() => void loadTopics(selectedClusterId, topicQuery, topicCursor, true)}>{topicsLoading ? 'Loading…' : 'Load more topics'}</button>}
        {topicDetailError && <p role="alert">{topicDetailError}</p>}{topicDetail && <article aria-labelledby="topic-detail-title"><h3 id="topic-detail-title">Topic: {topicDetail.data.name}</h3><p>{observationText(topicDetail)} · {topicDetail.data.partitions.length} partitions · {topicDetail.data.offlinePartitionCount} offline · {topicDetail.data.underReplicatedPartitionCount} under replicated</p><h4>Partitions</h4>{topicDetail.data.partitions.length === 0 ? <p>No partitions are observable.</p> : <table><thead><tr><th scope="col">ID</th><th scope="col">Leader</th><th scope="col">Replicas</th><th scope="col">ISR</th><th scope="col">Out of sync</th><th scope="col">Health</th></tr></thead><tbody>{topicDetail.data.partitions.map(partition => <tr key={partition.partitionId}><th scope="row">{partition.partitionId}</th><td>{partition.leaderBrokerId ?? 'No leader'}</td><td>{partition.replicaBrokerIds.join(', ')}</td><td>{partition.inSyncReplicaBrokerIds.join(', ')}</td><td>{partition.outOfSyncReplicaBrokerIds.join(', ') || 'None'}</td><td>{partition.healthReasons.length > 0 ? partition.healthReasons.join('; ') : String(partition.health)}</td></tr>)}</tbody></table>}<h4>Configuration</h4>{topicConfiguration === null ? (topicDetailError ? null : <p role="status">Loading configuration…</p>) : <ConfigurationView entries={topicConfiguration} />}</article>}
      </section>
    </>}
  </main>;
}
