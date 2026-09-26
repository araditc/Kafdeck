import { useCallback, useEffect, useState } from 'react';
import {
  ApiProblem,
  kafdeckApi,
  type ApiEnvelope,
  type ClusterData,
  type ConfigurationEntryData,
  type TopicDetailData,
  type TopicListItem,
  type OperatorSession,
  type ReadViewEnvelope,
  type TopicCatalogEntry,
} from '../shared/api.js';
import { RecordExplorer } from '../features/records/RecordExplorer.js';
import { ReadViewsExplorer } from '../features/readviews/ReadViewsExplorer.js';
import { productDescription, productName } from '../shared/product.js';
import { describeObservation, shouldAutoRefresh, visibleRefreshIntervalMs } from './operatorState.js';

function observationText(envelope: ApiEnvelope<unknown>) {
  const observed = new Date(envelope.observation.observedAt);
  return `${describeObservation(envelope.observation)} · observed ${observed.toLocaleString()}`;
}

function problemText(reason: unknown) {
  if (reason instanceof ApiProblem && reason.status === 401) return 'Operator authentication is required.';
  if (reason instanceof ApiProblem && reason.status === 403 && reason.code === 'urn:kafdeck:problem:operator-authorization-denied') return 'Access denied by Kafdeck operator authorization policy.';
  if (reason instanceof ApiProblem && reason.status === 403) return 'Access denied by Kafka authorization policy.';
  if (reason instanceof ApiProblem && reason.status === 501) return 'This capability is unsupported by the connected Kafka cluster.';
  return reason instanceof Error ? reason.message : 'The requested observation could not be loaded.';
}

function ConfigurationView({ entries }: { entries: ConfigurationEntryData[] }) {
  if (entries.length === 0) return <p className="text-secondary">No configuration entries are observable.</p>;
  return <div className="table-responsive"><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">Name</th><th scope="col">Value</th><th scope="col">Source</th></tr></thead><tbody>{entries.map(entry => <tr key={entry.name}><th scope="row">{entry.name}</th><td>{entry.isSensitive ? <span className="badge bg-orange-lt">Sensitive value redacted</span> : (entry.value ?? 'Not set')}</td><td>{entry.source ?? 'Unknown'}</td></tr>)}</tbody></table></div>;
}

export function AppShell() {
  const [operator, setOperator] = useState<OperatorSession | null>(null);
  const [authenticationRequired, setAuthenticationRequired] = useState(false);
  const [clusters, setClusters] = useState<ApiEnvelope<ClusterData>[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState<string>('');
  const [selected, setSelected] = useState<ApiEnvelope<ClusterData> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [brokerConfiguration, setBrokerConfiguration] = useState<{ brokerId: number; entries: ConfigurationEntryData[] } | null>(null);
  const [brokerError, setBrokerError] = useState<string | null>(null);
  const [topicQuery, setTopicQuery] = useState('');
  const [topics, setTopics] = useState<TopicListItem[]>([]);
  const [topicCursor, setTopicCursor] = useState<string | null>(null);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState<string | null>(null);
  const [topicDetail, setTopicDetail] = useState<ApiEnvelope<TopicDetailData> | null>(null);
  const [topicConfiguration, setTopicConfiguration] = useState<ConfigurationEntryData[] | null>(null);
  const [topicDetailError, setTopicDetailError] = useState<string | null>(null);
  const [topicCatalog, setTopicCatalog] = useState<ReadViewEnvelope<TopicCatalogEntry> | null>(null);

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
      if (reason instanceof ApiProblem && reason.status === 401) setAuthenticationRequired(true);
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
    void kafdeckApi.getOperatorSession(controller.signal)
      .then(session => {
        setOperator(session);
        setAuthenticationRequired(false);
      })
      .catch(reason => {
        if (reason instanceof DOMException && reason.name === 'AbortError') return;
        if (reason instanceof ApiProblem && reason.status === 401) setAuthenticationRequired(true);
      });
    void loadClusters(controller.signal);
    return () => controller.abort();
  }, [loadClusters]);

  useEffect(() => {
    if (!selectedClusterId) return;
    const controller = new AbortController();
    void loadTopics(selectedClusterId, topicQuery, null, false, controller.signal);
    return () => controller.abort();
  }, [loadTopics, selectedClusterId, topicQuery]);

  useEffect(() => {
    if (!selectedClusterId) return;
    let controller: AbortController | null = null;
    const refresh = () => {
      if (!shouldAutoRefresh(document.visibilityState)) return;
      controller?.abort();
      controller = new AbortController();
      void loadClusters(controller.signal);
      void loadTopics(selectedClusterId, topicQuery, null, false, controller.signal);
    };
    const timer = window.setInterval(refresh, visibleRefreshIntervalMs);
    document.addEventListener('visibilitychange', refresh);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', refresh);
      controller?.abort();
    };
  }, [loadClusters, loadTopics, selectedClusterId, topicQuery]);

  const selectCluster = (clusterId: string) => {
    setSelectedClusterId(clusterId);
    setSelected(clusters.find(item => item.data.clusterId === clusterId) ?? null);
    setError(null);
    setBrokerConfiguration(null);
    setBrokerError(null);
    setTopicQuery('');
    setTopics([]);
    setTopicCursor(null);
    setTopicDetail(null);
    setTopicConfiguration(null);
    setTopicDetailError(null);
    setTopicCatalog(null);
  };

  const openBroker = async (brokerId: number) => {
    setBrokerConfiguration(null);
    setBrokerError(null);
    try {
      const configuration = await kafdeckApi.getBrokerConfiguration(selectedClusterId, brokerId);
      setBrokerConfiguration({ brokerId, entries: configuration.data });
    } catch (reason) {
      setBrokerError(problemText(reason));
    }
  };

  const openTopic = async (topicName: string) => {
    setTopicDetail(null);
    setTopicConfiguration(null);
    setTopicDetailError(null);
    setTopicCatalog(null);
    try {
      const detail = await kafdeckApi.getTopic(selectedClusterId, topicName);
      setTopicDetail(detail);
      try {
        setTopicCatalog(await kafdeckApi.getTopicCatalog(selectedClusterId, topicName));
      } catch {
        // Catalog metadata is optional and separately authorized.
        setTopicCatalog(null);
      }
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

  return <div className="page kafdeck-shell">
    <header className="navbar navbar-expand-md bg-surface d-print-none kafdeck-topbar">
      <div className="container-xl">
        <h1 id="kafdeck-title" className="navbar-brand navbar-brand-autodark m-0">
          <a className="kafdeck-brand" href="#overview" aria-label={`${productName} home`}>
            <img className="kafdeck-brand-logo" src="/kafdeck-logo.webp" alt={productName} width={42} height={42} />
            <span className="kafdeck-accent" aria-hidden="true" />
            <span className="kafdeck-brand-copy">
              <span className="kafdeck-brand-name">{productName}</span>
              <span className="kafdeck-brand-subtitle">{productDescription}</span>
            </span>
          </a>
        </h1>
        <div className="kafdeck-toolbar">
          {operator && <div className="kafdeck-auth"><span className="text-secondary">Signed in as</span><strong>{operator.displayName ?? operator.email ?? 'operator'}</strong><button className="btn btn-sm btn-outline-secondary" type="button" onClick={() => void kafdeckApi.logout()}>Sign out</button></div>}
          {authenticationRequired && <div className="kafdeck-auth"><a className="btn btn-sm btn-primary" href="/api/v1/auth/login">Sign in with your identity provider</a></div>}
          <div>
            <label className="form-label" htmlFor="cluster-selector">Cluster</label>
            <select className="form-select form-select-sm" id="cluster-selector" value={selectedClusterId} onChange={event => selectCluster(event.target.value)} disabled={loading || clusters.length === 0}>{clusters.map(cluster => <option key={cluster.data.clusterId} value={cluster.data.clusterId}>{cluster.data.clusterId}</option>)}</select>
          </div>
          <button className="btn btn-sm btn-outline-primary" type="button" onClick={() => void loadClusters()} disabled={loading}>Refresh</button>
        </div>
      </div>
    </header>
    <nav className="kafdeck-section-nav" aria-label="Kafdeck sections">
      <div className="container-xl">
        <ul className="nav nav-pills py-2">
          <li className="nav-item"><a className="nav-link" href="#overview">Overview</a></li>
          <li className="nav-item"><a className="nav-link" href="#brokers">Brokers</a></li>
          <li className="nav-item"><a className="nav-link" href="#topics">Topics</a></li>
          <li className="nav-item"><a className="nav-link" href="#consumers">Consumers</a></li>
          <li className="nav-item"><a className="nav-link" href="#schemas">Schemas</a></li>
          <li className="nav-item"><a className="nav-link" href="#ecosystem">Ecosystem</a></li>
        </ul>
      </div>
    </nav>
    <main className="page-wrapper" aria-labelledby="kafdeck-title">
      <div className="page-body kafdeck-main">
        <div className="container-xl">
          <div className="page-header d-print-none kafdeck-page-header">
            <div className="row g-2 align-items-center">
              <div className="col">
                <div className="page-pretitle">Kafka operations</div>
                <h2 className="page-title">Cluster Explorer</h2>
              </div>
            </div>
          </div>
          {loading && <div className="alert alert-info kafdeck-status" role="status">Loading cluster observations…</div>}
          {error && <div className="alert alert-danger kafdeck-status" role="alert">{error}</div>}
          {!loading && !error && clusters.length === 0 && <div className="alert alert-warning kafdeck-status" role="status">No authorized clusters are available.</div>}
          {selected && <>
            <section className="card kafdeck-card" id="overview" aria-labelledby="overview-title">
              <h2 id="overview-title" className="card-title">Cluster overview</h2>
              <p className="kafdeck-observation"><strong>{selected.data.clusterId}</strong> · {observationText(selected)}</p>
              <dl><dt>Kafka cluster ID</dt><dd>{selected.data.kafkaClusterId ?? 'Not observed'}</dd><dt>Health</dt><dd><span className="badge bg-blue-lt">{String(selected.data.health)}</span></dd><dt>Controller</dt><dd>{selected.data.controllerBrokerId ?? 'Not observed'}</dd><dt>Brokers</dt><dd>{selected.data.brokers.length}</dd></dl>
              {selected.limitations.length > 0 && <aside aria-label="Cluster limitations"><h3 className="h4">Limitations</h3><ul className="mb-0">{selected.limitations.map((item, index) => <li key={`${item.capability}-${index}`}>{item.capability ?? 'cluster'}: {item.state}{item.reason ? ` — ${item.reason}` : ''}</li>)}</ul></aside>}
            </section>
            <section className="card kafdeck-card" id="brokers" aria-labelledby="brokers-title">
              <h2 id="brokers-title" className="card-title">Brokers</h2>
              {selected.data.brokers.length === 0 ? <p className="text-secondary">No broker metadata is currently observable.</p> : <div className="table-responsive"><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">ID</th><th scope="col">Host</th><th scope="col">Port</th><th scope="col">Role</th><th scope="col">Configuration</th></tr></thead><tbody>{selected.data.brokers.map(broker => <tr key={broker.brokerId}><th scope="row">{broker.brokerId}</th><td>{broker.host}</td><td>{broker.port}</td><td>{broker.isController ? <span className="badge bg-orange-lt">Controller</span> : <span className="badge bg-secondary-lt">Broker</span>}</td><td><button className="btn btn-sm btn-outline-primary" type="button" onClick={() => void openBroker(broker.brokerId)}>View configuration</button></td></tr>)}</tbody></table></div>}
              {brokerError && <div className="alert alert-danger" role="alert">{brokerError}</div>}
              {brokerConfiguration && <article aria-labelledby="broker-config-title"><h3 id="broker-config-title" className="h3">Broker {brokerConfiguration.brokerId} configuration</h3><ConfigurationView entries={brokerConfiguration.entries} /></article>}
            </section>
            <section className="card kafdeck-card" id="topics" aria-labelledby="topics-title">
              <h2 id="topics-title" className="card-title">Topics</h2>
              <div className="px-3 mb-3"><label className="form-label" htmlFor="topic-search">Search topics</label><input className="form-control" id="topic-search" type="search" value={topicQuery} onChange={event => setTopicQuery(event.target.value)} placeholder="Filter by topic name" /></div>
              {topicsLoading && topics.length === 0 && <div className="alert alert-info mx-3" role="status">Loading topics…</div>}
              {topicsError && <div className="alert alert-danger mx-3" role="alert">{topicsError}</div>}
              {!topicsLoading && !topicsError && topics.length === 0 && <p className="text-secondary">No topics match the current search.</p>}
              {topics.length > 0 && <div className="table-responsive"><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">Topic</th><th scope="col">Partitions</th><th scope="col">Offline</th><th scope="col">Under replicated</th><th scope="col">State</th></tr></thead><tbody>{topics.map(topic => <tr key={topic.name}><th scope="row"><button className="btn btn-link kafdeck-table-button" type="button" onClick={() => void openTopic(topic.name)}>{topic.name}</button></th><td>{topic.partitionCount}</td><td>{topic.offlinePartitionCount ?? 'Unknown'}</td><td>{topic.underReplicatedPartitionCount ?? 'Unknown'}</td><td><span className="badge bg-blue-lt">{String(topic.anomalyState)}</span></td></tr>)}</tbody></table></div>}
              {topicCursor && <button className="btn btn-sm btn-outline-primary mt-3" type="button" disabled={topicsLoading} onClick={() => void loadTopics(selectedClusterId, topicQuery, topicCursor, true)}>{topicsLoading ? 'Loading…' : 'Load more topics'}</button>}
              {topicDetailError && <div className="alert alert-danger mx-3" role="alert">{topicDetailError}</div>}
              {topicDetail && <article aria-labelledby="topic-detail-title"><h3 id="topic-detail-title" className="h3">Topic: {topicDetail.data.name}</h3><p className="kafdeck-observation">{observationText(topicDetail)} · {topicDetail.data.partitions.length} partitions · {topicDetail.data.offlinePartitionCount} offline · {topicDetail.data.underReplicatedPartitionCount} under replicated</p>{topicCatalog && <aside aria-label="Topic catalog metadata"><h4>Catalog metadata</h4><dl><dt>Description</dt><dd>{topicCatalog.data.description ?? 'Not set'}</dd><dt>Owner</dt><dd>{topicCatalog.data.owner ?? 'Not set'}</dd><dt>Domain</dt><dd>{topicCatalog.data.domain ?? 'Not set'}</dd><dt>Classification</dt><dd>{topicCatalog.data.classification ?? 'Not set'}</dd><dt>Tags</dt><dd>{topicCatalog.data.tags.join(', ') || 'None'}</dd><dt>Documentation reference</dt><dd>{topicCatalog.data.documentationReference ?? 'Not set'}</dd></dl></aside>}<h4>Partitions</h4>{topicDetail.data.partitions.length === 0 ? <p>No partitions are observable.</p> : <div className="table-responsive"><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">ID</th><th scope="col">Leader</th><th scope="col">Replicas</th><th scope="col">ISR</th><th scope="col">Out of sync</th><th scope="col">Health</th></tr></thead><tbody>{topicDetail.data.partitions.map(partition => <tr key={partition.partitionId}><th scope="row">{partition.partitionId}</th><td>{partition.leaderBrokerId ?? 'No leader'}</td><td>{partition.replicaBrokerIds.join(', ')}</td><td>{partition.inSyncReplicaBrokerIds.join(', ')}</td><td>{partition.outOfSyncReplicaBrokerIds.join(', ') || 'None'}</td><td>{partition.healthReasons.length > 0 ? partition.healthReasons.join('; ') : String(partition.health)}</td></tr>)}</tbody></table></div>}<h4>Configuration</h4>{topicConfiguration === null ? (topicDetailError ? null : <p role="status">Loading configuration…</p>) : <ConfigurationView entries={topicConfiguration} />}{topicDetail.data.partitions.length > 0 && <RecordExplorer clusterId={selectedClusterId} topicName={topicDetail.data.name} partitions={topicDetail.data.partitions.map(partition => partition.partitionId)} />}</article>}
            </section>
            <ReadViewsExplorer clusterId={selectedClusterId} />
          </>}
        </div>
      </div>
    </main>
    <img className="kafdeck-logo-watermark" src="/kafdeck-logo.webp" alt="" aria-hidden="true" />
  </div>;
}
