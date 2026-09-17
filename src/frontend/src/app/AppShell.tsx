import { useCallback, useEffect, useState } from 'react';
import { kafdeckApi, type ApiEnvelope, type ClusterData } from '../shared/api.js';
import { productDescription, productName } from '../shared/product.js';
import { describeObservation } from './operatorState.js';

function observationText(envelope: ApiEnvelope<unknown>) {
  const observed = new Date(envelope.observation.observedAt);
  return `${describeObservation(envelope.observation)} · observed ${observed.toLocaleString()}`;
}

export function AppShell() {
  const [clusters, setClusters] = useState<ApiEnvelope<ClusterData>[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState<string>('');
  const [selected, setSelected] = useState<ApiEnvelope<ClusterData> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
      setError(reason instanceof Error ? reason.message : 'Unable to load Kafdeck clusters.');
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [selectedClusterId]);

  useEffect(() => {
    const controller = new AbortController();
    void loadClusters(controller.signal);
    return () => controller.abort();
  }, [loadClusters]);

  const selectCluster = (clusterId: string) => {
    setSelectedClusterId(clusterId);
    setSelected(clusters.find(item => item.data.clusterId === clusterId) ?? null);
    setError(null);
  };

  return (
    <main aria-labelledby="kafdeck-title">
      <header>
        <div>
          <h1 id="kafdeck-title">{productName}</h1>
          <p>{productDescription}</p>
        </div>
        <div>
          <label htmlFor="cluster-selector">Cluster</label>{' '}
          <select
            id="cluster-selector"
            value={selectedClusterId}
            onChange={event => selectCluster(event.target.value)}
            disabled={loading || clusters.length === 0}
          >
            {clusters.map(cluster => (
              <option key={cluster.data.clusterId} value={cluster.data.clusterId}>{cluster.data.clusterId}</option>
            ))}
          </select>{' '}
          <button type="button" onClick={() => void loadClusters()} disabled={loading}>Refresh</button>
        </div>
      </header>

      <nav aria-label="Kafdeck sections">
        <a href="#overview">Overview</a>{' · '}
        <a href="#brokers">Brokers</a>{' · '}
        <a href="#topics">Topics</a>
      </nav>

      {loading && <p role="status">Loading cluster observations…</p>}
      {error && <p role="alert">{error}</p>}
      {!loading && !error && clusters.length === 0 && <p role="status">No clusters are configured.</p>}

      {selected && (
        <>
          <section id="overview" aria-labelledby="overview-title">
            <h2 id="overview-title">Cluster overview</h2>
            <p><strong>{selected.data.clusterId}</strong> · {observationText(selected)}</p>
            <dl>
              <dt>Kafka cluster ID</dt><dd>{selected.data.kafkaClusterId ?? 'Not observed'}</dd>
              <dt>Health</dt><dd>{String(selected.data.health)}</dd>
              <dt>Controller</dt><dd>{selected.data.controllerBrokerId ?? 'Not observed'}</dd>
              <dt>Brokers</dt><dd>{selected.data.brokers.length}</dd>
            </dl>
            {selected.limitations.length > 0 && (
              <aside aria-label="Cluster limitations">
                <h3>Limitations</h3>
                <ul>{selected.limitations.map((item, index) => <li key={`${item.capability}-${index}`}>{item.capability ?? 'cluster'}: {item.state}{item.reason ? ` — ${item.reason}` : ''}</li>)}</ul>
              </aside>
            )}
          </section>

          <section id="brokers" aria-labelledby="brokers-title">
            <h2 id="brokers-title">Brokers</h2>
            {selected.data.brokers.length === 0 ? <p>No broker metadata is currently observable.</p> : (
              <table>
                <thead><tr><th scope="col">ID</th><th scope="col">Host</th><th scope="col">Port</th><th scope="col">Role</th></tr></thead>
                <tbody>{selected.data.brokers.map(broker => (
                  <tr key={broker.brokerId}><th scope="row">{broker.brokerId}</th><td>{broker.host}</td><td>{broker.port}</td><td>{broker.isController ? 'Controller' : 'Broker'}</td></tr>
                ))}</tbody>
              </table>
            )}
          </section>

          <section id="topics" aria-labelledby="topics-title">
            <h2 id="topics-title">Topics</h2>
            <p>Topic search, pagination, partition health and configuration views use the read-only v0.1 API and are the next W09 slice.</p>
          </section>
        </>
      )}
    </main>
  );
}
