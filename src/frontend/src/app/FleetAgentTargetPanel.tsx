import { useEffect, useRef, useState } from 'react';
import { ApiProblem, kafdeckApi, type BrokerHostRegistry, type FleetSettings } from '../shared/api.js';
import { t } from '../shared/i18n.js';

export function FleetAgentTargetPanel({clusterId, onSaved}: {clusterId: string; onSaved: () => Promise<void>}) {
  const lifetime = useRef<AbortController | null>(null);
  const [settings, setSettings] = useState<FleetSettings | null>(null);
  const [registry, setRegistry] = useState<BrokerHostRegistry | null>(null);
  const [sources, setSources] = useState<string[]>([]);
  const [expected, setExpected] = useState(0);
  const [brokerId, setBrokerId] = useState('');
  const [credentialId, setCredentialId] = useState('');
  const [instance, setInstance] = useState('');
  const [revision, setRevision] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  useEffect(() => {
    const changed = () => setRevision(value => value + 1);
    window.addEventListener('kafdeck:fleet-settings-changed', changed);
    return () => window.removeEventListener('kafdeck:fleet-settings-changed', changed);
  }, []);
  useEffect(() => {
    const abort = new AbortController(); lifetime.current = abort;
    void Promise.all([kafdeckApi.fleetSettings(abort.signal), kafdeckApi.brokerHosts(abort.signal),
      kafdeckApi.monitoringSources(clusterId, abort.signal), kafdeckApi.getCluster(clusterId, abort.signal)])
      .then(([value, hosts, metrics, cluster]) => {
        if (abort.signal.aborted) return;
        setSettings(value); setRegistry(hosts); setExpected(cluster.data.brokers.length);
        setSources(metrics.state === 'available' ? metrics.rows.filter(row => row.instance !== 'kafdeck-metadata').map(row => row.instance) : []);
        const selected = value.configuration.agentTargets.find(target => target.clusterId === clusterId);
        setBrokerId(selected ? String(selected.brokerId) : ''); setCredentialId(selected?.credentialId ?? '');
        setInstance(selected?.monitoringInstance ?? '');
      }).catch(failure => { if (!abort.signal.aborted) setError(failure instanceof ApiProblem ? failure.message : t('Agent target settings could not be loaded.')); });
    return () => abort.abort();
  }, [clusterId, revision]);
  if (!settings || !registry) return error ? <p role="alert">{error}</p> : null;
  const credentials = settings.credentials.filter(value => !value.revoked && value.purpose === 'agent' && value.authentication === 'privateKey');
  return <details className="mb-4">
    <summary className="mb-3">{t('Select the broker for the next agent change')}</summary>
    {error && <div className="alert alert-danger" role="alert">{error}</div>}
    {notice && <div className="alert alert-success" role="status">{notice}</div>}
    <form onSubmit={event => {
      event.preventDefault(); if (busy || !lifetime.current || lifetime.current.signal.aborted) return;
      const signal = lifetime.current.signal;
      setBusy(true); setError(''); setNotice('');
      const configuration = {...settings.configuration, agentTargets: [
        ...settings.configuration.agentTargets.filter(target => target.clusterId !== clusterId),
        {clusterId, brokerId: Number(brokerId), monitoringInstance: instance, credentialId, expectedBrokers: expected},
      ]};
      void kafdeckApi.saveFleetConfiguration(settings.revision, configuration, signal).then(async value => {
        if (signal.aborted) return;
        setSettings(value); setNotice(t('Agent target saved. Continue with the agent preview.')); await onSaved();
      }).catch(failure => { if (!signal.aborted) setError(failure instanceof ApiProblem ? failure.message : t('Agent target settings could not be saved.')); })
        .finally(() => { if (!signal.aborted) setBusy(false); });
    }}>
      <fieldset className="border-0 p-0 m-0" disabled={busy}>
        <div className="row g-3 mb-3">
          <div className="col-md-4"><label className="form-label" htmlFor="fleet-agent-broker">{t('Broker')}</label>
            <select id="fleet-agent-broker" className="form-select" required value={brokerId} onChange={event => {
              const value = event.target.value; setBrokerId(value); setNotice('');
              const binding = registry.bindings.find(item => item.clusterId === clusterId && item.brokerId === Number(value));
              const credential = registry.agentCredentials?.find(item => item.hostId === binding?.hostId)?.credentialId;
              if (credential) setCredentialId(credential);
              setInstance(settings.configuration.logSources.find(item => item.clusterId === clusterId && item.brokerId === Number(value))?.monitoringInstance ?? '');
            }}><option value="">{t('Select a broker')}</option>{registry.bindings.filter(item => item.clusterId === clusterId).map(binding =>
              <option key={binding.brokerId} value={binding.brokerId}>{binding.brokerId}</option>)}</select></div>
          <div className="col-md-4"><label className="form-label" htmlFor="fleet-agent-credential">{t('Agent lifecycle credential')}</label>
            <select id="fleet-agent-credential" className="form-select" required value={credentialId} onChange={event => setCredentialId(event.target.value)}>
              <option value="">{t('Select a credential')}</option>{credentials.map(credential => <option key={credential.id}>{credential.id}</option>)}</select></div>
          <div className="col-md-4"><label className="form-label" htmlFor="fleet-agent-source">{t('Monitoring source')}</label>
            <select id="fleet-agent-source" className="form-select" required value={instance} onChange={event => setInstance(event.target.value)}>
              <option value="">{t('Select a monitoring source')}</option>{sources.map(source => <option key={source}>{source}</option>)}</select></div>
        </div>
        <p>{t('Expected brokers: {0}', [expected])}</p>
        <button type="submit" className="btn btn-primary" disabled={!brokerId || !credentialId || !instance || expected === 0}>{busy ? t('Saving…') : t('Save agent target')}</button>
      </fieldset>
    </form>
  </details>;
}
