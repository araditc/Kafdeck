import { useEffect, useRef, useState } from 'react';
import { ApiProblem, kafdeckApi, type BrokerHostDraft, type BrokerHostRegistry, type BrokerServiceTarget } from '../shared/api.js';
import { t } from '../shared/i18n.js';

const empty: BrokerHostDraft = { name: '', address: '', port: 22, user: 'kafdeck-probe', fingerprint: '', credentialId: '' };

export function BrokerHostsPanel({ clusterId }: { clusterId: string }) {
  const [registry, setRegistry] = useState<BrokerHostRegistry | null>(null);
  const [draft, setDraft] = useState(empty);
  const [hostId, setHostId] = useState('');
  const [targets, setTargets] = useState<BrokerServiceTarget[]>([]);
  const [targetId, setTargetId] = useState('');
  const [brokers, setBrokers] = useState<number[]>([]);
  const [broker, setBroker] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const lifetime = useRef<AbortController | null>(null);
  const report = (e: unknown) => setError(e instanceof ApiProblem ? e.message : t('Host verification could not be completed.'));
  useEffect(() => {
    const control = new AbortController(); lifetime.current = control;
    void Promise.all([kafdeckApi.brokerHosts(control.signal), kafdeckApi.getCluster(clusterId, control.signal)]).then(([state, cluster]) => {
      if (control.signal.aborted) return;
      setRegistry(state); setBrokers(cluster.data.brokers.map(item => item.brokerId));
      setDraft(previous => ({ ...previous, credentialId: state.credentials[0]?.id ?? '' }));
    }).catch(e => { if (!control.signal.aborted) report(e); });
    return () => control.abort();
  }, [clusterId]);
  async function action(work: (signal: AbortSignal) => Promise<void>) {
    const signal = lifetime.current?.signal;
    if (!signal || signal.aborted || busy) return;
    setBusy(true); setError(''); setNotice('');
    try { await work(signal); }
    catch (e) { if (!signal.aborted) report(e); }
    finally { if (!signal.aborted) setBusy(false); }
  }
  const serviceName = (target: BrokerServiceTarget) => target.adapter === 'systemd' ? target.unit : `${target.project} / ${target.service}`;
  return <section className="card kafdeck-card p-3" aria-labelledby="broker-hosts-heading">
    <h2 id="broker-hosts-heading" className="h3">{t('Broker hosts')}</h2>
    <p className="text-secondary">{t('Register a trusted SSH host, then verify which running service belongs to each broker. This step does not change or restart Kafka.')}</p>
    {error && <div className="alert alert-danger" role="alert"><bdi dir="auto">{error}</bdi></div>}
    {notice && <div className="alert alert-success" role="status">{notice}</div>}
    {!registry && !error && <p role="status">{t('Loading…')}</p>}
    {registry && <>
      <details className="mb-3">
        <summary className="mb-3">{t('Register SSH host')}</summary>
        <form onSubmit={e => { e.preventDefault(); void action(async signal => {
          const result = await kafdeckApi.registerBrokerHost(registry.revision, draft, signal);
          const state = await kafdeckApi.brokerHosts(signal);
          if (signal.aborted) return;
          setRegistry(state); setHostId(result.host.id); setTargets(result.targets); setTargetId('');
          setDraft({ ...empty, credentialId: draft.credentialId }); setNotice(t('SSH host verified and registered.'));
        }); }}>
          <fieldset disabled={busy} className="border-0 p-0 m-0">
            <div className="row g-3 mb-3">
              <div className="col-md-6"><label className="form-label" htmlFor="host-name">{t('Host name')}</label><input id="host-name" className="form-control" required maxLength={100} value={draft.name} onChange={e => setDraft({ ...draft, name: e.target.value })} /></div>
              <div className="col-md-4"><label className="form-label" htmlFor="host-address">{t('Host IP address')}</label><input id="host-address" className="form-control" dir="ltr" required maxLength={45} value={draft.address} onChange={e => setDraft({ ...draft, address: e.target.value })} /></div>
              <div className="col-md-2"><label className="form-label" htmlFor="host-port">{t('SSH port')}</label><input id="host-port" className="form-control" dir="ltr" required type="number" min={1} max={65535} value={draft.port} onChange={e => setDraft({ ...draft, port: Number(e.target.value) })} /></div>
              <div className="col-md-6"><label className="form-label" htmlFor="host-user">{t('Dedicated SSH user')}</label><input id="host-user" className="form-control" dir="ltr" required maxLength={128} value={draft.user} onChange={e => setDraft({ ...draft, user: e.target.value })} /></div>
              <div className="col-md-6"><label className="form-label" htmlFor="host-credential">{t('Server-side credential')}</label><select id="host-credential" className="form-select" required value={draft.credentialId} onChange={e => setDraft({ ...draft, credentialId: e.target.value })}>{registry.credentials.map(item => <option key={item.id} value={item.id}>{item.id} ({item.authentication})</option>)}</select></div>
              <div className="col-12"><label className="form-label" htmlFor="host-fingerprint">{t('Trusted SSH fingerprint')}</label><input id="host-fingerprint" className="form-control" dir="ltr" required placeholder="SHA256:…" maxLength={50} value={draft.fingerprint} onChange={e => setDraft({ ...draft, fingerprint: e.target.value.trim() })} aria-describedby="host-pin-help" /><p id="host-pin-help" className="form-hint">{t('Obtain this SHA256 fingerprint through a trusted administrator or server console. An unverified scan is not proof of host identity.')}</p></div>
            </div>
            <button className="btn btn-primary" type="submit">{busy ? t('Verifying…') : t('Verify and register host')}</button>
          </fieldset>
        </form>
      </details>
      <div className="d-flex flex-wrap align-items-end gap-3 mb-3">
        <div className="flex-grow-1"><label className="form-label" htmlFor="binding-host">{t('Registered host')}</label><select id="binding-host" className="form-select" disabled={busy} value={hostId} onChange={e => { setHostId(e.target.value); setTargets([]); setTargetId(''); setNotice(''); }}><option value="">{t('Select a host')}</option>{registry.hosts.map(host => <option key={host.id} value={host.id}>{host.connection.name} ({host.connection.address})</option>)}</select></div>
        <button type="button" className="btn btn-outline-primary" disabled={busy || !hostId} onClick={() => void action(async signal => {
          setTargets([]); setTargetId('');
          const result = await kafdeckApi.probeBrokerHost(hostId, signal);
          if (!signal.aborted) setTargets(result.targets);
        })}>{busy ? t('Verifying…') : t('Discover approved services')}</button>
        <button type="button" className="btn btn-outline-secondary" disabled={busy} onClick={() => void action(async signal => { const state = await kafdeckApi.brokerHosts(signal); if (!signal.aborted) setRegistry(state); })}>{t('Reload host registry')}</button>
      </div>
      {targets.length > 0 && <form className="row g-3 mb-4" onSubmit={e => { e.preventDefault(); void action(async signal => {
        await kafdeckApi.bindBrokerHost(registry.revision, clusterId, Number(broker), hostId, targetId, signal);
        const state = await kafdeckApi.brokerHosts(signal);
        if (!signal.aborted) { setRegistry(state); setNotice(t('Broker and service identity match. Binding saved.')); }
      }); }}>
        <div className="col-md-7"><label className="form-label" htmlFor="binding-service">{t('Approved service')}</label><select id="binding-service" className="form-select" required disabled={busy} value={targetId} onChange={e => setTargetId(e.target.value)}><option value="">{t('Select a service')}</option>{targets.map(target => <option key={target.id} value={target.id} disabled={target.status !== 'verified'}>{serviceName(target)} · {target.status === 'verified' ? `${t('Broker')} ${target.brokerId}` : t('Identity unavailable')}</option>)}</select></div>
        <div className="col-md-5"><label className="form-label" htmlFor="binding-broker">{t('Broker in selected cluster')}</label><select id="binding-broker" className="form-select" required disabled={busy} value={broker} onChange={e => setBroker(e.target.value)}><option value="">{t('Select a broker')}</option>{brokers.map(id => <option key={id} value={id}>{id}</option>)}</select></div>
        <div><button type="submit" className="btn btn-primary" disabled={busy || !targetId || broker === ''}>{busy ? t('Verifying…') : t('Verify and bind broker')}</button></div>
      </form>}
      <h3 className="h4">{t('Verified broker bindings')}</h3>
      <p className="text-secondary">{t('Verification is a dated observation. Reverify the binding after service, host key or cluster changes.')}</p>
      <div className="table-responsive" tabIndex={0} role="region" aria-label={t('Verified broker bindings')}><table className="table table-vcenter"><thead><tr><th>{t('Broker')}</th><th>{t('Host name')}</th><th>{t('Approved service')}</th><th>{t('Last verified')}</th></tr></thead><tbody>{registry.bindings.filter(item => item.clusterId === clusterId).map(item => <tr key={item.brokerId}><th scope="row">{item.brokerId}</th><td><bdi>{registry.hosts.find(host => host.id === item.hostId)?.connection.name}</bdi></td><td><bdi>{item.targetId}</bdi></td><td><time dateTime={item.verifiedAtUtc}>{new Date(item.verifiedAtUtc).toLocaleString()}</time></td></tr>)}</tbody></table></div>
      {!registry.bindings.some(item => item.clusterId === clusterId) && <p>{t('No verified host bindings for this cluster.')}</p>}
    </>}
  </section>;
}
