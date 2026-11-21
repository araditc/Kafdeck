import { useEffect, useRef, useState } from 'react';
import { ApiProblem, kafdeckApi, type FleetBootstrapConnection, type FleetBootstrapInspection,
  type FleetBootstrapOperation, type FleetBootstrapProgress } from '../shared/api.js';
import { t } from '../shared/i18n.js';

const connectionDefaults: FleetBootstrapConnection = {address: '', port: 22, user: '', fingerprint: '',
  authentication: 'password', secret: '', passphrase: '', sudoPassword: ''};
const utcInput = (milliseconds: number) => new Date(milliseconds).toISOString().slice(0, 16);

export function FleetBootstrapPanel({clusterId, onSaved}: {clusterId: string; onSaved: () => Promise<void>}) {
  const [name, setName] = useState('');
  const [connection, setConnection] = useState(connectionDefaults);
  const [inspection, setInspection] = useState<FleetBootstrapInspection | null>(null);
  const [selected, setSelected] = useState<string[]>([]);
  const [endpoint, setEndpoint] = useState(() => typeof window === 'undefined' ? '' : window.location.origin + '/monitoring/ingest');
  const [ca, setCa] = useState('');
  const [interval, setIntervalSeconds] = useState(30);
  const [from, setFrom] = useState(() => utcInput(Date.now() + 5 * 60000));
  const [to, setTo] = useState(() => utcInput(Date.now() + 65 * 60000));
  const [confirm, setConfirm] = useState(false);
  const [operations, setOperations] = useState<FleetBootstrapOperation[]>([]);
  const [progress, setProgress] = useState<FleetBootstrapProgress | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const control = useRef<AbortController | null>(null);
  useEffect(() => {
    const abort = new AbortController(); control.current = abort;
    void kafdeckApi.fleetBootstrapOperations(abort.signal).then(value => {
      if (!abort.signal.aborted) setOperations(value.operations.filter(item => item.clusterId === clusterId));
    }).catch(failure => { if (!abort.signal.aborted) setError(failure instanceof ApiProblem ? failure.message : t('Setup operations could not be loaded.')); });
    return () => abort.abort();
  }, [clusterId]);
  async function run(work: (signal: AbortSignal) => Promise<void>) {
    const signal = control.current?.signal;
    if (!signal || signal.aborted || busy) return;
    setBusy(true); setError('');
    try { await work(signal); }
    catch (failure) { if (!signal.aborted) setError(failure instanceof ApiProblem ? failure.message : t('Host setup could not be completed.')); }
    finally { if (!signal.aborted) setBusy(false); }
  }
  async function finished(value: FleetBootstrapProgress, signal: AbortSignal) {
    if (signal.aborted) return;
    setProgress(value); setConfirm(false);
    setConnection(previous => ({...previous, secret: '', passphrase: '', sudoPassword: ''}));
    if (value.stage === 'complete') await onSaved();
    const latest = await kafdeckApi.fleetBootstrapOperations(signal);
    if (!signal.aborted) setOperations(latest.operations.filter(item => item.clusterId === clusterId));
  }
  const title = (target: FleetBootstrapInspection['targets'][number]) => target.adapter === 'systemd' ? target.unit : target.project + ' / ' + target.service;
  return <details className="mb-4">
    <summary className="mb-3">{t('Prepare a Linux host from the dashboard')}</summary>
    <p>{t('Inspect the host with temporary administrative access. Review the services before creating restricted accounts and preparing the agent.')}</p>
    {error && <div className="alert alert-danger" role="alert">{error}</div>}
    <form className="mb-4" onSubmit={event => { event.preventDefault(); void run(async signal => {
      setProgress(null); setInspection(null); setSelected([]); setConfirm(false);
      const result = await kafdeckApi.inspectFleetHost({clusterId, name, connection}, signal);
      if (!signal.aborted) setInspection(result);
    }); }}>
      <fieldset className="border-0 p-0 m-0" disabled={busy}>
        <div className="row g-3 mb-3">
          <div className="col-md-6"><label className="form-label" htmlFor="setup-host-name">{t('Host name')}</label><input id="setup-host-name" className="form-control" required maxLength={100} value={name} onChange={event => {setName(event.target.value); setInspection(null);}} /></div>
          <div className="col-md-4"><label className="form-label" htmlFor="setup-host-address">{t('Host IP address')}</label><input id="setup-host-address" className="form-control" dir="ltr" required maxLength={45} value={connection.address} onChange={event => {setConnection({...connection, address: event.target.value}); setInspection(null);}} /></div>
          <div className="col-md-2"><label className="form-label" htmlFor="setup-host-port">{t('SSH port')}</label><input id="setup-host-port" className="form-control" type="number" min={1} max={65535} required value={connection.port} onChange={event => {setConnection({...connection, port: Number(event.target.value)}); setInspection(null);}} /></div>
          <div className="col-md-6"><label className="form-label" htmlFor="setup-host-user">{t('Temporary administrative SSH user')}</label><input id="setup-host-user" className="form-control" dir="ltr" required maxLength={128} value={connection.user} onChange={event => {setConnection({...connection, user: event.target.value}); setInspection(null);}} /></div>
          <div className="col-md-6"><label className="form-label" htmlFor="setup-host-auth">{t('Authentication')}</label><select id="setup-host-auth" className="form-select" value={connection.authentication} onChange={event => {setConnection({...connection, authentication: event.target.value as FleetBootstrapConnection['authentication'], secret: '', passphrase: ''}); setInspection(null);}}>
            <option value="password">{t('Password')}</option><option value="privateKey">{t('Private key')}</option></select></div>
          <div className="col-12"><label className="form-label" htmlFor="setup-host-fingerprint">{t('Trusted SSH fingerprint')}</label><input id="setup-host-fingerprint" className="form-control" dir="ltr" required maxLength={50} value={connection.fingerprint} placeholder="SHA256:…" onChange={event => {setConnection({...connection, fingerprint: event.target.value.trim()}); setInspection(null);}} />
            <p className="form-hint">{t('Confirm this fingerprint using a trusted host console or administrator.')}</p></div>
          <div className="col-12"><label className="form-label" htmlFor="setup-host-secret">{connection.authentication === 'password' ? t('SSH password') : t('Private key')}</label>
            {connection.authentication === 'password' ? <input id="setup-host-secret" type="password" className="form-control" autoComplete="new-password" required value={connection.secret} onChange={event => setConnection({...connection, secret: event.target.value})} /> :
              <textarea id="setup-host-secret" className="form-control font-monospace" dir="ltr" rows={5} autoComplete="off" spellCheck={false} required maxLength={65536} value={connection.secret} onChange={event => setConnection({...connection, secret: event.target.value})} />}</div>
          {connection.authentication === 'privateKey' && <div className="col-md-6"><label className="form-label" htmlFor="setup-host-passphrase">{t('Key passphrase')}</label><input id="setup-host-passphrase" type="password" className="form-control" autoComplete="new-password" value={connection.passphrase} onChange={event => setConnection({...connection, passphrase: event.target.value})} /></div>}
          {connection.user !== 'root' && <div className="col-md-6"><label className="form-label" htmlFor="setup-host-sudo">{t('Sudo password')}</label><input id="setup-host-sudo" type="password" className="form-control" autoComplete="new-password" value={connection.sudoPassword} onChange={event => setConnection({...connection, sudoPassword: event.target.value})} /><p className="form-hint">{t('Leave empty when this account has passwordless sudo.')}</p></div>}
        </div>
        <p className="form-hint">{t('Temporary administrative credentials stay in this form until preparation. They are never saved by Kafdeck and are cleared after preparation.')}</p>
        <button className="btn btn-outline-primary" disabled={busy}>{busy ? t('Inspecting host…') : t('Test SSH and inspect services')}</button>
      </fieldset>
    </form>
    {inspection && <form className="mb-4" onSubmit={event => {event.preventDefault(); void run(async signal => {
      const value = await kafdeckApi.prepareFleetHost({preview: inspection.preview, targetIds: selected, connection,
        endpoint, caPem: ca || null, intervalSeconds: interval, maintenanceFrom: Date.parse(from + 'Z') / 1000,
        maintenanceTo: Date.parse(to + 'Z') / 1000, confirmPreparation: confirm}, signal);
      await finished(value, signal);
    });}}>
      <fieldset className="border-0 p-0 m-0" disabled={busy}>
        <legend className="h4">{t('Services verified against the selected Kafka cluster')}</legend>
        <div className="mb-3">{inspection.targets.map(target => <label key={target.id} className="form-check mb-2"><input className="form-check-input" type="checkbox" checked={selected.includes(target.id)} onChange={event => setSelected(event.target.checked ? [...selected, target.id] : selected.filter(id => id !== target.id))} />
          <bdi>{title(target)}</bdi> · {t('Broker')} {target.brokerId} · <bdi>{target.adapter}</bdi></label>)}</div>
        <div className="row g-3 mb-3">
          <div className="col-md-9"><label className="form-label" htmlFor="setup-agent-endpoint">{t('Kafdeck ingestion URL reachable from brokers')}</label><input id="setup-agent-endpoint" type="url" className="form-control" dir="ltr" required value={endpoint} onChange={event => setEndpoint(event.target.value)} /></div>
          <div className="col-md-3"><label className="form-label" htmlFor="setup-agent-interval">{t('Sampling interval')}</label><select id="setup-agent-interval" className="form-select" value={interval} onChange={event => setIntervalSeconds(Number(event.target.value))}>{[10,30,60].map(seconds => <option key={seconds} value={seconds}>{t('{0} seconds', [seconds])}</option>)}</select></div>
          <div className="col-md-6"><label className="form-label" htmlFor="setup-window-from">{t('Maintenance starts (UTC)')}</label><input id="setup-window-from" className="form-control" type="datetime-local" required value={from} onChange={event => setFrom(event.target.value)} /></div>
          <div className="col-md-6"><label className="form-label" htmlFor="setup-window-to">{t('Maintenance ends (UTC)')}</label><input id="setup-window-to" className="form-control" type="datetime-local" required value={to} onChange={event => setTo(event.target.value)} /></div>
          <div className="col-12"><label className="form-label" htmlFor="setup-agent-ca">{t('Kafdeck public CA or certificate (PEM)')}</label><textarea id="setup-agent-ca" className="form-control font-monospace" rows={4} dir="ltr" maxLength={16384} spellCheck={false} value={ca} onChange={event => setCa(event.target.value)} /><button type="button" className="btn btn-outline-secondary mt-2" disabled={busy} onClick={() => void run(async signal => {const value = await kafdeckApi.fleetPublicCertificate(signal); if (!signal.aborted) {if (value.pem) setCa(value.pem); else setError(t('No local HTTPS certificate is configured. Supply your public CA certificate.'));}})}>{t('Use this Kafdeck server certificate')}</button><p className="form-hint">{t('Supply the public certificate for a private or self-signed Kafdeck HTTPS endpoint.')}</p></div>
        </div>
        <dl><dt>{t('Agent package')}</dt><dd><bdi>{inspection.package.version}</bdi></dd><dt>{t('Package SHA256')}</dt><dd className="text-break"><bdi>{inspection.package.digest}</bdi></dd></dl>
        <p>{t('Preparation creates restricted SSH accounts, stages the bundled agent and prepares launch settings. The agent change has a separate Kafka health check and restart confirmation.')}</p>
        <label className="form-check mb-3"><input type="checkbox" className="form-check-input" required checked={confirm} onChange={event => setConfirm(event.target.checked)} />{t('Confirm preparation of the selected services')}</label>
        <button className="btn btn-primary" disabled={busy || !selected.length || !confirm}>{busy ? t('Preparing host…') : t('Prepare host and save encrypted access')}</button>
      </fieldset>
    </form>}
    {progress && <div className={'alert mt-3 ' + (progress.stage === 'complete' ? 'alert-success' : 'alert-warning')} role="status">
      {progress.stage === 'complete' ? t('Host prepared. Continue with the offline broker agent preview below.') : progress.stage === 'failed' ? t('The unused setup reservation was cleared. Inspect the host to start a new preparation.') : t('Preparation has an uncertain outcome. Check the same operation before retrying.')}
      <p className="mb-0"><bdi>{progress.id}</bdi></p>
    </div>}
    {operations.some(item => !['complete', 'failed'].includes(item.stage)) && <div className="mt-3"><h3 className="h4">{t('Pending host setup operations')}</h3>
      <label className="form-check mb-3"><input type="checkbox" className="form-check-input" checked={confirm} onChange={event => setConfirm(event.target.checked)} />{t('Confirm resuming the same preparation with temporary administrative access')}</label>
      <ul className="list-unstyled">{operations.filter(item => !['complete', 'failed'].includes(item.stage)).map(operation => <li key={operation.id} className="mb-3">
        <p><bdi>{operation.name} ({operation.address})</bdi> · <bdi>{operation.id}</bdi></p>
        <div className="d-flex flex-wrap gap-2">
          <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={() => void run(async signal => finished(await kafdeckApi.resumeFleetBootstrap(operation.id, null, false, signal), signal))}>{t('Check setup operation')}</button>
          <button type="button" className="btn btn-outline-secondary" disabled={busy || !confirm || !connection.secret || connection.address !== operation.address}
            onClick={() => void run(async signal => finished(await kafdeckApi.resumeFleetBootstrap(operation.id, connection, confirm, signal), signal))}>{t('Resume the same preparation with temporary access')}</button>
        </div>
      </li>)}</ul></div>}
  </details>;
}
