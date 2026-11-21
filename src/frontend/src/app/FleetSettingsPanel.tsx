import { useEffect, useRef, useState } from 'react';
import { ApiProblem, kafdeckApi, type FleetCredential, type FleetSettings, type ManagedUser } from '../shared/api.js';
import { t } from '../shared/i18n.js';

export function FleetSettingsPanel({onSaved}: {onSaved: () => Promise<void>}) {
  const [expanded, setExpanded] = useState(false);
  const [settings, setSettings] = useState<FleetSettings | null>(null);
  const [users, setUsers] = useState<ManagedUser[]>([]);
  const [networks, setNetworks] = useState('');
  const [tab, setTab] = useState<'settings' | 'credentials'>('settings');
  const [credentialId, setCredentialId] = useState('');
  const [authentication, setAuthentication] = useState<'privateKey' | 'password'>('privateKey');
  const [purpose, setPurpose] = useState<'probe' | 'agent'>('probe');
  const [secret, setSecret] = useState('');
  const [passphrase, setPassphrase] = useState('');
  const [revoke, setRevoke] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const lifetime = useRef<AbortController | null>(null);
  useEffect(() => {
    const control = new AbortController(); lifetime.current = control;
    void Promise.all([kafdeckApi.fleetSettings(control.signal), kafdeckApi.listManagedUsers(control.signal)])
      .then(([value, people]) => {
        if (control.signal.aborted) return;
        setSettings(value); setExpanded(!value.configuration.enabled); setNetworks(value.configuration.allowedNetworks.join('\n')); setUsers(people.users);
      }).catch(failure => { if (!control.signal.aborted) setError(failure instanceof ApiProblem ? failure.message : t('Fleet settings could not be loaded.')); });
    return () => control.abort();
  }, []);
  useEffect(() => {
    const changed = () => {
      const signal = lifetime.current?.signal;
      if (!signal || signal.aborted) return;
      void kafdeckApi.fleetSettings(signal).then(value => {
        if (!signal.aborted) { setSettings(value); setNetworks(value.configuration.allowedNetworks.join('\n')); }
      }).catch(failure => { if (!signal.aborted) setError(failure instanceof ApiProblem ? failure.message : t('Fleet settings could not be loaded.')); });
    };
    window.addEventListener('kafdeck:fleet-settings-changed', changed);
    return () => window.removeEventListener('kafdeck:fleet-settings-changed', changed);
  }, []);
  async function run(work: (signal: AbortSignal) => Promise<FleetSettings>) {
    const signal = lifetime.current?.signal;
    if (!signal || signal.aborted || busy) return;
    setBusy(true); setError(''); setNotice('');
    try {
      const value = await work(signal);
      if (signal.aborted) return;
      setSettings(value); setSecret(''); setPassphrase(''); setRevoke('');
      setNotice(t('Fleet settings saved.')); await onSaved();
    } catch (failure) {
      if (!signal.aborted) setError(failure instanceof ApiProblem ? failure.message : t('Fleet settings could not be saved.'));
    } finally { if (!signal.aborted) setBusy(false); }
  }
  function selectCredential(credential: FleetCredential) {
    setCredentialId(credential.id); setAuthentication(credential.authentication); setPurpose(credential.purpose);
    setSecret(''); setPassphrase(''); setNotice('');
  }
  return <details className="mb-4" open={expanded} onToggle={event => setExpanded(event.currentTarget.open)}>
    <summary className="mb-3">{t('Fleet settings and SSH credentials')}</summary>
    {error && <div className="alert alert-danger" role="alert">{error}</div>}
    {notice && <div className="alert alert-success" role="status">{notice}</div>}
    {!settings && !error && <p role="status">{t('Loading…')}</p>}
    {settings && <>
      <div className="d-flex flex-wrap gap-2 mb-3" role="group" aria-label={t('Fleet settings')}>
        <button className={'btn ' + (tab === 'settings' ? 'btn-primary' : 'btn-outline-primary')} type="button" aria-pressed={tab === 'settings'} onClick={() => setTab('settings')}>{t('Access and networks')}</button>
        <button className={'btn ' + (tab === 'credentials' ? 'btn-primary' : 'btn-outline-primary')} type="button" aria-pressed={tab === 'credentials'} onClick={() => setTab('credentials')}>{t('SSH credentials')}</button>
      </div>
      {tab === 'settings' && <form onSubmit={event => {
        event.preventDefault(); void run(signal => kafdeckApi.saveFleetConfiguration(settings.revision,
          {...settings.configuration, allowedNetworks: networks.split(/[\s,]+/).filter(Boolean)}, signal));
      }}>
        <fieldset className="border-0 p-0 m-0" disabled={busy}>
          <label className="form-check mb-3"><input type="checkbox" className="form-check-input" checked={settings.configuration.enabled}
            onChange={event => setSettings({...settings, configuration: {...settings.configuration, enabled: event.target.checked}})} />{t('Enable Fleet host management')}</label>
          <div className="mb-3"><label className="form-label" htmlFor="fleet-networks">{t('Allowed SSH host networks')}</label>
            <textarea id="fleet-networks" className="form-control" dir="ltr" rows={3} required value={networks} placeholder="192.168.100.33/32"
              onChange={event => setNetworks(event.target.value)} aria-describedby="fleet-networks-help" />
            <p id="fleet-networks-help" className="form-hint">{t('Enter one CIDR per line. Use /32 for a single IPv4 host.')}</p></div>
          <div className="row g-3 mb-3">
            {(['logReaderUserIds', 'agentInstallerUserIds'] as const).map(field => <div className="col-md-6" key={field}>
              <fieldset><legend className="h4">{field === 'logReaderUserIds' ? t('Operators allowed to read logs') : t('Operators allowed to install agents')}</legend>
                {users.map(user => <label className="form-check" key={user.id}><input type="checkbox" className="form-check-input"
                  checked={settings.configuration[field].includes(user.id)} onChange={event => setSettings({...settings, configuration: {...settings.configuration,
                    [field]: event.target.checked ? [...settings.configuration[field], user.id] : settings.configuration[field].filter(id => id !== user.id)}})} />
                  <bdi>{user.email}</bdi></label>)}</fieldset></div>)}
          </div>
          <button className="btn btn-primary" type="submit">{busy ? t('Saving…') : t('Save Fleet settings')}</button>
        </fieldset>
      </form>}
      {tab === 'credentials' && <>
        <form className="mb-4" onSubmit={event => {
          event.preventDefault(); void run(signal => kafdeckApi.saveFleetCredential({expectedRevision: settings.revision,
            id: credentialId, authentication, secret, passphrase: passphrase || undefined, purpose}, signal));
        }}>
          <fieldset className="border-0 p-0 m-0" disabled={busy}>
            <div className="row g-3 mb-3">
              <div className="col-md-4"><label className="form-label" htmlFor="fleet-credential-id">{t('Credential name')}</label>
                <input id="fleet-credential-id" className="form-control" dir="ltr" required maxLength={128} pattern="[A-Za-z0-9][A-Za-z0-9_.@-]*" value={credentialId} onChange={event => setCredentialId(event.target.value)} /></div>
              <div className="col-md-4"><label className="form-label" htmlFor="fleet-credential-method">{t('Authentication')}</label>
                <select id="fleet-credential-method" className="form-select" value={authentication} onChange={event => setAuthentication(event.target.value as typeof authentication)}>
                  <option value="privateKey">{t('Private key')}</option><option value="password">{t('Password')}</option></select></div>
              <div className="col-md-4"><label className="form-label" htmlFor="fleet-credential-purpose">{t('Credential purpose')}</label>
                <select id="fleet-credential-purpose" className="form-select" value={purpose} onChange={event => setPurpose(event.target.value as typeof purpose)}>
                  <option value="probe">{t('Host discovery and logs')}</option><option value="agent">{t('Agent lifecycle')}</option></select></div>
              <div className="col-12"><label className="form-label" htmlFor="fleet-credential-secret">{authentication === 'privateKey' ? t('Private key') : t('Password')}</label>
                {authentication === 'privateKey' ? <textarea id="fleet-credential-secret" className="form-control font-monospace" dir="ltr" rows={5} required maxLength={65536} autoComplete="off" spellCheck={false} value={secret} onChange={event => setSecret(event.target.value)} /> :
                  <input id="fleet-credential-secret" className="form-control" type="password" autoComplete="new-password" required maxLength={65536} value={secret} onChange={event => setSecret(event.target.value)} />}</div>
              {authentication === 'privateKey' && <div className="col-md-6"><label className="form-label" htmlFor="fleet-key-passphrase">{t('Key passphrase')}</label>
                <input id="fleet-key-passphrase" className="form-control" type="password" autoComplete="new-password" maxLength={4096} value={passphrase} onChange={event => setPassphrase(event.target.value)} /></div>}
            </div>
            <p className="form-hint">{t('Credentials are encrypted before storage. Enter a replacement to rotate an existing credential.')}</p>
            <button className="btn btn-primary" type="submit">{busy ? t('Saving…') : t('Save encrypted credential')}</button>
          </fieldset>
        </form>
        <ul className="list-unstyled">{settings.credentials.map(credential => <li className="d-flex flex-wrap align-items-center gap-2 mb-3" key={credential.id}>
          <bdi>{credential.id}</bdi><span>{credential.revoked ? t('Revoked') : credential.stored ? t('Credential stored') : t('Configured reference')}</span>
          <button type="button" className="btn btn-sm btn-outline-primary" disabled={busy} onClick={() => selectCredential(credential)}>{t('Replace credential')}</button>
          {!credential.revoked && <button type="button" className="btn btn-sm btn-outline-danger" disabled={busy} onClick={() => setRevoke(credential.id)}>{t('Revoke credential')}</button>}
          {revoke === credential.id && <><span role="alert">{t('Revoke access for hosts using this credential?')}</span>
            <button type="button" className="btn btn-sm btn-danger" disabled={busy} onClick={() => void run(signal => kafdeckApi.saveFleetCredential({
              expectedRevision: settings.revision, id: credential.id, authentication: credential.authentication, purpose: credential.purpose, revoke: true}, signal))}>{t('Confirm revocation')}</button>
            <button type="button" className="btn btn-sm btn-outline-secondary" disabled={busy} onClick={() => setRevoke('')}>{t('Cancel')}</button></>}
        </li>)}</ul>
      </>}
    </>}
  </details>;
}
