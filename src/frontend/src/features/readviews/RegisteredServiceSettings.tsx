import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { ApiProblem, kafdeckApi, type RegisteredService, type RegisteredServiceSettingsData, type ServiceAuthentication } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';

export function RegisteredServiceSettings({ clusterId, service, onSaved, onClose }: {
  clusterId: string;
  service: RegisteredService;
  onSaved: () => void;
  onClose: () => void;
}) {
  const registry = service === 'schema-registry';
  const ksql = service === 'ksqldb';
  const field = useId();
  const [current, setCurrent] = useState<RegisteredServiceSettingsData | null>(null);
  const [url, setUrl] = useState('');
  const [authentication, setAuthentication] = useState<ServiceAuthentication>('none');
  const [username, setUsername] = useState('');
  const [secret, setSecret] = useState('');
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const request = useRef<AbortController | null>(null);
  const heading = useRef<HTMLHeadingElement>(null);

  useEffect(() => {
    heading.current?.focus();
    const controller = new AbortController();
    request.current = controller;
    void kafdeckApi.getRegisteredServiceSettings(clusterId, service, controller.signal).then(settings => {
      if (controller.signal.aborted) return;
      setCurrent(settings);
      setUrl(settings.url ?? '');
      setAuthentication(settings.authentication);
    }).catch(() => {
      if (!controller.signal.aborted) setError(ksql ? t("ksqlDB settings could not be loaded.")
        : registry ? t("Schema Registry settings could not be loaded.") : t("Kafka Connect settings could not be loaded."));
    }).finally(() => {
      if (request.current === controller) request.current = null;
    });
    return () => {
      request.current?.abort();
      request.current = null;
    };
  }, [clusterId, service, registry, ksql]);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!current || request.current || !url.trim()) return;
    const save = (event.nativeEvent as SubmitEvent).submitter?.getAttribute('value') === 'save';
    const controller = new AbortController();
    request.current = controller;
    setBusy(true);
    setError(null);
    setMessage(null);
    const draft = {
      url: url.trim(),
      authentication,
      revision: current.revision,
      username: authentication === 'basic' ? username : null,
      secret: authentication === 'none' ? null : secret,
    };
    try {
      if (save) {
        const saved = await kafdeckApi.saveRegisteredServiceSettings(clusterId, service, draft, controller.signal);
        if (controller.signal.aborted) return;
        setCurrent(saved);
        setUrl(saved.url ?? '');
        setUsername('');
        setSecret('');
        setMessage(ksql ? t("ksqlDB settings saved.") : registry ? t("Schema Registry settings saved.") : t("Kafka Connect settings saved."));
        onSaved();
      } else {
        await kafdeckApi.testRegisteredServiceSettings(clusterId, service, draft, controller.signal);
        if (!controller.signal.aborted)
          setMessage(ksql ? t("ksqlDB connection verified.") : registry ? t("Schema Registry connection verified.") : t("Kafka Connect connection verified."));
      }
    } catch (reason) {
      if (!controller.signal.aborted)
        setError(reason instanceof ApiProblem ? reason.message : ksql ? t("The ksqlDB request could not be completed.") : registry
          ? t("The Schema Registry request could not be completed.") : t("The Kafka Connect request could not be completed."));
    } finally {
      if (request.current === controller) request.current = null;
      if (!controller.signal.aborted) setBusy(false);
    }
  }

  return <section className="border rounded p-3 kafdeck-service-settings" aria-labelledby={field + '-title'}>
    <div className="kafdeck-ecosystem-header mb-3">
      <h3 id={field + '-title'} className="mb-0" tabIndex={-1} ref={heading}>
        {ksql ? t("ksqlDB settings") : registry ? t("Schema Registry settings") : t("Kafka Connect settings")}
      </h3>
      <button type="button" className="btn btn-sm btn-outline-secondary" disabled={busy} onClick={onClose}>{t("Close")}</button>
    </div>
    {current && <form onSubmit={event => void submit(event)} onChange={() => { setMessage(null); setError(null); }}>
      <fieldset disabled={busy} className="d-grid gap-3">
        <div>
          <label className="form-label" htmlFor={field + '-url'}>{ksql ? t("ksqlDB URL") : registry ? t("Schema Registry URL") : t("Kafka Connect URL")}</label>
          <input id={field + '-url'} className="form-control" type="url" required maxLength={2048} dir="ltr" value={url}
            placeholder={ksql ? "https://ksqldb.example.com" : registry ? "https://registry.example.com" : "https://connect.example.com"} onChange={event => setUrl(event.target.value)} />
        </div>
        <div>
          <label className="form-label" htmlFor={field + '-authentication'}>{t("Authentication")}</label>
          <select id={field + '-authentication'} className="form-select" value={authentication} onChange={event => {
            setAuthentication(event.target.value as ServiceAuthentication); setUsername(''); setSecret('');
          }}>
            <option value="none">{t("None")}</option>
            <option value="basic">{t("Basic authentication")}</option>
            <option value="bearer">{t("Bearer token")}</option>
          </select>
        </div>
        {authentication === 'basic' && <div>
          <label className="form-label" htmlFor={field + '-username'}>{ksql ? t("Username") : registry ? t("Registry username") : t("Connect username")}</label>
          <input id={field + '-username'} className="form-control" autoComplete="off" maxLength={512}
            dir="ltr" value={username} onChange={event => setUsername(event.target.value)} />
        </div>}
        {authentication !== 'none' && <>
          <div>
            <label className="form-label" htmlFor={field + '-secret'}>{authentication === 'basic'
              ? (ksql ? t("Password") : registry ? t("Registry password") : t("Connect password")) : t("Bearer token")}</label>
            <input id={field + '-secret'} className="form-control" type="password" autoComplete="new-password" maxLength={4096}
              dir="ltr" value={secret} onChange={event => setSecret(event.target.value)} />
          </div>
          {current.url && current.authentication === authentication &&
            <p className="text-secondary mb-0">{t("Leave the credentials blank to reuse them at the same URL.")}</p>}
        </>}
        <div className="d-flex flex-wrap gap-2">
          <button type="submit" value="test" className="btn btn-outline-primary">{t("Test connection")}</button>
          <button type="submit" value="save" className="btn btn-primary">
            {ksql ? t("Save ksqlDB settings") : registry ? t("Save Registry settings") : t("Save Connect settings")}
          </button>
        </div>
      </fieldset>
    </form>}
    {!current && !error && <p role="status">{ksql ? t("Loading ksqlDB settings…") : registry ? t("Loading Schema Registry settings…") : t("Loading Kafka Connect settings…")}</p>}
    {message && <p className="text-success mt-3 mb-0" role="status">{message}</p>}
    {error && <p className="alert alert-danger mt-3 mb-0" role="alert">{error}</p>}
  </section>;
}
