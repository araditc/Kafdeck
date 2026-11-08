import { useEffect, useRef, useState } from 'react';
import { ApiProblem, kafdeckApi } from '../shared/api.js';
import { t } from '../shared/i18n.js';
import {
  forgetKafkaConnection, kafkaProtocols, recentKafkaConnections, rememberKafkaConnection, saslMechanisms,
  type KafkaConnectionObservation, type KafkaConnectionProfile, type KafkaConnectionTarget, type KafkaProtocol, type SaslMechanism,
} from '../shared/kafkaConnections.js';
import { productName } from '../shared/product.js';
import { LanguageSelector } from './LanguageSelector.js';

type TestResult = { observation: KafkaConnectionObservation; profile: KafkaConnectionProfile; remember: boolean };

export function KafkaConnectionPage({ scope, onLogout, onManaged, onOpenCluster, onManageUsers, canRegister }: {
  scope: string;
  onLogout: () => void;
  onManaged: () => void;
  onOpenCluster: (clusterId: string) => void;
  onManageUsers?: (() => void) | null;
  canRegister: boolean;
}) {
  const [targets, setTargets] = useState<KafkaConnectionTarget[]>([]);
  const [targetsFailed, setTargetsFailed] = useState(false);
  const [recent, setRecent] = useState(() => recentKafkaConnections(scope));
  const [name, setName] = useState('');
  const [servers, setServers] = useState('');
  const [protocol, setProtocol] = useState<KafkaProtocol>('PLAINTEXT');
  const [mechanism, setMechanism] = useState<SaslMechanism>('PLAIN');
  const [tested, setTested] = useState<TestResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const request = useRef<AbortController | null>(null);
  const form = useRef<HTMLFormElement>(null);
  const title = useRef<HTMLHeadingElement>(null);
  const sasl = protocol.startsWith('SASL_');
  const tls = protocol === 'SSL' || protocol === 'SASL_SSL';

  useEffect(() => {
    const controller = new AbortController();
    void kafdeckApi.listKafkaConnectionTargets(controller.signal)
      .then(items => { if (!controller.signal.aborted) setTargets(items); })
      .catch(() => { if (!controller.signal.aborted) setTargetsFailed(true); });
    return () => { controller.abort(); request.current?.abort(); };
  }, []);

  useEffect(() => { title.current?.focus(); }, []);

  function resetTest() {
    setTested(null);
    setError(null);
  }

  function clearCredentials() {
    for (const field of form.current?.querySelectorAll<HTMLInputElement | HTMLTextAreaElement>('[data-credential]') ?? []) {
      field.value = '';
    }
  }

  function applyProfile(profile: KafkaConnectionProfile) {
    clearCredentials();
    resetTest();
    setName(profile.name);
    setServers(profile.bootstrapServers.join(', '));
    setProtocol(profile.protocol);
    setMechanism(profile.mechanism ?? 'PLAIN');
    form.current?.querySelector<HTMLInputElement>('[name="connection-name"]')?.focus();
  }

  async function connect(formElement: HTMLFormElement, register: boolean) {
    if (request.current) return;
    const fields = new FormData(formElement);
    const read = (field: string) => (fields.get(field) as string | null) || null;
    const bootstrapServers = servers.split(/[\s,]+/u).filter(Boolean);
    if (!name.trim() || !bootstrapServers.length) {
      setError('connection-input');
      return;
    }
    const profile = { name: name.trim(), bootstrapServers, protocol, mechanism: sasl ? mechanism : null };
    const draft = {
      ...profile,
      username: read('username'), password: read('password'), caCertificate: read('ca-certificate'),
      clientCertificate: read('client-certificate'), clientKey: read('client-key'), clientKeyPassword: read('client-key-password'),
    };
    const remember = ![draft.username, draft.password, draft.clientKeyPassword].some(value => value && profile.name.includes(value));
    const controller = new AbortController();
    request.current = controller;
    if (register) setError(null);
    else resetTest();
    setBusy(true);
    try {
      const observation = register
        ? await kafdeckApi.registerKafkaConnection(draft, controller.signal)
        : await kafdeckApi.testKafkaConnection(draft, controller.signal);
      if (controller.signal.aborted) return;
      const result = { observation, profile, remember };
      if (register && observation.savedTarget) {
        const target = observation.savedTarget;
        setTargets(previous => [...previous.filter(item => item.id !== target.id), target]);
        openConnection(result);
      } else {
        setTested(result);
      }
    } catch (reason) {
      if (!controller.signal.aborted) setError(reason instanceof ApiProblem ? reason.code ?? 'connection-failed' : 'connection-failed');
    } finally {
      request.current = null;
      if (!controller.signal.aborted) setBusy(false);
    }
  }

  function openConnection(result: TestResult) {
    clearCredentials();
    if (result.remember) setRecent(rememberKafkaConnection(scope, result.profile));
    setTested(null);
    if (result.observation.clusterId) onOpenCluster(result.observation.clusterId);
  }

  function errorMessage(code: string) {
    if (code.endsWith('connection-input')) return t("Check the Kafka connection settings.");
    if (code.endsWith('connection-target')) return t("This broker list and protocol are not enabled for your account.");
    if (code.endsWith('connection-network')) return t("Enter broker IP addresses within the networks permitted by this installation.");
    if (code.endsWith('connection-name')) return t("This Kafka connection name is already in use.");
    if (code.endsWith('connection-limit')) return t("This installation has reached its saved connection limit.");
    if (code.endsWith('connection-save-busy')) return t("Try saving the connection again.");
    if (code.endsWith('connection-busy')) return t("Kafka connection tests are busy. Try again.");
    return t("Kafka connection failed. Check the broker, credentials, and TLS settings.");
  }

  return <div className="kafdeck-sign-in">
    <header className="kafdeck-sign-in-header">
      <div className="kafdeck-brand">
        <img className="kafdeck-brand-logo" src="/kafdeck-logo.webp" alt="" width={42} height={42} />
        <span className="kafdeck-brand-name">{productName}</span>
      </div>
      <div className="kafdeck-connection-actions">
        <LanguageSelector />
        {onManageUsers && <button className="btn btn-outline-primary" type="button" onClick={onManageUsers}>{t("Manage users")}</button>}
        {targets.length > 0 && <button className="btn btn-outline-primary" type="button" onClick={onManaged}>{t("Open managed dashboard")}</button>}
        <button className="btn btn-outline-secondary" type="button" onClick={onLogout}>{t("Sign out")}</button>
      </div>
    </header>
    <main className="kafdeck-sign-in-main kafdeck-connection-main" aria-labelledby="kafdeck-connection-title">
      <h1 id="kafdeck-connection-title" ref={title} tabIndex={-1}>{t("Connect to Kafka")}</h1>
        <div className="kafdeck-sign-in-grid kafdeck-connection-grid">
          <section className="card" aria-labelledby="kafdeck-new-connection">
            <div className="card-body">
              <h2 id="kafdeck-new-connection" className="h3">{t("New connection")}</h2>
              <p className="text-secondary">{canRegister
                ? t("Choose an existing target or enter broker IP addresses to test and save a new connection.")
                : t("Use a Kafka broker list approved for your account.")}</p>
              {targetsFailed && <p role="alert">{t("Approved Kafka targets could not be loaded.")}</p>}
              <form ref={form} autoComplete="off" onSubmit={event => {
                event.preventDefault();
                void connect(event.currentTarget, false);
              }} onChange={resetTest}>
                <fieldset disabled={busy}>
                  {targets.length > 0 && <label className="form-label mb-3">
                    {t("Configured targets")}
                    <select className="form-select mt-1" value="" onChange={event => {
                      const target = targets.find(item => item.id === event.target.value);
                      if (target) applyProfile({ ...target, name: target.name ?? target.id, mechanism: target.mechanism ?? null });
                    }}>
                      <option value="">{t("Choose a configured target")}</option>
                      {targets.map(target => <option key={target.id} value={target.id}>{target.name ?? target.id}</option>)}
                    </select>
                  </label>}
                  <label className="form-label mb-3">{t("Connection name")}
                    <input className="form-control mt-1" name="connection-name" dir="auto" required maxLength={60} value={name} onChange={event => setName(event.target.value)} />
                  </label>
                  <label className="form-label mb-3">{t("Bootstrap servers")}
                    <textarea className="form-control mt-1" dir="ltr" required maxLength={4816} rows={2} placeholder="broker.example:9092" value={servers} onChange={event => { clearCredentials(); setServers(event.target.value); }} />
                  </label>
                  <label className="form-label mb-3">{t("Security protocol")}
                    <select className="form-select mt-1" value={protocol} onChange={event => { clearCredentials(); setProtocol(event.target.value as KafkaProtocol); }}>
                      {kafkaProtocols.map(value => <option key={value} value={value}>{value}</option>)}
                    </select>
                  </label>
                  {sasl && <fieldset className="mb-3">
                    <legend className="h4">SASL</legend>
                    <label className="form-label mb-2">{t("SASL mechanism")}
                      <select className="form-select mt-1" value={mechanism} onChange={event => setMechanism(event.target.value as SaslMechanism)}>
                        {saslMechanisms.map(value => <option key={value} value={value}>{value}</option>)}
                      </select>
                    </label>
                    <label className="form-label mb-2">{t("SASL username")}<input className="form-control mt-1" name="username" required maxLength={512} autoComplete="off" data-credential /></label>
                    <label className="form-label">{t("SASL password")}<input className="form-control mt-1" name="password" type="password" required maxLength={4096} autoComplete="new-password" data-credential /></label>
                  </fieldset>}
                  {tls && <fieldset className="mb-3">
                    <legend className="h4">TLS</legend>
                    <label className="form-label mb-2">{t("CA certificate (PEM, optional)")}<textarea className="form-control mt-1" name="ca-certificate" dir="ltr" maxLength={65536} rows={2} data-credential /></label>
                    <label className="form-label mb-2">{t("Client certificate (PEM, optional)")}<textarea className="form-control mt-1" name="client-certificate" dir="ltr" maxLength={65536} rows={2} data-credential /></label>
                    <label className="form-label mb-2">{t("Client private key (PEM, optional)")}<textarea className="form-control mt-1" name="client-key" dir="ltr" maxLength={65536} rows={2} spellCheck={false} data-credential /></label>
                    <label className="form-label">{t("Client key password (optional)")}<input className="form-control mt-1" name="client-key-password" type="password" autoComplete="new-password" maxLength={4096} data-credential /></label>
                  </fieldset>}
                  <button className="btn btn-primary" type="submit">{t("Test connection")}</button>
                </fieldset>
              </form>
              {busy && <p className="mt-3 mb-0" role="status">{t("Testing Kafka connection…")}</p>}
              {error && <p className="alert alert-danger mt-3 mb-0" role="alert">{errorMessage(error)}</p>}
              {tested && <div className="mt-3">
                <p className="text-success" role="status">{t("Kafka connection verified.")}</p>
                {tested.observation.requiresRegistration
                  ? <button className="btn btn-primary" type="button" disabled={busy} onClick={() => {
                    if (form.current) void connect(form.current, true);
                  }}>{t("Save and open connection")}</button>
                  : <button className="btn btn-primary" type="button" disabled={busy} onClick={() => openConnection(tested)}>{t("Open connection")}</button>}
              </div>}
            </div>
          </section>
          <section className="card" aria-labelledby="kafdeck-recent-connections">
            <div className="card-body">
              <h2 id="kafdeck-recent-connections" className="h3">{t("Recent connections")}</h2>
              <p className="text-secondary">{t("This browser remembers connection names and settings. Enter credentials again when needed.")}</p>
              {!recent.length && <p className="mb-0">{t("No recent Kafka connections.")}</p>}
              <ul className="kafdeck-recent-sign-ins">
                {recent.map(profile => <li key={JSON.stringify(profile)}>
                  <button className="btn btn-outline-primary" type="button" disabled={busy} onClick={() => applyProfile(profile)}><bdi>{profile.name}</bdi></button>
                  <button className="btn btn-outline-secondary" type="button" aria-label={t("Remove {0} from recent connections", [profile.name])}
                    onClick={() => setRecent(forgetKafkaConnection(scope, profile))}>{t("Remove")}</button>
                </li>)}
              </ul>
            </div>
          </section>
        </div>
    </main>
  </div>;
}
