import { useEffect, useRef, useState } from 'react';
import { kafdeckApi, type BrokerAgentHealth, type BrokerAgentPackage, type BrokerAgentPreview, type BrokerAgentProgress } from '../shared/api.js';
import { t } from '../shared/i18n.js';
import { readViewError, readViewProblemText } from '../features/readviews/readViewProblem.js';

const stageLabel = {
  unknown: 'No dispatch has been recorded.',
  publishing: 'Package publication can be reconciled.',
  awaitingRestart: 'Waiting for evidence of the broker restart.',
  awaitingMetrics: 'Waiting for a fresh metric from this broker.',
  awaitingHealth: 'Waiting for Kafka and replicas to recover.',
  reconcile: 'The SSH response was interrupted. Check operation status.',
  complete: 'Broker healthy and fresh metric verified.',
  alreadyInstalled: 'The approved package is already installed.',
  expired: 'The plan expired before dispatch. Preview again.',
} satisfies Record<BrokerAgentProgress['stage'], string>;

function Health({value}: {value: BrokerAgentHealth}) {
  return <div>
    <p>{t("Brokers: {0} · Topics: {1} · Affected partitions: {2}", [value.brokerCount,value.topics,value.affectedPartitions])}</p>
    {!value.healthy && <p role="alert">{t("Kafka health or replica safety could not be verified.")}</p>}
    {value.blockedPartitions.length > 0 && <ul>{value.blockedPartitions.map(partition =>
      <li key={partition}><bdi>{partition}</bdi></li>)}</ul>}
  </div>;
}

export function BrokerAgentPanel({clusterId}: {clusterId: string}) {
  const [saved, setSaved] = useState<string | null>(null);
  const [target, setTarget] = useState<{brokerId:number|null;packages:BrokerAgentPackage[]} | null>(null);
  const [version, setVersion] = useState('');
  const [preview, setPreview] = useState<BrokerAgentPreview | null>(null);
  const [renewing, setRenewing] = useState(false);
  const [progress, setProgress] = useState<BrokerAgentProgress | null>(null);
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const request = useRef<AbortController | null>(null);
  useEffect(() => {
    const abort = new AbortController();
    void kafdeckApi.brokerAgentTargets(clusterId, abort.signal).then(value => {
      if (!abort.signal.aborted) {
        setTarget(value);
        setSaved(value.pendingPlan);
      }
    }).catch(() => { /* This feature can be absent on a deployment. */ });
    return () => { abort.abort(); request.current?.abort(); };
  }, [clusterId]);

  async function run(task: (signal: AbortSignal) => Promise<void>) {
    if (request.current) return;
    const abort = new AbortController();
    request.current = abort;
    setBusy(true);
    setError(null);
    try { await task(abort.signal); }
    catch (failure) { if (!abort.signal.aborted) setError(failure); }
    finally {
      if (!abort.signal.aborted) setBusy(false);
      request.current = null;
    }
  }

  const observe = (plan: string, apply: boolean) => run(async signal => {
    if (apply) {
      setSaved(plan);
      setRenewing(false);
    }
    const result = await kafdeckApi.brokerAgentProgress(plan, apply, signal);
    if (!signal.aborted) setProgress(result);
  });
  const brokerId = target?.brokerId;
  if (brokerId === undefined || brokerId === null || !target?.packages.length) return null;
  const finished = progress?.stage === 'complete' || progress?.stage === 'alreadyInstalled' || progress?.stage === 'expired';
  const needsRenewal = progress?.stage === 'publishing' && progress.requiresRenewal;
  const canConfirm = !saved || renewing;
  function inspectVersion(version: string, renew: boolean) {
    setConfirm(false);
    setPreview(null);
    setRenewing(renew);
    void run(async signal => {
      const result = await kafdeckApi.previewBrokerAgent(clusterId, brokerId!, version, signal);
      if (!signal.aborted) setPreview(result);
    });
  }
  return <section className="card kafdeck-card p-3" aria-labelledby="broker-agent-heading">
    <h2 id="broker-agent-heading" className="card-title">{t("Offline broker agent")}</h2>
    <p>{t("Broker")} <bdi>{brokerId}</bdi></p>
    {!saved && <form className="d-flex flex-wrap align-items-end gap-2" onSubmit={event => {
      event.preventDefault();
      inspectVersion(version, false);
    }}>
      <div><label className="form-label" htmlFor="broker-agent-package">{t("Offline package version")}</label>
        <select className="form-select" id="broker-agent-package" value={version} disabled={busy}
          onChange={event => { setVersion(event.currentTarget.value); setPreview(null); setConfirm(false); }}>
          <option value="">{t("Select a package")}</option>
          {target.packages.map(item => <option key={item.version}>{item.version}</option>)}
        </select></div>
      <button className="btn btn-outline-primary" disabled={!version || busy}>{t("Preview agent change")}</button>
    </form>}
    {preview && <div className="mt-3">
      <dl>
        <dt>{t("Current agent version")}</dt>
        <dd>{preview.currentVersion ?? (preview.currentDigest ? t("Uncatalogued package") : t("Not installed"))}</dd>
        <dt>{t("Target agent version")}</dt><dd><bdi>{preview.package.version}</bdi></dd>
        <dt>{t("Package SHA256")}</dt><dd className="text-break"><bdi>{preview.package.digest}</bdi></dd>
        <dt>{t("Service restart")}</dt><dd><bdi>{preview.adapter}</bdi> · {preview.restartRequired ? t("Required") : t("Already installed")}</dd>
        <dt>{t("Maintenance window (UTC)")}</dt>
        <dd dir="ltr">{new Date(preview.maintenanceFrom*1000).toISOString()} – {new Date(preview.maintenanceTo*1000).toISOString()}</dd>
      </dl>
      <p>{preview.launchMode === 'argfile' ? t("Kafdeck updates its agent argument file and package.") :
        t("Kafdeck replaces the package used by the existing agent argument.")}</p>
      <p>{t("Existing JVM options and broker credentials stay in place.")}</p>
      <Health value={preview.health} />
      {preview.requiresWindow && <p role="status">{t("A restart requires the approved maintenance window.")}</p>}
      {canConfirm && preview.plan && <div className="d-flex flex-wrap align-items-center gap-2">
        {preview.restartRequired && <label className="form-check mb-0">
          <input type="checkbox" className="form-check-input" checked={confirm} onChange={event => setConfirm(event.currentTarget.checked)} />
          {t("Confirm restart of this broker in the maintenance window")}
        </label>}
        <button className="btn btn-primary" type="button" disabled={busy || preview.restartRequired && !confirm}
          onClick={() => void observe(preview.plan!, true)}>{t("Apply agent change")}</button>
      </div>}
    </div>}
    {saved && <div className="mt-3 d-flex flex-wrap gap-2">
      <button className="btn btn-outline-primary" type="button" disabled={busy}
        onClick={() => void observe(saved, false)}>{t("Check agent operation")}</button>
      {!finished && !needsRenewal && <button className="btn btn-outline-secondary" type="button" disabled={busy}
        onClick={() => void observe(saved, true)}>{t("Retry the same agent request")}</button>}
      {needsRenewal && progress.version && <button className="btn btn-outline-secondary" type="button" disabled={busy}
        onClick={() => inspectVersion(progress.version!, true)}>{t("Preview pending change in the current maintenance window")}</button>}
      {finished && <button className="btn btn-outline-secondary" type="button" disabled={busy}
       onClick={() => { setSaved(null); setPreview(null); setProgress(null); }}>{t("Preview another package")}</button>}
    </div>}
    {busy && <p role="status">{t("Checking agent operation…")}</p>}
    {!!error && <p role="alert">{readViewProblemText(readViewError(error))}</p>}
    {progress && <div aria-live="polite">
      <p>{t(stageLabel[progress.stage])} <bdi>{progress.operationId}</bdi></p>
      {progress.health && <Health value={progress.health} />}
      {progress.metricSeenAt && <p>{t("Fresh metric at {0}", [new Date(progress.metricSeenAt*1000).toISOString()])}
        {' '}<bdi>{progress.monitoringInstance}</bdi></p>}
    </div>}
  </section>;
}
