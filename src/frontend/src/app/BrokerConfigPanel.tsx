import { type FormEvent, useEffect, useState } from 'react';
import {
  mutationApi, type BrokerConfigPreview, type BrokerConfigSetting,
  type MutationStatus,
} from '../features/mutations/mutationApi.js';
import { kafdeckApi } from '../shared/api.js';
import { t } from '../shared/i18n.js';
import { ownedErrorPresentation, problemPresentation } from '../shared/presentationProblem.js';

export function BrokerConfigPanel({ clusterId }: { clusterId: string }) {
  const [settings, setSettings] = useState<BrokerConfigSetting[]>([]);
  const [brokerIds, setBrokerIds] = useState<number[]>([]);
  const [brokerId, setBrokerId] = useState('');
  const [value, setValue] = useState('');
  const [inherit, setInherit] = useState(false);
  const [review, setReview] = useState<BrokerConfigPreview>();
  const [restorable, setRestorable] = useState(false);
  const [challenge, setChallenge] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    const controller = new AbortController();
    void Promise.all([
      mutationApi.brokerConfigSettings(clusterId, controller.signal),
      kafdeckApi.getCluster(clusterId, controller.signal),
    ]).then(([catalog, cluster]) => {
      if (controller.signal.aborted) return;
      setSettings(catalog);
      setBrokerIds(cluster.data.brokers.map(broker => broker.brokerId));
      setBrokerId(String(cluster.data.brokers[0]?.brokerId ?? ''));
    }).catch(reason => { if (!controller.signal.aborted) setError(problemPresentation(ownedErrorPresentation(reason) ?? { raw: reason instanceof Error ? reason.message : t("Configuration request failed.") })); });
    return () => controller.abort();
  }, [clusterId]);

  const setting = settings[0];
  const operation = review?.operation;

  function resetPreview() {
    setReview(undefined);
    setChallenge('');
    setRestorable(false);
    setError(undefined);
  }

  async function run(task: () => Promise<void>) {
    setBusy(true);
    setError(undefined);
    try { await task(); }
    catch (reason) { setError(problemPresentation(ownedErrorPresentation(reason) ?? { raw: reason instanceof Error ? reason.message : t("Configuration request failed.") })); }
    finally { setBusy(false); }
  }

  async function preview(event?: FormEvent, restoreId?: string) {
    event?.preventDefault();
    if (!setting || brokerId === '') return;
    const command = restoreId ? 'restore' : inherit ? 'inherit' : 'set';
    resetPreview();
    await run(async () => {
      setReview(await mutationApi.previewBrokerConfig(clusterId, {
        key: setting.key, scope: 'brokerOverride', brokerId: Number(brokerId), command,
        ...(command === 'set' ? { value } : {}),
        ...(restoreId ? { restoreOperationId: restoreId } : {}),
      }));
      setRestorable(command === 'set');
    });
  }

  function update(task: () => Promise<MutationStatus>) {
    void run(async () => {
      const status = await task();
      setReview(current => current ? { ...current, operation: status } : current);
    });
  }

  return <section className="card kafdeck-card" aria-labelledby="fleet-broker-config-title">
    <div className="card-body">
      <h2 className="card-title" id="fleet-broker-config-title">{t("Dynamic broker settings")}</h2>
      {setting && <>
        <p><code>{setting.key}</code>{' · '}{t("Wait time in milliseconds when the log cleaner finds no work.")}</p>
        <form onSubmit={event => void preview(event)}>
          <fieldset disabled={busy}>
            <div className="row g-3 mb-3">
              <label className="col-sm-4">{t("Broker")}
                <select className="form-select" required value={brokerId} onChange={event => { setBrokerId(event.target.value); resetPreview(); }}>
                  {brokerIds.map(id => <option key={id} value={id}>{id}</option>)}
                </select>
              </label>
              {!inherit && <label className="col-sm-4">{t("New value")} ({setting.minValue}–{setting.maxValue})
                <input className="form-control" type="number" required step={1} min={setting.minValue} max={setting.maxValue}
                  value={value} onChange={event => { setValue(event.target.value); resetPreview(); }} />
              </label>}
            </div>
            <label className="form-check mb-3">
              <input className="form-check-input" type="checkbox" checked={inherit} onChange={event => { setInherit(event.target.checked); resetPreview(); }} />
              <span className="form-check-label">{t("Remove override and inherit")}</span>
            </label>
            <button type="submit" className="btn btn-primary" disabled={brokerId === ''}>{t("Preview configuration change")}</button>
          </fieldset>
        </form>
      </>}
      {error && <div className="alert alert-danger mt-3" role="alert">{error}</div>}
      {review && operation && <section className="mt-3" aria-label={t("Configuration preview")}>
        <dl className="row">
          <dt className="col-sm-4">{t("Target")}</dt><dd className="col-sm-8"><bdi>{review.target.clusterId} · {t("Broker")} {review.target.brokerId}</bdi></dd>
          <dt className="col-sm-4">{t("Effective value and source")}</dt><dd className="col-sm-8"><bdi>{review.effectiveValue} ms · {review.effectiveSource}</bdi></dd>
          <dt className="col-sm-4">{t("Expected value and source")}</dt><dd className="col-sm-8"><bdi>{review.expectedValue} ms · {review.expectedSource}</bdi></dd>
          {operation.safeProviderEvidence['configuration.source'] && <><dt className="col-sm-4">{t("Verified value and source")}</dt>
            <dd className="col-sm-8"><bdi>{operation.safeProviderEvidence['configuration.value']} ms · {operation.safeProviderEvidence['configuration.source']}</bdi></dd></>}
        </dl>
        <p><bdi>{t("Operation ID")} <code>{operation.operationId}</code></bdi></p>
        <p role="status"><bdi>{operation.state}</bdi>{operation.resultCode && <> · <bdi>{operation.resultCode}</bdi></>}</p>
        {operation.state === 'stalePreview' && <p>{t("The setting changed after preview. Create a fresh preview.")}</p>}
        {(operation.state === 'previewed' || operation.state === 'awaitingConfirmation') && <>
          {operation.confirmationChallenge && <label className="mb-2">{t("Type {0} to confirm", [operation.confirmationChallenge])}
            <input className="form-control" value={challenge} onChange={event => setChallenge(event.target.value)} />
          </label>}
          <button className="btn btn-outline-primary" type="button" disabled={busy || (!!operation.confirmationChallenge && challenge !== operation.confirmationChallenge)}
            onClick={() => update(() => mutationApi.confirm(operation, challenge || null))}>{t("Confirm configuration preview")}</button>
        </>}
        {operation.state === 'ready' && <button className="btn btn-danger" type="button" disabled={busy}
          onClick={() => update(() => mutationApi.executeWithoutMaterial(operation))}>{t("Apply configuration change")}</button>}
        {(operation.state === 'awaitingApproval' || operation.state === 'executing' || operation.state === 'ready') &&
          <button className="btn btn-outline-secondary ms-2" type="button" disabled={busy}
            onClick={() => update(() => mutationApi.get(operation.operationId))}>{t("Refresh operation status")}</button>}
        {operation.state === 'appliedVerified' && restorable && <button className="btn btn-outline-primary" type="button" disabled={busy}
          onClick={() => void preview(undefined, operation.operationId)}>{t("Preview restore")}</button>}
      </section>}
    </div>
  </section>;
}
