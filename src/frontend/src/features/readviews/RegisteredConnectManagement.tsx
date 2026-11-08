import { useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';
import { t } from '../../shared/i18n.js';
import { readViewError, readViewProblemText } from './readViewProblem.js';

type Action = 'create' | 'edit' | 'delete' | 'pause' | 'resume' | 'restart';

function actionLabel(action: Action) {
  switch (action) {
    case 'create': return t("Create connector");
    case 'edit': return t("Edit connector configuration");
    case 'delete': return t("Delete connector");
    case 'pause': return t("Pause connector");
    case 'resume': return t("Resume connector");
    case 'restart': return t("Restart connector");
  }
}

function parseSettings(raw: string, edit: boolean): Record<string, string | null> | null {
  try {
    const value: unknown = JSON.parse(raw);
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    const entries = Object.entries(value);
    if (!entries.length || entries.some(([key, item]) =>
      !key.trim() || (typeof item !== 'string' && !(edit && item === null)))) return null;
    return Object.fromEntries(entries);
  } catch {
    return null;
  }
}

export function RegisteredConnectManagement({ clusterId, selected, state, onExecuted }: {
  clusterId: string;
  selected: string | null;
  state?: string;
  onExecuted: (action: Action, name: string, operation: MutationStatus) => void;
}) {
  const [action, setAction] = useState<Action | null>(null);
  const [connectorName, setConnectorName] = useState('');
  const [input, setInput] = useState('');
  const [reviewKeys, setReviewKeys] = useState<string[]>([]);
  const [operation, setOperation] = useState<MutationStatus | null>(null);
  const [challenge, setChallenge] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const name = selected ?? connectorName.trim();

  function open(next: Action | null) {
    setAction(next);
    setOperation(null);
    setError(null);
    setChallenge('');
    setConnectorName('');
    setInput('');
    setReviewKeys([]);
  }

  async function apply(request: () => Promise<MutationStatus>, execute = false) {
    if (busy) return;
    setBusy(true);
    setError(null);
    try {
      const result = await request();
      setOperation(result);
      if (execute && action) {
        setInput('');
        onExecuted(action, name, result);
      }
    } catch (reason) {
      setError(readViewProblemText(readViewError(reason)));
    } finally {
      setBusy(false);
    }
  }

  function preview() {
    if (!action || !name) return;
    const id = crypto.randomUUID();
    if (action === 'create' || action === 'edit') {
      const settings = parseSettings(input, action === 'edit');
      if (!settings) {
        setError(t("Enter a JSON object with setting names and string values. Use null only to remove a setting when editing."));
        return;
      }
      if (action === 'create') settings.name = name;
      setReviewKeys(Object.keys(settings));
      void apply(() => action === 'edit'
        ? mutationApi.previewConnectSettingsChanges(clusterId, name, settings, id)
        : mutationApi.previewConnectConfiguration(clusterId, name, 'create', settings as Record<string, string>, id));
    } else if (action === 'delete') {
      void apply(() => mutationApi.previewConnectDelete(clusterId, name, id));
    } else {
      void apply(() => mutationApi.previewConnectControl(clusterId, name, { action }, id));
    }
  }

  function execute() {
    if (!operation) return;
    if (action === 'create' || action === 'edit') {
      const settings = parseSettings(input, action === 'edit');
      if (!settings) return;
      if (action === 'create') settings.name = name;
      void apply(() => action === 'edit'
        ? mutationApi.executeConnectSettingsChanges(operation, settings)
        : mutationApi.executeConnectConfiguration(operation, settings as Record<string, string>), true);
    } else void apply(() => mutationApi.executeConnectWithoutMaterial(operation), true);
  }

  const controls: Action[] = selected === null ? ['create'] : state
    ? ['edit', 'pause', 'resume', 'restart', 'delete'] : [];
  const canCancel = operation && ['awaitingConfirmation', 'awaitingApproval', 'ready'].includes(operation.state);
  return <div className="mb-3">
    <div className="d-flex flex-wrap gap-2">
      {controls.map(control => <button key={control} type="button"
        className={`btn btn-sm ${control === 'delete' ? 'btn-outline-danger' : 'btn-outline-primary'}`}
        disabled={busy || action !== null || (control === 'pause' && state === 'PAUSED') || (control === 'resume' && state === 'RUNNING')}
        onClick={() => open(control)}>{actionLabel(control)}</button>)}
    </div>
    {action && <section className="border rounded p-3 mt-3" aria-labelledby="connect-operation-title" aria-busy={busy}>
      <h4 id="connect-operation-title">{actionLabel(action)}</h4>
      {(selected || operation) && <p><bdi dir="auto">{name}</bdi></p>}
      {!operation && <form onSubmit={event => { event.preventDefault(); preview(); }}>
        {action === 'create' && <label className="form-label d-block">{t("Connector name")}
          <input className="form-control" dir="ltr" value={connectorName} onChange={event => setConnectorName(event.target.value)}
            autoComplete="off" required maxLength={512} disabled={busy} />
        </label>}
        {(action === 'create' || action === 'edit') && <>
          <p className="text-secondary" id="connect-operation-config-help">{action === 'edit'
            ? t("Enter only the settings you want to change. Use null to remove a key. Settings you omit, including credentials, are kept.")
            : t("Enter the Kafka Connect configuration as JSON with string values, including connector.class and all settings required by its plugin.")}</p>
          <label className="form-label d-block">{action === 'edit' ? t("Changed settings (JSON)") : t("Connector settings (JSON)")}
            <textarea className="form-control font-monospace" dir="ltr" value={input} onChange={event => setInput(event.target.value)}
              aria-describedby="connect-operation-config-help" placeholder={action === 'edit' ? '{"tasks.max":"2"}' : '{"connector.class":"…","tasks.max":"1"}'}
              autoComplete="off" spellCheck={false} rows={7} required disabled={busy} />
          </label>
        </>}
        <button className="btn btn-primary" type="submit" disabled={busy || !name}>{t("Preview operation")}</button>
      </form>}
      {operation && <>
        {reviewKeys.length > 0 && <p className="text-break">{t("Configuration keys:")}{' '}
          <bdi dir="ltr">{reviewKeys.join(', ')}</bdi></p>}
        {operation.state === 'awaitingConfirmation' && <>
          {operation.confirmationMode === 'typedTarget' && <>
            <p>{t("Type the exact server challenge")}{': '}<code dir="ltr">{operation.confirmationChallenge}</code></p>
            <input className="form-control mb-2" dir="ltr" aria-label={t("Type the exact server challenge")}
              autoComplete="off" value={challenge} onChange={event => setChallenge(event.target.value)} />
          </>}
          <button className="btn btn-primary" type="button" disabled={busy ||
            (operation.confirmationMode === 'typedTarget' && challenge !== operation.confirmationChallenge)}
            onClick={() => void apply(() => mutationApi.confirm(operation, operation.confirmationMode === 'typedTarget' ? challenge : null))}>
            {t("Confirm preview")}
          </button>
        </>}
        {operation.state === 'awaitingApproval' &&
          <p>{t("Another authorized operator must approve this operation in Automation.")}{' '}
            <code dir="ltr">{operation.operationId}</code></p>}
        {operation.state === 'ready' &&
          <button type="button" className={`btn ${action === 'delete' ? 'btn-danger' : 'btn-primary'}`}
            disabled={busy} onClick={execute}>{t("Execute operation")}</button>}
        <p className="mt-2 mb-1" role="status">{operation.state === 'appliedVerified' ? t("Kafka Connect change verified.")
          : operation.state === 'appliedUnverified' ? t("Kafka Connect accepted the request. Refresh connector status to check the result.")
            : <bdi dir="ltr">{operation.resultCode ?? operation.state}</bdi>}</p>
        <button type="button" className="btn btn-sm btn-outline-secondary" disabled={busy}
          onClick={() => void apply(() => mutationApi.get(operation.operationId))}>{t("Refresh operation status")}</button>
        {canCancel && <button type="button" className="btn btn-sm btn-outline-secondary ms-2" disabled={busy}
          onClick={() => void apply(() => mutationApi.cancel(operation))}>{t("Cancel before dispatch")}</button>}
      </>}
      {error && <p role="alert" className="mt-2 mb-1">{error}</p>}
      <button type="button" className="btn btn-sm btn-link mt-2" disabled={busy || !!canCancel}
        onClick={() => open(null)}>{t("Close operation")}</button>
    </section>}
  </div>;
}
