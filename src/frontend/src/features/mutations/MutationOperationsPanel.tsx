import { FormEvent, useCallback, useEffect, useState } from 'react';
import {
  MutationApiProblem,
  mutationApi,
  type MutationStatus,
} from './mutationApi.js';
import { mutationExecutionSurface } from './mutationExecutionSurface.js';
import { MutationPreviewWorkflows } from './MutationPreviewWorkflows.js';

const preDispatchStates = new Set([
  'previewed',
  'awaitingConfirmation',
  'awaitingApproval',
  'ready',
]);

function describeProblem(reason: unknown): string {
  if (reason instanceof MutationApiProblem) {
    if (reason.status === 401) return 'Operator authentication is required.';
    if (reason.status === 403) return 'The current operator is not authorized for this mutation action.';
    if (reason.status === 409) return `${reason.message} Refresh the operation before taking another pre-dispatch action.`;
    return reason.message;
  }

  return reason instanceof Error
    ? reason.message
    : 'The governed mutation operation could not be loaded.';
}

function stateMessage(operation: MutationStatus): string | null {
  switch (operation.state) {
    case 'executionUnknown':
      return 'Execution outcome is unknown. Do not retry blindly. Reconcile provider evidence and observed state before any follow-up action.';
    case 'partiallyApplied':
      return 'The provider reported partial application. Treat the operation as destructive state that requires reconciliation, not automatic retry.';
    case 'appliedUnverified':
      return 'The provider accepted the mutation but Kafdeck could not conclusively verify the final state.';
    case 'stalePreview':
      return 'The frozen preview no longer matches current provider state. Create a new preview before continuing.';
    case 'failedBeforeDispatch':
      return 'The operation failed before external dispatch; no provider application is claimed.';
    case 'failedDefinitive':
      return 'The provider definitively rejected the dispatched operation.';
    default:
      return null;
  }
}

function MutationSummary({ operation }: { operation: MutationStatus }) {
  const warning = stateMessage(operation);
  const evidence = Object.entries(operation.safeProviderEvidence);

  return <article aria-labelledby={`mutation-${operation.operationId}`}>
    <h3 id={`mutation-${operation.operationId}`}>{operation.operationKind}</h3>
    <dl>
      <dt>Operation ID</dt><dd><code>{operation.operationId}</code></dd>
      <dt>Cluster</dt><dd>{operation.clusterId}</dd>
      <dt>Risk</dt><dd>{operation.riskClass}</dd>
      <dt>State</dt><dd>{operation.state}</dd>
      <dt>Execution material</dt><dd>{operation.requiresExecutionMaterial ? 'Re-submission required' : 'No re-submission required'}</dd>
      <dt>Preview expires</dt><dd>{new Date(operation.previewExpiresAtUtc).toLocaleString()}</dd>
      <dt>Independent approval</dt><dd>{operation.requiresIndependentApproval ? 'Required' : 'Not required'}</dd>
      {operation.resultCode && <><dt>Result code</dt><dd>{operation.resultCode}</dd></>}
    </dl>
    {operation.riskClass === 'critical' && <p role="alert"><strong>Critical mutation.</strong> Independent approval by a distinct eligible operator is mandatory.</p>}
    {warning && <p role="alert"><strong>Outcome guidance:</strong> {warning}</p>}
    {evidence.length > 0 && <details><summary>Safe provider evidence</summary><dl>{evidence.map(([key, value]) => <span key={key}><dt>{key}</dt><dd>{value}</dd></span>)}</dl></details>}
  </article>;
}

function bytesToBase64(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let binary = '';
  const chunk = 0x8000;
  for (let offset = 0; offset < bytes.length; offset += chunk) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + chunk));
  }
  return btoa(binary);
}

function pairMap(source: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const rawLine of source.split('\n')) {
    const line = rawLine.trim();
    if (!line) continue;
    const separator = line.indexOf('=');
    if (separator <= 0) throw new Error('Each execution-material line must use key=value.');
    const key = line.slice(0, separator).trim();
    const value = line.slice(separator + 1);
    if (Object.prototype.hasOwnProperty.call(result, key)) throw new Error(`Duplicate key: ${key}`);
    result[key] = value;
  }
  return result;
}

type MutationOperationsPanelProps = {
  clusterId: string;
  enabled?: boolean;
};

export function MutationOperationsPanel({ clusterId, enabled }: MutationOperationsPanelProps) {
  const [mutationAvailable, setMutationAvailable] = useState<boolean | null>(enabled ?? null);
  const [approvals, setApprovals] = useState<MutationStatus[]>([]);
  const [selected, setSelected] = useState<MutationStatus | null>(null);
  const [lookupId, setLookupId] = useState('');
  const [typedChallenge, setTypedChallenge] = useState('');
  const [recordKey, setRecordKey] = useState('');
  const [recordValue, setRecordValue] = useState('');
  const [recordHeaders, setRecordHeaders] = useState('');
  const [schemaExecutionSource, setSchemaExecutionSource] = useState('');
  const [connectExecutionConfiguration, setConnectExecutionConfiguration] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const clearExecutionMaterial = () => {
    setRecordKey('');
    setRecordValue('');
    setRecordHeaders('');
    setSchemaExecutionSource('');
    setConnectExecutionConfiguration('');
  };

  const selectOperation = (operation: MutationStatus) => {
    setSelected(operation);
    setTypedChallenge('');
    clearExecutionMaterial();
  };

  const loadApprovals = useCallback(async (signal?: AbortSignal) => {
    try {
      const response = await mutationApi.listApprovals(50, signal);
      setMutationAvailable(true);
      setApprovals(response.items);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      if ((reason instanceof MutationApiProblem && reason.status === 404) ||
          reason instanceof SyntaxError) {
        setMutationAvailable(false);
        setApprovals([]);
        setSelected(null);
        setError(null);
        return;
      }

      setMutationAvailable(true);
      setError(describeProblem(reason));
    }
  }, []);

  useEffect(() => {
    if (enabled === false) {
      setMutationAvailable(false);
      return;
    }

    const controller = new AbortController();
    void loadApprovals(controller.signal);
    return () => controller.abort();
  }, [enabled, loadApprovals]);

  const refreshSelected = async () => {
    if (!selected) return;
    setLoading(true);
    setError(null);
    try {
      setSelected(await mutationApi.get(selected.operationId));
    } catch (reason) {
      setError(describeProblem(reason));
    } finally {
      setLoading(false);
    }
  };

  const lookup = async (event: FormEvent) => {
    event.preventDefault();
    const operationId = lookupId.trim();
    if (!operationId) return;

    setLoading(true);
    setError(null);
    try {
      selectOperation(await mutationApi.get(operationId));
    } catch (reason) {
      setError(describeProblem(reason));
    } finally {
      setLoading(false);
    }
  };

  const apply = async (action: () => Promise<MutationStatus>, clearMaterialAfter = false) => {
    setLoading(true);
    setError(null);
    try {
      const operation = await action();
      setSelected(operation);
      setTypedChallenge('');
      if (clearMaterialAfter) clearExecutionMaterial();
      await loadApprovals();
    } catch (reason) {
      setError(describeProblem(reason));
    } finally {
      setLoading(false);
    }
  };

  const executionSurface = selected === null
    ? 'none'
    : mutationExecutionSurface(selected);

  const executeRecord = () => {
    if (!selected) return;
    void apply(() => {
      const headers = Object.entries(pairMap(recordHeaders)).map(([name, value]) => ({ name, value: bytesToBase64(value) }));
      return mutationApi.executeRecordProduction(selected, [{ key: recordKey ? bytesToBase64(recordKey) : null, value: bytesToBase64(recordValue), headers }]);
    }, true);
  };

  const executeSchema = () => {
    if (!selected) return;
    void apply(() => mutationApi.executeSchemaCreate(selected, schemaExecutionSource), true);
  };

  const executeConnectConfiguration = () => {
    if (!selected) return;
    void apply(() => mutationApi.executeConnectConfiguration(selected, pairMap(connectExecutionConfiguration)), true);
  };

  if (mutationAvailable !== true) return null;

  return <section className="card kafdeck-card" id="mutations" aria-labelledby="mutations-title">
    <h2 id="mutations-title">Governed mutations</h2>
    <p>Mutation actions use frozen previews, explicit confirmation, current-request authorization rechecks and durable execution state. Unknown or partial outcomes are never presented as safe retries.</p>

    <MutationPreviewWorkflows clusterId={clusterId} onPreview={selectOperation} />

    <article aria-labelledby="mutation-lookup-title">
      <h3 id="mutation-lookup-title">Operation status</h3>
      <form onSubmit={event => void lookup(event)}>
        <label htmlFor="mutation-operation-id">Operation ID</label>{' '}
        <input id="mutation-operation-id" value={lookupId} onChange={event => setLookupId(event.target.value)} autoComplete="off" />{' '}
        <button type="submit" disabled={loading || lookupId.trim().length === 0}>Load operation</button>
      </form>
    </article>

    <article aria-labelledby="approval-inbox-title">
      <h3 id="approval-inbox-title">Independent approval inbox</h3>
      <p>Only operations for which the current operator is independently eligible and currently authorized are shown.</p>
      <button type="button" onClick={() => void loadApprovals()} disabled={loading}>Refresh approvals</button>
      {approvals.length === 0
        ? <p>No currently authorized mutation approvals are pending.</p>
        : <table><thead><tr><th scope="col">Kind</th><th scope="col">Cluster</th><th scope="col">Risk</th><th scope="col">Expires</th><th scope="col">Review</th></tr></thead><tbody>{approvals.map(operation => <tr key={operation.operationId}><th scope="row">{operation.operationKind}</th><td>{operation.clusterId}</td><td>{operation.riskClass}</td><td>{new Date(operation.previewExpiresAtUtc).toLocaleString()}</td><td><button type="button" onClick={() => selectOperation(operation)}>Review</button></td></tr>)}</tbody></table>}
    </article>

    {error && <p role="alert">{error}</p>}
    {selected && <article aria-labelledby="selected-mutation-title">
      <h3 id="selected-mutation-title">Selected operation</h3>
      <MutationSummary operation={selected} />
      <div aria-label="Mutation actions">
        <button type="button" onClick={() => void refreshSelected()} disabled={loading}>Refresh status</button>{' '}
        {selected.state === 'awaitingConfirmation' && <>
          {selected.confirmationMode === 'typedTarget' && <><label htmlFor="mutation-confirmation-target">Type the exact server challenge</label>{' '}<input id="mutation-confirmation-target" value={typedChallenge} onChange={event => setTypedChallenge(event.target.value)} aria-describedby="mutation-confirmation-help" autoComplete="off" />{' '}<small id="mutation-confirmation-help">Challenge: <code>{selected.confirmationChallenge ?? 'Unavailable'}</code></small>{' '}</>}
          <button type="button" disabled={loading || (selected.confirmationMode === 'typedTarget' && typedChallenge !== selected.confirmationChallenge)} onClick={() => void apply(() => mutationApi.confirm(selected, selected.confirmationMode === 'typedTarget' ? typedChallenge : null))}>Confirm preview</button>{' '}
        </>}
        {selected.state === 'awaitingApproval' && <>
          <button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.approve(selected))}>Approve independently</button>{' '}
          <button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.reject(selected))}>Reject</button>{' '}
        </>}
        {preDispatchStates.has(selected.state) && <button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.cancel(selected))}>Cancel before dispatch</button>}
        {executionSurface === 'genericNoMaterial' && <>{' '}<button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.executeWithoutMaterial(selected))}>Execute governed mutation</button></>}
        {executionSurface === 'connectNoMaterial' && selected.operationKind === 'connectDelete' && <>{' '}<button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.executeConnectWithoutMaterial(selected))}>Execute connector deletion</button></>}
        {executionSurface === 'connectNoMaterial' && selected.operationKind === 'connectAlter' && <>{' '}<button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.executeConnectWithoutMaterial(selected))}>Execute admitted connector control</button></>}
      </div>

      {executionSurface === 'recordMaterial' && <fieldset disabled={loading}><legend>Re-submit record execution material</legend><p>This material must digest-match the preview and is cleared from UI state after execution.</p><label htmlFor="execute-record-key">UTF-8 key (optional)</label>{' '}<input id="execute-record-key" value={recordKey} onChange={event => setRecordKey(event.target.value)} autoComplete="off" /><br /><label htmlFor="execute-record-value">UTF-8 value</label><br /><textarea id="execute-record-value" value={recordValue} onChange={event => setRecordValue(event.target.value)} rows={5} /><br /><label htmlFor="execute-record-headers">Headers, one name=value per line</label><br /><textarea id="execute-record-headers" value={recordHeaders} onChange={event => setRecordHeaders(event.target.value)} rows={4} /><br /><button type="button" disabled={!recordValue} onClick={executeRecord}>Execute with matching record material</button></fieldset>}

      {executionSurface === 'schemaMaterial' && <fieldset disabled={loading}><legend>Re-submit schema execution material</legend><p>The schema source must match the admitted fingerprint and is cleared from UI state after execution.</p><label htmlFor="execute-schema-source">Schema source</label><br /><textarea id="execute-schema-source" value={schemaExecutionSource} onChange={event => setSchemaExecutionSource(event.target.value)} rows={8} /><br /><button type="button" disabled={!schemaExecutionSource.trim()} onClick={executeSchema}>Execute schema registration</button></fieldset>}

      {executionSurface === 'connectMaterial' && <fieldset disabled={loading}><legend>Re-submit connector configuration</legend><p>Connector create/update configuration must digest-match the admitted preview. Secret-bearing values remain browser-memory/request material and are cleared after execution.</p><label htmlFor="execute-connect-config">Configuration, one key=value per line</label><br /><textarea id="execute-connect-config" value={connectExecutionConfiguration} onChange={event => setConnectExecutionConfiguration(event.target.value)} rows={8} autoComplete="off" /><br /><button type="button" disabled={!connectExecutionConfiguration.trim()} onClick={executeConnectConfiguration}>Execute connector configuration mutation</button></fieldset>}
    </article>}
  </section>;
}