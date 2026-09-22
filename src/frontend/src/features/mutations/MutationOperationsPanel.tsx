import { FormEvent, useCallback, useEffect, useState } from 'react';
import {
  MutationApiProblem,
  mutationApi,
  type MutationStatus,
} from './mutationApi.js';

const noMaterialExecutionKinds = new Set([
  'topicCreate',
  'topicAlter',
  'topicIncreasePartitions',
  'topicDelete',
  'consumerOffsetAlter',
  'consumerDelete',
  'schemaAlter',
  'schemaDelete',
  'recordsPurge',
]);

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
      <dt>Preview expires</dt><dd>{new Date(operation.previewExpiresAtUtc).toLocaleString()}</dd>
      <dt>Independent approval</dt><dd>{operation.requiresIndependentApproval ? 'Required' : 'Not required'}</dd>
      {operation.resultCode && <><dt>Result code</dt><dd>{operation.resultCode}</dd></>}
    </dl>
    {operation.riskClass === 'critical' && <p role="alert"><strong>Critical mutation.</strong> Independent approval by a distinct eligible operator is mandatory.</p>}
    {warning && <p role="alert"><strong>Outcome guidance:</strong> {warning}</p>}
    {evidence.length > 0 && <details><summary>Safe provider evidence</summary><dl>{evidence.map(([key, value]) => <span key={key}><dt>{key}</dt><dd>{value}</dd></span>)}</dl></details>}
  </article>;
}

export function MutationOperationsPanel() {
  const [approvals, setApprovals] = useState<MutationStatus[]>([]);
  const [selected, setSelected] = useState<MutationStatus | null>(null);
  const [lookupId, setLookupId] = useState('');
  const [typedChallenge, setTypedChallenge] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadApprovals = useCallback(async (signal?: AbortSignal) => {
    try {
      const response = await mutationApi.listApprovals(50, signal);
      setApprovals(response.items);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      setError(describeProblem(reason));
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void loadApprovals(controller.signal);
    return () => controller.abort();
  }, [loadApprovals]);

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
      const operation = await mutationApi.get(operationId);
      setSelected(operation);
      setTypedChallenge('');
    } catch (reason) {
      setError(describeProblem(reason));
    } finally {
      setLoading(false);
    }
  };

  const apply = async (action: () => Promise<MutationStatus>) => {
    setLoading(true);
    setError(null);
    try {
      const operation = await action();
      setSelected(operation);
      setTypedChallenge('');
      await loadApprovals();
    } catch (reason) {
      setError(describeProblem(reason));
    } finally {
      setLoading(false);
    }
  };

  const canExecuteWithoutMaterial = selected !== null &&
    selected.state === 'ready' &&
    noMaterialExecutionKinds.has(selected.operationKind);

  return <section id="mutations" aria-labelledby="mutations-title">
    <h2 id="mutations-title">Governed mutations</h2>
    <p>Mutation actions use frozen previews, explicit confirmation, current-request authorization rechecks and durable execution state. Unknown or partial outcomes are never presented as safe retries.</p>

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
        : <table><thead><tr><th scope="col">Kind</th><th scope="col">Cluster</th><th scope="col">Risk</th><th scope="col">Expires</th><th scope="col">Review</th></tr></thead><tbody>{approvals.map(operation => <tr key={operation.operationId}><th scope="row">{operation.operationKind}</th><td>{operation.clusterId}</td><td>{operation.riskClass}</td><td>{new Date(operation.previewExpiresAtUtc).toLocaleString()}</td><td><button type="button" onClick={() => { setSelected(operation); setTypedChallenge(''); }}>Review</button></td></tr>)}</tbody></table>}
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
        {canExecuteWithoutMaterial && <>{' '}<button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.executeWithoutMaterial(selected))}>Execute governed mutation</button></>}
      </div>
      {selected.state === 'ready' && !canExecuteWithoutMaterial && <p>This operation requires typed ephemeral execution material. Execute it only from its dedicated workflow; Kafdeck does not expose a generic execution-material console.</p>}
    </article>}
  </section>;
}
