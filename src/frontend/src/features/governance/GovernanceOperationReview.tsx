import { useEffect, useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';

export function governanceOutcome(state: string): string {
  switch (state) {
    case 'executionUnknown': return 'Execution outcome is unknown. Do not retry blindly; inspect positive evidence and reconcile before any follow-up.';
    case 'stalePreview': return 'The reviewed material or active policy changed. Prepare a fresh preview and obtain fresh confirmation and approval.';
    case 'awaitingApproval': return 'A distinct eligible human must approve this exact operation. Catalog assignment and policy files grant no execution rights.';
    case 'appliedVerified': return 'The backend verified this exact operation. Reload the current governed resource state.';
    case 'appliedUnverified': return 'Application was reported but not conclusively verified. Inspect durable evidence before deciding another action.';
    case 'partiallyApplied': return 'Partial application requires reconciliation. No automatic rollback or retry is offered.';
    case 'executing': return 'Execution is in progress. Refresh the same operation; do not create a replacement request.';
    default: return 'Current authorization, revision, restore generation and policy are checked by the backend before any effect.';
  }
}

export function governanceOperationActions(operation: MutationStatus, reviewed: boolean, challenge: string) {
  const preDispatch = ['previewed', 'awaitingConfirmation', 'awaitingApproval', 'ready'].includes(operation.state);
  return {
    confirm: reviewed && ['previewed', 'awaitingConfirmation'].includes(operation.state) &&
      (operation.confirmationMode !== 'typedTarget' || challenge === operation.confirmationChallenge),
    approve: reviewed && operation.state === 'awaitingApproval',
    execute: reviewed && operation.state === 'ready' && !operation.requiresExecutionMaterial,
    stop: reviewed && preDispatch,
  };
}

/** Controls never confer authority: all decisions and effects use the existing backend kernel. */
export function GovernanceOperationReview({ operation, material, busy, update, execute, recover, refresh, run, executionAllowed = true, executionBlockReason }: {
  operation: MutationStatus;
  material: unknown;
  busy: boolean;
  update: (operation: MutationStatus) => void;
  execute: (operation: MutationStatus, signal: AbortSignal) => Promise<MutationStatus>;
  recover?: (operation: MutationStatus, signal: AbortSignal) => Promise<MutationStatus>;
  refresh?: (operation: MutationStatus, signal: AbortSignal) => Promise<MutationStatus>;
  run: (action: (signal: AbortSignal) => Promise<void>) => Promise<void>;
  executionAllowed?: boolean;
  executionBlockReason?: string;
}) {
  const [reviewed, setReviewed] = useState(false);
  const [challenge, setChallenge] = useState('');
  useEffect(() => { setReviewed(false); setChallenge(''); }, [operation.operationId, operation.previewHash]);
  const baseAllowed = governanceOperationActions(operation, reviewed, challenge);
  const allowed = {...baseAllowed, execute: baseAllowed.execute && executionAllowed};
  function act(action: (signal: AbortSignal) => Promise<MutationStatus>) {
    return run(async signal => { const next = await action(signal); if (!signal.aborted) { update(next); setChallenge(''); } });
  }
  return <article aria-label="Immutable governance operation" className="mt-3">
    <h3>Review exact operation</h3>
    <dl><dt>Operation ID</dt><dd><code>{operation.operationId}</code></dd>
      <dt>Scope</dt><dd>{operation.clusterId}</dd><dt>Operation kind</dt><dd>{operation.operationKind}</dd>
      <dt>Risk / state</dt><dd>{operation.riskClass} / {operation.state}</dd>
      <dt>Plan fingerprint</dt><dd><code>{operation.previewHash}</code></dd>
      <dt>Expires (UTC)</dt><dd>{operation.previewExpiresAtUtc}</dd>
      <dt>Distinct eligible human approval</dt><dd>{operation.requiresIndependentApproval ? 'Required' : 'Not required by this operation'}</dd>
      {operation.resultCode && <><dt>Result code</dt><dd>{operation.resultCode}</dd></>}
    </dl>
    <p role="status">{governanceOutcome(operation.state)}</p>
    {!executionAllowed && <p role="status">{executionBlockReason ?? 'A current business requirement still blocks dispatch.'}</p>}
    {operation.riskClass === 'critical' && <p role="alert">This critical operation requires confirmation and independent human approval for this exact material.</p>}
    <details open><summary>Frozen reviewed material</summary><pre className="automation-proof">{JSON.stringify(material, null, 2)}</pre></details>
    <details><summary>Safe backend evidence</summary><pre className="automation-proof">{JSON.stringify(operation.safeProviderEvidence, null, 2)}</pre></details>
    <fieldset disabled={busy}>
      <label className="form-check"><input className="form-check-input" type="checkbox" checked={reviewed} onChange={event => setReviewed(event.target.checked)} />
        <span className="form-check-label">I inspected this immutable operation, scope, material and fingerprint.</span></label>
      {operation.confirmationMode === 'typedTarget' && <label className="form-label">Exact confirmation challenge: <code>{operation.confirmationChallenge}</code>
        <input className="form-control" autoComplete="off" value={challenge} onChange={event => setChallenge(event.target.value)} /></label>}
      <div className="d-flex flex-wrap gap-2 mt-2">
        <button type="button" className="btn btn-outline-primary" onClick={() => void act(signal => refresh ? refresh(operation, signal) : mutationApi.get(operation.operationId, signal))}>Refresh same operation</button>
        <button type="button" className="btn btn-outline-warning" disabled={!allowed.confirm} onClick={() => void act(signal => mutationApi.confirm(operation, challenge || null, signal))}>Confirm exact preview</button>
        <button type="button" className="btn btn-outline-warning" disabled={!allowed.approve} onClick={() => void act(signal => mutationApi.approve(operation, signal))}>Submit independent human approval</button>
        <button type="button" className="btn btn-danger" disabled={!allowed.execute} onClick={() => void act(signal => execute(operation, signal))}>Execute reviewed operation</button>
        {recover && <button type="button" className="btn btn-outline-warning" disabled={!reviewed || operation.state !== 'executionUnknown'} onClick={() => void act(signal => recover(operation, signal))}>Verify positive receipt; never redispatch</button>}
        <button type="button" className="btn btn-outline-secondary" disabled={!allowed.stop} onClick={() => void act(signal => mutationApi.cancel(operation, signal))}>Cancel undispatched operation</button>
        <button type="button" className="btn btn-outline-danger" disabled={!allowed.stop} onClick={() => void act(signal => mutationApi.reject(operation, signal))}>Reject undispatched operation</button>
      </div>
    </fieldset>
  </article>;
}
