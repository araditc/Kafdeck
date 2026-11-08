import { t } from '../../shared/i18n.js';
import { useEffect, useRef, useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';

export function governanceOutcome(state: string): string {
  switch (state) {
    case 'executionUnknown': return t("Execution outcome is unknown. Do not retry blindly; inspect positive evidence and reconcile before any follow-up.");
    case 'stalePreview': return t("The reviewed material or active policy changed. Prepare a fresh preview and obtain fresh confirmation and approval.");
    case 'awaitingApproval': return t("A distinct eligible human must approve this exact operation. Catalog assignment and policy files grant no execution rights.");
    case 'appliedVerified': return t("The backend verified this exact operation. Reload the current governed resource state.");
    case 'appliedUnverified': return t("Application was reported but not conclusively verified. Inspect durable evidence before deciding another action.");
    case 'partiallyApplied': return t("Partial application requires reconciliation. No automatic rollback or retry is offered.");
    case 'executing': return t("Execution is in progress. Refresh the same operation; do not create a replacement request.");
    default: return t("Current authorization, revision, restore generation and policy are checked by the backend before any effect.");
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
  const article = useRef<HTMLElement>(null);
  const heading = useRef<HTMLHeadingElement>(null);
  const [reviewed, setReviewed] = useState(false);
  const [challenge, setChallenge] = useState('');
  useEffect(() => { setReviewed(false); setChallenge(''); heading.current?.focus({ preventScroll: false }); }, [operation.operationId, operation.previewHash]);
  const baseAllowed = governanceOperationActions(operation, reviewed, challenge);
  const allowed = {...baseAllowed, execute: baseAllowed.execute && executionAllowed};
  async function act(action: (signal: AbortSignal) => Promise<MutationStatus>) {
    const startedArticle = article.current;
    const startedInside = startedArticle?.contains(document.activeElement) === true;
    let actionSignal: AbortSignal | undefined;
    try {
      await run(async signal => {
        actionSignal = signal;
        const next = await action(signal);
        if (!signal.aborted) { update(next); setChallenge(''); }
      });
    } finally {
      // Disabling the busy action can drop keyboard focus onto BODY. Recover
      // only that lost focus in this still-mounted scope; never override an
      // operator who moved elsewhere or focus a dispatch/retry control.
      if (startedInside && actionSignal && !actionSignal.aborted &&
          article.current === startedArticle && startedArticle?.isConnected &&
          document.hasFocus() && document.activeElement === document.body) {
        heading.current?.focus({ preventScroll: false });
      }
    }
  }
  return <article ref={article} aria-label={t("Immutable governance operation")} className="mt-3">
    <h3 ref={heading} tabIndex={-1}>{t("Review exact operation")}</h3>
    <dl><dt>{t("Operation ID")}</dt><dd><code dir="ltr" lang="en">{operation.operationId}</code></dd>
      <dt>{t("Scope")}</dt><dd><bdi dir="ltr" lang="en">{operation.clusterId}</bdi></dd><dt>{t("Operation kind")}</dt><dd><bdi dir="ltr" lang="en">{operation.operationKind}</bdi></dd>
      <dt>{t("Risk / state")}</dt><dd><bdi dir="ltr" lang="en">{operation.riskClass} / {operation.state}</bdi></dd>
      <dt>{t("Plan fingerprint")}</dt><dd><code dir="ltr" lang="en">{operation.previewHash}</code></dd>
      <dt>{t("Expires (UTC)")}</dt><dd><bdi dir="ltr" lang="en">{operation.previewExpiresAtUtc}</bdi></dd>
      <dt>{t("Distinct eligible human approval")}</dt><dd>{operation.requiresIndependentApproval ? t("Required") : t("Not required by this operation")}</dd>
      {operation.resultCode && <><dt>{t("Result code")}</dt><dd><bdi dir="ltr" lang="en">{operation.resultCode}</bdi></dd></>}
    </dl>
    <p role="status">{governanceOutcome(operation.state)}</p>
    {!executionAllowed && <p role="status">{executionBlockReason ?? t("A current business requirement still blocks dispatch.")}</p>}
    {operation.riskClass === 'critical' && <p role="alert">{['governancepolicyactivation','policyactivation'].includes(operation.operationKind.toLowerCase())
      ? t("Critical policy activation requires confirmation and independent human approval for this exact material.")
      : t("This critical operation requires confirmation and independent human approval for this exact material.")}</p>}
    <details open><summary>{t("Frozen reviewed material")}</summary><pre tabIndex={0} dir="ltr" lang="en" className="automation-proof">{JSON.stringify(material, null, 2)}</pre></details>
    <details><summary>{t("Safe backend evidence")}</summary><pre tabIndex={0} dir="ltr" lang="en" className="automation-proof">{JSON.stringify(operation.safeProviderEvidence, null, 2)}</pre></details>
    <fieldset disabled={busy}><legend className="h4">{t("Mutation actions")}</legend>
      <label className="form-check"><input dir="auto" className="form-check-input" type="checkbox" checked={reviewed} onChange={event => setReviewed(event.target.checked)} />
        <span className="form-check-label">{t("I inspected this immutable operation, scope, material and fingerprint.")}</span></label>
      {operation.confirmationMode === 'typedTarget' && <label className="form-label">{t("Exact confirmation challenge:")}{' '}<code dir="ltr" lang="en">{operation.confirmationChallenge}</code>
        <input dir="ltr" className="form-control" autoComplete="off" value={challenge} onChange={event => setChallenge(event.target.value)} /></label>}
      <div className="d-flex flex-wrap gap-2 mt-2">
        <button type="button" className="btn btn-outline-primary" onClick={() => void act(signal => refresh ? refresh(operation, signal) : mutationApi.get(operation.operationId, signal))}>{t("Refresh same operation")}</button>
        <button type="button" className="btn btn-outline-warning" disabled={!allowed.confirm} onClick={() => void act(signal => mutationApi.confirm(operation, challenge || null, signal))}>{t("Confirm exact preview")}</button>
        <button type="button" className="btn btn-outline-warning" disabled={!allowed.approve} onClick={() => void act(signal => mutationApi.approve(operation, signal))}>{t("Submit independent human approval")}</button>
        <button type="button" className="btn btn-danger" disabled={!allowed.execute} onClick={() => void act(signal => execute(operation, signal))}>{t("Execute reviewed operation")}</button>
        {recover && <button type="button" className="btn btn-outline-warning" disabled={!reviewed || operation.state !== 'executionUnknown'} onClick={() => void act(signal => recover(operation, signal))}>{t("Verify positive receipt; never redispatch")}</button>}
        <button type="button" className="btn btn-outline-secondary" disabled={!allowed.stop} onClick={() => void act(signal => mutationApi.cancel(operation, signal))}>{t("Cancel undispatched operation")}</button>
        <button type="button" className="btn btn-outline-danger" disabled={!allowed.stop} onClick={() => void act(signal => mutationApi.reject(operation, signal))}>{t("Reject undispatched operation")}</button>
      </div>
    </fieldset>
  </article>;
}
