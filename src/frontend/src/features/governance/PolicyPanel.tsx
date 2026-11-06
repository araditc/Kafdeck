import { t } from '../../shared/i18n.js';
import { useEffect, useRef, useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';
import { GovernanceOperationReview } from './GovernanceOperationReview.js';
import { governanceProblem, policyApi, scopedOperation, type PolicyActive, type PolicyDecision, type PolicyOperationMaterial, type PolicyValidation, type PolicyVersion } from './governanceApi.js';

const operationKinds = ['policyVersion', 'policyActivation', 'governancePolicyVersion', 'governancePolicyActivation'];
export function assertPolicyBinding(operation: MutationStatus, material: PolicyOperationMaterial, clusterId: string) {
  scopedOperation(operation, clusterId, operationKinds);
  if (material.operationId !== operation.operationId || material.previewHash !== operation.previewHash || material.clusterId !== clusterId || !material.document || !material.contentHash)
    throw new Error(t("Immutable policy material does not match this exact scoped operation."));
}

export function PolicyPanel({clusterId}: {clusterId: string}) {
  const [versions, setVersions] = useState<PolicyVersion[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [active, setActive] = useState<PolicyActive | null>(null);
  const [document, setDocument] = useState('');
  const [validation, setValidation] = useState<PolicyValidation | null>(null);
  const [versionId, setVersionId] = useState('');
  const [enable, setEnable] = useState(true);
  const [key, setKey] = useState('');
  const [lookup, setLookup] = useState('');
  const [operation, setOperation] = useState<MutationStatus | null>(null);
  const [material, setMaterial] = useState<PolicyOperationMaterial | null>(null);
  const [resourceKind, setResourceKind] = useState('topic');
  const [resourceId, setResourceId] = useState('');
  const [action, setAction] = useState('TopicAlter');
  const [risk, setRisk] = useState('high');
  const [decision, setDecision] = useState<PolicyDecision | null>(null);
  const [dryRun, setDryRun] = useState<PolicyDecision[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const lifetime = useRef<AbortController | null>(null);
  const inFlight = useRef(false);
  async function load(signal: AbortSignal, more = false) {
    const [page, state] = await Promise.all([policyApi.versions(clusterId, more ? cursor : null, signal), policyApi.active(clusterId, signal)]);
    if (!signal.aborted) { setVersions(previous => more ? [...previous, ...page.items].slice(-1000) : page.items); setCursor(page.nextCursor); setActive(state); setLoaded(true); }
  }
  useEffect(() => {
    const controller = new AbortController(); lifetime.current = controller; setKey(crypto.randomUUID());
    void load(controller.signal).catch(reason => { if (!controller.signal.aborted) setError(governanceProblem(reason)); });
    return () => controller.abort();
  }, [clusterId]);
  async function run(request: (signal: AbortSignal) => Promise<void>) {
    if (!lifetime.current || inFlight.current) return;
    inFlight.current = true; setBusy(true); setError(null);
    const signal = lifetime.current.signal;
    try { await request(signal); } catch (reason) { if (!signal.aborted) setError(governanceProblem(reason)); }
    finally { inFlight.current = false; if (!signal.aborted) setBusy(false); }
  }
  async function inspect(id: string, signal: AbortSignal) {
    setOperation(null); setMaterial(null);
    const frozen = await policyApi.operation(id, signal);
    const status = frozen.operation ?? await mutationApi.get(id, signal);
    assertPolicyBinding(status, frozen, clusterId);
    if (!signal.aborted) { setLookup(id); setMaterial(frozen); setOperation(status); }
  }
  function update(status: MutationStatus) {
    if (!material) throw new Error(t("Reload frozen policy material before continuing."));
    assertPolicyBinding(status, material, clusterId); setOperation(status);
  }
  function invalidatePreview() { setOperation(null); setMaterial(null); }
  const fixture = () => ({clusterId, resourceKind, resourceId, action, risk});
  return <section className="card kafdeck-card" id="governance-policy" aria-labelledby="governance-policy-title" aria-busy={busy}>
    <h2 id="governance-policy-title" className="card-title">{t("Governance policy")}</h2>
    <p>{t("Scope:")}{' '}<strong><bdi dir="ltr" lang="en">{clusterId}</bdi></strong>{t(". Closed, versioned policies narrow current permissions. No policy file, catalog assignment or approval grants missing rights.")}</p>
    <p>{t("Activation and deactivation are critical governed operations. Changed policy can stale an existing preview; unavailable active authority blocks execution.")}</p>
    {error && <p role="alert" className="alert alert-warning">{error}</p>}
    <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={() => void run(signal => load(signal))}>{t("Reload authorized policy state")}</button>
    {active && <dl><dt>{t("Active revision")}</dt><dd>{active.revision}</dd><dt>{t("Enabled")}</dt><dd>{active.enabled ? t("Yes") : t("No")}</dd><dt>{t("Active version")}</dt><dd>{active.versionId ?? t("None")}</dd><dt>{t("Authority digest")}</dt><dd><code dir="ltr" lang="en">{active.digest}</code></dd></dl>}
    <fieldset disabled={busy || !loaded} className="mt-3"><legend>{t("Validate and prepare a policy version")}</legend>
      <label className="form-label">{t("Closed schema JSON (maximum 256 KiB)")}<textarea dir="auto" className="form-control" rows={9} value={document} onChange={event => { setDocument(event.target.value); setValidation(null); invalidatePreview(); }} spellCheck={false} /></label>
      <label className="form-label">{t("Stable request idempotency key")}<input dir="ltr" className="form-control" value={key} maxLength={256} onChange={event => { setKey(event.target.value); invalidatePreview(); }} /></label>
      <button className="btn btn-outline-secondary" type="button" disabled={operation?.state === 'executionUnknown' || operation?.state === 'executing'} onClick={() => { setKey(crypto.randomUUID()); invalidatePreview(); }}>{t("Create key for an intentionally new policy request")}</button>
      <p>{t("Retain the same key and operation reference after an uncertain response. Use a new key only for an intentionally new request.")}</p>
      <div className="d-flex flex-wrap gap-2">
        <button className="btn btn-outline-primary" type="button" onClick={() => void run(async signal => { const value = await policyApi.validate(clusterId, document, signal); if (!signal.aborted) setValidation(value); })}>{t("Validate closed policy")}</button>
        <button className="btn btn-outline-warning" type="button" onClick={() => void run(async signal => { const status = await policyApi.previewVersion(clusterId, document, key, signal); if (!signal.aborted) await inspect(status.operationId, signal); })}>{t("Preview governed version write")}</button>
      </div>
      {validation && <section aria-label={t("Policy validation result")}><p role="status">{t("Valid:")}{' '}{String(validation.valid)}{t("; canonical hash")}{' '}<code dir="ltr" lang="en">{validation.contentHash}</code>.</p><pre tabIndex={0} dir="ltr" lang="en" className="automation-proof">{typeof validation.document === 'string' ? validation.document : JSON.stringify(validation.document, null, 2)}</pre></section>}
    </fieldset>
    <fieldset disabled={busy || !loaded} className="mt-3"><legend>{t("Review versions and governed activation (latest 1000 loaded versions retained)")}</legend>
      <label className="form-label">{t("Immutable policy version")}<select className="form-select" value={versionId} onChange={event => { setVersionId(event.target.value); invalidatePreview(); }}>
        <option value="">{t("Select a version")}</option>{versions.map(version => <option key={version.versionId} value={version.versionId}>{version.versionId} / {version.contentHash}</option>)}</select></label>
      {versions.find(version => version.versionId === versionId) && <details><summary>{t("Selected immutable version")}</summary><pre tabIndex={0} dir="ltr" lang="en" className="automation-proof">{versions.find(version => version.versionId === versionId)?.document}</pre></details>}
      <label className="form-check"><input dir="auto" className="form-check-input" type="checkbox" checked={enable} onChange={event => { setEnable(event.target.checked); invalidatePreview(); }} /><span className="form-check-label">{t("Enable selected policy (uncheck for governed deactivation)")}</span></label>
      <button className="btn btn-outline-warning" type="button" disabled={!versionId || active === null} onClick={() => void run(async signal => { if (!active) return; const status = await policyApi.previewActivation(clusterId, versionId, active.revision, enable, key, signal); if (!signal.aborted) await inspect(status.operationId, signal); })}>{t("Preview critical activation change")}</button>
      {cursor && <button className="btn btn-outline-primary" type="button" onClick={() => void run(signal => load(signal, true))}>{t("Read next policy version page")}</button>}
    </fieldset>
    <fieldset disabled={busy || !loaded} className="mt-3"><legend>{t("Explain and dry-run current policy")}</legend>
      <p>{t("Read-only evaluation uses current backend facts. It creates no approval and grants no effect rights.")}</p>
      <div className="row g-2">
        <label className="col-md-3 form-label">{t("Resource kind")}<select className="form-select" value={resourceKind} onChange={event => { setResourceKind(event.target.value); setDecision(null); setDryRun(null); }}>{['topic','consumerGroup','schema','connector','broker','cluster','catalog','policy','dataJob','generator','acl','scram','quota','notification','resource'].map(value => <option value={value} key={value}>{t(value)}</option>)}</select></label>
        <label className="col-md-3 form-label">{t("Exact resource ID")}<input dir="ltr" className="form-control" maxLength={249} value={resourceId} onChange={event => { setResourceId(event.target.value); setDecision(null); setDryRun(null); }} /></label>
        <label className="col-md-3 form-label">{t("Registered action")}<input dir="auto" className="form-control" maxLength={128} value={action} onChange={event => { setAction(event.target.value); setDecision(null); setDryRun(null); }} /></label>
        <label className="col-md-3 form-label">{t("Risk")}<select className="form-select" value={risk} onChange={event => { setRisk(event.target.value); setDecision(null); setDryRun(null); }}>{['low', 'moderate', 'high', 'critical'].map(value => <option value={value} key={value}>{t(value)}</option>)}</select></label>
      </div>
      <div className="d-flex flex-wrap gap-2"><button className="btn btn-outline-primary" type="button" onClick={() => void run(async signal => { const result = await policyApi.explain(fixture(), signal); if (!signal.aborted) setDecision(result); })}>{t("Explain current decision")}</button>
        <button className="btn btn-outline-primary" type="button" onClick={() => void run(async signal => { const result = await policyApi.dryRun([fixture()], signal); if (!signal.aborted) setDryRun(result.items); })}>{t("Dry-run this fixture")}</button></div>
      {decision && <pre tabIndex={0} dir="ltr" lang="en" aria-label={t("Current policy explanation")} className="automation-proof">{JSON.stringify(decision, null, 2)}</pre>}
      {dryRun && <pre tabIndex={0} dir="ltr" lang="en" aria-label={t("Read-only dry-run results")} className="automation-proof">{JSON.stringify(dryRun, null, 2)}</pre>}
    </fieldset>
    <form className="mt-3" onSubmit={event => { event.preventDefault(); void run(signal => inspect(lookup, signal)); }}>
      <label className="form-label">{t("Existing policy operation ID")}<input dir="auto" className="form-control" value={lookup} onChange={event => setLookup(event.target.value)} autoComplete="off" /></label>
      <button className="btn btn-outline-primary" disabled={busy} type="submit">{t("Inspect frozen policy operation")}</button>
    </form>
    {operation && material && <GovernanceOperationReview operation={operation} material={{kind: material.kind, clusterId: material.clusterId, versionId: material.versionId, contentHash: material.contentHash, document: material.document, expectedRevision: material.expectedRevision, enabled: material.enabled}} busy={busy} update={update} execute={(status, signal) => mutationApi.executeWithoutMaterial(status, signal)} recover={(status, signal) => policyApi.recover(status, signal)} refresh={async (status, signal) => { const frozen = await policyApi.operation(status.operationId, signal); return frozen.operation ?? mutationApi.get(status.operationId, signal); }} run={run} />}
  </section>;
}
