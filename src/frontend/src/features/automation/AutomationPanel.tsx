import { t } from '../../shared/i18n.js';
import { useEffect, useRef, useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';
import { automationApi, automationOutcome, type AutomationCapabilities, type CompensationEligibility, type DeclarativeInput, type DeclarativeObservation, type SealedPlan, type SourceRevision } from './automationApi.js';

const automationErrorMessages = {
  capabilitiesUnavailable: "Automation capabilities are unavailable or denied. No effect is enabled.",
  requestUnconfirmed: "Request denied, stale, invalid or unavailable. No successful effect is inferred; retain the same operation/key and refresh status before deciding another action.",
} as const;
type AutomationError = keyof typeof automationErrorMessages;

export function AutomationPanel({ notificationsAvailable = false }: { notificationsAvailable?: boolean }) {
  const [capabilities,setCapabilities]=useState<AutomationCapabilities|null>(null);
  const [manifest,setManifest]=useState(''); const [format,setFormat]=useState<'json'|'yaml'>('json');
  const [sourceRef,setSourceRef]=useState(''); const [sourceRevision,setSourceRevision]=useState(''); const [revision,setRevision]=useState('0');
  const [idempotency,setIdempotency]=useState(''); const [lookup,setLookup]=useState(''); const [resource,setResource]=useState('');
  const [observation,setObservation]=useState<DeclarativeObservation|null>(null); const [source,setSource]=useState<SourceRevision|null>(null);
  const [plan,setPlan]=useState<SealedPlan|null>(null); const [operation,setOperation]=useState<MutationStatus|null>(null);
  const [approvals,setApprovals]=useState<MutationStatus[]>([]); const [compensation,setCompensation]=useState<CompensationEligibility|null>(null);
  const [reviewed,setReviewed]=useState(false); const [challenge,setChallenge]=useState('');
  const [busy,setBusy]=useState(false); const [error,setError]=useState<AutomationError|null>(null);
  const lifetime=useRef<AbortController|null>(null); const active=useRef(false);
  useEffect(()=>{const controller=new AbortController();lifetime.current=controller;setIdempotency(crypto.randomUUID());
    void automationApi.capabilities(controller.signal).then(value=>{if(!controller.signal.aborted)setCapabilities(value);}).catch(()=>{if(!controller.signal.aborted)setError('capabilitiesUnavailable');});
    return()=>controller.abort();},[]);
  const enabled=capabilities?.enabled===true && capabilities.families.some(family=>family.kind==='NotificationSubscription' && family.apply);
  const input=():DeclarativeInput=>({manifest,format,sourceRef,sourceRevision,expectedOwnershipRevision:Number(revision)});
  async function run(action:(signal:AbortSignal)=>Promise<void>) {
    if(active.current || !lifetime.current)return;active.current=true;setBusy(true);setError(null);
    const signal=lifetime.current.signal;
    try{await action(signal);}catch{if(!signal.aborted)setError('requestUnconfirmed');}
    finally{active.current=false;if(!signal.aborted)setBusy(false);}
  }
  async function load(id:string,signal:AbortSignal) {
    setPlan(null);setOperation(null);setCompensation(null);setReviewed(false);setChallenge('');
    const sealed=await automationApi.show(id,signal); const status=await mutationApi.get(id,signal);
    if(signal.aborted)return;
    if(sealed.planId.toLowerCase()!==status.operationId.toLowerCase() || sealed.planFingerprint!==status.previewHash)throw new Error(t("Mismatched proof"));
    setLookup(id);setPlan(sealed);setOperation(status);
  }
  function useInverse() {
    if(!compensation?.inverseManifest)return;
    setManifest(compensation.inverseManifest);setFormat('json');setSourceRef(compensation.sourceRef);setSourceRevision(compensation.sourceRevision);setRevision(String(compensation.expectedOwnershipRevision));
    setSource(null);setObservation(null);setReviewed(false);if(plan)setResource(plan.resourceId);setIdempotency(crypto.randomUUID());
  }
  return <section className="card kafdeck-card" id="automation" aria-labelledby="automation-title" aria-busy={busy}>
    <h2 id="automation-title" className="card-title">{t("Governed automation")}</h2>
    <p>{t("Observe current state, inspect immutable plans and complete explicitly authorized changes. Every client uses the same backend policy, ownership and approval boundary.")}</p>
    <nav aria-label={t("Automation workflow connections")}><a href="#fleet">{t("Telemetry and runtime capabilities")}</a>{' · '}{notificationsAvailable&&<><a href="#notifications">{t("Alerts and delivery evidence")}</a>{' · '}</>}<a href="#mutations">{t("Operations and independent approval")}</a></nav>
    {(capabilities || !error) && <p role="status">{capabilities ? t("{0}: runtime {1}", [capabilities.apiVersion, enabled?t("enabled"):t("disabled")]):t("Loading authorized automation capabilities…")}</p>}
    {capabilities&&<ul>{capabilities.families.map(family=><li key={family.kind}>{family.kind}: {enabled&&family.apply?t("governed apply admitted"):family.ownership??t("effect unavailable")}{t("; destroy")}{' '}{enabled&&family.delete?t("admitted"):t("unsupported")}</li>)}</ul>}
    {error&&<p role="alert" className="alert alert-warning">{t(automationErrorMessages[error])}</p>}
    <fieldset disabled={busy||!enabled}><legend>{t("Registered source and desired state")}</legend>
      <div className="row g-3">
        <label className="col-md-6 form-label">{t("Source reference")}<input dir="ltr" className="form-control" value={sourceRef} onChange={e=>{setSourceRef(e.target.value);setSource(null);}} maxLength={128}/></label>
        <label className="col-md-6 form-label">{t("Resource ID for source inspection")}<input dir="ltr" className="form-control" value={resource} onChange={e=>{setResource(e.target.value);setSource(null);}} maxLength={128}/></label>
        <label className="col-md-8 form-label">{t("Desired canonical SHA256")}<input dir="ltr" className="form-control" value={sourceRevision} onChange={e=>setSourceRevision(e.target.value)} maxLength={64}/></label>
        <label className="col-md-4 form-label">{t("Expected ownership revision")}<input dir="auto" className="form-control" type="number" min="0" step="1" value={revision} onChange={e=>setRevision(e.target.value)}/></label>
      </div>
      <label className="form-label">{t("Manifest format")}<select className="form-select" value={format} onChange={e=>setFormat(e.target.value as 'json'|'yaml')}><option value="json">JSON</option><option value="yaml">YAML</option></select></label>
      <label className="form-label">{t("NotificationSubscription manifest (at most16 KiB; references only)")}<textarea dir="ltr" className="form-control font-monospace" rows={8} maxLength={16384} value={manifest} onChange={e=>setManifest(e.target.value)} spellCheck={false}/></label>
      <div className="d-flex flex-wrap gap-2"><button type="button" className="btn btn-outline-primary" onClick={()=>void run(async signal=>{const value=await automationApi.observe(input(),signal);if(!signal.aborted)setObservation(value);})}>{t("Observe ownership and drift")}</button>
      <button type="button" className="btn btn-outline-primary" onClick={()=>void run(async signal=>{const value=await automationApi.source(sourceRef,resource,signal);if(!signal.aborted)setSource(value);})}>{t("Read current source pin")}</button></div>
      {observation&&<dl aria-label={t("Fresh backend observation")}><dt>{t("Drift")}</dt><dd>{observation.drift}</dd><dt>{t("Ownership / revision")}</dt><dd>{observation.ownership} / <bdi dir="ltr" lang="en">{observation.ownershipRevision}</bdi></dd><dt>{t("Source matches ownership")}</dt><dd>{observation.sourceInSync?t("Yes"):t("No")}</dd><dt>{t("Deployment generation")}</dt><dd><bdi dir="ltr" lang="en">{observation.deploymentGeneration}</bdi></dd><dt>{t("Observed at")}</dt><dd><bdi dir="ltr" lang="en">{observation.observedAtUtc}</bdi></dd></dl>}
      {source&&<div><p>{t("Current source: revision")}{' '}{source.revision}{t(", generation")}{' '}<code dir="ltr" lang="en">{source.generation}</code>{t(", hash")}{' '}<code dir="ltr" lang="en">{source.sourceHash}</code>.</p><button className="btn btn-outline-warning" type="button" onClick={()=>void run(async signal=>{const value=await automationApi.advance(input(),source,signal);if(!signal.aborted){setSource(value);setObservation(null);setReviewed(false);}})}>{t("Explicitly advance source with this CAS proof")}</button><p>{t("SourceManage is separate authority. Advancing a pin grants neither plan nor apply permission.")}</p></div>}
      <label className="form-label">{t("Stable request idempotency key")}<input dir="auto" className="form-control" value={idempotency} onChange={e=>setIdempotency(e.target.value)} maxLength={128}/></label>
      <div className="d-flex flex-wrap gap-2"><button type="button" className="btn btn-outline-secondary" onClick={()=>setIdempotency(crypto.randomUUID())}>{t("Start a distinct reviewed request key")}</button>{[false,true].map(adopt=><button type="button" key={String(adopt)} className="btn btn-primary" onClick={()=>void run(async signal=>{const status=await automationApi.plan(input(),idempotency,adopt,signal);if(!signal.aborted)await load(status.operationId,signal);})}>{adopt?t("Preview explicit adoption"):t("Create immutable plan")}</button>)}</div>
    </fieldset>
    <fieldset disabled={busy||!enabled}><legend>{t("Saved plan and durable operation")}</legend>
      <label className="form-label">{t("Exact plan / operation UUID")}<input dir="auto" className="form-control" value={lookup} onChange={e=>{setLookup(e.target.value);setPlan(null);setOperation(null);setReviewed(false);setCompensation(null);}} maxLength={36}/></label>
      <div className="d-flex flex-wrap gap-2"><button className="btn btn-outline-primary" type="button" onClick={()=>void run(signal=>load(lookup,signal))}>{t("Refresh immutable plan and status")}</button><button className="btn btn-outline-primary" type="button" onClick={()=>void run(async signal=>{const inbox=await mutationApi.listApprovals(50,signal);if(!signal.aborted)setApprovals(inbox.items.filter(item=>item.operationKind.toLowerCase()==="declarativenotificationsubscription"));})}>{t("Read independently authorized approval inbox")}</button></div>
      {approvals.length>0&&<ul>{approvals.map(item=><li key={item.operationId}><button type="button" className="btn btn-link" onClick={()=>void run(signal=>load(item.operationId,signal))}><bdi dir="ltr" lang="en">{item.operationKind}</bdi>: <bdi dir="ltr" lang="en">{item.operationId}</bdi> ({item.state})</button></li>)}</ul>}
      {plan&&operation&&<article aria-label={t("Immutable plan review")}><h3>{plan.command}: <bdi dir="ltr" lang="en">{plan.resourceId}</bdi></h3><p role="status">{operation.state}: {automationOutcome(operation.state)}</p>
        <dl><dt>{t("Plan fingerprint")}</dt><dd><code dir="ltr" lang="en">{plan.planFingerprint}</code></dd><dt>{t("Owner / source")}</dt><dd><bdi dir="ltr" lang="en">{plan.ownerId}</bdi> / <bdi dir="ltr" lang="en">{plan.sourceId}</bdi></dd><dt>{t("Source revision")}</dt><dd><code dir="ltr" lang="en">{plan.sourceRevisionHash}</code></dd><dt>{t("Ownership revision")}</dt><dd><bdi dir="ltr" lang="en">{plan.expectedOwnershipRevision}</bdi></dd><dt>{t("Risk / independent approval")}</dt><dd>{plan.risk} / {plan.requiresIndependentApproval?t("Required"):t("Not required")}</dd><dt>{t("Expires")}</dt><dd><bdi dir="ltr" lang="en">{plan.expiresAtUtc}</bdi></dd></dl>
        <div className="row g-3">{(['before','desired','after'] as const).map(key=><div className="col-lg-4" key={key}><h4>{key}</h4><pre tabIndex={0} dir="ltr" lang="en" className="automation-proof">{JSON.stringify(plan[key],null,2)}</pre></div>)}</div>
        <p>{t("Changes:")}{' '}{plan.changes.join(', ')}</p>
        <label className="form-check"><input dir="auto" className="form-check-input" type="checkbox" checked={reviewed} onChange={e=>setReviewed(e.target.checked)}/><span className="form-check-label">{t("I inspected this exact immutable plan and understand its risk and source binding.")}</span></label>
        <label className="form-label">{t("Exact confirmation challenge")}<input dir="ltr" className="form-control" value={challenge} onChange={e=>setChallenge(e.target.value)} autoComplete="off"/></label>
        {operation.confirmationChallenge&&<p>{t("Required challenge:")}{' '}<code dir="ltr" lang="en">{operation.confirmationChallenge}</code></p>}
        <div className="d-flex flex-wrap gap-2">
          <button className="btn btn-outline-warning" type="button" disabled={!reviewed||!['previewed','awaitingConfirmation'].includes(operation.state)||(operation.confirmationMode==='typedTarget'&&challenge!==operation.confirmationChallenge)} onClick={()=>void run(async signal=>{await mutationApi.confirm(operation,challenge||null,signal);if(!signal.aborted)await load(plan.planId,signal);})}>{t("Confirm exact plan")}</button>
          <button className="btn btn-outline-warning" type="button" disabled={!reviewed||operation.state!=='awaitingApproval'} onClick={()=>void run(async signal=>{await mutationApi.approve(operation,signal);if(!signal.aborted)await load(plan.planId,signal);})}>{t("Submit independent human approval")}</button>
          <button className="btn btn-danger" type="button" disabled={!reviewed||operation.state!=='ready'} onClick={()=>void run(async signal=>{await automationApi.apply(plan,false,signal);if(!signal.aborted)await load(plan.planId,signal);})}>{t("Apply through backend governance")}</button>
          <button className="btn btn-outline-warning" type="button" disabled={!reviewed||!['executionUnknown','appliedVerified'].includes(operation.state)} onClick={()=>void run(async signal=>{await automationApi.apply(plan,true,signal);if(!signal.aborted)await load(plan.planId,signal);})}>{t("Verify positive receipt; never redispatch")}</button>
          <button className="btn btn-outline-primary" type="button" onClick={()=>void run(async signal=>{const value=await automationApi.compensation(plan.planId,signal);if(!signal.aborted)setCompensation(value);})}>{t("Inspect safe inverse eligibility")}</button>
        </div>
        <p>{t("Backend authorization decides eligibility on every request. A prompt, manifest owner, service identity or checkbox cannot replace distinct human approval.")}</p>
        {compensation&&<section aria-label={t("Safe inverse eligibility")}><h4>{compensation.eligibility}</h4><p>{t("Catalog")}{' '}<bdi dir="ltr" lang="en">{compensation.catalogVersion}</bdi>{t("; source operation")}{' '}<code dir="ltr" lang="en">{compensation.sourceOperationId}</code>.</p><p><bdi dir="auto">{compensation.reason}</bdi></p>{compensation.inverseManifest&&<><pre tabIndex={0} dir="ltr" lang="en" className="automation-proof">{compensation.inverseManifest}</pre><p>{compensation.requiresSourceAdvance?t("Explicit source advance is required before a fresh inverse preview."):t("Review the current source and ownership evidence before a fresh inverse preview.")}</p><button className="btn btn-outline-primary" type="button" onClick={useInverse}>{t("Stage inverse manifest for explicit review")}</button><button className="btn btn-outline-warning" type="button" disabled={!reviewed} onClick={()=>void run(async signal=>{const status=await automationApi.compensate(compensation.sourceOperationId,input(),idempotency,signal);if(!signal.aborted)await load(status.operationId,signal);})}>{t("Request a fresh governed inverse preview")}</button></>}</section>}
      </article>}
    </fieldset>
    <details><summary>{t("CLI, Terraform and MCP operator handoff")}</summary><p>{t("Use the same registered owner/source and immutable operation ID across clients. Advance a changed Git source explicitly, create and inspect a plan, obtain independent human approval, then apply. Terraform binds apply_plan_id and apply_plan_fingerprint; AwaitingApproval and ExecutionUnknown remain incomplete. MCP effects require explicit operator enablement and the same current authority. Destroy is unavailable for this family. No secret or bearer-token entry is offered here.")}</p><p>{t("Use the shipped v0.8 declarative CLI, Terraform provider and MCP operator guides for offline installation and exact client syntax. Refresh this panel after an external client changes the operation; no automatic retry or compensation occurs.")}</p></details>
  </section>;
}
