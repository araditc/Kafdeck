import { useEffect, useRef, useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';
import { automationApi, automationOutcome, type AutomationCapabilities, type CompensationEligibility, type DeclarativeInput, type DeclarativeObservation, type SealedPlan, type SourceRevision } from './automationApi.js';

export function AutomationPanel({ notificationsAvailable = false }: { notificationsAvailable?: boolean }) {
  const [capabilities,setCapabilities]=useState<AutomationCapabilities|null>(null);
  const [manifest,setManifest]=useState(''); const [format,setFormat]=useState<'json'|'yaml'>('json');
  const [sourceRef,setSourceRef]=useState(''); const [sourceRevision,setSourceRevision]=useState(''); const [revision,setRevision]=useState('0');
  const [idempotency,setIdempotency]=useState(''); const [lookup,setLookup]=useState(''); const [resource,setResource]=useState('');
  const [observation,setObservation]=useState<DeclarativeObservation|null>(null); const [source,setSource]=useState<SourceRevision|null>(null);
  const [plan,setPlan]=useState<SealedPlan|null>(null); const [operation,setOperation]=useState<MutationStatus|null>(null);
  const [approvals,setApprovals]=useState<MutationStatus[]>([]); const [compensation,setCompensation]=useState<CompensationEligibility|null>(null);
  const [reviewed,setReviewed]=useState(false); const [challenge,setChallenge]=useState('');
  const [busy,setBusy]=useState(false); const [error,setError]=useState<string|null>(null);
  const lifetime=useRef<AbortController|null>(null); const active=useRef(false);
  useEffect(()=>{const controller=new AbortController();lifetime.current=controller;setIdempotency(crypto.randomUUID());
    void automationApi.capabilities(controller.signal).then(value=>{if(!controller.signal.aborted)setCapabilities(value);}).catch(()=>{if(!controller.signal.aborted)setError('Automation capabilities are unavailable or denied. No effect is enabled.');});
    return()=>controller.abort();},[]);
  const enabled=capabilities?.enabled===true && capabilities.families.some(family=>family.kind==='NotificationSubscription' && family.apply);
  const input=():DeclarativeInput=>({manifest,format,sourceRef,sourceRevision,expectedOwnershipRevision:Number(revision)});
  async function run(action:(signal:AbortSignal)=>Promise<void>) {
    if(active.current || !lifetime.current)return;active.current=true;setBusy(true);setError(null);
    const signal=lifetime.current.signal;
    try{await action(signal);}catch{if(!signal.aborted)setError('Request denied, stale, invalid or unavailable. No successful effect is inferred; retain the same operation/key and refresh status before deciding another action.');}
    finally{active.current=false;if(!signal.aborted)setBusy(false);}
  }
  async function load(id:string,signal:AbortSignal) {
    setPlan(null);setOperation(null);setCompensation(null);setReviewed(false);setChallenge('');
    const sealed=await automationApi.show(id,signal); const status=await mutationApi.get(id,signal);
    if(signal.aborted)return;
    if(sealed.planId.toLowerCase()!==status.operationId.toLowerCase() || sealed.planFingerprint!==status.previewHash)throw new Error('Mismatched proof');
    setLookup(id);setPlan(sealed);setOperation(status);
  }
  function useInverse() {
    if(!compensation?.inverseManifest)return;
    setManifest(compensation.inverseManifest);setFormat('json');setSourceRef(compensation.sourceRef);setSourceRevision(compensation.sourceRevision);setRevision(String(compensation.expectedOwnershipRevision));
    setSource(null);setObservation(null);setReviewed(false);if(plan)setResource(plan.resourceId);setIdempotency(crypto.randomUUID());
  }
  return <section className="card kafdeck-card" id="automation" aria-labelledby="automation-title" aria-busy={busy}>
    <h2 id="automation-title" className="card-title">Governed automation</h2>
    <p>Observe current state, inspect immutable plans and complete explicitly authorized changes. Every client uses the same backend policy, ownership and approval boundary.</p>
    <nav aria-label="Automation workflow connections"><a href="#fleet">Telemetry and runtime capabilities</a>{' · '}{notificationsAvailable&&<><a href="#notifications">Alerts and delivery evidence</a>{' · '}</>}<a href="#mutations">Operations and independent approval</a></nav>
    <p role="status">{capabilities ? `${capabilities.apiVersion}: runtime ${enabled?'enabled':'disabled'}`:'Loading authorized automation capabilities…'}</p>
    {capabilities&&<ul>{capabilities.families.map(family=><li key={family.kind}>{family.kind}: {enabled&&family.apply?'governed apply admitted':family.ownership??'effect unavailable'}; destroy {enabled&&family.delete?'admitted':'unsupported'}</li>)}</ul>}
    {error&&<p role="alert" className="alert alert-warning">{error}</p>}
    <fieldset disabled={busy||!enabled}><legend>Registered source and desired state</legend>
      <div className="row g-3">
        <label className="col-md-6 form-label">Source reference<input className="form-control" value={sourceRef} onChange={e=>{setSourceRef(e.target.value);setSource(null);}} maxLength={128}/></label>
        <label className="col-md-6 form-label">Resource ID for source inspection<input className="form-control" value={resource} onChange={e=>{setResource(e.target.value);setSource(null);}} maxLength={128}/></label>
        <label className="col-md-8 form-label">Desired canonical SHA256<input className="form-control" value={sourceRevision} onChange={e=>setSourceRevision(e.target.value)} maxLength={64}/></label>
        <label className="col-md-4 form-label">Expected ownership revision<input className="form-control" type="number" min="0" step="1" value={revision} onChange={e=>setRevision(e.target.value)}/></label>
      </div>
      <label className="form-label">Manifest format<select className="form-select" value={format} onChange={e=>setFormat(e.target.value as 'json'|'yaml')}><option value="json">JSON</option><option value="yaml">YAML</option></select></label>
      <label className="form-label">NotificationSubscription manifest (at most16 KiB; references only)<textarea className="form-control font-monospace" rows={8} maxLength={16384} value={manifest} onChange={e=>setManifest(e.target.value)} spellCheck={false}/></label>
      <div className="d-flex flex-wrap gap-2"><button type="button" className="btn btn-outline-primary" onClick={()=>void run(async signal=>{const value=await automationApi.observe(input(),signal);if(!signal.aborted)setObservation(value);})}>Observe ownership and drift</button>
      <button type="button" className="btn btn-outline-primary" onClick={()=>void run(async signal=>{const value=await automationApi.source(sourceRef,resource,signal);if(!signal.aborted)setSource(value);})}>Read current source pin</button></div>
      {observation&&<dl aria-label="Fresh backend observation"><dt>Drift</dt><dd>{observation.drift}</dd><dt>Ownership / revision</dt><dd>{observation.ownership} / {observation.ownershipRevision}</dd><dt>Source matches ownership</dt><dd>{observation.sourceInSync?'Yes':'No'}</dd><dt>Deployment generation</dt><dd>{observation.deploymentGeneration}</dd><dt>Observed at</dt><dd>{observation.observedAtUtc}</dd></dl>}
      {source&&<div><p>Current source: revision {source.revision}, generation <code>{source.generation}</code>, hash <code>{source.sourceHash}</code>.</p><button className="btn btn-outline-warning" type="button" onClick={()=>void run(async signal=>{const value=await automationApi.advance(input(),source,signal);if(!signal.aborted){setSource(value);setObservation(null);setReviewed(false);}})}>Explicitly advance source with this CAS proof</button><p>SourceManage is separate authority. Advancing a pin grants neither plan nor apply permission.</p></div>}
      <label className="form-label">Stable request idempotency key<input className="form-control" value={idempotency} onChange={e=>setIdempotency(e.target.value)} maxLength={128}/></label>
      <div className="d-flex flex-wrap gap-2"><button type="button" className="btn btn-outline-secondary" onClick={()=>setIdempotency(crypto.randomUUID())}>Start a distinct reviewed request key</button>{[false,true].map(adopt=><button type="button" key={String(adopt)} className="btn btn-primary" onClick={()=>void run(async signal=>{const status=await automationApi.plan(input(),idempotency,adopt,signal);if(!signal.aborted)await load(status.operationId,signal);})}>{adopt?'Preview explicit adoption':'Create immutable plan'}</button>)}</div>
    </fieldset>
    <fieldset disabled={busy||!enabled}><legend>Saved plan and durable operation</legend>
      <label className="form-label">Exact plan / operation UUID<input className="form-control" value={lookup} onChange={e=>{setLookup(e.target.value);setPlan(null);setOperation(null);setReviewed(false);setCompensation(null);}} maxLength={36}/></label>
      <div className="d-flex flex-wrap gap-2"><button className="btn btn-outline-primary" type="button" onClick={()=>void run(signal=>load(lookup,signal))}>Refresh immutable plan and status</button><button className="btn btn-outline-primary" type="button" onClick={()=>void run(async signal=>{const inbox=await mutationApi.listApprovals(50,signal);if(!signal.aborted)setApprovals(inbox.items.filter(item=>item.operationKind.toLowerCase()==="declarativenotificationsubscription"));})}>Read independently authorized approval inbox</button></div>
      {approvals.length>0&&<ul>{approvals.map(item=><li key={item.operationId}><button type="button" className="btn btn-link" onClick={()=>void run(signal=>load(item.operationId,signal))}>{item.operationKind}: {item.operationId} ({item.state})</button></li>)}</ul>}
      {plan&&operation&&<article aria-label="Immutable plan review"><h3>{plan.command}: {plan.resourceId}</h3><p role="status">{operation.state}: {automationOutcome(operation.state)}</p>
        <dl><dt>Plan fingerprint</dt><dd><code>{plan.planFingerprint}</code></dd><dt>Owner / source</dt><dd>{plan.ownerId} / {plan.sourceId}</dd><dt>Source revision</dt><dd><code>{plan.sourceRevisionHash}</code></dd><dt>Ownership revision</dt><dd>{plan.expectedOwnershipRevision}</dd><dt>Risk / independent approval</dt><dd>{plan.risk} / {plan.requiresIndependentApproval?'Required':'Not required'}</dd><dt>Expires</dt><dd>{plan.expiresAtUtc}</dd></dl>
        <div className="row g-3">{(['before','desired','after'] as const).map(key=><div className="col-lg-4" key={key}><h4>{key}</h4><pre className="automation-proof">{JSON.stringify(plan[key],null,2)}</pre></div>)}</div>
        <p>Changes: {plan.changes.join(', ')}</p>
        <label className="form-check"><input className="form-check-input" type="checkbox" checked={reviewed} onChange={e=>setReviewed(e.target.checked)}/><span className="form-check-label">I inspected this exact immutable plan and understand its risk and source binding.</span></label>
        <label className="form-label">Exact confirmation challenge<input className="form-control" value={challenge} onChange={e=>setChallenge(e.target.value)} autoComplete="off"/></label>
        {operation.confirmationChallenge&&<p>Required challenge: <code>{operation.confirmationChallenge}</code></p>}
        <div className="d-flex flex-wrap gap-2">
          <button className="btn btn-outline-warning" type="button" disabled={!reviewed||!['previewed','awaitingConfirmation'].includes(operation.state)||(operation.confirmationMode==='typedTarget'&&challenge!==operation.confirmationChallenge)} onClick={()=>void run(async signal=>{await mutationApi.confirm(operation,challenge||null,signal);if(!signal.aborted)await load(plan.planId,signal);})}>Confirm exact plan</button>
          <button className="btn btn-outline-warning" type="button" disabled={!reviewed||operation.state!=='awaitingApproval'} onClick={()=>void run(async signal=>{await mutationApi.approve(operation,signal);if(!signal.aborted)await load(plan.planId,signal);})}>Submit independent human approval</button>
          <button className="btn btn-danger" type="button" disabled={!reviewed||operation.state!=='ready'} onClick={()=>void run(async signal=>{await automationApi.apply(plan,false,signal);if(!signal.aborted)await load(plan.planId,signal);})}>Apply through backend governance</button>
          <button className="btn btn-outline-warning" type="button" disabled={!reviewed||!['executionUnknown','appliedVerified'].includes(operation.state)} onClick={()=>void run(async signal=>{await automationApi.apply(plan,true,signal);if(!signal.aborted)await load(plan.planId,signal);})}>Verify positive receipt; never redispatch</button>
          <button className="btn btn-outline-primary" type="button" onClick={()=>void run(async signal=>{const value=await automationApi.compensation(plan.planId,signal);if(!signal.aborted)setCompensation(value);})}>Inspect safe inverse eligibility</button>
        </div>
        <p>Backend authorization decides eligibility on every request. A prompt, manifest owner, service identity or checkbox cannot replace distinct human approval.</p>
        {compensation&&<section aria-label="Safe inverse eligibility"><h4>{compensation.eligibility}</h4><p>Catalog {compensation.catalogVersion}; source operation <code>{compensation.sourceOperationId}</code>.</p><p>{compensation.reason}</p>{compensation.inverseManifest&&<><pre className="automation-proof">{compensation.inverseManifest}</pre><p>{compensation.requiresSourceAdvance?'Explicit source advance is required before a fresh inverse preview.':'Review the current source and ownership evidence before a fresh inverse preview.'}</p><button className="btn btn-outline-primary" type="button" onClick={useInverse}>Stage inverse manifest for explicit review</button><button className="btn btn-outline-warning" type="button" disabled={!reviewed} onClick={()=>void run(async signal=>{const status=await automationApi.compensate(compensation.sourceOperationId,input(),idempotency,signal);if(!signal.aborted)await load(status.operationId,signal);})}>Request a fresh governed inverse preview</button></>}</section>}
      </article>}
    </fieldset>
    <details><summary>CLI, Terraform and MCP operator handoff</summary><p>Use the same registered owner/source and immutable operation ID across clients. Advance a changed Git source explicitly, create and inspect a plan, obtain independent human approval, then apply. Terraform binds apply_plan_id and apply_plan_fingerprint; AwaitingApproval and ExecutionUnknown remain incomplete. MCP effects require explicit operator enablement and the same current authority. Destroy is unavailable for this family. No secret or bearer-token entry is offered here.</p><p>Use the shipped v0.8 declarative CLI, Terraform provider and MCP operator guides for offline installation and exact client syntax. Refresh this panel after an external client changes the operation; no automatic retry or compensation occurs.</p></details>
  </section>;
}
