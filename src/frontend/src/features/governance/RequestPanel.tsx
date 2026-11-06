import { t } from '../../shared/i18n.js';
import { useRef, useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';
import { GovernanceOperationReview } from './GovernanceOperationReview.js';
import { requestApi, assertRequestOperation, assertLinkedOperation, type RequestCommand, type RequestHistory, type RequestOperation, type ServiceRequest } from './workflowApi.js';
import { useGovernanceSession } from './useGovernanceSession.js';

export function requestBusinessOutcome(state: string) {
  switch(state.toLowerCase()) {
    case 'approved':return t("Business approval is recorded. The linked effect still needs current permissions, confirmation and any independent critical approval.");
    case 'needsreplan':return t("Material, policy or generation changed. Prepare a new operation and an explicit request revision; previous decisions cannot be reused.");
    case 'expired':return t("This revision expired. Replan explicitly; an expired decision cannot authorize an effect.");
    case 'rejected':return t("An eligible human rejected this revision. Its previous approval cannot authorize execution.");
    case 'cancelled':return t("Undispatched business work was cancelled. This does not assert rollback of a dispatched or unknown effect.");
    case 'submitted':return t("A current scoped human distinct from the requester must decide this exact revision.");
    default:return t("A draft binds an existing immutable operation. Submission and business decisions are separate governed changes.");
  }
}
export function RequestPanel({clusterId}:{clusterId:string}) {
  const selectedHeading = useRef<HTMLHeadingElement>(null);
  const [items,setItems]=useState<ServiceRequest[]>([]),[cursor,setCursor]=useState<string|null>(null),[selected,setSelected]=useState<ServiceRequest|null>(null);
  const [history,setHistory]=useState<RequestHistory[]>([]),[historyCursor,setHistoryCursor]=useState<string|null>(null);
  const [loaded,setLoaded]=useState(false),[command,setCommand]=useState<RequestCommand>('Draft'),[operationId,setOperationId]=useState('');
  const [reason,setReason]=useState(''),[expires,setExpires]=useState(''),[stableKey,setStableKey]=useState(''),[lookup,setLookup]=useState('');
  const [preview,setPreview]=useState<RequestOperation|null>(null),[linked,setLinked]=useState<MutationStatus|null>(null);
  async function load(signal:AbortSignal,more=false){const page=await requestApi.list(clusterId,more?cursor:null,signal);if(!signal.aborted){setItems(previous=>more?[...previous,...page.items].slice(-1000):page.items);setCursor(page.nextCursor);setLoaded(true);}}
  const {busy,error,run}=useGovernanceSession(clusterId,()=>{setItems([]);setCursor(null);setSelected(null);setHistory([]);setHistoryCursor(null);setLoaded(false);setCommand('Draft');setOperationId('');setReason('');setExpires('');setPreview(null);setLinked(null);setStableKey(crypto.randomUUID());setLookup('');},load);
  function invalidate(){setPreview(null);}
  function choose(request:ServiceRequest){window.setTimeout(()=>selectedHeading.current?.focus(),0);setSelected(request);setCommand('Submit');setOperationId(request.operationId);setReason(request.reason);setExpires(request.expiresAtUtc);setHistory([]);setHistoryCursor(null);setLinked(null);invalidate();setStableKey(crypto.randomUUID());}
  async function refreshRequest(signal:AbortSignal){if(!selected)return;const value=await requestApi.get(clusterId,selected.requestId,signal);if(!signal.aborted){setSelected(value);setLinked(null);}}
  async function inspect(id:string,signal:AbortSignal){setPreview(null);const value=await requestApi.operation(id,signal);assertRequestOperation(value,clusterId);if(!signal.aborted){setPreview(value);setLookup(id);}}
  async function prepare(signal:AbortSignal){const status=await requestApi.preview({clusterId,command,expectedVersion:command==='Draft'?0:selected?.version??0,
    ...(command!=='Draft'&&selected?{requestId:selected.requestId}:{}),...(command==='Draft'||command==='Revise'?{operationId,expiresAtUtc:expires}:{}),reason},stableKey,signal);if(!signal.aborted)await inspect(status.operationId,signal);}
  function update(status:MutationStatus){if(!preview)throw new Error(t("Reload frozen request material."));const next={...preview,operation:status};assertRequestOperation(next,clusterId);setPreview(next);}
  async function readHistory(signal:AbortSignal,more=false){if(!selected)return;const page=await requestApi.history(clusterId,selected.requestId,more?historyCursor:null,signal);if(!signal.aborted){setHistory(previous=>more?[...previous,...page.items].slice(-1000):page.items);setHistoryCursor(page.nextCursor);}}
  async function inspectLinked(signal:AbortSignal){if(!selected)return;const status=await mutationApi.get(selected.operationId,signal);assertLinkedOperation(selected,status);if(!signal.aborted)setLinked(status);}
  function updateLinked(status:MutationStatus){if(!selected)throw new Error(t("Select the immutable request first."));assertLinkedOperation(selected,status);setLinked(status);}
  return <section id="service-requests" className="card kafdeck-card" aria-labelledby="service-requests-title" aria-busy={busy}>
    <h2 id="service-requests-title" className="card-title">{t("Service requests and approvals")}</h2>
    <p>{t("Scope:")}{' '}<strong><bdi dir="ltr" lang="en">{clusterId}</bdi></strong>{t(". Business approval binds one immutable operation and revision. It grants no permissions and does not replace confirmation or independent critical approval.")}</p>
    {error&&<p role="alert" className="alert alert-warning">{error}</p>}
    <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>load(signal))}>{t("Reload authorized requests")}</button>
    {loaded&&items.length>0&&<p role="status">{t("{0} authorized requests loaded.", [items.length])}</p>}
    {loaded&&items.length===0&&<p role="status">{t("No authorized service requests in this page.")}</p>}
    {items.length>0&&<div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table" aria-label={t("Authorized requests; latest 1000 loaded records retained")}><thead><tr><th scope="col">{t("Request")}</th><th scope="col">{t("Material revision / CAS version")}</th><th scope="col">{t("Business state")}</th><th scope="col">{t("Linked effect state")}</th><th scope="col">{t("Review")}</th></tr></thead><tbody>{items.map(item=><tr key={item.requestId}><td><code dir="ltr" lang="en">{item.requestId}</code></td><td>{item.revision} / {item.version}</td><td><bdi dir="ltr" lang="en">{item.businessState}</bdi></td><td><bdi dir="ltr" lang="en">{item.executionState}</bdi></td><td><button type="button" className="btn btn-sm btn-outline-primary" disabled={busy} onClick={()=>choose(item)}>{t("Inspect request")}{' '}<bdi dir="ltr" lang="en">{item.requestId}</bdi></button></td></tr>)}</tbody></table></div>}
    {cursor&&<button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>load(signal,true))}>{t("Read next request page")}</button>}
    {selected&&<article aria-label={t("Selected immutable request revision")} className="mt-3">
      <h3 ref={selectedHeading} tabIndex={-1}>{t("Request revision and separate effect")}</h3><dl><dt>{t("Request ID")}</dt><dd><code dir="ltr" lang="en">{selected.requestId}</code></dd><dt>{t("Material revision / CAS version")}</dt><dd>{selected.revision} / {selected.version}</dd>
        <dt>{t("Original requester")}</dt><dd><bdi dir="ltr" lang="en">{selected.requesterPrincipalId}</bdi></dd><dt>{t("Business state")}</dt><dd><bdi dir="ltr" lang="en">{selected.businessState}</bdi></dd><dt>{t("Linked operation")}</dt><dd><code dir="ltr" lang="en">{selected.operationId}</code></dd>
        <dt>{t("Exact effect fingerprint")}</dt><dd><code dir="ltr" lang="en">{selected.previewHash}</code></dd><dt>{t("Material hash")}</dt><dd><code dir="ltr" lang="en">{selected.materialHash}</code></dd><dt>{t("Bound policy digest")}</dt><dd><code dir="ltr" lang="en">{selected.policyDigest}</code></dd>
        <dt>{t("Business expiry (UTC)")}</dt><dd><bdi dir="ltr" lang="en">{selected.expiresAtUtc}</bdi></dd><dt>{t("Effect state / code")}</dt><dd><bdi dir="ltr" lang="en">{selected.executionState}</bdi> / {selected.executionCode??t("None")}</dd></dl>
      <p role="status">{requestBusinessOutcome(selected.businessState)}</p>
      <details><summary>{t("Immutable business decisions")}</summary><pre tabIndex={0} dir="ltr" lang="en" className="automation-proof">{JSON.stringify(selected.decisions,null,2)}</pre></details>
      <div className="d-flex flex-wrap gap-2"><button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(refreshRequest)}>{t("Refresh selected request")}</button>
        <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>readHistory(signal))}>{t("Read immutable request history")}</button>
        <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(inspectLinked)}>{t("Inspect linked effect operation")}</button></div>
      {history.length>0&&<details open><summary>{t("Request history; latest 1000 loaded entries retained")}</summary><pre tabIndex={0} dir="ltr" lang="en" className="automation-proof" aria-label={t("Immutable request history")}>{JSON.stringify(history,null,2)}</pre></details>}
      {historyCursor&&<button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>readHistory(signal,true))}>{t("Read next request history page")}</button>}
      {linked&&<><h4>{t("Underlying effect confirmation and critical approval")}</h4><p>{t("The business decision above is independent from these existing kernel controls. Material-required effects must use their original typed family workflow.")}</p>
        <GovernanceOperationReview operation={linked} material={selected.binding??{operationId:selected.operationId,previewHash:selected.previewHash,materialHash:selected.materialHash,policyDigest:selected.policyDigest}} busy={busy} executionAllowed={selected.businessState.toLowerCase()==='approved'&&Date.parse(selected.expiresAtUtc)>Date.now()} executionBlockReason={t("Refresh this request after a current distinct human approves the exact revision. Business approval and kernel confirmation/critical approval are both required before dispatch.")} update={updateLinked} execute={(status,signal)=>mutationApi.executeWithoutMaterial(status,signal)} refresh={async(status,signal)=>{const current=await mutationApi.get(status.operationId,signal);assertLinkedOperation(selected,current);return current;}} run={run}/></>}
    </article>}
    <fieldset disabled={busy||!loaded} className="mt-3"><legend>{t("Prepare a governed request change")}</legend>
      <button type="button" className="btn btn-outline-secondary" onClick={()=>{setSelected(null);setCommand('Draft');setReason('');setOperationId('');setExpires('');setLinked(null);setHistory([]);setStableKey(crypto.randomUUID());invalidate();}}>{t("Start an intentionally new draft")}</button>
      <label className="form-label">{t("Business command")}<select className="form-select" value={command} onChange={event=>{setCommand(event.target.value as RequestCommand);invalidate();}}>{['Draft','Revise','Submit','Approve','Reject','Cancel'].map(value=><option value={value} key={value}>{t(value)}</option>)}</select></label>
      {command!=='Draft'&&<p>{t("Selected request:")}{' '}<code dir="ltr" lang="en">{selected?.requestId??t("Select a request first")}</code>{t("; expected CAS version:")}{' '}{selected?.version??t("Unknown")}{t(". Each new material revision requires fresh decisions.")}</p>}
      {(command==='Draft'||command==='Revise')&&<><label className="form-label">{t("Existing immutable effect operation ID")}<input dir="ltr" className="form-control" autoComplete="off" value={operationId} onChange={event=>{setOperationId(event.target.value);invalidate();}}/></label>
        <label className="form-label">{t("Request expiry UTC (ISO8601 ending in Z)")}<input dir="ltr" className="form-control" placeholder="2026-11-05T12:00:00Z" value={expires} onChange={event=>{setExpires(event.target.value);invalidate();}}/></label></>}
      <label className="form-label">{t("Business reason")}<input dir="auto" className="form-control" maxLength={2048} value={reason} onChange={event=>{setReason(event.target.value);invalidate();}}/></label>
      <label className="form-label">{t("Stable request change idempotency key")}<input dir="ltr" className="form-control" maxLength={256} value={stableKey} onChange={event=>{setStableKey(event.target.value);invalidate();}}/></label>
      <button type="button" className="btn btn-outline-secondary" disabled={preview?.operation.state==='executionUnknown'||preview?.operation.state==='executing'} onClick={()=>{setStableKey(crypto.randomUUID());invalidate();}}>{t("Create key for an intentionally new request change")}</button>
      <p>{t("Keep the same key and operation ID after an uncertain response. Approve/reject requires a current scoped human distinct from the requester.")}</p>
      <button type="button" className="btn btn-outline-warning" disabled={command!=='Draft'&&!selected} onClick={()=>void run(prepare)}>{t("Preview governed request change")}</button>
    </fieldset>
    <form className="mt-3" onSubmit={event=>{event.preventDefault();void run(signal=>inspect(lookup,signal));}}><label className="form-label">{t("Existing request change operation ID")}<input dir="auto" className="form-control" autoComplete="off" value={lookup} onChange={event=>setLookup(event.target.value)}/></label><button type="submit" className="btn btn-outline-primary" disabled={busy}>{t("Inspect frozen request change")}</button></form>
    {preview&&<GovernanceOperationReview operation={preview.operation} material={preview.plan} busy={busy} update={update} execute={(status,signal)=>mutationApi.executeWithoutMaterial(status,signal)} recover={(status,signal)=>requestApi.recover(status,signal)} refresh={async(status,signal)=>(await requestApi.operation(status.operationId,signal)).operation} run={run}/>}
  </section>;
}
