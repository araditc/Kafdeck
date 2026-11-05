import { useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';
import { GovernanceOperationReview } from './GovernanceOperationReview.js';
import { requestApi, assertRequestOperation, assertLinkedOperation, type RequestCommand, type RequestHistory, type RequestOperation, type ServiceRequest } from './workflowApi.js';
import { useGovernanceSession } from './useGovernanceSession.js';

export function requestBusinessOutcome(state: string) {
  switch(state.toLowerCase()) {
    case 'approved':return 'Business approval is recorded. The linked effect still needs current permissions, confirmation and any independent critical approval.';
    case 'needsreplan':return 'Material, policy or generation changed. Prepare a new operation and an explicit request revision; previous decisions cannot be reused.';
    case 'expired':return 'This revision expired. Replan explicitly; an expired decision cannot authorize an effect.';
    case 'rejected':return 'An eligible human rejected this revision. Its previous approval cannot authorize execution.';
    case 'cancelled':return 'Undispatched business work was cancelled. This does not assert rollback of a dispatched or unknown effect.';
    case 'submitted':return 'A current scoped human distinct from the requester must decide this exact revision.';
    default:return 'A draft binds an existing immutable operation. Submission and business decisions are separate governed changes.';
  }
}
export function RequestPanel({clusterId}:{clusterId:string}) {
  const [items,setItems]=useState<ServiceRequest[]>([]),[cursor,setCursor]=useState<string|null>(null),[selected,setSelected]=useState<ServiceRequest|null>(null);
  const [history,setHistory]=useState<RequestHistory[]>([]),[historyCursor,setHistoryCursor]=useState<string|null>(null);
  const [loaded,setLoaded]=useState(false),[command,setCommand]=useState<RequestCommand>('Draft'),[operationId,setOperationId]=useState('');
  const [reason,setReason]=useState(''),[expires,setExpires]=useState(''),[stableKey,setStableKey]=useState(''),[lookup,setLookup]=useState('');
  const [preview,setPreview]=useState<RequestOperation|null>(null),[linked,setLinked]=useState<MutationStatus|null>(null);
  async function load(signal:AbortSignal,more=false){const page=await requestApi.list(clusterId,more?cursor:null,signal);if(!signal.aborted){setItems(previous=>more?[...previous,...page.items].slice(-1000):page.items);setCursor(page.nextCursor);setLoaded(true);}}
  const {busy,error,run}=useGovernanceSession(clusterId,()=>{setItems([]);setCursor(null);setSelected(null);setHistory([]);setHistoryCursor(null);setLoaded(false);setCommand('Draft');setOperationId('');setReason('');setExpires('');setPreview(null);setLinked(null);setStableKey(crypto.randomUUID());setLookup('');},load);
  function invalidate(){setPreview(null);}
  function choose(request:ServiceRequest){setSelected(request);setCommand('Submit');setOperationId(request.operationId);setReason(request.reason);setExpires(request.expiresAtUtc);setHistory([]);setHistoryCursor(null);setLinked(null);invalidate();setStableKey(crypto.randomUUID());}
  async function refreshRequest(signal:AbortSignal){if(!selected)return;const value=await requestApi.get(clusterId,selected.requestId,signal);if(!signal.aborted){setSelected(value);setLinked(null);}}
  async function inspect(id:string,signal:AbortSignal){setPreview(null);const value=await requestApi.operation(id,signal);assertRequestOperation(value,clusterId);if(!signal.aborted){setPreview(value);setLookup(id);}}
  async function prepare(signal:AbortSignal){const status=await requestApi.preview({clusterId,command,expectedVersion:command==='Draft'?0:selected?.version??0,
    ...(command!=='Draft'&&selected?{requestId:selected.requestId}:{}),...(command==='Draft'||command==='Revise'?{operationId,expiresAtUtc:expires}:{}),reason},stableKey,signal);if(!signal.aborted)await inspect(status.operationId,signal);}
  function update(status:MutationStatus){if(!preview)throw new Error('Reload frozen request material.');const next={...preview,operation:status};assertRequestOperation(next,clusterId);setPreview(next);}
  async function readHistory(signal:AbortSignal,more=false){if(!selected)return;const page=await requestApi.history(clusterId,selected.requestId,more?historyCursor:null,signal);if(!signal.aborted){setHistory(previous=>more?[...previous,...page.items].slice(-1000):page.items);setHistoryCursor(page.nextCursor);}}
  async function inspectLinked(signal:AbortSignal){if(!selected)return;const status=await mutationApi.get(selected.operationId,signal);assertLinkedOperation(selected,status);if(!signal.aborted)setLinked(status);}
  function updateLinked(status:MutationStatus){if(!selected)throw new Error('Select the immutable request first.');assertLinkedOperation(selected,status);setLinked(status);}
  return <section id="service-requests" className="card kafdeck-card" aria-labelledby="service-requests-title" aria-busy={busy}>
    <h2 id="service-requests-title" className="card-title">Service requests and approvals</h2>
    <p>Scope: <strong>{clusterId}</strong>. Business approval binds one immutable operation and revision. It grants no permissions and does not replace confirmation or independent critical approval.</p>
    {error&&<p role="alert" className="alert alert-warning">{error}</p>}
    <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>load(signal))}>Reload authorized requests</button>
    {loaded&&items.length===0&&<p role="status">No authorized service requests in this page.</p>}
    {items.length>0&&<div className="table-responsive"><table className="table" aria-label="Authorized requests; latest 1000 loaded records retained"><thead><tr><th>Request</th><th>Material revision / CAS version</th><th>Business state</th><th>Linked effect state</th><th>Review</th></tr></thead><tbody>{items.map(item=><tr key={item.requestId}><td><code>{item.requestId}</code></td><td>{item.revision} / {item.version}</td><td>{item.businessState}</td><td>{item.executionState}</td><td><button type="button" className="btn btn-sm btn-outline-primary" disabled={busy} onClick={()=>choose(item)}>Inspect request {item.requestId}</button></td></tr>)}</tbody></table></div>}
    {cursor&&<button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>load(signal,true))}>Read next request page</button>}
    {selected&&<article aria-label="Selected immutable request revision" className="mt-3">
      <h3>Request revision and separate effect</h3><dl><dt>Request ID</dt><dd><code>{selected.requestId}</code></dd><dt>Material revision / CAS version</dt><dd>{selected.revision} / {selected.version}</dd>
        <dt>Original requester</dt><dd>{selected.requesterPrincipalId}</dd><dt>Business state</dt><dd>{selected.businessState}</dd><dt>Linked operation</dt><dd><code>{selected.operationId}</code></dd>
        <dt>Exact effect fingerprint</dt><dd><code>{selected.previewHash}</code></dd><dt>Material hash</dt><dd><code>{selected.materialHash}</code></dd><dt>Bound policy digest</dt><dd><code>{selected.policyDigest}</code></dd>
        <dt>Business expiry (UTC)</dt><dd>{selected.expiresAtUtc}</dd><dt>Effect state / code</dt><dd>{selected.executionState} / {selected.executionCode??'None'}</dd></dl>
      <p role="status">{requestBusinessOutcome(selected.businessState)}</p>
      <details><summary>Immutable business decisions</summary><pre className="automation-proof">{JSON.stringify(selected.decisions,null,2)}</pre></details>
      <div className="d-flex flex-wrap gap-2"><button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(refreshRequest)}>Refresh selected request</button>
        <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>readHistory(signal))}>Read immutable request history</button>
        <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(inspectLinked)}>Inspect linked effect operation</button></div>
      {history.length>0&&<details open><summary>Request history; latest 1000 loaded entries retained</summary><pre className="automation-proof" aria-label="Immutable request history">{JSON.stringify(history,null,2)}</pre></details>}
      {historyCursor&&<button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>readHistory(signal,true))}>Read next request history page</button>}
      {linked&&<><h4>Underlying effect confirmation and critical approval</h4><p>The business decision above is independent from these existing kernel controls. Material-required effects must use their original typed family workflow.</p>
        <GovernanceOperationReview operation={linked} material={selected.binding??{operationId:selected.operationId,previewHash:selected.previewHash,materialHash:selected.materialHash,policyDigest:selected.policyDigest}} busy={busy} executionAllowed={selected.businessState.toLowerCase()==='approved'&&Date.parse(selected.expiresAtUtc)>Date.now()} executionBlockReason="Refresh this request after a current distinct human approves the exact revision. Business approval and kernel confirmation/critical approval are both required before dispatch." update={updateLinked} execute={(status,signal)=>mutationApi.executeWithoutMaterial(status,signal)} refresh={async(status,signal)=>{const current=await mutationApi.get(status.operationId,signal);assertLinkedOperation(selected,current);return current;}} run={run}/></>}
    </article>}
    <fieldset disabled={busy||!loaded} className="mt-3"><legend>Prepare a governed request change</legend>
      <button type="button" className="btn btn-outline-secondary" onClick={()=>{setSelected(null);setCommand('Draft');setReason('');setOperationId('');setExpires('');setLinked(null);setHistory([]);setStableKey(crypto.randomUUID());invalidate();}}>Start an intentionally new draft</button>
      <label className="form-label">Business command<select className="form-select" value={command} onChange={event=>{setCommand(event.target.value as RequestCommand);invalidate();}}>{['Draft','Revise','Submit','Approve','Reject','Cancel'].map(value=><option key={value}>{value}</option>)}</select></label>
      {command!=='Draft'&&<p>Selected request: <code>{selected?.requestId??'Select a request first'}</code>; expected CAS version: {selected?.version??'Unknown'}. Each new material revision requires fresh decisions.</p>}
      {(command==='Draft'||command==='Revise')&&<><label className="form-label">Existing immutable effect operation ID<input className="form-control" autoComplete="off" value={operationId} onChange={event=>{setOperationId(event.target.value);invalidate();}}/></label>
        <label className="form-label">Request expiry UTC (ISO8601 ending in Z)<input className="form-control" placeholder="2026-11-05T12:00:00Z" value={expires} onChange={event=>{setExpires(event.target.value);invalidate();}}/></label></>}
      <label className="form-label">Business reason<input className="form-control" maxLength={2048} value={reason} onChange={event=>{setReason(event.target.value);invalidate();}}/></label>
      <label className="form-label">Stable request change idempotency key<input className="form-control" maxLength={256} value={stableKey} onChange={event=>{setStableKey(event.target.value);invalidate();}}/></label>
      <button type="button" className="btn btn-outline-secondary" disabled={preview?.operation.state==='executionUnknown'||preview?.operation.state==='executing'} onClick={()=>{setStableKey(crypto.randomUUID());invalidate();}}>Create key for an intentionally new request change</button>
      <p>Keep the same key and operation ID after an uncertain response. Approve/reject requires a current scoped human distinct from the requester.</p>
      <button type="button" className="btn btn-outline-warning" disabled={command!=='Draft'&&!selected} onClick={()=>void run(prepare)}>Preview governed request change</button>
    </fieldset>
    <form className="mt-3" onSubmit={event=>{event.preventDefault();void run(signal=>inspect(lookup,signal));}}><label className="form-label">Existing request change operation ID<input className="form-control" autoComplete="off" value={lookup} onChange={event=>setLookup(event.target.value)}/></label><button type="submit" className="btn btn-outline-primary" disabled={busy}>Inspect frozen request change</button></form>
    {preview&&<GovernanceOperationReview operation={preview.operation} material={preview.plan} busy={busy} update={update} execute={(status,signal)=>mutationApi.executeWithoutMaterial(status,signal)} recover={(status,signal)=>requestApi.recover(status,signal)} refresh={async(status,signal)=>(await requestApi.operation(status.operationId,signal)).operation} run={run}/>}
  </section>;
}
