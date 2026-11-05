import { useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';
import { GovernanceOperationReview } from './GovernanceOperationReview.js';
import { attributionApi, assertRateCardOperation, decimalEvidence, measurementOf, parseWorkflowJson, type MeasuredKind, type MeasurementPage, type RateCardDocument, type RateCardOperation, type RateCardState, type RateCardVersion } from './workflowApi.js';
import { useGovernanceSession } from './useGovernanceSession.js';

function defaultInterval(){const end=Math.floor(Date.now()/60_000)*60_000;return {from:new Date(end-3_600_000).toISOString(),to:new Date(end).toISOString()};}
export function AttributionPanel({clusterId}:{clusterId:string}) {
  const [from,setFrom]=useState(()=>defaultInterval().from),[to,setTo]=useState(()=>defaultInterval().to),[resource,setResource]=useState('');
  const [family,setFamily]=useState<'measurements'|'attribution'|'dashboards'>('dashboards'),[page,setPage]=useState<MeasurementPage|null>(null);
  const [versions,setVersions]=useState<RateCardVersion[]>([]),[cursor,setCursor]=useState<string|null>(null),[state,setState]=useState<RateCardState|null>(null);
  const [loaded,setLoaded]=useState(false),[metric,setMetric]=useState<MeasuredKind>('trafficBytes'),[currency,setCurrency]=useState('USD'),[amount,setAmount]=useState('');
  const [rateFrom,setRateFrom]=useState(''),[rateTo,setRateTo]=useState(''),[weights,setWeights]=useState('[]'),[selectedVersion,setSelectedVersion]=useState(''),[enabled,setEnabled]=useState(true);
  const [stableKey,setStableKey]=useState(''),[lookup,setLookup]=useState(''),[review,setReview]=useState<RateCardOperation|null>(null);
  const query=()=>({clusterId,fromUtc:from,toUtc:to,...(resource?{resourceId:resource}:{})});
  async function loadRates(signal:AbortSignal,more=false){const [values,active]=await Promise.all([attributionApi.versions(clusterId,more?cursor:null,signal),attributionApi.state(clusterId,signal)]);if(!signal.aborted){setVersions(previous=>more?[...previous,...values.items].slice(-1000):values.items);setCursor(values.nextCursor);setState(active);setLoaded(true);}}
  async function read(signal:AbortSignal,more=false){const value=await attributionApi.windows(family,query(),more?page?.nextCursor:null,signal);if(!signal.aborted)setPage(value);}
  const {busy,error,run}=useGovernanceSession(clusterId,()=>{const interval=defaultInterval();setFrom(interval.from);setTo(interval.to);setResource('');setFamily('dashboards');setPage(null);setVersions([]);setCursor(null);setState(null);setLoaded(false);setReview(null);setLookup('');setSelectedVersion('');setMetric('trafficBytes');setCurrency('USD');setAmount('');setRateFrom('');setRateTo('');setWeights('[]');setEnabled(true);setStableKey(crypto.randomUUID());},async signal=>{const interval=defaultInterval();await Promise.all([loadRates(signal),attributionApi.windows('dashboards',{clusterId,fromUtc:interval.from,toUtc:interval.to},null,signal).then(value=>{if(!signal.aborted)setPage(value);})]);});
  function invalidate(){setReview(null);}
  function invalidateQuery(){setPage(null);}
  async function inspect(id:string,signal:AbortSignal){setReview(null);const value=await attributionApi.operation(id,signal);assertRateCardOperation(value,clusterId);if(!signal.aborted){setLookup(id);setReview(value);}}
  function update(operation:MutationStatus){if(!review)throw new Error('Reload immutable rate-card material.');const value={...review,operation};assertRateCardOperation(value,clusterId);setReview(value);}
  function document():RateCardDocument{return {metricKind:metric,unit:metric==='trafficBytes'?'bytes':'byteSeconds',currency,amount,effectiveFromUtc:rateFrom,effectiveToUtc:rateTo,weights:parseWorkflowJson(weights) as RateCardDocument['weights']};}
  return <section id="governance-attribution" className="card kafdeck-card" aria-labelledby="governance-attribution-title" aria-busy={busy}>
    <h2 id="governance-attribution-title" className="card-title">Measured attribution and estimates</h2>
    <p>Scope: <strong>{clusterId}</strong>. Registered measurements retain units, time coverage, source provenance and ownership at measurement time. Missing evidence is unknown. Shared configured allocation is estimated; residual units remain explicit.</p>
    {error&&<p role="alert" className="alert alert-warning">{error}</p>}
    <fieldset disabled={busy}><legend>Bounded measured interval</legend>
      <label className="form-label">Measurement view<select className="form-select" value={family} onChange={event=>{setFamily(event.target.value as typeof family);invalidateQuery();}}>{['dashboards','attribution','measurements'].map(value=><option key={value}>{value}</option>)}</select></label>
      <label className="form-label">From UTC<input className="form-control" value={from} onChange={event=>{setFrom(event.target.value);invalidateQuery();}}/></label>
      <label className="form-label">To UTC<input className="form-control" value={to} onChange={event=>{setTo(event.target.value);invalidateQuery();}}/></label>
      <label className="form-label">Exact resource ID (optional)<input className="form-control" maxLength={512} value={resource} onChange={event=>{setResource(event.target.value);invalidateQuery();}}/></label>
      <p>At most 31 days per query, minimum one-minute interval. Each page is scoped; continuation is explicit and totals state their coverage.</p>
      <button type="button" className="btn btn-outline-primary" onClick={()=>void run(signal=>read(signal))}>Read measured attribution</button>
    </fieldset>
    {page&&<article aria-label="Measured attribution result" className="mt-3"><h3>Measurement quality and coverage</h3>
      <dl><dt>Status</dt><dd>{page.status}</dd><dt>Coverage</dt><dd>{page.coverage}</dd><dt>Actual interval (UTC)</dt><dd>{page.fromUtc} → {page.toUtc}</dd>
        <dt>Total estimate status</dt><dd>{page.totals?.status??'Unknown'}</dd><dt>Complete total</dt><dd>{page.totals?.complete?'Yes':'No; a page or incomplete evidence is not a full estimate'}</dd>
        <dt>Currency</dt><dd>{page.totals?.currency??'Unknown'}</dd><dt>Rounded total</dt><dd>{decimalEvidence(page.totals?.roundedAmount)}</dd><dt>Unrounded decimal evidence</dt><dd>{decimalEvidence(page.totals?.unroundedAmount)}</dd></dl>
      <p>Amounts use the backend's invariant decimals and midpoint-to-even rounding. No browser floating-point calculation, currency conversion, payment or billing transaction is performed.</p>
      {page.items.length===0&&<p role="status">No admitted measurements in this page. An empty result is not a zero bill.</p>}
      {page.items.length>0&&<div className="table-responsive"><table className="table" aria-label="Scoped measured windows and allocation provenance"><thead><tr><th>Resource / window UTC</th><th>Measured quantity / unit</th><th>Quality / coverage reasons</th><th>Ownership at measurement</th><th>Estimate / residual / provenance</th></tr></thead><tbody>{page.items.map(item=>{const m=measurementOf(item),a='measurement' in item?item:null;return <tr key={m.windowId}><td>{m.resourceId}<br/>{m.windowStartUtc} → {m.windowEndUtc}</td><td>{decimalEvidence(m.quantity)} {m.unit}<br/>{m.metricKind}</td><td>{a?.quality??m.quality}<br/>{[...new Set([...m.reasons,...a?.reasons??[]])].join('; ')||'No quality reasons reported'}</td><td>Revision {m.ownership.revision??'Unknown'}<br/>Team {m.ownership.teamId??'Unknown'}<br/>Application {m.ownership.applicationId??'Unknown'}<br/>{m.ownership.quality}</td><td>{a?<><span>{decimalEvidence(a.roundedAmount)} {a.currency??'Unknown currency'}</span><br/>Residual {decimalEvidence(a.residualQuantity)} {m.unit}<details><summary>Allocation and decimal evidence</summary><pre className="automation-proof">{JSON.stringify({allocations:a.allocations,unroundedAmount:a.unroundedAmount,rateCardHash:a.rateCardHash},null,2)}</pre></details></>:'Raw measurement; no cost estimate inferred'}<details><summary>Source provenance</summary><dl><dt>Provider / source</dt><dd>{m.provider} / {m.sourceId}</dd><dt>Collected UTC</dt><dd>{m.collectedAtUtc}</dd><dt>Provenance hash</dt><dd><code>{m.provenanceHash}</code></dd><dt>Window ID</dt><dd><code>{m.windowId}</code></dd></dl></details></td></tr>;})}</tbody></table></div>}
      {page.nextCursor&&<button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>read(signal,true))}>Read next measured window page</button>}
    </article>}
    <h3 className="mt-3">Versioned rate cards and explicit allocation</h3>
    <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={()=>void run(signal=>loadRates(signal))}>Reload authorized rate cards</button>
    {state&&<p>Current activation revision: {state.revision}; enabled: {String(state.enabled)}; version: <code>{state.versionId??'None'}</code>.</p>}
    <fieldset disabled={busy||!loaded} className="mt-3"><legend>Prepare a governed high-risk rate-card version</legend>
      <label className="form-label">Registered measured metric<select className="form-select" value={metric} onChange={event=>{setMetric(event.target.value as MeasuredKind);invalidate();}}><option value="trafficBytes">Traffic bytes / bytes</option><option value="storageByteSeconds">Storage byte-seconds / byteSeconds</option></select></label>
      <label className="form-label">Currency code<input className="form-control" maxLength={3} value={currency} onChange={event=>{setCurrency(event.target.value);invalidate();}}/></label>
      <label className="form-label">Decimal amount per exact measured unit<input className="form-control" inputMode="decimal" maxLength={64} value={amount} onChange={event=>{setAmount(event.target.value);invalidate();}}/></label>
      <label className="form-label">Rate effective from UTC<input className="form-control" value={rateFrom} onChange={event=>{setRateFrom(event.target.value);invalidate();}}/></label>
      <label className="form-label">Rate effective to UTC<input className="form-control" value={rateTo} onChange={event=>{setRateTo(event.target.value);invalidate();}}/></label>
      <label className="form-label">Explicit catalog allocation weights (JSON; maximum 100)<textarea className="form-control" rows={4} value={weights} onChange={event=>{setWeights(event.target.value);invalidate();}} spellCheck={false}/></label>
      <p>Each weight names an existing catalog team/application and an invariant decimal fraction. No equal split is invented. Allocations plus residual preserve admitted measured units.</p>
      <label className="form-label">Stable rate-card idempotency key<input className="form-control" maxLength={256} value={stableKey} onChange={event=>{setStableKey(event.target.value);invalidate();}}/></label>
      <button type="button" className="btn btn-outline-warning" onClick={()=>void run(async signal=>{const value=await attributionApi.previewVersion(clusterId,document(),stableKey,signal);if(!signal.aborted)await inspect(value.operationId,signal);})}>Preview governed rate-card version</button>
      <button type="button" className="btn btn-outline-secondary" disabled={review?.operation.state==='executionUnknown'||review?.operation.state==='executing'} onClick={()=>{setStableKey(crypto.randomUUID());invalidate();}}>Create key for an intentionally new rate-card change</button>
    </fieldset>
    <fieldset disabled={busy||!loaded} className="mt-3"><legend>Review versions and activate with current CAS revision</legend>
      <label className="form-label">Immutable rate-card version<select className="form-select" value={selectedVersion} onChange={event=>{setSelectedVersion(event.target.value);invalidate();}}><option value="">Select a version</option>{versions.map(value=><option key={value.versionId} value={value.versionId}>{value.versionId} / {value.contentHash}</option>)}</select></label>
      {versions.find(value=>value.versionId===selectedVersion)&&<details><summary>Selected immutable rate-card document</summary><pre className="automation-proof">{JSON.stringify(versions.find(value=>value.versionId===selectedVersion)?.document,null,2)}</pre></details>}
      <label className="form-check"><input type="checkbox" className="form-check-input" checked={enabled} onChange={event=>{setEnabled(event.target.checked);invalidate();}}/><span className="form-check-label">Enable selected rate-card interval (uncheck for governed deactivation)</span></label>
      <button type="button" className="btn btn-outline-warning" disabled={!selectedVersion||!state} onClick={()=>void run(async signal=>{if(!state)return;const value=await attributionApi.previewActivation(clusterId,selectedVersion,state.revision,enabled,stableKey,signal);if(!signal.aborted)await inspect(value.operationId,signal);})}>Preview governed rate-card activation</button>
      {cursor&&<button type="button" className="btn btn-outline-primary" onClick={()=>void run(signal=>loadRates(signal,true))}>Read next rate-card version page</button>}
    </fieldset>
    <form className="mt-3" onSubmit={event=>{event.preventDefault();void run(signal=>inspect(lookup,signal));}}><label className="form-label">Existing rate-card operation ID<input className="form-control" value={lookup} autoComplete="off" onChange={event=>setLookup(event.target.value)}/></label><button type="submit" className="btn btn-outline-primary" disabled={busy}>Inspect frozen rate-card operation</button></form>
    {review&&<GovernanceOperationReview operation={review.operation} material={{clusterId:review.clusterId,versionId:review.versionId,contentHash:review.contentHash,document:review.document,expectedRevision:review.expectedRevision,enabled:review.enabled}} busy={busy} update={update} execute={(status,signal)=>mutationApi.executeWithoutMaterial(status,signal)} recover={(status,signal)=>attributionApi.recover(status,signal)} refresh={async(status,signal)=>(await attributionApi.operation(status.operationId,signal)).operation} run={run}/>}
  </section>;
}
