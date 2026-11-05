import { useEffect, useRef, useState } from 'react';
import { mutationApi, type MutationStatus } from '../mutations/mutationApi.js';
import { GovernanceOperationReview } from './GovernanceOperationReview.js';
import { catalogApi, governanceProblem, parseCatalogImport, scopedOperation, type CatalogChange, type CatalogFamily, type CatalogHistory, type CatalogItem, type CatalogPreview } from './governanceApi.js';

const families: CatalogFamily[] = ['teams', 'applications', 'topics'];
function blank(kind: CatalogFamily): CatalogChange {
  return {kind: kind === 'teams' ? 'Team' : kind === 'applications' ? 'Application' : 'Topic', id: null, expectedRevision: 0,
    name: '', description: null, archived: false, teamId: null, applicationId: null, topicName: null,
    owner: null, domain: null, classification: null, documentationReference: null, tags: []};
}
export function assertCatalogBinding(preview: CatalogPreview, clusterId: string) {
  scopedOperation(preview.operation, clusterId, ['catalogMutation']);
  if (preview.planId !== preview.operation.operationId || !/^[a-f0-9]{64}$/i.test(preview.contentHash) || !preview.changes.length)
    throw new Error('Catalog preview material is incomplete or bound to another operation.');
}
function nullable(value: string) { return value === '' ? null : value; }

export function CatalogPanel({clusterId}: {clusterId: string}) {
  const [family, setFamily] = useState<CatalogFamily>('teams');
  const [items, setItems] = useState<CatalogItem[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [draft, setDraft] = useState<CatalogChange>(blank('teams'));
  const [topicIdentity, setTopicIdentity] = useState<string | null>(null);
  const [history, setHistory] = useState<CatalogHistory[]>([]);
  const [historyCursor, setHistoryCursor] = useState<string | null>(null);
  const [importText, setImportText] = useState('');
  const [key, setKey] = useState('');
  const [lookup, setLookup] = useState('');
  const [preview, setPreview] = useState<CatalogPreview | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const lifetime = useRef<AbortController | null>(null);
  const inFlight = useRef(false);
  const scopeEpoch = useRef(0);
  async function load(signal: AbortSignal, more = false) {
    const page = await catalogApi.list(clusterId, family, more ? cursor : null, signal);
    if (!signal.aborted) { setItems(previous => more ? [...previous, ...page.items].slice(-1000) : page.items); setCursor(page.nextCursor); setLoaded(true); }
  }
  useEffect(() => {
    const controller = new AbortController(); lifetime.current = controller; scopeEpoch.current += 1;
    setItems([]); setCursor(null); setLoaded(false); setPreview(null); setHistory([]); setHistoryCursor(null); setDraft(blank(family)); setTopicIdentity(null); setError(null);
    setKey(crypto.randomUUID()); inFlight.current = false; setBusy(false);
    void load(controller.signal).catch(reason => { if (!controller.signal.aborted) setError(governanceProblem(reason)); });
    return () => controller.abort();
  }, [clusterId, family]);
  async function run(request: (signal: AbortSignal) => Promise<void>) {
    if (!lifetime.current || inFlight.current) return;
    const epoch = scopeEpoch.current; inFlight.current = true; setBusy(true); setError(null);
    const signal = lifetime.current.signal;
    try { await request(signal); } catch (reason) { if (!signal.aborted) setError(governanceProblem(reason)); }
    finally { if (scopeEpoch.current === epoch) { inFlight.current = false; if (!signal.aborted) setBusy(false); } }
  }
  function edit(change: Partial<CatalogChange>) { setDraft(previous => ({...previous, ...change})); setPreview(null); }
  function select(item: CatalogItem) {
    setDraft({kind: item.kind.toLowerCase() === 'team' ? 'Team' : item.kind.toLowerCase() === 'application' ? 'Application' : 'Topic', id: item.id, expectedRevision: item.revision, name: item.name, description: item.description,
      archived: item.archived, teamId: item.teamId, applicationId: item.applicationId, topicName: item.topicName,
      owner: item.owner, domain: item.domain, classification: item.classification, documentationReference: item.documentationReference, tags: item.tags});
    setHistory([]); setHistoryCursor(null); setTopicIdentity(item.topicIdentity); setPreview(null); setError(null);
  }
  async function inspect(id: string, signal: AbortSignal) {
    setPreview(null); const result = await catalogApi.operation(id, signal); assertCatalogBinding(result, clusterId);
    if (!signal.aborted) { setLookup(result.operation.operationId); setPreview(result); }
  }
  function update(operation: MutationStatus) {
    if (!preview || operation.operationId !== preview.operation.operationId || operation.previewHash !== preview.operation.previewHash)
      throw new Error('Reload the frozen catalog plan before another decision.');
    const next = {...preview, operation}; assertCatalogBinding(next, clusterId); setPreview(next);
  }
  async function readHistory(signal: AbortSignal, more = false) {
    if (!draft.id) return;
    const result = await catalogApi.history(clusterId, family, draft.id, more ? historyCursor : null, signal);
    if (!signal.aborted) { setHistory(previous => more ? [...previous, ...result.items].slice(-1000) : result.items); setHistoryCursor(result.nextCursor); }
  }
  return <section className="card kafdeck-card" id="ownership-catalog" aria-labelledby="ownership-catalog-title" aria-busy={busy}>
    <h2 id="ownership-catalog-title" className="card-title">Ownership catalog</h2>
    <p>Scope: <strong>{clusterId}</strong>. Descriptive teams, applications and topic owners do not grant RBAC or declarative ownership. Historical references are archived, never silently reassigned.</p>
    {error && <p className="alert alert-warning" role="alert">{error}</p>}
    <label className="form-label">Catalog family<select className="form-select" value={family} disabled={busy} onChange={event => setFamily(event.target.value as CatalogFamily)}>{families.map(value => <option key={value}>{value}</option>)}</select></label>
    <button className="btn btn-outline-primary" type="button" disabled={busy} onClick={() => void run(signal => load(signal))}>Reload authorized catalog</button>
    {loaded && items.length === 0 && <p role="status">No authorized entries in this page.</p>}
    {items.length > 0 && <div className="table-responsive"><table className="table"><caption>Authorized {family}; latest 1000 loaded entries retained</caption><thead><tr><th scope="col">Name / ID</th><th scope="col">Revision</th><th scope="col">State</th><th scope="col">Action</th></tr></thead><tbody>{items.map(item => <tr key={item.id}>
      <td>{item.name}<br /><code>{item.id}</code></td><td>{item.revision}</td><td>{item.archived ? 'Archived' : 'Active'}{item.kind.toLowerCase() === 'topic' && <p>{item.topicIdentityCurrent === true ? 'Kafka identity current' : item.topicIdentityCurrent === false ? 'Kafka identity changed — historical assignment only; explicit rebind required' : 'Kafka identity unavailable — current ownership is not inferred'}</p>}</td><td><button className="btn btn-outline-primary" type="button" disabled={busy} onClick={() => select(item)}>Inspect {item.name}</button></td>
    </tr>)}</tbody></table></div>}
    {cursor && <button className="btn btn-outline-primary" type="button" disabled={busy} onClick={() => void run(signal => load(signal, true))}>Read next catalog page</button>}
    <fieldset className="mt-3" disabled={busy || !loaded}><legend>Governed metadata change</legend>
      <button className="btn btn-outline-primary" type="button" onClick={() => { setDraft(blank(family)); setTopicIdentity(null); setHistory([]); setHistoryCursor(null); setPreview(null); }}>Stage new {family === 'teams' ? 'team' : family === 'applications' ? 'application' : 'topic assignment'}</button>
      <p>Record ID: <code>{draft.id ?? 'Server generates an ID at preview'}</code>; expected revision: {draft.expectedRevision}.</p>
      <div className="row g-2">
        <label className="col-md-6 form-label">Name<input className="form-control" maxLength={128} value={draft.name} onChange={event => edit({name: event.target.value})} /></label>
        <label className="col-md-6 form-label">Description<input className="form-control" maxLength={2048} value={draft.description ?? ''} onChange={event => edit({description: nullable(event.target.value)})} /></label>
        {family !== 'teams' && <label className="col-md-6 form-label">Exact team ID (optional)<input className="form-control" maxLength={32} value={draft.teamId ?? ''} onChange={event => edit({teamId: nullable(event.target.value)})} /></label>}
        {family === 'topics' && <>
          <label className="col-md-6 form-label">Exact existing topic name<input className="form-control" maxLength={249} value={draft.topicName ?? ''} onChange={event => edit({topicName: nullable(event.target.value)})} /></label>
          <label className="col-md-6 form-label">Exact application ID (optional)<input className="form-control" maxLength={32} value={draft.applicationId ?? ''} onChange={event => edit({applicationId: nullable(event.target.value)})} /></label>
          <label className="col-md-6 form-label">Descriptive owner<input className="form-control" maxLength={128} value={draft.owner ?? ''} onChange={event => edit({owner: nullable(event.target.value)})} /></label>
          <label className="col-md-6 form-label">Domain<input className="form-control" maxLength={128} value={draft.domain ?? ''} onChange={event => edit({domain: nullable(event.target.value)})} /></label>
          <label className="col-md-6 form-label">Classification<input className="form-control" maxLength={128} value={draft.classification ?? ''} onChange={event => edit({classification: nullable(event.target.value)})} /></label>
          <label className="col-md-6 form-label">Documentation reference (HTTPS, never server-fetched)<input className="form-control" maxLength={2048} value={draft.documentationReference ?? ''} onChange={event => edit({documentationReference: nullable(event.target.value)})} /></label>
          <label className="col-md-6 form-label">Tags (one per line, maximum 20)<textarea className="form-control" rows={3} value={draft.tags.join('\n')} onChange={event => edit({tags: event.target.value.split('\n').filter(Boolean)})} /></label>
        </>}
      </div>
      <label className="form-check"><input className="form-check-input" type="checkbox" checked={draft.archived} disabled={draft.id === null} onChange={event => edit({archived: event.target.checked})} /><span className="form-check-label">Archive this historical record</span></label>
      {family === 'topics' && draft.id && <><p>Recorded Kafka identity: <code>{topicIdentity ?? 'Not observed'}</code>. A recreated topic does not inherit this assignment.</p>
        <label className="form-check"><input className="form-check-input" type="checkbox" checked={draft.rebindTopicIdentity === true} onChange={event => edit({rebindTopicIdentity: event.target.checked})} /><span className="form-check-label">Explicitly rebind metadata to the newly server-observed Kafka topic identity</span></label></>}
      <label className="form-label">Stable request idempotency key<input className="form-control" value={key} maxLength={256} onChange={event => { setKey(event.target.value); setPreview(null); }} /></label>
      <button className="btn btn-outline-secondary" type="button" disabled={preview?.operation.state === 'executionUnknown' || preview?.operation.state === 'executing'} onClick={() => { setKey(crypto.randomUUID()); setPreview(null); }}>Create key for an intentionally new catalog request</button>
      <p>Retain the key and operation after an uncertain response. Editing material requires an explicit fresh preview; no approval transfers to changed material.</p>
      <button className="btn btn-outline-warning" type="button" onClick={() => void run(async signal => { const result = await catalogApi.preview(clusterId, family, [draft], key, signal); assertCatalogBinding(result, clusterId); if (!signal.aborted) { setLookup(result.operation.operationId); setPreview(result); } })}>Preview exact metadata change</button>
      {draft.id && <button className="btn btn-outline-primary" type="button" onClick={() => void run(signal => readHistory(signal))}>Read immutable record history</button>}
    </fieldset>
    {history.length > 0 && <div className="table-responsive"><table className="table"><caption>Immutable revisions; latest 1000 loaded history entries retained</caption><thead><tr><th scope="col">Revision</th><th scope="col">Effective interval (UTC)</th><th scope="col">Actor / operation</th><th scope="col">Material</th></tr></thead><tbody>{history.map(item => <tr key={item.record.revision}>
      <td>{item.record.revision}</td><td>{item.effectiveFromUtc} → {item.effectiveToUtc ?? 'Current'}</td><td>{item.actorFingerprint}<br /><code>{item.operationId}</code></td><td><pre className="automation-proof">{JSON.stringify(item.record, null, 2)}</pre></td>
    </tr>)}</tbody></table>{historyCursor && <button className="btn btn-outline-primary" type="button" disabled={busy} onClick={() => void run(signal => readHistory(signal, true))}>Read next history page</button>}</div>}
    <fieldset className="mt-3" disabled={busy || !loaded}><legend>Bounded atomic catalog import</legend>
      <p>Supply a JSON array of typed Team/Application/Topic changes (maximum 500 changes / 1 MiB). Inspect every frozen change before confirmation; a conflict rejects the atomic commit.</p>
      <label className="form-label">Typed catalog changes JSON<textarea className="form-control" value={importText} rows={6} spellCheck={false} onChange={event => { setImportText(event.target.value); setPreview(null); }} /></label>
      <button className="btn btn-outline-warning" type="button" onClick={() => void run(async signal => { const changes = parseCatalogImport(importText); const result = await catalogApi.preview(clusterId, 'imports', changes, key, signal); assertCatalogBinding(result, clusterId); if (!signal.aborted) { setLookup(result.operation.operationId); setPreview(result); } })}>Preview atomic import</button>
    </fieldset>
    <form className="mt-3" onSubmit={event => { event.preventDefault(); void run(signal => inspect(lookup, signal)); }}>
      <label className="form-label">Existing catalog operation ID<input className="form-control" value={lookup} onChange={event => setLookup(event.target.value)} autoComplete="off" /></label>
      <button className="btn btn-outline-primary" type="submit" disabled={busy}>Inspect frozen catalog operation</button>
    </form>
    {preview && <GovernanceOperationReview operation={preview.operation} material={{contentHash: preview.contentHash, changes: preview.changes}} busy={busy} update={update} execute={(operation, signal) => catalogApi.execute(operation, signal)} recover={(operation, signal) => catalogApi.recover(operation, signal)} refresh={async (operation, signal) => (await catalogApi.operation(operation.operationId, signal)).operation} run={run} />}
  </section>;
}
