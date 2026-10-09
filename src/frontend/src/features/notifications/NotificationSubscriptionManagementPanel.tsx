import { useEffect, useRef, useState, type FormEvent } from 'react';
import {
  ApiProblem, kafdeckApi,
  type NotificationSubscriptionData,
  type NotificationSubscriptionWriteRequest,
  type NotificationSubscriptionWriteReceipt,
} from '../../shared/api.js';

interface Props {
  referenceSubscription: NotificationSubscriptionData | null;
  onUpdated: () => void;
}
const eventClassOptions = ['operational', 'mutation', 'security', 'slo', 'dataQuality'] as const;
type EventClass = typeof eventClassOptions[number];
type SubscriptionState = 'active' | 'paused' | 'retired';
type Mode = 'create' | 'replace';

export function isValidNotificationDestinationId(value: string): boolean {
  return /^[A-Za-z0-9._-]{1,128}$/.test(value);
}

function manageFailure(reason: unknown): string {
  if (reason instanceof ApiProblem && reason.status === 401)
    return 'Operator session expired; re-authenticate before any further change.';
  if (reason instanceof ApiProblem && reason.status === 403)
    return 'Subscription management is not authorized for this subscription/destination.';
  if (reason instanceof ApiProblem && reason.status === 404)
    return 'Management is disabled or this subscription is no longer accessible.';
  if (reason instanceof ApiProblem && reason.status === 409)
    return 'Subscription conflict: reload authorized current state and re-enter its CAS revision.';
  return 'Management request was not confirmed. Inspect the current state before retrying.';
}

export function NotificationSubscriptionManagementPanel({ referenceSubscription, onUpdated }: Props) {
  const [mode, setMode] = useState<Mode>('create');
  const [subscriptionId, setSubscriptionId] = useState('');
  const [destinationId, setDestinationId] = useState('');
  const [state, setState] = useState<SubscriptionState>('active');
  const [eventClasses, setEventClasses] = useState<EventClass[]>(['operational']);
  const [eventTypes, setEventTypes] = useState('');
  const [revision, setRevision] = useState('');
  const [prepared, setPrepared] = useState<{
    identity: string; payload: NotificationSubscriptionWriteRequest; snapshot: string;
  } | null>(null);
  const [receipt, setReceipt] = useState<NotificationSubscriptionWriteReceipt | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const inFlight = useRef<AbortController | null>(null);
  const submitting = useRef(false);

  useEffect(() => () => { inFlight.current?.abort(); }, []);
  useEffect(() => {
    if (!referenceSubscription) return;
    inFlight.current?.abort();
    setMode('replace');
    setSubscriptionId(referenceSubscription.subscriptionId);
    setDestinationId(referenceSubscription.destinationId);
    const next = referenceSubscription.state.toLowerCase();
    setState(next === 'retired' ? 'retired' : next === 'paused' ? 'paused' : 'active');
    setEventClasses(referenceSubscription.eventClasses.flatMap(value => {
      const found = eventClassOptions.find(option => option.toLowerCase() === value.toLowerCase());
      return found ? [found] : [];
    }));
    setEventTypes(referenceSubscription.eventTypes.join('\n'));
    setRevision(String(referenceSubscription.revision));
    setPrepared(null);
    setReceipt(null);
    setError(null);
  }, [referenceSubscription]);

  const snapshot = JSON.stringify({
    mode, subscriptionId, destinationId, state, eventClasses, eventTypes, revision,
  });
  function prepare(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setReceipt(null);
    setPrepared(null);
    setError(null);
    const identity = subscriptionId.trim();
    const dest = destinationId.trim();
    if (!/^[A-Za-z0-9._-]{1,128}$/.test(identity) || identity === '.' || identity === '..' ||
        !isValidNotificationDestinationId(dest)) {
      setError('Subscription/destination identities must be bounded opaque IDs, not URLs or paths.');
      return;
    }
    if (!eventClasses.length || new Set(eventClasses).size !== eventClasses.length) {
      setError('Select one or more distinct supported event classes.');
      return;
    }
    const types = eventTypes.split(/[\n,]/).map(s => s.trim()).filter(Boolean);
    if (types.length > 32 || new Set(types).size !== types.length ||
        types.some(type => !/^[A-Za-z0-9._-]{1,128}$/.test(type))) {
      setError('Event types must be unique, bounded identifiers (at most 32).');
      return;
    }
    let expectedRevision: number | null = null;
    if (mode === 'replace') {
      if (!/^[1-9][0-9]*$/.test(revision) || !Number.isSafeInteger(Number(revision))) {
        setError('Replace/retire requires a precise positive CAS revision from current authorized data.');
        return;
      }
      expectedRevision = Number(revision);
    } else if (state === 'retired') {
      setError('A new subscription cannot start in terminal Retired state.');
      return;
    }
    setPrepared({
      snapshot, identity,
      payload: { destinationId: dest, state, eventClasses: [...eventClasses], eventTypes: types, expectedRevision },
    });
  }

  async function confirm() {
    if (!prepared || prepared.snapshot !== snapshot || submitting.current) return;
    submitting.current = true;
    const controller = new AbortController();
    inFlight.current = controller;
    setBusy(true);
    setError(null);
    try {
      const response = await kafdeckApi.upsertNotificationSubscription(
        prepared.identity, prepared.payload, controller.signal);
      if (inFlight.current !== controller) return;
      setReceipt(response);
      setPrepared(null);
      setRevision(String(response.revision));
      setMode('replace');
      onUpdated();
    } catch (reason) {
      if (!controller.signal.aborted) {
        setPrepared(null);
        setError(manageFailure(reason));
      }
    } finally {
      if (inFlight.current === controller) {
        inFlight.current = null;
        submitting.current = false;
        setBusy(false);
      }
    }
  }

  return <article aria-labelledby="notification-subscription-manage-title">
    <h3 className="h4" id="notification-subscription-manage-title">Manage notification subscription</h3>
    <p className="text-secondary">
      Separate NotificationManage permission and server-side CAS/antiforgery are required.
      Destination profiles, provider URLs, credentials and delivery workers cannot be managed here.
    </p>
    <form onSubmit={prepare}>
      <label className="form-label" htmlFor="notification-manage-mode">Operation</label>
      <select className="form-select" id="notification-manage-mode" value={mode}
        disabled={busy} onChange={event => {
          const next = event.target.value as Mode;
          setMode(next);
          if (next === 'create' && state === 'retired') setState('active');
          setPrepared(null);
        }}>
        <option value="create">Create-only</option>
        <option value="replace">Replace or retire (CAS)</option>
      </select>
      <div className="row g-2 mt-2">
        <div className="col-md-6">
          <label className="form-label" htmlFor="notification-manage-id">Subscription ID</label>
          <input className="form-control" id="notification-manage-id" required
            value={subscriptionId} maxLength={128} disabled={busy}
            onChange={event => { setSubscriptionId(event.target.value); setPrepared(null); }} />
        </div>
        <div className="col-md-6">
          <label className="form-label" htmlFor="notification-manage-destination">Destination ID</label>
          <input className="form-control" id="notification-manage-destination" required
            value={destinationId} maxLength={128} disabled={busy}
            onChange={event => { setDestinationId(event.target.value); setPrepared(null); }} />
        </div>
        <div className="col-md-6">
          <label className="form-label" htmlFor="notification-manage-state">State</label>
          <select className="form-select" id="notification-manage-state" value={state}
            disabled={busy} onChange={event => { setState(event.target.value as SubscriptionState); setPrepared(null); }}>
            <option value="active">Active</option>
            <option value="paused">Paused</option>
            {mode === 'replace' && <option value="retired">Retired (terminal)</option>}
          </select>
        </div>
        {mode === 'replace' && <div className="col-md-6">
          <label className="form-label" htmlFor="notification-manage-revision">Expected revision</label>
          <input className="form-control" id="notification-manage-revision"
            inputMode="numeric" value={revision} required disabled={busy}
            onChange={event => { setRevision(event.target.value); setPrepared(null); }} />
        </div>}
      </div>
      <fieldset className="mt-3" disabled={busy}>
        <legend className="h5">Event classes</legend>
        {eventClassOptions.map(item => <label className="form-check form-check-inline" key={item}>
          <input type="checkbox" className="form-check-input" checked={eventClasses.includes(item)}
            onChange={event => {
              setEventClasses(current => event.target.checked ?
                [...current.filter(value => value !== item), item] :
                current.filter(value => value !== item));
              setPrepared(null);
            }} />
          <span className="form-check-label">{item}</span>
        </label>)}
      </fieldset>
      <label className="form-label" htmlFor="notification-manage-types">
        Exact event types (comma/newline separated; empty matches all types in selected classes)
      </label>
      {/* Covers 32 bounded event types plus separators/whitespace; semantic limits
           are still validated before user confirmation and enforced server-side. */}
      <textarea className="form-control" id="notification-manage-types" rows={2}
        maxLength={8192} value={eventTypes} disabled={busy}
        onChange={event => { setEventTypes(event.target.value); setPrepared(null); }} />
      <button type="submit" className="btn btn-outline-primary mt-3" disabled={busy}>
        Review subscription change
      </button>
    </form>
    {prepared?.snapshot === snapshot && <div role="group"
      aria-label="Confirm subscription management" className="mt-3">
      <p>Apply {mode === 'create' ? 'create-only' : 'CAS replacement'} to
        {' '}<strong>{prepared.identity}</strong> / <strong>{prepared.payload.destinationId}</strong>.
        {state === 'retired' && <strong> Retirement is irreversible.</strong>}</p>
      <p className="text-secondary">State {state}; classes {prepared.payload.eventClasses.join(', ')};
        filters {prepared.payload.eventTypes.length}; expected revision
        {' '}{prepared.payload.expectedRevision ?? 'create-only'}.</p>
      <button className="btn btn-primary" type="button" disabled={busy}
        onClick={() => { void confirm(); }}>
        {busy ? 'Submitting…' : 'Confirm subscription change'}
      </button>
    </div>}
    {error && <p role="alert" className="alert alert-danger">{error}</p>}
    {receipt && <p role="status" className="alert alert-info">
      Accepted subscription {receipt.subscriptionId}: {receipt.state}, revision {receipt.revision}.
      Check current authorized state before another update.
    </p>}
  </article>;
}
