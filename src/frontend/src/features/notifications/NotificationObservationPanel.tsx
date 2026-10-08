import { useEffect, useRef, useState, type FormEvent } from 'react';
import {
  ApiProblem,
  kafdeckApi,
  type NotificationDeliveryEvidenceData,
  type NotificationSubscriptionData,
  type NotificationSubscriptionListData,
} from '../../shared/api.js';

interface Props {
  initialPage: NotificationSubscriptionListData;
}

function observationFailure(reason: unknown): string {
  if (reason instanceof ApiProblem && reason.status === 401) return 'Your operator session has expired.';
  if (reason instanceof ApiProblem && (reason.status === 403 || reason.status === 404)) {
    return 'Notification evidence is unavailable or not authorized for this operator.';
  }
  return 'The notification observation could not be loaded.';
}

function displayTime(value: string | null): string {
  if (!value) return 'Not scheduled';
  const timestamp = new Date(value);
  return Number.isNaN(timestamp.getTime()) ? 'Unavailable' : timestamp.toLocaleString();
}

/**
 * Read-only W66 operator surface. Server-side OIDC, NotificationRead RBAC,
 * per-subscription/destination checks and fail-closed pagination remain authoritative.
 * This component never stores provider payloads, credentials, URLs or raw requests.
 */
export function NotificationObservationPanel({ initialPage }: Props) {
  const [items, setItems] = useState<NotificationSubscriptionData[]>(initialPage.items);
  const [page, setPage] = useState(initialPage);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);
  const [detail, setDetail] = useState<NotificationSubscriptionData | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [notificationId, setNotificationId] = useState('');
  const [destinationId, setDestinationId] = useState('');
  const [delivery, setDelivery] = useState<NotificationDeliveryEvidenceData | null>(null);
  const [deliveryError, setDeliveryError] = useState<string | null>(null);
  const [deliveryLoading, setDeliveryLoading] = useState(false);
  const listAbort = useRef<AbortController | null>(null);
  const detailAbort = useRef<AbortController | null>(null);
  const deliveryAbort = useRef<AbortController | null>(null);

  useEffect(() => () => {
    listAbort.current?.abort();
    detailAbort.current?.abort();
    deliveryAbort.current?.abort();
  }, []);

  async function loadMore() {
    if (loading || !page.truncated || page.authorizationFiltered ||
      page.continuationRestricted || !page.nextSubscriptionId) return;

    listAbort.current?.abort();
    const controller = new AbortController();
    listAbort.current = controller;
    setLoading(true);
    setListError(null);
    try {
      const next = await kafdeckApi.listNotificationSubscriptions(
        50, page.nextSubscriptionId, controller.signal,
      );
      if (listAbort.current !== controller) return;
      setItems(previous => {
        const seen = new Set(previous.map(item => item.subscriptionId));
        return [...previous, ...next.items.filter(item => !seen.has(item.subscriptionId))];
      });
      setPage(next);
    } catch (reason) {
      if (!controller.signal.aborted) setListError(observationFailure(reason));
    } finally {
      if (listAbort.current === controller) {
        listAbort.current = null;
        setLoading(false);
      }
    }
  }

  async function openSubscription(id: string) {
    detailAbort.current?.abort();
    const controller = new AbortController();
    detailAbort.current = controller;
    setDetail(null);
    setDetailError(null);
    try {
      const result = await kafdeckApi.getNotificationSubscription(id, controller.signal);
      if (detailAbort.current === controller) setDetail(result);
    } catch (reason) {
      if (!controller.signal.aborted) setDetailError(observationFailure(reason));
    } finally {
      if (detailAbort.current === controller) detailAbort.current = null;
    }
  }

  async function lookupDelivery(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    deliveryAbort.current?.abort();
    setDelivery(null);
    setDeliveryError(null);
    const id = notificationId.trim();
    const target = destinationId.trim();
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id) ||
      target.length === 0 || target.length > 128) {
      setDeliveryError('Provide a valid notification UUID and destination ID.');
      return;
    }

    const controller = new AbortController();
    deliveryAbort.current = controller;
    setDeliveryLoading(true);
    try {
      const result = await kafdeckApi.getNotificationDelivery(id, target, controller.signal);
      if (deliveryAbort.current === controller) setDelivery(result);
    } catch (reason) {
      if (!controller.signal.aborted) setDeliveryError(observationFailure(reason));
    } finally {
      if (deliveryAbort.current === controller) {
        deliveryAbort.current = null;
        setDeliveryLoading(false);
      }
    }
  }

  return <section className="card kafdeck-card" id="notifications" aria-labelledby="notifications-title">
    <h2 className="card-title" id="notifications-title">Notification observation — v0.8</h2>
    <p className="text-secondary">
      Read-only metadata from authorized subscriptions and delivery outcomes. No provider payload,
      destination credentials, transport URL, or management operation is available here.
    </p>
    {page.authorizationFiltered && <div className="alert alert-warning" role="status">
      Some subscriptions are hidden by your resource permissions.
      {page.continuationRestricted && ' Further pages cannot be requested without exposing a restricted cursor.'}
    </div>}
    {listError && <div className="alert alert-danger" role="alert">{listError}</div>}
    <div className="table-responsive"><table className="table table-vcenter card-table mb-0">
      <thead><tr><th scope="col">Subscription</th><th scope="col">Destination</th>
        <th scope="col">State</th><th scope="col">Revision</th><th scope="col">Event classes</th><th scope="col">Action</th></tr></thead>
      <tbody>{items.map(item => <tr key={item.subscriptionId}>
        <th scope="row">{item.subscriptionId}</th>
        <td>{item.destinationId}</td><td>{item.state}</td><td>{item.revision}</td>
        <td>{item.eventClasses.join(', ') || 'None'}</td>
        <td><button className="btn btn-sm btn-outline-primary" type="button" onClick={() => void openSubscription(item.subscriptionId)}>View details</button></td>
      </tr>)}</tbody>
    </table></div>
    {items.length === 0 && <p className="text-secondary" role="status">No authorized subscriptions on this page.</p>}
    {page.truncated && !page.authorizationFiltered && !page.continuationRestricted && page.nextSubscriptionId && <button
      type="button" className="btn btn-sm btn-outline-primary mt-3" disabled={loading}
      onClick={() => void loadMore()}>{loading ? 'Loading…' : 'Load more subscriptions'}</button>}
    {detailError && <div className="alert alert-danger" role="alert">{detailError}</div>}
    {detail && <article aria-labelledby="notification-subscription-detail-title">
      <h3 className="h4" id="notification-subscription-detail-title">Subscription details</h3>
      <dl><dt>Subscription ID</dt><dd>{detail.subscriptionId}</dd>
        <dt>Destination ID</dt><dd>{detail.destinationId}</dd>
        <dt>State</dt><dd>{detail.state}</dd>
        <dt>Revision</dt><dd>{detail.revision}</dd>
        <dt>Updated</dt><dd>{displayTime(detail.updatedAtUtc)}</dd>
        <dt>Event classes</dt><dd>{detail.eventClasses.join(', ') || 'None'}</dd>
        <dt>Exact event types</dt><dd>{detail.eventTypes.join(', ') || 'All admitted types in selected classes'}</dd></dl>
    </article>}
    <article aria-labelledby="notification-delivery-title">
      <h3 className="h4" id="notification-delivery-title">Delivery evidence lookup</h3>
      <form onSubmit={event => void lookupDelivery(event)}>
        <div className="row g-2">
          <div className="col-md-6"><label htmlFor="notification-evidence-id" className="form-label">Notification UUID</label>
            <input id="notification-evidence-id" className="form-control" value={notificationId}
              onChange={event => setNotificationId(event.target.value)} maxLength={36} required /></div>
          <div className="col-md-6"><label htmlFor="notification-evidence-destination" className="form-label">Destination ID</label>
            <input id="notification-evidence-destination" className="form-control" value={destinationId}
              onChange={event => setDestinationId(event.target.value)} maxLength={128} required /></div>
        </div>
        <button className="btn btn-primary mt-3" type="submit" disabled={deliveryLoading}>{deliveryLoading ? 'Loading…' : 'Look up evidence'}</button>
      </form>
      {deliveryError && <div className="alert alert-danger" role="alert">{deliveryError}</div>}
      {delivery && <dl aria-label="Authorized delivery evidence">
        <dt>Notification</dt><dd>{delivery.notificationId}</dd>
        <dt>Destination</dt><dd>{delivery.destinationId}</dd>
        <dt>State</dt><dd>{delivery.state}</dd>
        <dt>Attempts</dt><dd>{delivery.attemptCount}</dd>
        <dt>Revision</dt><dd>{delivery.revision}</dd>
        <dt>Created</dt><dd>{displayTime(delivery.createdAtUtc)}</dd>
        <dt>Updated</dt><dd>{displayTime(delivery.updatedAtUtc)}</dd>
        <dt>Next attempt</dt><dd>{displayTime(delivery.nextAttemptAtUtc)}</dd>
        <dt>Outcome</dt><dd>{delivery.outcomeCode ?? 'Not available'}</dd>
        <dt>Destination revision bound</dt><dd>{delivery.profileRevisionBound ? 'Yes' : 'No'}</dd>
      </dl>}
    </article>
  </section>;
}
