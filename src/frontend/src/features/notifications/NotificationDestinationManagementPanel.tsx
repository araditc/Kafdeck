import { applicationProblem, problemPresentation, type PresentationProblem } from '../../shared/presentationProblem.js';
import { t } from '../../shared/i18n.js';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import { ApiProblem, kafdeckApi, type NotificationManagedDestination, type NotificationManagedDestinationChange,
  type NotificationManagedAudit } from '../../shared/api.js';

function failure(error: unknown): PresentationProblem {
  if (error instanceof ApiProblem && error.status === 409) return applicationProblem("Revision conflict. Reload current state before preparing another change.");
  if (error instanceof ApiProblem && error.status === 401) return applicationProblem("Operator session expired.");
  if (error instanceof ApiProblem && error.status === 403) return applicationProblem("Destination management is not authorized.");
  return applicationProblem("The change was not confirmed. Reload current state before retrying.");
}

/** Only opaque provisioned choices; no URL, secret, locator or arbitrary provider request input. */
export function NotificationDestinationManagementPanel() {
  const [items, setItems] = useState<NotificationManagedDestination[]>([]);
  const [selected, setSelected] = useState<NotificationManagedDestination | null>(null);
  const [candidate, setCandidate] = useState('');
  const [enabled, setEnabled] = useState(false);
  const [credential, setCredential] = useState(false);
  const [prepared, setPrepared] = useState<NotificationManagedDestinationChange | null>(null);
  const [audit, setAudit] = useState<NotificationManagedAudit[]>([]);
  const [nextRevision, setNextRevision] = useState<number | null>(null);
  const [error, setError] = useState<PresentationProblem | null>(null);
  const [receipt, setReceipt] = useState<number | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  const [busy, setBusy] = useState(false);
  const current = useRef<AbortController | null>(null);
  useEffect(() => { void load(); return () => current.current?.abort(); }, []);
  async function load() {
    current.current?.abort(); const controller = new AbortController(); current.current = controller;
    setBusy(true); setError(null); setSelected(null); setPrepared(null); setAudit([]); setReceipt(null);
    try {
      const result = await kafdeckApi.listNotificationManagedDestinations(controller.signal);
      if (current.current === controller) { setItems(result.items); setUnavailable(false); }
    } catch (reason) {
      if (!controller.signal.aborted) {
        setItems([]);
        if (reason instanceof ApiProblem && reason.status === 404) setUnavailable(true);
        else setError(failure(reason));
      }
    } finally { if (current.current === controller) { setBusy(false); current.current = null; } }
  }
  function select(id: string) {
    current.current?.abort(); current.current = null; setBusy(false);
    const item = items.find(row => row.destinationId === id) ?? null;
    setSelected(item); setCandidate(item?.candidateId ?? item?.candidates[0]?.candidateId ?? '');
    setEnabled(item?.profileEnabled ?? false); setCredential(item?.credentialEnabled ?? false);
    setPrepared(null); setAudit([]); setNextRevision(null); setError(null); setReceipt(null);
  }
  function prepare(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); setReceipt(null); setError(null);
    if (!selected || !selected.candidates.some(row => row.candidateId === candidate) || !Number.isSafeInteger(selected.revision)) return;
    setPrepared({ candidateId: candidate, expectedRevision: selected.revision, profileEnabled: enabled, credentialEnabled: credential });
  }
  async function confirm() {
    if (!selected || !prepared || busy) return;
    const controller = new AbortController(); current.current = controller; setBusy(true); setError(null);
    try {
      const result = await kafdeckApi.changeNotificationManagedDestination(selected.destinationId, prepared, controller.signal);
      if (current.current !== controller) return;
      setReceipt(result.revision); setPrepared(null);
      const updated = { ...selected, ...result, candidateId: prepared.candidateId, configurationMismatch: false };
      setSelected(updated); setItems(rows => rows.map(row => row.destinationId === updated.destinationId ? updated : row));
    } catch (reason) { if (!controller.signal.aborted) { setPrepared(null); setError(failure(reason)); } }
    finally { if (current.current === controller) { current.current = null; setBusy(false); } }
  }
  async function readAudit(more: boolean) {
    if (!selected || busy) return;
    const controller = new AbortController(); current.current = controller; setBusy(true); setError(null);
    try {
      const result = await kafdeckApi.readNotificationManagedAudit(selected.destinationId, more ? nextRevision ?? 0 : 0, controller.signal);
      if (current.current === controller) {
        setAudit(rows => more ? [...rows, ...result.items].slice(-1000) : result.items);
        setNextRevision(result.items.length === 50 ? result.nextRevision : null);
      }
    } catch (reason) { if (!controller.signal.aborted) setError(failure(reason)); }
    finally { if (current.current === controller) { current.current = null; setBusy(false); } }
  }
  if (unavailable) return null;
  return <section className="card kafdeck-card" aria-labelledby="notification-destination-management-title">
    <h2 id="notification-destination-management-title">{t("Notification destination and credential management")}</h2>
    <p>{t("Choose an approved profile version. Rotation advances the shared revision and fences old queued intents. Revocation stops new claims; a message already sent requires reconciliation.")}</p>
    <button type="button" disabled={busy} onClick={() => void load()}>{t("Reload authorized destinations")}</button>
    {error && <p role="alert">{problemPresentation(error)}</p>}
    <label htmlFor="managed-destination">{t("Destination")}</label>
    <select id="managed-destination" value={selected?.destinationId ?? ''} disabled={busy} onChange={event => select(event.target.value)}>
      <option value="">{t("Choose a destination")}</option>{items.map(item => <option key={item.destinationId} value={item.destinationId}>{item.destinationId}</option>)}
    </select>
    {selected && <>
      <p>{t("Current shared revision:")}{' '}{selected.revision}. {selected.configurationMismatch && t("Provisioned configuration differs; select the reviewed replacement version.")}</p>
      <form onSubmit={prepare} onChange={() => setPrepared(null)}>
        <label htmlFor="managed-candidate">{t("Approved profile version")}</label>
        <select id="managed-candidate" value={candidate} disabled={busy} onChange={event => setCandidate(event.target.value)}>
          {selected.candidates.map(row => <option key={row.candidateId} value={row.candidateId}>{row.candidateId} — {row.provider}</option>)}
        </select>
        <label><input dir="auto" type="checkbox" checked={enabled} disabled={busy} onChange={event => setEnabled(event.target.checked)} />{t("Destination enabled")}</label>
        <label><input dir="auto" type="checkbox" checked={credential} disabled={busy} onChange={event => setCredential(event.target.checked)} />{t("Credential enabled")}</label>
        <button type="submit" disabled={busy}>{t("Review change")}</button>
      </form>
      {prepared && <aside aria-label={t("Reviewed destination change")}><p>{t("Revision")}{' '}<bdi dir="ltr" lang="en">{prepared.expectedRevision}</bdi> → {prepared.expectedRevision + 1}{t("; version")}{' '}<bdi dir="ltr" lang="en">{prepared.candidateId}</bdi>{t("; destination")}{' '}{prepared.profileEnabled ? t("enabled") : t("disabled")}{t("; credential")}{' '}{prepared.credentialEnabled ? t("enabled") : t("revoked")}.</p>
        <button type="button" disabled={busy} onClick={() => void confirm()}>{t("Confirm reviewed change")}</button></aside>}
      <button type="button" disabled={busy} onClick={() => void readAudit(false)}>{t("Read destination audit")}</button>
      {audit.length > 0 && <table><caption>{t("Durable destination changes (latest 1000 retained)")}</caption><thead><tr><th scope="col">{t("Revision")}</th><th scope="col">{t("Version")}</th><th scope="col">{t("Destination")}</th><th scope="col">{t("Credential")}</th><th scope="col">{t("Actor fingerprint")}</th><th scope="col">{t("Time")}</th></tr></thead>
        <tbody>{audit.map(row => <tr key={row.revision}><td>{row.revision}</td><td><bdi dir="ltr" lang="en">{row.candidateId}</bdi></td><td>{row.profileEnabled ? t("Enabled") : t("Disabled")}</td>
          <td>{row.credentialEnabled ? t("Enabled") : t("Revoked")}</td><td><bdi dir="ltr" lang="en">{row.actorFingerprint}</bdi></td><td><bdi dir="ltr" lang="en">{row.updatedAtUtc}</bdi></td></tr>)}</tbody></table>}
      {nextRevision !== null && <button type="button" disabled={busy} onClick={() => void readAudit(true)}>{t("Read next audit page")}</button>}
    </>}
    {receipt !== null && <p role="status">{t("Change recorded at shared revision")}{' '}{receipt}{t(". No provider message was sent by this management request.")}</p>}
  </section>;
}
