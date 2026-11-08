import { useState, type FormEvent } from 'react';
import { t } from '../../shared/i18n.js';
import { ownedErrorPresentation, problemPresentation } from '../../shared/presentationProblem.js';
import { mutationApi, type MutationStatus, type RecordProductionRecordInput } from '../mutations/mutationApi.js';
import { bytesToBase64, newIdempotencyKey } from '../mutations/MutationPreviewWorkflows.js';
import { StatusBadge } from '../../app/StatusBadge.js';
import { mutationStateStatusKind } from '../../app/statusPresentation.js';

export function TopicRecordPublisher({ clusterId, topicName }: { clusterId: string; topicName: string }) {
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [headers, setHeaders] = useState('');
  const [idempotencyKey, setIdempotencyKey] = useState(newIdempotencyKey);
  const [prepared, setPrepared] = useState<{ operation: MutationStatus; records: RecordProductionRecordInput[] } | null>(null);
  const [busy, setBusy] = useState(false);
  const [sent, setSent] = useState(false);
  const [error, setError] = useState<{ reason: unknown } | { invalidHeader: true } | null>(null);

  const operation = prepared?.operation;
  const canSend = !sent && (operation?.state === 'awaitingConfirmation' || operation?.state === 'ready');
  const updateDraft = (change: (input: string) => void, input: string) => {
    change(input);
    setIdempotencyKey(newIdempotencyKey());
    setError(null);
  };
  const preview = async (event: FormEvent) => {
    event.preventDefault();
    if (busy || prepared) return;
    setBusy(true);
    setError(null);
    try {
      const parsedHeaders: NonNullable<RecordProductionRecordInput['headers']> = [];
      for (const line of headers.split(/\r?\n/).filter(line => line.trim())) {
        const separator = line.indexOf('=');
        const name = separator < 0 ? '' : line.slice(0, separator).trim();
        if (!name) { setError({ invalidHeader: true }); return; }
        parsedHeaders.push({ name, value: bytesToBase64(line.slice(separator + 1)) });
      }
      const records = [{ key: key ? bytesToBase64(key) : null, value: bytesToBase64(value), headers: parsedHeaders }];
      const operation = await mutationApi.previewRecordProduction(clusterId, topicName, { records }, idempotencyKey);
      setPrepared({ operation, records });
    } catch (reason) {
      setError({ reason });
    } finally {
      setBusy(false);
    }
  };

  const send = async () => {
    if (!prepared || !canSend || busy) return;
    setBusy(true);
    setError(null);
    try {
      const confirmed = prepared.operation.state === 'ready' ? prepared.operation : await mutationApi.confirm(prepared.operation, null);
      setPrepared({ ...prepared, operation: confirmed });
      if (confirmed.state === 'ready') {
        setSent(true);
        const result = await mutationApi.executeRecordProduction(confirmed, prepared.records);
        setPrepared({ operation: result, records: [] });
        setKey(''); setValue(''); setHeaders('');
      }
    } catch (reason) {
      setError({ reason });
    } finally {
      setBusy(false);
    }
  };

  const check = async () => {
    if (!prepared || busy) return;
    setBusy(true);
    setError(null);
    try {
      setPrepared({ ...prepared, operation: await mutationApi.get(prepared.operation.operationId) });
    } catch (reason) {
      setError({ reason });
    } finally {
      setBusy(false);
    }
  };

  const errorCopy = error && 'reason' in error ? ownedErrorPresentation(error.reason) : null;
  return <section aria-labelledby="topic-publish-heading">
    <h4 id="topic-publish-heading">{t("Publish message")}</h4>
    <p className="text-secondary">{t("Review the message before sending it to this topic.")}</p>
    <form onSubmit={event => void preview(event)}>
      <fieldset disabled={busy || !!prepared} className="d-grid gap-3">
        <div><label className="form-label" htmlFor="topic-publish-key">{t("Message key (optional)")}</label>
          <input className="form-control" id="topic-publish-key" dir="auto" value={key} onChange={event => updateDraft(setKey, event.target.value)} autoComplete="off" /></div>
        <div><label className="form-label" htmlFor="topic-publish-value">{t("Message value")}</label>
          <textarea className="form-control" id="topic-publish-value" rows={5} dir="auto" value={value} onChange={event => updateDraft(setValue, event.target.value)} /></div>
        <div><label className="form-label" htmlFor="topic-publish-headers">{t("Headers (optional, one name=value per line)")}</label>
          <textarea className="form-control" id="topic-publish-headers" rows={2} dir="ltr" value={headers} onChange={event => updateDraft(setHeaders, event.target.value)} /></div>
        {!prepared && <div><button className="btn btn-outline-primary" type="submit">{t("Preview publication")}</button></div>}
      </fieldset>
    </form>
    {error && <p className="alert alert-danger mt-3" role="alert">{'invalidHeader' in error ? t("Enter each message header as name=value.")
      : errorCopy ? problemPresentation(errorCopy) : t("The publication request could not be completed.")}</p>}
    {operation && <div className="mt-3 d-grid gap-2">
      <div role="status">
        <StatusBadge kind={mutationStateStatusKind(operation.state)} label={operation.state} />
        {operation.state === 'appliedVerified' && <p>{t("Kafka acknowledged the message.")}{' '}
          {operation.safeProviderEvidence.partition !== undefined && operation.safeProviderEvidence.offset !== undefined &&
            <bdi>{t("Partition {0}, offset {1}", [operation.safeProviderEvidence.partition, operation.safeProviderEvidence.offset])}</bdi>}</p>}
        {operation.state === 'awaitingApproval' && <p>{t("This publication requires approval before it can be sent.")}</p>}
        {(operation.state === 'executionUnknown' || (sent && error)) && <p>{t("Check publication status before sending another message.")}</p>}
      </div>
      <div className="d-flex gap-2 flex-wrap">
        {canSend && <button className="btn btn-primary" type="button" disabled={busy} onClick={() => void send()}>{t("Publish message")}</button>}
        <button className="btn btn-outline-primary" type="button" disabled={busy} onClick={() => void check()}>{t("Check publication status")}</button>
        <button className="btn btn-outline-secondary" type="button" disabled={busy} onClick={() => {
          setPrepared(null); setSent(false); setError(null); setIdempotencyKey(newIdempotencyKey());
        }}>{sent ? t("New message") : t("Edit message")}</button>
      </div>
    </div>}
  </section>;
}
