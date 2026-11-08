import { useEffect, useRef, useState, type FormEvent } from 'react';
import { StatusBadge } from '../../app/StatusBadge.js';
import { mutationStateStatusKind } from '../../app/statusPresentation.js';
import type { SchemaDocument } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { MutationApiProblem, mutationApi, type MutationStatus, type SchemaFormatInput, type SchemaRegistrationPreviewInput } from '../mutations/mutationApi.js';
import { newIdempotencyKey } from '../mutations/MutationPreviewWorkflows.js';

function sourceFormat(format: string | undefined): SchemaFormatInput {
  switch (format?.toLowerCase()) {
    case 'protobuf': return 'protobuf';
    case 'json':
    case 'jsonschema': return 'jsonSchema';
    default: return 'avro';
  }
}

export function SchemaVersionRegistrar({ clusterId, subject, source, onRegistered, onClose }: {
  clusterId: string;
  subject: string;
  source: SchemaDocument;
  onRegistered: (version: number | null) => void;
  onClose: () => void;
}) {
  const [format, setFormat] = useState<SchemaFormatInput>(() => sourceFormat(source.format));
  const [schema, setSchema] = useState(source.schemaText);
  const [references, setReferences] = useState(source.references.length ? JSON.stringify(source.references, null, 2) : '');
  const [idempotencyKey, setIdempotencyKey] = useState(newIdempotencyKey);
  const [prepared, setPrepared] = useState<{ operation: MutationStatus; schema: string } | null>(null);
  const [challenge, setChallenge] = useState('');
  const [busy, setBusy] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const operation = prepared?.operation;
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => { heading.current?.focus(); }, []);

  function edit() {
    setPrepared(null);
    setChallenge('');
    setError(null);
    setSubmitted(false);
    setIdempotencyKey(newIdempotencyKey());
  }

  async function checkCompatibility(event: FormEvent) {
    event.preventDefault();
    if (busy || prepared || !schema.trim()) return;
    let parsed: unknown;
    try {
      parsed = references.trim() ? JSON.parse(references) : [];
    } catch {
      setError(t("Enter references as a JSON array with name, subject and version."));
      return;
    }
    if (!Array.isArray(parsed) || !parsed.every(item => item && typeof item.name === 'string' && item.name.trim() &&
      typeof item.subject === 'string' && item.subject.trim() && Number.isSafeInteger(item.version) && item.version > 0)) {
      setError(t("Enter references as a JSON array with name, subject and version."));
      return;
    }
    const draft: SchemaRegistrationPreviewInput = {
      format,
      schema,
      references: parsed.map(({ name, subject, version }) => ({ name, subject, version })),
    };
    setBusy(true);
    setError(null);
    try {
      const result = await mutationApi.previewSchemaRegister(clusterId, subject, draft, idempotencyKey);
      setPrepared({ operation: result, schema: draft.schema });
    } catch (reason) {
      setError(reason instanceof MutationApiProblem && reason.code === 'SchemaIncompatible'
        ? t("Schema Registry rejected this version as incompatible.")
        : reason instanceof MutationApiProblem ? reason.message : t("The compatibility check could not be completed."));
    } finally {
      setBusy(false);
    }
  }

  function applyResult(result: MutationStatus, sourceText: string) {
    setPrepared({ operation: result, schema: sourceText });
    if (result.state !== 'appliedVerified') return;
    const version = Number(result.safeProviderEvidence['schema.version']);
    onRegistered(Number.isSafeInteger(version) && version > 0 ? version : null);
  }

  async function register() {
    if (!prepared || busy || submitted) return;
    setBusy(true);
    setError(null);
    try {
      const current = prepared.operation;
      const confirmed = current.state === 'ready' ? current : await mutationApi.confirm(
        current, current.confirmationMode === 'typedTarget' ? challenge : null,
      );
      setPrepared({ ...prepared, operation: confirmed });
      if (confirmed.state !== 'ready') return;
      setSubmitted(true);
      applyResult(await mutationApi.executeSchemaCreate(confirmed, prepared.schema), prepared.schema);
    } catch (reason) {
      setError(reason instanceof MutationApiProblem ? reason.message : t("Check the operation status before attempting registration again."));
    } finally {
      setBusy(false);
    }
  }

  async function refreshStatus() {
    if (!prepared || busy) return;
    setBusy(true);
    setError(null);
    try {
      applyResult(await mutationApi.get(prepared.operation.operationId), prepared.schema);
    } catch (reason) {
      setError(reason instanceof MutationApiProblem ? reason.message : t("The operation status could not be loaded."));
    } finally {
      setBusy(false);
    }
  }

  return <section aria-labelledby="schema-registration-title" className="mt-3">
    <div className="d-flex align-items-center gap-2 mb-2">
      <h4 id="schema-registration-title" ref={heading} tabIndex={-1} className="mb-0">{t("New schema version")}</h4>
      <button type="button" className="btn btn-sm btn-outline-secondary" onClick={onClose} disabled={busy}>{t("Close")}</button>
    </div>
    <form onSubmit={event => void checkCompatibility(event)}>
      <fieldset disabled={busy || !!prepared} className="d-grid gap-3">
        <div>
          <label className="form-label" htmlFor="schema-candidate-format">{t("Format")}</label>
          <select id="schema-candidate-format" className="form-select" value={format} onChange={event => {
            setFormat(event.target.value as SchemaFormatInput); edit();
          }}>
            <option value="avro">Avro</option><option value="protobuf">Protobuf</option><option value="jsonSchema">JSON Schema</option>
          </select>
        </div>
        <div>
          <label className="form-label" htmlFor="schema-candidate-source">{t("Schema source")}</label>
          <textarea id="schema-candidate-source" className="form-control font-monospace" dir="ltr" rows={12}
            spellCheck={false} required value={schema} onChange={event => { setSchema(event.target.value); edit(); }} />
        </div>
        <div>
          <label className="form-label" htmlFor="schema-candidate-references">{t("References (optional JSON)")}</label>
          <textarea id="schema-candidate-references" className="form-control font-monospace" dir="ltr" rows={3}
            spellCheck={false} value={references} onChange={event => { setReferences(event.target.value); edit(); }} />
        </div>
        {!prepared && <div><button type="submit" className="btn btn-primary" disabled={busy || !schema.trim()}>{t("Check compatibility")}</button></div>}
      </fieldset>
    </form>
    {error && <p className="alert alert-danger mt-3" role="alert">{error}</p>}
    {operation && <div className="mt-3">
      <div role="status">
        <StatusBadge kind={mutationStateStatusKind(operation.state)} label={operation.state} />
        {(operation.state === 'awaitingConfirmation' || operation.state === 'ready') && <p>{t("Compatibility check passed. Register this version when ready.")}</p>}
        {operation.state === 'appliedVerified' && <p>{t("Schema version registered and verified.")}</p>}
        {operation.state === 'awaitingApproval' && <p>{t("This registration requires independent approval.")}</p>}
        {operation.state === 'stalePreview' && <p>{t("The subject changed. Check compatibility again before registering.")}</p>}
        {operation.state === 'executionUnknown' && <p>{t("Check the operation status before attempting registration again.")}</p>}
        <p>{t("Operation ID")}{' '}<code dir="ltr">{operation.operationId}</code></p>
      </div>
      {operation.state === 'awaitingConfirmation' && operation.confirmationMode === 'typedTarget' &&
        <div className="mb-2">
          <label className="form-label" htmlFor="schema-registration-challenge">{t("Type the exact server challenge")}</label>
          <code dir="ltr">{operation.confirmationChallenge}</code>
          <input id="schema-registration-challenge" className="form-control" dir="ltr" value={challenge} onChange={event => setChallenge(event.target.value)} />
        </div>}
      <div className="d-flex flex-wrap gap-2">
        {!submitted && (operation.state === 'awaitingConfirmation' || operation.state === 'ready') &&
          <button type="button" className="btn btn-primary"
            disabled={busy || (operation.state === 'awaitingConfirmation' && operation.confirmationMode === 'typedTarget' && challenge !== operation.confirmationChallenge)}
            onClick={() => void register()}>{t("Register version")}</button>}
        <button type="button" className="btn btn-outline-primary" disabled={busy} onClick={() => void refreshStatus()}>{t("Refresh status")}</button>
        <button type="button" className="btn btn-outline-secondary" disabled={busy} onClick={edit}>{t("Edit schema draft")}</button>
      </div>
    </div>}
  </section>;
}
