import { useId, useState, type FormEvent } from 'react';
import { kafdeckApi } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewError, readViewProblemText } from './readViewProblem.js';

const formats = ['JSON', 'JSON_SR', 'AVRO', 'PROTOBUF', 'DELIMITED'];
const fieldTypes = ['VARCHAR', 'INTEGER', 'BIGINT', 'DOUBLE', 'BOOLEAN'];
const keyTypes = ['VARCHAR', 'INTEGER', 'BIGINT', 'DOUBLE'];

export function KsqlCatalogForm({ clusterId, kind, onClose, onComplete }: {
  clusterId: string;
  kind: 'stream' | 'table';
  onClose: () => void;
  onComplete: (name: string, status: 'created' | 'unconfirmed') => void;
}) {
  const id = useId();
  const isTable = kind === 'table';
  const title = isTable ? t("Create table") : t("Create stream");
  const [fields, setFields] = useState([{ name: 'MESSAGE', type: 'VARCHAR' }]);
  const [sizeTopic, setSizeTopic] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    const data = new FormData(event.currentTarget);
    setBusy(true);
    setError(null);
    try {
      const result = await kafdeckApi.createKsqlCatalogEntry(clusterId, kind, {
        name: String(data.get('name') ?? '').trim(),
        topic: String(data.get('topic') ?? '').trim(),
        valueFormat: String(data.get('valueFormat') ?? ''),
        fields: fields.map(field => ({ ...field, name: field.name.trim() })),
        ...(isTable ? { primaryKey: {
          name: String(data.get('keyName') ?? '').trim(),
          type: String(data.get('keyType') ?? ''),
        } } : {}),
        ...(sizeTopic ? { partitions: Number(data.get('partitions')), replicas: Number(data.get('replicas')) } : {}),
      });
      onComplete(result.name, result.status);
    } catch (reason) {
      setError(readViewProblemText(readViewError(reason)));
    } finally {
      setBusy(false);
    }
  }

  function updateField(index: number, value: Partial<(typeof fields)[number]>) {
    setFields(previous => previous.map((field, i) => i === index ? { ...field, ...value } : field));
  }

  return <form className="border rounded p-3" autoComplete="off" aria-labelledby={id + '-heading'} onSubmit={event => void submit(event)}>
    <h3 id={id + '-heading'}>{title}</h3>
    <fieldset disabled={busy} className="d-grid gap-3">
      <div className="row g-3">
        <label className="col-md-4">
          <span className="form-label">{isTable ? t("Table name") : t("Stream name")}</span>
          <input name="name" className="form-control" dir="ltr" required maxLength={128} pattern="[A-Za-z_][A-Za-z0-9_]*" />
        </label>
        <label className="col-md-4">
          <span className="form-label">{t("Topic")}</span>
          <input name="topic" className="form-control" dir="ltr" required maxLength={249} pattern={String.raw`[A-Za-z0-9._\-]+`} />
        </label>
        <label className="col-md-4">
          <span className="form-label">{t("Value format")}</span>
          <select name="valueFormat" className="form-select" defaultValue="JSON">
            {formats.map(format => <option key={format}>{format}</option>)}
          </select>
        </label>
      </div>
      {isTable && <fieldset className="d-grid gap-2">
        <legend className="form-label">{t("Primary key")}</legend>
        <div className="row g-2">
          <label className="col-sm">
            <span className="form-label">{t("Key name")}</span>
            <input name="keyName" className="form-control" dir="ltr" defaultValue="ID" required
              maxLength={128} pattern="[A-Za-z_][A-Za-z0-9_]*" />
          </label>
          <label className="col-sm">
            <span className="form-label">{t("Key type")}</span>
            <select name="keyType" className="form-select" defaultValue="VARCHAR">
              {keyTypes.map(type => <option key={type}>{type}</option>)}
            </select>
          </label>
        </div>
      </fieldset>}
      <fieldset className="d-grid gap-2">
        <legend className="form-label">{isTable ? t("Value columns") : t("Stream columns")}</legend>
        {fields.map((field, index) => <div className="row g-2 align-items-end" key={index}>
          <label className="col-sm">
            <span className="form-label">{t("Column name")}</span>
            <input className="form-control" dir="ltr" value={field.name} required maxLength={128}
              pattern="[A-Za-z_][A-Za-z0-9_]*" onChange={event => updateField(index, { name: event.target.value })} />
          </label>
          <label className="col-sm">
            <span className="form-label">{t("Column type")}</span>
            <select className="form-select" value={field.type} onChange={event => updateField(index, { type: event.target.value })}>
              {fieldTypes.map(type => <option key={type}>{type}</option>)}
            </select>
          </label>
          <div className="col-sm-auto">
            <button type="button" className="btn btn-outline-secondary" disabled={fields.length === 1}
              onClick={() => setFields(previous => previous.filter((_, i) => i !== index))}>{t("Remove")}</button>
          </div>
        </div>)}
        <div><button type="button" className="btn btn-outline-secondary" disabled={fields.length >= 64}
          onClick={() => setFields(previous => [...previous, { name: '', type: 'VARCHAR' }])}>{t("Add column")}</button></div>
      </fieldset>
      <label className="form-check mb-0">
        <input type="checkbox" className="form-check-input" checked={sizeTopic}
          onChange={event => setSizeTopic(event.target.checked)} />
        <span className="form-check-label">{t("Create the topic if missing")}</span>
      </label>
      {sizeTopic && <div className="row g-3">
        <label className="col-sm-4">
          <span className="form-label">{t("Partitions")}</span>
          <input name="partitions" type="number" className="form-control" min={1} max={256} defaultValue={1} required />
        </label>
        <label className="col-sm-4">
          <span className="form-label">{t("Replicas")}</span>
          <input name="replicas" type="number" className="form-control" min={1} max={16} defaultValue={1} required />
        </label>
      </div>}
      <div className="kafdeck-ecosystem-actions">
        <button type="submit" className="btn btn-primary">
          {busy ? (isTable ? t("Creating table…") : t("Creating stream…")) : title}
        </button>
        <button type="button" className="btn btn-outline-secondary" onClick={onClose}>{t("Cancel")}</button>
      </div>
    </fieldset>
    {error && <p className="alert alert-danger mt-3 mb-0" role="alert">{error}</p>}
  </form>;
}
