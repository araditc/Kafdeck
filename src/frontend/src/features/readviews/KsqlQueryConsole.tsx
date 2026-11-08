import { useEffect, useId, useRef, useState } from 'react';
import { kafdeckApi, type KsqlQueryResult } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewError, readViewProblemText } from './readViewProblem.js';

export function KsqlQueryConsole({ clusterId }: { clusterId: string }) {
  const id = useId();
  const [statement, setStatement] = useState('');
  const [offsetReset, setOffsetReset] = useState<'latest' | 'earliest'>('latest');
  const [result, setResult] = useState<KsqlQueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const active = useRef<AbortController | null>(null);

  useEffect(() => () => active.current?.abort(), []);

  async function run() {
    if (!statement.trim() || active.current) return;
    const controller = new AbortController();
    active.current = controller;
    setBusy(true);
    setResult(null);
    setError(null);
    try {
      const response = await kafdeckApi.executeKsqlQuery(clusterId, statement.trim(), {
        limits: { maxRows: 1000, maxBytes: 2 * 1024 * 1024, maxDurationSeconds: 30 },
        offsetReset,
      }, controller.signal);
      if (!controller.signal.aborted) setResult(response.data);
    } catch (reason) {
      if (!controller.signal.aborted) setError(readViewProblemText(readViewError(reason)));
    } finally {
      if (active.current === controller) {
        active.current = null;
        setBusy(false);
      }
    }
  }

  function cancel() {
    active.current?.abort();
    active.current = null;
    setBusy(false);
  }

  return <div className="kafdeck-ksql-console" aria-busy={busy}>
    <label className="form-label" htmlFor={id}>{t("Read-only SELECT")}</label>
    <textarea className="form-control font-monospace" dir="ltr" id={id} rows={5} value={statement}
      onChange={event => setStatement(event.target.value)} disabled={busy} maxLength={65536}
      placeholder="SELECT * FROM ORDERS EMIT CHANGES LIMIT 100;" autoComplete="off" spellCheck={false} />
    <div className="kafdeck-ecosystem-actions">
      <div className="d-flex flex-wrap align-items-center gap-2">
        <label className="form-label mb-0" htmlFor={id + '-offset'}>{t("Read messages")}</label>
        <select className="form-select w-auto" id={id + '-offset'} value={offsetReset} disabled={busy}
          onChange={event => setOffsetReset(event.target.value === 'earliest' ? 'earliest' : 'latest')}>
          <option value="latest">{t("Only new messages")}</option>
          <option value="earliest">{t("Read from the beginning")}</option>
        </select>
      </div>
      <button type="button" className="btn btn-primary" disabled={busy || !statement.trim()} onClick={() => void run()}>
        {busy ? t("Running…") : t("Run bounded query")}
      </button>
      {busy && <button type="button" className="btn btn-outline-secondary" onClick={cancel}>{t("Cancel")}</button>}
    </div>
    {error && <p className="alert alert-danger mb-0" role="alert">{error}</p>}
    {result && <article className="kafdeck-ksql-result" aria-labelledby={id + '-result'}>
      <h4 id={id + '-result'}>{t("Query result")}</h4>
      <p role="status">{t("Rows:")}{' '}<bdi>{result.rows.length}</bdi> {' '}{t("· Bytes observed:")}{' '}
        <bdi>{result.responseBytes}</bdi> · {result.truncated
          ? t("Limited: {0}", [result.limitReason ?? t("server limit")]) : t("Complete within configured bounds")}</p>
      <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}>
        <table className="table table-vcenter">
          <thead><tr>{result.header.columnNames.map((name, index) =>
            <th key={index} scope="col"><bdi>{name}</bdi><br /><small>{result.header.columnTypes[index] ?? t("Unknown")}</small></th>)}</tr></thead>
          <tbody>{result.rows.map((row, index) => <tr key={index}>{row.columns.map((value, column) =>
            <td key={column}><code dir="ltr">{JSON.stringify(value)}</code></td>)}</tr>)}</tbody>
        </table>
      </div>
    </article>}
  </div>;
}
