import { useCallback, useEffect, useRef, useState } from 'react';
import { kafdeckApi, type BrokerLogPage, type BrokerLogTargets } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewError, readViewProblemText } from '../readviews/readViewProblem.js';

export function brokerLogsHref(clusterId: string, brokerId: number, eventTime?: number) {
  const search = new URLSearchParams({cluster: clusterId, brokerLogs: String(brokerId)});
  if (eventTime !== undefined) {
    search.set('logFrom', String(Math.floor(eventTime) - 300));
    search.set('logTo', String(Math.min(Math.floor(eventTime) + 300, Math.floor(Date.now() / 1000) + 1)));
  }
  return '?' + search + '#brokers';
}

export function alertLogsHref(clusterId: string, targets: BrokerLogTargets | null, instance: string, eventTime: number) {
  const target = targets?.targets.find(item => item.monitoringInstance === instance);
  return target && Number.isFinite(eventTime) ? brokerLogsHref(clusterId, target.brokerId, eventTime) : undefined;
}

export function useBrokerLogTargets(clusterId: string, enabled = true) {
  const [state, setState] = useState<{ clusterId: string; result: BrokerLogTargets | null; problem: unknown } | null>(null);
  useEffect(() => {
    if (!enabled) return;
    const abort = new AbortController();
    void kafdeckApi.brokerLogTargets(clusterId, abort.signal).then(result => {
      if (!abort.signal.aborted) setState({clusterId, result, problem: null});
    }, problem => {
      if (!abort.signal.aborted) setState({clusterId, result: null, problem});
    });
    return () => abort.abort();
  }, [clusterId, enabled]);
  return enabled && state?.clusterId === clusterId ? state : null;
}

const utcInput = (seconds: number) => new Date(seconds * 1000).toISOString().slice(0, 19);
const utcSeconds = (value: string) => value ? Math.floor(Date.parse(value + 'Z') / 1000) : NaN;
function linkSelection(clusterId: string) {
  const query = new URLSearchParams(location.search);
  const parse = (key: string) => {
    const value = query.get(key);
    return value && /^\d{1,10}$/.test(value) ? Number(value) : null;
  };
  return query.get('cluster') === clusterId ? {broker:parse('brokerLogs'), from:parse('logFrom'), to:parse('logTo')} :
    {broker:null, from:null, to:null};
}

export function BrokerLogs({clusterId}: {clusterId: string}) {
  const access = useBrokerLogTargets(clusterId);
  const [initial] = useState(() => linkSelection(clusterId));
  const [broker, setBroker] = useState(initial.broker);
  const [from, setFrom] = useState(() => utcInput(initial.from ?? Math.floor(Date.now() / 1000) - 900));
  const [to, setTo] = useState(() => utcInput(initial.to ?? Math.floor(Date.now() / 1000) + 1));
  const [page, setPage] = useState<BrokerLogPage | null>(null);
  const [problem, setProblem] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const request = useRef<AbortController | null>(null);
  const clear = useCallback(() => {
    request.current?.abort();
    setPage(null);
    setProblem(null);
    setLoading(false);
  }, []);
  const load = useCallback(async (brokerId: number, start: number, end: number) => {
    clear();
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || end <= start || end - start > 3600) {
      setProblem(t("Choose a log window of up to one hour."));
      return;
    }
    const abort = new AbortController();
    request.current = abort;
    setLoading(true);
    try {
      const result = await kafdeckApi.brokerLogs(clusterId, brokerId, start, end, abort.signal);
      if (!abort.signal.aborted) setPage(result);
    } catch (error) {
      if (!abort.signal.aborted) setProblem(error);
    } finally {
      if (!abort.signal.aborted) setLoading(false);
    }
  }, [clusterId, clear]);
  useEffect(() => () => request.current?.abort(), []);
  const targetExists = initial.broker !== null && (access?.result?.targets.some(target => target.brokerId === initial.broker) ?? false);
  useEffect(() => {
    if (targetExists && initial.broker !== null && initial.from !== null && initial.to !== null) {
      void load(initial.broker, initial.from, initial.to);
    }
  }, [initial, targetExists, load]);

  if (!access?.result?.allowed) {
    return initial.broker !== null && access ? <p role="status">{t("Broker logs are unavailable for this account or deployment.")}</p> : null;
  }
  if (access.result.targets.length === 0) return null;
  const selected = access.result.targets.find(target => target.brokerId === broker)?.brokerId ?? access.result.targets[0]!.brokerId;
  return <section className="card kafdeck-card broker-logs" aria-labelledby="broker-logs-title">
    <h3 className="card-title" id="broker-logs-title">{t("Broker service logs")}</h3>
    {initial.broker !== null && !targetExists && <p role="status">{t("The requested broker has no verified service binding.")}</p>}
    <form className="d-flex flex-wrap align-items-end gap-3" onSubmit={event => {
      event.preventDefault();
      void load(selected, utcSeconds(from), utcSeconds(to));
    }}>
      <div><label className="form-label" htmlFor="broker-log-broker">{t("Broker")}</label>
        <select id="broker-log-broker" className="form-select" value={selected}
          onChange={event => { clear(); setBroker(Number(event.currentTarget.value)); }}>
          {access.result.targets.map(target => <option key={target.brokerId} value={target.brokerId}>{target.brokerId}</option>)}
        </select>
      </div>
      <label>{t("From (UTC)")}<input type="datetime-local" step="1" required className="form-control" dir="ltr" value={from}
        onChange={event => { clear(); setFrom(event.currentTarget.value); }} /></label>
      <label>{t("To (UTC)")}<input type="datetime-local" step="1" required className="form-control" dir="ltr" value={to}
        onChange={event => { clear(); setTo(event.currentTarget.value); }} /></label>
      <button className="btn btn-primary" type="submit" disabled={loading}>{t("Read service logs")}</button>
      {loading && <button className="btn btn-outline-secondary" type="button" onClick={clear}>{t("Cancel")}</button>}
    </form>
    {loading && <p role="status">{t("Reading service logs…")}</p>}
    {!!problem && <p role="alert">{typeof problem === 'string' ? problem : readViewProblemText(readViewError(problem))}</p>}
    {page && <div className="mt-3" aria-live="polite">
      <p>{t("Verified service source")} · <bdi>{page.adapter}</bdi> · {t("Broker")} <bdi>{page.brokerId}</bdi></p>
      {page.truncated && <p role="status">{t("Log limit reached. Narrow the time window.")}</p>}
      {page.lines.length === 0 ? <p>{t("No log entries in this time window.")}</p> :
        <div className="broker-log-lines" role="region" aria-label={t("Broker log entries")} tabIndex={0}>
          <ol className="list-unstyled mb-0">{page.lines.map((line,index) => <li key={index}>
            <time dateTime={new Date(line.timeMs).toISOString()}>{new Date(line.timeMs).toISOString()}</time>{' '}
            <span>{line.text}</span>
          </li>)}</ol>
        </div>}
    </div>}
  </section>;
}
