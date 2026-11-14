import { useEffect, useRef, useState } from 'react';
import { t } from '../../shared/i18n.js';
import { TimeChart } from './Chart.js';
import { ImportIssue, maxImportBytes, metrics, parseMeasurements, type Series } from './measurements.js';

export function MeasurementImport() {
  const [series, setSeries] = useState<Series[]>([]);
  const [source, setSource] = useState('');
  const [metric, setMetric] = useState('');
  const [windowMinutes, setWindowMinutes] = useState(0);
  const [message, setMessage] = useState<ImportIssue | null>(null);
  const generation = useRef(0);
  useEffect(() => () => { generation.current++; }, []);
  async function load(file?: File) {
    const id = ++generation.current;
    setSeries([]); setSource(''); setMetric(''); setMessage(null);
    if (!file) return;
    try {
      if (file.size > maxImportBytes) throw new ImportIssue('limit', 0);
      const contents = await file.text();
      if (id !== generation.current) return;
      const next = parseMeasurements(contents);
      setSeries(next); setSource(next[0]!.source); setMetric(next[0]!.metric);
    } catch (error) {
      if (id === generation.current) setMessage(error instanceof ImportIssue ? error : new ImportIssue('format', 0));
    }
  }
  const sources = [...new Set(series.map(s => s.source))];
  const choices = series.filter(s => s.source === source);
  const active = choices.find(s => s.metric === metric);
  const comparable = series.filter(s => s.metric === metric);
  const end = comparable.reduce((latest, s) => Math.max(latest, s.points.at(-1)!.time), -Infinity);
  const start = windowMinutes ? end - windowMinutes * 60_000 :
    comparable.reduce((earliest, s) => Math.min(earliest, s.points[0]!.time), Infinity);
  const within = (s: Series) => s.points.filter(p => p.time >= start && p.time <= end);
  return <section className="card monitor-panel">
    <h3>{t("Workload and broker measurements")}</h3>
    <p>{t("Open a JSON Lines file from a workload or broker. Measurements stay in this browser tab.")}</p>
    <details><summary>{t("Accepted file formats")}</summary>
      <p>{t("Use one librdkafka statistics object per line, or provide a timestamp, source, metric and numeric value as shown below.")}</p>
      <pre dir="ltr">{'{"time":"2026-11-14T08:00:00Z","source":"worker-1","metric":"queueWaitP95","value":12.4}'}</pre>
      <dl>{Object.entries(metrics).map(([key,[label,unit]]) => <div key={key}><dt><code>{key}</code></dt><dd>{t(label)} · {unit}</dd></div>)}</dl>
    </details>
    <div className="monitor-controls"><label>{t("Measurement file (up to 8 MiB)")}
      <input className="form-control" type="file" accept=".ndjson,.jsonl,.json,application/json" onChange={e => {
        const file = e.currentTarget.files?.[0]; e.currentTarget.value = ''; if (file) void load(file);
      }} /></label>
      {series.length > 0 && <button className="btn btn-outline-secondary" onClick={() => void load()}>{t("Clear measurements")}</button>}
    </div>
    {message && <p role="alert">{message.kind === 'limit' ? t("File exceeds the measurement import limits.") :
      message.kind === 'duplicate' ? t("Duplicate timestamp for this source and metric on line {0}.", [message.line]) :
      t("Invalid measurement on line {0}. Check the file format.", [message.line])}</p>}
    {active && <><div className="monitor-controls">
      <label>{t("Source")}<select className="form-select" value={source} onChange={e => {
        setSource(e.target.value); setMetric(series.find(s => s.source === e.target.value)!.metric);
      }}>{sources.map(name => <option key={name}>{name}</option>)}</select></label>
      <label>{t("Metric")}<select className="form-select" value={metric} onChange={e => setMetric(e.target.value)}>
        {choices.map(s => <option key={s.metric} value={s.metric}>{t(metrics[s.metric][0])}</option>)}</select></label>
      <label>{t("History interval")}<select className="form-select" value={windowMinutes} onChange={e => setWindowMinutes(Number(e.target.value))}>
        <option value="0">{t("Entire capture")}</option>{[5,15,60].map(n => <option key={n} value={n}>{t("{0} minutes", [n])}</option>)}
      </select></label>
    </div><TimeChart title={t(metrics[active.metric][0])} unit={metrics[active.metric][1]} points={within(active)} range={[start, end]} />
    {comparable.length > 1 && <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Compare sources")}>
      <table className="table table-sm"><caption>{t("Compare sources")} · {t(metrics[active.metric][0])} ({metrics[active.metric][1]})</caption>
        <thead><tr><th scope="col">{t("Source")}</th><th scope="col">{t("Samples")}</th>
          <th scope="col">{t("Latest value")}</th><th scope="col">{t("Maximum")}</th><th scope="col">{t("Time (UTC)")}</th></tr></thead>
        <tbody>{comparable.map(s => {
          const points = within(s), latest = points.at(-1);
          return <tr key={s.source}><th scope="row"><bdi>{s.source}</bdi></th><td>{points.length}</td><td>{latest?.value ?? t("Unknown")}</td>
            <td>{latest ? Math.max(...points.map(p => p.value)) : t("Unknown")}</td>
            <td><bdi>{latest ? new Date(latest.time).toISOString() : t("Unknown")}</bdi></td></tr>;
        })}</tbody>
      </table></div>}</>}
  </section>;
}
