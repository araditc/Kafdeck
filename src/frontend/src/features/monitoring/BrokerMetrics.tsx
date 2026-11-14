import { useCallback, useEffect, useRef, useState } from 'react';
import { kafdeckApi, type BrokerMetricAlert } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewProblemText } from '../readviews/readViewProblem.js';
import { useObservation } from './useObservation.js';
import { TimeChart } from './Chart.js';
import { AlertRules } from './AlertRules.js';

const investigations: Record<string, string> = {
  KafkaMetadataOfflinePartitions: 'Inspect partition leaders and controller events for unavailable replicas.',
  KafkaMetadataUnderReplicated: 'Check ISR membership, follower disk latency and replication traffic.',
  KafkaTargetDown: 'Check the Kafka process, agent credentials and network path to Kafdeck.',
  KafkaOfflinePartitions: 'Inspect partition leaders and controller events for unavailable replicas.',
  KafkaUnderReplicated: 'Check ISR membership, follower disk latency and replication traffic.',
  KafkaHandlerBusy: 'Compare request queue time with handler idle time and broker CPU.',
  KafkaBrokerHandlerBusy: 'Compare request queue time with handler idle time and broker CPU.',
  KafkaRequestSlow: 'Compare queue, local processing, replication wait and response send time on this broker.',
};
function advice(alert: BrokerMetricAlert) {
  return Object.hasOwn(investigations, alert.name) ? investigations[alert.name] : undefined;
}

const diagnostics = [
  ['Produce request queue p99', 'kafka.network:RequestMetrics/RequestQueueTimeMs/99thPercentile', 'Produce'],
  ['Produce local processing p99', 'kafka.network:RequestMetrics/LocalTimeMs/99thPercentile', 'Produce'],
  ['Produce remote wait p99', 'kafka.network:RequestMetrics/RemoteTimeMs/99thPercentile', 'Produce'],
  ['Produce response send p99', 'kafka.network:RequestMetrics/ResponseSendTimeMs/99thPercentile', 'Produce'],
  ['Disk flush p99', 'kafka.log:LogFlushStats/LogFlushRateAndTimeMs/99thPercentile', ''],
  ['Network processor idle fraction', 'kafka.network:SocketServer/NetworkProcessorAvgIdlePercent/Value', ''],
  ['ISR shrink rate', 'kafka.server:ReplicaManager/IsrShrinksPerSec/OneMinuteRate', ''],
] as const;

function SeriesView({clusterId, metric, minutes, instance, filters, refresh}: {
  clusterId: string; metric: string; minutes: number; instance: string; filters: string; refresh: number;
}) {
  const read = useCallback((signal: AbortSignal) => kafdeckApi.monitoringSeries(clusterId, metric, minutes, instance, filters, signal),
    [clusterId, metric, minutes, instance, filters]);
  const {value, problem} = useObservation(read, refresh, 30_000);
  const [selection, setSelection] = useState('');
  if (problem) return <p role="status">{readViewProblemText(problem)}</p>;
  if (!value) return <p role="status">{t("Loading…")}</p>;
  if (value.state === 'unconfigured') return <p>{t("Monitoring source is not configured for this cluster.")}</p>;
  const choices = value.rows.map(row => {
    const name = Object.keys(row.labels).sort().filter(k => k !== '__name__' && k !== 'kafdeck_cluster')
      .map(k => k+'='+JSON.stringify(row.labels[k])).join(', ');
    return {row, name};
  });
  const choice = choices.find(s => s.name === selection) ?? choices[0];
  if (!choice) return <p>{t("No measurements in this interval.")}</p>;
  return <>
    {(value.truncated || value.warning) && <p role="status">{t("Partial response. Narrow the instance or time range.")}</p>}
    <div className="monitor-controls"><label>{t("Series labels")}<select className="form-select" value={choice.name}
      onChange={e => setSelection(e.target.value)}>
      {choices.map(s => <option key={s.name} value={s.name}>{s.name || metric}</option>)}</select></label></div>
    <TimeChart title={metric} unit={t("Value")} range={[value.from*1000, value.to*1000]}
      points={choice.row.points.flatMap(p => p.value === null ? [] : [{time:p.time,value:p.value}])} />
  </>;
}

export function BrokerMetrics({clusterId, refresh, manage, seconds}: {clusterId: string; refresh: number; manage: boolean; seconds:number}) {
  const [input, setInput] = useState(''), [search, setSearch] = useState('');
  const [metric, setMetric] = useState(''), [minutes, setMinutes] = useState(60);
  const [draftInstance, setDraftInstance] = useState(''), [instance, setInstance] = useState('');
  const [notify, setNotify] = useState(false);
  const [showHistory,setShowHistory] = useState(false);
  const [rawFilters, setRawFilters] = useState(''), [filters, setFilters] = useState('');
  const [invalid, setInvalid] = useState(false);
  const delivered = useRef(new Set<string>());
  const alertsRead = useCallback((s: AbortSignal) => kafdeckApi.monitoringAlerts(clusterId, s), [clusterId]);
  const namesRead = useCallback((s: AbortSignal) => kafdeckApi.monitoringNames(clusterId, search, s), [clusterId, search]);
  const alerts = useObservation(alertsRead, refresh, Math.min(15,seconds)*1000, notify), names = useObservation(namesRead, refresh,60000);
  useEffect(() => {
    if (!notify || alerts.value?.state !== 'available' || Notification.permission !== 'granted') return;
    const firing = alerts.value.rows.filter(a => a.state === 'firing');
    const active = new Set<string>();
    for (const alert of firing) {
      const key = JSON.stringify([alert.name, alert.instance, alert.labels, alert.since]);
      active.add(key);
      if (!delivered.current.has(key)) {
        try { new Notification(alert.name, {body:[clusterId, alert.instance, alert.metric, String(alert.value)].join(' · '), tag:key}); }
        catch { setNotify(false); break; }
      }
    }
    delivered.current = active;
  }, [alerts.value, notify, clusterId]);
  return <section className="card monitor-panel"><h3>{t("Cluster alerts")}</h3>
    {alerts.problem && <p role="alert">{t("Alert status unavailable.")} {readViewProblemText(alerts.problem)}</p>}
    {!alerts.value && !alerts.problem && <p role="status">{t("Loading…")}</p>}
    {alerts.value?.state === 'unconfigured' && <p>{t("Monitoring source is not configured for this cluster.")}</p>}
    {alerts.value?.state === 'available' && <>
      <p>{t("Read at {0}", [new Date(alerts.value.observedAt*1000).toISOString()])}</p>
      <SourceStatus clusterId={clusterId} refresh={refresh} />
      {(alerts.value.truncated || alerts.value.warning) && <p>{t("Partial observation")}</p>}
      {alerts.value.rows.length === 0 && <p>{t("No active alerts returned by the configured rules.")}</p>}
      {alerts.value.rows.map((alert, i) => {
        const next = advice(alert);
        return <div className={'alert mb-0 ' + (alert.state === 'firing' ? 'alert-danger' : 'alert-warning')} key={i}>
          <strong><bdi>{alert.name}</bdi> · {alert.state === 'firing' ? t("Firing") : t("Pending")}</strong>
          <div><bdi>{alert.severity} · {alert.instance} · {alert.metric} · {JSON.stringify(alert.labels)}</bdi></div>
          <p>{t("Measured {0}, threshold {1}. First detected {2}.", [alert.value, alert.threshold, new Date(alert.since*1000).toISOString()])}</p>
          {next && <p>{t(next)}</p>}
          <button type="button" className="btn btn-sm btn-outline-primary"
            onClick={() => {setInput(alert.metric);setSearch(alert.metric);setMetric(alert.metric);const exact=JSON.stringify(alert.labels);
              setFilters(exact);setRawFilters(exact);setInvalid(false);setInstance(alert.instance);setDraftInstance(alert.instance);}}>
            {t("Inspect measurement")}</button>
        </div>;
      })}
      {typeof Notification !== 'undefined' && <button className="btn btn-outline-secondary justify-self-start" type="button"
        onClick={async () => {if (notify) {setNotify(false);delivered.current.clear();} else {
          try {setNotify((await Notification.requestPermission()) === 'granted');} catch { setNotify(false); }
        }}}>{notify ? t("Disable desktop alerts") : t("Enable desktop alerts while monitoring")}</button>}
    </>}
    {alerts.value?.state === 'available' && <details onToggle={e=>setShowHistory(e.currentTarget.open)}>
      <summary>{t("Alert history (24 hours)")}</summary>
      {showHistory && <IncidentHistory clusterId={clusterId} refresh={refresh} />}
    </details>}
    <h3>{t("Metric explorer")}</h3>
    {names.value?.state === 'available' && <div className="monitor-controls">
      <label>{t("Investigate broker latency")}<select className="form-select" value={diagnostics.some(d=>d[1]===metric)? metric : ''}
        onChange={e=>{
          const target=diagnostics.find(d=>d[1]===e.target.value);
          if (!target) return;
          const exact=target[2] ? JSON.stringify({request:target[2]}) : '{}';
          setInput(target[1]);setSearch(target[1]);setMetric(target[1]);setFilters(exact);setRawFilters(exact);setInvalid(false);
          setInstance(draftInstance.trim());
        }}>
        <option value="">{t("Choose an investigation")}</option>
        {diagnostics.map(([title,name])=><option value={name} key={name}>{t(title)}</option>)}
      </select></label></div>}
    <form className="monitor-controls" onSubmit={e => {
      e.preventDefault();
      try {
        const parsed = JSON.parse(rawFilters.trim() || '{}');
        if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object' || Object.keys(parsed).length > 8 ||
          Object.entries(parsed).some(([k,v]) => !/^[a-zA-Z_][a-zA-Z0-9_]{0,99}$/.test(k) || k.startsWith('__') ||
            k === 'instance' || k === 'kafdeck_cluster' || typeof v !== 'string' || v.length > 255)) throw Error();
        setFilters(JSON.stringify(parsed));setInvalid(false);
      } catch {setInvalid(true);return;}
      setSearch(input.trim());if (input.trim() !== search) setMetric('');setInstance(draftInstance.trim());
    }}>
      <label>{t("Filter metric names")}<input className="form-control" maxLength={100} value={input} onChange={e => setInput(e.target.value)} /></label>
      <label>{t("Instance (optional)")}<input className="form-control" maxLength={255} value={draftInstance} onChange={e => setDraftInstance(e.target.value)} /></label>
      <label>{t("Exact labels (JSON, optional)")}<input className="form-control" maxLength={2500} dir="ltr" placeholder='{"topic":"orders"}'
        value={rawFilters} onChange={e => setRawFilters(e.target.value)} /></label>
      <button className="btn btn-outline-primary">{t("Search")}</button>
    </form>
    {invalid && <p role="alert">{t("Use up to eight exact label and value pairs.")}</p>}
    {names.problem && <p role="status">{readViewProblemText(names.problem)}</p>}
    {names.value?.state === 'available' && <>
      {(names.value.truncated || names.value.warning) && <p>{t("Partial metric list. Refine the name filter.")}</p>}
      <div className="monitor-controls"><label>{t("Metric")}<select className="form-select" value={metric} onChange={e => setMetric(e.target.value)}>
        <option value="">{t("Select a metric")}</option>
        {metric && !names.value.names.includes(metric) && <option>{metric}</option>}
        {names.value.names.map(name => <option key={name}>{name}</option>)}
      </select></label><label>{t("History interval")}<select className="form-select" value={minutes}
        onChange={e => setMinutes(Number(e.target.value))}>{[15,60,360,1440].map(n => <option key={n} value={n}>{t("{0} minutes",[n])}</option>)}</select></label></div>
      {names.value.names.length === 0 && <p>{t("No matching metrics.")}</p>}
    </>}
    {metric && <SeriesView key={JSON.stringify([metric,instance,filters,minutes])} {...{clusterId,metric,minutes,instance,filters,refresh}} />}
    {manage && names.value?.state === 'available' && <AlertRules clusterId={clusterId} metric={metric} />}
  </section>;
}

function IncidentHistory({clusterId,refresh}: {clusterId:string;refresh:number}) {
  const read=useCallback((s: AbortSignal)=>kafdeckApi.monitoringIncidents(clusterId,s),[clusterId]);
  const result=useObservation(read,refresh,30_000);
  if(result.problem) return <p role="alert">{readViewProblemText(result.problem)}</p>;
  if(result.value?.state!=='available') return <p role="status">{t("Loading…")}</p>;
  if(!result.value.rows.length) return <p>{t("No alerts in this interval.")}</p>;
  return <>{result.value.truncated && <p>{t("Most recent 50 alerts")}</p>}
    <ul>{result.value.rows.map((r,i)=><li key={i}>
      <bdi>{r.name} · {r.instance} · {r.metric} · {JSON.stringify(r.labels)}</bdi>
      <div>{t("Fired at {0} with value {1} (threshold {2}).",[new Date(r.fired*1000).toISOString(),r.value,r.threshold])}
        {' · '}{!r.ended ? t("Firing") : <>{r.reason==='recovered' ? t("Recovered") : r.reason==='ruleChanged' ? t("Rule changed") : t("No recent data")} · <bdi>{new Date(r.ended*1000).toISOString()}</bdi></>}</div>
    </li>)}</ul></>;
}

function SourceStatus({clusterId,refresh}: {clusterId:string;refresh:number}) {
  const read=useCallback((s: AbortSignal)=>kafdeckApi.monitoringSources(clusterId,s),[clusterId]);
  const result=useObservation(read,refresh,30_000);
  const display={waiting:t("Awaiting first sample"),stale:t("No recent data"),partial:t("Partial observation"),current:t("Current")};
  if(result.problem) return <p role="status">{readViewProblemText(result.problem)}</p>;
  if(result.value?.state!=='available') return null;
  return <details><summary>{t("Reporting sources")}</summary>
    <ul>{result.value.rows.map(r=><li key={r.instance}><bdi>{r.instance}</bdi> · {display[r.health]}
      {r.seen!==null && <> · <time dir="ltr">{new Date(r.seen*1000).toISOString()}</time></>}</li>)}</ul></details>;
}
