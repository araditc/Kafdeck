import { useCallback, useEffect, useRef, useState } from 'react';
import { kafdeckApi, type BrokerMetricSeries } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewError, readViewProblemText } from '../readviews/readViewProblem.js';
import { useObservation } from './useObservation.js';

const charts = [
  {title:'Broker CPU',metric:'kafdeck.jvm/cpu_percent',unit:'%'},
  {title:'GC time',metric:'kafdeck.jvm/gc_time_percent',unit:'%'},
  {title:'Produce request queue p99',metric:'kafka.network:RequestMetrics/RequestQueueTimeMs/99thPercentile',unit:'ms'},
  {title:'Metadata response time',metric:'kafka.metadata/response_ms',unit:'ms'},
  {title:'Offline partitions',metric:'kafka.metadata/offline_partitions',unit:'partitions'},
  {title:'Under-replicated partitions',metric:'kafka.metadata/under_replicated_partitions',unit:'partitions'},
] as const;
const colors=['#1f6bad','#ac4813','#00826f','#8353a4','#765f0a','#595f69'];

function Trend({clusterId,chart,minutes,seconds,refresh}: {clusterId:string;chart:typeof charts[number];minutes:number;seconds:number;refresh:number}) {
  const read=useCallback((signal:AbortSignal)=>kafdeckApi.monitoringSeries(clusterId,chart.metric,minutes,'',
    chart.unit==='ms' && chart.metric.includes('RequestMetrics') ? '{"request":"Produce"}' : '{}',signal),[clusterId,chart,minutes]);
  const {value,problem}=useObservation(read,refresh,seconds*1000);
  const rows=value?.state==='available'?value.rows:[];
  const latest=(r:BrokerMetricSeries)=>r.points.at(-1);
  const extent=Math.max(1,...rows.flatMap(r=>r.points.map(p=>p.value??0)));
  const scale=chart.unit==='%' ? Math.max(100,extent) : extent;
  const end=value?.state==='available'?value.to*1000:Date.now(), start=end-minutes*60000;
  const x=(time:number)=>4+432*(time-start)/(end-start), y=(v:number)=>90-84*v/scale;
  const gap=Math.max(120000,minutes*60000/120*3);
  const fmt=(n:number)=>n.toLocaleString(undefined,{maximumFractionDigits:2});
  return <figure className="monitor-trend" aria-label={t(chart.title)}>
    <figcaption><strong>{t(chart.title)}</strong> <span>{t(chart.unit)}</span></figcaption>
    {problem ? <p role="status">{readViewProblemText(problem)}</p> : !value ? <p>{t("Loading…")}</p> :
      rows.length===0 ? <p>{t("No measurements in this interval.")}</p> : <>
        <div className="monitor-axis" dir="ltr"><span>{fmt(scale)}</span><span>0</span>
        <svg viewBox="0 0 440 96" role="img" aria-label={t(chart.title)} preserveAspectRatio="none">
          <path d="M4 6H436 M4 48H436 M4 90H436" fill="none" stroke="currentColor" opacity=".18"/>
          {rows.map((r,i)=>{
            const pts=r.points.filter((p):p is {time:number;value:number}=>p.value!==null && p.value>=0);
            return <g key={JSON.stringify(r.labels)} stroke={colors[i%colors.length]}>
              <path fill="none" vectorEffect="non-scaling-stroke" strokeWidth="2" strokeDasharray={i%3===0?undefined:i%3===1?'7 3':'2 3'}
                d={pts.map((p,k)=>(k===0||p.time-pts[k-1]!.time>gap?'M':'L')+x(p.time)+' '+y(p.value)).join(' ')}/>
              {pts.map(p=><circle key={p.time} cx={x(p.time)} cy={y(p.value)} r="1.8" fill={colors[i%colors.length]}>
                <title>{r.labels.instance} · {new Date(p.time).toISOString()} · {fmt(p.value)} {chart.unit}</title></circle>)}
            </g>;})}
        </svg></div>
        <div className="d-flex justify-content-between small" dir="ltr"><time>{new Date(start).toLocaleTimeString()}</time><time>{new Date(end).toLocaleTimeString()}</time></div>
        <ul className="monitor-legend">{rows.map((r,i)=><li key={JSON.stringify(r.labels)}><i style={{background:colors[i%colors.length]}}/>
          <bdi>{r.labels.instance}</bdi> <strong>{latest(r)?.value==null?'—':fmt(latest(r)!.value!)}</strong>
          {latest(r) && end-latest(r)!.time>120000 && <small>{t("No recent data")}</small>}</li>)}</ul>
        {value.state==='available' && (value.truncated || value.warning) && <small>{t("Partial observation")}</small>}
      </>}
  </figure>;
}

export function MonitoringOverview({clusterId,manage,refresh,onInterval}: {clusterId:string;manage:boolean;refresh:number;onInterval:(v:number)=>void}) {
  const [minutes,setMinutes]=useState(15),[revision,setRevision]=useState(0),[saving,setSaving]=useState(false),[error,setError]=useState('');
  const controller=useRef<AbortController|null>(null);
  useEffect(()=>()=>controller.current?.abort(),[]);
  const read=useCallback((s:AbortSignal)=>kafdeckApi.monitoringSampling(clusterId,s),[clusterId]);
  const {value,problem}=useObservation(read,revision+refresh,60000);
  const seconds=value?.seconds??30;
  useEffect(()=>{onInterval(seconds)},[seconds,onInterval]);
  return <section className="card monitor-panel"><h3>{t("Monitoring overview")}</h3>
    <div className="monitor-controls"><label>{t("Sampling interval")}<select className="form-select" value={seconds} disabled={!manage || !value || saving}
      onChange={async e=>{const ctl=new AbortController();controller.current=ctl;setSaving(true);setError('');
        try {await kafdeckApi.setMonitoringSampling(clusterId,Number(e.target.value),ctl.signal);if(!ctl.signal.aborted)setRevision(n=>n+1);}
        catch(ex){if(!ctl.signal.aborted)setError(readViewProblemText(readViewError(ex)));}
        finally{if(!ctl.signal.aborted)setSaving(false);}}}>
      {Array.from(new Set([10,15,20,30,45,60,seconds])).sort((a,b)=>a-b).map(n=><option key={n} value={n}>{t("{0} seconds",[n])}</option>)}
    </select></label>
    <label>{t("Chart window")}<select className="form-select" value={minutes} onChange={e=>setMinutes(Number(e.target.value))}>
      {[15,60,360,1440].map(n=><option key={n} value={n}>{t("{0} minutes",[n])}</option>)}</select></label></div>
    {(error||problem) && <p role="alert">{error||readViewProblemText(problem!)}</p>}
    <div className="monitor-grid">{charts.map(chart=><Trend key={chart.metric} {...{chart,minutes,seconds,clusterId,refresh}}/>)}</div>
  </section>;
}
