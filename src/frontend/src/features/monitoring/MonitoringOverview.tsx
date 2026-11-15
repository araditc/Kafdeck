import { useCallback, useEffect, useRef, useState } from 'react';
import { kafdeckApi, type BrokerMetricSeries } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewError, readViewProblemText } from '../readviews/readViewProblem.js';
import { useObservation } from './useObservation.js';
import {type MonitoringChart,overviewCharts,brokerCharts} from './monitoringCharts.js';

const colors=['#1f6bad','#ac4813','#00826f','#8353a4','#765f0a','#595f69'];

function Trend({clusterId,chart,minutes,seconds,refresh,filters,aggregate,request}: {
  clusterId:string;chart:MonitoringChart;minutes:number;seconds:number;refresh:number;filters:string;aggregate:boolean;request:string
}) {
  const exact=chart.metric.includes('RequestMetrics') ? JSON.stringify({...JSON.parse(filters),request}) : filters;
  const read=useCallback((signal:AbortSignal)=>kafdeckApi.monitoringSeries(clusterId,chart.metric,minutes,'',
    exact,signal,aggregate),[clusterId,chart,minutes,exact,aggregate]);
  const {value,problem}=useObservation(read,refresh,seconds*1000);
  const rows=value?.state==='available'?value.rows.map(row=>({...row,
    points:row.points.map(p=>({...p,value:p.value===null || p.value<0 ? null : p.value*(chart.factor??1)}))})):[];
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
          {r.health==='stale' && <small>{t("No recent data")}</small>}</li>)}</ul>
        {value.state==='available' && value.truncated && <small>{t("Partial response. Narrow the instance or time range.")}</small>}
      </>}
  </figure>;
}

export function MetricCharts({clusterId,charts=overviewCharts,minutes,seconds,refresh,filters='{}',aggregate=true,request='Produce'}:{
  clusterId:string;charts?:MonitoringChart[];minutes:number;seconds:number;refresh:number;filters?:string;aggregate?:boolean;request?:string
}) {
  return <div className="monitor-grid">{charts.map(chart=><Trend key={chart.metric}
    {...{clusterId,chart,minutes,seconds,refresh,filters,aggregate,request}}/>)}</div>;
}

export function BrokerCharts({clusterId,minutes,seconds,refresh}:{clusterId:string;minutes:number;seconds:number;refresh:number}) {
  const [view,setView]=useState('Traffic'),[request,setRequest]=useState('Produce');
  return <section className="card monitor-panel">
    <div className="monitor-controls"><label>{t("Broker charts")}<select className="form-select" value={view} onChange={e=>setView(e.target.value)}>
      {Object.keys(brokerCharts).map(v=><option key={v} value={v}>{t(v)}</option>)}</select></label>
      {view==='Request latency' && <label>{t("Request type")}<select className="form-select" value={request} onChange={e=>setRequest(e.target.value)}>
        {['Produce','Fetch','FetchConsumer','FetchFollower'].map(r=><option key={r}>{r}</option>)}</select></label>}
    </div>
    {view==='Traffic' && <p className="small text-secondary">{t("Rates use Kafka's one-minute moving averages.")}</p>}
    {view==='Request latency' && request.startsWith('Fetch') && <p className="small text-secondary">{t("Fetch total time includes long polling.")}</p>}
    <MetricCharts charts={brokerCharts[view]!} {...{clusterId,minutes,seconds,refresh,request}}/>
  </section>;
}

export function MonitoringControls({clusterId,manage,refresh,onInterval,minutes,onMinutes,showWindow}: {
  clusterId:string;manage:boolean;refresh:number;onInterval:(v:number)=>void;minutes:number;onMinutes:(n:number)=>void;showWindow:boolean
}) {
  const [revision,setRevision]=useState(0),[saving,setSaving]=useState(false),[error,setError]=useState('');
  const controller=useRef<AbortController|null>(null);
  useEffect(()=>()=>controller.current?.abort(),[]);
  const read=useCallback((s:AbortSignal)=>kafdeckApi.monitoringSampling(clusterId,s),[clusterId]);
  const {value,problem}=useObservation(read,revision+refresh,60000);
  const seconds=value?.seconds??30;
  useEffect(()=>{onInterval(seconds)},[seconds,onInterval]);
  return <div className="monitor-toolbar">
    <div className="monitor-controls"><label>{t("Sampling interval")}<select className="form-select" value={seconds} disabled={!manage || !value || saving}
      onChange={async e=>{const ctl=new AbortController();controller.current=ctl;setSaving(true);setError('');
        try {await kafdeckApi.setMonitoringSampling(clusterId,Number(e.target.value),ctl.signal);if(!ctl.signal.aborted)setRevision(n=>n+1);}
        catch(ex){if(!ctl.signal.aborted)setError(readViewProblemText(readViewError(ex)));}
        finally{if(!ctl.signal.aborted)setSaving(false);}}}>
      {Array.from(new Set([10,15,20,30,45,60,seconds])).sort((a,b)=>a-b).map(n=><option key={n} value={n}>{t("{0} seconds",[n])}</option>)}
    </select></label>
    {showWindow && <label>{t("Chart window")}<select className="form-select" value={minutes} onChange={e=>onMinutes(Number(e.target.value))}>
      {[15,60,360,1440].map(n=><option key={n} value={n}>{t("{0} minutes",[n])}</option>)}</select></label>}</div>
    {(error||problem) && <p role="alert">{error||readViewProblemText(problem!)}</p>}
  </div>;
}
