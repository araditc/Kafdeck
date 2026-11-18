import { useCallback, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { kafdeckApi, type BrokerMetricAlert } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { describeObservation } from '../../app/operatorState.js';
import { StatusBadge } from '../../app/StatusBadge.js';
import { operationalTrendStatusKind } from '../../app/statusPresentation.js';
import { readViewProblemText, type ReadViewProblem } from '../readviews/readViewProblem.js';
import { useObservation, MonitoringActive } from './useObservation.js';
import { BarChart, TimeChart, type Sample } from './Chart.js';
import { MeasurementImport } from './MeasurementImport.js';
import { BrokerMetrics, SeriesView, SourceStatus } from './BrokerMetrics.js';
import { CatalogSelect, MetricSelect } from './CatalogSelect.js';
import { readViewError } from '../readviews/readViewProblem.js';
import { MonitoringControls, MetricCharts, BrokerCharts } from './MonitoringOverview.js';
import { topicCharts, partitionCharts } from './monitoringCharts.js';
import { AgentSetup } from './AgentSetup.js';
import { independentOffsetRates, comparableConsumerOffsets, type OffsetSample } from './offsetRates.js';

function Observation<T>({ state, children }: {
  state: { value?: T; problem?: ReadViewProblem }; children: (value: T) => ReactNode;
}) {
  if (state.problem) return <p role="status"><StatusBadge kind={state.problem.kind} /> {readViewProblemText(state.problem)}</p>;
  return state.value === undefined ? <p role="status">{t("Loading…")}</p> : <>{children(state.value)}</>;
}

function History({ clusterId, group, refresh, minutes }: {clusterId: string; group: string; refresh: number; minutes: number}) {
  const read = useCallback(async (signal: AbortSignal) => {
    const end = Date.now(), start = end - minutes * 60_000;
    const range = {from: new Date(start).toISOString(), to: new Date(end).toISOString()};
    const result = await kafdeckApi.getConsumerOperationalTrend(clusterId, group, 240, signal, range);
    return {result, start, end};
  }, [clusterId, group, minutes]);
  const state = useObservation(read, refresh, 60_000);
  return <Observation state={state}>{({result, start, end}) => <>
    <p><StatusBadge kind={operationalTrendStatusKind(result.state)} /> · <bdi>{result.provider}</bdi>
      {result.truncated && <> · {t("Truncated")} <bdi>{result.limitReason}</bdi></>}</p>
    <TimeChart title={t("Historical group lag")} unit={t("offsets")} range={[start, end]}
      points={result.points.filter(p => p.count > 0 && p.state !== 'unknown' && p.state !== 'unavailable')
        .map(p => ({time: Date.parse(p.observedAtUtc), value: p.average}))
        .filter(p => Number.isFinite(p.time) && p.time >= start && p.time <= end && Number.isFinite(p.value))
        .sort((a,b) => a.time - b.time)} />
  </>}</Observation>;
}

function GroupMonitoring({ clusterId, group, refresh }: {clusterId: string; group: string; refresh: number}) {
  const last = useRef<OffsetSample | null>(null);
  const live = useRef<{ lag: Sample[]; end: Sample[]; committed: Sample[] }>({lag: [], end: [], committed: []});
  const [minutes, setMinutes] = useState(60);
  const read = useCallback(async (signal: AbortSignal) => {
    try {
      const response = await kafdeckApi.getConsumerLag(clusterId, group, signal);
      signal.throwIfAborted();
      const at = Date.now();
      const current = {at, lag: response.data};
      const comparable=comparableConsumerOffsets(response.data,response.partial);
      const rates = last.current && comparable ? independentOffsetRates(last.current, current) : null;
      last.current = comparable ? current : null;
      const retain = (samples: Sample[], value: number | null) => {
        const next = samples.filter(sample => sample.time >= at - 15 * 60_000);
        if (value !== null && Number.isFinite(value) && value >= 0) next.push({time: at, value});
        return next.slice(-180);
      };
      live.current = {
        lag: retain(live.current.lag, response.partial || response.data.isPartial ? null : response.data.totalLag),
        end: retain(live.current.end, rates?.end ?? null),
        committed: retain(live.current.committed, rates?.committed ?? null),
      };
      return {response, at, rates, samples: live.current};
    } catch (err) {
      if (!signal.aborted) {
        last.current = null;
        live.current = {lag: [], end: [], committed: []};
      }
      throw err;
    }
  }, [clusterId, group]);
  const state = useObservation(read, refresh, 15_000);
  return <>
    <Observation state={state}>{({response, at, rates, samples}) => {
      const lag = response.data;
      const known = lag.partitions.filter(p => p.lag !== null && Number.isSafeInteger(p.lag) && p.lag >= 0)
        .sort((a,b) => b.lag! - a.lag!);
      const missingCommit=lag.partitions.filter(p=>p.committedOffset===null).length;
      const missingEnd=lag.partitions.filter(p=>p.endOffset===null).length;
      const allMissingCommit=lag.partitions.length>0 && missingCommit===lag.partitions.length;
      const missingLag=allMissingCommit?t("No committed offsets"):t("Offsets unavailable");
      const noRate=lag.partitions.length===0?t("No assigned or committed partitions"):t("Collecting comparable samples");
      return <>
        <p>{t("Read at {0}", [new Date(at).toISOString()])} · {response.partial || lag.isPartial ? t("Partial observation") : t("Current")}</p>
        <dl className="monitor-kpis">
          <div><dt>{t(lag.isPartial?"Known partition lag":"Total lag")}</dt><dd>{lag.totalLag ?? missingLag}</dd></div>
          <div><dt>{t("End offset growth / s")}</dt><dd>{rates?.end?.toFixed(1) ?? (missingEnd?t("End offsets unavailable"):noRate)}</dd></div>
          <div><dt>{t("Committed offset growth / s")}</dt><dd>{rates?.committed?.toFixed(1) ?? (missingCommit?t("No committed offsets"):noRate)}</dd></div>
        </dl>
        {missingCommit>0 && <p role="status">{t("No committed offset for {0} partitions. Lag and committed-offset growth require consumer commits; end-offset growth is measured independently.",[missingCommit])}</p>}
        {lag.limitations.length>0 && <details><summary>{t("Offset availability")}</summary><ul>{lag.limitations.map(l=><li key={l.code}>{l.safeMessage ?? l.message}</li>)}</ul></details>}
        <div className="monitor-grid">
          <TimeChart title={t("Group lag in this session")} unit={t("offsets")} points={samples.lag} />
          <TimeChart title={t("End offset growth / s")} unit={t("offsets/s")} points={samples.end} />
          <TimeChart title={t("Committed offset growth / s")} unit={t("offsets/s")} points={samples.committed} />
        </div>
        <BarChart title={t("Highest partition lag (up to 10)")} unit={t("offsets")}
          rows={known.slice(0,10).map(p => ({label: p.topic + ' / ' + p.partition, value: p.lag!}))} />
        <p className="small text-secondary">{t("Known lag for {0} of {1} returned partitions.", [known.length, lag.partitions.length])}</p>
      </>;
    }}</Observation>
    <div className="monitor-controls"><label>{t("History interval")}<select className="form-select" value={minutes}
      onChange={e => setMinutes(Number(e.target.value))}>{[15,60,360,1440].map(n =>
        <option key={n} value={n}>{t("{0} minutes", [n])}</option>)}</select></label></div>
    <History key={minutes} clusterId={clusterId} group={group} minutes={minutes} refresh={refresh} />
  </>;
}

function TopicMeasurements({clusterId,topic,partition,seconds,refresh}: {clusterId:string;topic:string;partition:string;seconds:number;refresh:number}) {
  const [metric,setMetric]=useState(''),[minutes,setMinutes]=useState(15);
  const [view,setView]=useState('charts');
  const filters=JSON.stringify(partition ? {topic,partition} : {topic});
  return <>
    <div className="monitor-controls"><label>{t("View")}<select className="form-select" value={view} onChange={e=>setView(e.target.value)}>
      <option value="charts">{t("Prepared charts")}</option><option value="explorer">{t("Metric explorer")}</option>
    </select></label>
    <label>{t("History interval")}<select className="form-select" value={minutes} onChange={e=>setMinutes(Number(e.target.value))}>
      {[15,60,360,1440].map(n=><option key={n} value={n}>{t("{0} minutes",[n])}</option>)}</select></label></div>
    {view==='charts' ? <MetricCharts charts={partition ? partitionCharts : topicCharts}
      aggregate={false} {...{clusterId,minutes,seconds,refresh,filters}}/> : <>
    <div className="monitor-controls">
      <MetricSelect {...{clusterId,topic,partition,refresh}} polling={seconds*1000} value={metric} onChange={setMetric}/>
    </div>
    {metric && <SeriesView key={JSON.stringify([metric,minutes])} instance="" {...{clusterId,metric,minutes,filters,refresh,seconds}}/>}
    </>}
  </>;
}

function Replication({ clusterId, topic, refresh, seconds }: {clusterId: string; topic: string; refresh: number;seconds:number}) {
  const [partition,setPartition]=useState('');
  const read = useCallback((signal: AbortSignal) => kafdeckApi.getTopic(clusterId, topic, signal), [clusterId, topic]);
  const state = useObservation(read, refresh, 30_000);
  return <Observation state={state}>{result => {
    const data = result.data;
    const leaders = new Map<number, number>();
    for (const p of data.partitions) {
      if (p.leaderBrokerId !== null && p.leaderBrokerId >= 0) leaders.set(p.leaderBrokerId, 1 + (leaders.get(p.leaderBrokerId) ?? 0));
    }
    return <>
      <p><bdi>{data.name}</bdi> · {describeObservation(result.observation)} · <bdi>{result.observation.observedAt}</bdi></p>
      <dl className="monitor-kpis"><div><dt>{t("Offline partitions")}</dt><dd>{data.offlinePartitionCount}</dd></div>
        <div><dt>{t("Under-replicated partitions")}</dt><dd>{data.underReplicatedPartitionCount}</dd></div></dl>
      <BarChart title={t("Partition leaders by broker")} unit={t("Partitions")}
        rows={[...leaders].sort(([a],[b]) => a-b).map(([broker, value]) => ({label: t("Broker {0}", [broker]), value}))} />
      <div className="monitor-controls"><label>{t("Partition")}<select className="form-select" value={partition}
        onChange={e=>setPartition(e.target.value)}>
        <option value="">{t("Topic-level metrics")}</option>
        {data.partitions.map(p=><option key={p.partitionId} value={p.partitionId}>{p.partitionId}</option>)}
      </select></label></div>
      <TopicMeasurements key={partition} {...{clusterId,topic,partition,refresh,seconds}}/>
    </>;
  }}</Observation>;
}

function TopicMonitoring({clusterId,refresh,manage,seconds}: {clusterId:string;refresh:number;manage:boolean;seconds:number}) {
  const [topic,setTopic]=useState(''),[revision,setRevision]=useState(0),[saving,setSaving]=useState(false),[error,setError]=useState('');
  const read=useCallback((signal:AbortSignal)=>kafdeckApi.monitoredTopics(clusterId,signal),[clusterId]);
  const selection=useObservation(read,revision+refresh,60000);
  const tracked=selection.value?.topics??[];
  const readTopics=useCallback(async(q:string,cursor:string|null,signal:AbortSignal)=>{
    const {data}=await kafdeckApi.listTopics(clusterId,q,cursor,signal);
    return {names:data.items.map(t=>t.name),next:data.nextCursor};
  },[clusterId]);
  async function toggle() {
    setSaving(true);setError('');
    try {await kafdeckApi.setTopicMonitoring(clusterId,topic,!tracked.includes(topic),new AbortController().signal);setRevision(n=>n+1);}
    catch(ex){setError(readViewProblemText(readViewError(ex)));}
    finally{setSaving(false);}
  }
  return <section className="card monitor-panel"><h3>{t("Topic and partition monitoring")}</h3>
    <div className="monitor-controls">
      <CatalogSelect label={t("Topic name")} searchLabel={t("Search topics")} placeholder={t("Choose a topic")}
        value={topic} onChange={setTopic} disabled={saving} read={readTopics}/>
      {topic && manage && selection.value && <button type="button" className="btn btn-outline-primary" disabled={saving} onClick={()=>void toggle()}>
        {tracked.includes(topic)?t("Stop monitoring this topic"):t("Monitor this topic")}
      </button>}
    </div>
    {tracked.length>0 && <div>{t("Monitored topics")} · {tracked.map(name=><button key={name} type="button" className="btn btn-sm btn-link"
      disabled={saving} onClick={()=>setTopic(name)}>{name}</button>)}</div>}
    {(error || selection.problem) && <p role="alert">{error || readViewProblemText(selection.problem!)}</p>}
    {topic && <Replication key={topic} {...{clusterId,topic,refresh,seconds}}/>}
  </section>;
}

function ConsumerMonitoring({clusterId,refresh}:{clusterId:string;refresh:number}) {
  const list = useCallback((signal: AbortSignal) => kafdeckApi.listConsumerGroups(clusterId, signal), [clusterId]);
  const groups = useObservation(list, refresh);
  const [group, setGroup] = useState('');
  return <section className="card monitor-panel"><h3>{t("Consumer activity")}</h3>
      <Observation state={groups}>{result => <>
        {result.partial && <p>{t("Partial observation")}</p>}
        {result.data.length ? <div className="monitor-controls"><label>{t("Consumer group")}
          <select className="form-select" value={result.data.some(g => g.groupId === group) ? group : ''} onChange={e => setGroup(e.target.value)}>
            <option value="">{t("Choose a group")}</option>
            {result.data.map(g => <option key={g.groupId}>{g.groupId}</option>)}</select></label></div>
          : <p>{t("No visible consumer groups.")}</p>}
        {group && result.data.some(g => g.groupId === group) && <GroupMonitoring key={group} group={group} clusterId={clusterId} refresh={refresh} />}
      </>}</Observation>
    </section>;
}

const monitoringTabs=[
  ['overview','Overview'],['brokers','Brokers'],['topics','Topics and partitions'],['consumers','Consumers'],
  ['alerts','Alerts'],['metrics','Metric explorer'],['sources','Sources']
] as const;
type MonitoringTab=typeof monitoringTabs[number][0];

export function MonitoringPage({clusterId,refresh,manage,logLinks=false}:{clusterId:string;refresh:number;manage:boolean;logLinks?:boolean}) {
  const [tab,setTab]=useState<MonitoringTab>('overview'),[visited,setVisited]=useState<MonitoringTab[]>(['overview']);
  const [seconds,setSeconds]=useState(30),[minutes,setMinutes]=useState(15),[revision,setRevision]=useState(0);
  const [inspection,setInspection]=useState<BrokerMetricAlert|null>(null);
  const update=refresh+revision;
  function activate(next:MonitoringTab) {
    setTab(next);setVisited(previous=>previous.includes(next)?previous:[...previous,next]);
  }
  function inspect(alert:BrokerMetricAlert) {setInspection(alert);activate('metrics');}
  return <div id="monitoring">
    <MonitoringControls {...{clusterId,manage,minutes}} showWindow={tab==='overview'||tab==='brokers'}
      refresh={update} onMinutes={setMinutes} onInterval={setSeconds}/>
    <div className="monitor-tabs" role="tablist" aria-label={t("Monitoring sections")} onKeyDown={e=>{
      const buttons=Array.from(e.currentTarget.querySelectorAll<HTMLButtonElement>('[role="tab"]'));
      const current=buttons.indexOf(e.target as HTMLButtonElement);
      const backwards=document.documentElement.dir==='rtl'?'ArrowRight':'ArrowLeft';
      let next=current;
      if(e.key==='Home') next=0;
      else if(e.key==='End') next=buttons.length-1;
      else if(e.key===backwards) next=(current+buttons.length-1)%buttons.length;
      else if(e.key==='ArrowLeft'||e.key==='ArrowRight') next=(current+1)%buttons.length;
      else return;
      e.preventDefault();buttons[next]?.focus();activate(monitoringTabs[next]![0]);
    }}>
      {monitoringTabs.map(([id,label])=><button type="button" role="tab" key={id} id={'monitor-tab-'+id}
        aria-selected={tab===id} aria-controls={'monitor-panel-'+id} tabIndex={tab===id?0:-1}
        onClick={()=>activate(id)}>{t(label)}</button>)}
    </div>
    {monitoringTabs.map(([id])=><div role="tabpanel" id={'monitor-panel-'+id} aria-labelledby={'monitor-tab-'+id}
      hidden={id!==tab} tabIndex={0} key={id}>
      <MonitoringActive.Provider value={id===tab}>{visited.includes(id) && <>
        {id==='overview' && <section className="card monitor-panel"><MetricCharts {...{clusterId,minutes,seconds}} refresh={update}/></section>}
        {id==='brokers' && <BrokerCharts {...{clusterId,minutes,seconds}} refresh={update}/>}
        {id==='topics' && <TopicMonitoring {...{clusterId,manage,seconds}} refresh={update}/>}
        {id==='consumers' && <ConsumerMonitoring clusterId={clusterId} refresh={update}/>}
        {(id==='alerts'||id==='metrics') && <BrokerMetrics view={id==='alerts'?'alerts':'explorer'}
          {...{clusterId,manage,seconds,inspection,logLinks}} refresh={update} onInspect={inspect}/>}
        {id==='metrics' && <details className="card monitor-panel"><summary>{t("Workload and broker measurements")}</summary><MeasurementImport/></details>}
        {id==='sources' && <><SourceStatus clusterId={clusterId} refresh={update}/>
          {manage && <AgentSetup clusterId={clusterId} onChanged={()=>setRevision(n=>n+1)}/>}</>}
      </>}</MonitoringActive.Provider>
    </div>)}
  </div>;
}
