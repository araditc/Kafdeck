import { useCallback, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { kafdeckApi } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { describeObservation } from '../../app/operatorState.js';
import { StatusBadge } from '../../app/StatusBadge.js';
import { operationalTrendStatusKind } from '../../app/statusPresentation.js';
import { readViewProblemText, type ReadViewProblem } from '../readviews/readViewProblem.js';
import { useObservation } from './useObservation.js';
import { BarChart, TimeChart, type Sample } from './Chart.js';
import { MeasurementImport } from './MeasurementImport.js';
import { BrokerMetrics } from './BrokerMetrics.js';
import { MonitoringOverview } from './MonitoringOverview.js';
import { AgentSetup } from './AgentSetup.js';
import { offsetRates, type OffsetSample } from './offsetRates.js';

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
      const rates = last.current && !response.partial ? offsetRates(last.current, current) : null;
      last.current = response.partial ? null : current;
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
      return <>
        <p>{t("Read at {0}", [new Date(at).toISOString()])} · {response.partial || lag.isPartial ? t("Partial observation") : t("Current")}</p>
        <dl className="monitor-kpis">
          <div><dt>{t("Total lag")}</dt><dd>{lag.totalLag ?? t("Unknown")}</dd></div>
          <div><dt>{t("End offset growth / s")}</dt><dd>{rates?.end.toFixed(1) ?? t("Unknown")}</dd></div>
          <div><dt>{t("Committed offset growth / s")}</dt><dd>{rates?.committed.toFixed(1) ?? t("Unknown")}</dd></div>
        </dl>
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

function Replication({ clusterId, topic, refresh }: {clusterId: string; topic: string; refresh: number}) {
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
    </>;
  }}</Observation>;
}

export function MonitoringPage({ clusterId, refresh, manage }: {clusterId: string; refresh: number; manage: boolean}) {
  const [agentRevision,setAgentRevision]=useState(0);
  const [seconds,setSeconds]=useState(30);
  const list = useCallback((signal: AbortSignal) => kafdeckApi.listConsumerGroups(clusterId, signal), [clusterId]);
  const groups = useObservation(list, refresh);
  const [group, setGroup] = useState('');
  const [draft, setDraft] = useState('');
  const [topic, setTopic] = useState('');
  return <div id="monitoring">
    <MonitoringOverview clusterId={clusterId} manage={manage} refresh={refresh+agentRevision} onInterval={setSeconds}/>
    {manage && <AgentSetup clusterId={clusterId} onChanged={()=>setAgentRevision(n=>n+1)} />}
    <BrokerMetrics clusterId={clusterId} refresh={refresh+agentRevision} manage={manage} seconds={seconds} />
    <section className="card monitor-panel"><h3>{t("Consumer activity")}</h3>
      <Observation state={groups}>{result => <>
        {result.partial && <p>{t("Partial observation")}</p>}
        {result.data.length ? <div className="monitor-controls"><label>{t("Consumer group")}
          <select className="form-select" value={result.data.some(g => g.groupId === group) ? group : ''} onChange={e => setGroup(e.target.value)}>
            <option value="">{t("Choose a group")}</option>
            {result.data.map(g => <option key={g.groupId}>{g.groupId}</option>)}</select></label></div>
          : <p>{t("No visible consumer groups.")}</p>}
        {group && result.data.some(g => g.groupId === group) && <GroupMonitoring key={group} group={group} clusterId={clusterId} refresh={refresh} />}
      </>}</Observation>
    </section>
    <section className="card monitor-panel"><h3>{t("Topic replication")}</h3>
      <form className="monitor-controls" onSubmit={e => {e.preventDefault(); setTopic(draft.trim());}}>
        <label>{t("Topic name")}<input className="form-control" value={draft} maxLength={249} required onChange={e => setDraft(e.target.value)} /></label>
        <button className="btn btn-outline-primary" disabled={!draft.trim()}>{t("Inspect topic")}</button>
      </form>
      {topic && <Replication key={topic} topic={topic} clusterId={clusterId} refresh={refresh} />}
    </section>
    <MeasurementImport />
  </div>;
}
