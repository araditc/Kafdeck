import {useCallback, useContext, useEffect, useState} from 'react';
import {MonitoringActive} from './useObservation.js';
import {kafdeckApi} from '../../shared/api.js';
import {t} from '../../shared/i18n.js';
import {readViewError, readViewProblemText} from '../readviews/readViewProblem.js';

interface Page { names: string[]; next: string | null }
interface Props {
  label: string; searchLabel: string; placeholder: string; value: string;
  onChange: (value:string)=>void; disabled?: boolean; refresh?: number;polling?:number;
  read: (query:string,cursor:string|null,signal:AbortSignal)=>Promise<Page>;
}

export function CatalogSelect({label,searchLabel,placeholder,value,onChange,disabled,read,refresh=0,polling=0}:Props) {
  const active=useContext(MonitoringActive);
  const [query,setQuery]=useState(''),[cursor,setCursor]=useState<string|null>(null);
  const [page,setPage]=useState<Page>({names:[],next:null}),[busy,setBusy]=useState(true),[error,setError]=useState('');
  useEffect(()=>{
    if(!active) return;
    const controller=new AbortController();
    setBusy(true);setError('');
    let retry:number|undefined;
    async function fetchPage() {
      try {
        const result=await read(query.trim(),cursor,controller.signal);
        if (!controller.signal.aborted) setPage(previous=>({
          names:cursor ? [...new Set([...previous.names,...result.names])] : result.names,
          next:result.next
        }));
        if (!controller.signal.aborted && polling && !result.names.length && !cursor)
          retry=window.setTimeout(fetchPage,polling);
      } catch(ex) {
        if(!controller.signal.aborted) setError(readViewProblemText(readViewError(ex)));
      } finally {if (!controller.signal.aborted) setBusy(false);}
    }
    const timer=window.setTimeout(fetchPage,query && !cursor ? 200 : 0);
    return ()=>{window.clearTimeout(timer);window.clearTimeout(retry);controller.abort();};
  },[read,query,cursor,refresh,polling,active]);
  return <>
    <label>{searchLabel}<input type="search" className="form-control" maxLength={100} value={query}
      onChange={e=>{setQuery(e.target.value);setCursor(null);setPage({names:[],next:null});}}/></label>
    <label>{label}<select className="form-select" dir="ltr" value={value} disabled={disabled} onChange={e=>onChange(e.target.value)}>
      <option value="">{placeholder}</option>
      {value && !page.names.includes(value) && <option>{value}</option>}
      {page.names.map(name=><option key={name}>{name}</option>)}
    </select></label>
    {page.next && <button type="button" className="btn btn-outline-secondary" disabled={busy} onClick={()=>setCursor(page.next)}>{t("Load more")}</button>}
    {error && <span role="alert">{error}</span>}
    {!busy && !error && !page.names.length && <span role="status">{t("No matching results")}</span>}
  </>;
}

export function MetricSelect({clusterId,topic='',partition='',value,onChange,disabled=false,refresh=0,polling=0}:{
  clusterId:string;topic?:string;partition?:string;value:string;onChange:(v:string)=>void;disabled?:boolean;refresh?:number;polling?:number
}) {
  const read=useCallback(async(q:string,cursor:string|null,s:AbortSignal)=>{
    const result=await kafdeckApi.monitoringNames(clusterId,q,s,cursor??'',topic,partition);
    const names=result.state==='available'?result.names:[];
    return {names,next:result.state==='available' && result.truncated ? names.at(-1)??null : null};
  },[clusterId,topic,partition]);
  return <CatalogSelect key={JSON.stringify([clusterId,topic,partition])} label={t("Metric")} searchLabel={t("Filter metric names")}
    placeholder={t("Select a metric")} {...{value,onChange,disabled,read,refresh,polling}}/>;
}
