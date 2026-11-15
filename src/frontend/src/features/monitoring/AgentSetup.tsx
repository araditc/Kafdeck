import { useEffect, useRef, useState } from 'react';
import { kafdeckApi } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewError, readViewProblemText, type ReadViewProblem } from '../readviews/readViewProblem.js';

export function AgentSetup({clusterId,onChanged}: {clusterId:string;onChanged:()=>void}) {
  const [instance,setInstance]=useState(''),[action,setAction]=useState('create');
  const [confirmed,setConfirmed]=useState(false),[busy,setBusy]=useState(false);
  const [token,setToken]=useState(''),[error,setError]=useState<ReadViewProblem | null>(null);
  const request=useRef<AbortController|null>(null);
  useEffect(()=>()=>request.current?.abort(),[]);
  return <details className="card monitor-panel" onToggle={e=>{if(!e.currentTarget.open){request.current?.abort();setBusy(false);setToken('');}}}>
    <summary>{t("Manage monitoring sources")}</summary>
    <a className="justify-self-start" href={'/api/v1/clusters/'+encodeURIComponent(clusterId)+'/monitoring/agent.jar'} download="kafdeck-kafka-agent.jar">{t("Download Kafka agent")}</a>
    <form className="monitor-controls" onSubmit={async e=>{
      e.preventDefault();if (busy || (action!=='create' && !confirmed)) return;
      const ctl=new AbortController();request.current=ctl;setToken('');setError(null);setBusy(true);
      try{
        const value=await kafdeckApi.enrollMonitor(clusterId,instance.trim(),action,ctl.signal);
        if(!ctl.signal.aborted){setToken(value.token ?? '');setConfirmed(false);onChanged();}
      }catch(ex){if(!ctl.signal.aborted)setError(readViewError(ex));}
      finally{if(!ctl.signal.aborted)setBusy(false);}
    }}>
      <label>{t("Broker or client instance")}<input className="form-control" required maxLength={160} disabled={busy} value={instance}
        onChange={e=>{setInstance(e.target.value);setToken('');setConfirmed(false);}} /></label>
      <label>{t("Action")}<select className="form-select" disabled={busy} value={action}
        onChange={e=>{setAction(e.target.value);setToken('');setConfirmed(false);}}>
        <option value="create">{t("Register agent")}</option><option value="rotate">{t("Rotate agent key")}</option>
        <option value="revoke">{t("Revoke agent and erase its history")}</option></select></label>
      {action!=='create' && <label><input type="checkbox" checked={confirmed} onChange={e=>setConfirmed(e.target.checked)} />
        {t("Confirm the change for {0}",[instance])}</label>}
      <button type="submit" className="btn btn-primary" disabled={busy || !instance.trim() || (action!=='create'&&!confirmed)}>{t("Apply")}</button>
    </form>
    {error && <p role="alert">{readViewProblemText(error)}</p>}
    {token && <><p>{t("Save this key in the agent token file on this broker. It is shown once.")}</p>
      <code dir="ltr" className="text-break">{token}</code><p>{t("Agent properties")}</p>
      <pre dir="ltr">{'endpoint='+window.location.origin+'/monitoring/ingest\ntokenFile=/etc/kafdeck/agent-token\nintervalSeconds=30'}</pre>
      <p>{t("Add this option to the Kafka JVM and restart the broker during its maintenance window.")}</p>
      <pre dir="ltr">-javaagent:/opt/kafdeck/kafdeck-kafka-agent.jar=/etc/kafdeck/agent.properties</pre>
      <p>{t("For a librdkafka client, send statistics callback JSON to this URL using its own source key.")}</p>
      <pre dir="ltr">{window.location.origin+'/monitoring/client-stats'}</pre></>}
  </details>;
}
