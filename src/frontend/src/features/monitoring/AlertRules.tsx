import { useCallback, useEffect, useRef, useState } from 'react';
import { kafdeckApi, type BrokerAlertRule } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewError, readViewProblemText, type ReadViewProblem } from '../readviews/readViewProblem.js';
import { useObservation } from './useObservation.js';

export function AlertRules({clusterId,metric}: {clusterId:string;metric:string}) {
  const empty: BrokerAlertRule = {name:'',metric:'',threshold:0,below:false,seconds:60,severity:'warning'};
  const [draft,setDraft]=useState(empty), [editing,setEditing]=useState(false);
  const [metricInput,setMetricInput]=useState<string|null>(null);
  const [revision,setRevision]=useState(0), [busy,setBusy]=useState(false);
  const [message,setMessage]=useState(''), [problem,setProblem]=useState<ReadViewProblem>();
  const request=useRef<AbortController | null>(null);
  useEffect(()=>()=>request.current?.abort(),[]);
  const read=useCallback((s: AbortSignal)=>kafdeckApi.monitoringRules(clusterId,s),[clusterId]);
  const result=useObservation(read, revision);
  const rules=result.value?.state==='available' ? result.value.rows : [];
  const selectedMetric=metricInput ?? metric;
  async function change(remove: boolean) {
    if (busy || (!remove && (!selectedMetric.trim() || !editing && rules.some(r=>r.name===draft.name.trim())))) return;
    const ctl=new AbortController(); request.current=ctl;
    setBusy(true); setMessage(''); setProblem(undefined);
    try {
      await kafdeckApi.changeMonitoringRule(clusterId,{...draft,name:draft.name.trim(),metric:selectedMetric.trim()},remove,ctl.signal);
      if (!ctl.signal.aborted) {
        setMessage(remove ? t("Rule removed.") : t("Rule saved.")); setEditing(false);setDraft(empty);setMetricInput(null);setRevision(n=>n+1);
      }
    } catch (ex) { if (!ctl.signal.aborted) setProblem(readViewError(ex)); }
    finally { if (!ctl.signal.aborted) setBusy(false); }
  }
  return <details><summary>{t("Custom alert rules")}</summary>
    {result.problem && <p role="alert">{readViewProblemText(result.problem)}</p>}
    <form className="monitor-controls py-2" onSubmit={e=>{e.preventDefault();void change(false);}}>
      <label>{t("Custom alert rules")}<select className="form-select" disabled={busy} value={editing ? draft.name : ''}
        onChange={e=>{const rule=rules.find(r=>r.name===e.target.value);setDraft(rule ?? empty);setMetricInput(rule?.metric ?? null);setEditing(!!rule);setMessage('');}}>
        <option value="">{t("New rule")}</option>{rules.map(r=><option key={r.name}>{r.name}</option>)}</select></label>
      <label>{t("Rule name")}<input required className="form-control" maxLength={100} readOnly={editing} disabled={busy} value={draft.name}
        onChange={e=>setDraft({...draft,name:e.target.value})}/></label>
      <label>{t("Metric")}<input required className="form-control" dir="ltr" maxLength={512} disabled={busy} value={selectedMetric}
        onChange={e=>setMetricInput(e.target.value)}/></label>
      <label>{t("Comparison")}<select className="form-select" disabled={busy} value={draft.below ? 'below':'above'}
        onChange={e=>setDraft({...draft,below:e.target.value==='below'})}><option value="above">{t("Above")}</option><option value="below">{t("Below")}</option></select></label>
      <label>{t("Threshold")}<input required className="form-control" type="number" step="any" disabled={busy} value={Number.isFinite(draft.threshold)?draft.threshold:''}
        onChange={e=>setDraft({...draft,threshold:e.target.valueAsNumber})}/></label>
      <label>{t("Duration (seconds)")}<input required className="form-control" type="number" min="0" max="86400" step="1" disabled={busy}
        value={Number.isFinite(draft.seconds)?draft.seconds:''} onChange={e=>setDraft({...draft,seconds:e.target.valueAsNumber})}/></label>
      <label>{t("Severity")}<select className="form-select" disabled={busy} value={draft.severity} onChange={e=>setDraft({...draft,severity:e.target.value})}>
        <option value="warning">{t("Warning")}</option><option value="critical">{t("Critical")}</option></select></label>
      <button className="btn btn-primary" disabled={busy || !result.value || !editing && rules.some(r=>r.name===draft.name.trim())}>{t("Save rule")}</button>
      {editing && <button className="btn btn-outline-danger" type="button" disabled={busy} onClick={()=>void change(true)}>{t("Remove rule")}</button>}
    </form>
    {problem && <p role="alert">{readViewProblemText(problem)}</p>}
    {message && <p role="status">{message}</p>}
  </details>;
}
