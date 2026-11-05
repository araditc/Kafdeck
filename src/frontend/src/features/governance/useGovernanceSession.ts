import { useEffect, useRef, useState } from 'react';
import { governanceProblem } from './governanceApi.js';

/** Every scope owns one abort lifetime; an old response cannot restore a previous operator's material. */
export function useGovernanceSession(scope: string, reset: () => void, initial: (signal: AbortSignal) => Promise<void>) {
  const lifetime=useRef<AbortController | null>(null), epoch=useRef(0), inFlight=useRef(false);
  const [busy,setBusy]=useState(false), [error,setError]=useState<string | null>(null);
  useEffect(()=>{ const controller=new AbortController(); lifetime.current=controller; epoch.current++; inFlight.current=false; setBusy(false); setError(null); reset();
    void initial(controller.signal).catch(reason=>{if(!controller.signal.aborted)setError(governanceProblem(reason));});
    return ()=>controller.abort();
  },[scope]);
  async function run(action:(signal:AbortSignal)=>Promise<void>) {
    if(!lifetime.current||inFlight.current)return; const current=epoch.current, signal=lifetime.current.signal;
    inFlight.current=true;setBusy(true);setError(null);
    try{await action(signal);}catch(reason){if(!signal.aborted)setError(governanceProblem(reason));}
    finally{if(epoch.current===current){inFlight.current=false;if(!signal.aborted)setBusy(false);}}
  }
  return {busy,error,run};
}
