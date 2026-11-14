import { useEffect, useState } from 'react';
import { readViewError, type ReadViewProblem } from '../readviews/readViewProblem.js';

type State<T> = { value?: T; problem?: ReadViewProblem; received?: number };

/** Callers memoize read and key the owning panel by resource and authority. */
export function useObservation<T>(read: (signal: AbortSignal) => Promise<T>, refresh: number, cadence = 0, background = false) {
  const [state, setState] = useState<State<T>>({});
  useEffect(() => {
    const controller = new AbortController();
    let timer: number;
    async function tick() {
      try {
        const value = await read(controller.signal);
        if (!controller.signal.aborted) setState({value, received: Date.now()});
      } catch (reason) {
        if (!controller.signal.aborted) setState({problem: readViewError(reason)});
      } finally {
        if (!controller.signal.aborted && cadence) timer = window.setTimeout(schedule, cadence);
      }
    }
    function schedule() {
      if (background || document.visibilityState === 'visible') void tick();
      else timer = window.setTimeout(schedule, cadence);
    }
    void tick();
    return () => { controller.abort(); window.clearTimeout(timer); };
  }, [read, refresh, cadence, background]);
  return state;
}
