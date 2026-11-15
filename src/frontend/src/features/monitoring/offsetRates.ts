import type { ConsumerLag } from '../../shared/api.js';

export type OffsetSample = { at: number; lag: ConsumerLag };
export function comparableConsumerOffsets(lag: ConsumerLag, partial: boolean) {
  return !(partial || lag.isPartial) || lag.limitations.length>0 &&
    lag.limitations.every(l=>l.code==='consumer_offset_missingcommittedoffset');
}

/** End-offset activity is still measurable before a consumer's first commit. */
export function independentOffsetRates(previous: OffsetSample, current: OffsetSample) {
  const seconds=(current.at-previous.at)/1000;
  if(seconds<=0 || seconds>90 || previous.lag.groupId!==current.lag.groupId ||
    !comparableConsumerOffsets(previous.lag,previous.lag.isPartial) || !comparableConsumerOffsets(current.lag,current.lag.isPartial) ||
    !current.lag.partitions.length || previous.lag.partitions.length!==current.lag.partitions.length) return null;
  const key=(topic:string,partition:number)=>JSON.stringify([topic,partition]);
  const old=new Map(previous.lag.partitions.map(p=>[key(p.topic,p.partition),p]));
  let end:number|null=0,committed:number|null=0;
  const delta=(a:number|null,b:number|null)=>a!==null && b!==null &&
    Number.isSafeInteger(a) && Number.isSafeInteger(b) && a>=0 && b>=a ? b-a : null;
  for(const p of current.lag.partitions) {
    const id=key(p.topic,p.partition),before=old.get(id);
    if(!before) return null;
    old.delete(id);
    const a=delta(before.endOffset,p.endOffset),b=delta(before.committedOffset,p.committedOffset);
    end=end===null || a===null ? null : end+a;
    committed=committed===null || b===null ? null : committed+b;
  }
  return {end:end!==null && Number.isSafeInteger(end)?end/seconds:null,
    committed:committed!==null && Number.isSafeInteger(committed)?committed/seconds:null};
}
export function offsetRates(previous: OffsetSample, current: OffsetSample) {
  const seconds = (current.at - previous.at) / 1000;
  if (seconds <= 0 || seconds > 90 || previous.lag.groupId !== current.lag.groupId ||
      previous.lag.isPartial || current.lag.isPartial || !current.lag.partitions.length ||
      previous.lag.partitions.length !== current.lag.partitions.length) return null;
  const key = (topic: string, partition: number) => JSON.stringify([topic, partition]);
  const offsets = new Map(previous.lag.partitions.map(p => [key(p.topic, p.partition), p]));
  let end = 0, committed = 0;
  for (const p of current.lag.partitions) {
    const id = key(p.topic, p.partition);
    const old = offsets.get(id);
    if (!old) return null;
    offsets.delete(id);
    const { endOffset: a, committedOffset: b } = old;
    const { endOffset: c, committedOffset: d } = p;
    if (a === null || b === null || c === null || d === null ||
        ![a,b,c,d].every(n => Number.isSafeInteger(n) && n >= 0) || c < a || d < b) return null;
    end += c - a;
    committed += d - b;
  }
  if (!Number.isSafeInteger(end) || !Number.isSafeInteger(committed)) return null;
  return { end: end / seconds, committed: committed / seconds };
}
