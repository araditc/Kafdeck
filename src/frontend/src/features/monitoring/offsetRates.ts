import type { ConsumerLag } from '../../shared/api.js';

export type OffsetSample = { at: number; lag: ConsumerLag };
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
