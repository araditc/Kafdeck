import type { Sample } from './Chart.js';

// Metric, label, unit. Counts are observations with the semantics documented in the guide.
export const metrics = {
  receivedRate: ['Received rate', 'records/s'], completedRate: ['Completion rate', 'records/s'],
  active: ['Active workers', 'workers'], queueDepth: ['Queue depth', 'records'],
  queueWaitP95: ['Queue wait p95', 'ms'], itemMean: ['Mean item time', 'ms'],
  publishMean: ['Mean publish acknowledgement', 'ms'], publishMax: ['Maximum publish acknowledgement', 'ms'],
  commitMean: ['Mean commit latency', 'ms'], commitMax: ['Maximum commit latency', 'ms'],
  errors: ['Errors in interval', 'events'], retries: ['Retries in interval', 'events'],
  cpu: ['CPU usage', '%'], gcPause: ['GC pause time', '%'], heap: ['Heap size', 'MiB'],
  allocation: ['Allocation rate', 'MiB/s'], pending: ['Pending work', 'items'],
  rttMean: ['Mean broker RTT', 'ms'], rttP99: ['Broker RTT p99', 'ms'], rttMax: ['Maximum broker RTT', 'ms'],
  internalLatency: ['Producer internal latency', 'ms'], outbufLatency: ['Output buffer latency', 'ms'],
  waitResponses: ['Requests awaiting response', 'requests'], replyQueue: ['Client callback queue', 'events'],
  rebalances: ['Rebalances since client start', 'events'], fetchQueue: ['Fetch queue', 'records'],
  requestQueue: ['Broker request queue time', 'ms'], localTime: ['Broker local processing time', 'ms'],
  remoteTime: ['Broker remote processing time', 'ms'], responseSend: ['Broker response send time', 'ms'],
  isrShrinks: ['ISR shrink rate', 'events/s'], handlerIdle: ['Request handler idle time', '%'],
  offlinePartitions: ['Offline partitions', 'partitions'], underReplicated: ['Under-replicated partitions', 'partitions'],
  bytesIn: ['Broker ingress', 'bytes/s'], bytesOut: ['Broker egress', 'bytes/s'],
  diskFlush: ['Disk flush latency', 'ms'],
} as const;
export type Metric = keyof typeof metrics;
export type Series = { source: string; metric: Metric; points: Sample[] };
export const maxImportBytes = 8 * 1024 * 1024;

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
}
function identifier(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 160 && !/[\u0000-\u001f\u007f\u202a-\u202e\u2066-\u2069]/u.test(value);
}
function measured(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}
export class ImportIssue extends Error {
  constructor(readonly kind: 'format' | 'limit' | 'duplicate', readonly line: number) { super(kind); }
}

/** Parse only numeric observations. Unrecognized fields (including payloads) are never retained. */
export function parseMeasurements(text: string): Series[] {
  if (text.length > maxImportBytes) throw new ImportIssue('limit', 0);
  const series = new Map<string, Series>();
  const seen = new Set<string>();
  let count = 0;
  let line = 0;
  function add(source: string, metric: Metric, time: number, value: number) {
    const key = JSON.stringify([source, metric]);
    const sampleKey = key + '/' + time;
    if (seen.has(sampleKey)) throw new ImportIssue('duplicate', line);
    seen.add(sampleKey);
    let entry = series.get(key);
    if (!entry) {
      entry = {source, metric, points: []};
      series.set(key, entry);
    }
    if (++count > 20_000 || series.size > 256 || entry.points.length >= 2000) throw new ImportIssue('limit', line);
    entry.points.push({time, value});
  }
  for (const raw of text.trim().split(/\r?\n/)) {
    line++;
    if (!raw.trim()) continue;
    if (line > 20_000) throw new ImportIssue('limit', line);
    let row;
    try { row = record(JSON.parse(raw)); } catch { throw new ImportIssue('format', line); }
    if (!row) throw new ImportIssue('format', line);
    if (row.type !== 'producer' && row.type !== 'consumer') {
      if (!identifier(row.source) || typeof row.time !== 'string' || !/^\d{4}-\d\d-\d\dT.+(?:Z|[+-]\d\d:\d\d)$/i.test(row.time)
        || !Number.isFinite(Date.parse(row.time)) || typeof row.metric !== 'string' || !Object.hasOwn(metrics, row.metric)
        || !measured(row.value)) throw new ImportIssue('format', line);
      add(row.source, row.metric as Metric, Date.parse(row.time), row.value);
      continue;
    }
    if (!measured(row.time) || !Number.isSafeInteger(row.time) || row.time > 253402300799 || !identifier(row.name))
      throw new ImportIssue('format', line);
    const ms = row.time * 1000;
    const client = row.name;
    const before = count;
    if (measured(row.replyq)) add(client, 'replyQueue', ms, row.replyq);
    const group = record(row.cgrp);
    if (measured(group?.rebalance_cnt)) add(client, 'rebalances', ms, group.rebalance_cnt);
    for (const item of Object.values(record(row.brokers) ?? {})) {
      const b = record(item);
      if (!b || !measured(b.nodeid) || !Number.isSafeInteger(b.nodeid)) continue;
      const source = client + ' / broker ' + b.nodeid;
      if (measured(b.waitresp_cnt)) add(source, 'waitResponses', ms, b.waitresp_cnt);
      const windows = [['rtt', 'avg', 'rttMean'], ['rtt', 'p99', 'rttP99'], ['rtt', 'max', 'rttMax'],
        ['int_latency', 'avg', 'internalLatency'], ['outbuf_latency', 'avg', 'outbufLatency']] as const;
      for (const [field, statistic, metric] of windows) {
        const window = record(b[field]);
        const v = window?.[statistic];
        if (measured(window?.cnt) && window.cnt > 0 && measured(v)) add(source, metric, ms, v / 1000);
      }
    }
    if (count === before) throw new ImportIssue('format', line);
  }
  if (count === 0) throw new ImportIssue('format', line);
  for (const entry of series.values()) entry.points.sort((a, b) => a.time - b.time);
  return [...series.values()];
}
