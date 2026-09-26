import { useEffect, useMemo, useRef, useState } from 'react';
import { ApiProblem, kafdeckApi, type RecordAnchorProjection, type RecordQuery, type RecordSafePage, type RecordSafeProjection } from '../../shared/api.js';

interface Props { clusterId: string; topicName: string; partitions: number[]; }
export type ViewMode = 'structured' | 'text' | 'hex';

export function withRecordProjectionPreference(query: RecordQuery, mode: ViewMode): RecordQuery {
  return { ...query, decode: mode === 'structured' };
}

export function activeRecordPageQuery(displayedQuery: RecordQuery | null, baseQuery: RecordQuery): RecordQuery {
  return displayedQuery ?? baseQuery;
}

function recordError(reason: unknown) {
  if (reason instanceof ApiProblem && reason.status === 403 && reason.code === 'urn:kafdeck:problem:operator-authorization-denied') return 'Record access is denied by Kafdeck policy.';
  if (reason instanceof ApiProblem && reason.status === 403) return 'Kafka denied record access for this target.';
  if (reason instanceof ApiProblem && reason.status === 501) return 'Record browsing is unsupported for this target.';
  return reason instanceof Error ? reason.message : 'Record data could not be loaded.';
}

function isAbort(reason: unknown) {
  return reason instanceof DOMException && reason.name === 'AbortError';
}

function fromBase64(value: string): Uint8Array {
  const binary = atob(value); const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function rawText(value: string | null) {
  if (!value) return '';
  try { return new TextDecoder('utf-8', { fatal: false }).decode(fromBase64(value)); } catch { return '[binary data]'; }
}

function rawHex(value: string | null) {
  if (!value) return '';
  try { return Array.from(fromBase64(value)).map(byte => byte.toString(16).padStart(2, '0')).join(' '); } catch { return '[binary data]'; }
}

function anchorQuery(anchor: RecordAnchorProjection | null): Partial<RecordQuery> {
  if (!anchor) return {};
  const kind = anchor.kind.toLowerCase();
  if (kind === 'offset' && anchor.offset !== null) return { offset: anchor.offset };
  if (kind === 'timestamp' && anchor.timestampUtc) return { timestampUtc: anchor.timestampUtc };
  return { anchor: kind === 'latest' ? 'latest' : 'earliest' };
}

function RecordValue({ record, mode }: { record: RecordSafeProjection; mode: ViewMode }) {
  if (record.valueKind === 'fullyRedacted') return <em>Payload fully redacted by policy.</em>;
  if (mode === 'structured') {
    if (record.structuredValue !== null) return <pre>{JSON.stringify(record.structuredValue, null, 2)}</pre>;
    return <em>Structured decoding unavailable for this record.</em>;
  }
  if (!record.rawValue) return <em>Raw representation withheld by masking policy.</em>;
  return <pre>{mode === 'hex' ? rawHex(record.rawValue) : rawText(record.rawValue)}</pre>;
}

export function RecordExplorer({ clusterId, topicName, partitions }: Props) {
  const [partition, setPartition] = useState(partitions[0] ?? 0);
  const [anchorKind, setAnchorKind] = useState<'earliest' | 'latest' | 'offset' | 'timestamp'>('latest');
  const [anchorValue, setAnchorValue] = useState('');
  const [maxRecords, setMaxRecords] = useState(100);
  const [keyPrefix, setKeyPrefix] = useState('');
  const [filter, setFilter] = useState('');
  const [filterLanguage, setFilterLanguage] = useState<'cel' | 'jq'>('cel');
  const [viewMode, setViewMode] = useState<ViewMode>('structured');
  const [page, setPage] = useState<RecordSafePage | null>(null);
  const [displayedQuery, setDisplayedQuery] = useState<RecordQuery | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tailing, setTailing] = useState(false);
  const browseController = useRef<AbortController | null>(null);
  const tailController = useRef<AbortController | null>(null);

  function stopBrowse() {
    browseController.current?.abort();
    browseController.current = null;
    setLoading(false);
  }

  function stopTail() {
    tailController.current?.abort(); tailController.current = null; setTailing(false);
  }

  useEffect(() => {
    stopBrowse();
    if (!partitions.includes(partition)) setPartition(partitions[0] ?? 0);
    setPage(null); setDisplayedQuery(null); setError(null); stopTail();
    // Topic/cluster changes invalidate all payload state.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clusterId, topicName, partitions.join(',')]);

  useEffect(() => () => {
    browseController.current?.abort();
    tailController.current?.abort();
  }, []);

  const baseQuery = useMemo<RecordQuery>(() => {
    const anchor: Partial<RecordQuery> = anchorKind === 'offset'
      ? { offset: Number(anchorValue || '0') }
      : anchorKind === 'timestamp'
        ? { timestampUtc: anchorValue ? new Date(anchorValue).toISOString() : new Date().toISOString() }
        : { anchor: anchorKind };
    return withRecordProjectionPreference({
      partition,
      ...anchor,
      direction: 'forward',
      maxRecords,
      ...(keyPrefix ? { keyPrefix } : {}),
      ...(filter ? { filter, filterLanguage } : {}),
    }, viewMode);
  }, [anchorKind, anchorValue, filter, filterLanguage, keyPrefix, maxRecords, partition, viewMode]);

  async function load(query: RecordQuery) {
    stopBrowse();
    const controller = new AbortController();
    browseController.current = controller;
    setLoading(true); setError(null);
    try {
      const result = await kafdeckApi.getRecords(clusterId, topicName, query, controller.signal);
      if (browseController.current === controller) {
        setPage(result);
        setDisplayedQuery(query);
      }
    } catch (reason) {
      if (!isAbort(reason) && browseController.current === controller) setError(recordError(reason));
    } finally {
      if (browseController.current === controller) {
        browseController.current = null;
        setLoading(false);
      }
    }
  }

  async function startTail() {
    stopTail();
    const controller = new AbortController(); tailController.current = controller; setTailing(true); setError(null);
    const { anchor: _anchor, offset: _offset, timestampUtc: _timestampUtc, ...tailBase } = baseQuery;
    const tailQuery: RecordQuery = { ...tailBase, anchor: 'latest' };
    try {
      await kafdeckApi.tailRecords(clusterId, topicName, tailQuery, frame => {
        if (tailController.current !== controller) return;
        if (frame.kind === 'records' && frame.page) {
          setDisplayedQuery(tailQuery);
          setPage(previous => previous ? { ...frame.page!, records: [...previous.records, ...frame.page!.records].slice(-maxRecords) } : frame.page);
        } else if (frame.kind === 'error' && frame.failure) setError(frame.failure.message);
        else if (frame.kind === 'admissionDenied') setError('Live-tail admission limit reached.');
        else if (frame.kind === 'completed') setTailing(false);
      }, controller.signal);
    } catch (reason) {
      if (!isAbort(reason) && tailController.current === controller) setError(recordError(reason));
    } finally {
      if (tailController.current === controller) { tailController.current = null; setTailing(false); }
    }
  }

  async function exportPage(format: 'json' | 'ndjson' | 'csv') {
    setError(null);
    try {
      const blob = await kafdeckApi.exportRecords(
        clusterId,
        topicName,
        activeRecordPageQuery(displayedQuery, baseQuery),
        format);
      const url = URL.createObjectURL(blob); const link = document.createElement('a');
      link.href = url; link.download = `${topicName}-${partition}.${format === 'ndjson' ? 'ndjson' : format}`;
      document.body.appendChild(link); link.click(); link.remove(); URL.revokeObjectURL(url);
    } catch (reason) { setError(recordError(reason)); }
  }

  const pageQuery = activeRecordPageQuery(displayedQuery, baseQuery);
  const previousQuery = page?.previousAnchor ? { ...pageQuery, ...anchorQuery(page.previousAnchor), direction: 'previous' as const } : null;
  const nextQuery = page?.nextAnchor ? { ...pageQuery, ...anchorQuery(page.nextAnchor), direction: 'forward' as const } : null;

  return <section className="kafdeck-data-explorer" aria-labelledby="record-explorer-title">
    <h4 id="record-explorer-title">Safe Data Explorer</h4>
    <p>Payload access is separately authorized, bounded and server-side masked before it reaches this browser.</p>
    <div>
      <label>Partition <select value={partition} onChange={event => { stopBrowse(); stopTail(); setPage(null); setDisplayedQuery(null); setError(null); setPartition(Number(event.target.value)); }}>{partitions.map(value => <option key={value} value={value}>{value}</option>)}</select></label>{' '}
      <label>Start <select value={anchorKind} onChange={event => setAnchorKind(event.target.value as typeof anchorKind)}><option value="latest">Latest</option><option value="earliest">Earliest</option><option value="offset">Offset</option><option value="timestamp">Timestamp</option></select></label>{' '}
      {anchorKind === 'offset' && <label>Offset <input type="number" min="0" value={anchorValue} onChange={event => setAnchorValue(event.target.value)} /></label>}
      {anchorKind === 'timestamp' && <label>Timestamp <input type="datetime-local" value={anchorValue} onChange={event => setAnchorValue(event.target.value)} /></label>}{' '}
      <label>Max records <input type="number" min="1" max="1000" value={maxRecords} onChange={event => setMaxRecords(Math.max(1, Math.min(1000, Number(event.target.value) || 1)))} /></label>
    </div>
    <div>
      <label>Key prefix <input value={keyPrefix} onChange={event => setKeyPrefix(event.target.value)} /></label>{' '}
      <label>Expression <select value={filterLanguage} onChange={event => setFilterLanguage(event.target.value as 'cel' | 'jq')}><option value="cel">CEL</option><option value="jq">jq-style</option></select>{' '}<input value={filter} onChange={event => setFilter(event.target.value)} placeholder="bounded deterministic filter" /></label>
    </div>
    <div>
      <button type="button" disabled={loading || tailing} onClick={() => void load(baseQuery)}>{loading ? 'Loading…' : 'Browse'}</button>{' '}
      <button type="button" disabled={!previousQuery || loading || tailing} onClick={() => previousQuery && void load(previousQuery)}>Previous page</button>{' '}
      <button type="button" disabled={!nextQuery || loading || tailing} onClick={() => nextQuery && void load(nextQuery)}>Next page</button>{' '}
      {!tailing ? <button type="button" onClick={() => void startTail()}>Start bounded live tail</button> : <button type="button" onClick={stopTail}>Stop live tail</button>}{' '}
      <button type="button" onClick={() => void exportPage('json')}>Export JSON</button>{' '}
      <button type="button" onClick={() => void exportPage('ndjson')}>Export NDJSON</button>{' '}
      <button type="button" onClick={() => void exportPage('csv')}>Export CSV</button>
    </div>
    <div><label>View <select value={viewMode} onChange={event => setViewMode(event.target.value as ViewMode)}><option value="structured">Structured</option><option value="text">Text</option><option value="hex">Hex</option></select></label></div>
    {error && <p role="alert">{error}</p>}
    {page && <>
      <p role="status">Offsets {page.lowWatermark}–{page.highWatermark} · read budget: {page.readBudgetOutcome} · filter budget: {page.filterBudgetOutcome} · masking policy: {page.policyId} v{page.policyVersion}</p>
      {page.limitations.length > 0 && <ul>{page.limitations.map((item, index) => <li key={`${item.code}-${index}`}>{item.code}: {item.count}</li>)}</ul>}
      {page.records.length === 0 ? <p>No records matched within the bounded scan.</p> : page.records.map(record => <article key={`${record.partition}-${record.offset}`}><h5>Offset {record.offset}</h5><p>{record.timestampUtc ?? 'No timestamp'} · key {record.keyRedacted ? 'redacted' : (record.key ?? 'none')} · {record.valueKind}</p><RecordValue record={record} mode={viewMode} />{record.redactedPaths.length > 0 && <p>Redacted: {record.redactedPaths.join(', ')}</p>}{record.headers.length > 0 && <details><summary>Headers</summary><ul>{record.headers.map((header, index) => <li key={`${header.name}-${index}`}>{header.name}: {header.isRedacted ? '[REDACTED]' : header.value}</li>)}</ul></details>}</article>)}
    </>}
  </section>;
}
