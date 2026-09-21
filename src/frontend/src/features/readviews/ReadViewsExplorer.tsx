import { useEffect, useState } from 'react';
import {
  ApiProblem,
  kafdeckApi,
  type ConnectClusterInfo,
  type ConnectConnectorDetail,
  type ConnectConnectorSummary,
  type ConsumerDiagnostics,
  type ConsumerGroupDetail,
  type ConsumerGroupSummary,
  type ConsumerLag,
  type KsqlServerInfo,
  type ReadViewEnvelope,
  type SchemaCompatibility,
  type SchemaDiff,
  type SchemaSubjectSummary,
  type SchemaVersionSummary,
} from '../../shared/api.js';

function readViewError(reason: unknown): string {
  if (reason instanceof ApiProblem && reason.status === 403) return 'Not authorized for this read view.';
  if (reason instanceof ApiProblem && reason.status === 404) return 'This read view is not configured.';
  if (reason instanceof ApiProblem && reason.status === 501) return 'This read view is unsupported by the configured provider.';
  if (reason instanceof ApiProblem && reason.status === 504) return 'The upstream read operation timed out.';
  return reason instanceof Error ? reason.message : 'The read view could not be loaded.';
}

function Limitations({ envelope }: { envelope: ReadViewEnvelope<unknown> }) {
  if (!envelope.partial || envelope.limitations.length === 0) return null;
  return <aside aria-label="Read-view limitations"><strong>Partial observation</strong><ul>{envelope.limitations.map(item => <li key={item.code}>{item.code}: {item.message}</li>)}</ul></aside>;
}

export function ReadViewsExplorer({ clusterId }: { clusterId: string }) {
  const [consumerGroups, setConsumerGroups] = useState<ReadViewEnvelope<ConsumerGroupSummary[]> | null>(null);
  const [consumerError, setConsumerError] = useState<string | null>(null);
  const [groupDetail, setGroupDetail] = useState<ReadViewEnvelope<ConsumerGroupDetail> | null>(null);
  const [groupLag, setGroupLag] = useState<ReadViewEnvelope<ConsumerLag> | null>(null);
  const [groupDiagnostics, setGroupDiagnostics] = useState<ReadViewEnvelope<ConsumerDiagnostics> | null>(null);

  const [subjects, setSubjects] = useState<ReadViewEnvelope<SchemaSubjectSummary[]> | null>(null);
  const [schemaError, setSchemaError] = useState<string | null>(null);
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null);
  const [versions, setVersions] = useState<ReadViewEnvelope<SchemaVersionSummary[]> | null>(null);
  const [compatibility, setCompatibility] = useState<ReadViewEnvelope<SchemaCompatibility> | null>(null);
  const [leftVersion, setLeftVersion] = useState<number | null>(null);
  const [rightVersion, setRightVersion] = useState<number | null>(null);
  const [schemaDiff, setSchemaDiff] = useState<ReadViewEnvelope<SchemaDiff> | null>(null);

  const [connectInfo, setConnectInfo] = useState<ReadViewEnvelope<ConnectClusterInfo> | null>(null);
  const [connectors, setConnectors] = useState<ReadViewEnvelope<ConnectConnectorSummary[]> | null>(null);
  const [connectorDetail, setConnectorDetail] = useState<ReadViewEnvelope<ConnectConnectorDetail> | null>(null);
  const [connectError, setConnectError] = useState<string | null>(null);

  const [ksqlInfo, setKsqlInfo] = useState<ReadViewEnvelope<KsqlServerInfo> | null>(null);
  const [ksqlError, setKsqlError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setConsumerGroups(null); setConsumerError(null); setGroupDetail(null); setGroupLag(null); setGroupDiagnostics(null);
    setSubjects(null); setSchemaError(null); setSelectedSubject(null); setVersions(null); setCompatibility(null); setSchemaDiff(null);
    setConnectInfo(null); setConnectors(null); setConnectorDetail(null); setConnectError(null);
    setKsqlInfo(null); setKsqlError(null);

    void kafdeckApi.listConsumerGroups(clusterId, controller.signal)
      .then(setConsumerGroups)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setConsumerError(readViewError(reason)); });

    void kafdeckApi.listSchemaSubjects(clusterId, controller.signal)
      .then(setSubjects)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setSchemaError(readViewError(reason)); });

    void Promise.all([
      kafdeckApi.getConnectInfo(clusterId, controller.signal),
      kafdeckApi.listConnectors(clusterId, controller.signal),
    ])
      .then(([info, list]) => { setConnectInfo(info); setConnectors(list); })
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setConnectError(readViewError(reason)); });

    void kafdeckApi.getKsqlInfo(clusterId, controller.signal)
      .then(setKsqlInfo)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setKsqlError(readViewError(reason)); });

    return () => controller.abort();
  }, [clusterId]);

  const openConsumer = async (groupId: string) => {
    setConsumerError(null); setGroupDetail(null); setGroupLag(null); setGroupDiagnostics(null);
    try {
      const [detail, lag, diagnostics] = await Promise.all([
        kafdeckApi.getConsumerGroup(clusterId, groupId),
        kafdeckApi.getConsumerLag(clusterId, groupId),
        kafdeckApi.getConsumerDiagnostics(clusterId, groupId),
      ]);
      setGroupDetail(detail); setGroupLag(lag); setGroupDiagnostics(diagnostics);
    } catch (reason) {
      setConsumerError(readViewError(reason));
    }
  };

  const openSubject = async (subject: string) => {
    setSchemaError(null); setSelectedSubject(subject); setVersions(null); setCompatibility(null); setSchemaDiff(null);
    try {
      const [versionResult, compatibilityResult] = await Promise.all([
        kafdeckApi.listSchemaVersions(clusterId, subject),
        kafdeckApi.getSchemaCompatibility(clusterId, subject),
      ]);
      setVersions(versionResult);
      setCompatibility(compatibilityResult);
      const values = versionResult.data.map(item => item.version);
      setRightVersion(values.at(-1) ?? null);
      setLeftVersion(values.length > 1 ? values.at(-2) ?? null : values[0] ?? null);
    } catch (reason) {
      setSchemaError(readViewError(reason));
    }
  };

  const compareSchemas = async () => {
    if (!selectedSubject || leftVersion === null || rightVersion === null) return;
    setSchemaError(null); setSchemaDiff(null);
    try {
      setSchemaDiff(await kafdeckApi.diffSchemaVersions(clusterId, selectedSubject, leftVersion, rightVersion));
    } catch (reason) {
      setSchemaError(readViewError(reason));
    }
  };

  const openConnector = async (name: string) => {
    setConnectError(null); setConnectorDetail(null);
    try {
      setConnectorDetail(await kafdeckApi.getConnector(clusterId, name));
    } catch (reason) {
      setConnectError(readViewError(reason));
    }
  };

  return <>
    <section id="consumers" aria-labelledby="consumers-title">
      <h2 id="consumers-title">Consumer groups</h2>
      {consumerError && <p role="status">{consumerError}</p>}
      {!consumerGroups && !consumerError && <p role="status">Loading authorized consumer groups…</p>}
      {consumerGroups && <>
        <Limitations envelope={consumerGroups} />
        {consumerGroups.data.length === 0 ? <p>No authorized consumer groups are observable.</p> :
          <table><thead><tr><th scope="col">Group</th><th scope="col">State</th><th scope="col">Members</th></tr></thead>
            <tbody>{consumerGroups.data.map(group => <tr key={group.groupId}>
              <th scope="row"><button type="button" onClick={() => void openConsumer(group.groupId)}>{group.groupId}</button></th>
              <td>{group.state}</td><td>{group.memberCount ?? 'Detail required'}</td>
            </tr>)}</tbody></table>}
      </>}
      {groupDetail && groupLag && groupDiagnostics && <article aria-labelledby="consumer-detail-title">
        <h3 id="consumer-detail-title">{groupDetail.data.groupId}</h3>
        <p>Kafka state: {groupDetail.data.state} · Diagnostic: {groupDiagnostics.data.state} · Total lag: {groupLag.data.totalLag ?? 'Unknown'}</p>
        <p>Members: {groupDetail.data.members.length} · Metrics: {groupDiagnostics.data.metricsAvailable ? 'available' : 'unavailable'} · History: {groupDiagnostics.data.historyAvailable ? 'available' : 'unavailable'}</p>
        <Limitations envelope={groupLag} /><Limitations envelope={groupDiagnostics} />
        {groupDiagnostics.data.evidence.length > 0 && <><h4>Evidence</h4><ul>{groupDiagnostics.data.evidence.map(item => <li key={item.code}>{item.safeMessage}</li>)}</ul></>}
        {groupLag.data.partitions.length > 0 && <><h4>Offsets and lag</h4><table><thead><tr><th>Topic</th><th>Partition</th><th>Committed</th><th>End</th><th>Lag</th><th>State</th></tr></thead><tbody>{groupLag.data.partitions.map(item => <tr key={`${item.topic}-${item.partition}`}><td>{item.topic}</td><td>{item.partition}</td><td>{item.committedOffset ?? 'Unknown'}</td><td>{item.endOffset ?? 'Unknown'}</td><td>{item.lag ?? 'Unknown'}</td><td>{item.state}</td></tr>)}</tbody></table></>}
      </article>}
    </section>

    <section id="schemas" aria-labelledby="schemas-title">
      <h2 id="schemas-title">Schema Registry</h2>
      {schemaError && <p role="status">{schemaError}</p>}
      {!subjects && !schemaError && <p role="status">Loading authorized schema subjects…</p>}
      {subjects && <>
        <Limitations envelope={subjects} />
        {subjects.data.length === 0 ? <p>No authorized schema subjects are observable.</p> :
          <ul>{subjects.data.map(item => <li key={item.subject}><button type="button" onClick={() => void openSubject(item.subject)}>{item.subject}</button></li>)}</ul>}
      </>}
      {selectedSubject && versions && <article aria-labelledby="schema-detail-title">
        <h3 id="schema-detail-title">{selectedSubject}</h3>
        <p>Compatibility: {compatibility?.data.mode ?? 'Unknown'}{compatibility?.data.isInherited ? ' (inherited)' : ''}</p>
        <table><thead><tr><th>Version</th><th>Schema ID</th><th>Format</th><th>References</th></tr></thead><tbody>{versions.data.map(item => <tr key={item.version}><td>{item.version}</td><td>{item.schemaId}</td><td>{item.format}</td><td>{item.references.length}</td></tr>)}</tbody></table>
        {versions.data.length > 0 && <div>
          <label htmlFor="schema-left-version">Left version</label>{' '}
          <select id="schema-left-version" value={leftVersion ?? ''} onChange={event => setLeftVersion(Number(event.target.value))}>{versions.data.map(item => <option key={item.version} value={item.version}>{item.version}</option>)}</select>{' '}
          <label htmlFor="schema-right-version">Right version</label>{' '}
          <select id="schema-right-version" value={rightVersion ?? ''} onChange={event => setRightVersion(Number(event.target.value))}>{versions.data.map(item => <option key={item.version} value={item.version}>{item.version}</option>)}</select>{' '}
          <button type="button" onClick={() => void compareSchemas()}>Compare versions</button>
        </div>}
        {schemaDiff && <div><h4>Schema diff</h4>{schemaDiff.data.isEqual ? <p>Selected versions are textually equivalent after normalization.</p> : <ul>{schemaDiff.data.hunks.map((hunk, index) => <li key={index}>Left {hunk.leftStartLine} ({hunk.leftLineCount} line(s)) → right {hunk.rightStartLine} ({hunk.rightLineCount} line(s)); removed {hunk.removedLines.length}, added {hunk.addedLines.length}.</li>)}</ul>}</div>}
      </article>}
    </section>

    <section id="ecosystem" aria-labelledby="ecosystem-title">
      <h2 id="ecosystem-title">Ecosystem read views</h2>
      <h3>Kafka Connect</h3>
      {connectError && <p role="status">{connectError}</p>}
      {connectInfo && <p>Version: {connectInfo.data.version ?? 'Unknown'} · Kafka cluster: {connectInfo.data.kafkaClusterId ?? 'Unknown'}</p>}
      {connectors && (connectors.data.length === 0 ? <p>No authorized connectors are observable.</p> : <ul>{connectors.data.map(item => <li key={item.name}><button type="button" onClick={() => void openConnector(item.name)}>{item.name}</button></li>)}</ul>)}
      {connectorDetail && <article><h4>{connectorDetail.data.name}</h4><p>State: {connectorDetail.data.state} · Worker: {connectorDetail.data.workerId ?? 'Unknown'} · Tasks: {connectorDetail.data.tasks.length}</p><table><thead><tr><th>Configuration key</th><th>Safe value</th></tr></thead><tbody>{Object.entries(connectorDetail.data.safeConfiguration).map(([key, value]) => <tr key={key}><th scope="row">{key}</th><td>{value ?? 'Not set'}</td></tr>)}</tbody></table></article>}

      <h3>ksqlDB</h3>
      {ksqlError && <p role="status">{ksqlError}</p>}
      {ksqlInfo && <p>Version: {ksqlInfo.data.version ?? 'Unknown'} · Kafka cluster: {ksqlInfo.data.kafkaClusterId ?? 'Unknown'} · Health: {ksqlInfo.data.state ?? 'Unknown'}</p>}
      <p>v0.4 does not execute SQL or metadata statements. Metadata that requires statement execution is reported unsupported.</p>
    </section>
  </>;
}
