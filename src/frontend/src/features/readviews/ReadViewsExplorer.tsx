import { useEffect, useRef, useState } from 'react';
import {
  ApiProblem,
  kafdeckApi,
  type ConnectClusterInfo,
  type ConnectConnectorDetail,
  type ConnectConnectorSummary,
  type ConnectPluginSummary,
  type ConnectPluginValidationResult,
  type ConnectProfileSummary,
  type ConsumerDiagnostics,
  type ConsumerGroupDetail,
  type ConsumerGroupSummary,
  type ConsumerLag,
  type KsqlServerInfo,
  type ReadViewEnvelope,
  type SchemaCompatibility,
  type SchemaCompatibilityExplanation,
  type SchemaDiff,
  type SchemaMockResult,
  type SchemaReferenceGraph,
  type SchemaSubjectSummary,
  type SchemaVersionSummary,
} from '../../shared/api.js';
import { MutationOperationsPanel } from '../mutations/MutationOperationsPanel.js';

function readViewError(reason: unknown): string {
  if (reason instanceof ApiProblem && reason.status === 403) return 'Not authorized for this read view.';
  if (reason instanceof ApiProblem && reason.status === 404) return 'This read view is not configured.';
  if (reason instanceof ApiProblem && reason.status === 501) return 'This read view is unsupported by the configured provider.';
  if (reason instanceof ApiProblem && reason.status === 504) return 'The upstream read operation timed out.';
  return reason instanceof Error ? reason.message : 'The read view could not be loaded.';
}

function parseConfigurationLines(source: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const rawLine of source.split('\n')) {
    const line = rawLine.trim();
    if (!line) continue;
    const separator = line.indexOf('=');
    if (separator <= 0) throw new Error('Each configuration line must use key=value.');
    const key = line.slice(0, separator).trim();
    const value = line.slice(separator + 1);
    if (!key) throw new Error('Configuration keys must not be empty.');
    if (Object.prototype.hasOwnProperty.call(result, key)) throw new Error(`Duplicate configuration key: ${key}`);
    result[key] = value;
  }
  if (Object.keys(result).length === 0) throw new Error('At least one configuration entry is required.');
  return result;
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
  const [referenceGraph, setReferenceGraph] = useState<ReadViewEnvelope<SchemaReferenceGraph> | null>(null);
  const [compatibilityExplanation, setCompatibilityExplanation] = useState<ReadViewEnvelope<SchemaCompatibilityExplanation> | null>(null);
  const [schemaMock, setSchemaMock] = useState<ReadViewEnvelope<SchemaMockResult> | null>(null);
  const [mockCount, setMockCount] = useState(1);
  const [mockSeed, setMockSeed] = useState(0);

  const [connectProfiles, setConnectProfiles] = useState<ReadViewEnvelope<ConnectProfileSummary[]> | null>(null);
  const [selectedConnectProfileId, setSelectedConnectProfileId] = useState<string | null>(null);
  const [connectInfo, setConnectInfo] = useState<ReadViewEnvelope<ConnectClusterInfo> | null>(null);
  const [connectors, setConnectors] = useState<ReadViewEnvelope<ConnectConnectorSummary[]> | null>(null);
  const [connectorDetail, setConnectorDetail] = useState<ReadViewEnvelope<ConnectConnectorDetail> | null>(null);
  const [connectPlugins, setConnectPlugins] = useState<ReadViewEnvelope<ConnectPluginSummary[]> | null>(null);
  const [selectedPluginClass, setSelectedPluginClass] = useState<string | null>(null);
  const [pluginConfiguration, setPluginConfiguration] = useState('');
  const [pluginFieldValues, setPluginFieldValues] = useState<Record<string, string>>({});
  const [pluginValidation, setPluginValidation] = useState<ReadViewEnvelope<ConnectPluginValidationResult> | null>(null);
  const [connectError, setConnectError] = useState<string | null>(null);
  const connectLoadGeneration = useRef(0);

  const [ksqlInfo, setKsqlInfo] = useState<ReadViewEnvelope<KsqlServerInfo> | null>(null);
  const [ksqlError, setKsqlError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    const initialConnectGeneration = ++connectLoadGeneration.current;
    setConsumerGroups(null); setConsumerError(null); setGroupDetail(null); setGroupLag(null); setGroupDiagnostics(null);
    setSubjects(null); setSchemaError(null); setSelectedSubject(null); setVersions(null); setCompatibility(null); setSchemaDiff(null); setReferenceGraph(null); setCompatibilityExplanation(null); setSchemaMock(null);
    setConnectProfiles(null); setSelectedConnectProfileId(null); setConnectInfo(null); setConnectors(null); setConnectorDetail(null); setConnectPlugins(null); setSelectedPluginClass(null); setPluginConfiguration(''); setPluginFieldValues({}); setPluginValidation(null); setConnectError(null);
    setKsqlInfo(null); setKsqlError(null);

    void kafdeckApi.listConsumerGroups(clusterId, controller.signal)
      .then(setConsumerGroups)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setConsumerError(readViewError(reason)); });

    void kafdeckApi.listSchemaSubjects(clusterId, controller.signal)
      .then(setSubjects)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setSchemaError(readViewError(reason)); });

    void kafdeckApi.listConnectProfiles(clusterId, controller.signal)
      .then(async profileResult => {
        if (initialConnectGeneration !== connectLoadGeneration.current) return;
        setConnectProfiles(profileResult);
        const selectedProfile =
          profileResult.data.find(profile => profile.isDefault) ??
          profileResult.data[0] ??
          null;
        if (!selectedProfile) return;

        setSelectedConnectProfileId(selectedProfile.id);
        const [info, list, plugins] = await Promise.all([
          kafdeckApi.getConnectProfileInfo(clusterId, selectedProfile.id, controller.signal),
          kafdeckApi.listConnectProfileConnectors(clusterId, selectedProfile.id, controller.signal),
          kafdeckApi.listConnectPlugins(clusterId, selectedProfile.id, controller.signal),
        ]);
        if (initialConnectGeneration !== connectLoadGeneration.current) return;
        setConnectInfo(info);
        setConnectors(list);
        setConnectPlugins(plugins);
      })
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
    setSchemaError(null); setSelectedSubject(subject); setVersions(null); setCompatibility(null); setSchemaDiff(null); setReferenceGraph(null); setCompatibilityExplanation(null); setSchemaMock(null);
    try {
      const [versionResult, compatibilityResult, explanationResult] = await Promise.all([
        kafdeckApi.listSchemaVersions(clusterId, subject),
        kafdeckApi.getSchemaCompatibility(clusterId, subject),
        kafdeckApi.getSchemaCompatibilityExplanation(clusterId, subject),
      ]);
      setVersions(versionResult);
      setCompatibility(compatibilityResult);
      setCompatibilityExplanation(explanationResult);
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

  const inspectSchemaReferences = async () => {
    if (!selectedSubject || rightVersion === null) return;
    setSchemaError(null); setReferenceGraph(null);
    try {
      setReferenceGraph(await kafdeckApi.getSchemaReferenceGraph(clusterId, selectedSubject, rightVersion));
    } catch (reason) {
      setSchemaError(readViewError(reason));
    }
  };

  const generateSchemaMock = async () => {
    if (!selectedSubject || rightVersion === null) return;
    setSchemaError(null); setSchemaMock(null);
    try {
      const count = Math.max(1, Math.min(10, Math.trunc(mockCount)));
      const seed = Math.trunc(mockSeed);
      setSchemaMock(await kafdeckApi.generateSchemaMock(clusterId, selectedSubject, rightVersion, count, seed));
    } catch (reason) {
      setSchemaError(readViewError(reason));
    }
  };

  const openConnectProfile = async (profileId: string) => {
    const generation = ++connectLoadGeneration.current;
    setConnectError(null);
    setSelectedConnectProfileId(profileId);
    setConnectInfo(null);
    setConnectors(null);
    setConnectorDetail(null);
    setConnectPlugins(null);
    setSelectedPluginClass(null);
    setPluginConfiguration('');
    setPluginFieldValues({});
    setPluginValidation(null);
    try {
      const [info, list, plugins] = await Promise.all([
        kafdeckApi.getConnectProfileInfo(clusterId, profileId),
        kafdeckApi.listConnectProfileConnectors(clusterId, profileId),
        kafdeckApi.listConnectPlugins(clusterId, profileId),
      ]);
      if (generation !== connectLoadGeneration.current) return;
      setConnectInfo(info);
      setConnectors(list);
      setConnectPlugins(plugins);
    } catch (reason) {
      if (generation === connectLoadGeneration.current) {
        setConnectError(readViewError(reason));
      }
    }
  };

  const openConnector = async (name: string) => {
    if (!selectedConnectProfileId) return;
    setConnectError(null); setConnectorDetail(null);
    try {
      setConnectorDetail(await kafdeckApi.getConnectProfileConnector(
        clusterId,
        selectedConnectProfileId,
        name,
      ));
    } catch (reason) {
      setConnectError(readViewError(reason));
    }
  };

  const selectPluginForValidation = async (connectorClass: string) => {
    if (!selectedConnectProfileId) return;
    setConnectError(null);
    setSelectedPluginClass(connectorClass);
    setPluginConfiguration('');
    setPluginFieldValues({});
    setPluginValidation(null);
    try {
      setPluginValidation(await kafdeckApi.validateConnectPlugin(
        clusterId,
        selectedConnectProfileId,
        connectorClass,
        { 'connector.class': connectorClass },
      ));
    } catch (reason) {
      setConnectError(readViewError(reason));
    }
  };

  const validateSmartPlugin = async () => {
    if (!selectedConnectProfileId || !selectedPluginClass) return;
    setConnectError(null);
    setPluginValidation(null);
    try {
      const configuration: Record<string, string> = {
        'connector.class': selectedPluginClass,
        ...pluginFieldValues,
      };
      setPluginValidation(await kafdeckApi.validateConnectPlugin(
        clusterId,
        selectedConnectProfileId,
        selectedPluginClass,
        configuration,
      ));
    } catch (reason) {
      setConnectError(readViewError(reason));
    } finally {
      setPluginFieldValues({});
    }
  };

  const validatePlugin = async () => {
    if (!selectedConnectProfileId || !selectedPluginClass) return;
    setConnectError(null);
    setPluginValidation(null);
    try {
      const configuration = parseConfigurationLines(pluginConfiguration);
      setPluginValidation(await kafdeckApi.validateConnectPlugin(
        clusterId,
        selectedConnectProfileId,
        selectedPluginClass,
        configuration,
      ));
    } catch (reason) {
      setConnectError(readViewError(reason));
    } finally {
      setPluginConfiguration('');
      setPluginFieldValues({});
    }
  };

  return <>
    <section className="card kafdeck-card" id="consumers" aria-labelledby="consumers-title">
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

    <section className="card kafdeck-card" id="schemas" aria-labelledby="schemas-title">
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
        {compatibilityExplanation && <aside aria-labelledby="schema-compatibility-explanation-title">
          <h4 id="schema-compatibility-explanation-title">Compatibility explanation</h4>
          <p>{compatibilityExplanation.data.summary}</p>
          <p>Scope: {compatibilityExplanation.data.scope}</p>
          <ul>{compatibilityExplanation.data.rules.map(rule => <li key={rule}>{rule}</li>)}</ul>
        </aside>}
        <table><thead><tr><th>Version</th><th>Schema ID</th><th>Format</th><th>References</th></tr></thead><tbody>{versions.data.map(item => <tr key={item.version}><td>{item.version}</td><td>{item.schemaId}</td><td>{item.format}</td><td>{item.references.length}</td></tr>)}</tbody></table>
        {versions.data.length > 0 && <div>
          <label htmlFor="schema-left-version">Left version</label>{' '}
          <select id="schema-left-version" value={leftVersion ?? ''} onChange={event => setLeftVersion(Number(event.target.value))}>{versions.data.map(item => <option key={item.version} value={item.version}>{item.version}</option>)}</select>{' '}
          <label htmlFor="schema-right-version">Right/tooling version</label>{' '}
          <select id="schema-right-version" value={rightVersion ?? ''} onChange={event => { setRightVersion(Number(event.target.value)); setReferenceGraph(null); setSchemaMock(null); }}>{versions.data.map(item => <option key={item.version} value={item.version}>{item.version}</option>)}</select>{' '}
          <button type="button" onClick={() => void compareSchemas()}>Compare versions</button>{' '}
          <button type="button" onClick={() => void inspectSchemaReferences()}>Inspect references</button>
        </div>}
        {schemaDiff && <div><h4>Schema diff</h4>{schemaDiff.data.isEqual ? <p>Selected versions are textually equivalent after normalization.</p> : <ul>{schemaDiff.data.hunks.map((hunk, index) => <li key={index}>Left {hunk.leftStartLine} ({hunk.leftLineCount} line(s)) → right {hunk.rightStartLine} ({hunk.rightLineCount} line(s)); removed {hunk.removedLines.length}, added {hunk.addedLines.length}.</li>)}</ul>}</div>}
        {referenceGraph && <div>
          <h4>Reference graph</h4>
          <p>{referenceGraph.data.nodes.length} node(s), {referenceGraph.data.edges.length} edge(s), {referenceGraph.data.totalSchemaBytes} schema byte(s). Cycle: {referenceGraph.data.hasCycle ? 'detected' : 'not detected'}.</p>
          {referenceGraph.data.nodes.length > 0 && <table><thead><tr><th>Subject</th><th>Version</th><th>Depth</th><th>Format</th></tr></thead><tbody>{referenceGraph.data.nodes.map(node => <tr key={`${node.subject}-${node.version}`}><th scope="row">{node.subject}</th><td>{node.version}</td><td>{node.depth}</td><td>{node.format}</td></tr>)}</tbody></table>}
          {referenceGraph.data.edges.length > 0 && <ul>{referenceGraph.data.edges.map((edge, index) => <li key={`${edge.fromSubject}-${edge.fromVersion}-${edge.name}-${index}`}>{edge.fromSubject} v{edge.fromVersion} → {edge.toSubject} v{edge.toVersion} ({edge.name})</li>)}</ul>}
        </div>}
        {rightVersion !== null && <div aria-labelledby="schema-mock-title">
          <h4 id="schema-mock-title">Bounded local mock</h4>
          <p>Examples are generated in memory only. This action does not register a schema or produce Kafka records.</p>
          <label htmlFor="schema-mock-count">Examples</label>{' '}
          <input id="schema-mock-count" type="number" min={1} max={10} value={mockCount} onChange={event => setMockCount(Number(event.target.value))} />{' '}
          <label htmlFor="schema-mock-seed">Seed</label>{' '}
          <input id="schema-mock-seed" type="number" value={mockSeed} onChange={event => setMockSeed(Number(event.target.value))} />{' '}
          <button type="button" onClick={() => void generateSchemaMock()}>Generate examples</button>
        </div>}
        {schemaMock && <div>
          <h4>Generated examples</h4>
          <p>Format: {schemaMock.data.format} · Seed: {schemaMock.data.seed} · Total bytes: {schemaMock.data.totalBytes}</p>
          {schemaMock.data.examples.map(example => <pre key={example.index} aria-label={`Generated schema example ${example.index + 1}`}><code>{example.json}</code></pre>)}
        </div>}
      </article>}
    </section>

    <section className="card kafdeck-card" id="ecosystem" aria-labelledby="ecosystem-title">
      <h2 id="ecosystem-title">Ecosystem read views</h2>
      <h3>Kafka Connect</h3>
      {connectError && <p role="status">{connectError}</p>}
      {!connectProfiles && !connectError && <p role="status">Loading configured Connect profiles…</p>}
      {connectProfiles && connectProfiles.data.length === 0 && <p>No Kafka Connect profiles are configured.</p>}
      {connectProfiles && connectProfiles.data.length > 0 && <div>
        <label htmlFor="connect-profile-selector">Connect profile</label>{' '}
        <select
          id="connect-profile-selector"
          value={selectedConnectProfileId ?? ''}
          onChange={event => void openConnectProfile(event.target.value)}
        >
          {connectProfiles.data.map(profile => <option key={profile.id} value={profile.id}>
            {profile.id}{profile.isDefault ? ' (default)' : ''}
          </option>)}
        </select>
        {selectedConnectProfileId && <p>Profile: <strong>{selectedConnectProfileId}</strong> · Mutation provider: {connectProfiles.data.find(profile => profile.id === selectedConnectProfileId)?.mutationProviderProfile ?? 'Unknown'}</p>}
      </div>}
      {connectInfo && <p>Version: {connectInfo.data.version ?? 'Unknown'} · Kafka cluster: {connectInfo.data.kafkaClusterId ?? 'Unknown'}</p>}
      {connectors && (connectors.data.length === 0 ? <p>No authorized connectors are observable in this profile.</p> : <ul>{connectors.data.map(item => <li key={item.name}><button type="button" onClick={() => void openConnector(item.name)}>{item.name}</button></li>)}</ul>)}
      {connectorDetail && <article><h4>{connectorDetail.data.name}</h4><p>State: {connectorDetail.data.state} · Worker: {connectorDetail.data.workerId ?? 'Unknown'} · Tasks: {connectorDetail.data.tasks.length}</p><table><thead><tr><th>Configuration key</th><th>Safe value</th></tr></thead><tbody>{Object.entries(connectorDetail.data.safeConfiguration).map(([key, value]) => <tr key={key}><th scope="row">{key}</th><td>{value ?? 'Not set'}</td></tr>)}</tbody></table></article>}
      {connectPlugins && <article aria-labelledby="connect-plugins-title">
        <h4 id="connect-plugins-title">Connector plugins</h4>
        {connectPlugins.data.length === 0 ? <p>No connector plugins were reported.</p> :
          <table><thead><tr><th>Class</th><th>Type</th><th>Version</th><th>Validate</th></tr></thead><tbody>{connectPlugins.data.map(plugin => <tr key={plugin.class}><th scope="row">{plugin.class}</th><td>{plugin.type}</td><td>{plugin.version ?? 'Unknown'}</td><td><button type="button" onClick={() => void selectPluginForValidation(plugin.class)}>Use</button></td></tr>)}</tbody></table>}
      </article>}
      {selectedPluginClass && <article aria-labelledby="connect-plugin-validation-title">
        <h4 id="connect-plugin-validation-title">Validate configuration</h4>
        <p>Plugin: <code>{selectedPluginClass}</code>. Values are sent only to the selected configured Connect profile and are cleared from this form after validation.</p>
        {pluginValidation && pluginValidation.data.fields.some(field => field.name !== 'connector.class') && <fieldset>
          <legend>Smart configuration form</legend>
          {pluginValidation.data.fields.filter(field => field.name !== 'connector.class').map(field => <div key={field.name}>
            <label htmlFor={`connect-plugin-field-${field.name}`}>{field.name}{field.required ? ' *' : ''}</label>{' '}
            {field.recommendedValues.length > 0
              ? <select
                  id={`connect-plugin-field-${field.name}`}
                  value={pluginFieldValues[field.name] ?? ''}
                  onChange={event => setPluginFieldValues(values => ({ ...values, [field.name]: event.target.value }))}
                >
                  <option value="">Select…</option>
                  {field.recommendedValues.map(value => <option key={value} value={value}>{value}</option>)}
                </select>
              : <input
                  id={`connect-plugin-field-${field.name}`}
                  type={field.type.toUpperCase() === 'PASSWORD' ? 'password' : 'text'}
                  value={pluginFieldValues[field.name] ?? ''}
                  onChange={event => setPluginFieldValues(values => ({ ...values, [field.name]: event.target.value }))}
                  autoComplete="off"
                />}
            {field.errors.length > 0 && <small role="status">{field.errors.join('; ')}</small>}
          </div>)}
          <button
            type="button"
            onClick={() => void validateSmartPlugin()}
            disabled={Object.keys(pluginFieldValues).length === 0}
          >
            Validate smart form
          </button>
          <p>Smart-form values remain in browser memory only and are cleared after each validation attempt.</p>
        </fieldset>}
        <details>
          <summary>Raw key=value validation</summary>
        <label htmlFor="connect-plugin-configuration">Configuration (one key=value per line)</label>
        <textarea
          id="connect-plugin-configuration"
          value={pluginConfiguration}
          onChange={event => setPluginConfiguration(event.target.value)}
          autoComplete="off"
          rows={6}
        />
        <button type="button" onClick={() => void validatePlugin()} disabled={pluginConfiguration.trim().length === 0}>Validate configuration</button>
        </details>
      </article>}
      {pluginValidation && <article aria-labelledby="connect-plugin-validation-result-title">
        <h4 id="connect-plugin-validation-result-title">Validation diagnostics</h4>
        <p>Error count: {pluginValidation.data.errorCount}</p>
        <table><thead><tr><th>Field</th><th>Type</th><th>Required</th><th>Errors</th><th>Recommended</th></tr></thead><tbody>{pluginValidation.data.fields.map(field => <tr key={field.name}><th scope="row">{field.name}</th><td>{field.type}</td><td>{field.required ? 'Yes' : 'No'}</td><td>{field.errors.join('; ') || 'None'}</td><td>{field.recommendedValues.join(', ') || 'None'}</td></tr>)}</tbody></table>
      </article>}

      <h3>ksqlDB</h3>
      {ksqlError && <p role="status">{ksqlError}</p>}
      {ksqlInfo && <p>Version: {ksqlInfo.data.version ?? 'Unknown'} · Kafka cluster: {ksqlInfo.data.kafkaClusterId ?? 'Unknown'} · Health: {ksqlInfo.data.state ?? 'Unknown'}</p>}
      <p>v0.4 exposes no SQL or metadata-statement execution surface. Metadata that would require statement execution is reported unsupported.</p>
    </section>

    <MutationOperationsPanel clusterId={clusterId} connectProfileId={selectedConnectProfileId} />
  </>;
}
