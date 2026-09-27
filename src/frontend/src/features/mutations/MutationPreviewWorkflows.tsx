import { FormEvent, useState } from 'react';
import {
  MutationApiProblem,
  mutationApi,
  type ConsumerOffsetSelectorKind,
  type DataGeneratorSourceKindInput,
  type DataJobKindInput,
  type MutationStatus,
  type RecordsPurgeSelectorKind,
  type SchemaFormatInput,
} from './mutationApi.js';

type WorkflowKind = 'topic' | 'record' | 'consumer' | 'schema' | 'connect' | 'dataJob' | 'generator' | 'purge';

function newIdempotencyKey(): string {
  if (
    typeof globalThis.crypto === 'undefined' ||
    typeof globalThis.crypto.randomUUID !== 'function'
  ) {
    throw new Error('Secure browser randomness is required to generate an Idempotency-Key.');
  }

  return globalThis.crypto.randomUUID();
}

function pairMap(source: string): Record<string, string> {
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
  return result;
}

function nullablePairMap(source: string): Record<string, string | null> {
  const result: Record<string, string | null> = {};
  for (const rawLine of source.split('\n')) {
    const line = rawLine.trim();
    if (!line) continue;
    const separator = line.indexOf('=');
    if (separator <= 0) throw new Error('Each configuration change must use key=value or key=<remove>.');
    const key = line.slice(0, separator).trim();
    const value = line.slice(separator + 1);
    if (Object.prototype.hasOwnProperty.call(result, key)) throw new Error(`Duplicate configuration key: ${key}`);
    result[key] = value === '<remove>' ? null : value;
  }
  return result;
}

function bytesToBase64(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let binary = '';
  const chunk = 0x8000;
  for (let offset = 0; offset < bytes.length; offset += chunk) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + chunk));
  }
  return btoa(binary);
}

function errorText(reason: unknown): string {
  if (reason instanceof MutationApiProblem) return reason.message;
  return reason instanceof Error ? reason.message : 'The mutation preview could not be created.';
}

export function MutationPreviewWorkflows({
  clusterId,
  connectProfileId,
  onPreview,
}: {
  clusterId: string;
  connectProfileId?: string | null;
  onPreview: (operation: MutationStatus) => void;
}) {
  const [workflow, setWorkflow] = useState<WorkflowKind>('topic');
  const [idempotencyKey, setIdempotencyKey] = useState(newIdempotencyKey);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (action: () => Promise<MutationStatus>) => {
    setBusy(true);
    setError(null);
    try {
      const operation = await action();
      onPreview(operation);
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setBusy(false);
    }
  };

  return <article aria-labelledby="mutation-preview-workflow-title">
    <h3 id="mutation-preview-workflow-title">Create governed preview</h3>
    <p>Each workflow is typed and scoped to the selected cluster. Kafdeck does not provide a generic provider command console.</p>
    <label htmlFor="mutation-workflow">Workflow</label>{' '}
    <select id="mutation-workflow" value={workflow} onChange={event => setWorkflow(event.target.value as WorkflowKind)}>
      <option value="topic">Topic administration</option>
      <option value="record">Record production</option>
      <option value="consumer">Consumer administration</option>
      <option value="schema">Schema Registry</option>
      <option value="connect">Kafka Connect</option>
      <option value="dataJob">Replay / forward data job</option>
      <option value="generator">Smart Mock / Data Generator</option>
      <option value="purge">Controlled purge</option>
    </select>
    <p><label htmlFor="mutation-idempotency-key">Idempotency-Key</label>{' '}<input id="mutation-idempotency-key" value={idempotencyKey} onChange={event => setIdempotencyKey(event.target.value)} autoComplete="off" />{' '}<button type="button" onClick={() => setIdempotencyKey(newIdempotencyKey())}>Generate new key</button></p>
    {error && <p role="alert">{error}</p>}
    {workflow === 'topic' && <TopicWorkflow clusterId={clusterId} idempotencyKey={idempotencyKey} busy={busy} submit={submit} />}
    {workflow === 'record' && <RecordWorkflow clusterId={clusterId} idempotencyKey={idempotencyKey} busy={busy} submit={submit} />}
    {workflow === 'consumer' && <ConsumerWorkflow clusterId={clusterId} idempotencyKey={idempotencyKey} busy={busy} submit={submit} />}
    {workflow === 'schema' && <SchemaWorkflow clusterId={clusterId} idempotencyKey={idempotencyKey} busy={busy} submit={submit} />}
    {workflow === 'connect' && <ConnectWorkflow key={`${clusterId}:${connectProfileId ?? 'default'}`} clusterId={clusterId} connectProfileId={connectProfileId ?? 'default'} idempotencyKey={idempotencyKey} busy={busy} submit={submit} />}
    {workflow === 'dataJob' && <DataJobWorkflow clusterId={clusterId} idempotencyKey={idempotencyKey} busy={busy} submit={submit} />}
    {workflow === 'generator' && <DataGeneratorWorkflow clusterId={clusterId} idempotencyKey={idempotencyKey} busy={busy} submit={submit} />}
    {workflow === 'purge' && <PurgeWorkflow clusterId={clusterId} idempotencyKey={idempotencyKey} busy={busy} submit={submit} />}
  </article>;
}

type Submit = (action: () => Promise<MutationStatus>) => Promise<void>;

function TopicWorkflow({ clusterId, idempotencyKey, busy, submit }: { clusterId: string; idempotencyKey: string; busy: boolean; submit: Submit }) {
  const [action, setAction] = useState<'create' | 'alter' | 'increase' | 'delete'>('create');
  const [topicName, setTopicName] = useState('');
  const [partitionCount, setPartitionCount] = useState(1);
  const [replicationFactor, setReplicationFactor] = useState(1);
  const [configuration, setConfiguration] = useState('');

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    void submit(() => {
      if (action === 'create') return mutationApi.previewTopicCreate(clusterId, topicName, { partitionCount, replicationFactor, configurations: pairMap(configuration) }, idempotencyKey);
      if (action === 'alter') return mutationApi.previewTopicAlter(clusterId, topicName, { changes: nullablePairMap(configuration) }, idempotencyKey);
      if (action === 'increase') return mutationApi.previewTopicIncreasePartitions(clusterId, topicName, { newPartitionCount: partitionCount }, idempotencyKey);
      return mutationApi.previewTopicDelete(clusterId, topicName, idempotencyKey);
    });
  };

  return <form onSubmit={onSubmit} aria-label="Topic mutation preview">
    <fieldset disabled={busy}><legend>Topic administration</legend>
      <label htmlFor="topic-mutation-action">Action</label>{' '}<select id="topic-mutation-action" value={action} onChange={event => setAction(event.target.value as typeof action)}><option value="create">Create</option><option value="alter">Alter configuration</option><option value="increase">Increase partitions</option><option value="delete">Delete</option></select><br />
      <label htmlFor="topic-mutation-name">Exact topic</label>{' '}<input id="topic-mutation-name" required value={topicName} onChange={event => setTopicName(event.target.value)} /><br />
      {(action === 'create' || action === 'increase') && <><label htmlFor="topic-partitions">{action === 'create' ? 'Partitions' : 'New partition count'}</label>{' '}<input id="topic-partitions" type="number" min="1" required value={partitionCount} onChange={event => setPartitionCount(Number(event.target.value))} /><br /></>}
      {action === 'create' && <><label htmlFor="topic-replication-factor">Replication factor</label>{' '}<input id="topic-replication-factor" type="number" min="1" required value={replicationFactor} onChange={event => setReplicationFactor(Number(event.target.value))} /><br /></>}
      {(action === 'create' || action === 'alter') && <><label htmlFor="topic-config">Allowlisted configuration, one key=value per line</label><br /><textarea id="topic-config" value={configuration} onChange={event => setConfiguration(event.target.value)} rows={5} /><p>{action === 'alter' ? 'Use key=<remove> to remove an admitted dynamic topic configuration.' : 'Only server-admitted topic configuration keys are accepted.'}</p></>}
      {action === 'delete' && <p role="alert">Topic deletion is destructive. The server owns the risk floor and confirmation requirements.</p>}
      <button type="submit" disabled={!topicName.trim() || !idempotencyKey.trim()}>Create preview</button>
    </fieldset>
  </form>;
}

function RecordWorkflow({ clusterId, idempotencyKey, busy, submit }: { clusterId: string; idempotencyKey: string; busy: boolean; submit: Submit }) {
  const [topicName, setTopicName] = useState('');
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [headers, setHeaders] = useState('');

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    void submit(() => {
      const parsedHeaders = Object.entries(pairMap(headers)).map(([name, headerValue]) => ({ name, value: bytesToBase64(headerValue) }));
      return mutationApi.previewRecordProduction(clusterId, topicName, { records: [{ key: key ? bytesToBase64(key) : null, value: bytesToBase64(value), headers: parsedHeaders }] }, idempotencyKey);
    });
  };

  return <form onSubmit={onSubmit} aria-label="Record production preview">
    <fieldset disabled={busy}><legend>Bounded record production</legend>
      <label htmlFor="record-topic">Exact topic</label>{' '}<input id="record-topic" required value={topicName} onChange={event => setTopicName(event.target.value)} /><br />
      <label htmlFor="record-key">UTF-8 key (optional)</label>{' '}<input id="record-key" value={key} onChange={event => setKey(event.target.value)} autoComplete="off" /><br />
      <label htmlFor="record-value">UTF-8 value</label><br /><textarea id="record-value" required value={value} onChange={event => setValue(event.target.value)} rows={5} /><br />
      <label htmlFor="record-headers">Headers, one name=value per line</label><br /><textarea id="record-headers" value={headers} onChange={event => setHeaders(event.target.value)} rows={4} />
      <p>Payload, key and header values remain browser-memory/request material only; this UI does not use localStorage or a durable payload staging surface.</p>
      <button type="submit" disabled={!topicName.trim() || !value || !idempotencyKey.trim()}>Create preview</button>
    </fieldset>
  </form>;
}

function ConsumerWorkflow({ clusterId, idempotencyKey, busy, submit }: { clusterId: string; idempotencyKey: string; busy: boolean; submit: Submit }) {
  const [action, setAction] = useState<'offset' | 'deleteGroup' | 'deleteOffsets'>('offset');
  const [groupId, setGroupId] = useState('');
  const [topicName, setTopicName] = useState('');
  const [partition, setPartition] = useState(0);
  const [selector, setSelector] = useState<ConsumerOffsetSelectorKind>('absolute');
  const [value, setValue] = useState(0);
  const [timestamp, setTimestamp] = useState('');

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    void submit(() => {
      if (action === 'deleteGroup') return mutationApi.previewConsumerDelete(clusterId, groupId, 'group', null, idempotencyKey);
      if (action === 'deleteOffsets') return mutationApi.previewConsumerDelete(clusterId, groupId, 'offsets', [{ topicName, partition }], idempotencyKey);
      const selectorInput = selector === 'timestamp'
        ? { kind: selector, timestampUtc: new Date(timestamp).toISOString() }
        : selector === 'absolute' || selector === 'relativeShift'
          ? { kind: selector, value }
          : { kind: selector };
      return mutationApi.previewConsumerOffsets(clusterId, groupId, [{ topicName, partition, selector: selectorInput }], idempotencyKey);
    });
  };

  return <form onSubmit={onSubmit} aria-label="Consumer mutation preview">
    <fieldset disabled={busy}><legend>Consumer administration</legend>
      <label htmlFor="consumer-action">Action</label>{' '}<select id="consumer-action" value={action} onChange={event => setAction(event.target.value as typeof action)}><option value="offset">Alter/reset offset</option><option value="deleteOffsets">Delete committed offset</option><option value="deleteGroup">Delete group</option></select><br />
      <label htmlFor="consumer-group">Exact group</label>{' '}<input id="consumer-group" required value={groupId} onChange={event => setGroupId(event.target.value)} /><br />
      {action !== 'deleteGroup' && <><label htmlFor="consumer-topic">Exact topic</label>{' '}<input id="consumer-topic" required value={topicName} onChange={event => setTopicName(event.target.value)} /><br /><label htmlFor="consumer-partition">Partition</label>{' '}<input id="consumer-partition" type="number" min="0" required value={partition} onChange={event => setPartition(Number(event.target.value))} /><br /></>}
      {action === 'offset' && <><label htmlFor="consumer-selector">Target selector</label>{' '}<select id="consumer-selector" value={selector} onChange={event => setSelector(event.target.value as ConsumerOffsetSelectorKind)}><option value="absolute">Absolute offset</option><option value="earliest">Earliest</option><option value="latest">Latest</option><option value="timestamp">Timestamp</option><option value="relativeShift">Relative shift</option></select><br />{(selector === 'absolute' || selector === 'relativeShift') && <><label htmlFor="consumer-offset-value">{selector === 'absolute' ? 'Offset' : 'Shift'}</label>{' '}<input id="consumer-offset-value" type="number" value={value} onChange={event => setValue(Number(event.target.value))} /><br /></>}{selector === 'timestamp' && <><label htmlFor="consumer-offset-time">Timestamp</label>{' '}<input id="consumer-offset-time" type="datetime-local" required value={timestamp} onChange={event => setTimestamp(event.target.value)} /><br /></>}</>}
      <p>Selectors are resolved to frozen concrete offsets during planning; they are not re-resolved at dispatch.</p>
      <button type="submit" disabled={!groupId.trim() || !idempotencyKey.trim() || (action !== 'deleteGroup' && !topicName.trim())}>Create preview</button>
    </fieldset>
  </form>;
}

function SchemaWorkflow({ clusterId, idempotencyKey, busy, submit }: { clusterId: string; idempotencyKey: string; busy: boolean; submit: Submit }) {
  const [action, setAction] = useState<'register' | 'compatibility' | 'globalCompatibility' | 'delete'>('register');
  const [subject, setSubject] = useState('');
  const [format, setFormat] = useState<SchemaFormatInput>('avro');
  const [schema, setSchema] = useState('');
  const [mode, setMode] = useState('backward');
  const [version, setVersion] = useState('');
  const [permanent, setPermanent] = useState(false);

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    void submit(() => {
      if (action === 'register') return mutationApi.previewSchemaRegister(clusterId, subject, { format, schema, references: [] }, idempotencyKey);
      if (action === 'compatibility') return mutationApi.previewSchemaCompatibility(clusterId, subject, mode, idempotencyKey);
      if (action === 'globalCompatibility') return mutationApi.previewSchemaCompatibility(clusterId, null, mode, idempotencyKey);
      return mutationApi.previewSchemaDelete(clusterId, subject, version ? Number(version) : null, permanent, idempotencyKey);
    });
  };

  return <form onSubmit={onSubmit} aria-label="Schema mutation preview">
    <fieldset disabled={busy}><legend>Schema Registry</legend>
      <label htmlFor="schema-action">Action</label>{' '}<select id="schema-action" value={action} onChange={event => setAction(event.target.value as typeof action)}><option value="register">Register schema</option><option value="compatibility">Alter subject compatibility</option><option value="globalCompatibility">Alter global compatibility</option><option value="delete">Delete subject/version</option></select><br />
      {action !== 'globalCompatibility' && <><label htmlFor="schema-subject">Exact subject</label>{' '}<input id="schema-subject" required value={subject} onChange={event => setSubject(event.target.value)} /><br /></>}
      {action === 'register' && <><label htmlFor="schema-format">Format</label>{' '}<select id="schema-format" value={format} onChange={event => setFormat(event.target.value as SchemaFormatInput)}><option value="avro">Avro</option><option value="protobuf">Protobuf</option><option value="jsonSchema">JSON Schema</option></select><br /><label htmlFor="schema-source">Schema source</label><br /><textarea id="schema-source" required value={schema} onChange={event => setSchema(event.target.value)} rows={8} /><p>Schema source is execution material for registration; it is re-submitted for execution and is not published in mutation status.</p></>}
      {(action === 'compatibility' || action === 'globalCompatibility') && <><label htmlFor="schema-compatibility-mode">Requested compatibility mode</label>{' '}<input id="schema-compatibility-mode" required value={mode} onChange={event => setMode(event.target.value)} /><br /></>}
      {action === 'delete' && <><label htmlFor="schema-version">Version (blank for subject)</label>{' '}<input id="schema-version" type="number" min="1" value={version} onChange={event => setVersion(event.target.value)} /><br /><label><input type="checkbox" checked={permanent} onChange={event => setPermanent(event.target.checked)} /> Permanent delete</label>{permanent && <p role="alert">Permanent schema deletion is CRITICAL and requires independent approval when provider capability is explicitly admitted.</p>}</>}
      <button type="submit" disabled={!idempotencyKey.trim() || (action !== 'globalCompatibility' && !subject.trim()) || (action === 'register' && !schema.trim())}>Create preview</button>
    </fieldset>
  </form>;
}

function ConnectWorkflow({ clusterId, connectProfileId, idempotencyKey, busy, submit }: { clusterId: string; connectProfileId: string; idempotencyKey: string; busy: boolean; submit: Submit }) {
  const [action, setAction] = useState<'create' | 'update' | 'control' | 'delete'>('create');
  const [connectorName, setConnectorName] = useState('');
  const [configuration, setConfiguration] = useState('');
  const [control, setControl] = useState<'pause' | 'resume' | 'restart'>('restart');
  const [taskId, setTaskId] = useState('');

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    void submit(() => {
      const isDefault = connectProfileId === 'default';
      if (action === 'create' || action === 'update') {
        const config = pairMap(configuration);
        return isDefault
          ? mutationApi.previewConnectConfiguration(clusterId, connectorName, action, config, idempotencyKey)
          : mutationApi.previewConnectProfileConfiguration(clusterId, connectProfileId, connectorName, action, config, idempotencyKey);
      }
      if (action === 'control') {
        const request = { action: control, taskId: taskId ? Number(taskId) : null };
        return isDefault
          ? mutationApi.previewConnectControl(clusterId, connectorName, request, idempotencyKey)
          : mutationApi.previewConnectProfileControl(clusterId, connectProfileId, connectorName, request, idempotencyKey);
      }
      return isDefault
        ? mutationApi.previewConnectDelete(clusterId, connectorName, idempotencyKey)
        : mutationApi.previewConnectProfileDelete(clusterId, connectProfileId, connectorName, idempotencyKey);
    });
  };

  return <form onSubmit={onSubmit} aria-label="Kafka Connect mutation preview">
    <fieldset disabled={busy}><legend>Kafka Connect</legend>
      <p>Connect profile: <strong>{connectProfileId}</strong>{connectProfileId === 'default' ? ' (legacy-compatible default)' : ''}</p>
      <label htmlFor="connect-action">Action</label>{' '}<select id="connect-action" value={action} onChange={event => setAction(event.target.value as typeof action)}><option value="create">Create connector</option><option value="update">Update connector</option><option value="control">Pause/resume/restart</option><option value="delete">Delete connector</option></select><br />
      <label htmlFor="connect-name">Exact connector</label>{' '}<input id="connect-name" required value={connectorName} onChange={event => setConnectorName(event.target.value)} /><br />
      {(action === 'create' || action === 'update') && <><label htmlFor="connect-config">Configuration, one key=value per line</label><br /><textarea id="connect-config" required value={configuration} onChange={event => setConfiguration(event.target.value)} rows={8} autoComplete="off" /><p>Configuration values can contain secrets. They remain request-scoped/browser-memory material and are never rendered in mutation status or provider evidence.</p></>}
      {action === 'control' && <><label htmlFor="connect-control">Control</label>{' '}<select id="connect-control" value={control} onChange={event => setControl(event.target.value as typeof control)}><option value="pause">Pause</option><option value="resume">Resume</option><option value="restart">Restart</option></select>{' '}<label htmlFor="connect-task">Task ID (optional for restart)</label>{' '}<input id="connect-task" type="number" min="0" value={taskId} onChange={event => setTaskId(event.target.value)} /></>}
      <button type="submit" disabled={!connectorName.trim() || !idempotencyKey.trim() || ((action === 'create' || action === 'update') && !configuration.trim())}>Create preview</button>
    </fieldset>
  </form>;
}

function DataJobWorkflow({ clusterId, idempotencyKey, busy, submit }: { clusterId: string; idempotencyKey: string; busy: boolean; submit: Submit }) {
  const [kind, setKind] = useState<DataJobKindInput>('forward');
  const [sourceClusterId, setSourceClusterId] = useState(clusterId);
  const [sourceProfileVersion, setSourceProfileVersion] = useState('current');
  const [destinationClusterId, setDestinationClusterId] = useState('');
  const [destinationProfileVersion, setDestinationProfileVersion] = useState('current');
  const [sourceTopic, setSourceTopic] = useState('');
  const [destinationTopic, setDestinationTopic] = useState('');
  const [sourcePartition, setSourcePartition] = useState(0);
  const [destinationPartition, setDestinationPartition] = useState(0);
  const [startInclusive, setStartInclusive] = useState(0);
  const [endExclusive, setEndExclusive] = useState(1);
  const [maxTotalRecords, setMaxTotalRecords] = useState(1000);
  const [maxTotalBytes, setMaxTotalBytes] = useState(10 * 1024 * 1024);
  const [maxDurationSeconds, setMaxDurationSeconds] = useState(600);
  const [serdeFormat, setSerdeFormat] = useState<'json' | 'cbor' | 'xml' | 'messagepack'>('json');
  const [projectedFields, setProjectedFields] = useState('');

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    if (endExclusive <= startInclusive) return;

    const transform = kind === 'reprocess'
      ? {
          kind: 'MaskedStructuredProjection' as const,
          serdeFormat,
          projectedFields: projectedFields
            .split(',')
            .map(value => value.trim())
            .filter(Boolean),
        }
      : null;

    void submit(() => mutationApi.previewDataJob(
      kind,
      {
        sourceClusterId,
        sourceProfileVersion,
        destinationClusterId,
        destinationProfileVersion,
        ranges: [{
          sourceTopic,
          sourcePartition,
          destinationTopic,
          destinationPartition,
          startInclusive,
          endExclusive,
        }],
        budget: {
          maxTotalRecords,
          maxTotalBytes,
          maxDurationSeconds,
        },
        transform,
      },
      idempotencyKey,
    ));
  };

  const invalid =
    !idempotencyKey.trim() ||
    !sourceClusterId.trim() ||
    !sourceProfileVersion.trim() ||
    !destinationClusterId.trim() ||
    !destinationProfileVersion.trim() ||
    !sourceTopic.trim() ||
    !destinationTopic.trim() ||
    endExclusive <= startInclusive ||
    maxTotalRecords < 1 ||
    maxTotalBytes < 1 ||
    maxDurationSeconds < 1 ||
    (kind === 'reprocess' && !projectedFields.trim());

  return <form onSubmit={onSubmit} aria-label="Governed data-job preview">
    <fieldset disabled={busy}><legend>Finite replay / forwarding job</legend>
      <p>Ranges are frozen at preview. Limits are finite. This workflow never creates destination topics and never accepts arbitrary scripts or provider commands.</p>
      <label htmlFor="data-job-kind">Kind</label>{' '}
      <select id="data-job-kind" value={kind} onChange={event => setKind(event.target.value as DataJobKindInput)}>
        <option value="replay">Replay</option>
        <option value="forward">Forward</option>
        <option value="dlq-forward">DLQ forward</option>
        <option value="reprocess">Reprocess with masked projection</option>
      </select><br />
      <label htmlFor="data-job-source-cluster">Source cluster</label>{' '}
      <input id="data-job-source-cluster" required value={sourceClusterId} onChange={event => setSourceClusterId(event.target.value)} />{' '}
      <label htmlFor="data-job-source-profile">Source profile version</label>{' '}
      <input id="data-job-source-profile" required value={sourceProfileVersion} onChange={event => setSourceProfileVersion(event.target.value)} /><br />
      <label htmlFor="data-job-destination-cluster">Destination cluster</label>{' '}
      <input id="data-job-destination-cluster" required value={destinationClusterId} onChange={event => setDestinationClusterId(event.target.value)} />{' '}
      <label htmlFor="data-job-destination-profile">Destination profile version</label>{' '}
      <input id="data-job-destination-profile" required value={destinationProfileVersion} onChange={event => setDestinationProfileVersion(event.target.value)} /><br />
      <label htmlFor="data-job-source-topic">Source topic</label>{' '}
      <input id="data-job-source-topic" required value={sourceTopic} onChange={event => setSourceTopic(event.target.value)} />{' '}
      <label htmlFor="data-job-source-partition">Partition</label>{' '}
      <input id="data-job-source-partition" type="number" min="0" value={sourcePartition} onChange={event => setSourcePartition(Number(event.target.value))} /><br />
      <label htmlFor="data-job-destination-topic">Destination topic</label>{' '}
      <input id="data-job-destination-topic" required value={destinationTopic} onChange={event => setDestinationTopic(event.target.value)} />{' '}
      <label htmlFor="data-job-destination-partition">Partition</label>{' '}
      <input id="data-job-destination-partition" type="number" min="0" value={destinationPartition} onChange={event => setDestinationPartition(Number(event.target.value))} /><br />
      <label htmlFor="data-job-start-offset">Start inclusive</label>{' '}
      <input id="data-job-start-offset" type="number" min="0" value={startInclusive} onChange={event => setStartInclusive(Number(event.target.value))} />{' '}
      <label htmlFor="data-job-end-offset">End exclusive</label>{' '}
      <input id="data-job-end-offset" type="number" min="1" value={endExclusive} onChange={event => setEndExclusive(Number(event.target.value))} /><br />
      <label htmlFor="data-job-max-records">Max total records</label>{' '}
      <input id="data-job-max-records" type="number" min="1" max="1000000" value={maxTotalRecords} onChange={event => setMaxTotalRecords(Number(event.target.value))} />{' '}
      <label htmlFor="data-job-max-bytes">Max total bytes</label>{' '}
      <input id="data-job-max-bytes" type="number" min="1" max="1073741824" value={maxTotalBytes} onChange={event => setMaxTotalBytes(Number(event.target.value))} />{' '}
      <label htmlFor="data-job-max-duration">Max duration seconds</label>{' '}
      <input id="data-job-max-duration" type="number" min="1" max="86400" value={maxDurationSeconds} onChange={event => setMaxDurationSeconds(Number(event.target.value))} /><br />
      {kind === 'reprocess' && <>
        <label htmlFor="data-job-serde">SerDe</label>{' '}
        <select id="data-job-serde" value={serdeFormat} onChange={event => setSerdeFormat(event.target.value as typeof serdeFormat)}>
          <option value="json">JSON</option><option value="cbor">CBOR</option><option value="xml">XML</option><option value="messagepack">MessagePack</option>
        </select>{' '}
        <label htmlFor="data-job-fields">Projected fields (comma-separated)</label>{' '}
        <input id="data-job-fields" required value={projectedFields} onChange={event => setProjectedFields(event.target.value)} />
      </>}
      <br /><button type="submit" disabled={invalid}>Create finite preview</button>
    </fieldset>
  </form>;
}

function DataGeneratorWorkflow({ clusterId, idempotencyKey, busy, submit }: { clusterId: string; idempotencyKey: string; busy: boolean; submit: Submit }) {
  const [destinationClusterId, setDestinationClusterId] = useState(clusterId);
  const [destinationProfileVersion, setDestinationProfileVersion] = useState('');
  const [destinationTopic, setDestinationTopic] = useState('');
  const [destinationPartition, setDestinationPartition] = useState(0);
  const [recordCount, setRecordCount] = useState(100);
  const [seed, setSeed] = useState(0);
  const [sourceKind, setSourceKind] = useState<DataGeneratorSourceKindInput>('BuiltInTemplate');
  const [schemaSubject, setSchemaSubject] = useState('');
  const [schemaVersion, setSchemaVersion] = useState(1);
  const [maxBatchRecords, setMaxBatchRecords] = useState(100);
  const [maxBatchBytes, setMaxBatchBytes] = useState(1024 * 1024);
  const [maxTotalRecords, setMaxTotalRecords] = useState(10_000);
  const [maxTotalBytes, setMaxTotalBytes] = useState(10 * 1024 * 1024);
  const [maxDurationSeconds, setMaxDurationSeconds] = useState(600);
  const [maxRecordsPerSecond, setMaxRecordsPerSecond] = useState(100);
  const [maxBytesPerSecond, setMaxBytesPerSecond] = useState(1024 * 1024);

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    void submit(() => mutationApi.previewDataGenerator(
      {
        destinationClusterId,
        destinationProfileVersion,
        destinationTopic,
        destinationPartition,
        recordCount,
        seed,
        source: sourceKind === 'Schema'
          ? {
              kind: 'Schema',
              schemaSubject: schemaSubject.trim(),
              schemaVersion,
              template: null,
            }
          : {
              kind: 'BuiltInTemplate',
              schemaSubject: null,
              schemaVersion: null,
              template: 'BasicJsonV1',
            },
        budget: {
          maxBatchRecords,
          maxBatchBytes,
          maxTotalRecords,
          maxTotalBytes,
          maxDurationSeconds,
          maxRecordsPerSecond,
          maxBytesPerSecond,
        },
      },
      idempotencyKey,
    ));
  };

  const sourceReady = sourceKind === 'BuiltInTemplate' || (schemaSubject.trim().length > 0 && schemaVersion > 0);

  return <form onSubmit={onSubmit} aria-label="Governed data generator preview">
    <fieldset disabled={busy}><legend>Smart Mock / bounded Data Generator</legend>
      <p>Generation is finite, deterministic and server-bounded. Generated payloads are materialized in memory only; Kafdeck does not provide an unbounded producer or durable generated-payload staging area.</p>
      <label htmlFor="generator-destination-cluster">Destination cluster</label>{' '}
      <input id="generator-destination-cluster" required value={destinationClusterId} onChange={event => setDestinationClusterId(event.target.value)} />{' '}
      <label htmlFor="generator-destination-profile">Destination profile version</label>{' '}
      <input id="generator-destination-profile" required value={destinationProfileVersion} onChange={event => setDestinationProfileVersion(event.target.value)} /><br />

      <label htmlFor="generator-destination-topic">Destination topic</label>{' '}
      <input id="generator-destination-topic" required value={destinationTopic} onChange={event => setDestinationTopic(event.target.value)} />{' '}
      <label htmlFor="generator-destination-partition">Partition</label>{' '}
      <input id="generator-destination-partition" type="number" min={0} required value={destinationPartition} onChange={event => setDestinationPartition(Number(event.target.value))} /><br />

      <label htmlFor="generator-source-kind">Source</label>{' '}
      <select id="generator-source-kind" value={sourceKind} onChange={event => setSourceKind(event.target.value as DataGeneratorSourceKindInput)}>
        <option value="BuiltInTemplate">Built-in BasicJsonV1</option>
        <option value="Schema">Schema Registry subject/version</option>
      </select>{' '}
      {sourceKind === 'Schema' && <>
        <label htmlFor="generator-schema-subject">Schema subject</label>{' '}
        <input id="generator-schema-subject" required value={schemaSubject} onChange={event => setSchemaSubject(event.target.value)} />{' '}
        <label htmlFor="generator-schema-version">Version</label>{' '}
        <input id="generator-schema-version" type="number" min={1} required value={schemaVersion} onChange={event => setSchemaVersion(Number(event.target.value))} />
      </>}
      <br />

      <label htmlFor="generator-record-count">Records</label>{' '}
      <input id="generator-record-count" type="number" min={1} max={100000} required value={recordCount} onChange={event => setRecordCount(Number(event.target.value))} />{' '}
      <label htmlFor="generator-seed">Deterministic seed</label>{' '}
      <input id="generator-seed" type="number" value={seed} onChange={event => setSeed(Number(event.target.value))} /><br />

      <details>
        <summary>Finite runtime budgets</summary>
        <p>Defaults remain HIGH risk. Increasing total/rate/duration ceilings above server defaults can raise the operation to CRITICAL and require independent approval. Hard caps are enforced server-side.</p>
        <label htmlFor="generator-max-batch-records">Max batch records</label>{' '}
        <input id="generator-max-batch-records" type="number" min={1} max={1000} value={maxBatchRecords} onChange={event => setMaxBatchRecords(Number(event.target.value))} />{' '}
        <label htmlFor="generator-max-batch-bytes">Max batch bytes</label>{' '}
        <input id="generator-max-batch-bytes" type="number" min={1} max={10 * 1024 * 1024} value={maxBatchBytes} onChange={event => setMaxBatchBytes(Number(event.target.value))} /><br />
        <label htmlFor="generator-max-total-records">Max total records</label>{' '}
        <input id="generator-max-total-records" type="number" min={1} max={100000} value={maxTotalRecords} onChange={event => setMaxTotalRecords(Number(event.target.value))} />{' '}
        <label htmlFor="generator-max-total-bytes">Max total bytes</label>{' '}
        <input id="generator-max-total-bytes" type="number" min={1} max={100 * 1024 * 1024} value={maxTotalBytes} onChange={event => setMaxTotalBytes(Number(event.target.value))} /><br />
        <label htmlFor="generator-max-duration">Max duration seconds</label>{' '}
        <input id="generator-max-duration" type="number" min={1} max={3600} value={maxDurationSeconds} onChange={event => setMaxDurationSeconds(Number(event.target.value))} />{' '}
        <label htmlFor="generator-max-record-rate">Max records/s</label>{' '}
        <input id="generator-max-record-rate" type="number" min={1} max={1000} value={maxRecordsPerSecond} onChange={event => setMaxRecordsPerSecond(Number(event.target.value))} />{' '}
        <label htmlFor="generator-max-byte-rate">Max bytes/s</label>{' '}
        <input id="generator-max-byte-rate" type="number" min={1} max={10 * 1024 * 1024} value={maxBytesPerSecond} onChange={event => setMaxBytesPerSecond(Number(event.target.value))} />
      </details>

      <p role="note">The destination must be explicitly enabled by deployment policy. No topic creation, arbitrary code/template execution, or generic Kafka producer proxy is implied.</p>
      <button type="submit" disabled={
        !idempotencyKey.trim() ||
        !destinationClusterId.trim() ||
        !destinationProfileVersion.trim() ||
        !destinationTopic.trim() ||
        recordCount < 1 ||
        !sourceReady
      }>Create generator preview</button>
    </fieldset>
  </form>;
}

function PurgeWorkflow({ clusterId, idempotencyKey, busy, submit }: { clusterId: string; idempotencyKey: string; busy: boolean; submit: Submit }) {
  const [topicName, setTopicName] = useState('');
  const [partition, setPartition] = useState(0);
  const [selector, setSelector] = useState<RecordsPurgeSelectorKind>('absolute');
  const [beforeOffset, setBeforeOffset] = useState(0);
  const [timestamp, setTimestamp] = useState('');

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    void submit(() => mutationApi.previewRecordsPurge(
      clusterId,
      [{ topicName, partition, selector: selector === 'absolute' ? { kind: selector, beforeOffset } : { kind: selector, timestampUtc: new Date(timestamp).toISOString() } }],
      idempotencyKey,
    ));
  };

  return <form onSubmit={onSubmit} aria-label="Controlled purge preview">
    <fieldset disabled={busy}><legend>Controlled record purge</legend>
      <p role="alert"><strong>Irreversible CRITICAL operation.</strong> DeleteRecords has no rollback or undo claim. The preview freezes explicit partition/offset targets and requires independent approval.</p>
      <label htmlFor="purge-topic">Exact topic</label>{' '}<input id="purge-topic" required value={topicName} onChange={event => setTopicName(event.target.value)} /><br />
      <label htmlFor="purge-partition">Partition</label>{' '}<input id="purge-partition" type="number" min="0" required value={partition} onChange={event => setPartition(Number(event.target.value))} /><br />
      <label htmlFor="purge-selector">Selector</label>{' '}<select id="purge-selector" value={selector} onChange={event => setSelector(event.target.value as RecordsPurgeSelectorKind)}><option value="absolute">Before offset</option><option value="timestamp">Timestamp</option></select><br />
      {selector === 'absolute' ? <><label htmlFor="purge-offset">Before offset</label>{' '}<input id="purge-offset" type="number" min="0" required value={beforeOffset} onChange={event => setBeforeOffset(Number(event.target.value))} /></> : <><label htmlFor="purge-time">Timestamp</label>{' '}<input id="purge-time" type="datetime-local" required value={timestamp} onChange={event => setTimestamp(event.target.value)} /></>}
      <br /><button type="submit" disabled={!topicName.trim() || !idempotencyKey.trim()}>Create destructive preview</button>
    </fieldset>
  </form>;
}