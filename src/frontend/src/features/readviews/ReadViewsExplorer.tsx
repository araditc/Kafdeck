import { t } from '../../shared/i18n.js';
import { useEffect, useRef, useState } from 'react';
import {
  kafdeckApi,
  type ConnectClusterInfo,
  type ConnectConnectorDetail,
  type ConnectConnectorSummary,
  type ConnectPluginSummary,
  type ConnectPluginValidationResult,
  type ConnectProfileSummary,
  type ControlledSerdeCapabilitiesData,
  type ControlledSerdeDecodedValue,
  type ControlledSerdeEncodedData,
  type ControlledSerdeFormat,
  type ConsumerDiagnostics,
  type ConsumerGroupDetail,
  type ConsumerGroupSummary,
  type ConsumerLag,
  type KsqlQueryResult,
  type OperationalAnalyticsResult,
  type OperationalSloResult,
  type OperationalTrendResult,
  type KsqlServerInfo,
  type LineageGraph,
  type ReadViewEnvelope,
  type StreamsApplicationSummary,
  type StreamsStateStoreObservation,
  type StreamsTopologyObservation,
  type SchemaCompatibility,
  type SchemaCompatibilityExplanation,
  type SchemaDiff,
  type SchemaMockResult,
  type SchemaReferenceGraph,
  type SchemaSubjectSummary,
  type SchemaVersionSummary,
} from '../../shared/api.js';
import { MutationOperationsPanel } from '../mutations/MutationOperationsPanel.js';
import { DataQualityPanel } from './DataQualityPanel.js';
import { StatusBadge } from '../../app/StatusBadge.js';
import {
  operationalEvidenceStatusKind,
  operationalTrendStatusKind,
} from '../../app/statusPresentation.js';
import {
  MutationApiProblem,
  mutationApi,
  type ConnectAutoRestartPolicyStatus,
  type MutationStatus,
} from '../mutations/mutationApi.js';

import { ReadViewInputError, readViewError, readViewFailure, readViewProblemText, type ReadViewProblem } from './readViewProblem.js';

function ReadViewProblemNotice({ problem }: { problem: ReadViewProblem }) {
  return <p role={problem.kind === 'denied' ? 'alert' : 'status'}>
    <StatusBadge kind={problem.kind} />{' '}
    {readViewProblemText(problem)}
  </p>;
}

function parseConfigurationLines(source: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const rawLine of source.split('\n')) {
    const line = rawLine.trim();
    if (!line) continue;
    const separator = line.indexOf('=');
    if (separator <= 0) throw new ReadViewInputError('configurationSyntax');
    const key = line.slice(0, separator).trim();
    const value = line.slice(separator + 1);
    if (!key) throw new ReadViewInputError('configurationEmptyKey');
    if (Object.prototype.hasOwnProperty.call(result, key)) throw new ReadViewInputError('configurationDuplicate', [key]);
    result[key] = value;
  }
  if (Object.keys(result).length === 0) throw new ReadViewInputError('configurationEmpty');
  return result;
}

function Limitations({ envelope }: { envelope: ReadViewEnvelope<unknown> }) {
  if (!envelope.partial || envelope.limitations.length === 0) return null;
  return <aside aria-label={t("Read-view limitations")}><strong><StatusBadge kind="partial" /> {' '}{t("Partial observation")}</strong><ul>{envelope.limitations.map(item => <li key={item.code}>{item.code}: {item.message}</li>)}</ul></aside>;
}

export function ReadViewsExplorer({ clusterId }: { clusterId: string }) {
  const [consumerGroups, setConsumerGroups] = useState<ReadViewEnvelope<ConsumerGroupSummary[]> | null>(null);
  const [consumerError, setConsumerError] = useState<ReadViewProblem | null>(null);
  const [groupDetail, setGroupDetail] = useState<ReadViewEnvelope<ConsumerGroupDetail> | null>(null);
  const [groupLag, setGroupLag] = useState<ReadViewEnvelope<ConsumerLag> | null>(null);
  const [groupDiagnostics, setGroupDiagnostics] = useState<ReadViewEnvelope<ConsumerDiagnostics> | null>(null);

  const [operationalAnalytics, setOperationalAnalytics] = useState<OperationalAnalyticsResult | null>(null);
  const [operationalTrend, setOperationalTrend] = useState<OperationalTrendResult | null>(null);
  const [operationalSlo, setOperationalSlo] = useState<OperationalSloResult | null>(null);
  const [operationalError, setOperationalError] = useState<ReadViewProblem | null>(null);
  const [sloError, setSloError] = useState<ReadViewProblem | null>(null);
  const [operationalBusy, setOperationalBusy] = useState(false);
  const [activeSloRequests, setActiveSloRequests] = useState(0);
  const sloBusy = activeSloRequests > 0;
  const [sloThreshold, setSloThreshold] = useState(100);
  const [sloTarget, setSloTarget] = useState(0.99);
  const consumerLoadGeneration = useRef(0);
  const sloEvaluationGeneration = useRef(0);

  const [subjects, setSubjects] = useState<ReadViewEnvelope<SchemaSubjectSummary[]> | null>(null);
  const [schemaError, setSchemaError] = useState<ReadViewProblem | null>(null);
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

  const [serdeCapabilities, setSerdeCapabilities] = useState<ControlledSerdeCapabilitiesData | null>(null);
  const [serdeFormat, setSerdeFormat] = useState<ControlledSerdeFormat>('cbor');
  const [serdePayloadBase64, setSerdePayloadBase64] = useState('');
  const [serdeStructuredJson, setSerdeStructuredJson] = useState('');
  const [serdeDecoded, setSerdeDecoded] = useState<ControlledSerdeDecodedValue | null>(null);
  const [serdeEncoded, setSerdeEncoded] = useState<ControlledSerdeEncodedData | null>(null);
  const [serdeError, setSerdeError] = useState<ReadViewProblem | null>(null);
  const [serdeBusy, setSerdeBusy] = useState(false);

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
  const [connectError, setConnectError] = useState<ReadViewProblem | null>(null);
  const connectLoadGeneration = useRef(0);
  const [autoRestartStatus, setAutoRestartStatus] = useState<ConnectAutoRestartPolicyStatus | null>(null);
  const [autoRestartOperation, setAutoRestartOperation] = useState<MutationStatus | null>(null);
  const [autoRestartError, setAutoRestartError] = useState<ReadViewProblem | null>(null);
  const [autoRestartBusy, setAutoRestartBusy] = useState(false);

  const [ksqlInfo, setKsqlInfo] = useState<ReadViewEnvelope<KsqlServerInfo> | null>(null);
  const [ksqlStatement, setKsqlStatement] = useState('');
  const [ksqlResult, setKsqlResult] = useState<ReadViewEnvelope<KsqlQueryResult> | null>(null);
  const [ksqlBusy, setKsqlBusy] = useState(false);
  const [ksqlError, setKsqlError] = useState<ReadViewProblem | null>(null);

  const [streamsApplications, setStreamsApplications] = useState<ReadViewEnvelope<StreamsApplicationSummary[]> | null>(null);
  const [streamsTopology, setStreamsTopology] = useState<ReadViewEnvelope<StreamsTopologyObservation> | null>(null);
  const [streamsStores, setStreamsStores] = useState<ReadViewEnvelope<StreamsStateStoreObservation> | null>(null);
  const [streamsError, setStreamsError] = useState<ReadViewProblem | null>(null);
  const [lineage, setLineage] = useState<ReadViewEnvelope<LineageGraph> | null>(null);
  const [lineageError, setLineageError] = useState<ReadViewProblem | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    const initialConnectGeneration = ++connectLoadGeneration.current;
    ++consumerLoadGeneration.current;
    ++sloEvaluationGeneration.current;
    setConsumerGroups(null); setConsumerError(null); setGroupDetail(null); setGroupLag(null); setGroupDiagnostics(null);
    setOperationalAnalytics(null); setOperationalTrend(null); setOperationalSlo(null); setOperationalError(null); setSloError(null); setOperationalBusy(false); setSloThreshold(100); setSloTarget(0.99);
    setSubjects(null); setSchemaError(null); setSelectedSubject(null); setVersions(null); setCompatibility(null); setSchemaDiff(null); setReferenceGraph(null); setCompatibilityExplanation(null); setSchemaMock(null);
    setSerdeCapabilities(null); setSerdeFormat('cbor'); setSerdePayloadBase64(''); setSerdeStructuredJson(''); setSerdeDecoded(null); setSerdeEncoded(null); setSerdeError(null); setSerdeBusy(false);
    setConnectProfiles(null); setSelectedConnectProfileId(null); setConnectInfo(null); setConnectors(null); setConnectorDetail(null); setConnectPlugins(null); setSelectedPluginClass(null); setPluginConfiguration(''); setPluginFieldValues({}); setPluginValidation(null); setConnectError(null); setAutoRestartStatus(null); setAutoRestartOperation(null); setAutoRestartError(null); setAutoRestartBusy(false);
    setKsqlInfo(null); setKsqlStatement(''); setKsqlResult(null); setKsqlBusy(false); setKsqlError(null);
    setStreamsApplications(null); setStreamsTopology(null); setStreamsStores(null); setStreamsError(null);
    setLineage(null); setLineageError(null);

    void kafdeckApi.listConsumerGroups(clusterId, controller.signal)
      .then(setConsumerGroups)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setConsumerError(readViewError(reason)); });

    void kafdeckApi.listSchemaSubjects(clusterId, controller.signal)
      .then(setSubjects)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setSchemaError(readViewError(reason)); });

    void kafdeckApi.getControlledSerdeCapabilities(controller.signal)
      .then(setSerdeCapabilities)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setSerdeError(readViewError(reason)); });

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
      .catch(reason => {
        if (
          initialConnectGeneration === connectLoadGeneration.current &&
          !(reason instanceof DOMException && reason.name === 'AbortError')
        ) {
          setConnectError(readViewError(reason));
        }
      });

    void kafdeckApi.getKsqlInfo(clusterId, controller.signal)
      .then(setKsqlInfo)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setKsqlError(readViewError(reason)); });

    void kafdeckApi.listStreamsApplications(clusterId, controller.signal)
      .then(setStreamsApplications)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setStreamsError(readViewError(reason)); });

    void kafdeckApi.getLineage(clusterId, controller.signal)
      .then(setLineage)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setLineageError(readViewError(reason)); });

    return () => controller.abort();
  }, [clusterId]);

  const runKsqlQuery = async () => {
    const statement = ksqlStatement.trim();
    if (!statement) return;

    setKsqlBusy(true);
    setKsqlError(null);
    setKsqlResult(null);
    try {
      setKsqlResult(await kafdeckApi.executeKsqlQuery(
        clusterId,
        statement,
        {
          maxRows: 1000,
          maxBytes: 2 * 1024 * 1024,
          maxDurationSeconds: 30,
        },
      ));
    } catch (reason) {
      setKsqlError(readViewError(reason));
    } finally {
      setKsqlBusy(false);
    }
  };

  const openStreamsApplication = async (applicationId: string) => {
    setStreamsError(null);
    setStreamsTopology(null);
    setStreamsStores(null);
    try {
      const [topology, stores] = await Promise.all([
        kafdeckApi.getStreamsTopology(clusterId, applicationId),
        kafdeckApi.getStreamsStateStores(clusterId, applicationId),
      ]);
      setStreamsTopology(topology);
      setStreamsStores(stores);
    } catch (reason) {
      setStreamsError(readViewError(reason));
    }
  };

  const openConsumer = async (groupId: string) => {
    const generation = ++consumerLoadGeneration.current;
    setConsumerError(null); setGroupDetail(null); setGroupLag(null); setGroupDiagnostics(null);
    ++sloEvaluationGeneration.current;
    setOperationalAnalytics(null); setOperationalTrend(null); setOperationalSlo(null); setOperationalError(null); setSloError(null); setOperationalBusy(false);
    try {
      const [detail, lag, diagnostics] = await Promise.all([
        kafdeckApi.getConsumerGroup(clusterId, groupId),
        kafdeckApi.getConsumerLag(clusterId, groupId),
        kafdeckApi.getConsumerDiagnostics(clusterId, groupId),
      ]);
      if (generation !== consumerLoadGeneration.current) return;
      setGroupDetail(detail); setGroupLag(lag); setGroupDiagnostics(diagnostics);
    } catch (reason) {
      if (generation === consumerLoadGeneration.current) {
        setConsumerError(readViewError(reason));
      }
      return;
    }

    setOperationalBusy(true);
    try {
      const [live, trend] = await Promise.all([
        kafdeckApi.getConsumerOperationalAnalytics(clusterId, groupId),
        kafdeckApi.getConsumerOperationalTrend(clusterId, groupId),
      ]);
      if (generation !== consumerLoadGeneration.current) return;
      setOperationalAnalytics(live);
      setOperationalTrend(trend);
    } catch (reason) {
      if (generation === consumerLoadGeneration.current) {
        setOperationalError(readViewError(reason));
      }
    } finally {
      if (generation === consumerLoadGeneration.current) {
        setOperationalBusy(false);
      }
    }
  };

  const evaluateConsumerSlo = async () => {
    if (!groupDetail) return;

    const generation = consumerLoadGeneration.current;
    const sloGeneration = ++sloEvaluationGeneration.current;
    const groupId = groupDetail.data.groupId;
    const threshold = Number(sloThreshold);
    const target = Number(sloTarget);
    if (!Number.isFinite(threshold) || threshold < 0 || !Number.isFinite(target) || target <= 0 || target >= 1) {
      setSloError({
        kind: 'unknown',
        copy: 'sloInvalid',
      });
      return;
    }

    setActiveSloRequests(count => count + 1);
    setSloError(null);
    setOperationalSlo(null);
    try {
      const result = await kafdeckApi.getConsumerOperationalSlo(
        clusterId,
        groupId,
        threshold,
        target,
      );
      if (
        generation !== consumerLoadGeneration.current ||
        sloGeneration !== sloEvaluationGeneration.current
      ) return;
      setOperationalSlo(result);
    } catch (reason) {
      if (
        generation === consumerLoadGeneration.current &&
        sloGeneration === sloEvaluationGeneration.current
      ) {
        setSloError(readViewError(reason));
      }
    } finally {
      setActiveSloRequests(count =>
        Math.max(0, count - 1));
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

  const decodeSerde = async () => {
    if (!serdePayloadBase64.trim()) return;
    setSerdeBusy(true); setSerdeError(null); setSerdeDecoded(null); setSerdeEncoded(null);
    try {
      setSerdeDecoded(await kafdeckApi.decodeControlledSerde(serdeFormat, serdePayloadBase64.trim()));
    } catch (reason) {
      setSerdeError(readViewError(reason));
    } finally {
      setSerdePayloadBase64('');
      setSerdeBusy(false);
    }
  };

  const encodeSerde = async () => {
    if (!serdeStructuredJson.trim()) return;
    setSerdeBusy(true); setSerdeError(null); setSerdeDecoded(null); setSerdeEncoded(null);
    try {
      const structuredValue = JSON.parse(serdeStructuredJson) as unknown;
      setSerdeEncoded(await kafdeckApi.encodeControlledSerde(serdeFormat, structuredValue));
    } catch (reason) {
      setSerdeError(readViewError(reason));
    } finally {
      setSerdeStructuredJson('');
      setSerdeBusy(false);
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
    setAutoRestartStatus(null);
    setAutoRestartOperation(null);
    setAutoRestartError(null);
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
    setAutoRestartStatus(null); setAutoRestartOperation(null); setAutoRestartError(null);
    try {
      setConnectorDetail(await kafdeckApi.getConnectProfileConnector(
        clusterId,
        selectedConnectProfileId,
        name,
      ));
    } catch (reason) {
      setConnectError(readViewError(reason));
      return;
    }

    try {
      setAutoRestartStatus(await mutationApi.getConnectAutoRestartPolicy(
        clusterId,
        selectedConnectProfileId,
        name,
      ));
    } catch (reason) {
      if (reason instanceof MutationApiProblem && reason.status === 404) {
        // Mutation/auto-restart endpoints are intentionally absent when
        // governed mutation mode is disabled. Read-only Connect remains usable.
        setAutoRestartStatus(null);
        return;
      }

      setAutoRestartError(
        readViewFailure(reason, 'autoRestartStatus'),
      );
    }
  };

  const previewAutoRestart = async (enabled: boolean) => {
    if (!selectedConnectProfileId || !connectorDetail) return;
    setAutoRestartBusy(true);
    setAutoRestartError(null);
    try {
      const operation = await mutationApi.previewConnectAutoRestartPolicy(
        clusterId,
        selectedConnectProfileId,
        connectorDetail.data.name,
        { enabled },
        crypto.randomUUID(),
      );
      setAutoRestartOperation(operation);
    } catch (reason) {
      setAutoRestartError(readViewFailure(reason, 'autoRestartPreview'));
    } finally {
      setAutoRestartBusy(false);
    }
  };

  const refreshAutoRestartOperation = async () => {
    if (!autoRestartOperation) return;
    setAutoRestartBusy(true);
    setAutoRestartError(null);
    try {
      setAutoRestartOperation(await mutationApi.get(autoRestartOperation.operationId));
    } catch (reason) {
      setAutoRestartError(readViewFailure(reason, 'autoRestartRefresh'));
    } finally {
      setAutoRestartBusy(false);
    }
  };

  const applyAutoRestart = async () => {
    if (!selectedConnectProfileId || !connectorDetail || !autoRestartOperation) return;
    setAutoRestartBusy(true);
    setAutoRestartError(null);
    try {
      const operation = await mutationApi.applyConnectAutoRestartPolicy(
        clusterId,
        selectedConnectProfileId,
        connectorDetail.data.name,
        autoRestartOperation.operationId,
      );
      setAutoRestartOperation(operation);
      setAutoRestartStatus(await mutationApi.getConnectAutoRestartPolicy(
        clusterId,
        selectedConnectProfileId,
        connectorDetail.data.name,
      ));
    } catch (reason) {
      setAutoRestartError(readViewFailure(reason, 'autoRestartApply'));
    } finally {
      setAutoRestartBusy(false);
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
      <h2 id="consumers-title">{t("Consumer groups")}</h2>
      {consumerError && <ReadViewProblemNotice problem={consumerError} />}
      {!consumerGroups && !consumerError && <p role="status">{t("Loading authorized consumer groups…")}</p>}
      {consumerGroups && <>
        <Limitations envelope={consumerGroups} />
        {consumerGroups.data.length === 0 ? <p>{t("No authorized consumer groups are observable.")}</p> :
          <table><thead><tr><th scope="col">{t("Group")}</th><th scope="col">{t("State")}</th><th scope="col">{t("Members")}</th></tr></thead>
            <tbody>{consumerGroups.data.map(group => <tr key={group.groupId}>
              <th scope="row"><button type="button" onClick={() => void openConsumer(group.groupId)}><bdi dir="ltr" lang="en">{group.groupId}</bdi></button></th>
              <td>{group.state}</td><td>{group.memberCount ?? t("Detail required")}</td>
            </tr>)}</tbody></table>}
      </>}
      {groupDetail && groupLag && groupDiagnostics && <article aria-labelledby="consumer-detail-title">
        <h3 id="consumer-detail-title"><bdi dir="ltr" lang="en">{groupDetail.data.groupId}</bdi></h3>
        <p>{t("Kafka state:")}{' '}{groupDetail.data.state} {' '}{t("· Diagnostic:")}{' '}{groupDiagnostics.data.state} {' '}{t("· Total lag:")}{' '}{groupLag.data.totalLag ?? t("Unknown")}</p>
        <p>{t("Members:")}{' '}<bdi dir="ltr" lang="en">{groupDetail.data.members.length}</bdi> {' '}{t("· Metrics:")}{' '}{groupDiagnostics.data.metricsAvailable ? t("available") : t("unavailable")} {' '}{t("· History:")}{' '}{groupDiagnostics.data.historyAvailable ? t("available") : t("unavailable")}</p>
        <Limitations envelope={groupLag} /><Limitations envelope={groupDiagnostics} />
        {groupDiagnostics.data.evidence.length > 0 && <><h4>{t("Evidence")}</h4><ul>{groupDiagnostics.data.evidence.map(item => <li key={item.code}>{item.safeMessage}</li>)}</ul></>}
        {groupLag.data.partitions.length > 0 && <><h4>{t("Offsets and lag")}</h4><table><thead><tr><th scope="col">{t("Topic")}</th><th scope="col">{t("Partition")}</th><th scope="col">{t("Committed")}</th><th scope="col">{t("End")}</th><th scope="col">{t("Lag")}</th><th scope="col">{t("State")}</th></tr></thead><tbody>{groupLag.data.partitions.map(item => <tr key={`${item.topic}-${item.partition}`}><td>{item.topic}</td><td>{item.partition}</td><td>{item.committedOffset ?? t("Unknown")}</td><td>{item.endOffset ?? t("Unknown")}</td><td>{item.lag ?? t("Unknown")}</td><td>{item.state}</td></tr>)}</tbody></table></>}

        <section aria-labelledby="consumer-operational-analytics-title">
          <h4 id="consumer-operational-analytics-title">{t("Operational analytics")}</h4>
          <p>{t("Bounded live and historical observations. Missing provider evidence remains unavailable; Kafdeck does not synthesize throughput or latency.")}</p>
          {operationalBusy && !operationalAnalytics && <p role="status">{t("Loading bounded operational evidence…")}</p>}
          {operationalError && <ReadViewProblemNotice problem={operationalError} />}
          {operationalAnalytics && <>
            <h5>{t("Live evidence")}</h5>
            <table>
              <thead><tr><th scope="col">{t("Metric")}</th><th scope="col">{t("State")}</th><th scope="col">{t("Value")}</th><th scope="col">{t("Source")}</th><th scope="col">{t("Observed")}</th></tr></thead>
              <tbody>{operationalAnalytics.items.map(item => <tr key={item.metric}>
                <th scope="row">{item.metric}</th>
                <td><StatusBadge kind={operationalEvidenceStatusKind(item.state)} label={item.state} /></td>
                <td>{item.value ?? t("Unavailable")}</td>
                <td>{item.source}</td>
                <td>{item.observedAtUtc ? item.observedAtUtc : t("No observation")}</td>
              </tr>)}</tbody>
            </table>
            {operationalAnalytics.truncated && <p><StatusBadge kind="partial" /> {operationalAnalytics.limitReason ?? t("Live evidence was truncated by a server limit.")}</p>}
          </>}

          {operationalTrend && <>
            <h5>{t("Lag trend")}</h5>
            <p>
              <StatusBadge kind={operationalTrendStatusKind(operationalTrend.state)} label={operationalTrend.state} />{' '}
              {t("Provider:")}{' '}{operationalTrend.provider} {' '}{t("· Points:")}{' '}<bdi dir="ltr" lang="en">{operationalTrend.points.length}</bdi>
              {operationalTrend.truncated ? t(" · Truncated: {0}", [operationalTrend.limitReason ?? t("server limit")]) : ''}
            </p>
            {operationalTrend.points.length === 0 ? <p>{t("No historical lag points are available for the bounded window.")}</p> :
              <table>
                <thead><tr><th scope="col">{t("Observed")}</th><th scope="col">{t("Average")}</th><th scope="col">{t("Min")}</th><th scope="col">{t("Max")}</th><th scope="col">{t("Samples")}</th><th scope="col">{t("Resolution")}</th><th scope="col">{t("State")}</th></tr></thead>
                <tbody>{operationalTrend.points.slice(-100).map((point, index) => <tr key={`${point.observedAtUtc}-${index}`}>
                  <td>{point.observedAtUtc}</td>
                  <td>{point.average}</td>
                  <td>{point.min}</td>
                  <td>{point.max}</td>
                  <td>{point.count}</td>
                  <td>{point.resolutionSeconds === 0 ? t("raw") : `${point.resolutionSeconds}s`}</td>
                  <td><StatusBadge kind={operationalEvidenceStatusKind(point.state)} label={point.state} /></td>
                </tr>)}</tbody>
              </table>}
          </>}

          <fieldset>
            <legend>{t("Operator-triggered lag SLO")}</legend>
            {sloError && <ReadViewProblemNotice problem={sloError} />}
            <p>{t("SLO evaluation is read-only and runs only when requested. It does not create alerts or perform Kafka mutations.")}</p>
            <label htmlFor="consumer-slo-threshold">{t("Maximum good lag")}</label>{' '}
            <input dir="auto" id="consumer-slo-threshold" type="number" min={0} value={sloThreshold} onChange={event => { ++sloEvaluationGeneration.current; setSloThreshold(Number(event.target.value)); setOperationalSlo(null); setSloError(null); }} />{' '}
            <label htmlFor="consumer-slo-target">{t("Target fraction")}</label>{' '}
            <input dir="auto" id="consumer-slo-target" type="number" min={0.0001} max={0.9999} step={0.001} value={sloTarget} onChange={event => { ++sloEvaluationGeneration.current; setSloTarget(Number(event.target.value)); setOperationalSlo(null); setSloError(null); }} />{' '}
            <button type="button" disabled={operationalBusy || sloBusy} onClick={() => void evaluateConsumerSlo()}>
              {sloBusy ? t("Evaluating…") : t("Evaluate bounded SLO")}
            </button>
          </fieldset>

          {operationalSlo && <div aria-live="polite">
            <h5>{t("SLO result")}</h5>
            <p>
              <StatusBadge kind={operationalEvidenceStatusKind(operationalSlo.state)} label={operationalSlo.state} />{' '}
              {t("Evaluated samples:")}{' '}{operationalSlo.evaluatedPoints} {' '}{t("· Good:")}{' '}{operationalSlo.goodPoints}
            </p>
            <p>
              {t("Evaluated definition: maximum good lag")}{' '}{operationalSlo.definition.maximumGoodValue} ·
              {' '}{t("target")}{' '}{(operationalSlo.definition.targetFraction * 100).toFixed(2)}% ·
              {' '}{t("window")}{' '}{operationalSlo.fromUtc} – {operationalSlo.toUtc}
            </p>
            <p>
              {t("Compliance:")}{' '}{operationalSlo.complianceFraction === null ? t("Unavailable") : `${(operationalSlo.complianceFraction * 100).toFixed(2)}%`} ·
              {' '}{t("Burn rate:")}{' '}{operationalSlo.burnRate === null ? t("Unavailable") : operationalSlo.burnRate.toFixed(2)}
            </p>
            {operationalSlo.reasonCode && <p>{t("Evidence limitation:")}{' '}<bdi dir="ltr" lang="en">{operationalSlo.reasonCode}</bdi></p>}
          </div>}
        </section>
      </article>}
    </section>

    <section className="card kafdeck-card" id="schemas" aria-labelledby="schemas-title">
      <h2 id="schemas-title">Schema Registry</h2>
      {schemaError && <ReadViewProblemNotice problem={schemaError} />}
      {!subjects && !schemaError && <p role="status">{t("Loading authorized schema subjects…")}</p>}
      {subjects && <>
        <Limitations envelope={subjects} />
        {subjects.data.length === 0 ? <p>{t("No authorized schema subjects are observable.")}</p> :
          <ul>{subjects.data.map(item => <li key={item.subject}><button type="button" onClick={() => void openSubject(item.subject)}>{item.subject}</button></li>)}</ul>}
      </>}
      {selectedSubject && versions && <article aria-labelledby="schema-detail-title">
        <h3 id="schema-detail-title">{selectedSubject}</h3>
        <p>{t("Compatibility:")}{' '}{compatibility?.data.mode ?? t("Unknown")}{compatibility?.data.isInherited ? ' (inherited)' : ''}</p>
        {compatibilityExplanation && <aside aria-labelledby="schema-compatibility-explanation-title">
          <h4 id="schema-compatibility-explanation-title">{t("Compatibility explanation")}</h4>
          <p>{compatibilityExplanation.data.summary}</p>
          <p>{t("Scope:")}{' '}{compatibilityExplanation.data.scope}</p>
          <ul>{compatibilityExplanation.data.rules.map(rule => <li key={rule}>{rule}</li>)}</ul>
        </aside>}
        <table><thead><tr><th scope="col">{t("Version")}</th><th scope="col">{t("Schema ID")}</th><th scope="col">{t("Format")}</th><th scope="col">{t("References")}</th></tr></thead><tbody>{versions.data.map(item => <tr key={item.version}><td>{item.version}</td><td><bdi dir="ltr" lang="en">{item.schemaId}</bdi></td><td>{item.format}</td><td><bdi dir="ltr" lang="en">{item.references.length}</bdi></td></tr>)}</tbody></table>
        {versions.data.length > 0 && <div>
          <label htmlFor="schema-left-version">{t("Left version")}</label>{' '}
          <select id="schema-left-version" value={leftVersion ?? ''} onChange={event => setLeftVersion(Number(event.target.value))}>{versions.data.map(item => <option key={item.version} value={item.version}>{item.version}</option>)}</select>{' '}
          <label htmlFor="schema-right-version">{t("Right/tooling version")}</label>{' '}
          <select id="schema-right-version" value={rightVersion ?? ''} onChange={event => { setRightVersion(Number(event.target.value)); setReferenceGraph(null); setSchemaMock(null); }}>{versions.data.map(item => <option key={item.version} value={item.version}>{item.version}</option>)}</select>{' '}
          <button type="button" onClick={() => void compareSchemas()}>{t("Compare versions")}</button>{' '}
          <button type="button" onClick={() => void inspectSchemaReferences()}>{t("Inspect references")}</button>
        </div>}
        {schemaDiff && <div><h4>{t("Schema diff")}</h4>{schemaDiff.data.isEqual ? <p>{t("Selected versions are textually equivalent after normalization.")}</p> : <ul>{schemaDiff.data.hunks.map((hunk, index) => <li key={index}>{t("Left")}{' '}{hunk.leftStartLine} (<bdi dir="ltr" lang="en">{hunk.leftLineCount}</bdi> {' '}{t("line(s)) → right")}{' '}{hunk.rightStartLine} (<bdi dir="ltr" lang="en">{hunk.rightLineCount}</bdi> {' '}{t("line(s)); removed")}{' '}<bdi dir="ltr" lang="en">{hunk.removedLines.length}</bdi>{t(", added")}{' '}<bdi dir="ltr" lang="en">{hunk.addedLines.length}</bdi>.</li>)}</ul>}</div>}
        {referenceGraph && <div>
          <h4>{t("Reference graph")}</h4>
          <p><bdi dir="ltr" lang="en">{referenceGraph.data.nodes.length}</bdi> {' '}{t("node(s),")}{' '}<bdi dir="ltr" lang="en">{referenceGraph.data.edges.length}</bdi> {' '}{t("edge(s),")}{' '}<bdi dir="ltr" lang="en">{referenceGraph.data.totalSchemaBytes}</bdi> {' '}{t("schema byte(s). Cycle:")}{' '}{referenceGraph.data.hasCycle ? t("detected") : t("not detected")}.</p>
          {referenceGraph.data.nodes.length > 0 && <table><thead><tr><th scope="col">{t("Subject")}</th><th scope="col">{t("Version")}</th><th scope="col">{t("Depth")}</th><th scope="col">{t("Format")}</th></tr></thead><tbody>{referenceGraph.data.nodes.map(node => <tr key={`${node.subject}-${node.version}`}><th scope="row">{node.subject}</th><td>{node.version}</td><td>{node.depth}</td><td>{node.format}</td></tr>)}</tbody></table>}
          {referenceGraph.data.edges.length > 0 && <ul>{referenceGraph.data.edges.map((edge, index) => <li key={`${edge.fromSubject}-${edge.fromVersion}-${edge.name}-${index}`}>{edge.fromSubject} v<bdi dir="ltr" lang="en">{edge.fromVersion}</bdi> → {edge.toSubject} v<bdi dir="ltr" lang="en">{edge.toVersion}</bdi> (<bdi dir="auto">{edge.name}</bdi>)</li>)}</ul>}
        </div>}
        {rightVersion !== null && <div aria-labelledby="schema-mock-title">
          <h4 id="schema-mock-title">{t("Bounded local mock")}</h4>
          <p>{t("Examples are generated in memory only. This action does not register a schema or produce Kafka records.")}</p>
          <label htmlFor="schema-mock-count">{t("Examples")}</label>{' '}
          <input dir="auto" id="schema-mock-count" type="number" min={1} max={10} value={mockCount} onChange={event => setMockCount(Number(event.target.value))} />{' '}
          <label htmlFor="schema-mock-seed">{t("Seed")}</label>{' '}
          <input dir="ltr" id="schema-mock-seed" type="number" value={mockSeed} onChange={event => setMockSeed(Number(event.target.value))} />{' '}
          <button type="button" onClick={() => void generateSchemaMock()}>{t("Generate examples")}</button>
        </div>}
        {schemaMock && <div>
          <h4>{t("Generated examples")}</h4>
          <p>{t("Format:")}{' '}{schemaMock.data.format} {' '}{t("· Seed:")}{' '}{schemaMock.data.seed} {' '}{t("· Total bytes:")}{' '}<bdi dir="ltr" lang="en">{schemaMock.data.totalBytes}</bdi></p>
          {schemaMock.data.examples.map(example => <pre tabIndex={0} dir="ltr" lang="en" key={example.index} aria-label={t("Generated schema example {0}", [example.index + 1])}><code dir="ltr" lang="en">{example.json}</code></pre>)}
        </div>}
      </article>}
    </section>

    <section className="card kafdeck-card" id="serde" aria-labelledby="serde-title">
      <h2 id="serde-title">{t("Controlled SerDe tooling")}</h2>
      <p>{t("Local bounded tooling for CBOR, XML and MessagePack. No provider proxy, runtime plugin loading, file/network resolution or durable payload storage is used.")}</p>
      {serdeError && <ReadViewProblemNotice problem={serdeError} />}
      {!serdeCapabilities && !serdeError && <p role="status">{t("Loading controlled SerDe capabilities…")}</p>}
      {serdeCapabilities && <>
        <p>{t("Server bounds: input")}{' '}<bdi dir="ltr" lang="en">{serdeCapabilities.limits.maxInputBytes}</bdi> {' '}{t("bytes · output")}{' '}<bdi dir="ltr" lang="en">{serdeCapabilities.limits.maxOutputBytes}</bdi> {' '}{t("bytes · depth")}{' '}{serdeCapabilities.limits.maxDepth} {' '}{t("· nodes")}{' '}{serdeCapabilities.limits.maxNodes}.</p>
        <table><thead><tr><th scope="col">{t("Format")}</th><th scope="col">{t("Decode")}</th><th scope="col">{t("Encode")}</th><th scope="col">{t("Limitations")}</th></tr></thead><tbody>
          {serdeCapabilities.formats.map(capability => <tr key={capability.format}>
            <th scope="row">{capability.format}</th>
            <td>{capability.decodeSupported ? t("Supported") : t("Unsupported")}</td>
            <td>{capability.encodeSupported ? t("Supported") : t("Unsupported")}</td>
            <td>{capability.limitations.join('; ')}</td>
          </tr>)}
        </tbody></table>
        <label htmlFor="serde-format">{t("Format")}</label>{' '}
        <select id="serde-format" value={serdeFormat} onChange={event => {
          setSerdeFormat(event.target.value as ControlledSerdeFormat);
          setSerdeDecoded(null); setSerdeEncoded(null); setSerdeError(null);
          setSerdePayloadBase64(''); setSerdeStructuredJson('');
        }}>
          {serdeCapabilities.formats.map(capability => <option key={capability.format} value={capability.format}>{capability.format}</option>)}
        </select>

        <div>
          <h3>{t("Decode")}</h3>
          <label htmlFor="serde-payload-base64">{t("Base64 payload")}</label>
          <textarea dir="auto" id="serde-payload-base64" rows={4} value={serdePayloadBase64} onChange={event => setSerdePayloadBase64(event.target.value)} autoComplete="off" />
          <button type="button" disabled={serdeBusy || serdePayloadBase64.trim().length === 0} onClick={() => void decodeSerde()}>{t("Decode locally")}</button>
        </div>

        <div>
          <h3>{t("Encode")}</h3>
          <label htmlFor="serde-structured-json">{t("Structured JSON")}</label>
          <textarea dir="ltr" id="serde-structured-json" rows={6} value={serdeStructuredJson} onChange={event => setSerdeStructuredJson(event.target.value)} autoComplete="off" />
          <button type="button" disabled={serdeBusy || serdeStructuredJson.trim().length === 0} onClick={() => void encodeSerde()}>{t("Encode locally")}</button>
        </div>
      </>}
      {serdeDecoded && <article aria-labelledby="serde-decoded-title"><h3 id="serde-decoded-title">{t("Decoded value")}</h3><pre tabIndex={0} dir="ltr" lang="en"><code dir="ltr" lang="en">{JSON.stringify(serdeDecoded.structuredValue, null, 2)}</code></pre></article>}
      {serdeEncoded && <article aria-labelledby="serde-encoded-title"><h3 id="serde-encoded-title">{t("Encoded payload")}</h3><p><bdi dir="ltr" lang="en">{serdeEncoded.byteCount}</bdi> {' '}{t("bytes")}</p><pre tabIndex={0} dir="ltr" lang="en"><code dir="ltr" lang="en">{serdeEncoded.payloadBase64}</code></pre></article>}
    </section>

    <section className="card kafdeck-card" id="ecosystem" aria-labelledby="ecosystem-title">
      <h2 id="ecosystem-title">{t("Ecosystem read views")}</h2>
      <h3 id="connect-title">Kafka Connect</h3>
      {connectError && <ReadViewProblemNotice problem={connectError} />}
      {!connectProfiles && !connectError && <p role="status">{t("Loading configured Connect profiles…")}</p>}
      {connectProfiles && connectProfiles.data.length === 0 && <p>{t("No Kafka Connect profiles are configured.")}</p>}
      {connectProfiles && connectProfiles.data.length > 0 && <div>
        <label htmlFor="connect-profile-selector">{t("Connect profile")}</label>{' '}
        <select
          id="connect-profile-selector"
          value={selectedConnectProfileId ?? ''}
          onChange={event => void openConnectProfile(event.target.value)}
        >
          {connectProfiles.data.map(profile => <option key={profile.id} value={profile.id}>
            {profile.id}{profile.isDefault ? ' (default)' : ''}
          </option>)}
        </select>
        {selectedConnectProfileId && <p>{t("Profile:")}{' '}<strong><bdi dir="ltr" lang="en">{selectedConnectProfileId}</bdi></strong> {' '}{t("· Mutation provider:")}{' '}{connectProfiles.data.find(profile => profile.id === selectedConnectProfileId)?.mutationProviderProfile ?? t("Unknown")}</p>}
      </div>}
      {connectInfo && <p>{t("Version:")}{' '}{connectInfo.data.version ?? t("Unknown")} {' '}{t("· Kafka cluster:")}{' '}{connectInfo.data.kafkaClusterId ?? t("Unknown")}</p>}
      {connectors && (connectors.data.length === 0 ? <p>{t("No authorized connectors are observable in this profile.")}</p> : <ul>{connectors.data.map(item => <li key={item.name}><button type="button" onClick={() => void openConnector(item.name)}><bdi dir="auto">{item.name}</bdi></button></li>)}</ul>)}
      {connectorDetail && <article><h4><bdi dir="auto">{connectorDetail.data.name}</bdi></h4><p>{t("State:")}{' '}{connectorDetail.data.state} {' '}{t("· Worker:")}{' '}{connectorDetail.data.workerId ?? t("Unknown")} {' '}{t("· Tasks:")}{' '}<bdi dir="ltr" lang="en">{connectorDetail.data.tasks.length}</bdi></p><table><thead><tr><th scope="col">{t("Configuration key")}</th><th scope="col">{t("Safe value")}</th></tr></thead><tbody>{Object.entries(connectorDetail.data.safeConfiguration).map(([key, value]) => <tr key={key}><th scope="row">{key}</th><td>{value ?? t("Not set")}</td></tr>)}</tbody></table>
        <section aria-labelledby="connect-auto-restart-title">
          <h5 id="connect-auto-restart-title">{t("Bounded auto-restart")}</h5>
          {autoRestartError && <p role="alert">{readViewProblemText(autoRestartError)}</p>}
          {autoRestartStatus && <>
            <p>{t("Deployment policy:")}{' '}{autoRestartStatus.deploymentEnabled ? t("enabled") : t("disabled")} {' '}{t("· Activation:")}{' '}{autoRestartStatus.active ? t("active") : t("inactive")} {' '}{t("· Circuit:")}{' '}{autoRestartStatus.circuitState ?? t("none")}</p>
            <p>{t("Attempts:")}{' '}{autoRestartStatus.attemptsUsed}/<bdi dir="ltr" lang="en">{autoRestartStatus.maxAttempts}</bdi> {' '}{t("· Next attempt:")}{' '}{autoRestartStatus.nextAttemptUtc ? autoRestartStatus.nextAttemptUtc : t("none")} {' '}{t("· Unresolved dispatch:")}{' '}{autoRestartStatus.hasUnresolvedDispatch ? t("yes") : t("no")}</p>
            {autoRestartStatus.terminalReason && <p>{t("Terminal reason:")}{' '}<bdi dir="auto">{autoRestartStatus.terminalReason}</bdi></p>}
            <button type="button" disabled={autoRestartBusy || !autoRestartStatus.deploymentEnabled || autoRestartStatus.active} onClick={() => void previewAutoRestart(true)}>{t("Preview enable")}</button>{' '}
            <button type="button" disabled={autoRestartBusy || !autoRestartStatus.active} onClick={() => void previewAutoRestart(false)}>{t("Preview disable")}</button>
          </>}
          {autoRestartOperation && <div>
            <p>{t("Governed operation:")}{' '}<code dir="ltr" lang="en">{autoRestartOperation.operationId}</code> {' '}{t("· Risk:")}{' '}{autoRestartOperation.riskClass} {' '}{t("· State:")}{' '}{autoRestartOperation.state}</p>
            <p>{t("Use the Governed mutations panel below for required confirmation/independent approval, then refresh this operation before applying.")}</p>
            <button type="button" disabled={autoRestartBusy} onClick={() => void refreshAutoRestartOperation()}>{t("Refresh operation")}</button>{' '}
            <button type="button" disabled={autoRestartBusy || autoRestartOperation.state !== 'ready'} onClick={() => void applyAutoRestart()}>{t("Apply policy")}</button>
          </div>}
        </section>
      </article>}
      {connectPlugins && <article aria-labelledby="connect-plugins-title">
        <h4 id="connect-plugins-title">{t("Connector plugins")}</h4>
        {connectPlugins.data.length === 0 ? <p>{t("No connector plugins were reported.")}</p> :
          <table><thead><tr><th scope="col">{t("Class")}</th><th scope="col">{t("Type")}</th><th scope="col">{t("Version")}</th><th scope="col">{t("Validate")}</th></tr></thead><tbody>{connectPlugins.data.map(plugin => <tr key={plugin.class}><th scope="row">{plugin.class}</th><td>{plugin.type}</td><td>{plugin.version ?? t("Unknown")}</td><td><button type="button" onClick={() => void selectPluginForValidation(plugin.class)}>{t("Use")}</button></td></tr>)}</tbody></table>}
      </article>}
      {selectedPluginClass && <article aria-labelledby="connect-plugin-validation-title">
        <h4 id="connect-plugin-validation-title">{t("Validate configuration")}</h4>
        <p>{t("Plugin:")}{' '}<code dir="ltr" lang="en">{selectedPluginClass}</code>{t(". Values are sent only to the selected configured Connect profile and are cleared from this form after validation.")}</p>
        {pluginValidation && pluginValidation.data.fields.some(field => field.name !== 'connector.class') && <fieldset>
          <legend>{t("Smart configuration form")}</legend>
          {pluginValidation.data.fields.filter(field => field.name !== 'connector.class').map(field => <div key={field.name}>
            <label htmlFor={`connect-plugin-field-${field.name}`}><bdi dir="auto">{field.name}</bdi>{field.required ? ' *' : ''}</label>{' '}
            {field.recommendedValues.length > 0
              ? <select
                  id={`connect-plugin-field-${field.name}`}
                  value={pluginFieldValues[field.name] ?? ''}
                  onChange={event => setPluginFieldValues(values => ({ ...values, [field.name]: event.target.value }))}
                >
                  <option value="">{t("Select…")}</option>
                  {field.recommendedValues.map(value => <option key={value} value={value}>{value}</option>)}
                </select>
              : <input dir="auto"
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
            {t("Validate smart form")}{' '}</button>
          <p>{t("Smart-form values remain in browser memory only and are cleared after each validation attempt.")}</p>
        </fieldset>}
        <details>
          <summary>{t("Raw key=value validation")}</summary>
        <label htmlFor="connect-plugin-configuration">{t("Configuration (one key=value per line)")}</label>
        <textarea dir="ltr"
          id="connect-plugin-configuration"
          value={pluginConfiguration}
          onChange={event => setPluginConfiguration(event.target.value)}
          autoComplete="off"
          rows={6}
        />
        <button type="button" onClick={() => void validatePlugin()} disabled={pluginConfiguration.trim().length === 0}>{t("Validate configuration")}</button>
        </details>
      </article>}
      {pluginValidation && <article aria-labelledby="connect-plugin-validation-result-title">
        <h4 id="connect-plugin-validation-result-title">{t("Validation diagnostics")}</h4>
        <p>{t("Error count:")}{' '}<bdi dir="ltr" lang="en">{pluginValidation.data.errorCount}</bdi></p>
        <table><thead><tr><th scope="col">{t("Field")}</th><th scope="col">{t("Type")}</th><th scope="col">{t("Required")}</th><th scope="col">{t("Errors")}</th><th scope="col">{t("Recommended")}</th></tr></thead><tbody>{pluginValidation.data.fields.map(field => <tr key={field.name}><th scope="row"><bdi dir="auto">{field.name}</bdi></th><td>{field.type}</td><td>{field.required ? t("Yes") : t("No")}</td><td>{field.errors.join('; ') || t("None")}</td><td>{field.recommendedValues.join(', ') || t("None")}</td></tr>)}</tbody></table>
      </article>}

      <h3 id="ksql-title">ksqlDB</h3>
      {ksqlError && <ReadViewProblemNotice problem={ksqlError} />}
      {ksqlInfo && <p>{t("Version:")}{' '}{ksqlInfo.data.version ?? t("Unknown")} {' '}{t("· Kafka cluster:")}{' '}{ksqlInfo.data.kafkaClusterId ?? t("Unknown")} {' '}{t("· Health:")}{' '}{ksqlInfo.data.state ?? t("Unknown")}</p>}
      <p>{t("v0.7 admits bounded single-statement SELECT queries only. DDL, DML, persistent-query creation, session substitution and generic provider forwarding are blocked before provider I/O.")}</p>
      <label htmlFor="ksql-query-editor">{t("Read-only SELECT")}</label>
      <textarea dir="auto"
        id="ksql-query-editor"
        rows={6}
        value={ksqlStatement}
        onChange={event => setKsqlStatement(event.target.value)}
        placeholder="SELECT * FROM ORDERS LIMIT 100;"
        autoComplete="off"
      />
      <button type="button" disabled={ksqlBusy || ksqlStatement.trim().length === 0} onClick={() => void runKsqlQuery()}>
        {ksqlBusy ? t("Running…") : t("Run bounded query")}
      </button>
      {ksqlResult && <article aria-labelledby="ksql-query-result-title">
        <h4 id="ksql-query-result-title">{t("Query result")}</h4>
        <p>{t("Rows:")}{' '}<bdi dir="ltr" lang="en">{ksqlResult.data.rows.length}</bdi> {' '}{t("· Bytes observed:")}{' '}<bdi dir="ltr" lang="en">{ksqlResult.data.responseBytes}</bdi> · {ksqlResult.data.truncated ? t("Limited: {0}", [ksqlResult.data.limitReason ?? t("server limit")]) : t("Complete within configured bounds")}</p>
        <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table><thead><tr>{ksqlResult.data.header.columnNames.map((name, index) => <th key={`${name}-${index}`} scope="col">{name}<br /><small>{ksqlResult.data.header.columnTypes[index] ?? t("Unknown")}</small></th>)}</tr></thead>
          <tbody>{ksqlResult.data.rows.map((row, rowIndex) => <tr key={rowIndex}>{row.columns.map((value, columnIndex) => <td key={columnIndex}><code dir="ltr" lang="en">{JSON.stringify(value)}</code></td>)}</tr>)}</tbody>
        </table></div>
      </article>}

      <h3 id="streams-title">{t("Kafka Streams evidence")}</h3>
      {streamsError && <ReadViewProblemNotice problem={streamsError} />}
      {!streamsApplications && !streamsError && <p role="status">{t("Loading registered Streams telemetry evidence…")}</p>}
      {streamsApplications && streamsApplications.data.length === 0 && <p>{t("No registered Streams applications were reported.")}</p>}
      {streamsApplications && streamsApplications.data.length > 0 && <table><thead><tr><th scope="col">{t("Application")}</th><th scope="col">{t("Evidence source")}</th><th scope="col">{t("Observed")}</th><th scope="col">{t("State")}</th></tr></thead><tbody>
        {streamsApplications.data.map(application => <tr key={application.applicationId}><th scope="row"><button type="button" onClick={() => void openStreamsApplication(application.applicationId)}><bdi dir="ltr" lang="en">{application.applicationId}</bdi></button></th><td>{application.evidenceSource}</td><td>{application.observedAtUtc}</td><td><StatusBadge kind={application.stale ? 'stale' : 'current'} label={application.stale ? t("Stale") : t("Observed")} /></td></tr>)}
      </tbody></table>}
      {streamsTopology && <article aria-labelledby="streams-topology-title">
        <h4 id="streams-topology-title">{t("Topology:")}{' '}<bdi dir="ltr" lang="en">{streamsTopology.data.applicationId}</bdi></h4>
        <p>{t("Source:")}{' '}{streamsTopology.data.evidenceSource} {' '}{t("· Observed")}{' '}{streamsTopology.data.observedAtUtc} · <StatusBadge kind={streamsTopology.data.stale ? 'stale' : 'current'} label={streamsTopology.data.stale ? t("Stale evidence") : t("Current evidence")} /></p>
        <table><thead><tr><th scope="col">{t("Node")}</th><th scope="col">{t("Type")}</th><th scope="col">{t("Input topics")}</th><th scope="col">{t("Output topics")}</th><th scope="col">{t("State stores")}</th></tr></thead><tbody>{streamsTopology.data.nodes.map(node => <tr key={node.id}><th scope="row"><bdi dir="auto">{node.name}</bdi></th><td>{node.type}</td><td>{node.inputTopics.join(', ') || t("None")}</td><td>{node.outputTopics.join(', ') || t("None")}</td><td>{node.stateStores.join(', ') || t("None")}</td></tr>)}</tbody></table>
      </article>}
      {streamsStores && <article aria-labelledby="streams-stores-title">
        <h4 id="streams-stores-title">{t("State stores:")}{' '}<bdi dir="ltr" lang="en">{streamsStores.data.applicationId}</bdi></h4>
        {streamsStores.data.stores.length === 0 ? <p>{t("No state-store metrics were exposed by the telemetry provider.")}</p> : <table><thead><tr><th scope="col">{t("Store")}</th><th scope="col">{t("Type")}</th><th scope="col">{t("Entries")}</th><th scope="col">{t("Size")}</th><th scope="col">{t("Health")}</th></tr></thead><tbody>{streamsStores.data.stores.map(store => <tr key={store.name}><th scope="row"><bdi dir="auto">{store.name}</bdi></th><td>{store.type}</td><td>{store.approximateEntries ?? t("Unknown")}</td><td>{store.sizeBytes ?? t("Unknown")}</td><td>{store.health ?? t("Unknown")}</td></tr>)}</tbody></table>}
      </article>}

      <h3 id="lineage-title">{t("Lineage")}</h3>
      {lineageError && <ReadViewProblemNotice problem={lineageError} />}
      {lineage && <>
        {lineage.data.partial && <p role="status"><StatusBadge kind="partial" /> {' '}{t("Lineage is partial:")}{' '}{lineage.data.limitations.map(item => item.message).join('; ')}</p>}
        {lineage.data.edges.length === 0 ? <p>{t("No lineage edges are available from registered evidence.")}</p> : <table><thead><tr><th scope="col">{t("Source")}</th><th scope="col">{t("Destination")}</th><th scope="col">{t("Evidence")}</th><th scope="col">{t("Provenance")}</th><th scope="col">{t("Confidence")}</th><th scope="col">{t("State")}</th></tr></thead><tbody>{lineage.data.edges.map((edge, index) => <tr key={`${edge.source.kind}:${edge.source.id}->${edge.destination.kind}:${edge.destination.id}:${index}`}><td>{edge.source.kind}:{edge.source.id}</td><td>{edge.destination.kind}:{edge.destination.id}</td><td><bdi dir="ltr" lang="en">{edge.evidenceKind}</bdi></td><td>{edge.provenance}</td><td>{edge.confidence.toFixed(2)}</td><td>{edge.stale ? t("Stale") : t("Current")}</td></tr>)}</tbody></table>}
      </>}
    </section>

    <DataQualityPanel clusterId={clusterId} />

    <MutationOperationsPanel clusterId={clusterId} connectProfileId={selectedConnectProfileId} />
  </>;
}
