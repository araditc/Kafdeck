import { KsqlQueryConsole } from './KsqlQueryConsole.js';
import { t } from '../../shared/i18n.js';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  ApiProblem,
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
  type ConsumerOffset,
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
  type SchemaDocument,
  type SchemaMockResult,
  type SchemaReferenceGraph,
  type SchemaSubjectSummary,
  type SchemaVersionSummary,
  type SchemaVersionDetail,
} from '../../shared/api.js';
import { SchemaVersionRegistrar } from './SchemaVersionRegistrar.js';
import { RegisteredServiceSettings } from './RegisteredServiceSettings.js';
import { MutationOperationsPanel } from '../mutations/MutationOperationsPanel.js';
import { DataQualityPanel } from './DataQualityPanel.js';
import { StatusBadge } from '../../app/StatusBadge.js';
import { shouldAutoRefresh, visibleRefreshIntervalMs } from '../../app/operatorState.js';
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

export function ReadViewsExplorer({
  clusterId, page = 'all', canManageConsumers = false, consumerRefresh = 0,
  canRegisterSchemas = false, canConfigureRegistry = false,
}: {
  clusterId: string;
  page?: 'all' | 'consumers' | 'schemas' | 'ecosystem';
  canManageConsumers?: boolean;
  consumerRefresh?: number;
  canRegisterSchemas?: boolean;
  canConfigureRegistry?: boolean;
}) {
  const consumers = page === 'all' || page === 'consumers';
  const schemas = page === 'all' || page === 'schemas';
  const ecosystem = page === 'all' || page === 'ecosystem';
  const [consumerGroups, setConsumerGroups] = useState<ReadViewEnvelope<ConsumerGroupSummary[]> | null>(null);
  const [consumerError, setConsumerError] = useState<ReadViewProblem | null>(null);
  const [activeGroup, setActiveGroup] = useState<string | null>(null);
  const [resetPartition, setResetPartition] = useState<ConsumerOffset | null>(null);
  const [showConsumerManagement, setShowConsumerManagement] = useState(false);
  const [detailProblems, setDetailProblems] = useState<Partial<Record<'members' | 'offsets' | 'diagnostics', ReadViewProblem>>>({});
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
  const activeGroupRef = useRef<string | null>(null);
  const consumerRequest = useRef<AbortController | null>(null);
  const consumerListRequest = useRef<AbortController | null>(null);
  const groupHeading = useRef<HTMLHeadingElement>(null);
  const openedGroupButton = useRef<string | null>(null);
  const consumerManagementHeading = useRef<HTMLHeadingElement>(null);
  const lastConsumerRefresh = useRef(consumerRefresh);
  const sloEvaluationGeneration = useRef(0);

  const [subjects, setSubjects] = useState<ReadViewEnvelope<SchemaSubjectSummary[]> | null>(null);
  const [schemaError, setSchemaError] = useState<ReadViewProblem | null>(null);
  const [schemaRegistryMissing, setSchemaRegistryMissing] = useState(false);
  const [registrySettingsOpen, setRegistrySettingsOpen] = useState(false);
  const [schemaCatalogRefresh, setSchemaCatalogRefresh] = useState(0);
  const registrySettingsButton = useRef<HTMLButtonElement>(null);
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null);
  const selectedSubjectRef = useRef<string | null>(null);
  const subjectRequest = useRef<AbortController | null>(null);
  const subjectHeading = useRef<HTMLHeadingElement>(null);
  const openedSubjectButton = useRef<string | null>(null);
  const [versions, setVersions] = useState<ReadViewEnvelope<SchemaVersionSummary[]> | null>(null);
  const [viewedVersion, setViewedVersion] = useState<number | null>(null);
  const [schemaContentRefresh, setSchemaContentRefresh] = useState(0);
  const [schemaContent, setSchemaContent] = useState<ReadViewEnvelope<SchemaVersionDetail> | null>(null);
  const [schemaContentError, setSchemaContentError] = useState<ReadViewProblem | null>(null);
  const [registrationSource, setRegistrationSource] = useState<SchemaDocument | null>(null);
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
    setActiveGroup(null); setResetPartition(null); setShowConsumerManagement(false); setDetailProblems({});
    setGroupDetail(null); setGroupLag(null); setGroupDiagnostics(null);
    setOperationalAnalytics(null); setOperationalTrend(null); setOperationalSlo(null); setOperationalError(null); setSloError(null); setOperationalBusy(false); setSloThreshold(100); setSloTarget(0.99);
    setSubjects(null); setSchemaError(null); setSchemaRegistryMissing(false); setSelectedSubject(null); setVersions(null); setViewedVersion(null);
    setRegistrationSource(null); setCompatibility(null); setLeftVersion(null); setRightVersion(null); setSchemaDiff(null);
    setReferenceGraph(null); setCompatibilityExplanation(null); setSchemaMock(null);
    setSerdeCapabilities(null); setSerdeFormat('cbor'); setSerdePayloadBase64(''); setSerdeStructuredJson(''); setSerdeDecoded(null); setSerdeEncoded(null); setSerdeError(null); setSerdeBusy(false);
    setConnectProfiles(null); setSelectedConnectProfileId(null); setConnectInfo(null); setConnectors(null); setConnectorDetail(null); setConnectPlugins(null); setSelectedPluginClass(null); setPluginConfiguration(''); setPluginFieldValues({}); setPluginValidation(null); setConnectError(null); setAutoRestartStatus(null); setAutoRestartOperation(null); setAutoRestartError(null); setAutoRestartBusy(false);
    setKsqlInfo(null); setKsqlError(null);
    setStreamsApplications(null); setStreamsTopology(null); setStreamsStores(null); setStreamsError(null);
    setLineage(null); setLineageError(null);

    if (schemas) void kafdeckApi.getControlledSerdeCapabilities(controller.signal)
      .then(setSerdeCapabilities)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setSerdeError(readViewError(reason)); });

    if (ecosystem) void kafdeckApi.listConnectProfiles(clusterId, controller.signal)
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

    if (ecosystem) void kafdeckApi.getKsqlInfo(clusterId, controller.signal)
      .then(setKsqlInfo)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setKsqlError(readViewError(reason)); });

    if (ecosystem) void kafdeckApi.listStreamsApplications(clusterId, controller.signal)
      .then(setStreamsApplications)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setStreamsError(readViewError(reason)); });

    if (ecosystem) void kafdeckApi.getLineage(clusterId, controller.signal)
      .then(setLineage)
      .catch(reason => { if (!(reason instanceof DOMException && reason.name === 'AbortError')) setLineageError(readViewError(reason)); });

    return () => {
      controller.abort();
      subjectRequest.current?.abort();
      selectedSubjectRef.current = null;
    };
  }, [clusterId, consumers, schemas, ecosystem]);

  useEffect(() => {
    if (!schemas) return;
    const controller = new AbortController();
    setSubjects(null);
    setSchemaRegistryMissing(false);
    setSchemaError(null);
    void kafdeckApi.listSchemaSubjects(clusterId, controller.signal)
      .then(result => { if (!controller.signal.aborted) setSubjects(result); })
      .catch(reason => {
        if (controller.signal.aborted) return;
        if (reason instanceof ApiProblem && reason.code === 'schema_registry_not_configured') setSchemaRegistryMissing(true);
        else setSchemaError(readViewError(reason));
      });
    return () => controller.abort();
  }, [clusterId, schemas, schemaCatalogRefresh]);

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

  const loadConsumerGroups = useCallback(async () => {
    if (consumerListRequest.current) return;
    const controller = new AbortController();
    consumerListRequest.current = controller;
    try {
      const groups = await kafdeckApi.listConsumerGroups(clusterId, controller.signal);
      if (controller.signal.aborted) return;
      setConsumerGroups(groups);
      setConsumerError(null);
    } catch (reason) {
      if (!controller.signal.aborted) setConsumerError(readViewError(reason));
    } finally {
      if (consumerListRequest.current === controller) consumerListRequest.current = null;
    }
  }, [clusterId]);

  useEffect(() => {
    if (!consumers) return;
    setConsumerGroups(null);
    setConsumerError(null);
    void loadConsumerGroups();
    return () => {
      activeGroupRef.current = null;
      consumerListRequest.current?.abort();
      consumerListRequest.current = null;
      consumerRequest.current?.abort();
      consumerRequest.current = null;
    };
  }, [consumers, loadConsumerGroups]);

  const openConsumer = useCallback(async (groupId: string, mode: 'open' | 'refresh' | 'background' = 'open') => {
    if (mode !== 'open' && activeGroupRef.current !== groupId) return;
    if (mode === 'background' && consumerRequest.current) return;
    consumerRequest.current?.abort();
    const controller = new AbortController();
    consumerRequest.current = controller;
    if (mode === 'open') ++consumerLoadGeneration.current;
    const generation = consumerLoadGeneration.current;
    const current = () => !controller.signal.aborted && generation === consumerLoadGeneration.current;
    if (mode === 'open') {
      activeGroupRef.current = groupId;
      setActiveGroup(groupId);
      setResetPartition(null); setShowConsumerManagement(false); setDetailProblems({});
      setGroupDetail(null); setGroupLag(null); setGroupDiagnostics(null);
      ++sloEvaluationGeneration.current;
      setOperationalAnalytics(null); setOperationalTrend(null); setOperationalSlo(null);
      setOperationalError(null); setSloError(null); setOperationalBusy(true);
    }

    function observe<T>(section: 'members' | 'offsets' | 'diagnostics', request: Promise<T>, update: (value: T) => void) {
      return request.then(value => {
        if (!current()) return;
        update(value);
        setDetailProblems(previous => {
          if (!previous[section]) return previous;
          const next = { ...previous };
          delete next[section];
          return next;
        });
      }).catch(reason => {
        if (current() && mode !== 'background')
          setDetailProblems(previous => ({ ...previous, [section]: readViewError(reason) }));
      });
    }

    const requests = [
      observe('members', kafdeckApi.getConsumerGroup(clusterId, groupId, controller.signal), setGroupDetail),
      observe('offsets', kafdeckApi.getConsumerLag(clusterId, groupId, controller.signal), setGroupLag),
    ];
    if (mode !== 'background')
      requests.push(observe('diagnostics', kafdeckApi.getConsumerDiagnostics(clusterId, groupId, controller.signal), setGroupDiagnostics));
    if (mode !== 'background') {
      setOperationalBusy(true);
      requests.push(Promise.all([
        kafdeckApi.getConsumerOperationalAnalytics(clusterId, groupId, controller.signal),
        kafdeckApi.getConsumerOperationalTrend(clusterId, groupId, undefined, controller.signal),
      ]).then(([live, trend]) => {
        if (!current()) return;
        setOperationalAnalytics(live);
        setOperationalTrend(trend);
        setOperationalError(null);
      }).catch(reason => {
        if (current()) setOperationalError(readViewError(reason));
      }).finally(() => {
        if (current()) setOperationalBusy(false);
      }));
    }
    await Promise.all(requests);
    if (consumerRequest.current === controller) consumerRequest.current = null;
  }, [clusterId]);

  useEffect(() => {
    if (lastConsumerRefresh.current === consumerRefresh) return;
    lastConsumerRefresh.current = consumerRefresh;
    if (!consumers) return;
    if (activeGroup) void openConsumer(activeGroup, 'refresh');
    else void loadConsumerGroups();
  }, [activeGroup, consumerRefresh, consumers, loadConsumerGroups, openConsumer]);

  useEffect(() => {
    if (!activeGroup) return;
    window.scrollTo(0, 0);
    groupHeading.current?.focus({ preventScroll: true });
    const timer = window.setInterval(() => {
      if (shouldAutoRefresh(document.visibilityState))
        void openConsumer(activeGroup, 'background');
    }, visibleRefreshIntervalMs);
    return () => window.clearInterval(timer);
  }, [activeGroup, openConsumer]);

  useEffect(() => {
    if (showConsumerManagement) consumerManagementHeading.current?.focus();
  }, [showConsumerManagement, resetPartition]);

  const closeConsumer = () => {
    activeGroupRef.current = null;
    consumerRequest.current?.abort();
    consumerRequest.current = null;
    ++consumerLoadGeneration.current;
    ++sloEvaluationGeneration.current;
    setActiveGroup(null);
    setShowConsumerManagement(false);
    setResetPartition(null);
    window.requestAnimationFrame(() => {
      const previous = openedGroupButton.current;
      if (previous) document.getElementById(previous)?.focus();
    });
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

  const openSubject = async (subject: string, refresh = false, preferredVersion: number | null = null) => {
    if (refresh && selectedSubjectRef.current !== subject) return;
    subjectRequest.current?.abort();
    const controller = new AbortController();
    const { signal } = controller;
    subjectRequest.current = controller;
    setSchemaError(null);
    if (!refresh) {
      selectedSubjectRef.current = subject;
      setSelectedSubject(subject);
      setVersions(null);
      setViewedVersion(null);
      setRegistrationSource(null);
      setCompatibility(null);
      setCompatibilityExplanation(null);
      setSchemaDiff(null);
      setReferenceGraph(null);
      setSchemaMock(null);
    }
    await Promise.allSettled([
      kafdeckApi.listSchemaVersions(clusterId, subject, signal).then(result => {
        if (signal.aborted) return;
        const sorted = [...result.data].sort((left, right) => right.version - left.version);
        const latest = sorted[0]?.version ?? null;
        setVersions({ ...result, data: sorted });
        setViewedVersion(preferredVersion !== null && sorted.some(item => item.version === preferredVersion) ? preferredVersion : latest);
        if (refresh) setSchemaContentRefresh(value => value + 1);
        setRightVersion(latest);
        setLeftVersion(sorted[1]?.version ?? latest);
      }).catch(reason => { if (!signal.aborted) setSchemaError(readViewError(reason)); }),
      kafdeckApi.getSchemaCompatibility(clusterId, subject, signal).then(result => {
        if (!signal.aborted) setCompatibility(result);
      }),
      kafdeckApi.getSchemaCompatibilityExplanation(clusterId, subject, signal).then(result => {
        if (!signal.aborted) setCompatibilityExplanation(result);
      }),
    ]);
  };

  useEffect(() => {
    if (!selectedSubject) return;
    window.scrollTo(0, 0);
    subjectHeading.current?.focus({ preventScroll: true });
  }, [selectedSubject]);

  useEffect(() => {
    setSchemaContent(null);
    setSchemaContentError(null);
    if (!selectedSubject || viewedVersion === null) return;
    const controller = new AbortController();
    void kafdeckApi.getSchemaVersion(clusterId, selectedSubject, viewedVersion, controller.signal)
      .then(result => { if (!controller.signal.aborted) setSchemaContent(result); })
      .catch(reason => { if (!controller.signal.aborted) setSchemaContentError(readViewError(reason)); });
    return () => controller.abort();
  }, [clusterId, selectedSubject, viewedVersion, schemaContentRefresh]);

  const closeSubject = () => {
    subjectRequest.current?.abort();
    selectedSubjectRef.current = null;
    setSelectedSubject(null);
    setRegistrationSource(null);
    setSchemaError(null);
    window.requestAnimationFrame(() => {
      if (openedSubjectButton.current) document.getElementById(openedSubjectButton.current)?.focus();
    });
  };

  const compareSchemas = async () => {
    if (!selectedSubject || leftVersion === null || rightVersion === null) return;
    setSchemaError(null); setSchemaDiff(null);
    const signal = subjectRequest.current?.signal;
    try {
      const result = await kafdeckApi.diffSchemaVersions(clusterId, selectedSubject, leftVersion, rightVersion, signal);
      if (!signal?.aborted) setSchemaDiff(result);
    } catch (reason) {
      if (!signal?.aborted) setSchemaError(readViewError(reason));
    }
  };

  const inspectSchemaReferences = async () => {
    if (!selectedSubject || rightVersion === null) return;
    setSchemaError(null); setReferenceGraph(null);
    const signal = subjectRequest.current?.signal;
    try {
      const result = await kafdeckApi.getSchemaReferenceGraph(clusterId, selectedSubject, rightVersion, signal);
      if (!signal?.aborted) setReferenceGraph(result);
    } catch (reason) {
      if (!signal?.aborted) setSchemaError(readViewError(reason));
    }
  };

  const generateSchemaMock = async () => {
    if (!selectedSubject || rightVersion === null) return;
    setSchemaError(null); setSchemaMock(null);
    const signal = subjectRequest.current?.signal;
    try {
      const count = Math.max(1, Math.min(10, Math.trunc(mockCount)));
      const seed = Math.trunc(mockSeed);
      const result = await kafdeckApi.generateSchemaMock(clusterId, selectedSubject, rightVersion, count, seed, signal);
      if (!signal?.aborted) setSchemaMock(result);
    } catch (reason) {
      if (!signal?.aborted) setSchemaError(readViewError(reason));
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

  const displayedSchema = schemaContent && schemaContent.data.subject === selectedSubject && schemaContent.data.version === viewedVersion
    ? schemaContent : null;

  return <>
    {consumers && <section className="card kafdeck-card" id="consumers" aria-labelledby="consumers-title">
      <h2 id="consumers-title">{t("Consumer groups")}</h2>
      {!activeGroup && <>
      {consumerError && <ReadViewProblemNotice problem={consumerError} />}
      {!consumerGroups && !consumerError && <p role="status">{t("Loading authorized consumer groups…")}</p>}
      {consumerGroups && <>
        <Limitations envelope={consumerGroups} />
        {consumerGroups.data.length === 0 ? <p>{t("No authorized consumer groups are observable.")}</p> :
          <div className="table-responsive"><table className="table table-vcenter"><thead><tr><th scope="col">{t("Group")}</th><th scope="col">{t("State")}</th><th scope="col">{t("Members")}</th></tr></thead>
            <tbody>{consumerGroups.data.map(group => <tr key={group.groupId}>
              <th scope="row"><button id={`consumer-open-${group.groupId}`} className="btn btn-link kafdeck-table-button" type="button"
                onClick={event => {
                  openedGroupButton.current = event.currentTarget.id;
                  void openConsumer(group.groupId);
                }}><bdi dir="ltr" lang="en">{group.groupId}</bdi></button></th>
              <td>{group.state}</td><td>{group.memberCount ?? t("Detail required")}</td>
            </tr>)}</tbody></table></div>}
      </>}
      </>}
      {activeGroup && <article aria-labelledby="consumer-detail-title">
        <div className="d-flex flex-wrap gap-2 mb-3">
          <button type="button" className="btn btn-sm btn-outline-primary" onClick={closeConsumer}>{t("Back to consumer groups")}</button>
          {canManageConsumers && <button type="button" className="btn btn-sm btn-outline-primary" onClick={() => {
            setResetPartition(null);
            setShowConsumerManagement(true);
          }}>{t("Manage consumer group")}</button>}
        </div>
        <h3 id="consumer-detail-title" ref={groupHeading} tabIndex={-1}><bdi dir="ltr" lang="en">{activeGroup}</bdi></h3>
        {groupDetail && <p>{t("Kafka state:")}{' '}{groupDetail.data.state}</p>}

        <section aria-labelledby="consumer-offsets-title" className="mb-4">
          <h4 id="consumer-offsets-title">{t("Offsets and lag")}</h4>
          {detailProblems.offsets && <ReadViewProblemNotice problem={detailProblems.offsets} />}
          {!groupLag && !detailProblems.offsets && <p role="status">{t("Loading consumer offsets…")}</p>}
          {groupLag && <>
            <p>{t("Total lag")}{' '}<bdi>{groupLag.data.totalLag ?? t("Unknown")}</bdi>{groupLag.data.isPartial && <> · <StatusBadge kind="partial" /></>}</p>
            <Limitations envelope={groupLag} />
            {groupLag.data.partitions.length === 0 ? <p>{t("No assigned partitions or committed offsets were found.")}</p> :
              <div className="table-responsive"><table className="table table-vcenter">
                <thead><tr><th scope="col">{t("Topic")}</th><th scope="col">{t("Partition")}</th><th scope="col">{t("Committed")}</th><th scope="col">{t("End")}</th><th scope="col">{t("Lag")}</th><th scope="col">{t("State")}</th>{canManageConsumers && <th scope="col">{t("Action")}</th>}</tr></thead>
                <tbody>{groupLag.data.partitions.map(item => <tr key={`${item.topic}:${item.partition}`}>
                  <th scope="row"><bdi dir="ltr">{item.topic}</bdi></th>
                  <td>{item.partition}</td><td>{item.committedOffset ?? t("Unknown")}</td>
                  <td>{item.endOffset ?? t("Unknown")}</td><td>{item.lag ?? t("Unknown")}</td><td>{item.state}</td>
                  {canManageConsumers && <td><button className="btn btn-sm btn-outline-primary" type="button"
                    aria-label={t("Reset offset for {0}, partition {1}", [item.topic, item.partition])}
                    onClick={() => { setResetPartition(item); setShowConsumerManagement(true); }}>{t("Reset offset")}</button></td>}
                </tr>)}</tbody>
              </table></div>}
          </>}
        </section>

        {showConsumerManagement && canManageConsumers && <section className="mb-4" aria-labelledby="consumer-management-title">
          <div className="d-flex flex-wrap gap-2 align-items-center mb-2">
            <h4 className="mb-0" id="consumer-management-title" ref={consumerManagementHeading} tabIndex={-1}>
              {resetPartition ? t("Reset offset") : t("Manage consumer group")}
            </h4>
            <button className="btn btn-sm btn-outline-secondary" type="button" onClick={() => {
              setShowConsumerManagement(false);
              groupHeading.current?.focus();
            }}>{t("Close")}</button>
          </div>
          <p>{t("Stop the group’s consumers before applying an offset reset. Kafka offsets change only after the preview is confirmed and executed.")}</p>
          <MutationOperationsPanel key={JSON.stringify([activeGroup, resetPartition?.topic, resetPartition?.partition])}
            clusterId={clusterId} family="consumer"
            consumerTarget={resetPartition ? { groupId: activeGroup, partition: {
              topicName: resetPartition.topic, partition: resetPartition.partition,
              committedOffset: groupLag?.data.partitions.find(item => item.topic === resetPartition.topic && item.partition === resetPartition.partition)?.committedOffset ?? null,
            } } : { groupId: activeGroup }}
            onApplied={() => void openConsumer(activeGroup, 'refresh')} />
        </section>}

        <section aria-labelledby="consumer-members-title" className="mb-4">
          <h4 id="consumer-members-title">{t("Members")}</h4>
          {detailProblems.members && <ReadViewProblemNotice problem={detailProblems.members} />}
          {!groupDetail && !detailProblems.members && <p role="status">{t("Loading consumer members…")}</p>}
          {groupDetail && <>
            <Limitations envelope={groupDetail} />
            {groupDetail.data.members.length === 0 ? <p>{t("This consumer group has no active members.")}</p> :
              <div className="table-responsive"><table className="table table-vcenter">
                <thead><tr><th scope="col">{t("Member ID")}</th><th scope="col">{t("Client ID")}</th><th scope="col">{t("Client host")}</th><th scope="col">{t("Group instance ID")}</th><th scope="col">{t("Assigned partitions")}</th></tr></thead>
                <tbody>{groupDetail.data.members.map(member => <tr key={member.memberId}>
                  <th scope="row"><bdi dir="ltr">{member.memberId}</bdi></th>
                  <td><bdi dir="ltr">{member.clientId ?? t("Unknown")}</bdi></td>
                  <td><bdi dir="ltr">{member.clientHost ?? t("Unknown")}</bdi></td>
                  <td><bdi dir="ltr">{member.groupInstanceId ?? t("None")}</bdi></td>
                  <td>{member.assignments.length === 0 ? t("None") : <ul className="mb-0">{member.assignments.map(assignment =>
                    <li key={`${assignment.topic}:${assignment.partition}`}><bdi dir="ltr">{assignment.topic} [{assignment.partition}]</bdi></li>)}</ul>}</td>
                </tr>)}</tbody>
              </table></div>}
          </>}
        </section>

        {detailProblems.diagnostics && <ReadViewProblemNotice problem={detailProblems.diagnostics} />}
        {groupDiagnostics && <>
          <p>{t("· Diagnostic:")}{' '}{groupDiagnostics.data.state} {' '}{t("· Metrics:")}{' '}{groupDiagnostics.data.metricsAvailable ? t("available") : t("unavailable")} {' '}{t("· History:")}{' '}{groupDiagnostics.data.historyAvailable ? t("available") : t("unavailable")}</p>
          <Limitations envelope={groupDiagnostics} />
          {groupDiagnostics.data.evidence.length > 0 && <><h4>{t("Evidence")}</h4><ul>{groupDiagnostics.data.evidence.map(item => <li key={item.code}>{item.safeMessage}</li>)}</ul></>}
        </>}

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
    </section>}

    {schemas && <><section className="card kafdeck-card kafdeck-ecosystem-card" id="schemas" aria-labelledby="schemas-title">
      <div className="card-body kafdeck-ecosystem-body">
      <header className="kafdeck-ecosystem-header">
        <h2 id="schemas-title">Schema Registry</h2>
        {canConfigureRegistry && !selectedSubject && !registrySettingsOpen &&
          <button ref={registrySettingsButton} type="button" className="btn btn-outline-primary"
            onClick={() => setRegistrySettingsOpen(true)}>{t("Configure Schema Registry")}</button>}
      </header>
      {canConfigureRegistry && registrySettingsOpen && !selectedSubject && <RegisteredServiceSettings
        clusterId={clusterId} service="schema-registry" onSaved={() => setSchemaCatalogRefresh(value => value + 1)}
        onClose={() => {
          setRegistrySettingsOpen(false);
          window.requestAnimationFrame(() => registrySettingsButton.current?.focus());
        }} />}
      {schemaError && <ReadViewProblemNotice problem={schemaError} />}
      {schemaRegistryMissing && <p role="status">{t("This cluster has no Schema Registry configured.")}</p>}
      {!subjects && !schemaError && !schemaRegistryMissing && <p role="status">{t("Loading authorized schema subjects…")}</p>}
      {!selectedSubject && subjects && <>
        <Limitations envelope={subjects} />
        {subjects.data.length === 0 ? <p>{t("No authorized schema subjects are observable.")}</p> :
          <ul className="list-group list-group-flush">{subjects.data.map(item =>
            <li key={item.subject} className="list-group-item">
              <button id={`schema-subject-${encodeURIComponent(item.subject)}`} className="btn btn-link kafdeck-table-button" type="button"
                onClick={event => {
                  openedSubjectButton.current = event.currentTarget.id;
                  void openSubject(item.subject);
                }}><bdi dir="auto">{item.subject}</bdi></button>
            </li>)}</ul>}
      </>}
      {selectedSubject && <article aria-labelledby="schema-detail-title">
        <div className="d-flex flex-wrap gap-2 mb-3">
          <button type="button" className="btn btn-sm btn-outline-primary" onClick={closeSubject}>{t("Back to subjects")}</button>
          <button type="button" className="btn btn-sm btn-outline-secondary"
            onClick={() => void openSubject(selectedSubject, true, viewedVersion)}>{t("Refresh versions")}</button>
        </div>
        <h3 id="schema-detail-title" ref={subjectHeading} tabIndex={-1}><bdi dir="auto">{selectedSubject}</bdi></h3>
        <p>{t("Compatibility:")}{' '}{compatibility?.data.mode ?? t("Unknown")}{compatibility?.data.isInherited ? ' (inherited)' : ''}</p>
        {!versions && !schemaError && <p role="status">{t("Loading schema versions…")}</p>}
        {versions && <>
          <Limitations envelope={versions} />
          {versions.data.length === 0 ? <p>{t("No schema versions are available.")}</p> :
            <div className="table-responsive"><table className="table table-vcenter">
              <thead><tr><th scope="col">{t("Version")}</th><th scope="col">{t("Schema ID")}</th><th scope="col">{t("Format")}</th><th scope="col">{t("References")}</th></tr></thead>
              <tbody>{versions.data.map(item => <tr key={item.version}>
                <th scope="row"><button className="btn btn-sm btn-outline-primary" type="button"
                  aria-pressed={viewedVersion === item.version} onClick={() => setViewedVersion(item.version)}>
                  {t("View version {0}", [item.version])}
                </button></th>
                <td>{item.schemaId}</td><td>{item.format}</td><td>{item.references.length}</td>
              </tr>)}</tbody>
            </table></div>}
        </>}
        {viewedVersion !== null && <section aria-labelledby="schema-content-title" className="mb-4">
          <h4 id="schema-content-title">{t("Schema source for version {0}", [viewedVersion])}</h4>
          {schemaContentError && <ReadViewProblemNotice problem={schemaContentError} />}
          {!displayedSchema && !schemaContentError && <p role="status">{t("Loading schema source…")}</p>}
          {displayedSchema && <>
            <Limitations envelope={displayedSchema} />
            <pre tabIndex={0} className="border rounded p-3" dir="ltr"><code>{displayedSchema.data.schema.schemaText}</code></pre>
            {displayedSchema.data.schema.references.length > 0 && <>
              <h5>{t("References")}</h5>
              <ul>{displayedSchema.data.schema.references.map(reference =>
                <li key={reference.name}><bdi dir="auto">{reference.name} → {reference.subject} v{reference.version}</bdi></li>)}</ul>
            </>}
            {canRegisterSchemas && !registrationSource && <button className="btn btn-primary" type="button"
              onClick={() => setRegistrationSource(displayedSchema.data.schema)}>{t("New schema version")}</button>}
          </>}
        </section>}
        {canRegisterSchemas && registrationSource && <SchemaVersionRegistrar
          clusterId={clusterId} subject={selectedSubject} source={registrationSource}
          onClose={() => setRegistrationSource(null)}
          onRegistered={version => void openSubject(selectedSubject, true, version)} />}
        {compatibilityExplanation && <aside aria-labelledby="schema-compatibility-explanation-title">
          <h4 id="schema-compatibility-explanation-title">{t("Compatibility explanation")}</h4>
          <p>{compatibilityExplanation.data.summary}</p>
          <p>{t("Scope:")}{' '}{compatibilityExplanation.data.scope}</p>
          <ul>{compatibilityExplanation.data.rules.map(rule => <li key={rule}>{rule}</li>)}</ul>
        </aside>}
        {versions && versions.data.length > 0 && <div>
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
      </div>
    </section>

    {!selectedSubject && <section className="card kafdeck-card" id="serde" aria-labelledby="serde-title">
      <div className="card-body kafdeck-ecosystem-body">
      <header className="kafdeck-ecosystem-header"><div>
        <h2 id="serde-title">{t("Controlled SerDe tooling")}</h2>
        <p>{t("Local bounded tooling for CBOR, XML and MessagePack. No provider proxy, runtime plugin loading, file/network resolution or durable payload storage is used.")}</p>
      </div></header>
      {serdeError && <ReadViewProblemNotice problem={serdeError} />}
      {!serdeCapabilities && !serdeError && <p role="status">{t("Loading controlled SerDe capabilities…")}</p>}
      {serdeCapabilities && <>
        <p>{t("Server bounds: input")}{' '}<bdi dir="ltr" lang="en">{serdeCapabilities.limits.maxInputBytes}</bdi> {' '}{t("bytes · output")}{' '}<bdi dir="ltr" lang="en">{serdeCapabilities.limits.maxOutputBytes}</bdi> {' '}{t("bytes · depth")}{' '}{serdeCapabilities.limits.maxDepth} {' '}{t("· nodes")}{' '}{serdeCapabilities.limits.maxNodes}.</p>
        <div className="table-responsive kafdeck-serde-capabilities" tabIndex={0} role="region" aria-label={t("Scrollable data table")}>
        <table className="table table-vcenter mb-0"><thead><tr><th scope="col">{t("Format")}</th><th scope="col">{t("Decode")}</th><th scope="col">{t("Encode")}</th><th scope="col">{t("Limitations")}</th></tr></thead><tbody>
          {serdeCapabilities.formats.map(capability => <tr key={capability.format}>
            <th scope="row">{capability.format}</th>
            <td>{capability.decodeSupported ? t("Supported") : t("Unsupported")}</td>
            <td>{capability.encodeSupported ? t("Supported") : t("Unsupported")}</td>
            <td>{capability.limitations.join('; ')}</td>
          </tr>)}
        </tbody></table>
        </div>
        <div className="kafdeck-serde-format">
        <label className="form-label" htmlFor="serde-format">{t("Format")}</label>
        <select className="form-select" id="serde-format" value={serdeFormat} onChange={event => {
          setSerdeFormat(event.target.value as ControlledSerdeFormat);
          setSerdeDecoded(null); setSerdeEncoded(null); setSerdeError(null);
          setSerdePayloadBase64(''); setSerdeStructuredJson('');
        }}>
          {serdeCapabilities.formats.map(capability => <option key={capability.format} value={capability.format}>{capability.format}</option>)}
        </select>
        </div>

        <div className="kafdeck-serde-editors">
          <form className="kafdeck-serde-editor" aria-labelledby="serde-decode-title" onSubmit={event => { event.preventDefault(); void decodeSerde(); }}>
            <h3 id="serde-decode-title">{t("Decode")}</h3>
            <div>
              <label className="form-label" htmlFor="serde-payload-base64">{t("Base64 payload")}</label>
              <textarea className="form-control" dir="ltr" id="serde-payload-base64" rows={6} value={serdePayloadBase64} onChange={event => setSerdePayloadBase64(event.target.value)} autoComplete="off" spellCheck={false} />
            </div>
            <button className="btn btn-outline-primary" type="submit" disabled={serdeBusy || serdePayloadBase64.trim().length === 0}>{t("Decode locally")}</button>
          </form>
          <form className="kafdeck-serde-editor" aria-labelledby="serde-encode-title" onSubmit={event => { event.preventDefault(); void encodeSerde(); }}>
            <h3 id="serde-encode-title">{t("Encode")}</h3>
            <div>
              <label className="form-label" htmlFor="serde-structured-json">{t("Structured JSON")}</label>
              <textarea className="form-control" dir="ltr" id="serde-structured-json" rows={6} value={serdeStructuredJson} onChange={event => setSerdeStructuredJson(event.target.value)} autoComplete="off" spellCheck={false} />
            </div>
            <button className="btn btn-outline-primary" type="submit" disabled={serdeBusy || serdeStructuredJson.trim().length === 0}>{t("Encode locally")}</button>
          </form>
        </div>
      </>}
      {serdeDecoded && <article aria-labelledby="serde-decoded-title"><h3 id="serde-decoded-title">{t("Decoded value")}</h3><pre tabIndex={0} dir="ltr" lang="en"><code dir="ltr" lang="en">{JSON.stringify(serdeDecoded.structuredValue, null, 2)}</code></pre></article>}
      {serdeEncoded && <article aria-labelledby="serde-encoded-title"><h3 id="serde-encoded-title">{t("Encoded payload")}</h3><p><bdi dir="ltr" lang="en">{serdeEncoded.byteCount}</bdi> {' '}{t("bytes")}</p><pre tabIndex={0} dir="ltr" lang="en"><code dir="ltr" lang="en">{serdeEncoded.payloadBase64}</code></pre></article>}
      </div>
    </section>}</>}

    {ecosystem && <section className="card kafdeck-card" id="ecosystem" aria-labelledby="ecosystem-title">
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
      <KsqlQueryConsole clusterId={clusterId} />

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
    </section>}

    {ecosystem && <DataQualityPanel clusterId={clusterId} />}

    {ecosystem && <MutationOperationsPanel clusterId={clusterId} connectProfileId={selectedConnectProfileId}
      {...(page === 'ecosystem' ? { family: 'connect' as const } : {})} />}
    {page === 'schemas' && !selectedSubject && subjects && canRegisterSchemas &&
      <MutationOperationsPanel clusterId={clusterId} family="schema" />}
  </>;
}
