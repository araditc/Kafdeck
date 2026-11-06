import { ApplicationCopyError, ownedErrorPresentation, problemPresentation } from '../../shared/presentationProblem.js';
import { t } from '../../shared/i18n.js';
import { FormEvent, useCallback, useEffect, useState } from 'react';
import {
  MutationApiProblem,
  mutationApi,
  type DataGeneratorStatus,
  type DataJobStatus,
  type MutationStatus,
  type DataReconciliationPreviewInput,
  validateDataReconciliationPreview,
} from './mutationApi.js';
import { mutationExecutionSurface } from './mutationExecutionSurface.js';
import { MutationPreviewWorkflows } from './MutationPreviewWorkflows.js';
import { StatusBadge } from '../../app/StatusBadge.js';
import { mutationStateStatusKind } from '../../app/statusPresentation.js';

const preDispatchStates = new Set([
  'previewed',
  'awaitingConfirmation',
  'awaitingApproval',
  'ready',
]);

function describeProblem(reason: unknown): string {
  const owned = ownedErrorPresentation(reason);
  if (owned && !(reason instanceof MutationApiProblem)) return problemPresentation(owned);
  if (reason instanceof MutationApiProblem) {
    if (reason.status === 401) return t("Operator authentication is required.");
    if (reason.status === 403) return t("The current operator is not authorized for this mutation action.");
    if (reason.status === 409) return t("{0} Refresh the operation before taking another pre-dispatch action.", [owned ? problemPresentation(owned) : reason.message]);
    return owned ? problemPresentation(owned) : reason.message;
  }

  return reason instanceof Error
    ? reason.message
    : t("The governed mutation operation could not be loaded.");
}

function stateMessage(operation: MutationStatus): string | null {
  switch (operation.state) {
    case 'executionUnknown':
      return t("Execution outcome is unknown. Do not retry blindly. Reconcile provider evidence and observed state before any follow-up action.");
    case 'partiallyApplied':
      return t("The provider reported partial application. Treat the operation as destructive state that requires reconciliation, not automatic retry.");
    case 'appliedUnverified':
      return t("The provider accepted the mutation but Kafdeck could not conclusively verify the final state.");
    case 'stalePreview':
      return t("The frozen preview no longer matches current provider state. Create a new preview before continuing.");
    case 'failedBeforeDispatch':
      return t("The operation failed before external dispatch; no provider application is claimed.");
    case 'failedDefinitive':
      return t("The provider definitively rejected the dispatched operation.");
    default:
      return null;
  }
}

function MutationSummary({ operation }: { operation: MutationStatus }) {
  const warning = stateMessage(operation);
  const evidence = Object.entries(operation.safeProviderEvidence);

  return <article aria-labelledby={`mutation-${operation.operationId}`}>
    <h3 id={`mutation-${operation.operationId}`}><bdi dir="ltr" lang="en">{operation.operationKind}</bdi></h3>
    <dl>
      <dt>{t("Operation ID")}</dt><dd><code dir="ltr" lang="en">{operation.operationId}</code></dd>
      <dt>{t("Cluster")}</dt><dd><bdi dir="ltr" lang="en">{operation.clusterId}</bdi></dd>
      <dt>{t("Risk")}</dt><dd>{operation.riskClass}</dd>
      <dt>{t("State")}</dt><dd><StatusBadge kind={mutationStateStatusKind(operation.state)} label={operation.state} /></dd>
      <dt>{t("Execution material")}</dt><dd>{operation.requiresExecutionMaterial ? t("Re-submission required") : t("No re-submission required")}</dd>
      <dt>{t("Preview expires")}</dt><dd>{operation.previewExpiresAtUtc}</dd>
      <dt>{t("Independent approval")}</dt><dd>{operation.requiresIndependentApproval ? t("Required") : t("Not required")}</dd>
      {operation.resultCode && <><dt>{t("Result code")}</dt><dd><bdi dir="ltr" lang="en">{operation.resultCode}</bdi></dd></>}
    </dl>
    {operation.riskClass === 'critical' && <p role="alert"><strong>{t("Critical mutation.")}</strong> {' '}{t("Independent approval by a distinct eligible operator is mandatory.")}</p>}
    {operation.operationKind === 'fleetUncertaintyDisposition' && (operation.reconciliationReview ? <section aria-label={t("Server-bound reconciliation review")}>
      <h4>{t("Immutable reconciliation observation")}</h4>
      <dl><dt>{t("Parent operation")}</dt><dd><bdi dir="ltr" lang="en">{operation.reconciliationReview.parentOperationId}</bdi></dd><dt>{t("Batch")}</dt><dd><bdi dir="ltr" lang="en">{operation.reconciliationReview.batchId}</bdi></dd>
        <dt>{t("Progress version")}</dt><dd><bdi dir="ltr" lang="en">{operation.reconciliationReview.expectedProgressVersion}</bdi></dd><dt>{t("Fenced worker generation")}</dt><dd><bdi dir="ltr" lang="en">{operation.reconciliationReview.workerGeneration}</bdi></dd>
        <dt>{t("Proof SHA256")}</dt><dd>{operation.reconciliationReview.proofReferenceSha256}</dd><dt>{t("Worker fence SHA256")}</dt><dd>{operation.reconciliationReview.fencedWorkerEvidenceSha256}</dd></dl>
      <table><caption>{t("Provider evidence bound by the server")}</caption><thead><tr><th scope="col">{t("Observation")}</th><th scope="col">{t("Physical Kafka identity")}</th><th scope="col">{t("Topic identity and partition")}</th><th scope="col">{t("Low / high watermarks")}</th><th scope="col">{t("Observed UTC")}</th></tr></thead>
        <tbody>{[operation.reconciliationReview.beforeDispatch,operation.reconciliationReview.reviewedObservation].map((item,index)=><tr key={index}>
          <th>{index===0?t("Before dispatch"):t("Reviewed")}</th><td><bdi dir="ltr" lang="en">{item.clusterId}</bdi> / <bdi dir="ltr" lang="en">{item.kafkaClusterId}</bdi></td><td><bdi dir="auto">{item.topicName}</bdi> / <bdi dir="ltr" lang="en">{item.topicId}</bdi> [{item.partition}]</td>
          <td>{item.lowWatermark} / {item.highWatermark}</td><td><bdi dir="ltr" lang="en">{item.observedAtUtc}</bdi></td></tr>)}</tbody></table>
      <p>{t("Confirm and obtain independent approval for this exact immutable preview. Successful reconciliation only fences the unresolved batch; it does not claim the background job has completed.")}</p>
    </section> : <p role="alert">{t("Reconciliation review is unavailable. Reload the complete server-bound preview before execution.")}</p>)}
    {warning && <p role="alert"><StatusBadge kind={mutationStateStatusKind(operation.state)} />{' '}<strong>{t("Outcome guidance:")}</strong> {warning}</p>}
    {evidence.length > 0 && <details><summary>{t("Safe provider evidence")}</summary><dl>{evidence.map(([key, value]) => <span key={key}><dt>{key}</dt><dd>{value}</dd></span>)}</dl></details>}
  </article>;
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

function pairMap(source: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const rawLine of source.split('\n')) {
    const line = rawLine.trim();
    if (!line) continue;
    const separator = line.indexOf('=');
    if (separator <= 0) throw new ApplicationCopyError("Each execution-material line must use key=value.");
    const key = line.slice(0, separator).trim();
    const value = line.slice(separator + 1);
    if (Object.prototype.hasOwnProperty.call(result, key)) throw new ApplicationCopyError("Duplicate key: {0}", [key]);
    result[key] = value;
  }
  return result;
}

type MutationOperationsPanelProps = {
  clusterId: string;
  connectProfileId?: string | null;
  enabled?: boolean;
};

type ReconciliationReview = DataReconciliationPreviewInput & {
  parentOperationId: string;
  generator: boolean;
  idempotencyKey: string;
};

export function MutationOperationsPanel({ clusterId, connectProfileId, enabled }: MutationOperationsPanelProps) {
  const [mutationAvailable, setMutationAvailable] = useState<boolean | null>(enabled ?? null);
  const [approvals, setApprovals] = useState<MutationStatus[]>([]);
  const [selected, setSelected] = useState<MutationStatus | null>(null);
  const [lookupId, setLookupId] = useState('');
  const [typedChallenge, setTypedChallenge] = useState('');
  const [recordKey, setRecordKey] = useState('');
  const [recordValue, setRecordValue] = useState('');
  const [recordHeaders, setRecordHeaders] = useState('');
  const [schemaExecutionSource, setSchemaExecutionSource] = useState('');
  const [connectExecutionConfiguration, setConnectExecutionConfiguration] = useState('');
  const [dataJobStatus, setDataJobStatus] = useState<DataJobStatus | null>(null);
  const [dataJobReconcileBatchId, setDataJobReconcileBatchId] = useState('');
  const [dataGeneratorStatus, setDataGeneratorStatus] = useState<DataGeneratorStatus | null>(null);
  const [dataGeneratorReconcileBatchId, setDataGeneratorReconcileBatchId] = useState('');
  const [proofReference, setProofReference] = useState('');
  const [workerFenceReference, setWorkerFenceReference] = useState('');
  const [reconciliationReview, setReconciliationReview] = useState<ReconciliationReview | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<{ reason: unknown } | null>(null);

  const clearExecutionMaterial = () => {
    setRecordKey('');
    setRecordValue('');
    setRecordHeaders('');
    setSchemaExecutionSource('');
    setConnectExecutionConfiguration('');
  };

  const selectOperation = (operation: MutationStatus) => {
    setSelected(operation);
    setTypedChallenge('');
    setDataJobStatus(null);
    setDataJobReconcileBatchId('');
    setDataGeneratorStatus(null);
    setDataGeneratorReconcileBatchId('');
    setProofReference(''); setWorkerFenceReference(''); setReconciliationReview(null);
    clearExecutionMaterial();
  };

  const loadApprovals = useCallback(async (signal?: AbortSignal) => {
    try {
      const response = await mutationApi.listApprovals(50, signal);
      setMutationAvailable(true);
      setApprovals(response.items);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      if ((reason instanceof MutationApiProblem && reason.status === 404) ||
          reason instanceof SyntaxError) {
        setMutationAvailable(false);
        setApprovals([]);
        setSelected(null);
        setError(null);
        return;
      }

      setMutationAvailable(true);
      setError({ reason });
    }
  }, []);

  useEffect(() => {
    if (enabled === false) {
      setMutationAvailable(false);
      return;
    }

    const controller = new AbortController();
    void loadApprovals(controller.signal);
    return () => controller.abort();
  }, [enabled, loadApprovals]);

  const refreshSelected = async () => {
    if (!selected) return;
    setReconciliationReview(null);
    setLoading(true);
    setError(null);
    try {
      setSelected(await mutationApi.get(selected.operationId));
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const lookup = async (event: FormEvent) => {
    event.preventDefault();
    const operationId = lookupId.trim();
    if (!operationId) return;

    setLoading(true);
    setError(null);
    try {
      selectOperation(await mutationApi.get(operationId));
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const apply = async (action: () => Promise<MutationStatus>, clearMaterialAfter = false) => {
    setLoading(true);
    setError(null);
    try {
      const operation = await action();
      setSelected(operation);
      setTypedChallenge('');
      if (clearMaterialAfter) clearExecutionMaterial();
      await loadApprovals();
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const loadDataGeneratorStatus = async () => {
    if (!selected || selected.operationKind !== 'dataGenerator') return;
    setReconciliationReview(null);
    setLoading(true);
    setError(null);
    try {
      const status = await mutationApi.getDataGeneratorStatus(selected.operationId);
      setDataGeneratorStatus(status);
      setDataGeneratorReconcileBatchId(status.pendingBatch?.batchId ?? '');
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const startDataGenerator = async () => {
    if (!selected || selected.operationKind !== 'dataGenerator') return;
    setLoading(true);
    setError(null);
    try {
      const operation = await mutationApi.startDataGenerator(selected.operationId);
      setSelected(operation);
      setDataGeneratorStatus(await mutationApi.getDataGeneratorStatus(operation.operationId));
      await loadApprovals();
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const cancelDataGenerator = async () => {
    if (!selected || selected.operationKind !== 'dataGenerator') return;
    setLoading(true);
    setError(null);
    try {
      setDataGeneratorStatus(await mutationApi.cancelDataGenerator(selected.operationId));
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const prepareReconciliation = (generator: boolean) => {
    if (!selected || selected.operationKind !== (generator ? 'dataGenerator' : 'dataJob')) return;
    setError(null); setReconciliationReview(null);
    try {
      const status = generator ? dataGeneratorStatus : dataJobStatus;
      const batchId = (generator ? dataGeneratorReconcileBatchId : dataJobReconcileBatchId).trim();
      if (!status?.pendingBatch || batchId !== status.pendingBatch.batchId) throw new ApplicationCopyError("Reload the exact unresolved batch before review.");
      if (typeof globalThis.crypto?.randomUUID !== 'function') throw new ApplicationCopyError("Secure randomness is required for the reconciliation Idempotency-Key.");
      if (status.progressVersion === null || status.progressVersion === undefined) throw new ApplicationCopyError("Reload durable progress before preparing a reconciliation.");
      const request: DataReconciliationPreviewInput = { batchId, expectedProgressVersion: status.progressVersion,
        proofReferenceSha256: proofReference.trim().toLowerCase(), fencedWorkerEvidenceSha256: workerFenceReference.trim().toLowerCase() };
      const idempotencyKey = globalThis.crypto.randomUUID();
      validateDataReconciliationPreview(selected.operationId, request, idempotencyKey);
      setReconciliationReview({ ...request, parentOperationId: selected.operationId, generator, idempotencyKey });
    } catch (reason) { setError({ reason }); }
  };

  const createReconciliationPreview = async () => {
    if (!selected || !reconciliationReview || selected.operationId !== reconciliationReview.parentOperationId || loading) return;
    setLoading(true);
    setError(null);
    try {
      const { parentOperationId, generator, idempotencyKey, ...request } = reconciliationReview;
      const operation = await (generator ? mutationApi.previewDataGeneratorReconciliation(parentOperationId, request, idempotencyKey)
        : mutationApi.previewDataJobReconciliation(parentOperationId, request, idempotencyKey));
      if (operation.operationKind !== 'fleetUncertaintyDisposition' || operation.riskClass !== 'critical' || !operation.requiresIndependentApproval)
        throw new ApplicationCopyError("The server did not return the required independently approved CRITICAL reconciliation plan.");
      selectOperation(operation);
      await loadApprovals();
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const executionSurface = selected === null
    ? 'none'
    : mutationExecutionSurface(selected);

  const executeRecord = () => {
    if (!selected) return;
    void apply(() => {
      const headers = Object.entries(pairMap(recordHeaders)).map(([name, value]) => ({ name, value: bytesToBase64(value) }));
      return mutationApi.executeRecordProduction(selected, [{ key: recordKey ? bytesToBase64(recordKey) : null, value: bytesToBase64(recordValue), headers }]);
    }, true);
  };

  const executeSchema = () => {
    if (!selected) return;
    void apply(() => mutationApi.executeSchemaCreate(selected, schemaExecutionSource), true);
  };

  const executeConnectConfiguration = () => {
    if (!selected) return;
    void apply(() => mutationApi.executeConnectConfiguration(selected, pairMap(connectExecutionConfiguration)), true);
  };

  const loadDataJobStatus = async () => {
    if (!selected || selected.operationKind !== 'dataJob') return;
    setReconciliationReview(null);
    setLoading(true);
    setError(null);
    try {
      const status = await mutationApi.getDataJobStatus(selected.operationId);
      setDataJobStatus(status);
      setDataJobReconcileBatchId(status.pendingBatch?.batchId ?? '');
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const startDataJob = async () => {
    if (!selected || selected.operationKind !== 'dataJob') return;
    setLoading(true);
    setError(null);
    try {
      const operation = await mutationApi.startDataJob(selected.operationId);
      setSelected(operation);
      setDataJobStatus(await mutationApi.getDataJobStatus(operation.operationId));
      await loadApprovals();
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  const cancelDataJob = async () => {
    if (!selected || selected.operationKind !== 'dataJob') return;
    setLoading(true);
    setError(null);
    try {
      setDataJobStatus(await mutationApi.cancelDataJob(selected.operationId));
    } catch (reason) {
      setError({ reason });
    } finally {
      setLoading(false);
    }
  };

  if (mutationAvailable !== true) return null;

  return <section className="card kafdeck-card" id="mutations" aria-labelledby="mutations-title">
    <h2 id="mutations-title">{t("Governed mutations")}</h2>
    <p>{t("Mutation actions use frozen previews, explicit confirmation, current-request authorization rechecks and durable execution state. Unknown or partial outcomes are never presented as safe retries.")}</p>

    <MutationPreviewWorkflows clusterId={clusterId} connectProfileId={connectProfileId ?? null} onPreview={selectOperation} />

    <article aria-labelledby="mutation-lookup-title">
      <h3 id="mutation-lookup-title">{t("Operation status")}</h3>
      <form onSubmit={event => void lookup(event)}>
        <label htmlFor="mutation-operation-id">{t("Operation ID")}</label>{' '}
        <input dir="ltr" id="mutation-operation-id" value={lookupId} onChange={event => setLookupId(event.target.value)} autoComplete="off" />{' '}
        <button type="submit" disabled={loading || lookupId.trim().length === 0}>{t("Load operation")}</button>
      </form>
    </article>

    <article aria-labelledby="approval-inbox-title">
      <h3 id="approval-inbox-title">{t("Independent approval inbox")}</h3>
      <p>{t("Only operations for which the current operator is independently eligible and currently authorized are shown.")}</p>
      <button type="button" onClick={() => void loadApprovals()} disabled={loading}>{t("Refresh approvals")}</button>
      {approvals.length === 0
        ? <p>{t("No currently authorized mutation approvals are pending.")}</p>
        : <table><thead><tr><th scope="col">{t("Kind")}</th><th scope="col">{t("Cluster")}</th><th scope="col">{t("Risk")}</th><th scope="col">{t("Expires")}</th><th scope="col">{t("Review")}</th></tr></thead><tbody>{approvals.map(operation => <tr key={operation.operationId}><th scope="row"><bdi dir="ltr" lang="en">{operation.operationKind}</bdi></th><td><bdi dir="ltr" lang="en">{operation.clusterId}</bdi></td><td>{operation.riskClass}</td><td>{operation.previewExpiresAtUtc}</td><td><button type="button" onClick={() => selectOperation(operation)}>{t("Review")}</button></td></tr>)}</tbody></table>}
    </article>

    {error && <p role="alert">{describeProblem(error.reason)}</p>}
    {selected && <article aria-labelledby="selected-mutation-title">
      <h3 id="selected-mutation-title">{t("Selected operation")}</h3>
      <MutationSummary operation={selected} />
      <div aria-label={t("Mutation actions")}>
        <button type="button" onClick={() => void refreshSelected()} disabled={loading}>{t("Refresh status")}</button>{' '}
        {selected.state === 'awaitingConfirmation' && <>
          {selected.confirmationMode === 'typedTarget' && <><label htmlFor="mutation-confirmation-target">{t("Type the exact server challenge")}</label>{' '}<input dir="ltr" id="mutation-confirmation-target" value={typedChallenge} onChange={event => setTypedChallenge(event.target.value)} aria-describedby="mutation-confirmation-help" autoComplete="off" />{' '}<small id="mutation-confirmation-help">{t("Challenge:")}{' '}<code dir="ltr" lang="en">{selected.confirmationChallenge ?? t("Unavailable")}</code></small>{' '}</>}
          <button type="button" disabled={loading || (selected.confirmationMode === 'typedTarget' && typedChallenge !== selected.confirmationChallenge)} onClick={() => void apply(() => mutationApi.confirm(selected, selected.confirmationMode === 'typedTarget' ? typedChallenge : null))}>{t("Confirm preview")}</button>{' '}
        </>}
        {selected.state === 'awaitingApproval' && <>
          <button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.approve(selected))}>{t("Approve independently")}</button>{' '}
          <button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.reject(selected))}>{t("Reject")}</button>{' '}
        </>}
        {preDispatchStates.has(selected.state) && <button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.cancel(selected))}>{t("Cancel before dispatch")}</button>}
        {executionSurface === 'genericNoMaterial' && selected.operationKind !== 'dataJob' && <>{' '}<button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.executeWithoutMaterial(selected))}>{t("Execute governed mutation")}</button></>}
        {selected.operationKind === 'dataJob' && selected.state === 'ready' && <>{' '}<button type="button" disabled={loading} onClick={() => void startDataJob()}>{t("Start governed data job")}</button></>}
        {selected.operationKind === 'dataJob' && <>{' '}<button type="button" disabled={loading} onClick={() => void loadDataJobStatus()}>{t("Load data-job status")}</button></>}
        {selected.operationKind === 'dataGenerator' && selected.state === 'ready' && <>{' '}<button type="button" disabled={loading} onClick={() => void startDataGenerator()}>{t("Start bounded generator")}</button></>}
        {selected.operationKind === 'dataGenerator' && <>{' '}<button type="button" disabled={loading} onClick={() => void loadDataGeneratorStatus()}>{t("Load generator status")}</button></>}
        {executionSurface === 'connectNoMaterial' && selected.operationKind === 'connectDelete' && <>{' '}<button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.executeConnectWithoutMaterial(selected))}>{t("Execute connector deletion")}</button></>}
        {executionSurface === 'connectNoMaterial' && selected.operationKind === 'connectAlter' && <>{' '}<button type="button" disabled={loading} onClick={() => void apply(() => mutationApi.executeConnectWithoutMaterial(selected))}>{t("Execute admitted connector control")}</button></>}
      </div>

      {selected.operationKind === 'dataJob' && dataJobStatus && <section aria-labelledby="data-job-status-title">
        <h4 id="data-job-status-title">{t("Data-job runtime status")}</h4>
        <dl>
          <dt>{t("Kind")}</dt><dd>{dataJobStatus.kind}</dd>
          <dt>{t("Progress phase")}</dt><dd>{dataJobStatus.progressPhase}</dd>
          <dt>{t("Source")}</dt><dd><bdi dir="ltr" lang="en">{dataJobStatus.sourceClusterId}</bdi></dd>
          <dt>{t("Destination")}</dt><dd><bdi dir="ltr" lang="en">{dataJobStatus.destinationClusterId}</bdi></dd>
          <dt>{t("Acknowledged records")}</dt><dd>{dataJobStatus.acknowledgedRecords}</dd>
          <dt>{t("Acknowledged bytes")}</dt><dd><bdi dir="ltr" lang="en">{dataJobStatus.acknowledgedBytes}</bdi></dd>
          <dt>{t("Worker generation")}</dt><dd><bdi dir="ltr" lang="en">{dataJobStatus.workerGeneration}</bdi></dd>
        </dl>
        <p>{t("Cancellation fences the current worker and stops future effects. It cannot erase an unresolved destination-write batch.")}</p>
        <button type="button" disabled={loading || dataJobStatus.progressPhase === 'Stopped' || dataJobStatus.pendingBatch !== null} onClick={() => void cancelDataJob()}>{t("Cancel future effects")}</button>
        {dataJobStatus.pendingBatch && <div role="alert">
          <p><strong>{t("Unresolved external write.")}</strong> {' '}{t("Batch")}{' '}<code dir="ltr" lang="en">{dataJobStatus.pendingBatch.batchId}</code> {' '}{t("remains durable and must not be blindly replayed.")}</p>
          <label htmlFor="data-job-reconcile-batch">{t("Exact unresolved batch ID")}</label>{' '}
          <input dir="ltr" id="data-job-reconcile-batch" maxLength={256} value={dataJobReconcileBatchId} onChange={event => { setDataJobReconcileBatchId(event.target.value); setReconciliationReview(null); }} autoComplete="off" />{' '}
          <button type="button" disabled={loading || dataJobReconcileBatchId.trim() !== dataJobStatus.pendingBatch.batchId || dataJobStatus.progressVersion == null} onClick={() => prepareReconciliation(false)}>{t("Review governed reconciliation")}</button>
        </div>}
        <table><thead><tr><th scope="col">{t("Range")}</th><th scope="col">{t("Source")}</th><th scope="col">{t("Destination")}</th><th scope="col">{t("Frozen offsets")}</th><th scope="col">{t("Next offset")}</th></tr></thead><tbody>
          {dataJobStatus.ranges.map(range => <tr key={range.rangeIndex}><td><bdi dir="ltr" lang="en">{range.rangeIndex}</bdi></td><td>{range.sourceTopic}[{range.sourcePartition}]</td><td>{range.destinationTopic}[{range.destinationPartition}]</td><td>[{range.startInclusive}, {range.endExclusive})</td><td>{range.nextSourceOffset ?? t("Not started")}</td></tr>)}
        </tbody></table>
      </section>}

      {selected.operationKind === 'dataGenerator' && dataGeneratorStatus && <section aria-labelledby="data-generator-status-title">
        <h4 id="data-generator-status-title">{t("Data Generator runtime status")}</h4>
        <dl>
          <dt>{t("Progress phase")}</dt><dd>{dataGeneratorStatus.progressPhase}</dd>
          <dt>{t("Destination")}</dt><dd><bdi dir="ltr" lang="en">{dataGeneratorStatus.destinationClusterId}</bdi> / {dataGeneratorStatus.destinationTopic}[{dataGeneratorStatus.destinationPartition}]</dd>
          <dt>{t("Source")}</dt><dd><bdi dir="ltr" lang="en">{dataGeneratorStatus.sourceKind}</bdi></dd>
          <dt>{t("Deterministic seed")}</dt><dd>{dataGeneratorStatus.seed}</dd>
          <dt>{t("Records planned")}</dt><dd><bdi dir="ltr" lang="en">{dataGeneratorStatus.recordCount}</bdi></dd>
          <dt>{t("Next record index")}</dt><dd><bdi dir="ltr" lang="en">{dataGeneratorStatus.nextRecordIndex}</bdi></dd>
          <dt>{t("Acknowledged records")}</dt><dd>{dataGeneratorStatus.acknowledgedRecords}</dd>
          <dt>{t("Acknowledged bytes")}</dt><dd><bdi dir="ltr" lang="en">{dataGeneratorStatus.acknowledgedBytes}</bdi></dd>
          <dt>{t("Worker generation")}</dt><dd><bdi dir="ltr" lang="en">{dataGeneratorStatus.workerGeneration}</bdi></dd>
          <dt>{t("Active runtime")}</dt><dd>{dataGeneratorStatus.activeRuntimeMilliseconds} ms</dd>
        </dl>
        <p>{t("Cancellation fences future generator effects. A pending external write is never blindly retried; exact provider evidence is required before reconciliation.")}</p>
        <button
          type="button"
          disabled={
            loading ||
            dataGeneratorStatus.pendingBatch !== null ||
            dataGeneratorStatus.progressPhase.toLocaleLowerCase() === 'stopped'
          }
          onClick={() => void cancelDataGenerator()}
        >
          {t("Cancel future generator effects")}{' '}</button>
        {dataGeneratorStatus.pendingBatch && <div role="alert">
          <p><strong>{t("Unresolved generator write.")}</strong> {' '}{t("Batch")}{' '}<code dir="ltr" lang="en">{dataGeneratorStatus.pendingBatch.batchId}</code>{t(", record index")}{' '}<bdi dir="ltr" lang="en">{dataGeneratorStatus.pendingBatch.recordIndex}</bdi>{t(", remains durable and must not be replayed automatically.")}</p>
          <label htmlFor="data-generator-reconcile-batch">{t("Exact unresolved generator batch ID")}</label>{' '}
          <input dir="ltr"
            id="data-generator-reconcile-batch"
            value={dataGeneratorReconcileBatchId}
            onChange={event => { setDataGeneratorReconcileBatchId(event.target.value); setReconciliationReview(null); }}
            maxLength={256}
            autoComplete="off"
          />{' '}
          <button
            type="button"
            disabled={loading || dataGeneratorReconcileBatchId.trim() !== dataGeneratorStatus.pendingBatch.batchId || dataGeneratorStatus.progressVersion == null}
            onClick={() => prepareReconciliation(true)}
          >
            {t("Review governed reconciliation")}{' '}</button>
        </div>}
      </section>}

      {((selected.operationKind === 'dataJob' && dataJobStatus?.pendingBatch) ||
        (selected.operationKind === 'dataGenerator' && dataGeneratorStatus?.pendingBatch)) && <fieldset disabled={loading}>
        <legend>{t("Reconciliation proof references")}</legend>
        <p>{t("A hash is a reference, not proof that no write occurred. Physically fence the old worker and obtain independent provider evidence first. The server binds the current batch, worker generation, topic identity and both watermarks. Reconciliation requires CRITICAL confirmation and a distinct authorized human approval.")}</p>
        <label htmlFor="reconciliation-proof">{t("External proof SHA256")}</label>{' '}
        <input dir="ltr" id="reconciliation-proof" value={proofReference} maxLength={64} autoComplete="off" onChange={event => { setProofReference(event.target.value); setReconciliationReview(null); }} />{' '}
        <label htmlFor="reconciliation-fence">{t("Physically fenced worker evidence SHA256")}</label>{' '}
        <input dir="auto" id="reconciliation-fence" value={workerFenceReference} maxLength={64} autoComplete="off" onChange={event => { setWorkerFenceReference(event.target.value); setReconciliationReview(null); }} />
      </fieldset>}
      {reconciliationReview && <aside aria-label={t("Reviewed reconciliation request")}>
        <h4>{t("Review reconciliation references")}</h4>
        <dl><dt>{t("Parent operation")}</dt><dd><bdi dir="ltr" lang="en">{reconciliationReview.parentOperationId}</bdi></dd><dt>{t("Exact batch")}</dt><dd><bdi dir="ltr" lang="en">{reconciliationReview.batchId}</bdi></dd>
          <dt>{t("Expected progress version")}</dt><dd><bdi dir="ltr" lang="en">{reconciliationReview.expectedProgressVersion}</bdi></dd>
          <dt>{t("External proof SHA256")}</dt><dd>{reconciliationReview.proofReferenceSha256}</dd>
          <dt>{t("Fenced worker evidence SHA256")}</dt><dd>{reconciliationReview.fencedWorkerEvidenceSha256}</dd></dl>
        <p>{t("This creates an immutable CRITICAL preview. It does not clear a batch or resume a worker.")}</p>
        <button type="button" disabled={loading} onClick={() => void createReconciliationPreview()}>{t("Create CRITICAL reconciliation preview")}</button>
      </aside>}

      {executionSurface === 'recordMaterial' && <fieldset disabled={loading}><legend>{t("Re-submit record execution material")}</legend><p>{t("This material must digest-match the preview and is cleared from UI state after execution.")}</p><label htmlFor="execute-record-key">{t("UTF-8 key (optional)")}</label>{' '}<input dir="ltr" id="execute-record-key" value={recordKey} onChange={event => setRecordKey(event.target.value)} autoComplete="off" /><br /><label htmlFor="execute-record-value">{t("UTF-8 value")}</label><br /><textarea dir="auto" id="execute-record-value" value={recordValue} onChange={event => setRecordValue(event.target.value)} rows={5} /><br /><label htmlFor="execute-record-headers">{t("Headers, one name=value per line")}</label><br /><textarea dir="auto" id="execute-record-headers" value={recordHeaders} onChange={event => setRecordHeaders(event.target.value)} rows={4} /><br /><button type="button" disabled={!recordValue} onClick={executeRecord}>{t("Execute with matching record material")}</button></fieldset>}

      {executionSurface === 'schemaMaterial' && <fieldset disabled={loading}><legend>{t("Re-submit schema execution material")}</legend><p>{t("The schema source must match the admitted fingerprint and is cleared from UI state after execution.")}</p><label htmlFor="execute-schema-source">{t("Schema source")}</label><br /><textarea dir="ltr" id="execute-schema-source" value={schemaExecutionSource} onChange={event => setSchemaExecutionSource(event.target.value)} rows={8} /><br /><button type="button" disabled={!schemaExecutionSource.trim()} onClick={executeSchema}>{t("Execute schema registration")}</button></fieldset>}

      {executionSurface === 'connectMaterial' && <fieldset disabled={loading}><legend>{t("Re-submit connector configuration")}</legend><p>{t("Connector create/update configuration must digest-match the admitted preview. Secret-bearing values remain browser-memory/request material and are cleared after execution.")}</p><label htmlFor="execute-connect-config">{t("Configuration, one key=value per line")}</label><br /><textarea dir="ltr" id="execute-connect-config" value={connectExecutionConfiguration} onChange={event => setConnectExecutionConfiguration(event.target.value)} rows={8} autoComplete="off" /><br /><button type="button" disabled={!connectExecutionConfiguration.trim()} onClick={executeConnectConfiguration}>{t("Execute connector configuration mutation")}</button></fieldset>}
    </article>}
  </section>;
}
