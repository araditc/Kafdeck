import { useEffect, useState } from 'react';
import {
  ApiProblem,
  kafdeckApi,
  type DataQualityEvidencePage,
  type DataQualityPolicy,
  type DataQualityPolicyLifecycleState,
  type DataQualityPolicyPage,
  type DataQualityPolicyUpsertRequest,
} from '../../shared/api.js';

const defaultDraft: DataQualityPolicyUpsertRequest = {
  version: 1,
  topicName: '',
  partitions: [0],
  rules: [
    {
      ruleId: 'id-required',
      kind: 'requiredPath',
      jsonPointer: '/id',
      expectedType: null,
      minimumNumber: null,
      maximumNumber: null,
      minimumLength: null,
      maximumLength: null,
    },
  ],
  budget: {
    recordsPerSecond: 100,
    bytesPerSecond: 1024 * 1024,
    evaluationWindowSeconds: 300,
    activePoliciesPerCluster: 20,
    concurrentReadersPerCluster: 2,
  },
  state: 'paused',
  expectedRevision: null,
};

function durationSeconds(value: string): number {
  const parts = value.split(':').map(part => Number(part));
  if (parts.length !== 3 || parts.some(part => !Number.isFinite(part))) return 300;
  const [hours, minutes, seconds] = parts;
  if (hours === undefined || minutes === undefined || seconds === undefined) return 300;
  return Math.max(1, Math.round(hours * 3600 + minutes * 60 + seconds));
}

function draftFromPolicy(policy: DataQualityPolicy): DataQualityPolicyUpsertRequest {
  return {
    version: policy.version,
    topicName: policy.topicName,
    partitions: [...policy.partitions],
    rules: policy.rules.map(rule => ({ ...rule })),
    budget: {
      recordsPerSecond: policy.budget.recordsPerSecond,
      bytesPerSecond: policy.budget.bytesPerSecond,
      evaluationWindowSeconds: durationSeconds(policy.budget.evaluationWindow),
      activePoliciesPerCluster: policy.budget.activePoliciesPerCluster,
      concurrentReadersPerCluster: policy.budget.concurrentReadersPerCluster,
    },
    state: policy.state,
    expectedRevision: policy.revision,
  };
}

function formatEvidenceCount(
  state: string,
  value: number,
): string {
  return state === 'available' || state === 'partial'
    ? String(value)
    : 'Unavailable';
}

function errorMessage(reason: unknown): string {
  if (reason instanceof ApiProblem) {
    if (reason.status === 401 || reason.status === 403) return 'You are not authorized for this data-quality operation.';
    if (reason.status === 404) return 'Data-quality management is not enabled or the policy is not visible.';
    if (reason.status === 409) return 'The policy changed concurrently. It has been refreshed; review the new revision before retrying.';
    return reason.message;
  }
  return reason instanceof Error ? reason.message : 'The data-quality operation failed.';
}

export function DataQualityPanel({ clusterId }: { clusterId: string }) {
  const [page, setPage] = useState<DataQualityPolicyPage | null>(null);
  const [selected, setSelected] = useState<DataQualityPolicy | null>(null);
  const [evidence, setEvidence] = useState<DataQualityEvidencePage | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [configured, setConfigured] = useState(true);
  const [editorOpen, setEditorOpen] = useState(false);
  const [editorPolicyId, setEditorPolicyId] = useState('');
  const [editorBody, setEditorBody] = useState(
    JSON.stringify(defaultDraft, null, 2),
  );

  const loadPolicies = async (signal?: AbortSignal) => {
    try {
      const result = await kafdeckApi.listDataQualityPolicies(
        clusterId,
        undefined,
        null,
        signal,
      );
      setPage(result);
      setConfigured(true);
      setError(null);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      if (reason instanceof ApiProblem && reason.status === 404) {
        setConfigured(false);
        setPage(null);
        setSelected(null);
        setEvidence(null);
        setError(null);
        return;
      }
      setError(errorMessage(reason));
    }
  };

  useEffect(() => {
    const controller = new AbortController();
    setPage(null);
    setSelected(null);
    setEvidence(null);
    setEditorOpen(false);
    setEditorPolicyId('');
    setEditorBody(JSON.stringify(defaultDraft, null, 2));
    setConfigured(true);
    setError(null);
    void loadPolicies(controller.signal);
    return () => controller.abort();
  }, [clusterId]);

  const openPolicy = async (policyId: string) => {
    setBusy(true);
    setError(null);
    setEvidence(null);
    try {
      const [policy, points] = await Promise.all([
        kafdeckApi.getDataQualityPolicy(clusterId, policyId),
        kafdeckApi.getDataQualityEvidence(clusterId, policyId),
      ]);
      setSelected(policy);
      setEvidence(points);
      setEditorOpen(false);
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setBusy(false);
    }
  };

  const refreshSelected = async () => {
    if (!selected) return;
    await openPolicy(selected.policyId);
    await loadPolicies();
  };

  const editSelected = () => {
    if (!selected) return;
    setEditorPolicyId(selected.policyId);
    setEditorBody(JSON.stringify(draftFromPolicy(selected), null, 2));
    setEditorOpen(true);
    setError(null);
  };

  const createPolicy = () => {
    setSelected(null);
    setEvidence(null);
    setEditorPolicyId('');
    setEditorBody(JSON.stringify(defaultDraft, null, 2));
    setEditorOpen(true);
    setError(null);
  };

  const savePolicy = async () => {
    const policyId = editorPolicyId.trim();
    if (!policyId) {
      setError('Policy ID is required.');
      return;
    }

    let request: DataQualityPolicyUpsertRequest;
    try {
      request = JSON.parse(editorBody) as DataQualityPolicyUpsertRequest;
    } catch {
      setError('Policy definition must be valid JSON.');
      return;
    }

    if (selected && selected.policyId === policyId) {
      request = {
        ...request,
        expectedRevision: selected.revision,
      };
    } else {
      request = {
        ...request,
        expectedRevision: null,
      };
    }

    setBusy(true);
    setError(null);
    try {
      const saved = await kafdeckApi.upsertDataQualityPolicy(
        clusterId,
        policyId,
        request,
      );
      setSelected(saved);
      setEditorBody(JSON.stringify(draftFromPolicy(saved), null, 2));
      setEditorOpen(false);
      await loadPolicies();
      const points = await kafdeckApi.getDataQualityEvidence(
        clusterId,
        policyId,
      );
      setEvidence(points);
    } catch (reason) {
      setError(errorMessage(reason));
      if (reason instanceof ApiProblem && reason.status === 409) {
        try {
          const current = await kafdeckApi.getDataQualityPolicy(
            clusterId,
            policyId,
          );
          setSelected(current);
          setEditorBody(JSON.stringify(draftFromPolicy(current), null, 2));
          await loadPolicies();
        } catch {
          // Preserve the original conflict; no implicit overwrite or retry.
        }
      }
    } finally {
      setBusy(false);
    }
  };

  const setState = async (state: DataQualityPolicyLifecycleState) => {
    if (!selected) return;
    setBusy(true);
    setError(null);
    try {
      const updated = await kafdeckApi.setDataQualityPolicyState(
        clusterId,
        selected.policyId,
        state,
        selected.revision,
      );
      setSelected(updated);
      setEditorBody(JSON.stringify(draftFromPolicy(updated), null, 2));
      await loadPolicies();
    } catch (reason) {
      setError(errorMessage(reason));
      if (reason instanceof ApiProblem && reason.status === 409) {
        await refreshSelected();
      }
    } finally {
      setBusy(false);
    }
  };

  return <section className="card kafdeck-card" id="data-quality" aria-labelledby="data-quality-title">
    <h2 id="data-quality-title">Data quality</h2>
    <p>
      Bounded policy lifecycle and aggregate evidence only. Raw Kafka keys, values,
      headers and payload exemplars are never exposed by this view.
    </p>

    {!configured && <p role="status">Data-quality persistence is not configured for this deployment.</p>}
    {error && <p role="alert">{error}</p>}
    {configured && !page && !error && <p role="status">Loading data-quality policies…</p>}

    {page && <>
      {page.authorizationFiltered && <p role="status">
        One or more policies were omitted by policy/topic authorization.
      </p>}
      {page.truncated && page.nextPolicyId === null && <p role="status">
        Additional policies exist, but no authorization-safe continuation cursor can be exposed for this page.
      </p>}
      <div>
        <button type="button" onClick={createPolicy} disabled={busy}>New policy</button>{' '}
        <button type="button" onClick={() => void loadPolicies()} disabled={busy}>Refresh policies</button>
      </div>
      {page.items.length === 0
        ? <p>No authorized data-quality policies are visible.</p>
        : <table>
            <thead><tr><th>Policy</th><th>Topic</th><th>State</th><th>Revision</th><th>Updated</th></tr></thead>
            <tbody>{page.items.map(policy => <tr key={policy.policyId}>
              <th scope="row"><button type="button" onClick={() => void openPolicy(policy.policyId)}>{policy.policyId}</button></th>
              <td>{policy.topicName}</td>
              <td>{policy.state}</td>
              <td>{policy.revision}</td>
              <td>{new Date(policy.updatedAtUtc).toLocaleString()}</td>
            </tr>)}</tbody>
          </table>}
    </>}

    {selected && <article aria-labelledby="data-quality-policy-title">
      <h3 id="data-quality-policy-title">{selected.policyId}</h3>
      <p>
        Topic: <strong>{selected.topicName}</strong> · Version: {selected.version} ·
        Revision: {selected.revision} · State: {selected.state}
      </p>
      <p>Partitions: {selected.partitions.join(', ')}</p>
      <p>
        Budget: {selected.budget.recordsPerSecond} records/s ·
        {selected.budget.bytesPerSecond} bytes/s ·
        window {selected.budget.evaluationWindow}
      </p>
      <table>
        <thead><tr><th>Rule</th><th>Kind</th><th>Path</th><th>Constraint</th></tr></thead>
        <tbody>{selected.rules.map(rule => <tr key={rule.ruleId}>
          <th scope="row">{rule.ruleId}</th>
          <td>{rule.kind}</td>
          <td><code>{rule.jsonPointer}</code></td>
          <td>{[
            rule.expectedType,
            rule.minimumNumber !== null ? `min=${rule.minimumNumber}` : null,
            rule.maximumNumber !== null ? `max=${rule.maximumNumber}` : null,
            rule.minimumLength !== null ? `minLength=${rule.minimumLength}` : null,
            rule.maximumLength !== null ? `maxLength=${rule.maximumLength}` : null,
          ].filter(Boolean).join(', ') || 'Closed predicate'}</td>
        </tr>)}</tbody>
      </table>

      <div>
        <button type="button" onClick={editSelected} disabled={busy}>Edit revision</button>{' '}
        {(['active', 'paused', 'disabled', 'retired'] as const).map(state =>
          <button
            key={state}
            type="button"
            disabled={busy || selected.state === state}
            onClick={() => void setState(state)}
          >
            Set {state}
          </button>)}
      </div>

      <h4>Aggregate evidence</h4>
      {!evidence && !busy && <p>No evidence loaded.</p>}
      {evidence?.authorizationFiltered && <p role="status">
        Historical points from topics you cannot read were omitted.
      </p>}
      {evidence?.truncated && <p role="status">Evidence is truncated at the server-owned point limit.</p>}
      {evidence && evidence.points.length === 0 && <p>No authorized evidence points are available in the current window.</p>}
      {evidence && evidence.points.length > 0 && <table>
        <thead><tr><th>Window</th><th>Topic / partition</th><th>State</th><th>Evaluated</th><th>Violations</th><th>Outcome</th></tr></thead>
        <tbody>{evidence.points.map((point, index) => <tr key={`${point.policyVersion}:${point.topicName}:${point.partition}:${point.windowStartUtc}:${index}`}>
          <td>{new Date(point.windowStartUtc).toLocaleString()} – {new Date(point.windowEndUtc).toLocaleString()}</td>
          <td>{point.topicName} / {point.partition}</td>
          <td>{point.state}</td>
          <td>{formatEvidenceCount(point.state, point.evaluatedRecords)}</td>
          <td>{formatEvidenceCount(point.state, point.violationCount)}</td>
          <td>{point.outcome}</td>
        </tr>)}</tbody>
      </table>}
    </article>}

    {editorOpen && <article aria-labelledby="data-quality-editor-title">
      <h3 id="data-quality-editor-title">{selected ? 'Edit policy revision' : 'Create policy'}</h3>
      <p>
        The server owns cluster identity, hard budgets, rule grammar and CAS.
        A 409 conflict requires refresh; this UI never retries an overwrite implicitly.
      </p>
      <label htmlFor="data-quality-policy-id">Policy ID</label>{' '}
      <input
        id="data-quality-policy-id"
        value={editorPolicyId}
        disabled={selected !== null}
        onChange={event => setEditorPolicyId(event.target.value)}
        autoComplete="off"
      />
      <label htmlFor="data-quality-policy-json">Policy definition</label>
      <textarea
        id="data-quality-policy-json"
        rows={18}
        value={editorBody}
        onChange={event => setEditorBody(event.target.value)}
        autoComplete="off"
      />
      <button type="button" onClick={() => void savePolicy()} disabled={busy}>
        {busy ? 'Saving…' : 'Save guarded revision'}
      </button>{' '}
      <button type="button" onClick={() => setEditorOpen(false)} disabled={busy}>Cancel</button>
    </article>}
  </section>;
}
