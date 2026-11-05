import { MutationApiProblem, mutationApi, postJson, readJson, type MutationStatus } from '../mutations/mutationApi.js';

export type CatalogKind = 'Team' | 'Application' | 'Topic';
export type CatalogFamily = 'teams' | 'applications' | 'topics';
export interface CatalogChange {
  kind: CatalogKind; id: string | null; expectedRevision: number; name: string; description: string | null;
  archived: boolean; teamId: string | null; applicationId: string | null; topicName: string | null;
  owner: string | null; domain: string | null; classification: string | null; documentationReference: string | null; tags: string[];
  rebindTopicIdentity?: boolean;
}
export interface CatalogItem extends CatalogChange {
  id: string; revision: number; topicIdentity: string | null; topicIdentityCurrent?: boolean | null; createdBy: string; createdAtUtc: string; updatedAtUtc: string;
}
export interface CatalogHistory { record: CatalogItem; actorFingerprint: string; authorityFingerprint: string; operationId: string; effectiveFromUtc: string; effectiveToUtc: string | null; }
export interface Page<T> { items: T[]; nextCursor: string | null; }
export interface CatalogPreview { operation: MutationStatus; planId: string; contentHash: string; changes: Array<{change: CatalogChange; id: string; topicIdentity: string | null}>; expiresAtUtc: string; }
export interface PolicyVersion { versionId: string; clusterId: string; contentHash: string; document: string; createdBy: string; createdAtUtc: string; }
export interface PolicyActive { clusterId: string; revision: number; enabled: boolean; versionId: string | null; contentHash: string | null; digest: string | null; }
export interface PolicyValidation { valid: boolean; contentHash: string; document: unknown; }
export interface PolicyFixture { clusterId: string; resourceKind: string; resourceId: string; action: string; risk: string; }
export interface PolicyDecision { allowed: boolean; code: string; digest: string; matchedRuleIds: string[]; }
export interface PolicyOperationMaterial { operationId: string; previewHash: string; kind: string; clusterId: string; versionId: string; contentHash: string; document: string; expectedRevision: number; enabled: boolean; operation?: MutationStatus; }

const base = '/api/v1/governance';
export function exactId(value: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value) || /^0{8}-0{4}-0{4}-0{4}-0{12}$/.test(value))
    throw new Error('Use an exact nonempty server-issued UUID.');
  return value;
}
export function catalogRecordId(value: string): string {
  if (!/^[0-9a-f]{32}$/.test(value) || /^0{32}$/.test(value)) throw new Error('Use the exact server-issued catalog record ID (32 lowercase hexadecimal characters).');
  return value;
}
export function policyVersionId(value: string): string {
  if (!/^[0-9a-f]{64}$/.test(value)) throw new Error('Use the exact immutable policy version SHA256.');
  return value;
}
export function boundedScope(value: string): string {
  if (!value || value.length > 128 || /[\x00-\x1f\x7f/\\?#]/.test(value) || value === '.' || value === '..')
    throw new Error('Use the exact authorized cluster identifier.');
  return value;
}
function stableKey(value: string) {
  if (!value.trim() || value.length > 256 || /[\x00-\x1f\x7f]/.test(value)) throw new Error('Use a stable idempotency key of at most 256 characters.');
}
function query(clusterId: string, cursor?: string | null) {
  const params = new URLSearchParams({clusterId: boundedScope(clusterId), pageSize: '50'});
  if (cursor) { if (cursor.length > 4096 || /[\x00-\x1f\x7f]/.test(cursor)) throw new Error('Invalid continuation cursor.'); params.set('cursor', cursor); }
  return params;
}
function family(value: CatalogFamily) { if (!['teams', 'applications', 'topics'].includes(value)) throw new Error('Unsupported catalog family.'); return value; }
export function validateCatalogChanges(changes: unknown): asserts changes is CatalogChange[] {
  if (!Array.isArray(changes) || !changes.length || changes.length > 500) throw new Error('Supply 1–500 typed catalog changes.');
  for (const change of changes) {
    if (!change || Object.keys(change).some(key => !['kind','id','expectedRevision','name','description','archived','teamId','applicationId','topicName','owner','domain','classification','documentationReference','tags','rebindTopicIdentity'].includes(key)))
      throw new Error('Catalog changes must use the closed typed schema; server identities and authority fields cannot be supplied.');
    if (!change || !['Team', 'Application', 'Topic'].includes(change.kind) || !Number.isSafeInteger(change.expectedRevision) || change.expectedRevision < 0 ||
      typeof change.name !== 'string' || !change.name.trim() || change.name.length > 128 || typeof change.archived !== 'boolean' ||
      !Array.isArray(change.tags) || change.tags.length > 20 || change.tags.some((tag: unknown) => typeof tag !== 'string' || !tag.trim() || tag.length > 64 || /[\x00-\x1f\x7f]/.test(tag)))
      throw new Error('Catalog changes require bounded names/tags, an exact revision and a registered kind.');
    for (const value of [change.id, change.teamId, change.applicationId]) if (value !== null) catalogRecordId(value);
    for (const [value, maximum] of [[change.description, 2048], [change.owner, 128], [change.domain, 128], [change.classification, 128], [change.documentationReference, 2048]] as const)
      if (value !== null && (typeof value !== 'string' || !value.trim() || value.length > maximum || /[\x00-\x1f\x7f]/.test(value))) throw new Error('Metadata must be bounded nonempty safe text or null.');
    if (change.documentationReference !== null && !/^https:\/\/[^\s]+$/i.test(change.documentationReference)) throw new Error('Documentation reference must use HTTPS. It is never fetched by the server.');
    if (change.kind === 'Topic' && (!change.topicName || change.topicName.length > 249 || /[\x00-\x1f\x7f]/.test(change.topicName))) throw new Error('Topic metadata needs the exact existing topic name.');
  }
  if (new TextEncoder().encode(JSON.stringify(changes)).length > 1024 * 1024) throw new Error('Catalog import exceeds 1 MiB.');
}
export function parseCatalogImport(source: string): CatalogChange[] {
  if (new TextEncoder().encode(source).length > 1024 * 1024) throw new Error('Catalog import exceeds 1 MiB.');
  const value: unknown = JSON.parse(source);
  // JSON.parse discards duplicate keys. Reject ambiguity (including escaped key
  // aliases) in the original text before converting it into a typed request.
  const stack: Array<{object: boolean; key: boolean; names: Set<string>}> = [];
  for (const match of source.matchAll(/"(?:\\.|[^"\\])*"|[{}\[\]:,]/g)) {
    const token = match[0]; const frame = stack.at(-1);
    if (token === '{' || token === '[') { stack.push({object: token === '{', key: token === '{', names: new Set()}); if (stack.length > 12) throw new Error('Catalog import nesting exceeds the closed schema bound.'); }
    else if (token === '}' || token === ']') stack.pop();
    else if (token === ',' && frame?.object) frame.key = true;
    else if (token === ':' && frame?.object) frame.key = false;
    else if (token.startsWith('"') && frame?.object && frame.key) { const name: string = JSON.parse(token); if (frame.names.has(name)) throw new Error('Catalog import contains duplicate JSON keys.'); frame.names.add(name); }
  }
  if (!Array.isArray(value)) throw new Error('Catalog import must be an array of typed changes.');
  const changes = value.map(entry => {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) throw new Error('Catalog import entries must be typed objects.');
    const item = entry as Record<string, unknown>;
    const kind = typeof item.kind === 'string' ? ({team:'Team', application:'Application', topic:'Topic'} as Record<string,string>)[item.kind.toLowerCase()] ?? item.kind : item.kind;
    return {id:null,description:null,archived:false,teamId:null,applicationId:null,topicName:null,owner:null,domain:null,classification:null,documentationReference:null,tags:[],...item,kind};
  });
  validateCatalogChanges(changes); return changes;
}
export function boundedPolicy(document: string) {
  if (!document.trim() || new TextEncoder().encode(document).length > 256 * 1024) throw new Error('Policy document must contain at most 256 KiB of JSON.');
  // Preserve the raw document: the backend rejects duplicate/unknown keys before canonicalization.
  JSON.parse(document);
}
export function scopedOperation(operation: MutationStatus, clusterId: string, kinds: string[]) {
  exactId(operation.operationId);
  if (operation.clusterId !== clusterId || !kinds.some(kind => kind.toLowerCase() === operation.operationKind.toLowerCase()) || !/^[a-f0-9]{64}$/i.test(operation.previewHash))
    throw new Error('The returned operation does not match this scope and governance family.');
  return operation;
}
export function governanceProblem(error: unknown) {
  if (error instanceof SyntaxError) return 'The governance response or input could not be verified. No effect is inferred; refresh the same operation before another decision.';
  if (error instanceof MutationApiProblem) {
    if (error.status === 401) return 'Operator authentication expired. Sign in again; no effect is inferred.';
    if (error.status === 403) return 'The current operator is not authorized for this scope or action.';
    if (error.status === 404) return 'This capability or scoped resource is unavailable. No hidden resource is inferred.';
    if (error.status === 409) return 'Revision, material or policy conflict. Refresh the same operation and current state; prepare a fresh preview when required.';
    if (error.status === 503) return 'Governance authority is unavailable. Execution remains blocked.';
    return `${error.code ?? 'GovernanceRequestFailed'}: the request was not confirmed. Refresh current state before another decision.`;
  }
  return error instanceof Error ? error.message : 'Request outcome is unavailable. Refresh the same operation before another decision.';
}
export const catalogApi = {
  list(clusterId: string, kind: CatalogFamily, cursor?: string | null, signal?: AbortSignal) { return readJson<Page<CatalogItem>>(`${base}/catalog/${family(kind)}?${query(clusterId, cursor)}`, signal); },
  history(clusterId: string, kind: CatalogFamily, id: string, cursor?: string | null, signal?: AbortSignal) { return readJson<Page<CatalogHistory>>(`${base}/catalog/${family(kind)}/${catalogRecordId(id)}/history?${query(clusterId, cursor)}`, signal); },
  preview(clusterId: string, kind: CatalogFamily | 'imports', changes: CatalogChange[], key: string, signal?: AbortSignal) {
    boundedScope(clusterId); validateCatalogChanges(changes); stableKey(key); if (kind !== 'imports') family(kind);
    return postJson<CatalogPreview>(`${base}/catalog/${kind}/preview`, {clusterId, changes}, {idempotencyKey: key, signal});
  },
  operation(id: string, signal?: AbortSignal) { return readJson<CatalogPreview>(`${base}/catalog/operations/${exactId(id)}`, signal); },
  execute(operation: MutationStatus, signal?: AbortSignal) { return mutationApi.executeWithoutMaterial(operation, signal); },
  recover(operation: MutationStatus, signal?: AbortSignal) { return postJson<MutationStatus>(`${base}/catalog/operations/${exactId(operation.operationId)}/recover`, {previewHash: operation.previewHash}, {signal}); },
};
export const policyApi = {
  versions(clusterId: string, cursor?: string | null, signal?: AbortSignal) { return readJson<Page<PolicyVersion>>(`${base}/policies/versions?${query(clusterId, cursor)}`, signal); },
  active(clusterId: string, signal?: AbortSignal) { return readJson<PolicyActive>(`${base}/policies/active?${new URLSearchParams({clusterId: boundedScope(clusterId)})}`, signal); },
  operation(id: string, signal?: AbortSignal) { return readJson<PolicyOperationMaterial>(`${base}/policies/operations/${exactId(id)}`, signal); },
  recover(operation: MutationStatus, signal?: AbortSignal) { return postJson<MutationStatus>(`${base}/policies/operations/${exactId(operation.operationId)}/reconcile`, undefined, {signal}); },
  validate(clusterId: string, document: string, signal?: AbortSignal) { boundedScope(clusterId); boundedPolicy(document); return postJson<PolicyValidation>(`${base}/policies/validate`, {clusterId, document}, {signal}); },
  explain(fixture: PolicyFixture, signal?: AbortSignal) { boundedScope(fixture.clusterId); return postJson<PolicyDecision>(`${base}/policies/explain`, fixture, {signal}); },
  dryRun(fixtures: PolicyFixture[], signal?: AbortSignal) {
    if (!fixtures.length || fixtures.length > 100) throw new Error('Dry-run requires 1–100 scoped fixtures.'); fixtures.forEach(item => boundedScope(item.clusterId));
    return postJson<{items: PolicyDecision[]}>(`${base}/policies/dry-run`, {fixtures}, {signal});
  },
  previewVersion(clusterId: string, document: string, key: string, signal?: AbortSignal) {
    boundedScope(clusterId); boundedPolicy(document); stableKey(key);
    return postJson<MutationStatus>(`${base}/policies/versions/preview`, {clusterId, document, idempotencyKey: key}, {idempotencyKey: key, signal});
  },
  previewActivation(clusterId: string, versionId: string, expectedRevision: number, enabled: boolean, key: string, signal?: AbortSignal) {
    boundedScope(clusterId); policyVersionId(versionId); stableKey(key);
    if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0) throw new Error('Use the current nonnegative activation revision.');
    return postJson<MutationStatus>(`${base}/policies/activations/preview`, {clusterId, versionId, expectedRevision, enabled, idempotencyKey: key}, {idempotencyKey: key, signal});
  },
};
