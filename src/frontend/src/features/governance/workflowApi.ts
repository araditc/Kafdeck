import { t } from '../../shared/i18n.js';
import { postJson, readJson, type MutationStatus } from '../mutations/mutationApi.js';
import { boundedScope, catalogRecordId, exactId, scopedOperation, type Page } from './governanceApi.js';

export type RequestCommand = 'Draft' | 'Revise' | 'Submit' | 'Approve' | 'Reject' | 'Cancel';
export interface RequestBinding {
  operationId: string; clusterId: string; originalRequesterPrincipalId: string; previewHash: string; materialHash: string;
  policyDigest: string; operationKind: string; risk: string; resourceKeys: string[]; operationExpiresAtUtc: string;
  authorizationTargets: Array<{action: string; clusterId: string; resourceName: string}>;
}
export interface RequestDecision { actorPrincipalId: string; approved: boolean; reason: string; authorityFingerprint: string; atUtc: string; }
export interface ServiceRequest {
  requestId: string; clusterId: string; revision: number; version: number; businessState: string; requesterPrincipalId: string; clientKind: string;
  operationId: string; previewHash: string; materialHash: string; policyDigest: string; reason: string; createdAtUtc: string; updatedAtUtc: string;
  expiresAtUtc: string; executionState: string; executionCode: string | null; decisions: RequestDecision[]; binding?: RequestBinding;
}
export interface RequestChange { clusterId: string; requestId?: string; expectedVersion: number; command: RequestCommand; operationId?: string; reason: string; expiresAtUtc?: string; idempotencyKey?: string; }
export interface RequestPlan { planId: string; clusterId: string; contentHash: string; requestId: string; command: RequestCommand; expectedVersion: number; binding: RequestBinding; reason: string; requestExpiresAtUtc: string; }
export interface RequestOperation { plan: RequestPlan; operation: MutationStatus; }
export interface RequestHistory { [key: string]: unknown; }

const base = '/api/v1/governance';
function key(value: string) { if (!value.trim() || value.length > 256 || /[\x00-\x1f\x7f]/.test(value)) throw new Error(t("Use a stable bounded idempotency key.")); }
function version(value: number) { if (!Number.isSafeInteger(value) || value < 0) throw new Error(t("Use the current nonnegative CAS version.")); }
function page(clusterId: string, cursor?: string | null) {
  const query = new URLSearchParams({clusterId: boundedScope(clusterId), pageSize: '50'});
  if (cursor) { if (cursor.length > 4096 || /[\x00-\x1f\x7f]/.test(cursor)) throw new Error(t("Invalid continuation cursor.")); query.set('cursor', cursor); }
  return query;
}
export function utcInstant(value: string) {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?(?:Z|\+00:00)$/.test(value) || !Number.isFinite(Date.parse(value)) || Number(value.slice(0,4))<1 || new Date(Date.parse(value)).toISOString().slice(0,19)!==value.slice(0,19)) throw new Error(t("Use an exact UTC ISO8601 instant ending in Z or +00:00."));
  return value;
}
export function validateRequestChange(change: RequestChange) {
  if(!change||Object.keys(change).some(name=>!['clusterId','requestId','expectedVersion','command','operationId','reason','expiresAtUtc','idempotencyKey'].includes(name)))throw new Error(t("Use the closed request change schema; actor and approval facts are server-owned."));
  boundedScope(change.clusterId); version(change.expectedVersion);
  if (!['Draft','Revise','Submit','Approve','Reject','Cancel'].includes(change.command)) throw new Error(t("Choose a registered request command."));
  if (!change.reason.trim() || change.reason.length > 2048 || /[\x00-\x1f\x7f]/.test(change.reason)) throw new Error(t("Supply a reason of at most 2048 safe characters."));
  if (change.requestId) catalogRecordId(change.requestId);
  if (change.command !== 'Draft' && !change.requestId) throw new Error(t("Select an existing server-issued request ID."));
  if (change.command === 'Draft' && (change.requestId || change.expectedVersion !== 0)) throw new Error(t("A new draft requires no request ID and CAS version zero."));
  if (change.command === 'Draft' || change.command === 'Revise') { if (!change.operationId || !change.expiresAtUtc) throw new Error(t("Draft/revision requires an existing operation and explicit UTC expiry.")); exactId(change.operationId); utcInstant(change.expiresAtUtc); }
  if (change.idempotencyKey) key(change.idempotencyKey);
}
export function assertRequestOperation(value: RequestOperation, clusterId: string) {
  scopedOperation(value.operation, clusterId, ['serviceRequestChange','serviceRequestDecision']);
  if (value.plan.planId !== value.operation.operationId || value.plan.clusterId !== clusterId || !/^[0-9a-f]{64}$/.test(value.plan.contentHash)) throw new Error(t("The frozen request change does not match its scoped operation."));
}
export function assertLinkedOperation(request: ServiceRequest, operation: MutationStatus) {
  if (operation.operationId !== request.operationId || operation.previewHash !== request.previewHash || operation.clusterId !== request.clusterId) throw new Error(t("The linked operation differs from this immutable request revision. Replan explicitly."));
}
export const requestApi = {
  list(clusterId: string, cursor?: string | null, signal?: AbortSignal) { return readJson<Page<ServiceRequest>>(`${base}/requests?${page(clusterId, cursor)}`, signal); },
  get(clusterId: string, id: string, signal?: AbortSignal) { return readJson<ServiceRequest>(`${base}/requests/${catalogRecordId(id)}?${new URLSearchParams({clusterId: boundedScope(clusterId)})}`, signal); },
  history(clusterId: string, id: string, cursor?: string | null, signal?: AbortSignal) { return readJson<Page<RequestHistory>>(`${base}/requests/${catalogRecordId(id)}/history?${page(clusterId,cursor)}`, signal); },
  preview(change: RequestChange, stableKey: string, signal?: AbortSignal) { key(stableKey); validateRequestChange(change); const family=change.command==='Approve'||change.command==='Reject'?'decisions':'changes'; return postJson<MutationStatus>(`${base}/requests/${family}/preview`, {...change,idempotencyKey: stableKey}, {idempotencyKey: stableKey, signal}); },
  operation(id: string, signal?: AbortSignal) { return readJson<RequestOperation>(`${base}/requests/plans/${exactId(id)}`, signal); },
  recover(operation: MutationStatus, signal?: AbortSignal) { return postJson<MutationStatus>(`${base}/requests/operations/${exactId(operation.operationId)}/recover`, undefined, {signal}); },
};

export type MeasuredKind = 'trafficBytes' | 'storageByteSeconds';
export interface MeasurementQuery { clusterId: string; fromUtc: string; toUtc: string; resourceId?: string; }
export interface MeasurementWindow {
  windowId: string; sourceId: string; provider: string; metricKind: string; unit: string; resourceId: string; windowStartUtc: string; windowEndUtc: string;
  collectedAtUtc: string; quantity: string | null; quality: string; reasons: string[]; provenanceHash: string;
  ownership: {revision: number | null; teamId: string | null; applicationId: string | null; topicIdentity: string | null; quality: string};
}
export interface AttributionWindow {
  measurement: MeasurementWindow; allocations: Array<{teamId: string | null; applicationId: string | null; quantity: string; quality: string}>;
  residualQuantity: string | null; rateCardHash: string | null; currency: string | null; unroundedAmount: string | null; roundedAmount: string | null;
  quality: string; reasons: string[];
}
export interface MeasurementPage extends Page<MeasurementWindow | AttributionWindow> { fromUtc: string; toUtc: string; coverage: string; status: string; totals: {currency: string | null; unroundedAmount: string | null; roundedAmount: string | null; status: string; complete: boolean} | null; }
export interface RateCardDocument { metricKind: MeasuredKind; unit: 'bytes' | 'byteSeconds'; currency: string; amount: string; effectiveFromUtc: string; effectiveToUtc: string; weights: Array<{teamId: string | null; applicationId: string | null; weight: string}>; }
export interface RateCardVersion { versionId: string; clusterId: string; contentHash: string; document: RateCardDocument | string; createdAtUtc: string; createdBy?: string; }
export interface RateCardState { clusterId: string; revision: number; enabled: boolean; versionId: string | null; contentHash?: string | null; }
export interface RateCardOperation { operation: MutationStatus; operationId: string; previewHash: string; clusterId: string; contentHash: string; document: RateCardDocument; [key: string]: unknown; }
export function decimalEvidence(value: string | null | undefined) { return value == null ? t("Unknown") : value; }
export function measurementOf(value: MeasurementWindow | AttributionWindow): MeasurementWindow { return 'measurement' in value ? value.measurement : value; }
export function parseWorkflowJson(source: string): unknown {
  if(new TextEncoder().encode(source).length>256*1024)throw new Error(t("The closed document exceeds 256 KiB."));
  const frames:Array<{object:boolean;key:boolean;names:Set<string>}>=[];
  for(const match of source.matchAll(/"(?:\\.|[^"\\])*"|[{}\[\]:,]/g)){
    const token=match[0],frame=frames.at(-1);
    if(token==='{'||token==='['){frames.push({object:token==='{',key:token==='{',names:new Set()});if(frames.length>12)throw new Error(t("The closed document exceeds depth 12."));}
    else if(token==='}'||token===']')frames.pop();else if(token===','&&frame?.object)frame.key=true;else if(token===':'&&frame?.object)frame.key=false;
    else if(token.startsWith('"')&&frame?.object&&frame.key){const name:string=JSON.parse(token);if(frame.names.has(name))throw new Error(t("Duplicate JSON keys are not admitted."));frame.names.add(name);}
  }
  return JSON.parse(source);
}
export function invariantDecimal(value: string) { if (!/^(?:0|[1-9]\d{0,28})(?:\.\d{1,28})?$/.test(value)) throw new Error(t("Use a nonnegative invariant decimal string; exponent, sign and locale separators are not admitted.")); return value; }
export function validateMeasurementQuery(query: MeasurementQuery) {
  boundedScope(query.clusterId); utcInstant(query.fromUtc); utcInstant(query.toUtc);
  const duration = Date.parse(query.toUtc) - Date.parse(query.fromUtc);
  if (duration < 60_000 || duration > 31 * 86_400_000) throw new Error(t("Choose a UTC interval of at least one minute and at most 31 days."));
  if (query.resourceId !== undefined && (!query.resourceId.trim() || query.resourceId.length > 512 || /[\x00-\x1f\x7f]/.test(query.resourceId))) throw new Error(t("Use a bounded exact resource ID."));
}
export function validateRateCard(document: RateCardDocument) {
  if(!document||Object.keys(document).some(k=>!['metricKind','unit','currency','amount','effectiveFromUtc','effectiveToUtc','weights'].includes(k)))throw new Error(t("Use the closed rate-card document schema."));
  if (!['trafficBytes','storageByteSeconds'].includes(document.metricKind) || document.unit !== (document.metricKind === 'trafficBytes' ? 'bytes' : 'byteSeconds')) throw new Error(t("Choose a registered measured series and its exact unit."));
  if (!/^[A-Z]{3}$/.test(document.currency)) throw new Error(t("Use a three-letter uppercase currency code."));
  invariantDecimal(document.amount); utcInstant(document.effectiveFromUtc); utcInstant(document.effectiveToUtc);
  if (Date.parse(document.effectiveToUtc) <= Date.parse(document.effectiveFromUtc)) throw new Error(t("The rate-card effective interval must increase."));
  if (!Array.isArray(document.weights) || document.weights.length > 100) throw new Error(t("Supply at most 100 explicit allocation weights."));
  const owners=new Set<string>();
  const one=10n**28n;let sum=0n;
  for (const weight of document.weights) { if(!weight||Object.keys(weight).some(k=>!['teamId','applicationId','weight'].includes(k)))throw new Error(t("Use the closed allocation-weight schema.")); if (weight.teamId !== null) catalogRecordId(weight.teamId); if (weight.applicationId !== null) catalogRecordId(weight.applicationId); if (!weight.teamId && !weight.applicationId) throw new Error(t("An allocation weight needs a catalog owner.")); invariantDecimal(weight.weight); const [integer,fraction='']=weight.weight.split('.');const scaled=BigInt(integer!)*one+BigInt(fraction.padEnd(28,'0'));if(scaled<=0n||scaled>one)throw new Error(t("Each allocation weight must be greater than zero and at most one."));sum+=scaled;const owner=`${weight.teamId??''}/${weight.applicationId??''}`; if(owners.has(owner))throw new Error(t("An allocation owner may appear only once.")); owners.add(owner); }
  if(sum>one)throw new Error(t("Configured allocation weights exceed the admitted measured quantity."));
}
export function assertRateCardOperation(value: RateCardOperation, clusterId: string) {
  scopedOperation(value.operation, clusterId, ['rateCardVersion','rateCardActivation']);
  if (value.operationId !== value.operation.operationId || value.previewHash !== value.operation.previewHash || value.clusterId !== clusterId || !/^[0-9a-f]{64}$/.test(value.contentHash)) throw new Error(t("Rate-card material does not match this exact scoped operation."));
}
export const attributionApi = {
  windows(family: 'measurements' | 'attribution' | 'dashboards', input: MeasurementQuery, cursor?: string | null, signal?: AbortSignal) {
    if (!['measurements','attribution','dashboards'].includes(family)) throw new Error(t("Choose a registered measurement read.")); validateMeasurementQuery(input);
    const query=page(input.clusterId,cursor); query.set('fromUtc',input.fromUtc); query.set('toUtc',input.toUtc); if(input.resourceId)query.set('resourceId',input.resourceId);
    return readJson<MeasurementPage>(`${base}/${family}?${query}`,signal);
  },
  versions(clusterId: string, cursor?: string | null, signal?: AbortSignal) { return readJson<Page<RateCardVersion>>(`${base}/rate-cards/versions?${page(clusterId,cursor)}`,signal); },
  state(clusterId: string, signal?: AbortSignal) { return readJson<RateCardState>(`${base}/rate-cards/state?${new URLSearchParams({clusterId:boundedScope(clusterId)})}`,signal); },
  previewVersion(clusterId: string, document: RateCardDocument, stableKey: string, signal?: AbortSignal) { boundedScope(clusterId); key(stableKey); validateRateCard(document); return postJson<MutationStatus>(`${base}/rate-cards/versions/preview`,{clusterId,document,idempotencyKey:stableKey},{idempotencyKey:stableKey,signal}); },
  previewActivation(clusterId: string, versionId: string, expectedRevision: number, enabled: boolean, stableKey: string, signal?: AbortSignal) { boundedScope(clusterId); key(stableKey); version(expectedRevision); if(!/^[0-9a-f]{64}$/.test(versionId))throw new Error(t("Use the immutable rate-card version hash.")); return postJson<MutationStatus>(`${base}/rate-cards/activations/preview`,{clusterId,versionId,expectedRevision,enabled,idempotencyKey:stableKey},{idempotencyKey:stableKey,signal}); },
  operation(id: string, signal?: AbortSignal) { return readJson<RateCardOperation>(`${base}/rate-cards/operations/${exactId(id)}`,signal); },
  recover(operation: MutationStatus, signal?: AbortSignal) { return postJson<MutationStatus>(`${base}/rate-cards/operations/${exactId(operation.operationId)}/recover`,undefined,{signal}); },
};
