import { t } from '../../shared/i18n.js';
import { readJson, postJson, type MutationStatus } from '../mutations/mutationApi.js';

export interface DeclarativeInput { manifest: string; format: 'json' | 'yaml'; sourceRef: string; sourceRevision: string; expectedOwnershipRevision: number; }
export interface AutomationCapabilities { enabled: boolean; apiVersion: string; families: Array<{kind: string; apply: boolean; delete?: boolean; ownership?: string}>; }
export interface DeclarativeObservation {
  drift: string; ownership: string; ownershipRevision: number; subscriptionRevision: number | null;
  sourceInSync: boolean; deploymentGeneration: string; ownershipGeneration: string | null;
  sourceGeneration: string | null; observedAtUtc: string; observedFingerprint: string;
}
export interface SealedPlan {
  planId: string; planFingerprint: string; deploymentId: string; resourceId: string;
  desiredFingerprint: string; expectedOwnershipRevision: number; expiresAtUtc: string;
  ownerId: string; sourceId: string; sourceRevisionHash: string; deploymentGeneration: string;
  before: unknown; desired: unknown; after: unknown; changes: string[]; risk: string;
  requiresIndependentApproval: boolean; state: string; command: string; compensatesPlanId?: string | null;
}
export interface SourceRevision { sourceRef: string; generation: string; revision: number; sourceHash: string; }
export interface CompensationEligibility {catalogVersion: number; sourceOperationId: string; eligibility: string; reason: string; inverseManifest: string | null; format: 'json'; sourceRef: string; sourceRevision: string; expectedOwnershipRevision: number; requiresSourceAdvance: boolean;}
export function operationId(value: string): string {
  if (!/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i.test(value) || /^0{8}-0{4}-0{4}-0{4}-0{12}$/.test(value)) throw new Error(t("Enter an exact operation UUID."));
  return value;
}
function segment(value: string): string {
  if (!/^[a-zA-Z0-9_-][a-zA-Z0-9._-]{0,127}$/.test(value)) throw new Error(t("Enter a registered source/resource identifier."));
  return encodeURIComponent(value);
}
export function validateDeclarativeInput(input: DeclarativeInput): void {
  if (!input.manifest || new TextEncoder().encode(input.manifest).length > 16384) throw new Error(t("A manifest of at most 16 KiB is required."));
  if (!['json','yaml'].includes(input.format) || !/^[a-f0-9]{64}$/.test(input.sourceRevision) || !Number.isSafeInteger(input.expectedOwnershipRevision) || input.expectedOwnershipRevision < 0) throw new Error(t("Use an exact canonical SHA256 and nonnegative ownership revision."));
  segment(input.sourceRef);
}
export function automationOutcome(state: string): string {
  switch(state.toLowerCase()) {
    case 'executionunknown': return t("Unknown effect — retain the operation reference. No blind retry; recovery needs a verified positive receipt.");
    case 'awaitingapproval': return t("Waiting for a distinct eligible human. No product effect is complete.");
    case 'appliedverified': return t("The backend verified the completed effect. Refresh observation before another change.");
    case 'ready': return t("Ready for an explicit apply after current backend revalidation.");
    default: return t("Review the immutable plan and current backend state before any action.");
  }
}
export const automationApi = {
  compensation: (id: string, signal?: AbortSignal) => readJson<CompensationEligibility>('/api/v1/declarative/plans/'+operationId(id)+'/compensation',signal),
  compensate(id: string, input: DeclarativeInput, key: string, signal?: AbortSignal) { validateDeclarativeInput(input); return postJson<MutationStatus>('/api/v1/declarative/plans/'+operationId(id)+'/compensation-preview',input,{idempotencyKey:key,signal}); },
  capabilities: (signal?: AbortSignal) => readJson<AutomationCapabilities>('/api/v1/declarative/capabilities',signal),
  observe(input: DeclarativeInput, signal?: AbortSignal) { validateDeclarativeInput(input); return postJson<DeclarativeObservation>('/api/v1/declarative/observe',input,{signal}); },
  plan(input: DeclarativeInput, key: string, adopt: boolean, signal?: AbortSignal) {
    validateDeclarativeInput(input); if (!key || key.length > 128 || /[\x00-\x1f]/.test(key)) throw new Error(t("Supply a stable idempotency key, at most128 characters."));
    return postJson<MutationStatus>('/api/v1/declarative/'+(adopt?'import-preview':'plan'),input,{idempotencyKey:key,signal});
  },
  show: (id: string, signal?: AbortSignal) => readJson<SealedPlan>('/api/v1/declarative/plans/'+operationId(id),signal),
  apply: (plan: SealedPlan, recover: boolean, signal?: AbortSignal) => postJson<MutationStatus>('/api/v1/declarative/plans/'+operationId(plan.planId)+(recover?'/recover':'/apply'),{planFingerprint:plan.planFingerprint},{signal}),
  source: (source: string, resource: string, signal?: AbortSignal) => readJson<SourceRevision>('/api/v1/declarative/sources/'+segment(source)+'?resourceId='+segment(resource),signal),
  advance(input: DeclarativeInput, current: SourceRevision, signal?: AbortSignal) {
    validateDeclarativeInput(input);
    return postJson<SourceRevision>('/api/v1/declarative/sources/'+segment(input.sourceRef)+'/advance',{manifest:input.manifest,format:input.format,expectedGeneration:current.generation,expectedRevision:current.revision,expectedSourceHash:current.sourceHash},{signal});
  },
};
