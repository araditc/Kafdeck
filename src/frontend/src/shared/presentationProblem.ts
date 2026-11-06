import { t } from './i18n.js';

// Closed application-owned copy only. External prose follows the raw branch,
// even when its spelling happens to match one of these resource keys.
export const applicationProblemCopies = [
  "Operator authentication is required.",
  "Access denied by Kafdeck operator authorization policy.",
  "Access denied by Kafka authorization policy.",
  "This capability is unsupported by the connected Kafka cluster.",
  "The requested observation could not be loaded.",
  "Record access is denied by Kafdeck policy.",
  "Kafka denied record access for this target.",
  "Record browsing is unsupported for this target.",
  "Record data could not be loaded.",
  "Live-tail admission limit reached.",
  "You are not authorized for this data-quality operation.",
  "Data-quality management is not enabled or the policy is not visible.",
  "The policy changed concurrently. It has been refreshed; review the new revision before retrying.",
  "The data-quality operation failed.",
  "Policy ID is required.",
  "Policy definition must be valid JSON.",
  "Operator session expired; re-authenticate before any further change.",
  "Subscription management is not authorized for this subscription/destination.",
  "Management is disabled or this subscription is no longer accessible.",
  "Subscription conflict: reload authorized current state and re-enter its CAS revision.",
  "Management request was not confirmed. Inspect the current state before retrying.",
  "Subscription/destination identities must be bounded opaque IDs, not URLs or paths.",
  "Select one or more distinct supported event classes.",
  "Event types must be unique, bounded identifiers (at most 32).",
  "Replace/retire requires a precise positive CAS revision from current authorized data.",
  "A new subscription cannot start in terminal Retired state.",
  "Your operator session has expired.",
  "Notification evidence is unavailable or not authorized for this operator.",
  "The notification observation could not be loaded.",
  "Provide a valid notification UUID and destination ID.",
  "Provide a destination ID of 1–128 characters.",
  "Use an exact nonempty server-issued UUID.",
  "Use the exact server-issued catalog record ID (32 lowercase hexadecimal characters).",
  "Use the exact immutable policy version SHA256.",
  "Use the exact authorized cluster identifier.",
  "Use a stable idempotency key of at most 256 characters.",
  "Invalid continuation cursor.",
  "Unsupported catalog family.",
  "Supply 1–500 typed catalog changes.",
  "Catalog changes must use the closed typed schema; server identities and authority fields cannot be supplied.",
  "Catalog changes require bounded names/tags, an exact revision and a registered kind.",
  "Metadata must be bounded nonempty safe text or null.",
  "Documentation reference must use HTTPS. It is never fetched by the server.",
  "Topic metadata needs the exact existing topic name.",
  "Catalog import exceeds 1 MiB.",
  "Catalog import nesting exceeds the closed schema bound.",
  "Catalog import contains duplicate JSON keys.",
  "Catalog import must be an array of typed changes.",
  "Catalog import entries must be typed objects.",
  "Policy document must contain at most 256 KiB of JSON.",
  "The returned operation does not match this scope and governance family.",
  "Dry-run requires 1–100 scoped fixtures.",
  "Use the current nonnegative activation revision.",
  "Use a stable bounded idempotency key.",
  "Use the current nonnegative CAS version.",
  "Use an exact UTC ISO8601 instant ending in Z or +00:00.",
  "Use the closed request change schema; actor and approval facts are server-owned.",
  "Choose a registered request command.",
  "Supply a reason of at most 2048 safe characters.",
  "Select an existing server-issued request ID.",
  "A new draft requires no request ID and CAS version zero.",
  "Draft/revision requires an existing operation and explicit UTC expiry.",
  "The frozen request change does not match its scoped operation.",
  "The linked operation differs from this immutable request revision. Replan explicitly.",
  "The closed document exceeds 256 KiB.",
  "The closed document exceeds depth 12.",
  "Duplicate JSON keys are not admitted.",
  "Use a nonnegative invariant decimal string; exponent, sign and locale separators are not admitted.",
  "Choose a UTC interval of at least one minute and at most 31 days.",
  "Use a bounded exact resource ID.",
  "Use the closed rate-card document schema.",
  "Choose a registered measured series and its exact unit.",
  "Use a three-letter uppercase currency code.",
  "The rate-card effective interval must increase.",
  "Supply at most 100 explicit allocation weights.",
  "Use the closed allocation-weight schema.",
  "An allocation weight needs a catalog owner.",
  "Each allocation weight must be greater than zero and at most one.",
  "An allocation owner may appear only once.",
  "Configured allocation weights exceed the admitted measured quantity.",
  "Rate-card material does not match this exact scoped operation.",
  "Choose a registered measurement read.",
  "Use the immutable rate-card version hash.",
  "Catalog preview material is incomplete or bound to another operation.",
  "Reload the frozen catalog plan before another decision.",
  "Immutable policy material does not match this exact scoped operation.",
  "Reload frozen policy material before continuing.",
  "Reload frozen request material.",
  "Select the immutable request first.",
  "Reload immutable rate-card material.",
  "Reconciliation requires an exact operation, batch, current progress version, SHA256 proof/fence references and Idempotency-Key.",
  "Each execution-material line must use key=value.",
  "Duplicate key: {0}",
  "Reload the exact unresolved batch before review.",
  "Secure randomness is required for the reconciliation Idempotency-Key.",
  "Reload durable progress before preparing a reconciliation.",
  "The server did not return the required independently approved CRITICAL reconciliation plan.",
  "Secure browser randomness is required to generate an Idempotency-Key.",
  "Each configuration line must use key=value.",
  "Configuration keys must not be empty.",
  "Duplicate configuration key: {0}",
  "Each configuration change must use key=value or key=<remove>.",
  "Live tail response body is unavailable.",
  "Revision conflict. Reload current state before preparing another change.",
  "Operator session expired.",
  "Destination management is not authorized.",
  "The change was not confirmed. Reload current state before retrying.",
  "Kafdeck request failed.",
  "The governed mutation request failed.",
  "The antiforgery token response was incomplete.",
] as const;
export type ApplicationProblemCopy = typeof applicationProblemCopies[number];
export type PresentationProblem = { copy: ApplicationProblemCopy; values?: readonly (string | number)[] | undefined } | { raw: string };

export function applicationProblem(copy: ApplicationProblemCopy, values?: readonly (string | number)[]): PresentationProblem {
  return { copy, values };
}

export function problemPresentation(problem: PresentationProblem): string {
  return 'copy' in problem ? t(problem.copy, problem.values) : problem.raw;
}

// Weak ownership metadata cannot be supplied by a provider response. It also
// lets API-generated fallback Errors retain their existing class and status.
const ownedErrors = new WeakMap<Error, PresentationProblem>();
export function markApplicationError<T extends Error>(error: T, copy: ApplicationProblemCopy, values?: readonly (string | number)[]): T {
  ownedErrors.set(error, applicationProblem(copy, values));
  return error;
}
export function ownedErrorPresentation(reason: unknown): PresentationProblem | null {
  return reason instanceof Error ? ownedErrors.get(reason) ?? null : null;
}
export class ApplicationCopyError extends Error {
  constructor(copy: ApplicationProblemCopy, values?: readonly (string | number)[]) {
    super(t(copy, values));
    this.name = 'ApplicationCopyError';
    markApplicationError(this, copy, values);
  }
}
