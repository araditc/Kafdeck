import { ownedErrorPresentation, problemPresentation, type PresentationProblem } from '../../shared/presentationProblem.js';
import { ApiProblem } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { readViewHttpStatusKind, type UiStatusKind } from '../../app/statusPresentation.js';

// Persist the application-owned identity, never a translation of backend prose.
export const readViewProblemMessages = {
  denied: 'Not authorized for this read view.',
  unconfigured: 'This read view is not configured.',
  unsupported: 'This read view is unsupported by the configured provider.',
  unavailable: 'The upstream read operation is currently unavailable.',
  failed: 'The read view could not be loaded.',
  sloInvalid: 'SLO threshold must be non-negative and target must be between 0 and 1.',
  configurationSyntax: 'Each configuration line must use key=value.',
  configurationEmptyKey: 'Configuration keys must not be empty.',
  configurationDuplicate: 'Duplicate configuration key: {0}',
  configurationEmpty: 'At least one configuration entry is required.',
  autoRestartStatus: 'Auto-restart policy status could not be loaded.',
  autoRestartPreview: 'Auto-restart policy preview failed.',
  autoRestartRefresh: 'Auto-restart operation refresh failed.',
  autoRestartApply: 'Auto-restart policy apply failed.',
} as const;

type ReadViewCopy = keyof typeof readViewProblemMessages;
export type ReadViewProblem = { kind: UiStatusKind } & (
  | { copy: ReadViewCopy; values?: readonly string[] | undefined }
  | { message: string }
  | { presentation: PresentationProblem }
);

export class ReadViewInputError extends Error {
  constructor(readonly copy: ReadViewCopy, readonly values?: readonly string[]) {
    super(readViewProblemMessages[copy]);
    this.name = 'ReadViewInputError';
  }
}

export function readViewProblemText(problem: ReadViewProblem): string {
  return 'copy' in problem ? t(readViewProblemMessages[problem.copy], problem.values) : 'presentation' in problem ? problemPresentation(problem.presentation) : problem.message;
}

export function readViewFailure(reason: unknown, fallback: ReadViewCopy = 'failed'): ReadViewProblem {
  if (reason instanceof ReadViewInputError) return { kind: 'unknown', copy: reason.copy, values: reason.values };
  const owned = ownedErrorPresentation(reason);
  if (owned) return { kind: 'unknown', presentation: owned };
  return reason instanceof Error ? { kind: 'unknown', message: reason.message } : { kind: 'unknown', copy: fallback };
}

export function readViewError(reason: unknown): ReadViewProblem {
  if (reason instanceof ApiProblem) {
    const kind = readViewHttpStatusKind(reason.status);
    if (reason.status === 401 || reason.status === 403) return { kind, copy: 'denied' };
    if (reason.status === 404) return { kind, copy: 'unconfigured' };
    if (reason.status === 501) return { kind, copy: 'unsupported' };
    if (reason.status === 502 || reason.status === 503 || reason.status === 504) return { kind, copy: 'unavailable' };
    const owned = ownedErrorPresentation(reason);
    return owned ? { kind, presentation: owned } : { kind, message: reason.message };
  }
  return readViewFailure(reason);
}
