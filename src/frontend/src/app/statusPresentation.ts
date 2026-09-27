export type UiStatusKind =
  | 'supported'
  | 'current'
  | 'denied'
  | 'unsupported'
  | 'blocked'
  | 'unconfigured'
  | 'partial'
  | 'stale'
  | 'unavailable'
  | 'unknown'
  | 'awaitingConfirmation'
  | 'awaitingApproval'
  | 'externalAction';

export type UiStatusPresentation = {
  label: string;
  badgeClass: string;
  description: string;
};

const presentations: Record<UiStatusKind, UiStatusPresentation> = {
  supported: {
    label: 'Supported',
    badgeClass: 'bg-green-lt',
    description: 'The capability is admitted and supported by current evidence.',
  },
  current: {
    label: 'Current',
    badgeClass: 'bg-green-lt',
    description: 'The observation is current within its bounded freshness contract.',
  },
  denied: {
    label: 'Denied',
    badgeClass: 'bg-red-lt',
    description: 'The current operator or upstream policy denied access.',
  },
  unsupported: {
    label: 'Unsupported',
    badgeClass: 'bg-secondary-lt',
    description: 'The configured provider does not support this admitted capability.',
  },
  blocked: {
    label: 'Blocked',
    badgeClass: 'bg-orange-lt',
    description: 'A lower-level primitive may exist, but Kafdeck will not activate it under the current safety contract.',
  },
  unconfigured: {
    label: 'Unconfigured',
    badgeClass: 'bg-yellow-lt',
    description: 'No configured provider/profile is available for this capability.',
  },
  partial: {
    label: 'Partial',
    badgeClass: 'bg-yellow-lt',
    description: 'Only part of the requested evidence is available.',
  },
  stale: {
    label: 'Stale',
    badgeClass: 'bg-yellow-lt',
    description: 'The evidence exists but is older than the current freshness boundary.',
  },
  unavailable: {
    label: 'Unavailable',
    badgeClass: 'bg-orange-lt',
    description: 'The capability is configured but the bounded observation could not complete.',
  },
  unknown: {
    label: 'Unknown',
    badgeClass: 'bg-blue-lt',
    description: 'There is not enough evidence to make a stronger claim.',
  },
  awaitingConfirmation: {
    label: 'Awaiting confirmation',
    badgeClass: 'bg-yellow-lt',
    description: 'A governed preview requires operator confirmation before it can advance.',
  },
  awaitingApproval: {
    label: 'Awaiting approval',
    badgeClass: 'bg-orange-lt',
    description: 'A governed operation requires eligible independent approval before it can advance.',
  },
  externalAction: {
    label: 'External action required',
    badgeClass: 'bg-red-lt',
    description: 'An ambiguous or unresolved external effect requires reconciliation and must not be blindly retried.',
  },
};

export function uiStatusPresentation(kind: UiStatusKind): UiStatusPresentation {
  return presentations[kind];
}


export function fleetCapabilityStatusKind(state: string): UiStatusKind {
  switch (state) {
    case 'supported': return 'supported';
    case 'unsupported': return 'unsupported';
    case 'blocked': return 'blocked';
    case 'unconfigured': return 'unconfigured';
    default: return 'unknown';
  }
}

export function readViewHttpStatusKind(status: number): UiStatusKind {
  switch (status) {
    case 401:
    case 403:
      return 'denied';
    case 404:
      return 'unconfigured';
    case 501:
      return 'unsupported';
    case 502:
    case 503:
    case 504:
      return 'unavailable';
    default:
      return 'unknown';
  }
}

export function mutationStateStatusKind(state: string): UiStatusKind {
  switch (state) {
    case 'awaitingConfirmation':
      return 'awaitingConfirmation';
    case 'awaitingApproval':
      return 'awaitingApproval';
    case 'executionUnknown':
    case 'partiallyApplied':
    case 'appliedUnverified':
      return 'externalAction';
    case 'stalePreview':
      return 'stale';
    case 'failedBeforeDispatch':
    case 'failedDefinitive':
    case 'rejected':
    case 'cancelled':
    case 'expired':
      return 'blocked';
    case 'appliedVerified':
      return 'current';
    default:
      return 'unknown';
  }
}
