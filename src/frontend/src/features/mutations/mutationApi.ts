export type MutationRiskClass = 'low' | 'moderate' | 'high' | 'critical';
export type MutationConfirmationMode = 'explicit' | 'typedTarget';
export type MutationOperationState =
  | 'previewed'
  | 'awaitingConfirmation'
  | 'awaitingApproval'
  | 'ready'
  | 'executing'
  | 'appliedVerified'
  | 'appliedUnverified'
  | 'partiallyApplied'
  | 'executionUnknown'
  | 'rejected'
  | 'expired'
  | 'cancelled'
  | 'stalePreview'
  | 'failedBeforeDispatch'
  | 'failedDefinitive';

export interface MutationStatus {
  operationId: string;
  clusterId: string;
  operationKind: string;
  riskClass: MutationRiskClass;
  confirmationMode: MutationConfirmationMode;
  requiresIndependentApproval: boolean;
  state: MutationOperationState;
  previewHash: string;
  previewExpiresAtUtc: string;
  confirmationChallenge: string | null;
  resultCode: string | null;
  safeProviderEvidence: Record<string, string>;
  version: number;
  createdAtUtc: string;
  updatedAtUtc: string;
}

export interface MutationApprovalInbox {
  items: MutationStatus[];
}

export class MutationApiProblem extends Error {
  readonly status: number;
  readonly code: string | null;

  constructor(status: number, message: string, code: string | null) {
    super(message);
    this.name = 'MutationApiProblem';
    this.status = status;
    this.code = code;
  }
}

interface CsrfToken {
  requestToken: string;
  headerName: string;
}

let csrfToken: CsrfToken | null = null;

async function parseProblem(response: Response): Promise<MutationApiProblem> {
  let problem: { detail?: string; title?: string; code?: string; type?: string } = {};
  try {
    problem = (await response.json()) as typeof problem;
  } catch {
    // Never expose raw provider/server response bodies to the operator UI.
  }

  return new MutationApiProblem(
    response.status,
    problem.detail ?? problem.title ?? 'The governed mutation request failed.',
    problem.code ?? problem.type ?? null,
  );
}

async function readJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const init: RequestInit = {
    method: 'GET',
    headers: { Accept: 'application/json' },
    credentials: 'same-origin',
  };
  if (signal !== undefined) init.signal = signal;

  const response = await fetch(path, init);
  if (!response.ok) throw await parseProblem(response);
  return (await response.json()) as T;
}

async function csrf(signal?: AbortSignal): Promise<CsrfToken> {
  if (csrfToken !== null) return csrfToken;

  const token = await readJson<CsrfToken>('/api/v1/auth/csrf', signal);
  if (!token.requestToken || !token.headerName) {
    throw new MutationApiProblem(
      500,
      'The antiforgery token response was incomplete.',
      'urn:kafdeck:problem:antiforgery-token-invalid',
    );
  }

  csrfToken = token;
  return token;
}

async function postJson<T>(
  path: string,
  body: unknown,
  options?: { idempotencyKey?: string; signal?: AbortSignal },
): Promise<T> {
  const token = await csrf(options?.signal);
  const headers: Record<string, string> = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    [token.headerName]: token.requestToken,
  };
  if (options?.idempotencyKey) {
    headers['Idempotency-Key'] = options.idempotencyKey;
  }

  const init: RequestInit = {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    credentials: 'same-origin',
  };
  if (options?.signal !== undefined) init.signal = options.signal;

  const response = await fetch(path, init);
  if (!response.ok) {
    const problem = await parseProblem(response);
    if (problem.code === 'urn:kafdeck:problem:antiforgery-validation-failed') {
      csrfToken = null;
    }
    throw problem;
  }

  return (await response.json()) as T;
}

function mutationPath(operationId: string): string {
  return `/api/v1/mutations/${encodeURIComponent(operationId)}`;
}

export const mutationApi = {
  get(operationId: string, signal?: AbortSignal) {
    return readJson<MutationStatus>(mutationPath(operationId), signal);
  },

  listApprovals(limit = 50, signal?: AbortSignal) {
    const bounded = Math.max(1, Math.min(100, Math.trunc(limit)));
    return readJson<MutationApprovalInbox>(
      `/api/v1/mutations/approvals?limit=${bounded}`,
      signal,
    );
  },

  confirm(
    operation: MutationStatus,
    typedTargetChallenge: string | null,
    signal?: AbortSignal,
  ) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/confirm`,
      {
        previewHash: operation.previewHash,
        typedTargetChallenge,
      },
      { signal },
    );
  },

  approve(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/approve`,
      { previewHash: operation.previewHash },
      { signal },
    );
  },

  reject(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/reject`,
      { previewHash: operation.previewHash },
      { signal },
    );
  },

  cancel(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/cancel`,
      { previewHash: operation.previewHash },
      { signal },
    );
  },

  executeWithoutMaterial(operation: MutationStatus, signal?: AbortSignal) {
    return postJson<MutationStatus>(
      `${mutationPath(operation.operationId)}/execute`,
      {},
      { signal },
    );
  },
};
