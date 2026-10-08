/**
 * One operator-identity invalidation signal for both read and governed
 * mutation HTTP clients. Never reconstructs identity from a mutation result.
 */
export const operatorSessionLostEvent = 'kafdeck:operator-session-lost';

export function notifySessionLossIfUnauthorized(status: number): void {
  if (status !== 401 || typeof window === 'undefined') return;
  // Publish before parsing an error body so the UI cannot retain evidence
  // while a server response is being consumed.
  window.dispatchEvent(new Event(operatorSessionLostEvent));
}
