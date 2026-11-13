/** Foreground read budget, including JSON/body consumption; never retries a write. */
export const browserReadTimeoutMs = 15_000;

export async function boundedBrowserRead<T>(read: (signal: AbortSignal) => Promise<T>, caller?: AbortSignal,
  timeoutMs = browserReadTimeoutMs): Promise<T> {
  const controller = new AbortController();
  const cancel = () => controller.abort(caller?.reason);
  if (caller?.aborted) cancel();
  else caller?.addEventListener('abort', cancel, { once: true });
  const timer = setTimeout(() => controller.abort(new DOMException('Browser read deadline exceeded.', 'TimeoutError')), timeoutMs);
  try {
    controller.signal.throwIfAborted();
    return await read(controller.signal);
  } finally {
    clearTimeout(timer);
    caller?.removeEventListener('abort', cancel);
  }
}
