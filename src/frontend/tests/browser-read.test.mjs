import assert from 'node:assert/strict';
import test from 'node:test';
import { boundedBrowserRead, browserReadTimeoutMs } from '../dist/test-source/shared/browserRead.js';
import { kafdeckApi } from '../dist/test-source/shared/api.js';

test('E01 a stalled read receives a finite deadline and no implicit retry', async () => {
  assert.equal(browserReadTimeoutMs, 15_000); let attempts = 0;
  await assert.rejects(boundedBrowserRead(signal => new Promise((_resolve, reject) => {
    attempts++; signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  }), undefined, 5), error => error.name === 'TimeoutError');
  assert.equal(attempts, 1);
});
test('E01 caller cancellation retains its reason and takes precedence over deadline', async () => {
  const caller = new AbortController(); const reason = new DOMException('Navigation changed.', 'AbortError');
  const pending = boundedBrowserRead(signal => new Promise((_resolve, reject) => {
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  }), caller.signal, 1000);
  caller.abort(reason); await assert.rejects(pending, error => error === reason);
});
test('E01 already cancelled reads never dispatch', async () => {
  const caller = new AbortController(); caller.abort(); let calls = 0;
  await assert.rejects(boundedBrowserRead(async () => { calls++; }, caller.signal), error => error.name === 'AbortError');
  assert.equal(calls, 0);
});
test('E01 fast success clears its timer and caller listener', async () => {
  const caller = new AbortController(); let observed;
  assert.equal(await boundedBrowserRead(async signal => { observed = signal; return 7; }, caller.signal, 5), 7);
  caller.abort(); await new Promise(resolve => setTimeout(resolve, 10)); assert.equal(observed.aborted, false);
});
test('E01 stalled error bodies produce an actionable client timeout without another request', async t => {
  t.mock.timers.enable({apis:['setTimeout']});
  const oldWindow=globalThis.window,oldFetch=globalThis.fetch;let requests=0;
  globalThis.window={location:{hash:''}};
  globalThis.fetch=async (_url,init)=>{requests++;return {ok:false,status:502,json:()=>new Promise((_resolve,reject)=>init.signal.addEventListener('abort',()=>reject(init.signal.reason),{once:true}))};};
  try {
    const pending=kafdeckApi.getOperatorSession();await Promise.resolve();await Promise.resolve();
    t.mock.timers.tick(browserReadTimeoutMs);
    await assert.rejects(pending,error=>error.status===408&&error.code==='urn:kafdeck:problem:browser-read-timeout');
    assert.equal(requests,1);
  } finally {globalThis.window=oldWindow;globalThis.fetch=oldFetch;}
});
