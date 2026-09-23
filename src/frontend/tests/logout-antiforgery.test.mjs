import assert from 'node:assert/strict';
import test from 'node:test';
import { kafdeckApi } from '../dist/test-source/shared/api.js';

test('OIDC logout obtains antiforgery token and submits it in the top-level form POST', async () => {
  const originals = {
    fetch: globalThis.fetch,
    document: globalThis.document,
    sessionStorage: globalThis.sessionStorage,
    window: globalThis.window,
    history: globalThis.history,
  };

  const removedKeys = [];
  const fetchCalls = [];
  let submittedForm = null;

  const form = {
    method: '',
    action: '',
    hidden: false,
    children: [],
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    submit() {
      submittedForm = this;
    },
  };

  try {
    globalThis.sessionStorage = {
      getItem() { return null; },
      setItem() {},
      removeItem(key) { removedKeys.push(key); },
    };
    globalThis.window = {
      location: {
        hash: '',
        pathname: '/',
        search: '',
      },
    };
    globalThis.history = { replaceState() {} };
    globalThis.document = {
      body: {
        appendChild(node) {
          assert.equal(node, form);
          return node;
        },
      },
      createElement(tagName) {
        if (tagName === 'form') return form;
        if (tagName === 'input') {
          return { type: '', name: '', value: '' };
        }
        throw new Error(`Unexpected element ${tagName}`);
      },
    };
    globalThis.fetch = async (path, init) => {
      fetchCalls.push({ path, init });
      return {
        ok: true,
        async json() {
          return {
            requestToken: 'csrf-request-token',
            headerName: 'X-Kafdeck-CSRF',
            formFieldName: '__RequestVerificationToken',
          };
        },
      };
    };

    await kafdeckApi.logout();

    assert.deepEqual(removedKeys, ['kafdeck.deploymentAccessToken']);
    assert.equal(fetchCalls.length, 1);
    assert.equal(fetchCalls[0].path, '/api/v1/auth/csrf');
    assert.equal(fetchCalls[0].init.method, 'GET');
    assert.equal(form.method, 'POST');
    assert.equal(form.action, '/api/v1/auth/logout');
    assert.equal(form.hidden, true);
    assert.equal(form.children.length, 1);
    assert.deepEqual(form.children[0], {
      type: 'hidden',
      name: '__RequestVerificationToken',
      value: 'csrf-request-token',
    });
    assert.equal(submittedForm, form);
  } finally {
    globalThis.fetch = originals.fetch;
    globalThis.document = originals.document;
    globalThis.sessionStorage = originals.sessionStorage;
    globalThis.window = originals.window;
    globalThis.history = originals.history;
  }
});
