import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {automationApi,automationOutcome,validateDeclarativeInput} from '../dist/test-source/features/automation/automationApi.js';
import {AutomationPanel} from '../dist/test-source/features/automation/AutomationPanel.js';

const request={manifest:'{}',format:'json',sourceRef:'registered-source',sourceRevision:'a'.repeat(64),expectedOwnershipRevision:0};
test('automation requests reject invalid identities and bounds before network',async()=>{
  let calls=0; const original=globalThis.fetch;globalThis.fetch=async()=>{calls++;throw new Error('Must not send');};
  try {
    for(const change of [{manifest:'x'.repeat(16385)},{sourceRef:'../source'},{sourceRevision:'secret'},{expectedOwnershipRevision:-1},{expectedOwnershipRevision:0.5}])assert.throws(()=>validateDeclarativeInput({...request,...change}));
    assert.throws(()=>automationApi.show('../../execute'));
    assert.throws(()=>automationApi.plan(request,'',false));
    assert.equal(calls,0);
  }finally{globalThis.fetch=original;}
});
test('unknown and pending outcomes never advertise successful retry or automatic approval',()=>{
  assert.match(automationOutcome('ExecutionUnknown'),/No blind retry/);
  assert.match(automationOutcome('awaitingApproval'),/distinct eligible human/);
  assert.match(automationOutcome('AppliedVerified'),/backend verified/);
});
test('rendered automation starts fail-closed with labelled source, ownership, approval and client handoff',()=>{
  const html=renderToStaticMarkup(React.createElement(AutomationPanel,{notificationsAvailable:true}));
  assert.match(html,/id="automation"/); assert.match(html,/aria-labelledby="automation-title"/);
  assert.match(html,/<fieldset disabled=""/); assert.match(html,/Expected ownership revision/);
  assert.match(html,/Stable request idempotency key/); assert.match(html,/No secret or bearer-token entry/);
  assert.match(html,/CLI, Terraform and MCP operator handoff/); assert.match(html,/href="#notifications"/);
  assert.doesNotMatch(html,/dangerouslySetInnerHTML|type="password"/);
});
