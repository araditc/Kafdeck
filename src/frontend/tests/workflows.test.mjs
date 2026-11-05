import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {requestApi,attributionApi,validateRequestChange,validateMeasurementQuery,validateRateCard,parseWorkflowJson,decimalEvidence,assertRequestOperation,assertLinkedOperation,assertRateCardOperation} from '../dist/test-source/features/governance/workflowApi.js';
import {RequestPanel,requestBusinessOutcome} from '../dist/test-source/features/governance/RequestPanel.js';
import {AttributionPanel} from '../dist/test-source/features/governance/AttributionPanel.js';
import {GovernanceOperationReview} from '../dist/test-source/features/governance/GovernanceOperationReview.js';

const id='11111111-1111-4111-8111-111111111111',requestId='11111111111141118111111111111111',hash='a'.repeat(64);
const operation={operationId:id,clusterId:'alpha',operationKind:'serviceRequestChange',riskClass:'moderate',confirmationMode:'explicit',requiresIndependentApproval:false,requiresExecutionMaterial:false,state:'ready',previewHash:hash,previewExpiresAtUtc:'2026-11-05T12:00:00Z',confirmationChallenge:null,resultCode:null,safeProviderEvidence:{},version:1,createdAtUtc:'2026-11-05T11:00:00Z',updatedAtUtc:'2026-11-05T11:00:00Z'};
const draft={clusterId:'alpha',command:'Draft',expectedVersion:0,operationId:id,reason:'Controlled change',expiresAtUtc:'2026-11-05T12:00:00Z'};
const rate={metricKind:'trafficBytes',unit:'bytes',currency:'USD',amount:'0.0000000000000000000000000001',effectiveFromUtc:'2026-11-05T11:00:00Z',effectiveToUtc:'2026-11-05T12:00:00Z',weights:[]};

test('requests bind a pre-existing operation and exact CAS version; business revisions do not invent authority',()=>{
  assert.doesNotThrow(()=>validateRequestChange(draft));
  assert.doesNotThrow(()=>validateRequestChange({...draft,expiresAtUtc:'2026-11-05T12:00:00.1234567+00:00'}));
  assert.doesNotThrow(()=>validateRequestChange({clusterId:'alpha',command:'Approve',expectedVersion:2,requestId,reason:'Reviewed exact material'}));
  for(const change of [{...draft,expectedVersion:1},{...draft,operationId:'../execute'},{...draft,requestId},{...draft,reason:'x'.repeat(2049)},{...draft,expiresAtUtc:'2026-11-05T12:00:00+01:00'},{...draft,expiresAtUtc:'2026-02-30T12:00:00Z'},{...draft,command:'execute'},{...draft,requestApproved:true},{clusterId:'alpha',command:'Submit',expectedVersion:1,reason:'Missing request'}])assert.throws(()=>validateRequestChange(change));
});
test('frozen business change and linked effect have independent exact scope/hash binding',()=>{
  const value={plan:{planId:id,clusterId:'alpha',contentHash:hash},operation};
  assert.doesNotThrow(()=>assertRequestOperation(value,'alpha'));
  assert.throws(()=>assertRequestOperation({...value,operation:{...operation,operationKind:'topicDelete'}},'alpha'));
  assert.throws(()=>assertRequestOperation({...value,plan:{...value.plan,planId:'other'}},'alpha'));
  const request={operationId:id,previewHash:hash,clusterId:'alpha'};
  assert.doesNotThrow(()=>assertLinkedOperation(request,operation));
  assert.throws(()=>assertLinkedOperation(request,{...operation,previewHash:'b'.repeat(64)}));
  assert.throws(()=>assertLinkedOperation(request,{...operation,clusterId:'hidden'}));
});
test('business approved/expired/unknown semantics remain separate from critical effect controls',()=>{
  assert.match(requestBusinessOutcome('Approved'),/does not replace|still needs/);
  assert.match(requestBusinessOutcome('NeedsReplan'),/previous decisions cannot be reused/);
  assert.match(requestBusinessOutcome('Expired'),/cannot authorize/);
  assert.match(requestBusinessOutcome('Cancelled'),/does not assert rollback/);
  const html=renderToStaticMarkup(React.createElement(RequestPanel,{clusterId:'alpha'}));
  assert.match(html,/Business approval/);assert.match(html,/<fieldset[^>]*disabled=""/);assert.match(html,/not replace confirmation or independent critical approval/);
});
test('a ready kernel operation remains undispatchable while its current business requirement is unapproved',()=>{
  const html=renderToStaticMarkup(React.createElement(GovernanceOperationReview,{operation,material:{},busy:false,executionAllowed:false,executionBlockReason:'Current business approval required',update(){},execute(){throw Error('must not dispatch');},async run(){}}));
  assert.match(html,/Current business approval required/);assert.match(html,/<button[^>]*disabled=""[^>]*>Execute reviewed operation/);
});
test('measurement queries are exact, finite UTC windows; source URLs and aliases cannot select routes',()=>{
  const query={clusterId:'alpha',fromUtc:'2026-11-05T11:00:00Z',toUtc:'2026-11-05T12:00:00Z'};
  assert.doesNotThrow(()=>validateMeasurementQuery(query));
  for(const change of [{...query,clusterId:'../hidden'},{...query,toUtc:query.fromUtc},{...query,toUtc:'2027-11-05T12:00:00Z'},{...query,fromUtc:'2026-11-05T11:00:00'},{...query,resourceId:'x\n'}])assert.throws(()=>validateMeasurementQuery(change));
  assert.throws(()=>attributionApi.windows('../../proxy',query));
});
test('rate cards retain exact decimal evidence, registered units and real owner references',()=>{
  assert.doesNotThrow(()=>validateRateCard(rate));
  for(const change of [{...rate,amount:'-1'},{...rate,amount:'1e-4'},{...rate,amount:'1,25'},{...rate,currency:'usd'},{...rate,unit:'records'},{...rate,metricKind:'consumerLag'},{...rate,weights:[{teamId:'display label',applicationId:null,weight:'1'}]},{...rate,url:'https://evil.invalid/'}])assert.throws(()=>validateRateCard(change));
  assert.equal(decimalEvidence(null),'Unknown');assert.equal(decimalEvidence('0'),'0');assert.equal(decimalEvidence(rate.amount),rate.amount);
  const html=renderToStaticMarkup(React.createElement(AttributionPanel,{clusterId:'alpha'}));
  assert.match(html,/Missing evidence is unknown/);assert.match(html,/No equal split is invented/);assert.doesNotMatch(html,/Rounded total.*>0</);
});
test('original allocation JSON rejects escaped duplicate keys and authority fields before request preparation',()=>{
  for(const text of ['[{"teamId":null,"teamId":"other"}]','[{"weight":"1","\\u0077eight":"0"}]','['.repeat(13)+'0'+']'.repeat(13)])assert.throws(()=>parseWorkflowJson(text));
  assert.deepEqual(parseWorkflowJson('[{"teamId":null,"applicationId":"'+requestId+'","weight":"0.5"}]'),[{teamId:null,applicationId:requestId,weight:'0.5'}]);
});
test('request preview chooses the compiled decision authority route and retains CSRF/idempotency',async()=>{
  const original=globalThis.fetch,priorWindow=globalThis.window,calls=[];globalThis.window={location:{hash:''},history:{replaceState(){}}};
  globalThis.fetch=async(path,init)=>{calls.push({path,init});return new Response(JSON.stringify(path==='/api/v1/auth/csrf'?{headerName:'X-Kafdeck-CSRF',requestToken:'fixture'}:operation),{status:200});};
  try{
    await requestApi.preview(draft,'stable-draft');
    await requestApi.preview({clusterId:'alpha',command:'Approve',expectedVersion:2,requestId,reason:'Current scoped decision'},'stable-decision');
    const draftWire=calls.find(x=>x.path==='/api/v1/governance/requests/changes/preview'),decision=calls.find(x=>x.path==='/api/v1/governance/requests/decisions/preview');
    assert.equal(JSON.parse(draftWire.init.body).operationId,id);assert.equal(JSON.parse(decision.init.body).expectedVersion,2);
    assert.equal(decision.init.headers['Idempotency-Key'],'stable-decision');assert.equal(decision.init.headers['X-Kafdeck-CSRF'],'fixture');assert.equal(decision.init.credentials,'same-origin');
  }finally{globalThis.fetch=original;if(priorWindow===undefined)delete globalThis.window;else globalThis.window=priorWindow;}
});
test('request and rate-card recovery POSTs are bodyless and do not dispatch again',async()=>{
  const original=globalThis.fetch,priorWindow=globalThis.window,calls=[];globalThis.window={location:{hash:''},history:{replaceState(){}}};
  globalThis.fetch=async(path,init)=>{calls.push({path,init});return new Response(JSON.stringify(path==='/api/v1/auth/csrf'?{headerName:'X-Kafdeck-CSRF',requestToken:'fixture'}:operation),{status:200});};
  try{await requestApi.recover(operation);await attributionApi.recover({...operation,operationKind:'rateCardActivation'});const writes=calls.filter(x=>x.path.endsWith('/recover'));assert.equal(writes.length,2);for(const row of writes){assert.equal(row.init.method,'POST');assert.equal(row.init.body,undefined);assert.equal(row.init.credentials,'same-origin');assert.equal(row.init.headers['X-Kafdeck-CSRF'],'fixture');}assert.equal(calls.some(x=>x.path.endsWith('/execute')),false);}
  finally{globalThis.fetch=original;if(priorWindow===undefined)delete globalThis.window;else globalThis.window=priorWindow;}
});
test('measurement continuation preserves exact interval and filter; unknown totals are not converted to zero',async()=>{
  const original=globalThis.fetch,priorWindow=globalThis.window;globalThis.window={location:{hash:''},history:{replaceState(){}}};let path;globalThis.fetch=async input=>{path=input;return new Response(JSON.stringify({items:[],nextCursor:null,coverage:'Missing',status:'Unknown',totals:{currency:null,roundedAmount:null,unroundedAmount:null,status:'Unknown',complete:false}}),{status:200});};
  try{const result=await attributionApi.windows('dashboards',{clusterId:'alpha',fromUtc:'2026-11-05T11:00:00Z',toUtc:'2026-11-05T12:00:00Z',resourceId:'orders'},'signed+/=');const query=new URL(path,'https://fixture.invalid').searchParams;assert.equal(query.get('resourceId'),'orders');assert.equal(query.get('cursor'),'signed+/=');assert.equal(query.get('fromUtc'),'2026-11-05T11:00:00Z');assert.equal(result.totals.roundedAmount,null);}
  finally{globalThis.fetch=original;if(priorWindow===undefined)delete globalThis.window;else globalThis.window=priorWindow;}
});
test('rate-card inspection requires exact frozen operation scope and material',()=>{
  const value={operation:{...operation,operationKind:'rateCardActivation'},operationId:id,previewHash:hash,clusterId:'alpha',contentHash:hash};
  assert.doesNotThrow(()=>assertRateCardOperation(value,'alpha'));assert.throws(()=>assertRateCardOperation({...value,previewHash:'b'.repeat(64)},'alpha'));assert.throws(()=>assertRateCardOperation({...value,clusterId:'hidden'},'alpha'));
});
