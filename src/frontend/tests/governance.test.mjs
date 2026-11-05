import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {catalogApi,policyApi,boundedScope,exactId,validateCatalogChanges,parseCatalogImport,governanceProblem} from '../dist/test-source/features/governance/governanceApi.js';
import {CatalogPanel,assertCatalogBinding} from '../dist/test-source/features/governance/CatalogPanel.js';
import {PolicyPanel,assertPolicyBinding} from '../dist/test-source/features/governance/PolicyPanel.js';
import {GovernanceOperationReview,governanceOperationActions,governanceOutcome} from '../dist/test-source/features/governance/GovernanceOperationReview.js';
import {MutationApiProblem} from '../dist/test-source/features/mutations/mutationApi.js';

const id='11111111-1111-4111-8111-111111111111', hash='a'.repeat(64);
const change={kind:'Team',id:null,expectedRevision:0,name:'Operators',description:null,archived:false,teamId:null,applicationId:null,topicName:null,owner:null,domain:null,classification:null,documentationReference:null,tags:[]};
const operation={operationId:id,clusterId:'alpha',operationKind:'catalogMutation',riskClass:'moderate',confirmationMode:'explicit',requiresIndependentApproval:false,requiresExecutionMaterial:false,state:'ready',previewHash:hash,previewExpiresAtUtc:'2026-11-05T01:00:00Z',confirmationChallenge:null,resultCode:null,safeProviderEvidence:{},version:1,createdAtUtc:'2026-11-05T00:00:00Z',updatedAtUtc:'2026-11-05T00:00:00Z'};
test('catalog and policy reject path aliases, invalid IDs and unsafe revisions before authority/network acquisition',()=>{
  for(const value of ['','..','a/b','a\\b','a?b','a#b','a\n'])assert.throws(()=>boundedScope(value));
  for(const value of ['../execute','', '00000000-0000-0000-0000-000000000000'])assert.throws(()=>exactId(value));
  assert.throws(()=>catalogApi.list('alpha','../../mutations'));
  assert.throws(()=>policyApi.previewActivation('alpha',id,-1,true,'key'));
  assert.throws(()=>policyApi.previewActivation('alpha',id,1.5,true,'key'));
});
test('atomic catalog import enforces count, referenced UUIDs and metadata bounds',()=>{
  validateCatalogChanges([change]);
  for(const changes of [[],Array(501).fill(change),[{...change,expectedRevision:-1}],[{...change,name:'x'.repeat(129)}],[{...change,teamId:'../team'}],[{...change,tags:Array(21).fill('x')}],[{...change,documentationReference:'javascript:alert(1)'}]])assert.throws(()=>validateCatalogChanges(changes));
  assert.throws(()=>validateCatalogChanges([{...change,kind:'Topic',topicName:null}]));
});
test('catalog previews bind exact operation, scope, registered family and frozen content',()=>{
  const preview={operation,planId:id,contentHash:hash,changes:[change]};
  assert.doesNotThrow(()=>assertCatalogBinding(preview,'alpha'));
  for(const modified of [{...preview,operation:{...operation,clusterId:'other'}},{...preview,operation:{...operation,operationKind:'topicDelete'}},{...preview,contentHash:'invalid'},{...preview,changes:[]}])assert.throws(()=>assertCatalogBinding(modified,'alpha'));
});
test('catalog import preserves valid optional defaults and rejects original duplicate or injected keys',()=>{
  const [item]=parseCatalogImport('[{"kind":"team","expectedRevision":0,"name":"گروه"}]');
  assert.equal(item.kind,'Team');assert.equal(item.id,null);assert.equal(item.description,null);assert.deepEqual(item.tags,[]);
  for(const document of ['[{"kind":"Team","expectedRevision":0,"name":"first","name":"last"}]','[{"kind":"Team","expectedRevision":0,"name":"first","\\u006eame":"last"}]','[{"kind":"Team","expectedRevision":0,"name":"ops","requesterPrincipalId":"admin"}]'])assert.throws(()=>parseCatalogImport(document));
  assert.doesNotThrow(()=>validateCatalogChanges([{...change,name:'ک'.repeat(128),description:'ک'.repeat(2048),tags:['ک'.repeat(64)]}]));
});
test('policy reopening binds immutable material to exact operation fingerprint and scope',()=>{
  const status={...operation,operationKind:'governancePolicyActivation'};
  const material={operationId:id,previewHash:hash,clusterId:'alpha',document:'{}',contentHash:hash};
  assert.doesNotThrow(()=>assertPolicyBinding(status,material,'alpha'));
  for(const modified of [{...material,previewHash:'b'.repeat(64)},{...material,clusterId:'other'},{...material,document:''}])assert.throws(()=>assertPolicyBinding(status,modified,'alpha'));
});
test('unknown, stale, incomplete and critical waiting states never offer execute',()=>{
  for(const state of ['executionUnknown','executing','stalePreview','awaitingApproval','partiallyApplied','appliedUnverified','expired'])assert.equal(governanceOperationActions({...operation,state},true,'').execute,false);
  assert.equal(governanceOperationActions(operation,false,'').execute,false);
  assert.equal(governanceOperationActions({...operation,requiresExecutionMaterial:true},true,'').execute,false);
  assert.equal(governanceOperationActions(operation,true,'').execute,true);
  assert.match(governanceOutcome('executionUnknown'),/Do not retry blindly/);
  assert.match(governanceOutcome('awaitingApproval'),/distinct eligible human/);
});
test('critical confirmation requires the exact server challenge without replacing independent approval',()=>{
  const critical={...operation,state:'awaitingConfirmation',riskClass:'critical',confirmationMode:'typedTarget',confirmationChallenge:'policy alpha',requiresIndependentApproval:true};
  assert.equal(governanceOperationActions(critical,true,'POLICY ALPHA').confirm,false);
  assert.equal(governanceOperationActions(critical,true,'policy alpha').confirm,true);
  assert.equal(governanceOperationActions(critical,true,'policy alpha').approve,false);
});
test('rendered governance starts disabled, labels finite import/history and current authority semantics',()=>{
  const catalog=renderToStaticMarkup(React.createElement(CatalogPanel,{clusterId:'alpha'}));
  const policy=renderToStaticMarkup(React.createElement(PolicyPanel,{clusterId:'alpha'}));
  assert.match(catalog,/Ownership catalog/);assert.match(catalog,/<fieldset[^>]*disabled=""/);assert.match(catalog,/maximum 500 changes \/ 1 MiB/);assert.match(catalog,/do not grant RBAC/);
  assert.match(policy,/Governance policy/);assert.match(policy,/<fieldset[^>]*disabled=""/);assert.match(policy,/Dry-run this fixture/);assert.match(policy,/critical governed operations/);
});
test('rendered immutable material is escaped and no unknown effect has an enabled dispatch control',()=>{
  const html=renderToStaticMarkup(React.createElement(GovernanceOperationReview,{operation:{...operation,state:'executionUnknown'},material:{name:'<img src=x onerror=alert(1)>'},busy:false,update(){},execute(){throw Error('must not execute');},async run(){}}));
  assert.doesNotMatch(html,/<img|onerror="/);assert.match(html,/&lt;img/);
  assert.match(html,/<button[^>]*disabled=""[^>]*>Execute reviewed operation/);
  assert.match(html,/Refresh same operation/);
});
test('governance problems distinguish current denial, stale material and unavailable authority without raw provider detail',()=>{
  assert.match(governanceProblem(new MutationApiProblem(403,'secret','ScopeDenied')),/not authorized/);
  assert.match(governanceProblem(new MutationApiProblem(409,'secret','PolicyVersionConflict')),/policy conflict/);
  assert.match(governanceProblem(new MutationApiProblem(503,'secret','GovernanceUnavailable')),/Execution remains blocked/);
  assert.doesNotMatch(governanceProblem(new MutationApiProblem(503,'secret','GovernanceUnavailable')),/secret/);
});
test('policy transport preserves raw duplicate keys for authoritative rejection and sends CSRF/idempotency',async()=>{
  const original=globalThis.fetch, priorWindow=globalThis.window, calls=[];
  globalThis.window={location:{hash:''},history:{replaceState(){}}};
  globalThis.fetch=async(path,init)=>{
    calls.push({path,init});
    if(path==='/api/v1/auth/csrf')return new Response(JSON.stringify({headerName:'X-Kafdeck-CSRF',requestToken:'fixture-token'}),{status:200});
    return new Response(JSON.stringify(operation),{status:200});
  };
  try{
    const document='{"schemaVersion":1,"schemaVersion":2,"rules":[]}';
    await policyApi.previewVersion('alpha',document,'stable-key');
    const request=calls.find(row=>row.path.endsWith('/policies/versions/preview'));
    assert.equal(JSON.parse(request.init.body).document,document);
    assert.equal(request.init.headers['Idempotency-Key'],'stable-key');
    assert.equal(request.init.headers['X-Kafdeck-CSRF'],'fixture-token');
    assert.equal(request.init.credentials,'same-origin');
  }finally{globalThis.fetch=original; if(priorWindow===undefined)delete globalThis.window;else globalThis.window=priorWindow;}
});
