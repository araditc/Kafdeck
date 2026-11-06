import assert from 'node:assert/strict';
import test, {afterEach} from 'node:test';
import fs from 'node:fs';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {translate, setLocale, getLocale, normalizeLocale, initializeLocale, localePreferenceKey} from '../dist/test-source/shared/i18n.js';
import {messages} from '../dist/test-source/shared/locales/messages.js';
import {CatalogPanel} from '../dist/test-source/features/governance/CatalogPanel.js';
import {RequestPanel, requestBusinessOutcome} from '../dist/test-source/features/governance/RequestPanel.js';
import {GovernanceOperationReview, governanceOperationActions, governanceOutcome} from '../dist/test-source/features/governance/GovernanceOperationReview.js';
import {filterProductCommands} from '../dist/test-source/app/commandPaletteModel.js';
import {decimalEvidence} from '../dist/test-source/features/governance/workflowApi.js';
import {uiStatusPresentation} from '../dist/test-source/app/statusPresentation.js';

afterEach(() => setLocale('en', false));
const id='11111111-1111-4111-8111-111111111111', hash='a'.repeat(64);
const operation={operationId:id,clusterId:'alpha',operationKind:'catalogMutation',riskClass:'critical',confirmationMode:'typedTarget',requiresIndependentApproval:true,requiresExecutionMaterial:false,state:'awaitingConfirmation',previewHash:hash,previewExpiresAtUtc:'2026-11-05T01:00:00Z',confirmationChallenge:'alpha <exact> 012.50',resultCode:'Invariant.Result',safeProviderEvidence:{code:'Evidence.123'},version:1,createdAtUtc:'2026-11-05T00:00:00Z',updatedAtUtc:'2026-11-05T00:00:00Z'};
function review(status){return renderToStaticMarkup(React.createElement(GovernanceOperationReview,{operation:status,material:{amount:'012.5000',timestamp:'2026-11-05T01:00:00Z',name:'<img src=x onerror="attack()">'},busy:false,update(){},async execute(){throw Error('must not execute');},async run(){}}));}

test('translation resources have complete matching keys, placeholders and runtime values',()=>{
  const en=JSON.parse(fs.readFileSync(new URL('../src/shared/locales/en.json',import.meta.url),'utf8'));
  const fa=JSON.parse(fs.readFileSync(new URL('../src/shared/locales/fa.json',import.meta.url),'utf8'));
  assert.ok(Object.keys(en).length>1200);
  assert.deepEqual(Object.keys(en).sort(),Object.keys(fa).sort());assert.deepEqual(fa,messages);
  for(const [key,value] of Object.entries(en)){
    assert.equal(value,key);assert.ok(fa[key].trim(),key);
    assert.deepEqual((key.match(/\{\d+\}/g)||[]).sort(),(fa[key].match(/\{\d+\}/g)||[]).sort(),key);
    assert.doesNotMatch(fa[key],/[\u202a-\u202e\u2066-\u2069]/,key);
  }
});

test('English fallback handles unsupported locales, absent keys and prototype names as plain text',()=>{
  assert.equal(normalizeLocale('fa'),'fa');for(const invalid of ['fa-IR','ar',null,{},'constructor'])assert.equal(normalizeLocale(invalid),'en');
  for(const key of ['Unreviewed future message','constructor','__proto__'])assert.equal(translate('fa',key),key);
  assert.equal(translate('en','Operation ID'),'Operation ID');assert.equal(translate('fa','Operation ID'),'شناسه عملیات');
});

test('interpolation escapes through React and isolates exact invariant values only in RTL',()=>{
  const value='<img src=x onerror="attack()"> 012.5000 2026-11-05T01:00:00Z';
  const translated=translate('fa','Duplicate key: {0}',[value]);assert.ok(translated.includes('\u2068'+value+'\u2069'));
  const html=renderToStaticMarkup(React.createElement('span',null,translated));assert.doesNotMatch(html,/<img/);assert.match(html,/&lt;img/);assert.match(html,/012\.5000/);
  assert.equal(translate('en','Duplicate key: {0}',[value]),'Duplicate key: '+value);
});

test('Persian catalog/request labels preserve API option values and escaped scope identity',()=>{
  setLocale('fa',false);
  const scope='alpha<img src=x>';
  const catalog=renderToStaticMarkup(React.createElement(CatalogPanel,{clusterId:scope}));
  const request=renderToStaticMarkup(React.createElement(RequestPanel,{clusterId:scope}));
  assert.match(catalog,/کاتالوگ مالکیت/);assert.match(request,/درخواست‌های خدمت و تأییدها/);
  assert.match(catalog,/<option value="teams" selected="">تیم‌ها<\/option>/);
  assert.match(request,/<option value="Draft" selected="">پیش‌نویس<\/option>/);
  for(const html of [catalog,request]){assert.match(html,/alpha&lt;img src=x&gt;/);assert.doesNotMatch(html,/<img src=x>/);}
});

test('locale never changes exact confirmation, approval or unknown dispatch decisions',()=>{
  const cases=[];
  for(const state of ['previewed','awaitingConfirmation','awaitingApproval','ready','executionUnknown','executing','stalePreview','partiallyApplied','expired']){
    for(const reviewed of [false,true])for(const challenge of [operation.confirmationChallenge,'ALPHA <exact> 012.50','alpha <exact> 12.5'])cases.push({...operation,state,reviewed,challenge});
  }
  setLocale('en',false);const english=cases.map(c=>governanceOperationActions(c,c.reviewed,c.challenge));
  setLocale('fa',false);assert.deepEqual(cases.map(c=>governanceOperationActions(c,c.reviewed,c.challenge)),english);
  assert.equal(governanceOperationActions({...operation,state:'executionUnknown'},true,operation.confirmationChallenge).execute,false);
  assert.match(governanceOutcome('executionUnknown'),/کورکورانه تکرار نکنید/);
  assert.match(requestBusinessOutcome('needsReplan'),/تصمیم‌های قبلی قابل استفاده مجدد نیستند/);
});

test('frozen approval material, identifiers, amounts, timestamps and challenges remain identical across locales',()=>{
  setLocale('en',false);const en=review(operation);
  setLocale('fa',false);const fa=review(operation);
  const material=html=>[...html.matchAll(/<pre[^>]*>([\s\S]*?)<\/pre>/g)].map(match=>match[1]);assert.deepEqual(material(en),material(fa));
  for(const html of [en,fa]){assert.ok(html.includes(id));assert.ok(html.includes(hash));assert.ok(html.includes('2026-11-05T01:00:00Z'));assert.ok(html.includes('alpha &lt;exact&gt; 012.50'));assert.ok(html.includes('Invariant.Result'));assert.match(html,/dir="ltr" lang="en"/);assert.doesNotMatch(html,/<img src=x/);}
  assert.match(fa,/تأیید مستقل انسانی/);
});

test('Persian command search and status descriptions follow current locale after module initialization',()=>{
  setLocale('en',false);assert.equal(uiStatusPresentation('unknown').label,'Unknown');
  setLocale('fa',false);assert.equal(uiStatusPresentation('unknown').label,'نامعلوم');
  assert.ok(filterProductCommands('کاتالوگ مالکیت').some(command=>command.id==='catalog'));
  assert.ok(filterProductCommands('تولیدکننده').some(command=>command.id==='generator'));
});

test('language storage persists only the preference and never acquires identity or moves focus',()=>{
  const oldWindow=globalThis.window,oldDocument=globalThis.document;
  const writes=[],events=new Map();let saved='fa';
  globalThis.document={documentElement:{lang:'en',dir:'ltr'},activeElement:{focus(){throw Error('locale moved focus');}}};
  globalThis.window={localStorage:{getItem(key){assert.equal(key,localePreferenceKey);return saved;},setItem(key,value){writes.push([key,value]);}},addEventListener(name,fn){events.set(name,fn);}};
  try {
    initializeLocale();assert.equal(getLocale(),'fa');assert.deepEqual(writes,[]);assert.equal(document.documentElement.dir,'rtl');
    setLocale('en');assert.deepEqual(writes,[[localePreferenceKey,'en']]);assert.equal(document.documentElement.lang,'en');
    events.get('storage')({key:'operator-token',newValue:'fa'});assert.equal(getLocale(),'en');
    events.get('storage')({key:localePreferenceKey,newValue:'fa'});assert.equal(getLocale(),'fa');assert.equal(writes.length,1);
    saved='unknown';initializeLocale();assert.equal(getLocale(),'en');
    window.localStorage={getItem(){throw Error('disabled');},setItem(){throw Error('disabled');}};assert.doesNotThrow(()=>initializeLocale());assert.doesNotThrow(()=>setLocale('fa'));
  } finally { if(oldWindow===undefined)delete globalThis.window;else globalThis.window=oldWindow;if(oldDocument===undefined)delete globalThis.document;else globalThis.document=oldDocument; }
});

test('source message coverage includes dynamic application descriptors and every literal translation call',()=>{
  const sourceRoot=new URL('../src/',import.meta.url);
  function walk(directory){return fs.readdirSync(directory,{withFileTypes:true}).flatMap(entry=>entry.isDirectory()?walk(new URL(entry.name+'/',directory)):/\.tsx?$/.test(entry.name)&&entry.name!=='messages.ts'?[new URL(entry.name,directory)]:[]);}
  let calls=0;
  for(const file of walk(sourceRoot)){
    const source=fs.readFileSync(file,'utf8');
    for(const match of source.matchAll(/\bt\(\s*("(?:[^"\\]|\\.)*")/g)){const key=JSON.parse(match[1]);assert.ok(Object.hasOwn(messages,key),file.pathname+': '+key);calls++;}
  }
  assert.ok(calls>1400);
  // These descriptors are allocated outside React; translated rendering/search
  // must follow the current locale, including after initial English import.
  setLocale('fa',false);
  for(const kind of ['supported','current','denied','unsupported','blocked','unconfigured','partial','stale','unavailable','unknown','awaitingConfirmation','awaitingApproval','externalAction']){
    const value=uiStatusPresentation(kind);assert.match(value.label,/[\u0600-\u06ff]/);assert.match(value.description,/[\u0600-\u06ff]/);
  }
  for(const command of filterProductCommands('')){assert.match(translate('fa',command.description),/[\u0600-\u06ff]/,command.id);}
});

test('locale preference storage is a closed enum even when callers supply credentials or payload-shaped values',()=>{
  const oldWindow=globalThis.window,oldDocument=globalThis.document;
  const writes=[];
  globalThis.window={localStorage:{setItem(key,value){writes.push([key,value]);}}};
  globalThis.document={documentElement:{}};
  try {
    for(const value of ['fa','en','Bearer sensitive-token','{"payload":"secret"}',{locale:'fa',token:'secret'},null,1,'fa\nsecret','fa-IR'])setLocale(value);
    assert.deepEqual(writes.map(([key])=>key),Array(9).fill('kafdeck.locale.v1'));
    assert.deepEqual(writes.map(([,value])=>value),['fa','en','en','en','en','en','en','en','en']);
    assert.doesNotMatch(JSON.stringify(writes),/secret|Bearer|payload/);
  } finally { if(oldWindow===undefined)delete globalThis.window;else globalThis.window=oldWindow;if(oldDocument===undefined)delete globalThis.document;else globalThis.document=oldDocument; }
});


test('Persian critical guidance distinguishes another human, nontransferable decisions and unknown effects',()=>{
  setLocale('fa',false);
  assert.match(translate('fa','Distinct eligible human approval'),/انسان دیگر/);
  assert.match(translate('fa','A current scoped human distinct from the requester must decide this exact revision.'),/غیر از درخواست‌کننده/);
  assert.match(translate('fa','Keep the same key and operation ID after an uncertain response. Approve/reject requires a current scoped human distinct from the requester.'),/همان کلید و شناسه عملیات/);
  assert.match(requestBusinessOutcome('needsReplan'),/تصمیم‌های قبلی قابل استفاده مجدد نیستند/);
  assert.match(requestBusinessOutcome('expired'),/تصمیم منقضی مجوز اثر نمی‌دهد/);
  assert.match(governanceOutcome('executionUnknown'),/کورکورانه تکرار نکنید/);
  assert.equal(decimalEvidence(null),'نامعلوم');
  assert.equal(decimalEvidence('12345678912345.012300'),'12345678912345.012300');
  assert.match(translate('fa','{0}: runtime {1}',['v0.9',translate('fa','disabled')]),/غیرفعال/);
  assert.match(translate('fa',' · Truncated: {0}',[translate('fa','server limit')]),/سقف سرور/);
});
