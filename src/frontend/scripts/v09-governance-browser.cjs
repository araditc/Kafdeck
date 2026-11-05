// Compiled UI in actual Chromium with explicit API fixtures. This is NOT genuine identity/database/effect evidence.
const {createServer}=require('node:http');
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..');
const sourceIdentity=require('./governance-browser-source.cjs');
const {chromium}=require(path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
const dist=path.join(root,'src/frontend/dist'),evidence=process.env.KAFDECK_V09_UI_EVIDENCE||path.join(root,'artifacts/v09/governance-ui-evidence.json');
const hash='a'.repeat(64),now='2026-11-05T00:00:00Z';
const ids={catalog:'11111111-1111-4111-8111-111111111111',version:'22222222-2222-4222-8222-222222222222',activation:'33333333-3333-4333-8333-333333333333',wrong:'44444444-4444-4444-8444-444444444444',versionId:'5'.repeat(64)};
const document='{"schemaVersion":1,"rules":[]}';
const activeDocument=JSON.stringify({schemaVersion:1,rules:[{id:'deny-create',priority:1,scope:{clusterId:'alpha',resourceKind:'topic',resourceIds:['orders'],action:'TopicCreate'},conditions:[],outcome:'Deny'}]});
const server=createServer((req,res)=>{
  const name=decodeURIComponent(new URL(req.url,'http://localhost').pathname),file=path.resolve(dist,name==='/'?'index.html':'.'+name);
  if(!file.startsWith(dist+path.sep)||!fs.existsSync(file)||!fs.statSync(file).isFile()){res.writeHead(404);res.end();return;}
  res.writeHead(200,{'Content-Type':({'.html':'text/html','.js':'text/javascript','.css':'text/css','.woff2':'font/woff2'})[path.extname(file)]||'application/octet-stream'});fs.createReadStream(file).pipe(res);
});
(async()=>{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  const browser=await chromium.launch({headless:true,channel:'chromium'});
  const operations=new Map();let effects=0,authenticated=true,denied=false,csrf=0,imports=0,approverPrivateStatusAttempts=0,reconciliations=0;const outside=[];
  const status=(id,kind,critical=false)=>({operationId:id,clusterId:'alpha',operationKind:kind,riskClass:critical?'critical':'moderate',confirmationMode:critical?'typedTarget':'explicit',requiresIndependentApproval:critical,requiresExecutionMaterial:false,state:'previewed',previewHash:hash,previewExpiresAtUtc:'2099-11-05T01:00:00Z',confirmationChallenge:critical?'activate alpha':null,resultCode:null,safeProviderEvidence:{},version:1,createdAtUtc:now,updatedAtUtc:now});
  const preview=(op,changes)=>({operation:op,planId:op.operationId,contentHash:hash,changes,expiresAtUtc:op.previewExpiresAtUtc});
  try{
    const context=await browser.newContext({viewport:{width:1440,height:1000}});
    await context.addInitScript(()=>{
      const original=window.fetch;
      window.fetch=function(input,init){
        const target=typeof input==='string'?input:input instanceof URL?input.href:input.url;
        if(target.includes('/governance/policies/operations/')&&target.endsWith('/reconcile')){
          window.__policyRecoveryWire={bodyUndefined:init?.body===undefined,sameOriginCredentials:init?.credentials==='same-origin',antiforgeryHeader:new Headers(init?.headers).has('X-Kafdeck-CSRF')};
        }
        return original.call(this,input,init);
      };
    });
    const fixtureRoute=async route=>{
      const request=route.request(),url=new URL(request.url()),p=url.pathname;
      const fixtureApprover=request.headers()['x-kafdeck-ui-fixture']==='approver';
      if(url.origin!==origin){outside.push(url.origin);return route.abort();}
      if(!p.startsWith('/api/v1/'))return route.continue();
      const send=(value,code=200)=>route.fulfill({status:code,contentType:'application/json',body:JSON.stringify(value)});
      if(p==='/api/v1/auth/session')return send(authenticated?{authenticated:true,authenticationMode:'oidc',displayName:'UI fixture',email:'fixture@example.invalid',authenticatedAt:now}:{title:'Authentication required'},authenticated?200:401);
      if(p==='/api/v1/auth/csrf')return send({headerName:'X-Kafdeck-CSRF',requestToken:'fixture-csrf'});
      if(p==='/api/v1/clusters')return send({data:[{data:{clusterId:'alpha',kafkaClusterId:'fixture',brokers:[],health:'healthy',healthReasons:[],failureCode:null,controllerBrokerId:null},observation:{observedAt:now,freshness:'fresh',cacheAgeMs:0,partial:false},limitations:[]}]});
      if(p==='/api/v1/clusters/alpha/topics')return send({data:{items:[],nextCursor:null}});
      if(p.includes('/governance/')&&denied)return send({title:'Denied',code:'ScopeDenied'},403);
      if(request.method()==='POST'&&p.includes('/governance/')){assert.equal(request.headers()['x-kafdeck-csrf'],'fixture-csrf');csrf++;}
      if(p.match(/\/governance\/catalog\/(teams|applications|topics)$/))return send({items:[],nextCursor:null});
      if(p.endsWith('/catalog/teams/preview')||p.endsWith('/catalog/imports/preview')){
        const body=request.postDataJSON();if(p.endsWith('/imports/preview'))imports++;
        const op=status(ids.catalog,'catalogMutation');operations.set(op.operationId,{op,changes:body.changes});return send(preview(op,body.changes));
      }
      if(p.includes('/catalog/operations/')){const id=p.split('/').pop();if(id===ids.wrong)return send(preview({...status(id,'catalogMutation'),clusterId:'hidden'},[{name:'hidden'}]));const value=operations.get(id);return send(preview(value.op,value.changes));}
      if(p.endsWith('/policies/versions'))return send({items:[{versionId:ids.versionId,clusterId:'alpha',contentHash:hash,document:activeDocument,createdBy:'fixture',createdAtUtc:now}],nextCursor:null});
      if(p.endsWith('/policies/active'))return send({clusterId:'alpha',revision:1,enabled:true,versionId:ids.versionId,contentHash:hash,digest:hash});
      if(p.endsWith('/policies/validate')){assert.deepEqual(Object.keys(request.postDataJSON()).sort(),['clusterId','document']);assert.equal(request.postDataJSON().clusterId,'alpha');return send({valid:true,contentHash:hash,document});}
      if(p.endsWith('/policies/explain')){assert.equal(request.postDataJSON().action,'TopicCreate');return send({allowed:false,code:'GovernanceDenied',digest:hash,matchedRuleIds:['deny-create']});}
      if(p.endsWith('/policies/dry-run')){assert.equal(request.postDataJSON().fixtures[0].action,'TopicCreate');return send({items:[{allowed:false,code:'GovernanceDenied',digest:hash,matchedRuleIds:['deny-create']}]});}
      if(p.endsWith('/policies/versions/preview')||p.endsWith('/policies/activations/preview')){
        const activation=p.includes('/activations/'),op=status(activation?ids.activation:ids.version,activation?'governancePolicyActivation':'governancePolicyVersion',activation);operations.set(op.operationId,{op});return send(op);
      }
      if(p.includes('/policies/operations/')&&p.endsWith('/reconcile')){
        assert.equal(request.method(),'POST');assert.equal(request.postData(),null);
        const op=operations.get(p.split('/').at(-2)).op;assert.equal(op.state,'executionUnknown');op.state='appliedVerified';reconciliations++;return send(op);
      }
      if(p.includes('/policies/operations/')){const op=operations.get(p.split('/').pop()).op;return send({operation:op,operationId:op.operationId,previewHash:op.previewHash,kind:op.operationKind,clusterId:'alpha',versionId:ids.versionId,contentHash:hash,document:op.operationKind==='governancePolicyActivation'?activeDocument:document,expectedRevision:1,enabled:true});}
      if(p.startsWith('/api/v1/mutations/')){
        const parts=p.split('/'),id=parts[4],value=operations.get(id);if(!value)return send({title:'Unavailable'},404);
        if(parts.length===5){if(fixtureApprover){approverPrivateStatusAttempts++;return send({title:'Requester-only status'},403);}return send(value.op);}
        assert.equal(request.headers()['x-kafdeck-csrf'],'fixture-csrf');
        if(parts[5]==='confirm')value.op.state=value.op.requiresIndependentApproval?'awaitingApproval':'ready';
        if(parts[5]==='approve'){if(!fixtureApprover)return send({title:'Distinct human required'},403);value.op.state='ready';}
        if(parts[5]==='execute'){assert.equal(value.op.state,'ready');effects++;value.op.state='appliedVerified';}
        return send(value.op);
      }
      return send({title:'Fixture endpoint unavailable'},403);
    };
    await context.route('**/*',fixtureRoute);
    const page=await context.newPage();await page.goto(origin);
    const catalog=page.locator('#ownership-catalog'),policy=page.locator('#governance-policy');
    await catalog.getByText('No authorized entries in this page.',{exact:true}).waitFor();
    await catalog.getByLabel('Name',{exact:true}).fill('<img src=x onerror="window.__unsafe=true">');
    await catalog.getByRole('button',{name:'Preview exact metadata change',exact:true}).click();
    const catalogReview=catalog.getByRole('article',{name:'Immutable governance operation'});await catalogReview.waitFor();
    assert.equal(await page.locator('img[src="x"]').count(),0);assert.equal(await page.evaluate(()=>window.__unsafe),undefined);
    await catalogReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await catalogReview.getByRole('button',{name:'Confirm exact preview',exact:true}).click();
    await catalogReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).click();
    await catalogReview.getByText('appliedVerified',{exact:false}).waitFor();assert.equal(effects,1);
    operations.get(ids.catalog).op.state='executionUnknown';await catalogReview.getByRole('button',{name:'Refresh same operation',exact:true}).click();
    await catalogReview.getByText(/Execution outcome is unknown/).waitFor();assert.equal(await catalogReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).isDisabled(),true);
    await catalog.getByLabel('Typed catalog changes JSON').fill(JSON.stringify(Array(501).fill({kind:'Team'})));
    await catalog.getByRole('button',{name:'Preview atomic import',exact:true}).click();await catalog.getByText('Supply 1–500 typed catalog changes.',{exact:true}).waitFor();assert.equal(imports,0);
    await catalog.getByLabel('Existing catalog operation ID').fill(ids.wrong);await catalog.getByRole('button',{name:'Inspect frozen catalog operation',exact:true}).click();
    await catalog.getByText(/does not match this scope/).waitFor();assert.equal(await catalog.getByRole('article',{name:'Immutable governance operation'}).count(),0);
    await policy.getByLabel('Closed schema JSON (maximum 256 KiB)').fill(document);await policy.getByRole('button',{name:'Validate closed policy',exact:true}).click();await policy.getByRole('region',{name:'Policy validation result'}).waitFor();
    console.log('Governance fixture probe: active-version-dt');
    const currentVersion=(await policy.locator('dt').filter({hasText:/^Active version$/}).locator('xpath=following-sibling::dd[1]').textContent()).trim();assert.equal(currentVersion,ids.versionId);
    console.log('Governance fixture probe: active-version-select');await policy.getByLabel('Immutable policy version').selectOption(currentVersion);
    console.log('Governance fixture probe: selected-version-summary');
    const visibleDocument=await policy.getByText('Selected immutable version',{exact:true}).locator('..').locator('pre').textContent();
    console.log('Governance fixture probe: selected-version-json');assert.equal(JSON.parse(visibleDocument).rules[0].outcome,'Deny');
    console.log('Governance fixture resource-kind labels:',JSON.stringify({exact:await policy.getByLabel('Resource kind',{exact:true}).count(),partial:await policy.getByLabel('Resource kind').count()}));
    await policy.getByLabel('Resource kind').selectOption('topic');
    await policy.getByLabel('Exact resource ID',{exact:true}).fill('orders');await policy.getByLabel('Registered action',{exact:true}).fill('TopicCreate');
    await policy.getByRole('button',{name:'Preview governed version write',exact:true}).click();
    const policyReview=policy.getByRole('article',{name:'Immutable governance operation'});await policyReview.waitFor();
    await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();await policyReview.getByRole('button',{name:'Confirm exact preview',exact:true}).click();await policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).click();
    await policyReview.getByText('appliedVerified',{exact:false}).waitFor();
    await policy.getByLabel('Immutable policy version').selectOption(ids.versionId);await policy.getByRole('button',{name:'Preview critical activation change',exact:true}).click();
    await policyReview.getByText(/Critical policy activation/).waitFor();await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    const confirm=policyReview.getByRole('button',{name:'Confirm exact preview',exact:true});assert.equal(await confirm.isDisabled(),true);
    await policyReview.getByLabel('Exact confirmation challenge:',{exact:false}).fill('activate alpha');await confirm.click();
    await policyReview.getByRole('button',{name:'Submit independent human approval',exact:true}).click();await policy.getByText(/not authorized for this scope/).waitFor();
    assert.equal(await policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).isDisabled(),true);
    const approverContext=await browser.newContext({viewport:{width:1440,height:1000},extraHTTPHeaders:{'X-Kafdeck-UI-Fixture':'approver'}});
    await approverContext.route('**/*',fixtureRoute);const approver=await approverContext.newPage();await approver.goto(origin);
    const approvalPanel=approver.locator('#governance-policy');await approvalPanel.getByRole('heading',{name:'Governance policy',exact:true}).waitFor();
    await approvalPanel.getByLabel('Existing policy operation ID').fill(ids.activation);await approvalPanel.getByRole('button',{name:'Inspect frozen policy operation',exact:true}).click();
    const approvalReview=approvalPanel.getByRole('article',{name:'Immutable governance operation'});await approvalReview.waitFor();
    await approvalReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await approvalReview.getByRole('button',{name:'Submit independent human approval',exact:true}).click();await approvalReview.getByText('ready',{exact:false}).waitFor();
    await page.goto(origin);await policy.getByRole('heading',{name:'Governance policy',exact:true}).waitFor();
    await policy.getByLabel('Existing policy operation ID').fill(ids.activation);await policy.getByRole('button',{name:'Inspect frozen policy operation',exact:true}).click();await policyReview.waitFor();
    await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).click();await policyReview.getByText('appliedVerified',{exact:false}).waitFor();assert.equal(effects,3);assert.equal(approverPrivateStatusAttempts,0);await approverContext.close();
    operations.get(ids.activation).op.state='executionUnknown';await policyReview.getByRole('button',{name:'Refresh same operation',exact:true}).click();
    await policyReview.getByText(/Execution outcome is unknown/).waitFor();assert.equal(await policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).isDisabled(),true);
    await policyReview.getByRole('button',{name:'Verify positive receipt; never redispatch',exact:true}).click();await policyReview.getByText('appliedVerified',{exact:false}).waitFor();
    assert.deepEqual(await page.evaluate(()=>window.__policyRecoveryWire),{bodyUndefined:true,sameOriginCredentials:true,antiforgeryHeader:true});assert.equal(reconciliations,1);assert.equal(effects,3);
    assert.equal(await policy.getByLabel('Resource kind').count(),1);await policy.getByLabel('Resource kind').selectOption('topic');
    await policy.getByLabel('Exact resource ID',{exact:true}).fill('orders');await policy.getByLabel('Registered action',{exact:true}).fill('TopicCreate');
    await policy.getByRole('button',{name:'Explain current decision',exact:true}).click();await policy.getByLabel('Current policy explanation').getByText(/GovernanceDenied/).waitFor();
    await policy.getByRole('button',{name:'Dry-run this fixture',exact:true}).click();await policy.getByLabel('Read-only dry-run results').getByText(/GovernanceDenied/).waitFor();
    await page.setViewportSize({width:390,height:844});await policy.scrollIntoViewIfNeeded();assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),true);
    authenticated=false;await page.evaluate(()=>window.dispatchEvent(new Event('focus')));await catalog.waitFor({state:'hidden'});await policy.waitFor({state:'hidden'});
    authenticated=true;denied=true;await page.reload();await page.locator('#ownership-catalog').getByText(/not authorized for this scope/).waitFor();assert.equal(await page.getByRole('button',{name:'Preview exact metadata change',exact:true}).isDisabled(),true);
    assert.deepEqual(outside,[]);assert.ok(csrf>=5);fs.mkdirSync(path.dirname(evidence),{recursive:true});
    fs.writeFileSync(evidence,JSON.stringify({...sourceIdentity(root),actualCompiledUi:true,actualChromium:true,browserVersion:browser.version(),apiFixtures:true,genuineAuthenticationDatabaseOrEffectClaimed:false,catalogPreviewConfirmExecute:true,policyVersionAndCriticalActivation:true,distinctHumanDenialUi:true,separateApproverContext:true,approverUsesScopedFrozenReview:true,requesterReloadResume:true,wrappedSelectLabelMatched:true,exactExplanationAndDryRunLocators:true,policyRecoveryBodylessWithCsrf:true,policyPositiveReceiptNoRedispatch:true,unknownNeverDispatch:true,crossScopeRejected:true,import501RejectedBeforeNetwork:true,csrf:true,untrustedMetadataEscaped:true,sessionLossClearsPanels:true,scopeDenialDisablesControls:true,mobileWidth:390,noHorizontalOverflow:true,noExternalRequests:true,fixtureEffects:effects},null,2));
    console.log('W73/W75 compiled governance UI acceptance passed (explicit API fixtures).');await context.close();
  }finally{await browser.close();server.close();}
})().catch(error=>{console.error(error);server.close();process.exitCode=1;});
