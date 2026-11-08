// Genuine compiled UI + Keycloak sessions + actual API. No API response mocks or token injection.
'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..'),sourceIdentity=require('./governance-browser-source.cjs');
const {chromium}=require(path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
(async()=>{
  let browser,phase='input',lastHttp={endpoint:'none',status:0},failurePage=null,failureOrigin=null,failureEvidence=null,signedIn=false;
  try{
    const [apiUrl,identityPath,scenarioPath,evidencePath]=process.argv.slice(2);
    if(!apiUrl||!identityPath||!scenarioPath||!evidencePath)throw Error('input');
    const read=file=>JSON.parse(fs.readFileSync(file,'utf8').replace(/^\uFEFF/,''));
    const api=new URL(apiUrl),identity=read(identityPath),scenario=read(scenarioPath),issuer=new URL(identity.issuer);
    if(!['request','attribution'].includes(scenario.kind)||!scenario.clusterId)throw Error('scenario');
    for(const endpoint of [api,issuer])if(!['localhost','127.0.0.1','[::1]'].includes(endpoint.hostname))throw Error('loopback fixture only');
    if(!identity.humanUsername||!identity.humanPasswordFile||!identity.browserClientId)throw Error('identity');
    failureOrigin=api.origin;failureEvidence=evidencePath;
    const launch={headless:true,channel:'chromium'};if(process.env.KAFDECK_G2_BROWSER_EXECUTABLE)launch.executablePath=process.env.KAFDECK_G2_BROWSER_EXECUTABLE;
    browser=await chromium.launch(launch);
    // Disposable loopback browser certificates only; TLS source/backend/CLI validation is independent evidence.
    const context=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}}),page=await context.newPage();failurePage=page;page.setDefaultTimeout(30000);
    const result={...sourceIdentity(root),actualCompiledUi:true,actualChromium:true,browserVersion:browser.version(),apiFixtures:false,tokenInjection:false,genuineKeycloakBrowserSession:true,kind:scenario.kind,loopbackFixtureBrowserCertificateErrorsIgnored:true,tlsConformanceClaimed:false};
    async function signIn(target,user,passwordFile){await target.goto(new URL('/api/v1/auth/login',api).href,{waitUntil:'domcontentloaded'});await target.locator('#username').fill(user);await target.locator('#password').fill(fs.readFileSync(passwordFile,'utf8').trim());const returned=target.waitForURL(url=>url.origin===api.origin&&!url.pathname.includes('signin-oidc'),{timeout:60000});await target.locator('#kc-login').click();await returned;await target.goto(api.origin,{waitUntil:'domcontentloaded'});}
    async function openWorkflow(target,kind){const request=kind==='request';await target.getByRole('link',{name:request?'Requests':'Measured attribution',exact:true}).click();await target.waitForURL(url=>url.hash===(request?'#service-requests':'#governance-attribution'));}
    const endpoint=pathname=>pathname.endsWith('/requests/changes/preview')?'request-change-preview':pathname.endsWith('/requests/decisions/preview')?'request-decision-preview':pathname.endsWith('/rate-cards/versions/preview')?'rate-version-preview':pathname.endsWith('/rate-cards/activations/preview')?'rate-activation-preview':/\/mutations\/[^/]+\/confirm$/.test(pathname)?'mutation-confirm':/\/mutations\/[^/]+\/execute$/.test(pathname)?'mutation-execute':'none';
    async function click(target,control,label,status=200){const pending=target.waitForResponse(response=>new URL(response.url()).origin===api.origin&&endpoint(new URL(response.url()).pathname)===label&&response.request().method()==='POST');await control.click();const response=await pending;lastHttp={endpoint:label,status:response.status()};assert.equal(response.status(),status);return response;}
    async function get(ctx,pathname,label){const response=await ctx.request.get(new URL(pathname,api).href);lastHttp={endpoint:label,status:response.status()};assert.equal(response.status(),200);return response.json();}
    async function apply(target,review){await review.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();const challenge=review.locator('label').filter({hasText:'Exact confirmation challenge:'});if(await challenge.count())await challenge.locator('input').fill((await challenge.locator('code').textContent()).trim());await click(target,review.getByRole('button',{name:'Confirm exact preview',exact:true}),'mutation-confirm');await click(target,review.getByRole('button',{name:'Execute reviewed operation',exact:true}),'mutation-execute');await review.getByText('appliedVerified',{exact:false}).waitFor();}
    phase='genuine-oidc-sign-in';await signIn(page,identity.humanUsername,identity.humanPasswordFile);signedIn=true;
    phase='authorized-cluster-bootstrap';const clusters=await get(context,'/api/v1/clusters','cluster-bootstrap');
    assert.ok(Array.isArray(clusters.data));assert.ok(clusters.data.some(item=>item.data?.clusterId===scenario.clusterId));
    phase='authorized-cluster-selection';
    await page.locator('#cluster-selector').selectOption(scenario.clusterId);
    phase='authorized-workflow-navigation';await openWorkflow(page,scenario.kind);
    if(scenario.kind==='request'){
      if(!identity.approverUsername||!identity.approverPasswordFile||!(scenario.actualTopicName||scenario.topicName))throw Error('request scenario');
      // CLI-human and cookie principals intentionally remain client-bound. Prepare this target
      // with the genuine cookie actor rather than borrowing an operation owned by the CLI.
      phase='cookie-owned-topic-preview';
      const csrf=await get(context,'/api/v1/auth/csrf','cookie-csrf');
      assert.equal(typeof csrf.headerName,'string');assert.equal(typeof csrf.requestToken,'string');
      const targetTopic=scenario.actualTopicName||scenario.topicName;
      const previewResponse=await context.request.post(new URL(`/api/v1/clusters/${encodeURIComponent(scenario.clusterId)}/topics/${encodeURIComponent(targetTopic)}/mutations/create/preview`,api).href,
        {headers:{[csrf.headerName]:csrf.requestToken,'Idempotency-Key':'browser-request-target-'+crypto.randomUUID()},data:{partitionCount:1,replicationFactor:1,configurations:{}}});
      lastHttp={endpoint:'cookie-topic-preview',status:previewResponse.status()};assert.equal(previewResponse.status(),201);
      const target=await previewResponse.json();assert.match(target.operationId,/^[a-f0-9-]{36}$/i);assert.match(target.previewHash,/^[a-f0-9]{64}$/i);assert.equal(target.clusterId,scenario.clusterId);
      phase='cookie-owned-topic-confirm';const confirmed=await context.request.post(new URL(`/api/v1/mutations/${target.operationId}/confirm`,api).href,
        {headers:{[csrf.headerName]:csrf.requestToken},data:{previewHash:target.previewHash,typedTargetChallenge:target.confirmationChallenge??null}});
      lastHttp={endpoint:'cookie-topic-confirm',status:confirmed.status()};assert.equal(confirmed.status(),200);const targetState=await confirmed.json();assert.equal(targetState.state.toLowerCase(),'ready');
      scenario.operationId=target.operationId;scenario.previewHash=target.previewHash;
      scenario.expiresAtUtc=new Date(Date.parse(target.previewExpiresAtUtc)-1000).toISOString();
      result.underlyingOperationPreparedThroughAuthenticatedCookieApi=true;result.linkedOperationId=target.operationId;
      const panel=page.locator('#service-requests');await panel.getByRole('heading',{name:'Service requests and approvals',exact:true}).waitFor();await page.waitForFunction(()=>!document.querySelector('#service-requests fieldset')?.disabled);
      phase='request-draft';await panel.getByLabel('Existing immutable effect operation ID',{exact:true}).fill(scenario.operationId);await panel.getByLabel('Request expiry UTC (ISO8601 ending in Z)',{exact:true}).fill(scenario.expiresAtUtc);await panel.getByLabel('Business reason',{exact:true}).fill('Genuine browser immutable request acceptance');await click(page,panel.getByRole('button',{name:'Preview governed request change',exact:true}),'request-change-preview');
      let review=panel.getByRole('article',{name:'Immutable governance operation'});await apply(page,review);
      phase='request-durable-draft-read';const listing=await get(context,'/api/v1/governance/requests?'+new URLSearchParams({clusterId:scenario.clusterId,pageSize:'200'}),'request-list');const matches=listing.items.filter(x=>x.operationId===scenario.operationId&&x.previewHash===scenario.previewHash);assert.equal(matches.length,1);const requestId=matches[0].requestId;assert.match(requestId,/^[a-f0-9]{32}$/);result.requestId=requestId;
      await panel.getByRole('button',{name:'Reload authorized requests',exact:true}).click();await panel.getByRole('button',{name:'Inspect request '+requestId,exact:true}).click();await panel.getByLabel('Business command').selectOption('Submit');phase='request-submit';await click(page,panel.getByRole('button',{name:'Preview governed request change',exact:true}),'request-change-preview');await apply(page,review);await panel.getByRole('button',{name:'Refresh selected request',exact:true}).click();
      phase='request-self-decision-denial';await panel.getByLabel('Business command').selectOption('Approve');await click(page,panel.getByRole('button',{name:'Preview governed request change',exact:true}),'request-decision-preview',403);await panel.getByRole('alert').getByText(/not authorized/).waitFor();
      phase='genuine-second-human-sign-in';const approverContext=await browser.newContext({ignoreHTTPSErrors:true}),approver=await approverContext.newPage();approver.setDefaultTimeout(30000);await signIn(approver,identity.approverUsername,identity.approverPasswordFile);await approver.locator('#cluster-selector').selectOption(scenario.clusterId);await openWorkflow(approver,'request');
      const decisions=approver.locator('#service-requests');await decisions.getByRole('button',{name:'Inspect request '+requestId,exact:true}).click();await decisions.getByLabel('Business command').selectOption('Approve');await decisions.getByLabel('Business reason',{exact:true}).fill('Independent current human approved exact browser revision');phase='request-distinct-human-decision';await click(approver,decisions.getByRole('button',{name:'Preview governed request change',exact:true}),'request-decision-preview');await apply(approver,decisions.getByRole('article',{name:'Immutable governance operation'}));await approverContext.close();
      phase='request-approved-original-session';await panel.getByRole('button',{name:'Refresh selected request',exact:true}).click();const current=await get(context,`/api/v1/governance/requests/${requestId}?`+new URLSearchParams({clusterId:scenario.clusterId}),'request-current');assert.equal(current.businessState.toLowerCase(),'approved');assert.equal(current.operationId,scenario.operationId);assert.equal(current.previewHash,scenario.previewHash);assert.equal(current.decisions.length,1);
      await panel.getByRole('button',{name:'Read immutable request history',exact:true}).click();await panel.getByLabel('Immutable request history').getByText(/Approved/i).waitFor();
      phase='request-linked-effect';await panel.getByRole('button',{name:'Inspect linked effect operation',exact:true}).click();review=panel.getByRole('article',{name:'Immutable governance operation'});await review.waitFor();await review.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
      // The producer supplies an independently preconfirmed non-material target. Business approval does not fill any kernel approval requirement.
      const execute=review.getByRole('button',{name:'Execute reviewed operation',exact:true});if(await execute.isDisabled())throw Error('underlying kernel gates not satisfied');
      await click(page,execute,'mutation-execute');await review.getByText('appliedVerified',{exact:false}).waitFor();
      const saved=await get(context,`/api/v1/governance/requests/${requestId}?`+new URLSearchParams({clusterId:scenario.clusterId}),'request-final');assert.equal(saved.executionState.toLowerCase(),'appliedverified');
      Object.assign(result,{realDraftSubmitGovernedEffects:true,genuineTwoHumanRequestApproval:true,requesterSelfDecisionDenied:true,immutableBusinessHistory:true,businessAndKernelStateSeparate:true,realLinkedEffectVerifiedByBackend:true,brokerEffectRequiresProducerVerification:true});
    }else{
      const {window,newRate}=scenario;if(!window||!newRate)throw Error('attribution scenario');const panel=page.locator('#governance-attribution');await panel.getByRole('heading',{name:'Measured attribution and estimates',exact:true}).waitFor();
      phase='measured-dashboard-interval';await panel.getByLabel('From UTC',{exact:true}).fill(window.fromUtc);await panel.getByLabel('To UTC',{exact:true}).fill(window.toUtc);await panel.getByLabel('Exact resource ID (optional)',{exact:true}).fill(window.resourceId);await panel.getByRole('button',{name:'Read measured attribution',exact:true}).click();const rendered=panel.getByRole('article',{name:'Measured attribution result'});await rendered.waitFor();
      const query=new URLSearchParams({clusterId:scenario.clusterId,fromUtc:window.fromUtc,toUtc:window.toUtc,resourceId:window.resourceId,pageSize:'200'});
      phase='measured-provenance';const data=await get(context,'/api/v1/governance/dashboards?'+query,'dashboard-read');const rows=data.items.filter(x=>x.measurement.resourceId===window.resourceId);assert.ok(rows.length>0);const observed=rows.find(x=>x.measurement.quantity===window.quantity);assert.ok(observed);assert.equal(observed.measurement.unit,window.unit);assert.match(observed.measurement.provenanceHash,/^[a-f0-9]{64}$/);assert.equal(observed.currency,window.currency);if(window.expectedRoundedAmount!==undefined)assert.equal(observed.roundedAmount,window.expectedRoundedAmount);
      assert.ok((await rendered.textContent()).includes(window.quantity));assert.ok((await rendered.textContent()).includes(window.resourceId));const originalRate=observed.rateCardHash;result.measurementProvenanceHash=observed.measurement.provenanceHash;
      phase='rate-card-version-input';await page.waitForFunction(()=>!document.querySelector('#governance-attribution fieldset:nth-of-type(2)')?.disabled);await panel.getByLabel('Registered measured metric').selectOption(newRate.metricKind);await panel.getByLabel('Currency code',{exact:true}).fill(newRate.currency);await panel.getByLabel('Decimal amount per exact measured unit').fill(newRate.amount);await panel.getByLabel('Rate effective from UTC').fill(newRate.effectiveFromUtc);await panel.getByLabel('Rate effective to UTC').fill(newRate.effectiveToUtc);await panel.getByLabel('Explicit catalog allocation weights (JSON; maximum 100)').fill(JSON.stringify(newRate.weights));
      phase='rate-card-version-effect';const response=await click(page,panel.getByRole('button',{name:'Preview governed rate-card version',exact:true}),'rate-version-preview');const versionOperation=await response.json();let review=panel.getByRole('article',{name:'Immutable governance operation'});await apply(page,review);
      const material=await get(context,'/api/v1/governance/rate-cards/operations/'+versionOperation.operationId,'rate-version-material');assert.match(material.versionId,/^[a-f0-9]{64}$/);result.rateVersionId=material.versionId;
      await panel.getByRole('button',{name:'Reload authorized rate cards',exact:true}).click();await panel.getByLabel('Immutable rate-card version').selectOption(material.versionId);await panel.getByRole('button',{name:'Create key for an intentionally new rate-card change',exact:true}).click();
      phase='rate-card-activation-effect';await click(page,panel.getByRole('button',{name:'Preview governed rate-card activation',exact:true}),'rate-activation-preview');await apply(page,review);
      const state=await get(context,'/api/v1/governance/rate-cards/state?'+new URLSearchParams({clusterId:scenario.clusterId}),'rate-card-state');assert.equal(state.versionId,material.versionId);assert.equal(state.enabled,true);
      phase='historical-rate-provenance';const after=await get(context,'/api/v1/governance/dashboards?'+query,'dashboard-recheck');const retained=after.items.find(x=>x.measurement.windowId===observed.measurement.windowId);assert.ok(retained);assert.equal(retained.rateCardHash,originalRate);assert.equal(retained.roundedAmount,observed.roundedAmount);
      Object.assign(result,{realMeasuredWindowRendered:true,realSourceProvenance:true,invariantDecimalEstimateVerified:true,realHighRateVersionAndActivationUi:true,historicalRateBindingPreserved:true,metricsAcquisitionRequiresProducerVerification:true});
    }
    phase='current-session-cross-scope-denial';const deniedPath=scenario.kind==='request'?'/api/v1/governance/requests?clusterId=forbidden&pageSize=50':'/api/v1/governance/rate-cards/state?clusterId=forbidden';const denied=await context.request.get(new URL(deniedPath,api).href);assert.ok([403,404].includes(denied.status()));result.genuineSessionCrossScopeDenied=true;
    phase='session-logout-clears-workflows';
    const logout=page.waitForResponse(response=>response.request().method()==='POST'&&response.url()===new URL('/api/v1/auth/logout',api).href);logout.catch(()=>{});
    await page.getByRole('button',{name:'Sign out',exact:true}).click();
    assert.ok((await logout).status()<400);
    const signedOut=await context.request.get(new URL('/api/v1/auth/session',api).href);assert.equal(signedOut.status(),401);
    await page.locator('#service-requests').waitFor({state:'hidden'});await page.locator('#governance-attribution').waitFor({state:'hidden'});result.logoutClearsWorkflowState=true;
    fs.mkdirSync(path.dirname(evidencePath),{recursive:true});fs.writeFileSync(evidencePath,JSON.stringify(result,null,2));console.log('V09_GENUINE_WORKFLOW_BROWSER_PASSED kind='+scenario.kind);await context.close();
  }catch{
    // Record only a closed phase/status and an authenticated application screenshot; never identity-page fields, URLs, tokens or raw exceptions.
    try{if(signedIn&&failurePage&&!failurePage.isClosed()&&failureEvidence){const current=new URL(failurePage.url());if(current.origin===failureOrigin&&current.pathname==='/'&&await failurePage.locator('input[type="password"]').count()===0&&await failurePage.getByRole('button',{name:'Sign out',exact:true}).count()===1){const screenshot=failureEvidence.replace(/(?:-evidence)?\.json$/,'-failure.png');fs.mkdirSync(path.dirname(screenshot),{recursive:true});await failurePage.screenshot({path:screenshot,fullPage:true,timeout:5000});fs.writeFileSync(screenshot.replace(/\.png$/,'.json'),JSON.stringify({failed:true,phase,lastHttp},null,2));}}}catch{}
    console.error('V09_GENUINE_WORKFLOW_BROWSER_FAILED phase='+phase+' endpoint='+lastHttp.endpoint+' status='+lastHttp.status);process.exitCode=3;
  }finally{if(browser)await browser.close();}
})();
