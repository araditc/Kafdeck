// Genuine shipped API/UI + Keycloak session acceptance. No routed/mocked API responses or token injection.
'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..');
const sourceIdentity=require('./governance-browser-source.cjs');
const {chromium}=require(path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
(async()=>{
  let browser,phase='input',lastHttp={endpoint:'none',status:0},failurePage=null,failureApiOrigin=null,failureEvidence=null,signedIn=false;
  const safeCounts={apiVersions:0,apiActivePresent:0,domActiveTerms:0,domOptions:0,domActivePresent:0,domSelectedSummaries:0,domSelectedPre:0,screenshotCaptured:0};
  try{
    const [apiUrl,manifestPath,evidencePath]=process.argv.slice(2);
    if(!apiUrl||!manifestPath||!evidencePath)throw Error('input');
    const api=new URL(apiUrl),manifest=JSON.parse(fs.readFileSync(manifestPath,'utf8').replace(/^\uFEFF/,'')),issuer=new URL(manifest.issuer);
    failureApiOrigin=api.origin;failureEvidence=evidencePath;
    for(const endpoint of [api,issuer])if(!['127.0.0.1','[::1]','localhost'].includes(endpoint.hostname))throw Error('disposable loopback lab only');
    if(!manifest.browserClientId||!manifest.humanUsername||!manifest.humanPasswordFile||!manifest.approverUsername||!manifest.approverPasswordFile)throw Error('browser lab identity missing');
    const launch={headless:true,channel:'chromium'};if(process.env.KAFDECK_G2_BROWSER_EXECUTABLE)launch.executablePath=process.env.KAFDECK_G2_BROWSER_EXECUTABLE;
    browser=await chromium.launch(launch);
    // Only disposable loopback test certificates are ignored in this browser context.
    // Genuine backend/CLI TLS validation is independent evidence; this is not a TLS conformance claim.
    const context=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}}),page=await context.newPage();
    failurePage=page;
    page.setDefaultTimeout(30000);const requests=[];
    page.on('request',request=>{const url=new URL(request.url());if(url.origin===api.origin&&url.pathname.startsWith('/api/v1/governance/'))requests.push({path:url.pathname,method:request.method()});});
    // Only compiled endpoint labels and numeric statuses may enter public
    // diagnostics. Never print a URL, query, request body, response or token.
    const endpointLabel=pathname=>{
      const exact={'/api/v1/governance/policies/validate':'policy-validate','/api/v1/governance/policies/explain':'policy-explain','/api/v1/governance/policies/dry-run':'policy-dry-run',
        '/api/v1/governance/policies/active':'policy-state-read','/api/v1/governance/policies/versions':'policy-versions-read',
        '/api/v1/governance/policies/versions/preview':'policy-version-preview','/api/v1/governance/policies/activations/preview':'policy-activation-preview'};
      if(exact[pathname])return exact[pathname];
      if(/^\/api\/v1\/mutations\/[a-f0-9-]+\/confirm$/i.test(pathname))return 'mutation-confirm';
      if(/^\/api\/v1\/mutations\/[a-f0-9-]+\/approve$/i.test(pathname))return 'mutation-approve';
      if(/^\/api\/v1\/mutations\/[a-f0-9-]+\/execute$/i.test(pathname))return 'mutation-execute';
      return null;
    };
    async function checkedClick(target,control,endpoint,expected=200,method='POST'){
      const pending=target.waitForResponse(response=>{const url=new URL(response.url());return url.origin===api.origin&&endpointLabel(url.pathname)===endpoint&&response.request().method()===method;});
      await control.click();const response=await pending;lastHttp={endpoint,status:response.status()};assert.equal(lastHttp.status,expected);
    }
    async function signIn(target,user,passwordFile){
      await target.goto(new URL('/api/v1/auth/login',api).href,{waitUntil:'domcontentloaded'});
      await target.locator('#username').fill(user);await target.locator('#password').fill(fs.readFileSync(passwordFile,'utf8').trim());
      const returned=target.waitForURL(url=>url.origin===api.origin&&!url.pathname.includes('signin-oidc'),{timeout:60000});
      await target.locator('#kc-login').click();await returned;await target.goto(new URL('/',api).href,{waitUntil:'domcontentloaded'});
    }
    phase='genuine-oidc-sign-in';await signIn(page,manifest.humanUsername,manifest.humanPasswordFile);signedIn=true;
    phase='real-browser-cookie-csrf-denial';
    const csrfDenied=await context.request.post(new URL('/api/v1/governance/catalog/teams/preview',api).href,
      {headers:{'Idempotency-Key':'csrf-negative-'+crypto.randomUUID()},data:{clusterId:'permitted',changes:[{kind:'Team',expectedRevision:0,name:'csrf-negative'}]}});
    assert.equal(csrfDenied.status(),403);
    phase='actual-generated-contract';
    const discoveryResponse=await context.request.get(new URL('/api/v1/openapi/v0.9-governance.json',api).href);assert.equal(discoveryResponse.status(),200);
    const governanceDiscovery=await discoveryResponse.json();assert.equal(governanceDiscovery.openapi,'3.1.0');
    assert.ok(governanceDiscovery.paths['/api/v1/governance/catalog/teams'].get);
    assert.ok(governanceDiscovery.paths['/api/v1/governance/policies/activations/preview'].post);
    assert.ok(governanceDiscovery.paths['/api/v1/governance/policies/operations/{operationId}/reconcile'].post);
    assert.equal(governanceDiscovery.paths['/api/v1/governance/policies/active'].get.parameters.find(p=>p.name==='clusterId').required,true);
    const catalog=page.locator('#ownership-catalog'),policy=page.locator('#governance-policy');
    await page.getByRole('link',{name:'Ownership catalog',exact:true}).click();
    await catalog.getByRole('heading',{name:'Ownership catalog',exact:true}).waitFor();
    await page.waitForFunction(()=>!document.querySelector('#ownership-catalog fieldset')?.disabled);
    phase='real-catalog-preview-confirm-execute';
    const teamName='browser-team-'+crypto.randomUUID().replaceAll('-','').slice(0,12);
    await catalog.getByLabel('Name',{exact:true}).fill(teamName);
    await catalog.getByLabel('Description',{exact:true}).fill('Genuine Keycloak browser catalog acceptance');
    await catalog.getByRole('button',{name:'Preview exact metadata change',exact:true}).click();
    const review=catalog.getByRole('article',{name:'Immutable governance operation'});await review.waitFor();
    await review.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await review.getByRole('button',{name:'Confirm exact preview',exact:true}).click();
    await review.getByRole('button',{name:'Execute reviewed operation',exact:true}).click();
    await review.getByText('appliedVerified',{exact:false}).waitFor();
    phase='real-durable-history';await catalog.getByRole('button',{name:'Reload authorized catalog',exact:true}).click();
    await catalog.getByRole('button',{name:'Inspect '+teamName,exact:true}).click();
    await catalog.getByRole('button',{name:'Read immutable record history',exact:true}).click();
    const history=catalog.getByRole('table',{name:'Immutable revisions; latest 1000 loaded history entries retained'});await history.waitFor();
    assert.match(await history.textContent(),new RegExp(teamName));
    phase='policy-state-ready';
    await page.getByRole('link',{name:'Governance policy',exact:true}).click();
    await policy.getByRole('heading',{name:'Governance policy',exact:true}).waitFor();
    await page.waitForFunction(()=>document.querySelector('#governance-policy fieldset')?.disabled===false);
    phase='policy-validation-input';
    await policy.getByLabel('Closed schema JSON (maximum 256 KiB)').fill('{"schemaVersion":1,"rules":[]}');
    phase='policy-validation-submit';await checkedClick(page,policy.getByRole('button',{name:'Validate closed policy',exact:true}),'policy-validate');
    phase='policy-validation-result';await policy.getByRole('region',{name:'Policy validation result'}).waitFor();
    phase='policy-state-explicit-refresh';
    const refreshedVersions=page.waitForResponse(response=>{const url=new URL(response.url());return url.origin===api.origin&&endpointLabel(url.pathname)==='policy-versions-read'&&response.request().method()==='GET';});
    await checkedClick(page,policy.getByRole('button',{name:'Reload authorized policy state',exact:true}),'policy-state-read',200,'GET');
    const refreshed=await refreshedVersions;lastHttp={endpoint:'policy-versions-read',status:refreshed.status()};assert.equal(lastHttp.status,200);
    phase='policy-state-refresh-rendered';await page.waitForFunction(()=>document.querySelector('#governance-policy')?.getAttribute('aria-busy')==='false'&&document.querySelector('#governance-policy fieldset')?.disabled===false);
    phase='policy-api-active';
    const activeResponse=await context.request.get(new URL('/api/v1/governance/policies/active?clusterId=permitted',api).href);lastHttp={endpoint:'policy-active',status:activeResponse.status()};assert.equal(lastHttp.status,200);
    const actualActive=await activeResponse.json();assert.equal(actualActive.enabled,true);assert.match(actualActive.versionId,/^[a-f0-9]{64}$/);
    phase='policy-api-versions';
    const versionsResponse=await context.request.get(new URL('/api/v1/governance/policies/versions?clusterId=permitted&pageSize=200',api).href);lastHttp={endpoint:'policy-versions',status:versionsResponse.status()};assert.equal(lastHttp.status,200);
    const actualVersions=await versionsResponse.json();assert.ok(Array.isArray(actualVersions.items));safeCounts.apiVersions=actualVersions.items.length;
    const actualVersion=actualVersions.items.find(item=>item.versionId===actualActive.versionId);safeCounts.apiActivePresent=Number(!!actualVersion);assert.ok(actualVersion);
    phase='policy-dom-active-term';
    const activeTerm=policy.locator('dt').filter({hasText:/^Active version$/});safeCounts.domActiveTerms=await activeTerm.count();assert.equal(safeCounts.domActiveTerms,1);
    const currentVersion=(await activeTerm.locator('xpath=following-sibling::dd[1]').textContent({timeout:5000})).trim();assert.equal(currentVersion,actualActive.versionId);
    phase='policy-dom-active-option';
    const versionSelect=policy.getByLabel('Immutable policy version');const optionValues=await versionSelect.locator('option').evaluateAll(options=>options.map(option=>option.value));
    safeCounts.domOptions=optionValues.length;safeCounts.domActivePresent=Number(optionValues.includes(currentVersion));assert.equal(safeCounts.domActivePresent,1);
    phase='policy-dom-select-active';await versionSelect.selectOption(currentVersion,{timeout:10000});
    phase='policy-dom-selected-summary';
    const selectedSummary=policy.getByText('Selected immutable version',{exact:true});await selectedSummary.waitFor({state:'visible',timeout:10000});safeCounts.domSelectedSummaries=await selectedSummary.count();assert.equal(safeCounts.domSelectedSummaries,1);
    phase='policy-dom-selected-pre';const selectedPre=selectedSummary.locator('..').locator('pre');safeCounts.domSelectedPre=await selectedPre.count();assert.equal(safeCounts.domSelectedPre,1);
    const visibleDocument=await selectedPre.textContent({timeout:5000});assert.equal(visibleDocument,actualVersion.document);
    phase='policy-dom-selected-json';const currentPolicy=JSON.parse(visibleDocument);
    phase='policy-active-denial-rule';
    const deny=currentPolicy.rules.find(rule=>rule.outcome==='Deny'&&rule.scope.clusterId==='permitted'&&rule.scope.resourceKind==='topic'&&rule.scope.action==='TopicCreate'&&Array.isArray(rule.scope.resourceIds)&&rule.scope.resourceIds.length>0);
    assert.ok(deny,'The actual fixture policy must contain the scoped TopicCreate denial selector.');
    // A wrapping label also includes its select option text. Chromium's exact
    // label name therefore differs from the visible "Resource kind" prefix.
    phase='policy-denial-kind-select';const resourceKindSelect=policy.getByLabel('Resource kind');assert.equal(await resourceKindSelect.count(),1);await resourceKindSelect.selectOption(deny.scope.resourceKind);
    phase='policy-denial-resource-fill';
    await policy.getByLabel('Exact resource ID',{exact:true}).fill(deny.scope.resourceIds[0]);
    phase='policy-denial-action-fill';
    await policy.getByLabel('Registered action',{exact:true}).fill(deny.scope.action);
    phase='policy-explanation-submit';await checkedClick(page,policy.getByRole('button',{name:'Explain current decision',exact:true}),'policy-explain');
    phase='policy-explanation-denied-result';await policy.getByLabel('Current policy explanation').getByText(/GovernanceDenied/).waitFor();
    phase='policy-dry-run-submit';await checkedClick(page,policy.getByRole('button',{name:'Dry-run this fixture',exact:true}),'policy-dry-run');
    phase='policy-dry-run-denied-result';await policy.getByLabel('Read-only dry-run results').getByText(/GovernanceDenied/).waitFor();
    phase='policy-version-preview';
    const activationRevision=Number((await policy.locator('dt').filter({hasText:/^Active revision$/}).locator('xpath=following-sibling::dd[1]').textContent()).trim());assert.ok(Number.isSafeInteger(activationRevision));
    await checkedClick(page,policy.getByRole('button',{name:'Preview governed version write',exact:true}),'policy-version-preview');
    const policyReview=policy.getByRole('article',{name:'Immutable governance operation'});await policyReview.waitFor();
    await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    phase='policy-version-confirm';await checkedClick(page,policyReview.getByRole('button',{name:'Confirm exact preview',exact:true}),'mutation-confirm');
    phase='policy-version-execute';await checkedClick(page,policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}),'mutation-execute');
    phase='policy-version-verified';await policyReview.getByText('appliedVerified',{exact:false}).waitFor();
    phase='policy-activation-preview';
    await policy.getByRole('button',{name:'Reload authorized policy state',exact:true}).click();
    await policy.getByRole('button',{name:'Create key for an intentionally new policy request',exact:true}).click();
    await policy.getByLabel('Immutable policy version').selectOption(currentVersion);
    await checkedClick(page,policy.getByRole('button',{name:'Preview critical activation change',exact:true}),'policy-activation-preview');
    await policyReview.getByText(/Critical policy activation/).waitFor();
    const activationId=(await policyReview.locator('dd code').first().textContent()).trim();
    const challenge=(await policyReview.locator('label').filter({hasText:'Exact confirmation challenge:'}).locator('code').textContent()).trim();
    await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await policyReview.getByLabel('Exact confirmation challenge:',{exact:false}).fill(challenge);
    phase='policy-activation-confirm';await checkedClick(page,policyReview.getByRole('button',{name:'Confirm exact preview',exact:true}),'mutation-confirm');
    phase='policy-self-approval-denied';
    await checkedClick(page,policyReview.getByRole('button',{name:'Submit independent human approval',exact:true}),'mutation-approve',403);await policy.getByText(/not authorized for this scope/).waitFor();
    assert.equal(await policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).isDisabled(),true);
    phase='real-independent-human-ui-approval';
    const approverContext=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}}),approver=await approverContext.newPage();approver.setDefaultTimeout(30000);
    await signIn(approver,manifest.approverUsername,manifest.approverPasswordFile);
    await approver.getByRole('link',{name:'Governance policy',exact:true}).click();
    const approvalPanel=approver.locator('#governance-policy');await approvalPanel.getByRole('heading',{name:'Governance policy',exact:true}).waitFor();
    await approvalPanel.getByLabel('Existing policy operation ID').fill(activationId);
    await approvalPanel.getByRole('button',{name:'Inspect frozen policy operation',exact:true}).click();
    const approvalReview=approvalPanel.getByRole('article',{name:'Immutable governance operation'});await approvalReview.waitFor();
    await approvalReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    phase='policy-independent-approval-submit';await checkedClick(approver,approvalReview.getByRole('button',{name:'Submit independent human approval',exact:true}),'mutation-approve');
    phase='policy-independent-approval-ready';await approvalReview.getByText('ready',{exact:false}).waitFor();
    // Returning to a tab deliberately revalidates the operator and clears old
    // authorization-bound UI state. Reopen the exact durable operation after
    // a genuine requester reload instead of assuming stale component state.
    phase='real-ui-approved-policy-activation-effect';await page.reload({waitUntil:'domcontentloaded'});
    await policy.getByRole('heading',{name:'Governance policy',exact:true}).waitFor();
    await policy.getByLabel('Existing policy operation ID').fill(activationId);
    await policy.getByRole('button',{name:'Inspect frozen policy operation',exact:true}).click();await policyReview.waitFor();
    await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    phase='policy-activation-execute';await checkedClick(page,policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}),'mutation-execute');
    phase='policy-activation-verified';await policyReview.getByText('appliedVerified',{exact:false}).waitFor();
    phase='policy-activation-state-reload';
    await policy.getByRole('button',{name:'Reload authorized policy state',exact:true}).click();
    await page.waitForFunction(expected=>{const labels=Array.from(document.querySelectorAll('#governance-policy dt'));return labels.find(item=>item.textContent==='Active revision')?.nextElementSibling?.textContent===String(expected);},activationRevision+1);
    assert.equal((await policy.locator('dt').filter({hasText:/^Active version$/}).locator('xpath=following-sibling::dd[1]').textContent()).trim(),currentVersion);
    await approverContext.close();
    phase='genuine-session-scope-denial';
    const denied=await page.evaluate(async()=>{const response=await fetch('/api/v1/governance/catalog/teams?clusterId=forbidden&pageSize=50',{credentials:'same-origin'});return response.status;});
    assert.ok([403,404].includes(denied));
    const clusterOptions=await page.locator('#cluster-selector option').allTextContents();assert.ok(!clusterOptions.includes('forbidden'));
    phase='genuine-session-logout';
    const logout=page.waitForResponse(response=>response.request().method()==='POST'&&response.url()===new URL('/api/v1/auth/logout',api).href);
    logout.catch(()=>{});
    await page.getByRole('button',{name:'Sign out',exact:true}).click();
    const logoutResponse=await logout;lastHttp={endpoint:'logout',status:logoutResponse.status()};
    assert.ok(lastHttp.status<400);
    phase='genuine-session-cleared';
    const signedOut=await context.request.get(new URL('/api/v1/auth/session',api).href);
    lastHttp={endpoint:'session-after-logout',status:signedOut.status()};assert.equal(lastHttp.status,401);
    await catalog.waitFor({state:'hidden'});await policy.waitFor({state:'hidden'});
    const signedOutPage=await context.newPage();
    await signedOutPage.goto(new URL('/#ownership-catalog',api).href,{waitUntil:'domcontentloaded'});
    await signedOutPage.getByRole('heading',{name:'Sign in to Kafdeck',exact:true}).waitFor();
    await signedOutPage.locator('#ownership-catalog').waitFor({state:'hidden'});
    fs.mkdirSync(path.dirname(evidencePath),{recursive:true});
    fs.writeFileSync(evidencePath,JSON.stringify({...sourceIdentity(root),actualCompiledUi:true,actualChromium:true,browserVersion:browser.version(),apiFixtures:false,tokenInjection:false,genuineKeycloakBrowserSession:true,realCatalogPreviewConfirmationVerifiedEffect:true,realDurableCatalogHistory:true,realPolicyValidationExplanationDryRun:true,realPolicyVersionWriteUiEffect:true,realCriticalActivationUiEffect:true,genuineTwoHumanBrowserApproval:true,requesterSelfApprovalDenied:true,reactivatedSamePolicyPreservesRuleSemantics:true,genuineSessionCrossScopeDenied:true,logoutClearsGovernanceState:true,loopbackFixtureBrowserCertificateErrorsIgnored:true,tlsConformanceClaimed:false,governanceRequests:requests.map(item=>({method:item.method,family:item.path.includes('/catalog/')?'catalog':'policy'}))},null,2));
    console.log('V09_GENUINE_GOVERNANCE_BROWSER_PASSED');await context.close();
  }catch{
    // Keep credentials, issuer URLs, authorization codes and tokens out of public CI logs.
    try{
      if(signedIn&&failurePage&&!failurePage.isClosed()&&failureEvidence){
        const current=new URL(failurePage.url());
        if(current.origin===failureApiOrigin&&current.pathname==='/'&&await failurePage.locator('input[type="password"]').count()===0&&await failurePage.getByRole('button',{name:'Sign out',exact:true}).count()===1){
          const screenshot=failureEvidence.endsWith('-evidence.json')?failureEvidence.replace(/-evidence\.json$/,'-failure.png'):failureEvidence.replace(/\.json$/,'-browser-failure.png');
          fs.mkdirSync(path.dirname(screenshot),{recursive:true});await failurePage.screenshot({path:screenshot,fullPage:true,timeout:5000});safeCounts.screenshotCaptured=1;
          fs.writeFileSync(screenshot.replace(/\.png$/,'.json'),JSON.stringify({failed:true,phase,lastHttp,safeCounts},null,2));
        }
      }
    }catch{/* A diagnostic failure must never disclose browser exception details. */}
    const counts=Object.entries(safeCounts).map(([name,value])=>name+':'+Math.max(0,Math.min(10000,Number.isInteger(value)?value:0))).join(',');
    console.error('V09_GENUINE_GOVERNANCE_BROWSER_FAILED phase='+phase+' endpoint='+lastHttp.endpoint+' status='+lastHttp.status+' counts='+counts);process.exitCode=3;
  }finally{if(browser)await browser.close();}
})();
