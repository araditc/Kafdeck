// Genuine shipped API/UI + Keycloak session acceptance. No routed/mocked API responses or token injection.
'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..');
const sourceIdentity=require('./governance-browser-source.cjs');
const {chromium}=require(path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
(async()=>{
  let browser,phase='input';
  try{
    const [apiUrl,manifestPath,evidencePath]=process.argv.slice(2);
    if(!apiUrl||!manifestPath||!evidencePath)throw Error('input');
    const api=new URL(apiUrl),manifest=JSON.parse(fs.readFileSync(manifestPath,'utf8').replace(/^\uFEFF/,'')),issuer=new URL(manifest.issuer);
    for(const endpoint of [api,issuer])if(!['127.0.0.1','[::1]','localhost'].includes(endpoint.hostname))throw Error('disposable loopback lab only');
    if(!manifest.browserClientId||!manifest.humanUsername||!manifest.humanPasswordFile||!manifest.approverUsername||!manifest.approverPasswordFile)throw Error('browser lab identity missing');
    const launch={headless:true,channel:'chromium'};if(process.env.KAFDECK_G2_BROWSER_EXECUTABLE)launch.executablePath=process.env.KAFDECK_G2_BROWSER_EXECUTABLE;
    browser=await chromium.launch(launch);
    // Only disposable loopback test certificates are ignored in this browser context.
    // Genuine backend/CLI TLS validation is independent evidence; this is not a TLS conformance claim.
    const context=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}}),page=await context.newPage();
    page.setDefaultTimeout(30000);const requests=[];
    page.on('request',request=>{const url=new URL(request.url());if(url.origin===api.origin&&url.pathname.startsWith('/api/v1/governance/'))requests.push({path:url.pathname,method:request.method()});});
    async function signIn(target,user,passwordFile){
      await target.goto(new URL('/api/v1/auth/login',api).href,{waitUntil:'domcontentloaded'});
      await target.locator('#username').fill(user);await target.locator('#password').fill(fs.readFileSync(passwordFile,'utf8').trim());
      const returned=target.waitForURL(url=>url.origin===api.origin&&!url.pathname.includes('signin-oidc'),{timeout:60000});
      await target.locator('#kc-login').click();await returned;await target.goto(new URL('/',api).href,{waitUntil:'domcontentloaded'});
    }
    phase='genuine-oidc-sign-in';await signIn(page,manifest.humanUsername,manifest.humanPasswordFile);
    phase='actual-generated-contract';
    const discoveryResponse=await context.request.get(new URL('/api/v1/openapi/v0.9-governance.json',api).href);assert.equal(discoveryResponse.status(),200);
    const governanceDiscovery=await discoveryResponse.json();assert.equal(governanceDiscovery.openapi,'3.1.0');
    assert.ok(governanceDiscovery.paths['/api/v1/governance/catalog/teams'].get);
    assert.ok(governanceDiscovery.paths['/api/v1/governance/policies/activations/preview'].post);
    assert.ok(governanceDiscovery.paths['/api/v1/governance/policies/operations/{operationId}/reconcile'].post);
    assert.equal(governanceDiscovery.paths['/api/v1/governance/policies/active'].get.parameters.find(p=>p.name==='clusterId').required,true);
    const catalog=page.locator('#ownership-catalog'),policy=page.locator('#governance-policy');
    await catalog.getByRole('heading',{name:'Ownership catalog',exact:true}).waitFor();
    await policy.getByRole('heading',{name:'Governance policy',exact:true}).waitFor();
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
    phase='real-policy-validate-explain-dry-run';
    await page.waitForFunction(()=>!document.querySelector('#governance-policy fieldset')?.disabled);
    await policy.getByLabel('Closed schema JSON (maximum 256 KiB)').fill('{"schemaVersion":1,"rules":[]}');
    await policy.getByRole('button',{name:'Validate closed policy',exact:true}).click();await policy.getByRole('region',{name:'Policy validation result'}).waitFor();
    await policy.getByLabel('Resource kind',{exact:true}).selectOption('topic');
    await policy.getByLabel('Exact resource ID',{exact:true}).fill('browser-policy-read-probe');
    await policy.getByLabel('Registered action',{exact:true}).fill('TopicCreate');
    await policy.getByRole('button',{name:'Explain current decision',exact:true}).click();await policy.getByLabel('Current policy explanation').waitFor();
    await policy.getByRole('button',{name:'Dry-run this fixture',exact:true}).click();await policy.getByLabel('Read-only dry-run results').waitFor();
    phase='real-ui-policy-version-write';
    const currentVersion=(await policy.locator('dt').filter({hasText:/^Active version$/}).locator('xpath=following-sibling::dd[1]').textContent()).trim();assert.match(currentVersion,/^[a-f0-9]{64}$/);
    const activationRevision=Number((await policy.locator('dt').filter({hasText:/^Active revision$/}).locator('xpath=following-sibling::dd[1]').textContent()).trim());assert.ok(Number.isSafeInteger(activationRevision));
    await policy.getByRole('button',{name:'Preview governed version write',exact:true}).click();
    const policyReview=policy.getByRole('article',{name:'Immutable governance operation'});await policyReview.waitFor();
    await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await policyReview.getByRole('button',{name:'Confirm exact preview',exact:true}).click();await policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).click();await policyReview.getByText('appliedVerified',{exact:false}).waitFor();
    phase='real-ui-critical-activation-request';
    await policy.getByRole('button',{name:'Reload authorized policy state',exact:true}).click();
    await policy.getByRole('button',{name:'Create key for an intentionally new policy request',exact:true}).click();
    await policy.getByLabel('Immutable policy version').selectOption(currentVersion);
    await policy.getByRole('button',{name:'Preview critical activation change',exact:true}).click();
    await policyReview.getByText(/Critical policy activation/).waitFor();
    const activationId=(await policyReview.locator('dd code').first().textContent()).trim();
    const challenge=(await policyReview.locator('label').filter({hasText:'Exact confirmation challenge:'}).locator('code').textContent()).trim();
    await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await policyReview.getByLabel('Exact confirmation challenge:',{exact:false}).fill(challenge);
    await policyReview.getByRole('button',{name:'Confirm exact preview',exact:true}).click();
    await policyReview.getByRole('button',{name:'Submit independent human approval',exact:true}).click();await policy.getByText(/not authorized for this scope/).waitFor();
    assert.equal(await policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).isDisabled(),true);
    phase='real-independent-human-ui-approval';
    const approverContext=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}}),approver=await approverContext.newPage();approver.setDefaultTimeout(30000);
    await signIn(approver,manifest.approverUsername,manifest.approverPasswordFile);
    const approvalPanel=approver.locator('#governance-policy');await approvalPanel.getByRole('heading',{name:'Governance policy',exact:true}).waitFor();
    await approvalPanel.getByLabel('Existing policy operation ID').fill(activationId);
    await approvalPanel.getByRole('button',{name:'Inspect frozen policy operation',exact:true}).click();
    const approvalReview=approvalPanel.getByRole('article',{name:'Immutable governance operation'});await approvalReview.waitFor();
    await approvalReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await approvalReview.getByRole('button',{name:'Submit independent human approval',exact:true}).click();await approvalReview.getByText('ready',{exact:false}).waitFor();
    // Returning to a tab deliberately revalidates the operator and clears old
    // authorization-bound UI state. Reopen the exact durable operation after
    // a genuine requester reload instead of assuming stale component state.
    phase='real-ui-approved-policy-activation-effect';await page.goto(new URL('/',api).href,{waitUntil:'domcontentloaded'});
    await policy.getByRole('heading',{name:'Governance policy',exact:true}).waitFor();
    await policy.getByLabel('Existing policy operation ID').fill(activationId);
    await policy.getByRole('button',{name:'Inspect frozen policy operation',exact:true}).click();await policyReview.waitFor();
    await policyReview.getByLabel('I inspected this immutable operation, scope, material and fingerprint.').check();
    await policyReview.getByRole('button',{name:'Execute reviewed operation',exact:true}).click();await policyReview.getByText('appliedVerified',{exact:false}).waitFor();
    await policy.getByRole('button',{name:'Reload authorized policy state',exact:true}).click();
    await page.waitForFunction(expected=>{const labels=Array.from(document.querySelectorAll('#governance-policy dt'));return labels.find(item=>item.textContent==='Active revision')?.nextElementSibling?.textContent===String(expected);},activationRevision+1);
    assert.equal((await policy.locator('dt').filter({hasText:/^Active version$/}).locator('xpath=following-sibling::dd[1]').textContent()).trim(),currentVersion);
    await approverContext.close();
    phase='genuine-session-scope-denial';
    const denied=await page.evaluate(async()=>{const response=await fetch('/api/v1/governance/catalog/teams?clusterId=forbidden&pageSize=50',{credentials:'same-origin'});return response.status;});
    assert.ok([403,404].includes(denied));
    const clusterOptions=await page.locator('#cluster-selector option').allTextContents();assert.ok(!clusterOptions.includes('forbidden'));
    phase='genuine-session-logout';await page.getByRole('button',{name:'Sign out',exact:true}).click();await catalog.waitFor({state:'hidden'});await policy.waitFor({state:'hidden'});
    fs.mkdirSync(path.dirname(evidencePath),{recursive:true});
    fs.writeFileSync(evidencePath,JSON.stringify({...sourceIdentity(root),actualCompiledUi:true,actualChromium:true,browserVersion:browser.version(),apiFixtures:false,tokenInjection:false,genuineKeycloakBrowserSession:true,realCatalogPreviewConfirmationVerifiedEffect:true,realDurableCatalogHistory:true,realPolicyValidationExplanationDryRun:true,realPolicyVersionWriteUiEffect:true,realCriticalActivationUiEffect:true,genuineTwoHumanBrowserApproval:true,requesterSelfApprovalDenied:true,reactivatedSamePolicyPreservesRuleSemantics:true,genuineSessionCrossScopeDenied:true,logoutClearsGovernanceState:true,loopbackFixtureBrowserCertificateErrorsIgnored:true,tlsConformanceClaimed:false,governanceRequests:requests.map(item=>({method:item.method,family:item.path.includes('/catalog/')?'catalog':'policy'}))},null,2));
    console.log('V09_GENUINE_GOVERNANCE_BROWSER_PASSED');await context.close();
  }catch{
    // Keep credentials, issuer URLs, authorization codes and tokens out of public CI logs.
    console.error('V09_GENUINE_GOVERNANCE_BROWSER_FAILED phase='+phase);process.exitCode=3;
  }finally{if(browser)await browser.close();}
})();
