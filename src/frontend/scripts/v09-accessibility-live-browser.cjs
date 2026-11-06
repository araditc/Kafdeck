// Actual compiled UI, genuine OIDC cookie actors and durable API assertions.
// No API response mocks or token injection. The host labels its durable ACK-loss fixture;
// actual native screen-reader observation is separate from automated browser evidence.
'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..'),sourceIdentity=require('./governance-browser-source.cjs');
const {chromium}=require(path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
(async()=>{
  let browser,probe,phase='input',lastStatus=0,result,evidencePath,applicationOrigin,signOutNames=[],diagnoseWindowReturn,manualStopDirectory;
  try{
    const [apiUrl,identityPath,scenarioPath,output]=process.argv.slice(2);evidencePath=output;
    if(!apiUrl||!identityPath||!scenarioPath||!output)throw Error('input');
    const read=file=>JSON.parse(fs.readFileSync(file,'utf8').replace(/^\uFEFF/,''));
    const api=new URL(apiUrl),identity=read(identityPath),scenario=read(scenarioPath),issuer=new URL(identity.issuer);
    if(scenario.manualHandoffDirectory)manualStopDirectory=path.resolve(scenario.manualHandoffDirectory);
    applicationOrigin=api.origin;
    for(const endpoint of [api,issuer])if(!['localhost','127.0.0.1','[::1]'].includes(endpoint.hostname))throw Error('loopback fixture only');
    if(!scenario.clusterId||!scenario.topicName||!identity.humanUsername||!identity.humanPasswordFile||!identity.approverUsername||!identity.approverPasswordFile)throw Error('scenario');
    fs.mkdirSync(path.dirname(output),{recursive:true});
    const launch={headless:process.env.KAFDECK_W79_HEADED!=='1',channel:'chromium'};if(process.env.KAFDECK_G2_BROWSER_EXECUTABLE)launch.executablePath=process.env.KAFDECK_G2_BROWSER_EXECUTABLE;
    browser=await chromium.launch(launch);
    result={...sourceIdentity(root),actualCompiledUi:true,actualChromium:true,browserVersion:browser.version(),apiFixtures:false,tokenInjection:false,genuineKeycloakBrowserSession:true,
      browserProfile:launch.headless?'headless':'headed',nativeWindowProofRequiredInManual:true,
      fixturePreparation:'Underlying immutable topic effect preview and exact kernel confirmation through authenticated cookie API; all product workflow controls use keyboard',
      loopbackFixtureBrowserCertificateErrorsIgnored:true,tlsConformanceClaimed:false,manualScreenReader:{status:'not-observed',producer:'separate actual AT issue log required'},mandatoryMissing:[]};
    probe=require('./v09-accessibility-probe.cjs')(root,result,{manualActionDelayMs:scenario.manualHandoffDirectory?1500:0});
    // Selectors use the same literal catalog as the product without loading or
    // changing application state.
    const messages=JSON.parse(fs.readFileSync(path.join(root,'src/frontend/src/shared/locales/fa.json'),'utf8'));
    signOutNames=['Sign out',messages['Sign out']].filter(Boolean);result.routeTrace=[];
    let locale='en';const local=(message,values=[])=>((locale==='fa'?messages[message]:null)??message).replace(/\{(\d+)\}/g,(token,index)=>values[Number(index)]===undefined?token:locale==='fa'?'\u2068'+String(values[Number(index)])+'\u2069':String(values[Number(index)]));
    const button=(panel,name,values)=>panel.getByRole('button',{name:local(name,values),exact:true});
    const dynamicButton=(panel,prefix,value)=>panel.getByRole('button',{name:local(prefix)+' '+value,exact:true});
    // Wrapping select labels include their option text in Playwright's label-text
    // matching. Keep the registered command label scoped and uniquely verified.
    const label=(panel,name)=>panel.getByLabel(local(name),{exact:name!=='Business command'});
    const article=panel=>panel.getByRole('article',{name:local('Immutable governance operation'),exact:true});
    async function signIn(target,user,passwordFile){
      target.on('response',response=>{const url=new URL(response.url());if(url.origin!==api.origin||!url.pathname.startsWith('/api/v1/governance/'))return;const family=url.pathname.split('/')[4];if(!['catalog','policies','requests','dashboards','rate-cards','measurements'].includes(family))return;result.routeTrace.push({family,method:['GET','POST','HEAD','OPTIONS'].includes(response.request().method())?response.request().method():'other',status:response.status()});result.routeTrace=result.routeTrace.slice(-40);});
      // Authentication automation is fixture setup; computer-use never handles passwords.
      await target.goto(new URL('/api/v1/auth/login',api).href,{waitUntil:'domcontentloaded'});await target.locator('#username').fill(user);await target.locator('#password').fill(fs.readFileSync(passwordFile,'utf8').trim());
      const returned=target.waitForURL(url=>url.origin===api.origin&&!url.pathname.includes('signin-oidc'),{timeout:60000});returned.catch(()=>{});await target.locator('#kc-login').click();await returned;await target.goto(api.origin,{waitUntil:'domcontentloaded'});
      await probe.select(target,target.locator('#cluster-selector'),scenario.clusterId,'authorized-cluster');
      if(locale==='fa')await probe.select(target,target.locator('#kafdeck-language'),'fa','persian-journey-locale');
    }
    async function get(ctx,pathname){const response=await ctx.request.get(new URL(pathname,api).href);lastStatus=response.status();assert.equal(lastStatus,200);return response.json();}
    async function handshake(name,metadata){
      if(!scenario.handshakeDirectory)return null;
      const directory=path.resolve(scenario.handshakeDirectory);fs.mkdirSync(directory,{recursive:true});
      const ready=path.join(directory,name+'-ready.json');if(fs.existsSync(ready))throw Error('stale fixture handshake');
      fs.writeFileSync(path.join(directory,name+'.json'),JSON.stringify(metadata));
      const deadline=Date.now()+180000;
      while(!fs.existsSync(ready)){if(Date.now()>deadline)throw Error('fixture handshake timeout');await new Promise(resolve=>setTimeout(resolve,250));}
      return read(ready);
    }
    async function manualNativeWindowCheckpoint(direction){
      const directory=path.resolve(scenario.manualHandoffDirectory),suffix='window-'+direction+'-'+locale;
      const resume=path.join(directory,'resume-'+suffix+'.json');if(fs.existsSync(resume))throw Error('stale native window handoff');
      fs.writeFileSync(path.join(directory,'ready-'+suffix+'.json'),JSON.stringify({locale,direction,actor:direction==='away'?'reviewer':'requester',sourceSha:result.sourceSha,authenticatedApplication:true,requiredInput:'actual native window activation; no synthetic focus event'}));
      const deadline=Date.now()+120000;while(!fs.existsSync(resume)){if(Date.now()>deadline)throw Error('native window handoff timeout');await new Promise(resolve=>setTimeout(resolve,250));}
    }
    const manualFocusSessions=[];
    let manualActionCursor=0;
    async function manualPause(actorPage,stage,metadata={}){
      if(!scenario.manualHandoffDirectory)return;
      const directory=path.resolve(scenario.manualHandoffDirectory);fs.mkdirSync(directory,{recursive:true});
      fs.writeFileSync(path.join(directory,'safe-authenticated-actors-'+locale+'.json'),JSON.stringify({locale,stage,actor:metadata.actor??'requester',sourceSha:result.sourceSha,authenticatedApplication:true,nativeFocusEmulationDisabled:!launch.headless,workflowResumed:false}));
      const focusActor=async()=>{await actorPage.bringToFront();await actorPage.locator('#service-requests').waitFor({state:'visible',timeout:120000});await actorPage.waitForFunction(()=>document.hasFocus(),undefined,{timeout:120000});};
      await focusActor();
      const suffix=locale+'-'+stage,resume=path.join(directory,'resume-'+suffix+'.json');if(fs.existsSync(resume))throw Error('stale AT handoff');
      const keyboardActionRange={from:manualActionCursor,to:result.keyboard.actions.length};
      const keyboardActionsSincePrevious=result.keyboard.actions.slice(manualActionCursor).filter(action=>typeof action.label==='string').map(({label,tabs,reverseSequenceVerified,firstDocumentEntry})=>({label,tabs,reverseSequenceVerified,firstDocumentEntry}));manualActionCursor=result.keyboard.actions.length;
      fs.writeFileSync(path.join(directory,'ready-'+suffix+'.json'),JSON.stringify({locale,stage,...metadata,sourceSha:result.sourceSha,browserVersion:result.browserVersion,authenticatedApplication:true,inputSource:'Playwright Chromium keyboard',keyboardInputMethod:result.keyboard.inputMethod,manualActionDelayMs:result.keyboard.manualActionDelayMs,keyboardActionRange,keyboardActionsSincePrevious}));
      const focusRequest=path.join(directory,'focus-'+suffix+'.json'),focusReady=path.join(directory,'focus-'+suffix+'-ready.json');
      const deadline=Date.now()+1800000;while(!fs.existsSync(resume)){
        if(Date.now()>deadline)throw Error('AT handoff timeout');
        if(fs.existsSync(focusRequest)&&!fs.existsSync(focusReady)){await focusActor();fs.writeFileSync(focusReady,JSON.stringify({locale,stage,actor:metadata.actor??'requester',sourceSha:result.sourceSha,actualActorFocused:true,authenticatedApplication:true,workflowResumed:false}));}
        await new Promise(resolve=>setTimeout(resolve,250));
      }
      await focusActor();
    }
    async function assertControlledAutomationCopy(target,language,stage){
      // These are the panel's two closed application-owned errors, never
      // arbitrary backend prose. Observe them without changing browser state.
      const keys=[
        'Automation capabilities are unavailable or denied. No effect is enabled.',
        'Request denied, stale, invalid or unavailable. No successful effect is inferred; retain the same operation/key and refresh status before deciding another action.',
      ];
      const panel=target.locator('#automation'),actual=(await panel.getByRole('alert').allTextContents()).map(text=>text.trim());
      if(!actual.length)return;
      const expected=keys.map(key=>language==='fa'?messages[key]:key);
      assert.ok(actual.every(text=>expected.includes(text)),'closed automation error did not follow current locale');
      for(const loading of ['Loading authorized automation capabilities…',messages['Loading authorized automation capabilities…']])assert.equal(await panel.getByText(loading,{exact:true}).count(),0,'automation failure still announced loading');
      result.controlledAutomationCopyChecks??=[];result.controlledAutomationCopyChecks.push({language,stage,errorCount:actual.length,closedApplicationCopy:true,loadingCleared:true});
    }
    async function assertControlledReadViewCopy(target,language,stage){
      const key='Not authorized for this read view.',expected=language==='fa'?messages[key]:key;
      for(const headingId of ['streams-title','lineage-title']){
        // Isolate the adjacent closed denial text from its status badge. Other
        // backend/technical Error prose is deliberately outside this assertion.
        const notice=target.locator('#'+headingId).locator('xpath=following-sibling::p[1][@role="alert"]');await notice.waitFor({state:'visible'});
        const actual=await notice.evaluate(element=>[...element.childNodes].filter(node=>node.nodeType===Node.TEXT_NODE).map(node=>node.textContent).join('').trim());
        assert.equal(actual,expected,'closed read-view denial did not follow current locale');
        result.controlledReadViewCopyChecks??=[];result.controlledReadViewCopyChecks.push({language,stage,headingId,closedDenialLocalized:true});
      }
    }
    async function preserveLocaleAcrossRoundTrip(target,panel,stage,scanBoth=false){
      const snapshot=()=>panel.evaluate(element=>({material:[...element.querySelectorAll('code,pre,bdi[dir="ltr"]')].map(node=>node.textContent),states:[...element.querySelectorAll('article dl')].map(dl=>dl.querySelectorAll('dd')[3]?.textContent),controls:[...element.querySelectorAll('input,textarea,select')].map(node=>({tag:node.tagName,type:node.getAttribute('type'),value:node.value,checked:node.checked,disabled:node.disabled}))}));
      const before=await snapshot();let mutationRequests=0;const listener=request=>{if(new URL(request.url()).origin===api.origin&&!['GET','HEAD','OPTIONS'].includes(request.method()))mutationRequests++;};target.on('request',listener);
      try{
        for(const next of [locale==='en'?'fa':'en',locale]){
          await probe.select(target,target.locator('#kafdeck-language'),next,stage+'-switch-'+next);await target.waitForFunction(next=>document.documentElement.lang===next&&document.documentElement.dir===(next==='fa'?'rtl':'ltr'),next);
          assert.deepEqual(await snapshot(),before,'locale changed frozen material or form/review state');assert.equal(await target.locator('#kafdeck-language').evaluate(element=>element===document.activeElement),true,'locale switch moved focus');assert.equal(mutationRequests,0,'locale emitted a mutation');
          await assertControlledAutomationCopy(target,next,stage);
          await assertControlledReadViewCopy(target,next,stage);
          if(scanBoth)await probe.scan(target,locale+'-'+stage+'-switch-'+next,output);
        }
      }finally{target.off('request',listener);}
      result.localeRoundTrips??=[];result.localeRoundTrips.push({journeyLocale:locale,stage,invariantMaterial:true,invariantInputsAndReview:true,languageControlRetainsFocus:true,mutatingRequests:mutationRequests,typedChallengeFields:await panel.locator('label').filter({hasText:local('Exact confirmation challenge:')}).locator('input').count()});
    }
    async function requestAction(target,control,suffix,expected=200){
      const waiting=target.waitForResponse(response=>{const url=new URL(response.url());return url.origin===api.origin&&url.pathname.endsWith(suffix)&&response.request().method()==='POST';});
      waiting.catch(()=>{});
      await probe.activate(target,control,'activate-'+suffix.split('/').at(-1));const response=await waiting;lastStatus=response.status();assert.equal(lastStatus,expected);return response.json();
    }
    async function applied(target,review){
      await review.waitFor();await probe.check(target,label(review,'I inspected this immutable operation, scope, material and fingerprint.'),'acknowledge-exact-material');
      const challenge=review.locator('label').filter({hasText:local('Exact confirmation challenge:')});if(await challenge.count())await probe.text(target,challenge.locator('input'),(await challenge.locator('code').textContent()).trim(),'exact-challenge');
      await requestAction(target,button(review,'Confirm exact preview'),'/confirm');await requestAction(target,button(review,'Execute reviewed operation'),'/execute');await review.getByText('appliedVerified',{exact:false}).waitFor();
    }
    let context,page;async function boundEffect(){
      const csrf=await get(context,'/api/v1/auth/csrf');
      const response=await context.request.post(new URL(`/api/v1/clusters/${encodeURIComponent(scenario.clusterId)}/topics/${encodeURIComponent(scenario.topicName)}/mutations/create/preview`,api).href,
        {headers:{[csrf.headerName]:csrf.requestToken,'Idempotency-Key':'w79-topic-'+crypto.randomUUID()},data:{partitionCount:1,replicationFactor:1,configurations:{}}});
      lastStatus=response.status();assert.equal(lastStatus,201);const operation=await response.json();assert.match(operation.operationId,/^[a-f0-9-]{36}$/i);
      const confirmed=await context.request.post(new URL('/api/v1/mutations/'+operation.operationId+'/confirm',api).href,{headers:{[csrf.headerName]:csrf.requestToken},data:{previewHash:operation.previewHash,typedTargetChallenge:operation.confirmationChallenge??null}});lastStatus=confirmed.status();assert.equal(lastStatus,200);assert.equal((await confirmed.json()).state,'ready');return operation;
    }
    result.localeJourneys=[];
    for(locale of ['en','fa']){
    context=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}});page=await context.newPage();page.setDefaultTimeout(30000);result.keyboard.actions.push({locale,begin:true});
    phase='genuine-cookie-identity-'+locale;await signIn(page,identity.humanUsername,identity.humanPasswordFile);
    const second=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}}),approver=await second.newPage();approver.setDefaultTimeout(30000);await signIn(approver,identity.approverUsername,identity.approverPasswordFile);await page.bringToFront();
    phase='authoritative-opaque-session-scope-'+locale;
    const requesterSession=await get(context,'/api/v1/auth/session'),requesterAgain=await get(context,'/api/v1/auth/session'),reviewerSession=await get(second,'/api/v1/auth/session');
    assert.match(requesterSession.sessionScope,/^[a-f0-9]{64}$/i);assert.equal(requesterAgain.sessionScope,requesterSession.sessionScope);assert.match(reviewerSession.sessionScope,/^[a-f0-9]{64}$/i);assert.notEqual(reviewerSession.sessionScope,requesterSession.sessionScope);result.stableOpaqueActorScope=true;result.distinctHumanOpaqueScopes=true;
    const catalog=page.locator('#ownership-catalog'),requests=page.locator('#service-requests');await page.waitForFunction(()=>{
      const catalogForm=document.querySelector('#ownership-catalog fieldset'),requestForm=document.querySelector('#service-requests fieldset');
      return !!catalogForm&&!!requestForm&&!catalogForm.disabled&&!requestForm.disabled;
    });
    if(scenario.manualHandoffDirectory&&!launch.headless){
      for(const [actor,actorContext,actorPage] of [['requester',context,page],['reviewer',second,approver]]){
        const session=await actorContext.newCDPSession(actorPage);manualFocusSessions.push(session);
        // Remove Playwright's default simulated focus for the entire manual
        // journey. Native activation must produce the real browser events.
        await session.send('Emulation.setFocusEmulationEnabled',{enabled:false});
        result.manualNativeFocusConfiguration??=[];result.manualNativeFocusConfiguration.push({locale,actor,method:'Emulation.setFocusEmulationEnabled',enabled:false,scope:'entire authenticated manual journey',syntheticFocusEvents:false});
      }
    }
    await manualPause(page,'before-actions');
    phase='keyboard-skip-main-'+locale;await probe.activate(page,page.getByRole('link',{name:local('Skip to main content'),exact:true}),'skip-to-main');assert.equal(await page.locator('#kafdeck-main').evaluate(element=>element===document.activeElement),true);result.keyboardSkipToMain??=[];result.keyboardSkipToMain.push({locale,method:'Tab then Enter',activeId:'kafdeck-main'});
    phase='keyboard-catalog-escaped-input';const escapedName='w79-'+crypto.randomUUID().slice(0,8)+' <w79-escaped>& " فارسی';
    await probe.text(page,label(catalog,'Name'),escapedName,'catalog-name');await probe.text(page,label(catalog,'Description'),'Keyboard catalog; no markup interpretation','catalog-description');
    const catalogPreview=await requestAction(page,button(catalog,'Preview exact metadata change'),'/teams/preview',201);let review=article(catalog);await applied(page,review);
    await probe.activate(page,button(catalog,'Reload authorized catalog'),'catalog-reload');await probe.activate(page,dynamicButton(catalog,'Inspect',escapedName),'inspect-created-catalog');
    assert.equal(await label(catalog,'Name').inputValue(),escapedName);assert.equal(await page.locator('w79-escaped').count(),0);
    await probe.activate(page,button(catalog,'Read immutable record history'),'catalog-history');await catalog.getByRole('table',{name:local('Immutable revisions; latest 1000 loaded history entries retained')}).waitFor();
    result.catalogKeyboardAppliedAndHistory=true;result.escapedCatalogInterpolation=true;
    await manualPause(page,'catalog');
    if(scenario.handshakeDirectory){
      phase='fixture-catalog-ack-loss';const operationId=catalogPreview.operationId??catalogPreview.operation?.operationId;
      assert.match(operationId,/^[a-f0-9-]{36}$/i);const currentPlan=await get(context,'/api/v1/governance/catalog/operations/'+operationId);
      const ready=await handshake('catalog-applied-'+locale,{operationId,previewHash:currentPlan.operation.previewHash,catalogRecordId:currentPlan.changes[0].id});assert.equal(ready.operationId,operationId);scenario.unknownCatalogOperationId=operationId;
    }
    phase='keyboard-request-draft';const target=await boundEffect(),expiry=new Date(Math.min(Date.parse(target.previewExpiresAtUtc)-1000,scenario.requestExpirySeconds?Date.now()+scenario.requestExpirySeconds*1000:Infinity)).toISOString();
    await probe.text(page,label(requests,'Existing immutable effect operation ID'),target.operationId,'bound-operation');
    await probe.text(page,label(requests,'Request expiry UTC (ISO8601 ending in Z)'),expiry,'invariant-utc-expiry');await probe.text(page,label(requests,'Business reason'),'W79 keyboard exact immutable revision','business-reason');
    await requestAction(page,button(requests,'Preview governed request change'),'/requests/changes/preview');review=article(requests);await applied(page,review);
    let listing=await get(context,'/api/v1/governance/requests?'+new URLSearchParams({clusterId:scenario.clusterId,pageSize:'200'}));const matching=listing.items.filter(item=>item.operationId===target.operationId);assert.equal(matching.length,1);const requestId=matching[0].requestId;result.requestId=requestId;
    await probe.activate(page,button(requests,'Reload authorized requests'),'request-reload');await probe.activate(page,dynamicButton(requests,'Inspect request',requestId),'inspect-request');
    phase='window-return-selected-before-'+locale;
    if(scenario.manualHandoffDirectory&&!launch.headless){
      // Chromium can restore the original DevTools session's focus override
      // after the initial manual setup. Remove it again before preparing the
      // populated form and recording any native-window evidence. Configuration
      // transitions must never count as the real away/return proof below.
      const nativeFocus=()=>Promise.all([page,approver].map(actorPage=>actorPage.evaluate(()=>({documentHasFocus:document.hasFocus(),visibility:document.visibilityState}))));
      const before=await nativeFocus();
      for(const [index,session] of manualFocusSessions.slice(-2).entries()){
        await session.send('Emulation.setFocusEmulationEnabled',{enabled:false});
        result.manualNativeFocusConfiguration.push({locale,actor:index===0?'requester':'reviewer',method:'Emulation.setFocusEmulationEnabled',enabled:false,scope:'before native-window proof setup',syntheticFocusEvents:false});
      }
      result.manualNativeFocusBoundaries??=[];result.manualNativeFocusBoundaries.push({locale,before,after:await nativeFocus(),beforeNativeEventRecorder:true,syntheticFocusEvents:false});
    }
    const selectedRevision=requests.getByRole('article',{name:local('Selected immutable request revision'),exact:true});await selectedRevision.waitFor();assert.equal(await selectedRevision.locator('code').first().textContent(),requestId);
    await probe.text(page,label(requests,'Business reason'),'W79 typed field survives real window return','window-return-populated-reason');
    const selectedForm=async()=>({requestId:await selectedRevision.locator('code').first().textContent(),material:await selectedRevision.locator('code,pre,bdi[dir="ltr"]').allTextContents(),reason:await label(requests,'Business reason').inputValue(),key:await label(requests,'Stable request change idempotency key').inputValue(),command:await label(requests,'Business command').inputValue()});
    const populatedBefore=await selectedForm(),reasonHandle=await label(requests,'Business reason').elementHandle();
    const windowEvents=await page.evaluateHandle(()=>{
      const events=[],record=event=>{events.push({event,elapsedMs:performance.now(),documentHasFocus:document.hasFocus(),visibility:document.visibilityState});if(events.length>20)events.shift();};
      const focus=()=>record('focus'),blur=()=>record('blur'),visibility=()=>record('visibilitychange');window.addEventListener('focus',focus);window.addEventListener('blur',blur);document.addEventListener('visibilitychange',visibility);record('initial');
      return {events,stop:()=>{window.removeEventListener('focus',focus);window.removeEventListener('blur',blur);document.removeEventListener('visibilitychange',visibility);}};
    });
    result.windowReturnCurrent={locale,profile:launch.headless?'headless':'headed',nativeWindowProofClaimed:false,sessionGetStatus:null,opaqueScopeMatched:null};
    diagnoseWindowReturn=async()=>({
      ...result.windowReturnCurrent,selectedMatchCount:await selectedRevision.count(),selectedVisible:await selectedRevision.isVisible(),
      original:await reasonHandle.evaluate(element=>({connected:element.isConnected,currentActiveEqualsOriginal:element===document.activeElement,documentHasFocus:document.hasFocus(),visibility:document.visibilityState,inertAncestor:!!element.closest('[inert]'),hiddenAncestor:!!element.closest('[hidden]'),activeTag:document.activeElement?.tagName??null,activeType:document.activeElement?.getAttribute('type')??null})),
      actualWindowEvents:await windowEvents.evaluate(state=>state.events),
    });
    phase='window-return-switch-away-'+locale;
    if(scenario.manualHandoffDirectory&&!launch.headless)await manualNativeWindowCheckpoint('away');else await approver.bringToFront();
    if(!launch.headless){
      const returningSession=page.waitForResponse(response=>{const url=new URL(response.url());return url.origin===api.origin&&url.pathname==='/api/v1/auth/session'&&response.request().method()==='GET';},{timeout:scenario.manualHandoffDirectory?120000:30000});returningSession.catch(()=>{});
      phase='window-return-session-response-'+locale;if(scenario.manualHandoffDirectory)await manualNativeWindowCheckpoint('return');else await page.bringToFront();const revalidated=await returningSession;lastStatus=revalidated.status();result.windowReturnCurrent.sessionGetStatus=lastStatus;assert.equal(lastStatus,200);result.windowReturnCurrent.opaqueScopeMatched=(await revalidated.json()).sessionScope===requesterSession.sessionScope;assert.equal(result.windowReturnCurrent.opaqueScopeMatched,true);
      const events=await windowEvents.evaluate(state=>state.events);assert.ok(events.some(event=>event.event==='blur')&&events.some(event=>event.event==='focus'),'headed native window events missing');result.windowReturnCurrent.nativeWindowProofClaimed=true;
    }else{
      await page.bringToFront();result.windowReturnCurrent.capability='native-window-focus-unsupported-not-claimed';result.mandatoryMissing.push('headed actual native window focus and session revalidation proof: '+locale);
    }
    phase='window-return-selected-visible-'+locale;await selectedRevision.waitFor({state:'visible'});
    phase='window-return-original-focus-'+locale;await page.waitForFunction(element=>element?.isConnected&&element===document.activeElement,reasonHandle);
    phase='window-return-preserved-values-'+locale;
    assert.deepEqual(await selectedForm(),populatedBefore);result.windowReturnChecks??=[];const windowCheck={locale,profile:launch.headless?'headless':'headed',actualWindowSwitch:!launch.headless,nativeEventsVerified:!launch.headless,sessionHttpStatus:result.windowReturnCurrent.sessionGetStatus,opaqueScopeStable:result.windowReturnCurrent.opaqueScopeMatched,selectedMaterialAndInputsPreserved:true,originalInputNodeAndFocusPreserved:true,actualWindowEvents:await windowEvents.evaluate(state=>state.events)};result.windowReturnChecks.push(windowCheck);
    if(!launch.headless){result.actualWindowReturnPreservesSelectedRequestAndTypedFields??=[];result.actualWindowReturnPreservesSelectedRequestAndTypedFields.push(windowCheck);}
    result.windowReturnCurrent=await diagnoseWindowReturn();await windowEvents.evaluate(state=>state.stop());diagnoseWindowReturn=null;
    await probe.select(page,label(requests,'Business command'),'Submit','submit-command');await requestAction(page,button(requests,'Preview governed request change'),'/requests/changes/preview');await applied(page,review);
    await probe.activate(page,button(requests,'Refresh selected request'),'refresh-submitted-request');
    await manualPause(page,'submitted',{requestId,operationId:target.operationId});
    phase='keyboard-self-approval-denied';await probe.select(page,label(requests,'Business command'),'Approve','self-approve-command');await requestAction(page,button(requests,'Preview governed request change'),'/requests/decisions/preview',403);await requests.getByRole('alert').waitFor();result.requesterSelfDecisionDenied=true;
    phase='keyboard-independent-human-approval';await approver.bringToFront();
    const decisions=approver.locator('#service-requests');
    const reviewerReload=approver.waitForResponse(response=>{const url=new URL(response.url());return url.origin===api.origin&&url.pathname==='/api/v1/governance/requests'&&response.request().method()==='GET';});reviewerReload.catch(()=>{});
    await probe.activate(approver,button(decisions,'Reload authorized requests'),'independent-request-reload');lastStatus=(await reviewerReload).status();assert.equal(lastStatus,200);
    await probe.activate(approver,dynamicButton(decisions,'Inspect request',requestId),'independent-inspect');await probe.select(approver,label(decisions,'Business command'),'Approve','independent-approve-command');
    await probe.text(approver,label(decisions,'Business reason'),'W79 distinct current human exact revision','approval-reason');await requestAction(approver,button(decisions,'Preview governed request change'),'/requests/decisions/preview');await article(decisions).waitFor();await manualPause(approver,'reviewer-before-approval',{requestId,actor:'reviewer'});
    await applied(approver,article(decisions));await second.close();await page.bringToFront();
    await probe.activate(page,button(requests,'Refresh selected request'),'refresh-approved');const current=await get(context,`/api/v1/governance/requests/${requestId}?`+new URLSearchParams({clusterId:scenario.clusterId}));assert.equal(current.businessState.toLowerCase(),'approved');assert.equal(current.decisions.length,1);assert.equal(current.operationId,target.operationId);result.keyboardDraftSubmitDistinctHumanApprove=true;
    await probe.activate(page,button(requests,'Inspect linked effect operation'),'approved-linked-effect');const approvedEffect=article(requests).first();await probe.check(page,label(approvedEffect,'I inspected this immutable operation, scope, material and fingerprint.'),'approved-reviewed-kernel-ready');assert.equal(await button(approvedEffect,'Execute reviewed operation').isEnabled(),true);result.approvedBusinessAndKernelGatesAllowExplicitDispatch=true;
    const typedChallenge=approvedEffect.locator('label').filter({hasText:local('Exact confirmation challenge:')});if(await typedChallenge.count())await probe.text(page,typedChallenge.locator('input'),(await typedChallenge.locator('code').textContent()).trim(),'preserved-exact-typed-challenge');
    await preserveLocaleAcrossRoundTrip(page,requests,'approved-selected-request');
    await manualPause(page,'approved',{requestId,operationId:target.operationId});
    phase='desktop-scan-'+locale;await probe.scan(page,locale+'-desktop',output);
    phase='rtl-material-invariance';const materialBefore=await requests.locator('article').first().locator('code,pre').allTextContents();let localeWrites=0;const onRequest=request=>{if(new URL(request.url()).origin===api.origin&&!['GET','HEAD','OPTIONS'].includes(request.method()))localeWrites++;};page.on('request',onRequest);
    await probe.select(page,page.locator('#kafdeck-language'),locale==='en'?'fa':'en','switch-locale');assert.deepEqual(await requests.locator('article').first().locator('code,pre').allTextContents(),materialBefore);assert.equal(await catalog.locator('input').first().inputValue(),escapedName);assert.equal(localeWrites,0);
    await probe.select(page,page.locator('#kafdeck-language'),locale,'restore-journey-locale');page.off('request',onRequest);await page.waitForFunction(locale=>document.documentElement.lang===locale&&document.documentElement.dir===(locale==='fa'?'rtl':'ltr'),locale);result.localePreservesFrozenMaterialAndMakesNoMutation=true;
    await page.setViewportSize({width:320,height:800});await probe.tabTo(page,page.locator('#kafdeck-language'),'mobile-language-focus');await probe.scan(page,locale+'-mobile-320',output);await page.setViewportSize({width:1440,height:1000});
    if(scenario.handshakeDirectory&&scenario.requestExpirySeconds){
      phase='actual-request-expiry-'+locale;const ready=await handshake('request-submitted-'+locale,{requestId,operationId:target.operationId,expiresAtUtc:expiry});assert.equal(ready.requestId,requestId);
      await probe.activate(page,button(requests,'Refresh selected request'),'expired-revision-refresh');const expired=await get(context,`/api/v1/governance/requests/${requestId}?`+new URLSearchParams({clusterId:scenario.clusterId}));assert.equal(expired.businessState.toLowerCase(),'expired');result.actualExpiryObservedBeforeReplan=true;
      await probe.activate(page,button(requests,'Inspect linked effect operation'),'expired-linked-effect');const blocked=article(requests).first();await probe.check(page,label(blocked,'I inspected this immutable operation, scope, material and fingerprint.'),'expired-reviewed-kernel-ready');assert.equal(await button(blocked,'Execute reviewed operation').isDisabled(),true);
      await manualPause(page,'expired',{requestId});
    }
    phase='keyboard-explicit-replan';const revised=await boundEffect();await probe.activate(page,button(requests,'Refresh selected request'),'refresh-before-revision');await probe.select(page,label(requests,'Business command'),'Revise','revise-command');
    await probe.text(page,label(requests,'Existing immutable effect operation ID'),revised.operationId,'replan-new-operation');await probe.text(page,label(requests,'Request expiry UTC (ISO8601 ending in Z)'),new Date(Date.parse(revised.previewExpiresAtUtc)-1000).toISOString(),'replan-expiry');
    await probe.activate(page,button(requests,'Create key for an intentionally new request change'),'replan-new-change-key');await requestAction(page,button(requests,'Preview governed request change'),'/requests/changes/preview');await applied(page,review);
    const after=await get(context,`/api/v1/governance/requests/${requestId}?`+new URLSearchParams({clusterId:scenario.clusterId}));assert.equal(after.revision,current.revision+1);assert.equal(after.operationId,revised.operationId);assert.equal(after.decisions.length,0);result.keyboardExplicitRevisionInvalidatesOldDecision=true;
    await manualPause(page,'replanned',{requestId,operationId:revised.operationId});
    // A real producer must supply a fault-created durable unknown operation.
    // Reading it is safe; we do not manufacture state or infer unknown from timeouts.
    phase='durable-unknown-read';if(scenario.unknownCatalogOperationId){
      const unknown=await get(context,'/api/v1/governance/catalog/operations/'+encodeURIComponent(scenario.unknownCatalogOperationId));assert.equal(unknown.operation.state,'executionUnknown');
      await probe.text(page,label(catalog,'Existing catalog operation ID'),scenario.unknownCatalogOperationId,'unknown-same-operation-id');await probe.activate(page,button(catalog,'Inspect frozen catalog operation'),'unknown-inspect-same');
      const unknownReview=article(catalog);await unknownReview.getByText(local('Execution outcome is unknown. Do not retry blindly; inspect positive evidence and reconcile before any follow-up.'),{exact:true}).waitFor();assert.equal(await button(unknownReview,'Execute reviewed operation').isDisabled(),true);assert.equal(await button(catalog,'Create key for an intentionally new catalog request').isDisabled(),true);result.keyboardActualDurableUnknownInspected=true;
      await probe.scan(page,locale+'-durable-unknown',output);
      await probe.check(page,label(unknownReview,'I inspected this immutable operation, scope, material and fingerprint.'),'unknown-preserved-review-acknowledgment');
      await preserveLocaleAcrossRoundTrip(page,catalog,'unknown-frozen-operation',true);
      await manualPause(page,'unknown',{operationId:scenario.unknownCatalogOperationId});
      const historyPath='/api/v1/governance/catalog/teams/'+encodeURIComponent(unknown.changes[0].id)+'/history?'+new URLSearchParams({clusterId:scenario.clusterId,pageSize:'200'});const beforeRecovery=await get(context,historyPath);assert.equal(beforeRecovery.items.length,1);assert.equal(beforeRecovery.items[0].operationId,scenario.unknownCatalogOperationId);
      await probe.check(page,label(unknownReview,'I inspected this immutable operation, scope, material and fingerprint.'),'unknown-review-positive-evidence');await requestAction(page,button(unknownReview,'Verify positive receipt; never redispatch'),'/recover');await unknownReview.getByText('appliedVerified',{exact:false}).waitFor();assert.equal(await button(unknownReview,'Verify positive receipt; never redispatch').isDisabled(),true);result.keyboardActualUnknownPositiveReceiptRecovery=true;
      const csrf=await get(context,'/api/v1/auth/csrf');const repeated=await context.request.post(new URL('/api/v1/governance/catalog/operations/'+encodeURIComponent(scenario.unknownCatalogOperationId)+'/recover',api).href,{headers:{[csrf.headerName]:csrf.requestToken},data:{previewHash:unknown.operation.previewHash}});lastStatus=repeated.status();assert.equal(lastStatus,200);const repeatedState=await repeated.json();assert.equal(repeatedState.operationId,scenario.unknownCatalogOperationId);assert.equal(repeatedState.state,'appliedVerified');const afterRecovery=await get(context,historyPath);assert.deepEqual(afterRecovery,beforeRecovery);result.actualAuthenticatedRecoveryRepeatedWithoutDuplicateHistory=true;
      await manualPause(page,'recovered',{operationId:scenario.unknownCatalogOperationId});
    }else result.mandatoryMissing.push('actual fault-created durable unknown operation keyboard journey');
    phase='browser-emulated-preferences-'+locale;await probe.preferences(page,locale,output);
    result.localeJourneys.push({locale,catalog:true,submit:true,approveDistinctHuman:true,explicitReplan:true,unknown:!!scenario.unknownCatalogOperationId});await context.close();
    }
    phase='actual-unsupported-locale-fallback';const fallback=await browser.newContext({ignoreHTTPSErrors:true});const fallbackPage=await fallback.newPage();await fallbackPage.goto(api.origin,{waitUntil:'domcontentloaded'});
    await fallbackPage.evaluate(()=>localStorage.setItem('kafdeck.locale.v1','unsupported-w79'));await fallbackPage.reload({waitUntil:'domcontentloaded'});await fallbackPage.waitForFunction(()=>document.documentElement.lang==='en'&&document.documentElement.dir==='ltr');assert.equal(await fallbackPage.locator('#kafdeck-language').inputValue(),'en');result.actualUnsupportedLocaleFallsBackToEnglish=true;await fallback.close();
    result.mandatoryMissing.push('observed English and Persian actual screen-reader full journeys (separate manual log)');
    result.status='automated-and-keyboard-passed-manual-acceptance-open';fs.writeFileSync(output,JSON.stringify(result,null,2));console.log('V09_W79_KEYBOARD_AXE_PASSED_MANUAL_OPEN');
  }catch(error){
    if(result&&evidencePath){
      const closedCategories=new Map([['keyboard target unreachable','keyboard-target-unreachable'],['disabled keyboard target','keyboard-target-disabled']]);
      const category=closedCategories.get(error?.message)??(error?.name==='TimeoutError'?'automation-timeout':error?.name==='AssertionError'?'assertion':'closed-producer-failure');
      result.status='failed';result.failure={phase,lastStatus,category,probe:probe?await probe.diagnose():null};
      if(diagnoseWindowReturn){try{result.failure.windowReturn=await diagnoseWindowReturn();}catch{result.failure.windowReturn={unavailable:true};}}
      try{const target=probe?.getCurrentPage();if(target&&!target.isClosed()){const url=new URL(target.url());if(url.origin===applicationOrigin&&url.pathname==='/'&&await target.locator('input[type="password"]').count()===0&&await target.locator('#service-requests').isVisible()&&await target.getByRole('button',{name:new RegExp('^(?:'+signOutNames.map(name=>name.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')).join('|')+')$')}).count()===1){const screenshot=evidencePath.replace(/\.json$/,'-failure.png');await target.screenshot({path:screenshot,fullPage:true,timeout:5000});result.failure.screenshot=path.basename(screenshot);}}}catch{result.failure.screenshotUnavailable=true;}
      fs.writeFileSync(evidencePath,JSON.stringify(result,null,2));
    }
    console.error('V09_W79_ACCESSIBILITY_FAILED phase='+phase+' status='+lastStatus);process.exitCode=3;
  }finally{
    let safeToClose=true;
    if(browser&&manualStopDirectory){
      safeToClose=false;
      try{
        const request=path.join(manualStopDirectory,'owned-reader-stop-request.json'),ready=path.join(manualStopDirectory,'owned-reader-stop-ready.json');
        fs.writeFileSync(request,JSON.stringify({readerStopRequiredBeforeBrowserClose:true}));
        const deadline=Date.now()+10000;while(!fs.existsSync(ready)&&Date.now()<deadline)await new Promise(resolve=>setTimeout(resolve,100));
        if(fs.existsSync(ready))safeToClose=JSON.parse(fs.readFileSync(ready,'utf8').replace(/^\uFEFF/,'')).ownedReaderStopped===true;
      }catch{}
      if(!safeToClose){console.error('V09_W79_READER_STOP_UNVERIFIED_BROWSER_RETAINED');process.exitCode=3;}
    }
    if(browser&&safeToClose)await browser.close();
  }
})();
