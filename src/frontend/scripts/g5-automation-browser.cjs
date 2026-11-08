// Actual compiled-UI Chromium proof with explicit API fixtures, not authentication/effect proof.
const {createServer}=require('node:http');
const fs=require('node:fs'); const path=require('node:path'); const assert=require('node:assert/strict');
assert.ok(Number(process.versions.node.split('.')[0])>=24,'G5 frontend acceptance requires supported Node24 or later.');
const root=path.resolve(__dirname,'../../..');
const {chromium}=require(path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
const dist=path.join(root,'src/frontend/dist'); const evidence=process.env.KAFDECK_G5_UI_EVIDENCE||path.join(root,'artifacts/g5/automation-ui-evidence.json');
const id='11111111-1111-4111-8111-111111111111'; const hash='a'.repeat(64);
const server=createServer((req,res)=>{
  const name=decodeURIComponent(new URL(req.url,'http://localhost').pathname); const file=path.resolve(dist,name==='/'?'index.html':'.'+name);
  if(!file.startsWith(dist+path.sep)||!fs.existsSync(file)||!fs.statSync(file).isFile()){res.writeHead(404);res.end();return;}
  const type={'.html':'text/html','.js':'text/javascript','.css':'text/css','.webp':'image/webp','.woff2':'font/woff2'}[path.extname(file)]||'application/octet-stream';
  res.writeHead(200,{'Content-Type':type});fs.createReadStream(file).pipe(res);
});
(async()=>{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  const browser=await chromium.launch({headless:true,channel:'chromium'}); let effects=0,observations=0,authenticated=true,denied=false,state='awaitingApproval'; const outgoing=[];
  try{
    const context=await browser.newContext({viewport:{width:1440,height:1000}});
    await context.route('**/*',async route=>{
      const request=route.request();const url=new URL(request.url());
      if(url.origin!==origin){outgoing.push(url.origin);return route.abort();}
      if(!url.pathname.startsWith('/api/v1/'))return route.continue();
      const send=(value,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(value)});
      if(url.pathname==='/api/v1/auth/mode')return send({authenticationMode:'oidc',connectionWorkspace:false});
      if(url.pathname==='/api/v1/auth/session')return send(authenticated?{authenticated:true,authenticationMode:'oidc',displayName:'UI fixture operator',email:null,authenticatedAt:'2026-11-05T00:00:00Z'}:{title:'Authentication required'},authenticated?200:401);
      if(url.pathname==='/api/v1/auth/csrf')return send({headerName:'X-Kafdeck-CSRF',requestToken:'fixture-csrf'});
      if(url.pathname==='/api/v1/clusters')return send({data:[]});
      if(url.pathname==='/api/v1/declarative/capabilities')return denied?send({title:'Denied'},403):send({enabled:true,apiVersion:'kafdeck.io/v0.8',families:[{kind:'NotificationSubscription',apply:true,delete:false}]});
      if(url.pathname==='/api/v1/declarative/observe'){
        assert.equal(request.headers()['x-kafdeck-csrf'],'fixture-csrf'); observations++;
        return send({drift:'OutOfSync',ownership:'Managed',ownershipRevision:3,subscriptionRevision:3,sourceInSync:false,deploymentGeneration:id,ownershipGeneration:id,sourceGeneration:id,observedAtUtc:'2026-11-05T00:00:00Z',observedFingerprint:hash});
      }
      if(url.pathname==='/api/v1/declarative/plans/'+id)return send({planId:id,planFingerprint:hash,deploymentId:'fixture',resourceId:'subscription',desiredFingerprint:hash,expectedOwnershipRevision:3,expiresAtUtc:'2026-11-05T01:00:00Z',ownerId:'team-ops',sourceId:'registered-source',sourceRevisionHash:hash,deploymentGeneration:id,before:{state:'Active'},desired:{state:'Paused',untrusted:'<img src=x onerror="window.__unsafe=true">'},after:{state:'Paused'},changes:['UpdateSubscription'],risk:'Critical',requiresIndependentApproval:true,state,command:'Apply'});
      if(url.pathname==='/api/v1/mutations/'+id)return send({operationId:id,clusterId:'fixture',operationKind:'declarativeNotificationSubscription',riskClass:'critical',confirmationMode:'typedTarget',requiresIndependentApproval:true,requiresExecutionMaterial:false,state,previewHash:hash,previewExpiresAtUtc:'2026-11-05T01:00:00Z',confirmationChallenge:'subscription',resultCode:null,safeProviderEvidence:{},version:1,createdAtUtc:'2026-11-05T00:00:00Z',updatedAtUtc:'2026-11-05T00:00:00Z'});
      if(url.pathname.endsWith('/apply')||url.pathname.endsWith('/execute'))effects++;
      return send({title:'Fixture endpoint unavailable'},403);
    });
    const page=await context.newPage();await page.goto(origin);
    await page.getByRole('link',{name:'Automation',exact:true}).click();
    await page.getByRole('heading',{name:'Governed automation',exact:true}).waitFor();
    const lookup=page.getByLabel('Exact plan / operation UUID');await lookup.fill(id);await lookup.press('Tab');
    assert.match(await page.evaluate(()=>document.activeElement.textContent),/Refresh immutable plan/);
    await page.getByRole('button',{name:'Refresh immutable plan and status',exact:true}).click();
    await page.getByRole('article',{name:'Immutable plan review'}).waitFor();
    assert.match(await page.getByRole('article',{name:'Immutable plan review'}).textContent(),/Waiting for a distinct eligible human/);
    assert.equal(await page.locator('img[src="x"]').count(),0);assert.equal(await page.evaluate(()=>window.__unsafe),undefined);
    const measureContrast=element=>{
      const style=getComputedStyle(element);
      const luminance=color=>{
        // Resolve CSS Color4 (including Tabler OKLCH) into sRGB before WCAG math.
        const canvas=document.createElement('canvas');canvas.width=1;canvas.height=1;
        const context=canvas.getContext('2d');context.fillStyle=color;context.fillRect(0,0,1,1);
        const channels=Array.from(context.getImageData(0,0,1,1).data).slice(0,3).map(value=>{const c=value/255;return c<=0.04045?c/12.92:((c+0.055)/1.055)**2.4;});
        return channels[0]*0.2126+channels[1]*0.7152+channels[2]*0.0722;
      };
      const foreground=luminance(style.color),background=luminance(style.backgroundColor);
      return {ratio:(Math.max(foreground,background)+0.05)/(Math.min(foreground,background)+0.05),foreground:style.color,background:style.backgroundColor};
    };
    const contrast=await page.locator('pre.automation-proof').first().evaluate(measureContrast);
    assert.ok(contrast.ratio>=4.5,'Immutable plan text must have readable foreground/background contrast: '+JSON.stringify(contrast));
    await page.emulateMedia({colorScheme:'dark'});await page.waitForFunction(()=>document.documentElement.dataset.bsTheme==='dark');
    const darkContrast=await page.locator('pre.automation-proof').first().evaluate(measureContrast);
    assert.ok(darkContrast.ratio>=4.5,'Dark-mode immutable plan contrast must remain readable: '+JSON.stringify(darkContrast));
    await page.emulateMedia({colorScheme:'light'});await page.waitForFunction(()=>document.documentElement.dataset.bsTheme==='light');
    const apply=page.getByRole('button',{name:'Apply through backend governance',exact:true});assert.equal(await apply.isDisabled(),true);
    state='executionUnknown';await page.getByRole('button',{name:'Refresh immutable plan and status',exact:true}).click();
    await page.getByText(/Unknown effect — retain/).waitFor();assert.equal(await apply.isDisabled(),true);
    await page.getByLabel('I inspected this exact immutable plan',{exact:false}).check();assert.equal(await apply.isDisabled(),true);
    await page.getByLabel('Source reference',{exact:true}).fill('registered-source');
    await page.getByLabel('Desired canonical SHA256').fill(hash);
    await page.getByLabel('NotificationSubscription manifest',{exact:false}).fill('{}');
    await page.getByRole('button',{name:'Observe ownership and drift',exact:true}).click();
    await page.getByText('OutOfSync',{exact:true}).waitFor();assert.equal(observations,1);assert.equal(effects,0);
    await page.setViewportSize({width:390,height:844});await page.locator('#automation').scrollIntoViewIfNeeded();
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),true);
    fs.mkdirSync(path.dirname(evidence),{recursive:true});await page.screenshot({path:evidence.replace(/\.json$/,'-mobile.png'),fullPage:true});
    authenticated=false;await page.evaluate(()=>window.dispatchEvent(new Event('focus')));
    await page.getByRole('heading',{name:'Governed automation',exact:true}).waitFor({state:'hidden'});
    authenticated=true;denied=true;await page.reload();await page.getByText('Automation capabilities are unavailable or denied.',{exact:false}).waitFor();
    assert.equal(await page.getByRole('button',{name:'Observe ownership and drift',exact:true}).isDisabled(),true);
    assert.equal(effects,0);assert.deepEqual(outgoing,[]);
    fs.mkdirSync(path.dirname(evidence),{recursive:true});fs.writeFileSync(evidence,JSON.stringify({actualCompiledUi:true,actualChromium:true,browserVersion:browser.version(),nodeVersion:process.version,apiFixtures:true,genuineAuthenticationOrBackendEffectsClaimed:false,keyboardPlanLookup:true,mobileWidth:390,noHorizontalOverflow:true,planTextContrastAtLeast4point5:true,darkModePlanContrastAtLeast4point5:true,unknownNeverApply:true,independentApprovalGuidance:true,csrfOnObservation:true,untrustedPlanEscaped:true,sessionLossClearsPanel:true,capabilityDenialDisablesActions:true,noExternalRequests:true,protectedEffectsSent:effects},null,2));
    console.log('G5 compiled automation UI browser acceptance passed.');await context.close();
  }finally{await browser.close();server.close();}
})().catch(error=>{console.error(error);server.close();process.exitCode=1;});
