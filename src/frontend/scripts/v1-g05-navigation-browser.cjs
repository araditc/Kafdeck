// Actual compiled UI in Chromium; API/identity responses are explicit fixtures.
// No genuine Keycloak authentication, database or Kafka effect is claimed.
'use strict';
const {createServer}=require('node:http');
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..');
const {chromium}=require(process.env.KAFDECK_BROWSER_PLAYWRIGHT||path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
const dist=path.join(root,'src/frontend/dist');
const evidence=process.env.KAFDECK_G05_EVIDENCE||path.join(root,'artifacts/g05-results/browser.json');
const server=createServer((req,res)=>{
  const name=decodeURIComponent(new URL(req.url,'http://localhost').pathname),file=path.resolve(dist,name==='/'?'index.html':'.'+name);
  if(!file.startsWith(dist+path.sep)||!fs.existsSync(file)||!fs.statSync(file).isFile()){res.writeHead(404);res.end();return;}
  res.writeHead(200,{'Content-Type':({'.html':'text/html','.js':'text/javascript','.css':'text/css','.webp':'image/webp'})[path.extname(file)]||'application/octet-stream'});
  fs.createReadStream(file).pipe(res);
});
(async()=>{
  let browser;
  try{
    await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
    const launch={headless:true};if(process.env.KAFDECK_G2_BROWSER_EXECUTABLE)launch.executablePath=process.env.KAFDECK_G2_BROWSER_EXECUTABLE;
    browser=await chromium.launch(launch);
    const context=await browser.newContext({viewport:{width:1280,height:900}}),page=await context.newPage();
    let scope='a'.repeat(64),externalAllowed=true;const reads=[],navigation=[],outside=[],errors=[];
    page.on('pageerror',error=>errors.push(error.message));
    await context.route('**/*',async route=>{
      const request=route.request(),url=new URL(request.url()),p=url.pathname;
      if(url.origin!==origin){outside.push(url.origin);return route.abort();}
      if(!p.startsWith('/api/v1/'))return route.continue();
      const send=(value,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(value)});
      if(p==='/api/v1/auth/mode')return send({authenticationMode:'oidc'});
      if(p==='/api/v1/auth/session')return send({authenticated:true,authenticationMode:'oidc',displayName:'Navigation fixture',authenticatedAt:'2026-11-13T00:00:00Z',sessionScope:scope});
      if(p==='/api/v1/auth/navigation'){
        const clusterId=url.searchParams.get('clusterId');navigation.push({clusterId,method:request.method()});
        return send({clusterId,ownershipCatalog:clusterId==='b'&&externalAllowed,governancePolicy:false,serviceRequests:false,governanceAttribution:false});
      }
      if(p==='/api/v1/clusters'&&url.searchParams.get('view')==='directory')return send({data:[{clusterId:'a'},{clusterId:'b'}]});
      if(p==='/api/v1/clusters/a'||p==='/api/v1/clusters/b')return send({data:{clusterId:p.split('/').pop(),brokers:[],health:'healthy',healthReasons:[]},observation:{observedAt:'2026-11-13T00:00:00Z',freshness:'fresh',partial:false},limitations:[]});
      if(p.startsWith('/api/v1/governance/')){reads.push({path:p,cluster:url.searchParams.get('clusterId')});return send({items:[],nextCursor:null});}
      return send({},403);
    });
    const nav=page.getByRole('navigation',{name:'Kafdeck sections'});
    await page.goto(origin+'/?cluster=a#ownership-catalog');
    await page.getByRole('status').filter({hasText:'This page is unavailable for the current account or cluster.'}).waitFor();
    await page.waitForFunction(()=>document.getElementById('cluster-selector')?.options.length===2);
    assert.equal(await nav.getByRole('link',{name:'Ownership catalog',exact:true}).count(),0);
    assert.equal(reads.length,0);
    await page.getByRole('button',{name:/Commands/}).click();
    assert.equal(await page.getByRole('option',{name:/Ownership catalog/}).isDisabled(),true);
    assert.equal(await page.getByRole('option',{name:/Governance policy/}).isDisabled(),true);
    await page.keyboard.press('Escape');
    await page.getByLabel('Cluster',{exact:true}).selectOption('b');
    await nav.getByRole('link',{name:'Ownership catalog',exact:true}).waitFor();
    await page.waitForFunction(()=>!!document.querySelector('#ownership-catalog-title'));
    assert.ok(reads.some(r=>r.cluster==='b'));
    assert.ok(reads.every(r=>r.cluster==='b'));
    // A new authoritative session scope removes the old grant and mounted panel.
    externalAllowed=false;scope='b'.repeat(64);
    await page.evaluate(()=>window.dispatchEvent(new Event('focus')));
    await page.getByRole('status').filter({hasText:'This page is unavailable for the current account or cluster.'}).waitFor();
    assert.equal(await nav.getByRole('link',{name:'Ownership catalog',exact:true}).count(),0);
    assert.equal(await page.locator('#ownership-catalog-title').count(),0);
    await page.setViewportSize({width:390,height:844});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),true);
    assert.deepEqual(outside,[]);assert.deepEqual(errors,[]);
    assert.ok(navigation.every(r=>r.method==='GET'));
    fs.mkdirSync(path.dirname(evidence),{recursive:true});
    await page.screenshot({path:path.join(path.dirname(evidence),'navigation-denied-mobile.png'),fullPage:true});
    fs.writeFileSync(evidence,JSON.stringify({...require('./governance-browser-source.cjs')(root),actualCompiledUi:true,actualChromium:true,browserVersion:browser.version(),apiFixtures:true,genuineIdentityOrProviderEffectsClaimed:false,deniedDeepLinkNoGovernanceReads:true,deniedPaletteDisabled:true,clusterScopedAllowedCatalog:true,newSessionRevokesMountedPage:true,mobileNoOverflow:true,navigationRequests:navigation,governanceReads:reads,noExternalRequests:true,pageErrors:errors},null,2));
    await context.close();console.log('G05 compiled navigation browser acceptance passed (explicit API fixtures).');
  }finally{if(browser)await browser.close();server.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
