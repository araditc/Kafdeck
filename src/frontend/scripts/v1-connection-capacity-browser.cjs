'use strict';
// Real compiled UI/Chrome; explicit wire fixtures complement real-cache API tests.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{createServer}=require('node:http');
const root=path.resolve(__dirname,'../../..'),dist=path.join(root,'src/frontend/dist');
const {chromium}=require(process.env.KAFDECK_BROWSER_PLAYWRIGHT||path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
const server=createServer((req,res)=>{const file=path.join(dist,new URL(req.url,'http://localhost').pathname);if(!file.startsWith(dist+path.sep))return res.end();const selected=fs.existsSync(file)&&fs.statSync(file).isFile()?file:path.join(dist,'index.html');res.setHeader('Content-Type',selected.endsWith('.js')?'application/javascript':selected.endsWith('.css')?'text/css':'text/html');fs.createReadStream(selected).pipe(res);});
(async()=>{let browser;try{
 await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
 browser=await chromium.launch({headless:true,...(process.env.KAFDECK_G2_BROWSER_EXECUTABLE?{executablePath:process.env.KAFDECK_G2_BROWSER_EXECUTABLE}:{})});
 const context=await browser.newContext(),page=await context.newPage(),requests=[],errors=[];let saved=false;
 page.on('pageerror',e=>errors.push(e.message));
 await context.route('**/api/v1/**',async route=>{
  const r=route.request(),p=new URL(r.url()).pathname;requests.push({method:r.method(),path:p});
  const send=(x,status=200)=>route.fulfill({status,contentType:status===409?'application/problem+json':'application/json',body:JSON.stringify(x)});
  if(p==='/api/v1/auth/mode')return send({authenticationMode:'oidc',connectionWorkspace:true,setupState:'complete'});
  if(p==='/api/v1/auth/session')return send({authenticated:true,authenticationMode:'oidc',sessionScope:'a'.repeat(64),recentConnectionScope:'b'.repeat(64),canRegisterKafkaConnections:true,displayName:'Capacity fixture'});
  if(p==='/api/v1/auth/csrf')return send({requestToken:'fixture-csrf',headerName:'X-Kafdeck-CSRF'});
  if(p==='/api/v1/connections/targets'&&r.method()==='GET')return send(saved?[{id:'connection-saved',name:'Saved capacity target',bootstrapServers:['127.0.0.1:19092'],protocol:'SASL_PLAINTEXT',mechanism:'PLAIN',registered:true}]:[]);
  if(p==='/api/v1/connections/test')return send({clusterId:null,observedAt:'2026-11-13T00:00:00Z',brokers:[],topics:[],topicsTruncated:false,requiresRegistration:true,savedTarget:null});
  if(p==='/api/v1/connections/targets'&&r.method()==='POST'){saved=true;return send({type:'urn:kafdeck:problem:connection-credential-capacity',status:409,clusterId:'connection-saved',targetSaved:true,title:'Capacity'},409);}
  return send({},403);
 });
 await page.goto(origin+'/#connections');await page.getByLabel('Connection name',{exact:true}).fill('Saved capacity target');await page.getByLabel('Bootstrap servers',{exact:true}).fill('127.0.0.1:19092');await page.getByLabel(/^Security protocol/).selectOption('SASL_PLAINTEXT');await page.getByLabel('SASL username',{exact:true}).fill('fixture-user');await page.getByLabel('SASL password',{exact:true}).fill('fixture-capacity-secret');
 await page.getByRole('button',{name:'Test connection',exact:true}).click();await page.getByRole('button',{name:'Save and open connection',exact:true}).click();
 await page.getByRole('alert').filter({hasText:'This connection is saved, but credential capacity is full.'}).waitFor();
 await page.getByRole('option',{name:'Saved capacity target',exact:true}).waitFor({state:'attached'});
 assert.equal(await page.getByRole('button',{name:'Save and open connection',exact:true}).count(),0);assert.equal(await page.locator('#cluster-selector').count(),0);
 assert.equal(requests.filter(r=>r.path==='/api/v1/connections/targets'&&r.method==='POST').length,1);assert.equal(requests.filter(r=>r.path==='/api/v1/connections/targets'&&r.method==='GET').length,2);
 assert.equal(await page.evaluate(()=>JSON.stringify({...localStorage}).includes('fixture-capacity-secret')),false);assert.deepEqual(errors,[]);
 const output=path.join(root,'artifacts/v1-connection-capacity-browser.json');fs.mkdirSync(path.dirname(output),{recursive:true});fs.writeFileSync(output,JSON.stringify({actualCompiledUi:true,browserVersion:browser.version(),controlledApiFixtures:true,correctCapacityRemediation:true,savedTargetRefreshed:true,noAutomaticRetryOrDashboardOpen:true,noSecretInLocalStorage:true,requests,pageErrors:errors},null,2));
 await context.close();console.log('Q01 compiled Chrome partial-save/capacity remediation passed.');
}finally{if(browser)await browser.close();server.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
