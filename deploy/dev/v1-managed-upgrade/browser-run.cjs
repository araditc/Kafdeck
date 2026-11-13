'use strict';
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{spawn,spawnSync}=require('node:child_process');
const edge=process.env.KAFDECK_E01_EDGE_CASES==='1';const measure=require(edge?'./browser-edge-cases.cjs':'./browser-responsiveness.cjs');
const root=path.resolve(__dirname,'../../..'),lab=path.resolve(process.argv[2]),old=path.resolve(process.argv[3]),candidate=path.resolve(process.argv[4]);
const {chromium}=require(process.env.KAFDECK_BROWSER_PLAYWRIGHT),api='https://localhost:18460';
const read=f=>JSON.parse(fs.readFileSync(f,'utf8').replace(/^\uFEFF/,''));
let host,browser,phase='start';
let lastPhase;const progress=setInterval(()=>{if(phase!==lastPhase){console.log('E01 '+phase);lastPhase=phase;}},1000);progress.unref();
async function stop(){if(host&&host.exitCode===null){spawnSync('taskkill',['/PID',String(host.pid),'/T','/F'],{windowsHide:true,stdio:'ignore'});await new Promise(r=>host.exitCode===null?host.once('exit',r):r());}host=null;}
async function start(executable,directory){
 const fd=fs.openSync(path.join(directory,'e01-private.log'),'a');host=spawn(executable===candidate?'dotnet':executable,executable===candidate?[candidate]:[],{cwd:directory,env:{...process.env,ASPNETCORE_CONTENTROOT:directory,ASPNETCORE_ENVIRONMENT:'Production'},windowsHide:true,stdio:['ignore',fd,fd]});fs.closeSync(fd);
 const c=await browser.newContext({ignoreHTTPSErrors:true});try{for(let i=0;i<120;i++){assert.equal(host.exitCode,null);try{if((await c.request.get(api+'/healthz',{timeout:500})).ok())return;}catch{}await new Promise(r=>setTimeout(r,100));}throw Error('Owned API startup timeout');}finally{await c.close();}
}
async function login(checkpoint){
 const context=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}}),page=await context.newPage();
 await page.goto(api+'/api/v1/auth/login');await page.locator('#username').fill(checkpoint.email);await page.locator('#password').fill(checkpoint.password);
 const start=performance.now();await Promise.all([page.waitForURL(u=>u.origin===api&&!u.pathname.includes('signin-oidc')),page.locator('#kc-login').click()]);
 return {page,context,loginMs:performance.now()-start};
}
(async()=>{
 await new Promise((resolve,reject)=>{const socket=require('node:net').createServer();socket.once('error',reject);socket.listen(18460,'::',()=>socket.close(resolve));});
 browser=await chromium.launch({headless:true,executablePath:process.env.KAFDECK_G2_BROWSER_EXECUTABLE});const out={developmentCandidate:true,publicationClaimed:false,providers:[]};
 try{for(const provider of ['sqlite','postgres']){
  const directory=path.join(lab,provider),checkpoint=read(path.join(directory,'checkpoint-private.json'));let baseline;
  if(provider==='sqlite'&&!edge){
   phase='baseline-start';fs.cpSync(path.join(path.dirname(old),'wwwroot'),path.join(directory,'wwwroot'),{recursive:true});await start(old,directory);const a=await login(checkpoint);
   phase='baseline-measure';baseline=await measure(a.page,a.context,api,checkpoint.target,{baseline:true});await a.context.close();await stop();
  }
  fs.cpSync(path.join(root,'src/frontend/dist'),path.join(directory,'wwwroot'),{recursive:true});phase=provider+'-candidate-start';await start(candidate,directory);
  const loginTimes=[];if(!edge)for(let i=0;i<5;i++){const a=await login(checkpoint);loginTimes.push(a.loginMs);await a.context.close();}
  phase=provider+'-measure';const a=await login(checkpoint);const metrics=await measure(a.page,a.context,api,checkpoint.target);await a.page.screenshot({path:path.join(root,'artifacts/stabilization-results/e01-'+provider+'.png'),fullPage:false});await a.context.close();await stop();
  out.providers.push({provider,baseline,loginMs:loginTimes,metrics});fs.writeFileSync(path.join(root,'artifacts/stabilization-results/'+(edge?'e01-edge-cases.json':'e01-browser.json')),JSON.stringify(out,null,2));
 }}finally{await stop();await browser.close();}
 console.log('E01 real Chrome finite responsiveness traces passed on both databases.');
})().catch(error=>{fs.writeFileSync(path.join(lab,'e01-failure-private.log'),String(error.stack||error));console.error('E01_FAILED phase='+phase+'; private diagnostic retained.');process.exitCode=1;});
