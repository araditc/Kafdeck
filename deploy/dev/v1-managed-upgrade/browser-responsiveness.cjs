'use strict';
// Finite single-user Chrome observations; not the W82 multi-user/soak campaign.
const assert=require('node:assert/strict');
module.exports=async function measure(page,context,api,target,{baseline=false}={}){
 const requests=[];page.on('request',r=>{if(r.url().includes('/api/v1/'))requests.push({method:r.method(),path:new URL(r.url()).pathname});});
 const session=await (await context.request.get(api+'/api/v1/auth/session')).json();
 await page.goto(api+'/?cluster='+encodeURIComponent(target)+'#topics');
 await page.locator('#topic-search').waitFor();
 await page.getByRole('button',{name:'g06-reference-00',exact:true}).waitFor();
 const input=page.locator('#topic-search');await input.fill('g06-reference');await input.focus();
 const before=await page.evaluate(()=>({scroll:scrollY,value:document.querySelector('#topic-search').value}));
 const response=page.waitForResponse(r=>new URL(r.url()).pathname==='/api/v1/auth/session');
 await page.evaluate(()=>window.dispatchEvent(new Event('focus')));await response;
 await page.waitForFunction(()=>!document.querySelector('.kafdeck-shell')?.hidden);
 const retained=await page.evaluate(()=>({value:document.querySelector('#topic-search')?.value,focused:document.activeElement?.id==='topic-search',scroll:scrollY}));
 if(baseline)return {baseline:true,sessionScopeValid:/^[a-f0-9]{64}$/.test(session.sessionScope||''),focusReturn:{before,after:retained},requests:requests.length};
 assert.match(session.sessionScope,/^[a-f0-9]{64}$/);assert.equal(retained.value,before.value);assert.equal(retained.focused,true);assert.ok(Math.abs(retained.scroll-before.scroll)<=1);
 await input.fill('');
 const traces=[];
 async function clickJourney(name,action,ready){
  await page.evaluate(()=>{window.__e01Start=performance.now();window.__e01Feedback=null;const o=new MutationObserver(()=>{if(window.__e01Feedback===null)requestAnimationFrame(()=>{window.__e01Feedback??=performance.now()-window.__e01Start;});});o.observe(document.querySelector('#root'),{subtree:true,childList:true,attributes:true,characterData:true});window.__e01Observer=o;});
  await action();await ready();await page.evaluate(()=>new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r))));
  const trace=await page.evaluate(()=>{window.__e01Observer.disconnect();return {feedbackMs:window.__e01Feedback,readyMs:performance.now()-window.__e01Start};});
  assert.ok(trace.feedbackMs!==null,name+' has visible feedback');traces.push({journey:name,...trace});
 }
 const nav=id=>page.locator('nav a[href="#'+id+'"]');
 for(let i=0;i<10;i++){
  await clickJourney('topic-detail',()=>page.getByRole('button',{name:'g06-reference-00',exact:true}).click(),()=>page.locator('#topic-detail-title').waitFor());
  await page.getByRole('button',{name:'Back to topics',exact:true}).click();
  await clickJourney('consumer-navigation',()=>nav('consumers').click(),()=>page.locator('[id^="consumer-open-"]').first().waitFor());
  await clickJourney('consumer-detail',()=>page.locator('[id^="consumer-open-"]').first().click(),()=>page.locator('#consumer-offsets-title').waitFor());
  await clickJourney('schema-navigation',()=>nav('schemas').click(),()=>page.locator('[id^="schema-subject-"]').first().waitFor());
  await clickJourney('subject-detail',()=>page.locator('[id^="schema-subject-"]').first().click(),()=>page.locator('#schema-detail-title').waitFor());
  await clickJourney('ecosystem-navigation',()=>nav('ecosystem').click(),()=>page.getByRole('button',{name:'Refresh ksqlDB catalog',exact:true}).waitFor());
  await clickJourney('ecosystem-configure',()=>page.getByRole('button',{name:'Configure ksqlDB',exact:true}).click(),()=>page.getByLabel('ksqlDB URL',{exact:true}).waitFor());
  await page.locator('.kafdeck-service-settings').getByRole('button',{name:'Close',exact:true}).click();
  await clickJourney('topic-navigation',()=>nav('topics').click(),()=>page.locator('#topic-search').waitFor());
 }
 // Quiet refresh must not replace the search node or clear an in-progress draft.
 await input.fill('g06-reference');await input.focus();
 const oldNode=await input.elementHandle();const countBefore=requests.length;
 await page.getByRole('button',{name:'Refresh',exact:true}).click();
 await page.waitForFunction(()=>!document.querySelector('button.btn-sm.btn-outline-primary:disabled'));
 assert.equal(await input.inputValue(),'g06-reference');assert.equal(await oldNode.evaluate(n=>n.isConnected),true);
 const quantile=(a,q)=>a.slice().sort((x,y)=>x-y)[Math.ceil(a.length*q)-1];
 const summary=Object.fromEntries([...new Set(traces.map(x=>x.journey))].map(j=>{const a=traces.filter(x=>x.journey===j);return [j,{n:a.length,feedbackP95Ms:quantile(a.map(x=>x.feedbackMs),.95),feedbackP99Ms:quantile(a.map(x=>x.feedbackMs),.99),readyP95Ms:quantile(a.map(x=>x.readyMs),.95),readyP99Ms:quantile(a.map(x=>x.readyMs),.99)}];}));
 // Budgets are finite reference acceptance, not a population p99 or WAN guarantee.
 for(const [name,s]of Object.entries(summary)){assert.ok(s.feedbackP95Ms<=250,name+' feedback p95');assert.ok(s.feedbackP99Ms<=500,name+' feedback p99');assert.ok(s.readyP95Ms<=5000,name+' ready p95');assert.ok(s.readyP99Ms<=10000,name+' ready p99');}
 return {baseline:false,chrome:await page.context().browser().version(),viewport:{width:1440,height:1000},network:'numeric loopback; genuine Keycloak/Kafka; controlled HTTP integration providers',dataset:{topics:20,partitionsPerTopic:3,records:100,groups:5,subjects:20,connectors:2},sessionScopeValid:true,focusReturn:{before,after:retained},quietRefresh:{draftRetained:true,nodeRetained:true,requests:requests.length-countBefore},apiRequestCount:requests.length,requests,traces,summary,budgets:{feedbackP95Ms:250,feedbackP99Ms:500,readyP95Ms:5000,readyP99Ms:10000,readDeadlineMs:15000},quantiles:'nearest rank; n=10 per journey; finite reference only'};
};
