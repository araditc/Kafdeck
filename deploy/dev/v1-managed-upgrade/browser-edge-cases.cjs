'use strict';
const assert=require('node:assert/strict');
module.exports=async function(page,context,api,target){
 await page.goto(api+'/?cluster='+target+'#topics');await page.getByRole('button',{name:'g06-reference-00',exact:true}).waitFor();
 const search=page.locator('#topic-search');await search.fill('g06-reference');
 await page.waitForResponse(r=>r.url().includes('/topics?')&&r.status()===200);
 // Exercise the real ten-second background refresh, rather than clicking Refresh.
 await search.focus();const old=await search.elementHandle();const scrollBefore=await page.evaluate(()=>{scrollTo(0,Math.min(250,document.documentElement.scrollHeight-innerHeight));return scrollY;});
 const quiet=await page.waitForResponse(r=>r.url().includes('/topics?')&&r.status()===200,{timeout:15000});await quiet.finished();
 const quietAfter=await page.evaluate(()=>({scroll:scrollY,focused:document.activeElement?.id,value:document.querySelector('#topic-search')?.value}));
 assert.equal(await old.evaluate(n=>n.isConnected),true);assert.equal(quietAfter.value,'g06-reference');assert.equal(quietAfter.focused,'topic-search');assert.equal(quietAfter.scroll,scrollBefore);
 const response=page.waitForResponse(r=>new URL(r.url()).pathname==='/api/v1/auth/session');await page.evaluate(()=>window.dispatchEvent(new Event('focus')));await response;await page.waitForFunction(()=>!document.querySelector('.kafdeck-shell')?.hidden);
 const focusAfter=await page.evaluate(()=>({scroll:scrollY,focused:document.activeElement?.id,value:document.querySelector('#topic-search')?.value}));
 assert.equal(focusAfter.value,'g06-reference');assert.equal(focusAfter.focused,'topic-search');assert.equal(focusAfter.scroll,scrollBefore,'nonzero focus-return scroll must remain unchanged');
 // Stall only a controlled browser read; do not modify a server, Kafka or writes.
 let stalled=0;const holds=[];await page.route('**/api/v1/clusters/*/topics?**',async route=>{stalled++;await new Promise(r=>holds.push(r));try{await route.abort();}catch{}});
 const start=performance.now();await search.fill('deadline-fixture');await page.getByText('The read timed out. Retry the read or check the connection.',{exact:true}).waitFor({timeout:19000});const deadlineMs=performance.now()-start;
 assert.ok(deadlineMs>=14500&&deadlineMs<=19000);assert.equal(stalled,1);assert.equal(await search.inputValue(),'deadline-fixture');
 await search.fill('cancel-fixture');await page.waitForTimeout(300);const beforeCancel=stalled;await page.locator('nav a[href="#brokers"]').click();
 for(const done of holds)done();await page.unroute('**/api/v1/clusters/*/topics?**');assert.ok(beforeCancel>=2);assert.equal(await page.locator('#topic-search').count(),0);
 // A new physical Kafka definition uses the same reference broker deliberately.
 const token=await (await context.request.get(api+'/api/v1/auth/csrf')).json();
 const current=await (await context.request.get(api+'/api/v1/connections/targets')).json();let second=current.find(x=>x.name==='E01 second logical target')?.clusterId;
 if(!second){const registered=await context.request.post(api+'/api/v1/connections/targets',{data:{name:'E01 second logical target',bootstrapServers:['127.0.0.1:19092','127.0.0.1:19091'],protocol:'PLAINTEXT'},headers:{[token.headerName]:token.requestToken}});assert.equal(registered.status(),200);second=(await registered.json()).clusterId;}
 assert.notEqual(second,target);
 await page.reload();await page.locator('#cluster-selector option[value="'+second+'"]').waitFor({state:'attached'});
 const switches=[];for(let i=0;i<10;i++){const id=i%2?target:second;const t=performance.now();await page.locator('#cluster-selector').selectOption(id);await page.waitForFunction(id=>document.querySelector('#cluster-selector')?.value===id,id);switches.push(performance.now()-t);}
 assert.ok(Math.max(...switches)<=5000);
 return {quietRefresh:{draftRetained:true,nodeRetained:true,focusRetained:true,scrollBefore,after:quietAfter},focusReturn:{before:scrollBefore,after:focusAfter},readDeadline:{deadlineMs,attemptsBeforeRetry:1,draftRetained:true},navigationCancellation:{lateReadDidNotRemountPriorPage:true},clusterSwitch:{n:10,readyMs:switches,logicalTargets:2,physicalKafkaBrokers:1},fixtureStallsOnly:true};
};
