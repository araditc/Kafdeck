'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require(process.env.KAFDECK_HOST_PLAYWRIGHT || './browser-g2/node_modules/playwright');
(async () => {
  const browser = await chromium.launch({ headless: true, channel: process.env.KAFDECK_HOST_BROWSER_CHANNEL });
  try {
    const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
    const errors=[]; page.on('pageerror', error => errors.push(error.message));
    await page.goto(process.argv[2]);
    await page.getByText('Register SSH host', { exact: true }).click();
    const registration = page.getByRole('form', {name:'Register SSH host',exact:true});
    await registration.getByLabel('Host name', { exact: true }).fill('Acceptance broker host');
    await registration.getByLabel('Host IP address', { exact: true }).fill(process.env.KAFDECK_HOST_ADDRESS);
    await registration.getByLabel('SSH port', { exact: true }).fill(process.env.KAFDECK_HOST_PORT);
    await registration.getByLabel('Trusted SSH fingerprint', { exact: true }).fill(process.env.KAFDECK_HOST_PIN);
    assert.equal(await registration.locator('input[type=password]').count(),0);
    await page.getByRole('button', { name: 'Verify and register host', exact: true }).click();
    await page.getByText('SSH host verified and registered.', { exact: true }).waitFor();
    const options = await page.locator('#binding-service option').evaluateAll(nodes => nodes.filter(n=>n.value&&!n.disabled).map(n=>({ id:n.value, broker:n.textContent.match(/Broker (\d+)$/)[1] })));
    assert.equal(options.length,Number(process.env.KAFDECK_HOST_COUNT));
    for (const option of options) {
      await page.getByLabel('Approved service', { exact: true }).selectOption(option.id);
      await page.getByLabel('Broker in selected cluster', { exact: true }).selectOption(option.broker);
      const response = page.waitForResponse(r=>r.url().endsWith('/fleet/hosts/bindings')&&r.request().method()==='POST');
      await page.getByRole('button', { name:'Verify and bind broker',exact:true }).click();
      assert.equal((await response).status(),200);
      await page.getByText('Broker and service identity match. Binding saved.', {exact:true}).waitFor();
      await page.getByRole('button', {name:'Verify and bind broker',exact:true}).waitFor();
    }
    await page.reload();
    await page.locator('tbody tr').first().waitFor();
    assert.equal(await page.locator('tbody tr').count(),options.length);
    const evidence=process.env.KAFDECK_HOST_EVIDENCE;
    fs.mkdirSync(evidence,{recursive:true});
    await page.screenshot({path:path.join(evidence,process.env.KAFDECK_HOST_ADAPTER+'-fleet.png'),fullPage:true});
    await page.setViewportSize({width:390,height:844});
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
    await page.screenshot({path:path.join(evidence,process.env.KAFDECK_HOST_ADAPTER+'-fleet-mobile.png'),fullPage:true});
    assert.deepEqual(errors,[]);
    fs.writeFileSync(path.join(evidence,process.env.KAFDECK_HOST_ADAPTER+'-browser.json'),JSON.stringify({
      component:'BrokerHostsPanel',identity:'normalized owner session fixture; production authorization handlers',
      realSsh:true,realKafka:true,bindings:options,reloadRetained:true,mobileOverflow:false,pageErrors:errors,
      observedAtUtc:new Date().toISOString(),
    },null,2));
  } finally { await browser.close(); }
})().catch(e=>{console.error(e);process.exitCode=1;});
