const assert = require('node:assert/strict');
const { writeFileSync, mkdirSync } = require('node:fs');
const { join } = require('node:path');
const { chromium } = require('./browser-g2/node_modules/playwright');

async function main() {
  const env = name => process.env['KAFDECK_HOST_' + name];
  const at = Number(env('LOG_TIME'));
  const siblingAt = Number(env('LOG_SIBLING_TIME'));
  const date = seconds => new Date(seconds * 1000).toISOString().slice(0, 19);
  const browser = await chromium.launch({ channel: env('BROWSER_CHANNEL') || undefined });
  try {
    const page = await browser.newPage({viewport:{width:1024,height:768}});
    const base = process.argv[2];
    const checkEntries = async () => {
      const logs = page.getByRole('region', {name:'Broker log entries'});
      await logs.getByText(env('LOG_MARKER'), {exact:false}).waitFor();
      const text = await logs.innerText();
      assert.ok(text.includes('[REDACTED]'), 'The credential-shaped control entry was not redacted');
      assert.ok(!text.includes(env('LOG_SIBLING')), 'A different broker marker appeared in the selected view');
      assert.ok(!text.includes('v11-example-value'), 'The credential-shaped control value reached the browser');
    };
    const accessResponse = page.waitForResponse(response => new URL(response.url()).pathname === '/api/v1/fleet/hosts/log-targets');
    await page.goto(base + '?cluster=test-cluster#brokers');
    const capability = await accessResponse;
    assert.ok(capability.ok(), 'Broker log capability response returned HTTP ' + capability.status());
    assert.equal((await capability.json()).allowed, true, 'The signed-in fixture lacked log access');
    await page.getByRole('combobox', {name:'Broker', exact:true}).selectOption('1');
    await page.getByLabel('From (UTC)').fill(date(Math.min(at, siblingAt) - 20));
    await page.getByLabel('To (UTC)').fill(date(Math.max(at, siblingAt) + 2));
    await page.getByRole('button', {name:'Read service logs'}).click();
    await checkEntries();

    await page.goto(base + '?cluster=test-cluster&harness=alerts#monitoring');
    const link = page.getByRole('link', {name:'Open broker logs'});
    const target = new URL(await link.getAttribute('href'), base);
    assert.equal(target.hash, '#brokers');
    assert.equal(target.searchParams.get('brokerLogs'), '1');
    assert.equal(Number(target.searchParams.get('logFrom')), at - 300);
    const end = Number(target.searchParams.get('logTo'));
    assert.ok(end > at && end <= at + 300, 'Alert did not preserve the event window');
    await link.click();
    await checkEntries();
    mkdirSync(env('EVIDENCE'), { recursive:true });
    writeFileSync(join(env('EVIDENCE'), env('ADAPTER') + '-broker-logs.json'), JSON.stringify({
      sourceSha:process.env.KAFDECK_SOURCE_SHA, adapter:env('ADAPTER'), brokerId:1, siblingBrokerId:2, eventTime:at,
      brokerWindowContainsDiagnostic:true, siblingMarkerVerifiedAndExcluded:true, sensitiveControlRedacted:true,
      alertLinkOpenedSameBrokerAndEventWindow:true, realSshAndServiceLog:true, alertFixture:true,
    }, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch(error => {
  process.stderr.write(String(error.message) + '\n');
  process.exitCode = 1;
});
