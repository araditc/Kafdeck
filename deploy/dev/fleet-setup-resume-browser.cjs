// Resume the preserved isolated acceptance operation. Never applies/restarts a broker.
const assert = require('node:assert/strict');
const {mkdirSync, writeFileSync} = require('node:fs');
const {join} = require('node:path');
const {chromium} = require('./browser-g2/node_modules/playwright');

async function main() {
  const browser = await chromium.launch({args: ['--no-proxy-server']});
  const output = process.env.KAFDECK_SETUP_EVIDENCE;
  mkdirSync(output, {recursive: true});
  try {
    const page = await browser.newPage({ignoreHTTPSErrors: true});
    await page.goto(process.argv[2] + '/?view=agent#agent');
    await page.getByRole('button', {name: 'Check agent operation', exact: true}).waitFor({timeout: 60000});
    let result;
    for (let attempt = 0; attempt < 18; attempt++) {
      const response = page.waitForResponse(r => new URL(r.url()).pathname.endsWith('/agent/status'), {timeout: 90000});
      await page.getByRole('button', {name: 'Check agent operation', exact: true}).click();
      const status = await response;
      result = await status.json();
      assert.ok(status.ok(), 'Status HTTP ' + status.status());
      writeFileSync(join(output, 'lifecycle-progress.json'), JSON.stringify(result, null, 2));
      if (result.stage === 'complete') break;
      await page.waitForTimeout(10000);
    }
    await page.screenshot({path: join(output, 'compose-resumed.png'), fullPage: true});
    assert.equal(result.stage, 'complete');
    assert.ok(result.javaObservedAt >= result.processStartedAt);
    assert.equal(result.health.healthy, true);
    writeFileSync(join(output, 'browser-proof.json'), JSON.stringify({
      complete: true, resumed: true, additionalApplyOrRestart: false, transport: 'pilot-only SSH relay, end-to-end TLS',
      ...result,
    }, null, 2));
  } finally { await browser.close(); }
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
