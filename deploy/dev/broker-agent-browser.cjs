const assert = require('node:assert/strict');
const { mkdirSync, writeFileSync } = require('node:fs');
const { join } = require('node:path');
const { chromium } = require('./browser-g2/node_modules/playwright');

async function main() {
  const browser = await chromium.launch({ channel: process.env.KAFDECK_HOST_BROWSER_CHANNEL || undefined });
  try {
    const page = await browser.newPage();
    const base = process.argv[2];
    const observe = async (button, operation) => {
      const pending = page.waitForResponse(response => new URL(response.url()).pathname.endsWith('/agent/' + operation));
      await page.getByRole('button',{ name:button, exact:true }).click();
      const response = await pending;
      const body = await response.json();
      assert.ok(response.ok(), operation + ' failed with HTTP ' + response.status() + ' (' + (body.code ?? body.type ?? 'unknown') + ')');
      return body;
    };
    async function change(version, lost) {
      await page.getByLabel('Offline package version').selectOption(version);
      const plan = await observe('Preview agent change', 'preview');
      assert.equal(plan.package.version, version);
      assert.ok(plan.plan && plan.health.healthy && plan.restartRequired);
      assert.ok(plan.health.affectedPartitions > 0);
      assert.equal(plan.currentVersion, lost ? null : '1.1.0-test.1');
      await page.getByRole('checkbox', {name:'Confirm restart of this broker in the maintenance window'}).check();
      let progress = await observe('Apply agent change', 'apply');
      if (lost) {
        assert.equal(progress.stage, 'reconcile');
        progress = await observe('Retry the same agent request', 'apply');
      }
      const end = Date.now() + 120_000;
      while (progress.stage !== 'complete' && Date.now() < end) {
        await new Promise(resolve => setTimeout(resolve, 3000));
        progress = await observe('Check agent operation', 'status');
      }
      assert.equal(progress.stage, 'complete');
      assert.equal(progress.digest, plan.package.digest);
      assert.equal(progress.version, version);
      assert.ok(progress.health.healthy && progress.metricSeenAt > progress.processStartedAt);
      assert.equal(progress.monitoringInstance, 'fixture-agent-' + process.env.KAFDECK_AGENT_BROKER);
      return progress;
    }
    await page.goto(base + '/#agent');
    const install = await change('1.1.0-test.1', true);
    await page.getByRole('button',{name:'Preview another package',exact:true}).click();
    const upgrade = await change('1.1.0-test.2', false);
    assert.notEqual(install.digest, upgrade.digest);
    assert.ok(upgrade.processStartedAt > install.processStartedAt);
    const evidence = process.env.KAFDECK_HOST_EVIDENCE;
    mkdirSync(evidence,{recursive:true});
    writeFileSync(join(evidence, 'agent-' + process.env.KAFDECK_AGENT_ADAPTER + '.json'),
      JSON.stringify({ adapter:process.env.KAFDECK_AGENT_ADAPTER, install, upgrade, interruptedReplyReconciled:true,
        observedThroughFleet:true, sourceCommit:process.env.KAFDECK_SOURCE_SHA },null,2));
  } finally {
    await browser.close();
  }
}
main().catch(error => { process.stderr.write(String(error.message) + '\n'); process.exitCode = 1; });
