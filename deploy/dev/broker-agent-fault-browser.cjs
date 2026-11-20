const assert = require('node:assert/strict');
const { mkdirSync, writeFileSync } = require('node:fs');
const { join } = require('node:path');
const { chromium } = require('./browser-g2/node_modules/playwright');

async function main() {
  const browser = await chromium.launch({ channel: process.env.KAFDECK_HOST_BROWSER_CHANNEL || undefined });
  try {
    const page = await browser.newPage();
    const base = process.argv[2];
    const click = (name) => page.getByRole('button', { name, exact: true }).click();
    async function observe(button, route) {
      const pending = page.waitForResponse(res => new URL(res.url()).pathname.endsWith('/agent/' + route), { timeout: 90000 });
      await click(button);
      const response = await pending;
      const body = await response.json();
      assert.ok(response.ok(), route + ' status ' + response.status() + ' (' + (body.code ?? 'unknown') + ')');
      return body;
    }
    async function control(phase) {
      const response = await page.request.post(base + '/fixture/agent-faults/' + phase, {
        headers: { 'X-Kafdeck-Fault-Fixture': process.env.KAFDECK_AGENT_FAULT_FIXTURE_KEY },
        timeout: 65000,
      });
      assert.equal(response.status(), 204, 'fixture control ' + phase);
    }
    async function waitStage(stage) {
      const deadline = Date.now() + 110000;
      do {
        const result = await observe('Check agent operation', 'status');
        if (result.stage === stage) return result;
        assert.notEqual(result.stage, 'complete', 'completed before ' + stage);
        await new Promise(resolve => setTimeout(resolve, 1800));
      } while (Date.now() < deadline);
      assert.fail('No observation of ' + stage);
    }
    await page.goto(base + '/#agent');
    await page.getByLabel('Offline package version').selectOption('1.1.0-test.1');
    const preview = await observe('Preview agent change', 'preview');
    assert.ok(preview.plan && preview.restartRequired && preview.health.healthy && preview.health.affectedPartitions > 0);
    await page.getByRole('checkbox', {name:'Confirm restart of this broker in the maintenance window'}).check();
    const interrupted = await observe('Apply agent change', 'apply');
    assert.equal(interrupted.stage, 'reconcile');
    assert.equal(interrupted.lastConfirmed, 'DispatchAuthorized');
    await page.reload();
    await control('pause');
    const unhealthy = await waitStage('awaitingHealth');
    assert.equal(unhealthy.health.healthy, false);
    assert.equal(unhealthy.lastConfirmed, 'RestartObserved');
    await page.getByText(/restore Kafka and replica health/).waitFor();
    assert.equal(await page.getByRole('button', {name:'Retry the same agent request',exact:true}).count(), 0);
    assert.equal(await page.getByRole('button', {name:'Preview agent change',exact:true}).count(), 0);

    await control('resume');
    const missing = await waitStage('awaitingMetrics');
    assert.equal(missing.health.healthy, true);
    assert.equal(missing.lastConfirmed, 'KafkaHealthy');
    assert.equal(unhealthy.operationId, missing.operationId);
    assert.equal(unhealthy.processStartedAt, missing.processStartedAt);
    assert.equal(missing.monitoringInstance, 'fixture-agent-' + process.env.KAFDECK_AGENT_BROKER);
    await page.getByText(/monitoring source and ingestion/).waitFor();
    assert.equal(await page.getByRole('button', {name:'Retry the same agent request',exact:true}).count(), 0);
    assert.equal(await page.getByRole('button', {name:'Preview agent change',exact:true}).count(), 0);
    await control('allow-metrics');
    const complete = await waitStage('complete');
    assert.equal(complete.operationId, interrupted.operationId);
    assert.equal(complete.operationId, missing.operationId);
    assert.equal(complete.processStartedAt, missing.processStartedAt);
    assert.equal(complete.digest, preview.package.digest);
    assert.ok(complete.health.healthy && complete.metricSeenAt > complete.processStartedAt);
    const evidence = process.env.KAFDECK_HOST_EVIDENCE;
    mkdirSync(evidence, {recursive:true});
    writeFileSync(join(evidence, 'agent-faults-' + process.env.KAFDECK_AGENT_ADAPTER + '.json'),
      JSON.stringify({ sourceCommit:process.env.KAFDECK_SOURCE_SHA, brokerId:preview.brokerId,
        before:preview.health, interrupted, unhealthy, missing, complete,
        operatedThroughFleet:true, retryWhileRecoveringHidden:true }, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch(error => { process.stderr.write(String(error.stack ?? error) + '\n'); process.exitCode = 1; });
