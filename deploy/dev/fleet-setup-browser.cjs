const assert = require('node:assert/strict');
const {readFileSync, mkdirSync, writeFileSync} = require('node:fs');
const {join} = require('node:path');
const {chromium} = require('./browser-g2/node_modules/playwright');

async function main() {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({ignoreHTTPSErrors:true});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const base = process.argv[2];
    const observe = async (name, suffix) => {
      const pending = page.waitForResponse(response => new URL(response.url()).pathname.endsWith(suffix), {timeout:150000});
      await page.getByRole('button', {name, exact:true}).click();
      const response = await pending;
      const body = await response.json();
      assert.ok(response.ok(), suffix + ': HTTP ' + response.status() + ' ' + (body.code ?? body.type ?? ''));
      return body;
    };
    await page.goto(base);
    await page.getByLabel('Enable Fleet host management').check();
    await page.getByLabel('Allowed SSH host networks').fill(process.env.KAFDECK_SETUP_ADDRESS + '/32');
    await observe('Save Fleet settings', '/fleet/settings/configuration');
    await page.getByText('Prepare a Linux host from the dashboard', {exact:true}).click();
    await page.locator('#setup-host-name').fill('Isolated onboarding pilot');
    await page.locator('#setup-host-address').fill(process.env.KAFDECK_SETUP_ADDRESS);
    await page.locator('#setup-host-port').fill(process.env.KAFDECK_SETUP_PORT);
    await page.locator('#setup-host-user').fill(process.env.KAFDECK_SETUP_ADMIN_USER);
    await page.locator('#setup-host-auth').selectOption(process.env.KAFDECK_SETUP_PASSWORD ? 'password' : 'privateKey');
    await page.locator('#setup-host-fingerprint').fill(process.env.KAFDECK_SETUP_PIN);
    await page.locator('#setup-host-secret').fill(process.env.KAFDECK_SETUP_PASSWORD || readFileSync(process.env.KAFDECK_SETUP_ADMIN_KEY, 'utf8'));
    if (process.env.KAFDECK_SETUP_SUDO_PASSWORD) await page.locator('#setup-host-sudo').fill(process.env.KAFDECK_SETUP_SUDO_PASSWORD);
    const inspected = await observe('Test SSH and inspect services', '/fleet/bootstrap/inspect');
    assert.equal(inspected.brokerRestarted, false);
    const broker = inspected.targets.find(target => target.brokerId === 2);
    assert.ok(broker, 'The real sibling Kafka broker was not discovered');
    assert.equal(broker.adapter, process.env.KAFDECK_SETUP_ADAPTER);
    const service = broker.adapter === 'systemd' ? broker.unit : broker.project + ' / ' + broker.service;
    await page.getByLabel(service + ' · Broker 2 · ' + broker.adapter, {exact:true}).check();
    await page.locator('#setup-agent-endpoint').fill(base + '/monitoring/ingest');
    await observe('Use this Kafdeck server certificate', '/fleet/settings/public-certificate');
    assert.match(await page.locator('#setup-agent-ca').inputValue(), /BEGIN CERTIFICATE/);
    await page.locator('#setup-agent-interval').selectOption('10');
    await page.locator('#setup-window-from').fill(new Date(Date.now()-60000).toISOString().slice(0,16));
    await page.locator('#setup-window-to').fill(new Date(Date.now()+1800000).toISOString().slice(0,16));
    await page.getByLabel('Confirm preparation of the selected services', {exact:true}).check();
    const prepared = await observe('Prepare host and save encrypted access', '/fleet/bootstrap/prepare');
    assert.equal(prepared.stage, 'complete', 'Preparation did not verify: ' + (prepared.code ?? 'unknown'));
    assert.equal(prepared.brokerRestarted, false);
    assert.equal(await page.locator('#setup-host-secret').inputValue(), '');
    const evidence = process.env.KAFDECK_SETUP_EVIDENCE;
    mkdirSync(evidence, {recursive:true});
    await page.screenshot({path:join(evidence, 'fleet-prepared-' + broker.adapter + '.png'), fullPage:true});
    await page.goto(base + '/#agent');
    await page.getByLabel('Offline package version').selectOption(inspected.package.version);
    const plan = await observe('Preview agent change', '/agent/preview');
    assert.ok(plan.plan && plan.health.healthy && plan.restartRequired);
    assert.equal(plan.launchMode, 'argfile');
    await page.getByLabel('Confirm restart of this broker in the maintenance window').check();
    let progress = await observe('Apply agent change', '/agent/apply');
    const deadline = Date.now()+150000;
    while (progress.stage !== 'complete' && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 3000));
      progress = await observe('Check agent operation', '/agent/status');
    }
    assert.equal(progress.stage, 'complete');
    assert.ok(progress.health.healthy && progress.metricSeenAt > progress.processStartedAt);
    assert.equal(progress.digest, inspected.package.digest);
    assert.deepEqual(errors, []);
    await page.screenshot({path:join(evidence, 'fleet-agent-verified-' + broker.adapter + '.png'), fullPage:true});
    writeFileSync(join(evidence, 'fleet-setup-' + broker.adapter + '.json'), JSON.stringify({
      source:process.env.KAFDECK_SOURCE_SHA, adapter:broker.adapter, inspectedServices:inspected.targets,
      preparation:{id:prepared.id, stage:prepared.stage, brokerRestarted:prepared.brokerRestarted},
      lifecycle:progress, browserErrors:errors, administrativeKeyCleared:true,
    },null,2));
  } finally { await browser.close(); }
}
main().catch(error => {process.stderr.write(String(error.message)+'\n');process.exitCode=1;});
