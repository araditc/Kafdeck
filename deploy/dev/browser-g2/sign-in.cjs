// Explicit disposable lab only. Credentials, redirect/state, codes and tokens
// remain in memory; output contains a closed success marker or generic failure.
'use strict';
const fs = require('node:fs');
const crypto = require('node:crypto');
const readline = require('node:readline');
const { chromium } = require('playwright');

(async () => {
  let browser;
  try {
    const input = readline.createInterface({ input: process.stdin, terminal: false });
    const message = await new Promise(resolve => input.once('line', resolve));
    input.close();
    if (message.length > 16384) throw new Error('budget');
    const { authorization, manifestPath } = JSON.parse(message);
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8').replace(/^\uFEFF/, ''));
    const target = new URL(authorization);
    const issuer = new URL(manifest.issuer);
    if (target.origin !== issuer.origin || target.pathname !== issuer.pathname + '/protocol/openid-connect/auth') throw new Error('authority');
    const callback = new URL(target.searchParams.get('redirect_uri'));
    if (!['127.0.0.1', '[::1]'].includes(callback.hostname) || callback.protocol !== 'http:' || callback.pathname !== '/oauth/callback') throw new Error('callback');
    const certificate = new crypto.X509Certificate(fs.readFileSync(manifest.caPemFile));
    const pin = crypto.createHash('sha256').update(certificate.publicKey.export({ type: 'spki', format: 'der' })).digest('base64');
    const options = { headless: true, args: ['--ignore-certificate-errors-spki-list=' + pin] };
    if (process.env.KAFDECK_G2_BROWSER_EXECUTABLE) options.executablePath = process.env.KAFDECK_G2_BROWSER_EXECUTABLE;
    browser = await chromium.launch(options);
    const context = await browser.newContext();
    const page = await context.newPage();
    page.setDefaultTimeout(20000);
    await page.goto(target.href, { waitUntil: 'domcontentloaded' });
    await page.locator('#username').fill(manifest.humanUsername);
    await page.locator('#password').fill(fs.readFileSync(manifest.humanPasswordFile, 'utf8').trim());
    const completed = page.waitForURL(url => url.origin === callback.origin && url.pathname === callback.pathname, { timeout: 30000 });
    await page.locator('#kc-login').click();
    await completed;
    await context.close();
    process.stdout.write('G2_RENDERED_BROWSER_CALLBACK_COMPLETED\n');
  } catch {
    process.stderr.write('G2 rendered browser sign-in failed; private details withheld.\n');
    process.exitCode = 3;
  } finally {
    if (browser) await browser.close();
  }
})();
