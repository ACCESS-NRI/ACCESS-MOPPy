// Run against the local Studio preview. Scientific probes and job APIs are intercepted.
const { chromium } = require('playwright');
const { expect } = require('playwright/test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');

async function main() {
  const url = process.env.STUDIO_URL || 'http://127.0.0.1:8877/';
  const token = process.env.STUDIO_TOKEN;
  assert(token, 'Set STUDIO_TOKEN to the preview session token.');
  const browser = await chromium.launch({headless: true,
    ...(process.env.CHROMIUM_PATH ? {executablePath: process.env.CHROMIUM_PATH} : {})});
  try {
    const page = await browser.newPage({viewport: {width: 1440, height: 1100}});
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.addInitScript(token => sessionStorage.setItem('moppy-token', token), token);
    const tas = 'atmos.tas.tavg-h2m-hxy-u.mon.glb';
    const pr = 'atmos.pr.tavg-u-hxy-u.mon.glb';
    const unknown = 'atmos.unknown.tavg-u-hxy-u.mon.glb';
    const fixed = 'atmos.areacella.ti-u-hxy-u.fx.glb';
    const rows = [
      {name: tas, mapping: 'mapped', input_status: 'not_scanned', required_inputs: ['tas_native'], alias: 'Amon.tas'},
      {name: pr, mapping: 'mapped', input_status: 'not_scanned', required_inputs: ['rain', 'snow'], alias: 'Amon.pr'},
      {name: unknown, mapping: 'unsupported', input_status: 'not_scanned', reason: 'No validated mapping <script>unsafe()</script>'},
      {name: fixed, mapping: 'mapped', input_status: 'not_required', reason: 'Generated or bundled inputs'},
    ].map(r => ({...r, priority: 'Core', time_subsets: ['all']}));
    const result = {request_version: 'v1.2.2.5', experiment: 'piControl', priority: 'Core',
      opportunities: 'all', experiments: ['piControl', 'historical'], variables: rows,
      version: '1.9.1', module: 'conda/analysis3-26.11'};
    let lastQuery, lastScan, heldScan, hold = false, jobCalls = 0;
    await page.route('**/api/request', async route => {
      lastQuery = route.request().postDataJSON();
      await route.fulfill({json: {...result, variables: lastQuery.experiment ? rows : []}});
    });
    await page.route('**/api/assessment', async route => {
      lastScan = route.request().postDataJSON();
      const reply = () => route.fulfill({json: {version: '1.9.1', module: 'conda/analysis3-26.11',
        scan: {headers_read: 2}, variables: lastScan.config.variables.map(name => ({
          ...rows.find(r => r.name === name), name,
          mapping: name === unknown ? 'unsupported' : 'mapped',
          input_status: name === unknown ? 'not_scanned' : name === tas ? 'found' : name === pr ? 'missing_inputs' : name === fixed ? 'not_required' : 'missing_files',
          sample_files: name === tas ? ['output000/atmosphere/test.nc'] : [],
          missing_inputs: name === pr ? ['snow'] : [],
        }))}});
      if (hold) heldScan = reply; else await reply();
    });
    await page.route(/\/api\/(plans|runs\/[^/]+\/submit)$/, async route => {
      jobCalls++; await route.fulfill({status: 400, json: {detail: 'Jobs are disabled in this test'}});
    });
    const click = async id => {await page.locator('#' + id).click(); await expect(page.locator('#' + id)).toBeEnabled();};
    const row = name => page.locator('.variable-row').filter({has: page.locator('code', {hasText: name})});
    const checked = name => row(name).locator('[data-select-variable]');
    await page.goto(url);
    await expect(page.locator('#variable-count')).toHaveText('3 of 3 selected');
    await row(tas).locator('summary').click();
    await row(tas).locator('[data-resource="mem"]').fill('32GB');
    await click('request-experiments-load');
    assert.equal(lastQuery.experiment, '');
    await expect(page.locator('#request-experiments option')).toHaveCount(2);
    await page.locator('#request-priority').selectOption('High');
    await click('request-load');
    assert.equal(lastQuery.priority, 'High');
    assert.equal(lastQuery.experiment, 'piControl');
    await expect(page.locator('#variable-count')).toHaveText('3 of 5 selected');
    await expect(checked(unknown)).not.toBeChecked();
    await expect(checked(fixed)).not.toBeChecked();
    await expect(row(tas).locator('[data-resource="mem"]')).toHaveValue('32GB');
    await expect(row(unknown).locator('script')).toHaveCount(0);
    await click('request-load');
    await expect(page.locator('.variable-row')).toHaveCount(5);
    await page.locator('#select-mapped').click();
    await expect(checked(fixed)).toBeChecked();
    await expect(checked(unknown)).not.toBeChecked();
    await page.locator('[data-key="input_folder"]').fill('/tmp/archive');
    await expect(page.locator('#select-mapped')).toBeDisabled();
    await click('scan-inputs');
    assert.equal(lastScan.config.variables.length, 5);
    await expect(page.locator('#assessment-status')).toContainText('1 with inputs found');
    await page.locator('#select-found').click();
    await expect(page.locator('#variable-count')).toHaveText('1 of 5 selected');
    await expect(checked(tas)).toBeChecked();
    await page.locator('#variable-assessment').selectOption('missing');
    await expect(page.locator('.variable-row')).toHaveCount(2);
    await page.locator('#variable-assessment').selectOption('');
    const downloadPromise = page.waitForEvent('download');
    await click('download-assessment');
    const download = await downloadPromise;
    const report = JSON.parse(await fs.readFile(await download.path(), 'utf8'));
    assert.equal(report.variables.length, 5);
    assert.equal(report.assessment.version, '1.9.1');
    await click('tab-yaml');
    const yaml = await page.locator('#yaml').inputValue();
    assert(yaml.includes(tas)); assert(!yaml.includes(unknown));
    assert(yaml.includes('32GB'));
    await click('tab-builder');
    await expect(page.locator('#select-found')).toBeDisabled();
    hold = true;
    await page.locator('#scan-inputs').click();
    await expect.poll(() => Boolean(heldScan)).toBe(true);
    await page.locator('[data-key="model_id"]').fill('ACCESS-CM3');
    await heldScan();
    await expect(page.locator('#scan-inputs')).toBeEnabled();
    await expect(page.locator('#assessment-status')).toContainText('Settings changed during the scan');
    await expect(page.locator('#select-found')).toBeDisabled();
    await page.locator('#cmor-target').selectOption('CMIP6');
    await expect(page.locator('#request-load')).toBeDisabled();
    await expect(page.locator('#scan-inputs')).toBeEnabled();
    await page.locator('#cmor-target').selectOption('CMIP7');
    await page.locator('[data-key="model_id"]').fill('ACCESS-ESM1-6');
    await click('request-load');
    await page.locator('#request-panel').scrollIntoViewIfNeeded();
    await page.screenshot({path: '/tmp/studio-cmip7-request.png', fullPage: true});
    await page.setViewportSize({width: 390, height: 844});
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    await page.screenshot({path: '/tmp/studio-cmip7-request-mobile.png', fullPage: true});
    assert.equal(jobCalls, 0);
    assert.deepEqual(errors, []);
    console.log('CMIP7 request browser checks passed; no job API called.');
  } finally {await browser.close();}
}
main().catch(error => {console.error(error); process.exitCode = 1;});
