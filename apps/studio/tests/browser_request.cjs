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
    let lastQuery, lastScan, heldScan, heldRequest, hold = false, holdRequest = false, failRequest = false, jobCalls = 0;
    let runs = [], runsUnavailable = false;
    await page.route('**/api/runs', route => route.fulfill(runsUnavailable
      ? {status: 503, json: {detail: 'Batch history unavailable'}} : {json: runs}));
    await page.route('**/api/runs/recent-plan', route => route.fulfill({json: runs[0]}));
    await page.route('**/api/request', async route => {
      lastQuery = route.request().postDataJSON();
      const query = lastQuery;
      const reply = () => failRequest
        ? route.fulfill({status: 503, json: {detail: 'Request release unavailable'}})
        : route.fulfill({json: {...result, experiment: query.experiment, priority: query.priority, variables: query.experiment ? rows : []}});
      if (holdRequest) heldRequest = reply; else await reply();
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
    const resultCheck = name => page.locator('[data-request-name="' + name + '"]');
    await page.goto(url);
    await expect(page.locator('#page-home')).toBeVisible();
    await expect(page.locator('#page-build')).not.toBeVisible();
    await expect(page.locator('[data-page="home"]')).toHaveAttribute('aria-current', 'page');
    await expect(page.locator('#home-draft-count')).toHaveText('3');
    await expect(page.locator('#home-run-count')).toHaveText('0');
    await expect(page.locator('#home-recent')).toContainText('No saved batches yet');
    await page.screenshot({path: '/tmp/studio-overview.png', fullPage: true});
    await page.locator('#page-home [data-build-section="request-panel"]').click();
    await expect(page.locator('#page-build')).toBeVisible();
    await expect(page.locator('#request-panel')).toBeFocused();
    await expect(page.locator('#variable-count')).toHaveText('3 of 3 selected');
    await row(tas).locator('summary').click();
    await row(tas).locator('[data-resource="mem"]').fill('32GB');
    await click('request-experiments-load');
    assert.equal(lastQuery.experiment, '');
    await expect(page.locator('#request-experiments option')).toHaveCount(2);
    await page.locator('#request-experiment').fill('historical');
    await expect(page.locator('[data-key="experiment_id"]')).toHaveValue('historical');
    await page.locator('#request-priority').selectOption('High');
    await click('request-load');
    assert.equal(lastQuery.priority, 'High');
    assert.equal(lastQuery.experiment, 'historical');
    await expect(page.locator('#variable-count')).toHaveText('3 of 3 selected');
    await expect(page.locator('#request-results')).toBeVisible();
    await expect(page.locator('#request-status')).toContainText('Nothing has been added yet');
    await expect(resultCheck(unknown)).not.toBeChecked();
    await expect(resultCheck(fixed)).toBeChecked();
    await expect(row(fixed)).toHaveCount(0);
    await expect(row(tas).locator('[data-resource="mem"]')).toHaveValue('32GB');
    await expect(page.locator('#request-list script')).toHaveCount(0);
    await page.locator('#request-search').fill('unknown');
    await expect(page.locator('.request-choice')).toHaveCount(1);
    await page.locator('#request-select-visible').click();
    await expect(resultCheck(unknown)).toBeChecked();
    await page.locator('#request-search').fill('');
    await page.locator('#request-clear-selection').click();
    await expect(page.locator('#request-add')).toBeDisabled();
    await page.locator('#request-select-mapped').click();
    await expect(page.locator('#request-add')).toHaveText('Add 3 selected variables to batch →');
    await resultCheck(pr).uncheck();
    await expect(page.locator('#request-selection-count')).toContainText('2 selected');
    await page.locator('#request-panel').screenshot({path: '/tmp/studio-request-preview.png'});
    await click('request-add');
    await expect(page.locator('#variable-count')).toHaveText('4 of 4 selected');
    await expect(page.locator('#variables-panel')).toBeFocused();
    await expect(page.locator('#request-added')).toContainText('Added 1 new variable and selected 1 existing entry');
    await expect(checked(fixed)).toBeChecked();
    await expect(row(unknown)).toHaveCount(0);
    await expect(checked(pr)).toBeChecked(); // Existing selections are retained.
    await expect(row(tas).locator('[data-resource="mem"]')).toHaveValue('32GB');
    await click('request-add');
    await expect(page.locator('.variable-row')).toHaveCount(4); // Add twice never duplicates.
    // Unsupported entries only enter the batch through explicit selection and Add.
    await resultCheck(unknown).check();
    await click('request-add');
    await expect(checked(unknown)).toBeChecked();
    await expect(page.locator('#variable-count')).toHaveText('5 of 5 selected');
    await page.locator('[data-page="home"]').click();
    await expect(page.locator('#home-draft-count')).toHaveText('5');
    await expect(page.locator('#home-draft-context')).toContainText('historical');
    runs = [{id: 'recent-plan', name: 'Review this batch', state: 'review', created: '2026-11-28T00:00:00Z'},
      {id: 'old-run', name: 'Earlier run', state: 'completed', created: '2026-11-27T00:00:00Z'}];
    await click('home-refresh');
    await expect(page.locator('#home-review-count')).toHaveText('1');
    await expect(page.locator('#home-run-count')).toHaveText('2');
    runsUnavailable = true;
    await click('home-refresh');
    await expect(page.locator('#home-run-count')).toHaveText('—');
    await expect(page.locator('#home-recent')).toContainText('Batch history is unavailable');
    runsUnavailable = false;
    await click('home-refresh');
    await page.locator('[data-open-run="recent-plan"]').click();
    await expect(page.locator('#page-runs')).toBeVisible();
    await expect(page.locator('#run-detail')).toContainText('Review this batch');
    await page.locator('[data-page="build"]').click();
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
    await page.locator('#yaml').fill(yaml + '\nstart_year: 1850\nend_year: 1850\n');
    await page.locator('[data-page="home"]').click();
    await page.locator('#page-home [data-build-section="request-panel"]').click();
    await expect(page.locator('[data-key="start_year"]')).toHaveValue('1850');
    await expect(page.locator('[data-key="end_year"]')).toHaveValue('1850');
    await expect(page.locator('#select-found')).toBeDisabled();
    await expect(page.locator('#request-results')).not.toBeVisible();
    holdRequest = true;
    await page.locator('#request-load').click();
    await expect.poll(() => Boolean(heldRequest)).toBe(true);
    await page.locator('#request-experiment').fill('piControl');
    await heldRequest();
    await expect(page.locator('#request-load')).toBeEnabled();
    await expect(page.locator('#request-results')).not.toBeVisible();
    await expect(page.locator('#variable-count')).toHaveText('1 of 5 selected');
    holdRequest = false; failRequest = true;
    await click('request-load');
    await expect(page.locator('#request-status')).toContainText('Request release unavailable');
    await expect(page.locator('#variable-count')).toHaveText('1 of 5 selected');
    failRequest = false;
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
    await page.locator('#request-panel').screenshot({path: '/tmp/studio-cmip7-request.png'});
    await page.setViewportSize({width: 390, height: 844});
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    await page.locator('#request-panel').screenshot({path: '/tmp/studio-cmip7-request-mobile.png'});
    await page.locator('[data-page="home"]').click();
    await expect(page.locator('#page-home')).toBeVisible();
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    await page.locator('#theme').click();
    await page.screenshot({path: '/tmp/studio-overview-mobile-dark.png', fullPage: true});
    await page.goto(url + '#unknown');
    await expect(page.locator('#page-home')).toBeVisible();
    assert.equal(jobCalls, 0);
    assert.deepEqual(errors, []);
    console.log('CMIP7 request browser checks passed; no job API called.');
  } finally {await browser.close();}
}
main().catch(error => {console.error(error); process.exitCode = 1;});
