// Run against a local Studio server; all job review/submission calls are intercepted.
// Requires Playwright: npm install --no-save --package-lock=false playwright
const { chromium } = require('playwright');
const { expect } = require('playwright/test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

async function main() {
  const url = process.env.STUDIO_URL || 'http://127.0.0.1:8767/';
  const token = process.env.STUDIO_TOKEN;
  assert(token, 'Set STUDIO_TOKEN to the local preview session token.');
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}),
  });
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'studio-builder-'));
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.addInitScript(token => sessionStorage.setItem('moppy-token', token), token);
    let reviewed, submissions = 0;
    await page.route('**/api/plans', async route => {
      reviewed = route.request().postDataJSON().config;
      await route.fulfill({json: {id: 'test-plan', name: 'Builder test', config: reviewed,
        directory: reviewed.output_folder, yaml: JSON.stringify(reviewed), script: '# Test only',
        sha256: 'test', warnings: [], runtime: {version: 'test', module: 'test', monitor_walltime: '01:00:00'}}});
    });
    await page.route('**/api/runs/*/submit', async route => {
      submissions++;
      await route.fulfill({status: 400, json: {detail: 'Submission is disabled in browser tests.'}});
    });
    const api = async (endpoint, config) => {
      const response = await page.request.post(url + 'api/' + endpoint, {
        headers: {'X-MOPPY-Token': token}, data: config,
      });
      assert.equal(response.status(), 200, await response.text());
      return response.json();
    };
    const row = name => page.locator('.variable-row').filter({has: page.locator('code', {hasText: new RegExp('^' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '$')})});
    const checked = name => row(name).locator('[data-select-variable]');
    const click = async id => { await page.locator('#' + id).click(); await expect(page.locator('#' + id)).toBeEnabled(); };
    await page.goto(url + '#build');
    await expect(page.locator('#variable-count')).toHaveText('3 of 3 selected');
    const template = await (await page.request.get(url + 'api/template', {headers: {'X-MOPPY-Token': token}})).json();
    const cmip7Variables = [
      'atmos.tas.tavg-h2m-hxy-u.mon.glb',
      'atmos.pr.tavg-u-hxy-u.mon.glb',
      'ocean.tos.tavg-u-hxy-sea.mon.glb',
    ];
    assert.equal(template.config.cmip_version, 'CMIP7');
    assert.deepEqual(template.config.variables, cmip7Variables);
    await expect(page.locator('[data-key="cmip_version"]')).toHaveValue('CMIP7');
    for (const name of cmip7Variables) await expect(checked(name)).toBeChecked();
    await expect(page.locator('#variable-add')).toHaveAttribute('placeholder', cmip7Variables.join('\n'));
    await page.locator('#cmor-target').selectOption('CMIP6');
    await expect(page.locator('#summary-target')).toHaveText('CMIP6');
    await expect(page.locator('#target-variables-note')).toBeVisible();
    await click('review');
    await expect(page.locator('#notice')).toContainText('CMIP6 requires table.variable');
    assert.equal(reviewed, undefined);
    for (const target of ['CMIP6', 'CMIP6Plus', 'CMIP7']) {
      const before = await page.locator('.variable-heading code').allTextContents();
      await page.locator('#cmor-target').selectOption(target);
      // Merely switching target never discards existing names or settings.
      assert.deepEqual(await page.locator('.variable-heading code').allTextContents(), before);
      await expect(page.locator('#use-starter-variables')).toHaveText('Use ' + target + ' starter variables');
      await click('use-starter-variables');
      const names = target === 'CMIP7' ? cmip7Variables : ['Amon.tas', 'Amon.pr', 'Omon.tos'];
      assert.deepEqual(await page.locator('.variable-heading code').allTextContents(), names);
      await expect(page.locator('#target-variables-note')).not.toBeVisible();
      await expect(page.locator('#variable-add')).toHaveAttribute('placeholder', names.join('\n'));
      await expect(page.locator('#summary-target')).toHaveText(target);
      await click('tab-yaml');
      const switched = (await api('config/parse', {yaml: await page.locator('#yaml').inputValue()})).config;
      assert.equal(switched.cmip_version, target);
      assert.deepEqual(switched.variables, names);
      assert.equal(switched.input_folder, template.config.input_folder);
      assert.equal(switched.output_folder, template.config.output_folder);
      await click('tab-builder');
      await click('review');
      assert.equal(reviewed.cmip_version, target);
      await expect(page.locator('#review-content')).toContainText('Target: ' + target);
      await page.locator('#back-review').click();
    }
    const original = {...template.config, cmip_version: 'CMIP6',
      variables: ['Amon.tas', 'Amon.pr', 'Omon.tos'], scheduler_options: '#PBS -P iq82',
      input_folder: '/g/data/p73/archive/example', output_folder: '/scratch/iq82/test/new-batch',
      script_dir: '/scratch/iq82/test/new-batch',
      parent_info: {parent_experiment_id: 'piControl', branch_time_in_parent: 73048.5},
      file_patterns: {'Amon.tas': 'atmosphere/*.nc'},
      variable_resources: {'Amon.tas': {mem: '32GB'}, 'Amon.pr': {cpus_per_node: 2}},
      custom_science: {nested: [1, 'keep', false]},
    };
    const yaml = (await api('config/render', {config: original})).yaml;
    const fixture = path.join(directory, 'batch.yml');
    await fs.writeFile(fixture, yaml);
    await page.locator('#config-path').fill(fixture);
    await click('load-path');
    await expect(page.locator('#notice')).toContainText('Batch config loaded');
    await expect(page.locator('[data-key="cmip_version"]')).toHaveValue('CMIP6');
    await checked('Amon.pr').uncheck();
    await page.locator('#variable-table').selectOption('Omon');
    await click('clear-visible');
    await expect(page.locator('#variable-count')).toHaveText('1 of 3 selected');
    await page.locator('#variable-table').selectOption('');
    await expect(checked('Amon.tas')).toBeChecked();
    await expect(checked('Amon.pr')).not.toBeChecked();
    await expect(checked('Omon.tos')).not.toBeChecked();
    await page.locator('#variable-search').fill('pr');
    await click('select-visible');
    await expect(page.locator('#variable-count')).toHaveText('2 of 3 selected');
    await page.locator('#variable-search').fill('');
    await checked('Amon.pr').uncheck();
    await page.locator('#cmor-target').selectOption('CMIP7');
    const compound = 'atmos.tas.tavg-h2m-hxy-u.mon.glb';
    await page.locator('#variable-add').fill('Amon.tas, ' + compound + '\nAmon.tas');
    await click('add-variables');
    await expect(page.locator('#variable-count')).toHaveText('2 of 4 selected');
    await row('Amon.tas').locator('summary').click();
    await row('Amon.tas').locator('[data-resource="mem"]').fill('64GB');
    await row('Amon.tas').locator('[data-resource="cpus_per_node"]').fill('8');
    await page.locator('[data-key="enable_chunking"]').selectOption('false');
    await page.locator('[data-key="use_jobfs_staging"]').selectOption('true');
    await page.locator('[data-key="qc_plots"]').check();
    await page.locator('[data-key="output_folder"]').fill('/scratch/iq82/test/edited-batch');
    await click('tab-yaml');
    let edited = (await api('config/parse', {yaml: await page.locator('#yaml').inputValue()})).config;
    assert.equal(edited.cmip_version, 'CMIP7');
    assert.deepEqual(edited.variables, ['Amon.tas', compound]);
    assert.deepEqual(edited.parent_info, original.parent_info);
    assert.deepEqual(edited.file_patterns, original.file_patterns);
    assert.deepEqual(edited.custom_science, original.custom_science);
    assert.deepEqual(edited.variable_resources, {'Amon.tas': {mem: '64GB', cpus_per_node: 8}, 'Amon.pr': {cpus_per_node: 2}});
    assert.equal(edited.enable_chunking, false);
    assert.equal(edited.use_jobfs_staging, true);
    assert.equal(edited.script_dir, edited.output_folder);
    await page.locator('#yaml').fill(await page.locator('#yaml').inputValue() + '\nstart_year: 2000\nend_year: 2001\n');
    await expect(page.locator('#summary-vars')).toHaveText('2');
    await click('apply-yaml');
    await expect(page.locator('#variable-count')).toHaveText('2 of 4 selected');
    await expect(checked('Omon.tos')).not.toBeChecked();
    await expect(page.locator('[data-key="start_year"]')).toHaveValue('2000');
    await row('Amon.tas').locator('summary').click();
    await row('Amon.tas').locator('[data-resource="mem"]').fill('');
    const downloadPromise = page.waitForEvent('download');
    await click('download');
    const download = await downloadPromise;
    const exported = (await api('config/parse', {yaml: await fs.readFile(await download.path(), 'utf8')})).config;
    assert.deepEqual(exported.variables, ['Amon.tas', compound]);
    assert.deepEqual(exported.variable_resources['Amon.tas'], {cpus_per_node: 8});
    assert.deepEqual(exported.custom_science, original.custom_science);
    await click('review');
    await expect(page.locator('#review-dialog')).toBeVisible();
    assert.deepEqual(reviewed, exported);
    await expect(page.locator('#submit')).toBeDisabled();
    await page.locator('#back-review').click();
    await click('clear-visible');
    await click('review');
    await expect(page.locator('#notice')).toContainText('Select at least one variable');
    await expect(page.locator('#review-dialog')).not.toBeVisible();
    await click('download');
    await expect(page.locator('#notice')).toContainText('Select at least one variable');
    await page.locator('#variable-add').fill('Amon.tas');
    await click('download');
    await expect(page.locator('#notice')).toContainText('Click Add variables');
    await page.locator('#variable-add').fill('invalid <script>');
    await click('add-variables');
    await expect(page.locator('#notice')).toContainText('Use CMIP7 compound identifiers');
    await expect(page.locator('.variable-row')).toHaveCount(4);
    await page.locator('#variable-add').fill('');
    await click('select-visible');
    await page.locator('#upload').setInputFiles({name: 'bad.yml', mimeType: 'application/yaml', buffer: Buffer.from('variables: not-a-list')});
    await expect(page.locator('#notice')).toContainText('Use CMIP7 compound identifiers');
    await expect(page.locator('.variable-row')).toHaveCount(4);
    await page.locator('#upload').setInputFiles({name: 'batch.yml', mimeType: 'application/yaml', buffer: Buffer.from(yaml)});
    await expect(page.locator('#notice')).toContainText('Loaded batch.yml');
    await expect(page.locator('#variable-count')).toHaveText('3 of 3 selected');
    await row('Amon.pr').locator('.remove-variable').click();
    await expect(page.locator('#variable-count')).toHaveText('2 of 2 selected');
    await click('tab-yaml');
    const removed = (await api('config/parse', {yaml: await page.locator('#yaml').inputValue()})).config;
    assert(!('Amon.pr' in removed.variable_resources));
    await click('new-batch');
    await expect(page.locator('#variable-count')).toHaveText('3 of 3 selected');
    await expect(page.locator('[data-key="enable_chunking"]')).toHaveValue('');
    await expect(page.locator('[data-key="use_jobfs_staging"]')).toHaveValue('');
    await click('tab-yaml');
    const defaults = (await api('config/parse', {yaml: await page.locator('#yaml').inputValue()})).config;
    assert.equal(defaults.cmip_version, 'CMIP7');
    assert.deepEqual(defaults.variables, cmip7Variables);
    assert(!('enable_chunking' in defaults));
    assert(!('use_jobfs_staging' in defaults));
    await click('tab-builder');
    const saved = {id: 'saved-builder-test', state: 'review', name: 'Saved configuration',
      config: original, yaml, directory: original.output_folder, created: '2026-01-01T00:00:00Z'};
    await page.route('**/api/runs', route => route.fulfill({json: [saved]}));
    await page.route('**/api/runs/saved-builder-test', route => route.fulfill({json: saved}));
    await page.locator('nav [data-page="runs"]').click();
    await page.locator('[data-run="saved-builder-test"]').click();
    await click('edit-saved-plan');
    await expect(page.locator('#page-build')).toBeVisible();
    await expect(page.locator('#name')).toHaveValue('Saved configuration');
    await expect(page.locator('#variable-count')).toHaveText('3 of 3 selected');
    await checked('Amon.pr').uncheck();
    await click('review');
    assert.deepEqual(reviewed.variables, ['Amon.tas', 'Omon.tos']);
    await page.locator('#back-review').click();
    if (process.env.STUDIO_SCREENSHOT) {
      await page.locator('#variables-panel').scrollIntoViewIfNeeded();
      await page.screenshot({path: process.env.STUDIO_SCREENSHOT});
    }
    await page.locator('#theme').click();
    await expect(page.locator('body')).toHaveClass('dark');
    await page.setViewportSize({width: 390, height: 844});
    await page.locator('#variables-panel').scrollIntoViewIfNeeded();
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), 'Mobile page must not overflow horizontally');
    assert.equal(submissions, 0);
    assert.deepEqual(errors, []);
    if (process.env.STUDIO_SCREENSHOT) await page.screenshot({path: process.env.STUDIO_SCREENSHOT.replace(/\.png$/, '-mobile.png')});
    console.log('Passed: import, variable selection/filtering, manual additions, overrides, options, YAML round-trip, export, review, invalid input, reset, saved-plan editing and mobile layout. No jobs submitted.');
  } finally {
    await browser.close();
    await fs.rm(directory, {recursive: true, force: true});
  }
}
main().catch(error => {console.error(error); process.exitCode = 1;});
