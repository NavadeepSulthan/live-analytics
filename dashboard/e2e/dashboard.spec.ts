import { expect, test, type Page } from '@playwright/test';

const READ_KEY = process.env.READ_KEY ?? 'demo-read-key-change-me-0';

async function connect(page: Page) {
  await page.goto('/');
  await page.getByLabel('Read API key').fill(READ_KEY);
  await page.getByRole('button', { name: 'Connect' }).click();
  await expect(page.getByTestId('feed-status')).toHaveText(/live/, { timeout: 15_000 });
}

test('rejects an invalid read key', async ({ page }) => {
  await page.goto('/');
  await page.getByLabel('Read API key').fill('definitely-not-a-valid-key');
  await page.getByRole('button', { name: 'Connect' }).click();
  await expect(page.getByRole('alert')).toContainText('rejected');
});

test('streams live metrics and events', async ({ page }) => {
  await connect(page);
  await expect(page.getByTestId('kpi-requests-s')).not.toContainText('–', { timeout: 15_000 });
  await expect(page.getByTestId('kpi-query-time')).toContainText('ms');
  await expect(page.getByTestId('log-row').first()).toBeVisible();
  await expect(page.locator('.services tbody tr').first()).not.toContainText('No traffic');
});

test('virtualizes the log and pages in older events on scroll', async ({ page }) => {
  await connect(page);
  const heading = page.locator('.logs h2');
  await expect(heading).toContainText(/[1-9]/);
  const loadedCount = async () => Number((await heading.innerText()).replace(/\D/g, ''));
  const before = await loadedCount();

  const scroller = page.getByTestId('log-scroll');
  await expect(async () => {
    await scroller.evaluate((el) => el.scrollTo({ top: el.scrollHeight }));
    expect(await loadedCount()).toBeGreaterThan(before);
  }).toPass({ timeout: 15_000 });

  expect(await page.getByTestId('log-row').count()).toBeLessThan(100);
});

test('filters events by service and switches to historical ranges', async ({ page }) => {
  await connect(page);
  const firstService = page.locator('.services tbody tr').first();
  await expect(firstService).not.toContainText('No traffic');
  const name = (await firstService.locator('td').first().innerText()).trim();
  await firstService.click();
  await expect(page.getByLabel('Service filter')).toHaveValue(name);
  await expect(page.getByTestId('log-row').first()).toBeVisible();
  const services = await page.locator('[data-testid=log-row] span:nth-child(2)').allInnerTexts();
  expect(new Set(services)).toEqual(new Set([name]));

  await page.getByRole('button', { name: '1h', exact: true }).click();
  await expect(page.getByTestId('feed-status')).toHaveText(/historical/);
  await expect(page.getByTestId('kpi-query-time')).toContainText('ms');
});
