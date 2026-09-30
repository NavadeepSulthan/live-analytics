import { defineConfig, devices } from '@playwright/test';

// Expects the stack to be running (make infra-up, the three services, and traffic from
// `make load`); in CI the workflow starts them before this suite.
export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [['github'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL: process.env.DASHBOARD_URL ?? 'http://localhost:5174',
    trace: 'retain-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: process.env.DASHBOARD_URL
    ? undefined
    : { command: 'npm run dev', url: 'http://localhost:5174', reuseExistingServer: true },
});
