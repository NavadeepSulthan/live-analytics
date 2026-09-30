/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react()],
  // 5173 is used by the collaborative-canvas project.
  server: { port: 5174, strictPort: true },
  worker: { format: 'es' },
  build: { sourcemap: true },
  // Unit tests only; e2e/ belongs to Playwright.
  test: { include: ['src/**/*.test.ts'] },
});
