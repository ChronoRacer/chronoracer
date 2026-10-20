import { defineConfig, searchForWorkspaceRoot } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';

export default defineConfig({
  root: path.resolve('src/client'),
  plugins: [react()],
  build: {
    outDir: path.resolve('dist/client'),
    emptyOutDir: true,
  },
  server: {
    fs: {
      allow: [searchForWorkspaceRoot(process.cwd()), path.resolve('../infra/contract')],
    },
    proxy: {
      '/auth': 'http://127.0.0.1:8080',
      '/healthz': 'http://127.0.0.1:8080',
      '/api': 'http://127.0.0.1:8080',
      '/ws': {
        target: 'ws://127.0.0.1:8080',
        ws: true,
      },
    },
  },
});
