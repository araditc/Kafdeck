import { build } from 'vite';
import { fileURLToPath } from 'node:url';
await build({ root: fileURLToPath(new URL('./', import.meta.url)), build: {
  outDir: fileURLToPath(new URL('../../dist/host-harness', import.meta.url)), emptyOutDir: true,
} });
