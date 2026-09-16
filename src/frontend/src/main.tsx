import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { AppShell } from './app/AppShell.js';

const rootElement = document.getElementById('root');

if (rootElement === null) {
  throw new Error('Kafdeck root element was not found.');
}

createRoot(rootElement).render(
  <StrictMode>
    <AppShell />
  </StrictMode>,
);
