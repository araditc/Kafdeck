import { t } from './shared/i18n.js';
import { initializeLocale } from './shared/i18n.js';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import '@tabler/core/dist/css/tabler.min.css';
import './app/kafdeck-theme.css';
import { AppShell } from './app/AppShell.js';

initializeLocale();

const systemColorScheme = window.matchMedia('(prefers-color-scheme: dark)');
const applySystemTheme = () => {
  document.documentElement.dataset.bsTheme = systemColorScheme.matches ? 'dark' : 'light';
};
applySystemTheme();
systemColorScheme.addEventListener('change', applySystemTheme);

const rootElement = document.getElementById('root');

if (rootElement === null) {
  throw new Error(t("Kafdeck root element was not found."));
}

createRoot(rootElement).render(
  <StrictMode>
    <AppShell />
  </StrictMode>,
);
