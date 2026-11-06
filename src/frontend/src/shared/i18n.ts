import { useSyncExternalStore } from 'react';
import { messages } from './locales/messages.js';

export type Locale = 'en' | 'fa';
export const localePreferenceKey = 'kafdeck.locale.v1';
let locale: Locale = 'en';
const listeners = new Set<() => void>();
export function normalizeLocale(value: unknown): Locale { return value === 'fa' ? 'fa' : 'en'; }
export function getLocale(): Locale { return locale; }
export function translate(language: Locale, message: string, values: readonly (string | number)[] = []): string {
  const translated = language === 'fa' && Object.hasOwn(messages, message) ? messages[message] ?? message : message;
  // Values remain plain React text. Isolates prevent an identifier from changing
  // the visual order of surrounding RTL copy; no HTML or locale numeric conversion.
  return translated.replace(/\{(\d+)\}/g, (token, index: string) => {
    const value = values[Number(index)];
    return value === undefined ? token : language === 'fa' ? `\u2068${String(value)}\u2069` : String(value);
  });
}
/** Only application-owned message literals belong here; never pass backend prose. */
export function t(message: string, values?: readonly (string | number)[]): string { return translate(locale, message, values); }
export function setLocale(value: unknown, persist = true): void {
  const next = normalizeLocale(value);
  if (typeof document !== 'undefined') { document.documentElement.lang = next; document.documentElement.dir = next === 'fa' ? 'rtl' : 'ltr'; }
  if (persist && typeof window !== 'undefined') { try { window.localStorage.setItem(localePreferenceKey, next); } catch { /* Preference storage is optional. */ } }
  if (locale === next) return;
  locale = next;
  for (const listener of listeners) listener();
}
export function initializeLocale(): void {
  let saved: unknown = 'en';
  try { saved = window.localStorage.getItem(localePreferenceKey); } catch { /* English fallback without storage. */ }
  setLocale(saved, false);
  window.addEventListener('storage', event => { if (event.key === localePreferenceKey || event.key === null) setLocale(event.newValue, false); });
}
export function useLocale(): Locale {
  return useSyncExternalStore(listener => { listeners.add(listener); return () => { listeners.delete(listener); }; }, getLocale, () => 'en');
}
