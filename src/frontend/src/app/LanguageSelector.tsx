import { setLocale, useLocale } from '../shared/i18n.js';
export function LanguageSelector() {
  const locale = useLocale();
  return <div className="kafdeck-language"><label className="form-label" htmlFor="kafdeck-language">{locale === 'fa' ? 'زبان' : 'Language'}</label>
    <select id="kafdeck-language" className="form-select form-select-sm" value={locale} onChange={event => setLocale(event.target.value)}>
      <option value="en" lang="en" dir="ltr">English</option><option value="fa" lang="fa" dir="rtl">فارسی</option>
    </select></div>;
}
