import { useEffect, useRef, useState, type FormEvent } from 'react';
import { t } from '../shared/i18n.js';
import { productDescription, productName } from '../shared/product.js';
import { readRecentSignIns, removeRecentSignIn } from '../shared/recentSignIns.js';
import type { AccessMode, SetupState } from '../shared/api.js';
import { LanguageSelector } from './LanguageSelector.js';
import { InitialSetup } from './InitialSetup.js';
import { dashboardPageFor } from './dashboardPage.js';

interface SignInPageProps {
  mode: AccessMode | null;
  busy: boolean;
  error: 'invalid' | 'unavailable' | null;
  connectionWorkspace?: boolean;
  setupState?: SetupState | null;
  onSetupState?: (state: SetupState) => void;
  onToken: (token: string, label: string) => void;
  onRetry: () => void;
}

export function SignInPage({ mode, busy, error, connectionWorkspace, setupState, onSetupState, onToken, onRetry }: SignInPageProps) {
  const [recent, setRecent] = useState(readRecentSignIns);
  const [label, setLabel] = useState('');
  const tokenInput = useRef<HTMLInputElement>(null);
  const heading = useRef<HTMLHeadingElement>(null);
  const profiles = recent.filter(entry => entry.mode === mode);
  const returnPage = typeof window === 'undefined' ? undefined : dashboardPageFor(window.location.hash.slice(1));
  const signInHref = returnPage
    ? `/api/v1/auth/login?returnUrl=${encodeURIComponent(`/${window.location.search}#${returnPage}`)}`
    : '/api/v1/auth/login';
  const settingUp = setupState === 'available' || setupState === 'recovery';

  useEffect(() => {
    if (!busy && document.activeElement === document.body) heading.current?.focus();
  }, [busy]);

  function submitToken(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const input = tokenInput.current;
    const token = input?.value.trim();
    if (busy || !input || !token) return;
    input.value = '';
    onToken(token, label.trim());
  }

  function profileName(entry: (typeof profiles)[number]) {
    return entry.label || (entry.mode === 'oidc' ? t("Identity provider") : t("Deployment token"));
  }

  return <div className="kafdeck-sign-in">
    <header className="kafdeck-sign-in-header">
      <div className="kafdeck-brand">
        <img className="kafdeck-brand-logo" src="/kafdeck-logo.webp" alt="" width={42} height={42} />
        <span className="kafdeck-accent" aria-hidden="true" />
        <span className="kafdeck-brand-copy">
          <span className="kafdeck-brand-name">{productName}</span>
          <span className="kafdeck-brand-subtitle">{t(productDescription)}</span>
        </span>
      </div>
      <LanguageSelector />
    </header>
    <main className="kafdeck-sign-in-main" aria-labelledby="kafdeck-sign-in-title">
      <h1 id="kafdeck-sign-in-title" ref={heading} tabIndex={-1}>{settingUp ? t("Set up Kafdeck") : t("Sign in to Kafdeck")}</h1>
      <div className={connectionWorkspace || settingUp ? 'kafdeck-sign-in-grid kafdeck-sign-in-grid-single' : 'kafdeck-sign-in-grid'}>
        <section className="card" aria-labelledby="kafdeck-sign-in-method">
          <div className="card-body">
            <h2 id="kafdeck-sign-in-method" className="h3">{settingUp ? t("First administrator") : t("Sign in")}</h2>
            {settingUp && !busy && onSetupState && <InitialSetup state={setupState} onState={onSetupState} />}
            {busy && <p role="status" className="text-secondary">{t("Checking operator session…")}</p>}
            {error === 'unavailable' && <p role="alert">{t("Sign-in could not be verified. Try again.")}</p>}
            {mode === 'token' && <form onSubmit={submitToken}>
              <label className="form-label" htmlFor="kafdeck-sign-in-token">{t("Deployment token")}</label>
              <input id="kafdeck-sign-in-token" ref={tokenInput} className="form-control" type="password"
                dir="ltr" autoComplete="off" required disabled={busy} aria-invalid={error === 'invalid'}
                aria-describedby={error === 'invalid' ? 'kafdeck-invalid-token' : undefined} />
              {error === 'invalid' && <p id="kafdeck-invalid-token" className="text-danger mt-2" role="alert">{t("This deployment token was not accepted.")}</p>}
              <label className="form-label mt-3" htmlFor="kafdeck-sign-in-profile">{t("Profile name (optional)")}</label>
              <input id="kafdeck-sign-in-profile" className="form-control" dir="auto" maxLength={40}
                value={label} onChange={event => setLabel(event.target.value)} disabled={busy} />
              <button className="btn btn-primary mt-3" type="submit" disabled={busy}>{t("Sign in")}</button>
            </form>}
            {mode === 'oidc' && !settingUp && !busy && <a className="btn btn-primary" href={signInHref}>{t("Sign in with your identity provider")}</a>}
            {(mode === null || mode === 'local') && !busy && <button className="btn btn-outline-primary" type="button" onClick={onRetry}>{t("Try again")}</button>}
          </div>
        </section>
        {!connectionWorkspace && !settingUp && <section className="card" aria-labelledby="kafdeck-recent-sign-ins">
          <div className="card-body">
            <h2 id="kafdeck-recent-sign-ins" className="h3">{t("Recent sign-ins")}</h2>
            {profiles.length === 0 && <p className="text-secondary mb-0">{t("Successful sign-ins will appear here on this browser.")}</p>}
            {profiles.length > 0 && <ul className="kafdeck-recent-sign-ins">
              {profiles.map(entry => <li key={entry.label}>
                {entry.mode === 'oidc'
                  ? <a className="btn btn-outline-primary" href={signInHref}><bdi>{profileName(entry)}</bdi></a>
                  : <button className="btn btn-outline-primary" type="button" disabled={busy} onClick={() => {
                    setLabel(entry.label);
                    tokenInput.current?.focus();
                  }}><bdi>{profileName(entry)}</bdi></button>}
                <button className="btn btn-outline-secondary" type="button"
                  aria-label={t("Remove {0} from recent sign-ins", [profileName(entry)])}
                  onClick={() => setRecent(removeRecentSignIn(entry.mode, entry.label))}>{t("Remove")}</button>
              </li>)}
            </ul>}
            {mode === 'token' && profiles.length > 0 && <p className="text-secondary mt-3 mb-0">{t("Choose a profile, then enter its deployment token.")}</p>}
          </div>
        </section>}
      </div>
    </main>
  </div>;
}
