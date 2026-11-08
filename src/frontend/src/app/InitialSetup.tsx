import { useState, type FormEvent } from 'react';
import { ApiProblem, kafdeckApi, type SetupState } from '../shared/api.js';
import { t } from '../shared/i18n.js';

export function InitialSetup({ state, onState }: {
  state: Exclude<SetupState, 'complete'>;
  onState: (state: SetupState) => void;
}) {
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const fields = new FormData(form);
    const code = String(fields.get('code') ?? '').trim();
    const email = String(fields.get('email') ?? '').trim();
    const password = String(fields.get('password') ?? '');
    if (busy) return;
    if (password !== fields.get('confirmation')) {
      setProblem(t("The passwords must match."));
      return;
    }
    setBusy(true);
    setProblem(null);
    try {
      const next = await kafdeckApi.completeInitialSetup({ code, email, password });
      form.reset();
      onState(next);
    } catch (error) {
      if (error instanceof ApiProblem && error.code?.endsWith(':setup-closed')) {
        window.location.reload();
        return;
      }
      if (error instanceof ApiProblem && error.code?.endsWith(':setup-code')) setProblem(t("The setup code is invalid or expired."));
      else if (error instanceof ApiProblem && error.code?.endsWith(':setup-realm-in-use')) setProblem(t("The identity realm already has users. Contact the installer."));
      else setProblem(t("Setup could not be completed. Check the settings or contact the installer."));
    } finally {
      setBusy(false);
    }
  }

  if (state === 'recovery') return <p role="alert">{t("Setup needs recovery on the server. Contact the installer.")}</p>;
  return <>
    <p className="text-secondary">{t("Enter the one-time code from the Kafdeck server to create the first administrator.")}</p>
    <form onSubmit={event => void submit(event)}>
      <fieldset disabled={busy}>
        <label className="form-label mb-3">{t("Setup code")}
          <input className="form-control mt-1" type="password" name="code" autoComplete="off" dir="ltr" required maxLength={32} />
        </label>
        <label className="form-label mb-3">{t("Administrator email")}
          <input className="form-control mt-1" type="email" name="email" autoComplete="username" dir="ltr" required maxLength={254} />
        </label>
        <label className="form-label mb-3">{t("Administrator password")}
          <input className="form-control mt-1" type="password" name="password" autoComplete="new-password" required minLength={12} maxLength={128} />
        </label>
        <label className="form-label mb-3">{t("Confirm password")}
          <input className="form-control mt-1" type="password" name="confirmation" autoComplete="new-password" required minLength={12} maxLength={128} />
        </label>
        <button className="btn btn-primary" type="submit">{t("Create administrator")}</button>
      </fieldset>
      {busy && <p className="mt-3 mb-0" role="status">{t("Creating the administrator…")}</p>}
      {problem && <p className="alert alert-danger mt-3 mb-0" role="alert">{problem}</p>}
    </form>
  </>;
}
