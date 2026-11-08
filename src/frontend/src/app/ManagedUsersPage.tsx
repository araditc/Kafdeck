import { useEffect, useRef, useState, type FormEvent } from 'react';
import { ApiProblem, kafdeckApi, type ManagedUser, type ManagedUserAccess, type ManagedUsers } from '../shared/api.js';
import { t } from '../shared/i18n.js';
import { LanguageSelector } from './LanguageSelector.js';

function roleLabel(role: ManagedUser['role']) {
  if (role === 'owner') return t("First administrator");
  if (role === 'manager') return t("Cluster manager");
  return role === 'operator' ? t("Operator") : t("Viewer");
}

function AccessFields({ clusters, names, user }: { clusters: string[]; names: Record<string, string> | undefined; user?: ManagedUser }) {
  return <>
    <div className="mb-3">
      <label className="form-label" htmlFor="kafdeck-managed-role">{t("Role")}</label>
      <select className="form-select" id="kafdeck-managed-role" name="role" defaultValue={user?.role ?? 'viewer'}>
        <option value="viewer">{roleLabel('viewer')}</option>
        <option value="operator">{roleLabel('operator')}</option>
        <option value="manager">{roleLabel('manager')}</option>
      </select>
    </div>
    <fieldset className="mb-3">
      <legend className="h4">{t("Cluster access")}</legend>
      {clusters.length === 0 && <p className="text-secondary">{t("No configured clusters.")}</p>}
      {clusters.map(id => <label key={id} className="form-check">
        <input className="form-check-input" type="checkbox" name="cluster" value={id} defaultChecked={user?.clusterIds.includes(id)} />
        <span className="form-check-label"><bdi>{names?.[id] ?? id}</bdi></span>
      </label>)}
    </fieldset>
  </>;
}

export function ManagedUsersPage({ onBack, onLogout }: { onBack: () => void; onLogout: () => void }) {
  const [data, setData] = useState<ManagedUsers | null>(null);
  const [editing, setEditing] = useState<ManagedUser | null>(null);
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  const heading = useRef<HTMLHeadingElement>(null);

  useEffect(() => {
    const controller = new AbortController();
    heading.current?.focus();
    void kafdeckApi.listManagedUsers(controller.signal)
      .then(users => { if (!controller.signal.aborted) setData(users); })
      .catch(() => { if (!controller.signal.aborted) setProblem(t("Users could not be loaded.")); });
    return () => controller.abort();
  }, []);

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    const form = event.currentTarget;
    const fields = new FormData(form);
    const access = {
      role: fields.get('role') as ManagedUserAccess['role'],
      clusterIds: fields.getAll('cluster').map(String),
    };
    setBusy(true);
    setProblem(null);
    try {
      const user = editing
        ? await kafdeckApi.updateManagedUser(editing.id, { ...access, enabled: fields.has('enabled') })
        : await kafdeckApi.createManagedUser({ ...access, email: String(fields.get('email') ?? ''), password: String(fields.get('password') ?? '') });
      setData(previous => previous && ({
        ...previous,
        users: editing ? previous.users.map(entry => entry.id === user.id ? user : entry) : [...previous.users, user],
      }));
      form.reset();
      setEditing(null);
    } catch (error) {
      if (error instanceof ApiProblem && error.code?.endsWith(':identity-duplicate')) setProblem(t("A user with that email already exists."));
      else setProblem(t("The user change could not be completed. Reload the list before retrying."));
      void kafdeckApi.listManagedUsers().then(setData).catch(() => {});
    } finally {
      setBusy(false);
    }
  }

  return <div className="kafdeck-sign-in">
    <header className="kafdeck-sign-in-header">
      <span className="kafdeck-brand-name">Kafdeck</span>
      <div className="kafdeck-connection-actions">
        <LanguageSelector />
        <button className="btn btn-outline-primary" type="button" onClick={onBack}>{t("Back to Kafdeck")}</button>
        <button className="btn btn-outline-secondary" type="button" onClick={onLogout}>{t("Sign out")}</button>
      </div>
    </header>
    <main className="kafdeck-sign-in-main kafdeck-connection-main" aria-labelledby="kafdeck-managed-users">
      <h1 id="kafdeck-managed-users" ref={heading} tabIndex={-1}>{t("Manage users")}</h1>
      {problem && <p className="alert alert-danger" role="alert">{problem}</p>}
      {!data && !problem && <p role="status">{t("Loading users…")}</p>}
      {data && <div className="kafdeck-sign-in-grid">
        <section className="card"><div className="card-body">
          <h2 className="h3">{t("Users")}</h2>
          <ul className="list-group list-group-flush">
            {data.users.map(user => <li className="list-group-item px-0" key={user.id}>
              <strong><bdi>{user.email}</bdi></strong>
              <p className="mb-1 text-secondary">{roleLabel(user.role)} · {user.enabled ? t("Enabled") : t("Disabled")}</p>
              {user.role !== 'owner' && <>
                <p className="mb-2">{user.clusterIds.length ? user.clusterIds.map((id, i) => <span key={id}>{i > 0 && ', '}<bdi>{data.clusterNames?.[id] ?? id}</bdi></span>) : t("No cluster access")}</p>
                <button className="btn btn-sm btn-outline-primary" type="button" disabled={busy}
                  aria-label={t("Edit access for {0}", [user.email])} onClick={() => { setEditing(user); setProblem(null); }}>{t("Edit access")}</button>
              </>}
            </li>)}
          </ul>
        </div></section>
        <section className="card"><div className="card-body">
          <h2 className="h3">{editing ? t("Edit user access") : t("Create user")}</h2>
          {editing ? <p><bdi>{editing.email}</bdi></p> : <p className="text-secondary">{t("The user must change the temporary password on first sign-in.")}</p>}
          <form key={editing?.id ?? 'new'} onSubmit={event => void save(event)}>
            <fieldset disabled={busy}>
              {!editing && <>
                <label className="form-label mb-3">{t("User email")}
                  <input className="form-control mt-1" type="email" name="email" autoComplete="off" dir="ltr" required maxLength={254} />
                </label>
                <label className="form-label mb-3">{t("Temporary password")}
                  <input className="form-control mt-1" type="password" name="password" autoComplete="new-password" required minLength={12} maxLength={128} />
                </label>
              </>}
              <AccessFields clusters={data.clusterIds} names={data.clusterNames} {...(editing ? { user: editing } : {})} />
              {editing && <label className="form-check mb-3">
                <input className="form-check-input" type="checkbox" name="enabled" defaultChecked={editing.enabled} />
                <span className="form-check-label">{t("Account enabled")}</span>
              </label>}
              <div className="d-flex flex-wrap gap-2">
                <button className="btn btn-primary" type="submit">{editing ? t("Save access") : t("Create user")}</button>
                {editing && <button className="btn btn-outline-secondary" type="button" onClick={() => setEditing(null)}>{t("Cancel")}</button>}
              </div>
            </fieldset>
          </form>
        </div></section>
      </div>}
    </main>
  </div>;
}
