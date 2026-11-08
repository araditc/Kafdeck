type RecentSignIn = { mode: 'token' | 'oidc'; label: string };
export const recentSignInsKey = 'kafdeck.recent-sign-ins.v1';
const maxEntries = 5;

function safeEntry(value: unknown): RecentSignIn | null {
  if (!value || typeof value !== 'object') return null;
  const { mode, label } = value as Partial<RecentSignIn>;
  if ((mode !== 'oidc' && mode !== 'token') || typeof label !== 'string') return null;
  const cleanLabel = label.trim();
  if (cleanLabel.length > 40 || /[\p{Cc}\u202a-\u202e\u2066-\u2069]/u.test(cleanLabel)) return null;
  return { mode, label: mode === 'oidc' ? '' : cleanLabel };
}

function normalize(values: unknown[]): RecentSignIn[] {
  const result: RecentSignIn[] = [];
  for (const value of values) {
    const entry = safeEntry(value);
    if (entry && !result.some(item => item.mode === entry.mode && item.label === entry.label)) result.push(entry);
    if (result.length >= maxEntries) break;
  }
  return result;
}

export function readRecentSignIns(): RecentSignIn[] {
  try {
    const saved: unknown = JSON.parse(window.localStorage.getItem(recentSignInsKey) ?? '[]');
    return Array.isArray(saved) ? normalize(saved) : [];
  } catch { return []; }
}

function saveRecentSignIns(entries: RecentSignIn[]) {
  try { window.localStorage.setItem(recentSignInsKey, JSON.stringify(normalize(entries))); }
  catch { /* Remembering labels is optional. */ }
}

export function rememberSignIn(mode: RecentSignIn['mode'], label = '') {
  saveRecentSignIns([{ mode, label }, ...readRecentSignIns()]);
}

export function removeRecentSignIn(mode: RecentSignIn['mode'], label: string) {
  const remaining = readRecentSignIns().filter(item => item.mode !== mode || item.label !== label);
  saveRecentSignIns(remaining);
  return remaining;
}
