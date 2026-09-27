let inMemoryDeploymentAccessToken: string | null = null;

function scrubAccessTokenFragment(fragment: URLSearchParams) {
  fragment.delete('access_token');
  const remainder = fragment.toString();
  history.replaceState(
    null,
    '',
    `${window.location.pathname}${window.location.search}${remainder ? `#${remainder}` : ''}`,
  );
}

export function deploymentAccessToken(): string | null {
  const fragment = new URLSearchParams(
    window.location.hash.startsWith('#')
      ? window.location.hash.slice(1)
      : window.location.hash,
  );
  const supplied = fragment.get('access_token');
  if (supplied) {
    inMemoryDeploymentAccessToken = supplied;
    scrubAccessTokenFragment(fragment);
  }
  return inMemoryDeploymentAccessToken;
}

export function withDeploymentAccessToken(
  headers: Record<string, string>,
): Record<string, string> {
  const token = deploymentAccessToken();
  if (token) headers['X-Kafdeck-Access-Token'] = token;
  return headers;
}

export function clearDeploymentAccessToken() {
  inMemoryDeploymentAccessToken = null;
}
