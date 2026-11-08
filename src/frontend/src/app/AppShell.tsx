import { applicationProblem, ownedErrorPresentation, problemPresentation, type PresentationProblem } from '../shared/presentationProblem.js';
import { t } from '../shared/i18n.js';
import { useLocale } from '../shared/i18n.js';
import { LanguageSelector } from './LanguageSelector.js';
import { SignInPage } from './SignInPage.js';
import { KafkaConnectionPage } from './KafkaConnectionPage.js';
import { ManagedUsersPage } from './ManagedUsersPage.js';
import { clearDeploymentAccessToken, deploymentAccessToken, setDeploymentAccessToken } from '../shared/deploymentAccess.js';
import { rememberSignIn } from '../shared/recentSignIns.js';
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import {
  ApiProblem,
  kafdeckApi,
  operatorSessionLostEvent,
  type AccessMode,
  type SetupState,
  type ApiEnvelope,
  type ClusterData,
  type ConfigurationEntryData,
  type TopicDetailData,
  type TopicListItem,
  type OperatorSession,
  type NotificationSubscriptionListData,
  type ReadViewEnvelope,
  type TopicCatalogEntry,
  type FleetCapabilityStatus,
} from '../shared/api.js';
import { RecordExplorer } from '../features/records/RecordExplorer.js';
import { TopicRecordPublisher } from '../features/records/TopicRecordPublisher.js';
import { ReadViewsExplorer } from '../features/readviews/ReadViewsExplorer.js';
import { RegisteredConnectView } from '../features/readviews/RegisteredConnectView.js';
import { RegisteredKsqlView } from '../features/readviews/RegisteredKsqlView.js';
import { NotificationObservationPanel } from '../features/notifications/NotificationObservationPanel.js';
import { NotificationSubscriptionManagementPanel } from '../features/notifications/NotificationSubscriptionManagementPanel.js';
import { NotificationDestinationManagementPanel } from '../features/notifications/NotificationDestinationManagementPanel.js';
import { productDescription, productName } from '../shared/product.js';
import { describeObservation, shouldAutoRefresh, visibleRefreshIntervalMs } from './operatorState.js';
import { CommandPalette } from './CommandPalette.js';
import { dashboardPageFor, dashboardPages, type DashboardPage } from './dashboardPage.js';
import { MutationOperationsPanel } from '../features/mutations/MutationOperationsPanel.js';
import { StatusBadge } from './StatusBadge.js';
import { AutomationPanel } from '../features/automation/AutomationPanel.js';
import { CatalogPanel } from '../features/governance/CatalogPanel.js';
import { PolicyPanel } from '../features/governance/PolicyPanel.js';
import { RequestPanel } from '../features/governance/RequestPanel.js';
import { AttributionPanel } from '../features/governance/AttributionPanel.js';
import { fleetCapabilityStatusKind } from './statusPresentation.js';

function observationText(envelope: ApiEnvelope<unknown>) {
  const observed = new Date(envelope.observation.observedAt);
  return t("{0} · observed {1}", [describeObservation(envelope.observation), observed.toISOString()]);
}

function problemText(reason: unknown): PresentationProblem {
  if (reason instanceof ApiProblem && reason.status === 401) return applicationProblem("Operator authentication is required.");
  if (reason instanceof ApiProblem && reason.status === 403 && reason.code === 'urn:kafdeck:problem:operator-authorization-denied') return applicationProblem("Access denied by Kafdeck operator authorization policy.");
  if (reason instanceof ApiProblem && reason.status === 403) return applicationProblem("Access denied by Kafka authorization policy.");
  if (reason instanceof ApiProblem && reason.status === 501) return applicationProblem("This capability is unsupported by the connected Kafka cluster.");
  return ownedErrorPresentation(reason) ?? (reason instanceof Error ? { raw: reason.message } : applicationProblem("The requested observation could not be loaded."));
}

function ConfigurationView({ entries }: { entries: ConfigurationEntryData[] }) {
  if (entries.length === 0) return <p className="text-secondary">{t("No configuration entries are observable.")}</p>;
  return <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">{t("Name")}</th><th scope="col">{t("Value")}</th><th scope="col">{t("Source")}</th></tr></thead><tbody>{entries.map(entry => <tr key={entry.name}><th scope="row"><bdi dir="auto">{entry.name}</bdi></th><td>{entry.isSensitive ? <span className="badge bg-orange-lt">{t("Sensitive value redacted")}</span> : (entry.value ?? t("Not set"))}</td><td>{entry.source ?? t("Unknown")}</td></tr>)}</tbody></table></div>;
}

/** Pure visibility policy: NotificationRead and NotificationManage are independent. */
export function notificationManagementSurfaces(readAvailable: boolean, manageAvailable: boolean) {
  return {
    navigation: readAvailable || manageAvailable,
    insideRead: readAvailable && manageAvailable,
    standalone: !readAvailable && manageAvailable,
  };
}

/** A subscription-management grant does not imply destination-governance availability. */
export function destinationGovernanceVisible(manageAvailable: boolean, governanceAvailable: boolean) {
  return manageAvailable && governanceAvailable;
}

/** Only the server's opaque identity/authentication/policy binding can preserve a review. */
export function authoritativeSessionScope(session: unknown): string | null {
  if (!session || typeof session !== 'object' || !Object.hasOwn(session, 'sessionScope')) return null;
  const scope = (session as { sessionScope?: unknown }).sessionScope;
  return typeof scope === 'string' && /^[a-f0-9]{64}$/i.test(scope) ? scope : null;
}

export function canPreserveOperatorSession(currentScope: string | null, incoming: unknown): boolean {
  return currentScope !== null && currentScope === authoritativeSessionScope(incoming);
}

/** Retain a verified focus candidate until the actual window focus arrives. */
export function verificationFocusDecision(input: {
  candidateEpoch: number; currentEpoch: number; candidateScope: string;
  currentScope: string | null; connected: boolean; documentFocused: boolean; bodyActive: boolean;
}): 'discard' | 'defer' | 'restore' {
  if (input.candidateEpoch !== input.currentEpoch || input.candidateScope !== input.currentScope || !input.connected) return 'discard';
  if (!input.documentFocused) return 'defer';
  return input.bodyActive ? 'restore' : 'discard';
}

export function AppShell() {
  useLocale();
  const [operator, setOperator] = useState<OperatorSession | null>(null);
  const [operatorScope, setOperatorScope] = useState<string | null>(null);
  const [verificationPending, setVerificationPending] = useState(true);
  const privatePage = useRef<HTMLDivElement>(null);
  const verifiedSession = useRef<{ scope: string | null; key: string; mode: AccessMode } | null>(null);
  const legacySessionEpoch = useRef(0);
  const verificationEpoch = useRef(0);
  const verificationFocus = useRef<{ element: HTMLElement; scope: string; epoch: number; ready: boolean } | null>(null);
  const [accessMode, setAccessMode] = useState<AccessMode | null>(null);
  const accessModeRef = useRef<AccessMode | null>(null);
  const [connectionWorkspace, setConnectionWorkspace] = useState(false);
  const [setupState, setSetupState] = useState<SetupState | null>(null);
  const [userManagementScope, setUserManagementScope] = useState<string | null>(null);
  const [signInError, setSignInError] = useState<'invalid' | 'unavailable' | null>(null);
  const [clusters, setClusters] = useState<Pick<ClusterData, 'clusterId' | 'displayName'>[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState<string>(() =>
    typeof window === 'undefined' ? '' : new URLSearchParams(window.location.search).get('cluster') ?? '');
  const selectedClusterRef = useRef(selectedClusterId);
  const [requestedPage, setRequestedPage] = useState<DashboardPage>(() =>
    typeof window === 'undefined' ? 'overview' : dashboardPageFor(window.location.hash.slice(1)) ?? 'overview');
  const [consumerRefresh, setConsumerRefresh] = useState(0);
  const [pageFocus, setPageFocus] = useState<{ id: string } | null>(null);
  const [dashboardLocation, setDashboardLocation] = useState(() =>
    typeof window !== 'undefined' && dashboardPageFor(window.location.hash.slice(1)) !== undefined);
  const pageHeading = useRef<HTMLHeadingElement>(null);
  useEffect(() => {
    const onLocation = () => {
      const page = dashboardPageFor(window.location.hash.slice(1));
      setRequestedPage(page ?? 'overview');
      setDashboardLocation(page !== undefined);
      setPageFocus({ id: 'kafdeck-dashboard-page-title' });
    };
    window.addEventListener('popstate', onLocation);
    window.addEventListener('hashchange', onLocation);
    return () => {
      window.removeEventListener('popstate', onLocation);
      window.removeEventListener('hashchange', onLocation);
    };
  }, []);
  useEffect(() => {
    if (!pageFocus) return;
    const frame = window.requestAnimationFrame(() => {
      if (!pageHeading.current) return;
      const target = document.getElementById(pageFocus.id) ?? pageHeading.current;
      if (!target.matches('input,button,select,textarea,a[href],[tabindex]')) target.tabIndex = -1;
      window.scrollTo(0, 0);
      target.focus({ preventScroll: true });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [pageFocus]);

  function navigateDashboard(target: string, focus?: string) {
    const destination = dashboardPageFor(target);
    if (destination) {
      if (window.location.hash !== `#${destination}`) window.history.pushState(null, '', `#${destination}`);
      setRequestedPage(destination);
      setDashboardLocation(true);
    }
    setPageFocus({ id: focus ?? 'kafdeck-dashboard-page-title' });
  }
  const [selected, setSelected] = useState<ApiEnvelope<ClusterData> | null>(null);
  const [clusterObservationError, setClusterObservationError] = useState<PresentationProblem | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<PresentationProblem | null>(null);
  const [brokerConfiguration, setBrokerConfiguration] = useState<{ brokerId: number; entries: ConfigurationEntryData[] } | null>(null);
  const [brokerError, setBrokerError] = useState<PresentationProblem | null>(null);
  const [topicQuery, setTopicQuery] = useState('');
  const [topics, setTopics] = useState<TopicListItem[]>([]);
  const [topicCursor, setTopicCursor] = useState<string | null>(null);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState<PresentationProblem | null>(null);
  const [topicDetail, setTopicDetail] = useState<ApiEnvelope<TopicDetailData> | null>(null);
  const [activeTopic, setActiveTopic] = useState<string | null>(null);
  const [topicView, setTopicView] = useState<'summary' | 'configuration' | 'messages' | 'publish' | 'manage' | 'purge'>('summary');
  const topicHeading = useRef<HTMLHeadingElement>(null);
  const topicDetailRequest = useRef<AbortController | null>(null);
  const topicListGeneration = useRef(0);
  const topicListPending = useRef(false);
  const loadedExtraTopics = useRef(false);
  const loadedTopicQuery = useRef<string | null>(null);
  const clusterListRequest = useRef<{ scope: string; signal: AbortSignal | undefined } | null>(null);
  const [topicConfiguration, setTopicConfiguration] = useState<ConfigurationEntryData[] | null>(null);
  const [topicDetailError, setTopicDetailError] = useState<PresentationProblem | null>(null);
  const [topicConfigurationError, setTopicConfigurationError] = useState<PresentationProblem | null>(null);
  const [topicCatalog, setTopicCatalog] = useState<ReadViewEnvelope<TopicCatalogEntry> | null>(null);
  const [fleetCapabilities, setFleetCapabilities] = useState<FleetCapabilityStatus[]>([]);
  const [fleetError, setFleetError] = useState<PresentationProblem | null>(null);
  const [notificationView, setNotificationView] = useState<{ sessionKey: string; page: NotificationSubscriptionListData } | null>(null);
  const [notificationManagement, setNotificationManagement] = useState<{ sessionKey: string; destinationGovernanceAvailable: boolean } | null>(null);
  const sessionCheckAbort = useRef<AbortController | null>(null);
  const recentConnectionScope = operator?.recentConnectionScope;
  const showManagedWorkspace = !recentConnectionScope || dashboardLocation;
  const selectedDirectoryEntry = clusters.find(cluster => cluster.clusterId === selectedClusterId);
  const canUseTopicTools = !!selectedDirectoryEntry &&
    (!selectedDirectoryEntry.displayName || operator?.canManageUsers || operator?.managedClusterManager);

  useEffect(() => {
    if (activeTopic) topicHeading.current?.focus({ preventScroll: true });
  }, [activeTopic]);

  const clearObservations = useCallback(() => {
    topicDetailRequest.current?.abort();
    setActiveTopic(null);
    ++topicListGeneration.current;
    topicListPending.current = false;
    loadedExtraTopics.current = false;
    loadedTopicQuery.current = null;
    selectedClusterRef.current = '';
    clusterListRequest.current = null;
    setClusters([]); setSelectedClusterId(''); setSelected(null); setError(null); setLoading(false);
    setClusterObservationError(null);
    setBrokerConfiguration(null); setBrokerError(null); setTopicQuery('');
    setTopics([]); setTopicCursor(null); setTopicsLoading(false); setTopicsError(null); setTopicDetail(null);
    setTopicConfiguration(null); setTopicDetailError(null); setTopicCatalog(null);
    setFleetCapabilities([]); setFleetError(null);
  }, []);

  const invalidateOperatorSession = useCallback(() => {
    sessionCheckAbort.current?.abort();
    sessionCheckAbort.current = null;
    verificationEpoch.current += 1;
    verificationFocus.current = null;
    if (privatePage.current) { privatePage.current.hidden = true; privatePage.current.inert = true; privatePage.current.style.display = 'none'; }
    if (verifiedSession.current) clearObservations();
    verifiedSession.current = null;
    setOperator(null); setOperatorScope(null);
    setUserManagementScope(null);
    setNotificationView(null); setNotificationManagement(null);
    clearDeploymentAccessToken();
    setVerificationPending(false); setSignInError(null);
  }, [clearObservations]);

  const revalidateOperatorSession = useCallback((submission?: { token: string; label: string }) => {
    sessionCheckAbort.current?.abort();
    const controller = new AbortController();
    sessionCheckAbort.current = controller;
    const epoch = ++verificationEpoch.current;
    const previous = verifiedSession.current;
    const active = document.activeElement;
    const originalFocus = active instanceof HTMLElement && privatePage.current?.contains(active)
      ? active : verificationFocus.current?.element ?? null;
    verificationFocus.current = previous && originalFocus?.isConnected
      ? { element: originalFocus, scope: previous.key, epoch, ready: false } : null;
    // Fence synchronously, before fetch or React's next commit. Mounted private
    // state is neither visible, focusable nor in the accessibility tree pending
    // the authoritative response, including the operator name in the header.
    if (privatePage.current) {
      privatePage.current.hidden = true; privatePage.current.inert = true;
      privatePage.current.style.display = 'none';
    }
    setVerificationPending(true);
    setSignInError(null);
    const current = () => !controller.signal.aborted && sessionCheckAbort.current === controller;
    let attemptedToken = false;
    void (async () => {
      const access = accessModeRef.current ? null : await kafdeckApi.getAccessMode(controller.signal);
      if (!current()) return null;
      const mode = access?.authenticationMode ?? accessModeRef.current;
      if (mode !== 'token' && mode !== 'oidc' && mode !== 'local') throw new Error('Invalid authentication mode.');
      if (access) setConnectionWorkspace(access.connectionWorkspace === true);
      if (access) setSetupState(access.setupState ?? null);
      accessModeRef.current = mode;
      setAccessMode(mode);
      if (mode === 'token') {
        if (submission) setDeploymentAccessToken(submission.token);
        attemptedToken = !!deploymentAccessToken();
      } else {
        clearDeploymentAccessToken();
      }
      if (mode === 'token' && !attemptedToken && !previous) return null;
      return kafdeckApi.getOperatorSession(controller.signal);
    })()
      .then(session => {
        if (!current() || !session) return;
        const mode = session.authenticationMode;
        if (session.authenticated !== true || mode !== accessModeRef.current) throw new Error('Invalid operator session.');
        const scope = authoritativeSessionScope(session);
        const sameScope = previous?.mode === mode && !submission &&
          (mode !== 'oidc' || canPreserveOperatorSession(previous.scope, session));
        const key = sameScope && previous ? previous.key : scope !== null ? `server:${scope}` : `legacy:${++legacySessionEpoch.current}`;
        if (!sameScope) {
          if (previous) clearObservations();
          setNotificationView(null); setNotificationManagement(null);
          verificationFocus.current = null;
        } else if (verificationFocus.current?.epoch === epoch) {
          verificationFocus.current.ready = true;
        }
        if (!previous && session.authenticationMode === 'oidc' && !session.recentConnectionScope) rememberSignIn('oidc', '');
        if (!previous && mode === 'token' && submission) {
          const { token, label } = submission;
          rememberSignIn('token', label && !token.includes(label) && !label.includes(token) ? label : '');
        }
        verifiedSession.current = { scope, key, mode };
        setOperatorScope(key); setOperator(session.authenticationMode === 'oidc' ? session : null);
      })
      .catch(reason => {
        if (!current()) return;
        if (verifiedSession.current) clearObservations();
        verifiedSession.current = null; verificationFocus.current = null;
        setOperator(null); setOperatorScope(null);
        setUserManagementScope(null);
        setNotificationView(null); setNotificationManagement(null);
        if (attemptedToken) clearDeploymentAccessToken();
        const denied = reason instanceof ApiProblem && (reason.status === 401 || reason.status === 403);
        setSignInError(denied ? (attemptedToken ? 'invalid' : null) : 'unavailable');
      })
      .finally(() => {
        if (current()) {
          sessionCheckAbort.current = null;
          setVerificationPending(false);
        }
      });
  }, [clearObservations]);

  useLayoutEffect(() => {
    // Also reconcile direct fencing when a very fast response batches both
    // pending state changes into one React commit.
    if (privatePage.current) {
      privatePage.current.hidden = verificationPending;
      privatePage.current.inert = verificationPending;
      privatePage.current.style.display = verificationPending ? 'none' : '';
    }
    const restore = verificationFocus.current;
    if (!verificationPending && restore?.ready) {
      const decision = verificationFocusDecision({
        candidateEpoch: restore.epoch, currentEpoch: verificationEpoch.current,
        candidateScope: restore.scope, currentScope: operatorScope,
        connected: restore.element.isConnected, documentFocused: document.hasFocus(),
        bodyActive: document.activeElement === document.body,
      });
      // A visibility response can precede the browser's actual focus event.
      // Keep the connected candidate then; a later focused verification may
      // recover it, while another focused control always ends the candidate.
      if (decision !== 'defer') verificationFocus.current = null;
      if (decision === 'restore') restore.element.focus({ preventScroll: false });
    }
  });

  useEffect(() => {
    const onSessionLost = () => invalidateOperatorSession();
    const onReturnToTab = () => {
      if (verifiedSession.current && document.visibilityState === 'visible') revalidateOperatorSession();
    };
    window.addEventListener(operatorSessionLostEvent, onSessionLost);
    window.addEventListener('focus', onReturnToTab);
    document.addEventListener('visibilitychange', onReturnToTab);
    revalidateOperatorSession();
    return () => {
      window.removeEventListener(operatorSessionLostEvent, onSessionLost);
      window.removeEventListener('focus', onReturnToTab);
      document.removeEventListener('visibilitychange', onReturnToTab);
      sessionCheckAbort.current?.abort();
    };
  }, [invalidateOperatorSession, revalidateOperatorSession]);

  const loadClusters = useCallback(async (signal?: AbortSignal, quiet = false) => {
    const observationScope = verifiedSession.current?.key ?? null;
    if (!observationScope) return;
    const pending = clusterListRequest.current;
    if (pending?.scope === observationScope && !pending.signal?.aborted) return;
    const request = { scope: observationScope, signal };
    clusterListRequest.current = request;
    const current = () => clusterListRequest.current === request && !signal?.aborted &&
      verifiedSession.current?.key === observationScope;
    if (!quiet) { setLoading(true); setError(null); }
    try {
      const response = await kafdeckApi.listClusterDirectory(signal);
      if (!current()) return;
      setClusters(response.data);
      const currentId = selectedClusterRef.current;
      const nextId = response.data.some(item => item.clusterId === currentId)
        ? currentId : response.data[0]?.clusterId ?? '';
      selectedClusterRef.current = nextId;
      setSelectedClusterId(nextId);
      if (nextId !== currentId) {
        setSelected(null);
        setClusterObservationError(null);
      }
      setError(null);
      setLoading(false);
    } catch (reason) {
      if (quiet || !current()) return;
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      setError(problemText(reason));
    } finally {
      if (!quiet && current()) setLoading(false);
      if (clusterListRequest.current === request) clusterListRequest.current = null;
    }
  }, [operatorScope]);

  const loadTopics = useCallback(async (clusterId: string, query: string, cursor: string | null, append = false, signal?: AbortSignal, quiet = false) => {
    if (!clusterId) return;
    const observationScope = verifiedSession.current?.key ?? null;
    if (!observationScope || (quiet && (topicListPending.current || loadedExtraTopics.current))) return;
    const generation = ++topicListGeneration.current;
    topicListPending.current = true;
    const current = () => generation === topicListGeneration.current && !signal?.aborted && selectedClusterRef.current === clusterId &&
      (verifiedSession.current?.key ?? null) === observationScope;
    if (!quiet) { setTopicsLoading(true); setTopicsError(null); }
    try {
      const response = await kafdeckApi.listTopics(clusterId, query, cursor, signal);
      if (!current()) return;
      if (append) loadedExtraTopics.current = true;
      else if (!quiet) loadedExtraTopics.current = false;
      loadedTopicQuery.current = JSON.stringify([clusterId, query]);
      setTopics(previous => {
        if (append) return [...previous, ...response.data.items];
        // Keep row identity and order when the broker returns the same page.
        if (JSON.stringify(previous) === JSON.stringify(response.data.items)) return previous;
        return response.data.items;
      });
      setTopicCursor(response.data.nextCursor);
      setTopicsError(null);
    } catch (reason) {
      if (quiet || !current()) return;
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      setTopicsError(problemText(reason));
    } finally {
      if (generation === topicListGeneration.current) topicListPending.current = false;
      if (!quiet && current()) setTopicsLoading(false);
    }
  }, [operatorScope]);

  useEffect(() => {
    const observationScope = verifiedSession.current?.key ?? null;
    if (!operatorScope || !observationScope || !showManagedWorkspace) return;
    const controller = new AbortController();
    void loadClusters(controller.signal);
    void kafdeckApi.getFleetCapabilities(controller.signal)
      .then(response => {
        if (controller.signal.aborted || (verifiedSession.current?.key ?? null) !== observationScope) return;
        setFleetCapabilities(response.capabilities);
        setFleetError(null);
      })
      .catch(reason => {
        if (controller.signal.aborted || (verifiedSession.current?.key ?? null) !== observationScope) return;
        if (reason instanceof DOMException && reason.name === 'AbortError') return;
        setFleetError(problemText(reason));
      });
    return () => controller.abort();
  }, [loadClusters, operatorScope, showManagedWorkspace]);

  // Expose observation only to an OIDC operator after a successful
  // server-authorized NotificationRead collection preflight.
  useEffect(() => {
    setNotificationView(null);
    setNotificationManagement(null);
    if (!operator || !showManagedWorkspace) return;
    const controller = new AbortController();
    const sessionKey = operatorScope;
    if (!sessionKey) return;
    void kafdeckApi.listNotificationSubscriptions(50, null, controller.signal)
      .then(page => { if (!controller.signal.aborted && verifiedSession.current?.key === sessionKey) setNotificationView({ sessionKey, page }); })
      .catch(() => { if (!controller.signal.aborted && verifiedSession.current?.key === sessionKey) setNotificationView(null); });
    // Management is independently authorized and hidden when its explicit
    // server-side feature flag is disabled, unavailable or forbidden.
    void kafdeckApi.getNotificationManagementCapabilities(controller.signal)
      .then(capability => {
        if (!controller.signal.aborted && verifiedSession.current?.key === sessionKey && capability.subscriptionCasAvailable) {
          setNotificationManagement({ sessionKey, destinationGovernanceAvailable: capability.destinationGovernanceAvailable === true });
        }
      })
      .catch(() => { if (!controller.signal.aborted && verifiedSession.current?.key === sessionKey) setNotificationManagement(null); });
    return () => controller.abort();
  }, [operatorScope, showManagedWorkspace]);

  const notificationSessionKey = operator ? operatorScope : null;
  const visibleNotificationPage = notificationView?.sessionKey === notificationSessionKey
    ? notificationView?.page ?? null
    : null;
  const managementSurfaces = notificationManagementSurfaces(
    visibleNotificationPage !== null,
    notificationManagement?.sessionKey === notificationSessionKey,
  );

  const menu = dashboardPages.filter(([id]) => {
    if (id === 'notifications') return managementSurfaces.navigation;
    if (id === 'automation') return !!operator;
    if (id === 'ownership-catalog' || id === 'governance-policy' || id === 'service-requests' || id === 'governance-attribution')
      return !!operator && !!selectedClusterId;
    return true;
  });
  const activePage = menu.some(([id]) => id === requestedPage) ? requestedPage : 'overview';
  const [clusterRefresh, setClusterRefresh] = useState(0);
  const wantsClusterObservation = activePage === 'overview' || activePage === 'brokers';
  useEffect(() => {
    if (!showManagedWorkspace || !operatorScope || !selectedDirectoryEntry || !wantsClusterObservation) return;
    const controller = new AbortController();
    const clusterId = selectedDirectoryEntry.clusterId;
    const isCurrent = () => !controller.signal.aborted && selectedClusterRef.current === clusterId &&
      verifiedSession.current?.key === operatorScope;
    setClusterObservationError(null);
    const observe = async () => {
      try {
        const result = await kafdeckApi.getCluster(clusterId, controller.signal);
        if (isCurrent() && result.data.clusterId === clusterId) {
          setSelected(result);
          setClusterObservationError(null);
        }
      } catch (reason) {
        if (isCurrent()) setClusterObservationError(problemText(reason));
      }
    };
    void observe();
    return () => controller.abort();
  }, [showManagedWorkspace, operatorScope, selectedDirectoryEntry?.clusterId, wantsClusterObservation, clusterRefresh]);

  useEffect(() => {
    if (!selectedClusterId || !showManagedWorkspace || requestedPage !== 'topics' || activeTopic) return;
    if (loadedTopicQuery.current === JSON.stringify([selectedClusterId, topicQuery])) return;
    const controller = new AbortController();
    const timer = window.setTimeout(() => void loadTopics(selectedClusterId, topicQuery, null, false, controller.signal), topicQuery ? 200 : 0);
    return () => { window.clearTimeout(timer); controller.abort(); };
  }, [loadTopics, selectedClusterId, topicQuery, showManagedWorkspace, requestedPage, activeTopic]);

  useEffect(() => {
    if (!showManagedWorkspace || !operatorScope) return;
    const controller = new AbortController();
    let pending = false;
    const refresh = async () => {
      if (pending) return;
      if (!shouldAutoRefresh(document.visibilityState)) return;
      pending = true;
      try {
        if (!selectedClusterId) await loadClusters(controller.signal, true);
        else if (requestedPage === 'topics' && !activeTopic)
          await loadTopics(selectedClusterId, topicQuery, null, false, controller.signal, true);
        else if (wantsClusterObservation) {
          const result = await kafdeckApi.getCluster(selectedClusterId, controller.signal);
          if (!controller.signal.aborted && selectedClusterRef.current === selectedClusterId &&
            verifiedSession.current?.key === operatorScope) setSelected(result);
        }
      } catch { /* A background failure leaves the last successful view visible. */ }
      finally { pending = false; }
    };
    const timer = window.setInterval(refresh, visibleRefreshIntervalMs);
    document.addEventListener('visibilitychange', refresh);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', refresh);
      controller.abort();
    };
  }, [loadClusters, loadTopics, selectedClusterId, topicQuery, showManagedWorkspace, requestedPage, activeTopic, operatorScope, wantsClusterObservation]);

  const selectCluster = (clusterId: string) => {
    topicDetailRequest.current?.abort();
    setActiveTopic(null);
    ++topicListGeneration.current;
    topicListPending.current = false;
    loadedExtraTopics.current = false;
    loadedTopicQuery.current = null;
    selectedClusterRef.current = clusterId;
    setSelectedClusterId(clusterId);
    const location = new URL(window.location.href);
    if (clusterId) location.searchParams.set('cluster', clusterId);
    else location.searchParams.delete('cluster');
    window.history.replaceState(null, '', location);
    setSelected(null);
    setClusterObservationError(null);
    setError(null);
    setBrokerConfiguration(null);
    setBrokerError(null);
    setTopicQuery('');
    setTopics([]);
    setTopicCursor(null);
    setTopicsLoading(false);
    setTopicsError(null);
    setTopicDetail(null);
    setTopicConfiguration(null);
    setTopicDetailError(null);
    setTopicCatalog(null);
  };

  const openBroker = async (brokerId: number) => {
    const observationScope = verifiedSession.current?.key ?? null;
    const current = () => selectedClusterRef.current === selectedClusterId &&
      (verifiedSession.current?.key ?? null) === observationScope;
    setBrokerConfiguration(null);
    setBrokerError(null);
    try {
      const configuration = await kafdeckApi.getBrokerConfiguration(selectedClusterId, brokerId);
      if (!current()) return;
      setBrokerConfiguration({ brokerId, entries: configuration.data });
    } catch (reason) {
      if (current()) setBrokerError(problemText(reason));
    }
  };

  const openTopic = async (topicName: string, refresh = false) => {
    topicDetailRequest.current?.abort();
    const controller = new AbortController();
    topicDetailRequest.current = controller;
    const observationScope = verifiedSession.current?.key ?? null;
    const current = () => !controller.signal.aborted && selectedClusterRef.current === selectedClusterId &&
      (verifiedSession.current?.key ?? null) === observationScope;
    if (!refresh) {
      setActiveTopic(topicName);
      setTopicView('summary');
      setTopicDetail(null);
      setTopicConfiguration(null);
      setTopicCatalog(null);
      window.scrollTo(0, 0);
    }
    setTopicConfigurationError(null);
    setTopicDetailError(null);
    await Promise.allSettled([
      kafdeckApi.getTopic(selectedClusterId, topicName, controller.signal)
        .then(detail => { if (current()) setTopicDetail(detail); })
        .catch(reason => { if (current()) setTopicDetailError(problemText(reason)); }),
      kafdeckApi.getTopicConfiguration(selectedClusterId, topicName, controller.signal)
        .then(configuration => { if (current()) setTopicConfiguration(configuration.data); })
        .catch(reason => { if (current()) setTopicConfigurationError(problemText(reason)); }),
      kafdeckApi.getTopicCatalog(selectedClusterId, topicName, controller.signal)
        .then(catalog => { if (current()) setTopicCatalog(catalog); }),
    ]);
  };

  const closeTopic = () => {
    topicDetailRequest.current?.abort();
    setActiveTopic(null);
    setTopicDetail(null);
    setTopicDetailError(null);
    setTopicConfiguration(null);
    setTopicCatalog(null);
    setPageFocus({ id: 'topic-search' });
  };

  const signOut = () => {
    invalidateOperatorSession();
    if (operator) void kafdeckApi.logout().catch(() => setSignInError('unavailable'));
  };

  if (!operatorScope) return <SignInPage mode={accessMode} busy={verificationPending} error={signInError} connectionWorkspace={connectionWorkspace}
    setupState={setupState} onSetupState={setSetupState}
    onToken={(token, label) => revalidateOperatorSession({ token, label })} onRetry={() => revalidateOperatorSession()} />;

  const verificationStatus = verificationPending && <div className="container-xl py-3" role="status" aria-live="polite">
      <p>{t("Checking operator session…")}</p>
      <p>{t("Authorized content is hidden until verification completes.")}</p>
    </div>;

  const openUserManagement = operator?.canManageUsers ? () => setUserManagementScope(operatorScope) : null;
  if (openUserManagement && userManagementScope === operatorScope) return <>
    {verificationStatus}
    <div ref={privatePage} hidden={verificationPending} inert={verificationPending} aria-hidden={verificationPending || undefined} style={{display: verificationPending ? 'none' : undefined}}>
      <ManagedUsersPage onBack={() => setUserManagementScope(null)} onLogout={signOut} />
    </div>
  </>;

  if (recentConnectionScope && !showManagedWorkspace) return <>
    {verificationStatus}
    <div ref={privatePage} hidden={verificationPending} inert={verificationPending} aria-hidden={verificationPending || undefined} style={{display: verificationPending ? 'none' : undefined}}>
      <KafkaConnectionPage key={operatorScope} scope={recentConnectionScope} onLogout={signOut}
        onManaged={() => navigateDashboard('overview')}
        onOpenCluster={id => {
          selectCluster(id);
          navigateDashboard('topics');
        }}
        onManageUsers={openUserManagement} canRegister={operator?.canRegisterKafkaConnections === true} />
    </div>
  </>;

  const pageLabel = dashboardPages.find(([id]) => id === activePage)![1];

  return <>
    {verificationStatus}
    <div ref={privatePage} className="page kafdeck-shell" hidden={verificationPending} inert={verificationPending} aria-hidden={verificationPending || undefined} style={{display: verificationPending ? 'none' : undefined}}>
    <a className="kafdeck-skip-link" href="#kafdeck-main" onClick={event => {
      event.preventDefault();
      setPageFocus({ id: 'kafdeck-main' });
    }}>{t("Skip to main content")}</a>
    <header className="navbar navbar-expand-md bg-surface d-print-none kafdeck-topbar">
      <div className="container-xl">
        <h1 id="kafdeck-title" className="navbar-brand navbar-brand-autodark m-0">
          <a className="kafdeck-brand" href="#overview" onClick={event => {
            if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
            event.preventDefault();
            navigateDashboard('overview');
          }}>
            <img className="kafdeck-brand-logo" src="/kafdeck-logo.webp" alt="" width={42} height={42} />
            <span className="kafdeck-accent" aria-hidden="true" />
            <span className="kafdeck-brand-copy">
              <span id="kafdeck-brand-name" className="kafdeck-brand-name">{productName}</span>
              <span id="kafdeck-brand-subtitle" className="kafdeck-brand-subtitle">{t(productDescription)}</span>
            </span>
          </a>
        </h1>
        <div className="kafdeck-toolbar">
          <LanguageSelector />
          <CommandPalette navigate={navigateDashboard} />
          {openUserManagement && <button className="btn btn-sm btn-outline-primary" type="button" onClick={openUserManagement}>{t("Manage users")}</button>}
          {recentConnectionScope && <button className="btn btn-sm btn-outline-primary" type="button" onClick={() => {
            window.history.pushState(null, '', '#connections');
            setDashboardLocation(false);
          }}>{t("Kafka connections")}</button>}
          {(operator || accessMode === 'token') && <div className="kafdeck-auth">
            {operator && <><span className="text-secondary">{t("Signed in as")}</span><strong>{operator.displayName ?? operator.email ?? t("operator")}</strong></>}
            <button className="btn btn-sm btn-outline-secondary" type="button" onClick={signOut}>{t("Sign out")}</button>
          </div>}
          <div>
            <label className="form-label" htmlFor="cluster-selector">{t("Cluster")}</label>
            <select className="form-select form-select-sm" id="cluster-selector" value={selectedClusterId} onChange={event => selectCluster(event.target.value)} disabled={clusters.length === 0}>{clusters.map(cluster => <option key={cluster.clusterId} value={cluster.clusterId}>{cluster.displayName ?? cluster.clusterId}</option>)}</select>
          </div>
          <button className="btn btn-sm btn-outline-primary" type="button" onClick={() => {
            if (!selectedDirectoryEntry) void loadClusters(undefined, clusters.length > 0);
            else if (requestedPage === 'consumers') setConsumerRefresh(version => version + 1);
            else if (wantsClusterObservation) {
              setClusterRefresh(version => version + 1);
              void loadClusters(undefined, true);
            }
            else if (requestedPage !== 'topics') void loadClusters(undefined, true);
            else if (activeTopic) void openTopic(activeTopic, true);
            else void loadTopics(selectedClusterId, topicQuery, null);
          }} disabled={loading}>{t("Refresh")}</button>
        </div>
      </div>
    </header>
    <nav className="kafdeck-section-nav" aria-label={t("Kafdeck sections")}>
      <div className="container-xl">
        <ul className="nav nav-pills py-2">
          {menu.map(([id, label]) => <li className="nav-item" key={id}>
            <a className={`nav-link${id === activePage ? ' active' : ''}`} href={`#${id}`}
              aria-current={id === activePage ? 'page' : undefined} onClick={event => {
                if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
                event.preventDefault();
                navigateDashboard(id);
              }}>{t(label)}</a>
          </li>)}
        </ul>
      </div>
    </nav>
    <main id="kafdeck-main" tabIndex={-1} className="page-wrapper" aria-labelledby="kafdeck-dashboard-page-title">
      <div className="page-body kafdeck-main">
        <div className="container-xl">
          <div className="page-header d-print-none kafdeck-page-header">
            <div className="row g-2 align-items-center">
              <div className="col">
                <div className="page-pretitle">{t("Kafka operations")}</div>
                <h2 className="page-title" id="kafdeck-dashboard-page-title" tabIndex={-1} ref={pageHeading}>{t(pageLabel)}</h2>
              </div>
            </div>
          </div>
          {loading && clusters.length === 0 && <div className="alert alert-info kafdeck-status" role="status">{t("Loading cluster observations…")}</div>}
          {activePage === 'automation' && <>
            <AutomationPanel key={`automation:${operatorScope}`} notificationsAvailable={managementSurfaces.navigation} />
            {selectedClusterId && <MutationOperationsPanel key={`mutations:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />}
          </>}
          {activePage === 'ownership-catalog' && <CatalogPanel key={`catalog:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />}
          {activePage === 'governance-policy' && <PolicyPanel key={`policy:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />}
          {activePage === 'service-requests' && <RequestPanel key={`requests:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />}
          {activePage === 'governance-attribution' && <AttributionPanel key={`attribution:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />}
          {error && <div className="alert alert-danger kafdeck-status" role="alert">{problemPresentation(error)}</div>}
          {!loading && !error && clusters.length === 0 && <div className="alert alert-warning kafdeck-status" role="status">{t("No authorized clusters are available.")}</div>}
          {activePage === 'fleet' && <section className="card kafdeck-card" id="fleet" aria-labelledby="fleet-title">
            <h2 id="fleet-title" className="card-title">{t("Fleet Operations")}</h2>
            <p className="text-secondary">{t("Capability-driven status from the active Kafdeck runtime. Unsupported or blocked operations remain fail-closed rather than being simulated through alternate provider paths.")}</p>
            {fleetError && <div className="alert alert-danger" role="alert">{problemPresentation(fleetError)}</div>}
            {!fleetError && fleetCapabilities.length === 0 && <p role="status">{t("Loading fleet capability status…")}</p>}
            {fleetCapabilities.length > 0 && <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0 kafdeck-fleet-capability-table"><thead><tr><th scope="col">{t("Capability")}</th><th scope="col">{t("State")}</th><th scope="col">{t("Workstream")}</th><th scope="col">{t("Reason")}</th></tr></thead><tbody>{fleetCapabilities.map(capability => <tr key={capability.id}><th scope="row"><bdi dir="auto">{capability.displayName}</bdi></th><td><StatusBadge kind={fleetCapabilityStatusKind(capability.state)} label={capability.state} /></td><td>{capability.workstream}</td><td><bdi dir="auto">{capability.reason}</bdi></td></tr>)}</tbody></table></div>}
          </section>}
          {activePage === 'notifications' && <>{visibleNotificationPage && <NotificationObservationPanel key={notificationSessionKey ?? undefined}
              initialPage={visibleNotificationPage}
              canManageSubscriptions={managementSurfaces.insideRead} />}
          {destinationGovernanceVisible(
            notificationManagement?.sessionKey === notificationSessionKey,
            notificationManagement?.destinationGovernanceAvailable === true,
          ) &&
            <NotificationDestinationManagementPanel key={'destinations-' + (notificationSessionKey ?? '')} />}
          {managementSurfaces.standalone && <section className="card kafdeck-card"
            id="notifications" aria-label={t("Authorized notification subscription management")}>
            <NotificationSubscriptionManagementPanel key={notificationSessionKey ?? undefined}
              referenceSubscription={null} onUpdated={() => { /* No NotificationRead grant. */ }} />
          </section>}</>}
          {selectedDirectoryEntry && <>
            {wantsClusterObservation && clusterObservationError && <div className="alert alert-danger kafdeck-status" role="alert">{problemPresentation(clusterObservationError)}</div>}
            {wantsClusterObservation && !selected && !clusterObservationError && <div className="alert alert-info kafdeck-status" role="status">{t("Loading cluster observations…")}</div>}
            {activePage === 'overview' && selected && <section className="card kafdeck-card" id="overview" aria-labelledby="overview-title">
              <h2 id="overview-title" className="card-title">{t("Cluster overview")}</h2>
              <p className="kafdeck-observation"><strong><bdi dir="auto">{selected.data.displayName ?? selected.data.clusterId}</bdi></strong> · {observationText(selected)}</p>
              <dl><dt>{t("Kafka cluster ID")}</dt><dd>{selected.data.kafkaClusterId ?? t("Not observed")}</dd><dt>{t("Health")}</dt><dd><span className="badge bg-blue-lt"><bdi dir="ltr" lang="en">{String(selected.data.health)}</bdi></span></dd><dt>{t("Controller")}</dt><dd>{selected.data.controllerBrokerId ?? t("Not observed")}</dd><dt>{t("Brokers")}</dt><dd><bdi dir="ltr" lang="en">{selected.data.brokers.length}</bdi></dd></dl>
              {selected.limitations.length > 0 && <aside aria-label={t("Cluster limitations")}><h3 className="h4">{t("Limitations")}</h3><ul className="mb-0">{selected.limitations.map((item, index) => <li key={`${item.capability}-${index}`}>{item.capability ?? t("cluster")}: {item.state}{item.reason ? ` — ${item.reason}` : ''}</li>)}</ul></aside>}
            </section>}
            {activePage === 'brokers' && selected && <section className="card kafdeck-card" id="brokers" aria-labelledby="brokers-title">
              <h2 id="brokers-title" className="card-title">{t("Brokers")}</h2>
              {selected.data.brokers.length === 0 ? <p className="text-secondary">{t("No broker metadata is currently observable.")}</p> : <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">{t("ID")}</th><th scope="col">{t("Host")}</th><th scope="col">{t("Port")}</th><th scope="col">{t("Role")}</th><th scope="col">{t("Configuration")}</th></tr></thead><tbody>{selected.data.brokers.map(broker => <tr key={broker.brokerId}><th scope="row"><bdi dir="ltr" lang="en">{broker.brokerId}</bdi></th><td><bdi dir="auto">{broker.host}</bdi></td><td>{broker.port}</td><td>{broker.isController ? <span className="badge bg-orange-lt">{t("Controller")}</span> : <span className="badge bg-secondary-lt">{t("Broker")}</span>}</td><td><button className="btn btn-sm btn-outline-primary" type="button" onClick={() => void openBroker(broker.brokerId)}>{t("View configuration")}</button></td></tr>)}</tbody></table></div>}
              {brokerError && <div className="alert alert-danger" role="alert">{problemPresentation(brokerError)}</div>}
              {brokerConfiguration && <article aria-labelledby="broker-config-title"><h3 id="broker-config-title" className="h3">{t("Broker")}{' '}<bdi dir="ltr" lang="en">{brokerConfiguration.brokerId}</bdi> {' '}{t("configuration")}</h3><ConfigurationView entries={brokerConfiguration.entries} /></article>}
            </section>}
            {activePage === 'topics' && <section className="card kafdeck-card" id="topics" aria-labelledby="topics-title">
              <h2 id="topics-title" className="card-title">{t("Topics")}</h2>
              {!activeTopic && <>
              <div className="px-3 mb-3"><label className="form-label" htmlFor="topic-search">{t("Search topics")}</label><input dir="ltr" className="form-control" id="topic-search" type="search" value={topicQuery} onChange={event => setTopicQuery(event.target.value)} placeholder={t("Filter by topic name")} /></div>
              {topicsLoading && topics.length === 0 && <div className="alert alert-info mx-3" role="status">{t("Loading topics…")}</div>}
              {topicsError && <div className="alert alert-danger mx-3" role="alert">{problemPresentation(topicsError)}</div>}
              {!topicsLoading && !topicsError && topics.length === 0 && <p className="text-secondary">{t("No topics match the current search.")}</p>}
              {topics.length > 0 && <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">{t("Topic")}</th><th scope="col">{t("Partitions")}</th><th scope="col">{t("Offline")}</th><th scope="col">{t("Under replicated")}</th><th scope="col">{t("State")}</th></tr></thead><tbody>{topics.map(topic => <tr key={topic.name}><th scope="row"><button className="btn btn-link kafdeck-table-button" type="button" onClick={() => void openTopic(topic.name)}><bdi dir="auto">{topic.name}</bdi></button></th><td><bdi dir="ltr" lang="en">{topic.partitionCount}</bdi></td><td>{topic.offlinePartitionCount ?? t("Unknown")}</td><td>{topic.underReplicatedPartitionCount ?? t("Unknown")}</td><td><span className="badge bg-blue-lt"><bdi dir="ltr" lang="en">{String(topic.anomalyState)}</bdi></span></td></tr>)}</tbody></table></div>}
              {topicCursor && <button className="btn btn-sm btn-outline-primary mt-3" type="button" disabled={topicsLoading} onClick={() => void loadTopics(selectedClusterId, topicQuery, topicCursor, true)}>{topicsLoading ? t("Loading…") : t("Load more topics")}</button>}
              </>}
              {activeTopic && <article className="px-3 pb-3" aria-labelledby="topic-detail-title">
                <button className="btn btn-sm btn-outline-primary mb-3" type="button" onClick={closeTopic}>{t("Back to topics")}</button>
                <h3 id="topic-detail-title" className="h3" ref={topicHeading} tabIndex={-1}>{t("Topic:")}{' '}<bdi dir="auto">{activeTopic}</bdi></h3>
                <div className="d-flex flex-wrap gap-2 mb-3" role="group" aria-label={t("Topic views")}>
                  {([
                    ['summary', t("Overview")], ['configuration', t("Configuration")],
                    ...(canUseTopicTools ? [['messages', t("Messages")]] as const : []),
                    ...(canUseTopicTools && operator ? [['publish', t("Publish message")], ['manage', t("Manage topic")], ['purge', t("Controlled purge")]] as const : []),
                  ] as const).map(([view, label]) =>
                    <button key={view} className={`btn btn-sm ${topicView === view ? 'btn-primary' : 'btn-outline-primary'}`}
                      type="button" aria-pressed={topicView === view} onClick={() => setTopicView(view)}>{label}</button>)}
                </div>
                {topicView === 'summary' && <>
                  {topicDetailError && <div className="alert alert-danger" role="alert">{problemPresentation(topicDetailError)}</div>}
                  {!topicDetail && !topicDetailError && <p role="status">{t("Loading topic details…")}</p>}
                  {topicDetail && <><p className="kafdeck-observation">{observationText(topicDetail)} · <bdi dir="ltr" lang="en">{topicDetail.data.partitions.length}</bdi> {' '}{t("partitions ·")}{' '}<bdi dir="ltr" lang="en">{topicDetail.data.offlinePartitionCount}</bdi> {' '}{t("offline ·")}{' '}<bdi dir="ltr" lang="en">{topicDetail.data.underReplicatedPartitionCount}</bdi> {' '}{t("under replicated")}</p>{topicCatalog && <aside aria-label={t("Topic catalog metadata")}><h4>{t("Catalog metadata")}</h4><dl><dt>{t("Description")}</dt><dd>{topicCatalog.data.description ?? t("Not set")}</dd><dt>{t("Owner")}</dt><dd>{topicCatalog.data.owner ?? t("Not set")}</dd><dt>{t("Domain")}</dt><dd>{topicCatalog.data.domain ?? t("Not set")}</dd><dt>{t("Classification")}</dt><dd>{topicCatalog.data.classification ?? t("Not set")}</dd><dt>{t("Tags")}</dt><dd>{topicCatalog.data.tags.join(', ') || t("None")}</dd><dt>{t("Documentation reference")}</dt><dd>{topicCatalog.data.documentationReference ?? t("Not set")}</dd></dl></aside>}<h4>{t("Partitions")}</h4>{topicDetail.data.partitions.length === 0 ? <p>{t("No partitions are observable.")}</p> : <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">{t("ID")}</th><th scope="col">{t("Leader")}</th><th scope="col">{t("Replicas")}</th><th scope="col">ISR</th><th scope="col">{t("Out of sync")}</th><th scope="col">{t("Health")}</th></tr></thead><tbody>{topicDetail.data.partitions.map(partition => <tr key={partition.partitionId}><th scope="row"><bdi dir="ltr" lang="en">{partition.partitionId}</bdi></th><td>{partition.leaderBrokerId ?? t("No leader")}</td><td>{partition.replicaBrokerIds.join(', ')}</td><td>{partition.inSyncReplicaBrokerIds.join(', ')}</td><td>{partition.outOfSyncReplicaBrokerIds.join(', ') || t("None")}</td><td>{partition.healthReasons.length > 0 ? partition.healthReasons.join('; ') : String(partition.health)}</td></tr>)}</tbody></table></div>}</>}
                </>}
                {topicView === 'configuration' && <section aria-label={t("Configuration")}>
                  {topicConfigurationError && <div className="alert alert-danger" role="alert">{problemPresentation(topicConfigurationError)}</div>}
                  {topicConfiguration !== null ? <ConfigurationView entries={topicConfiguration} /> :
                    !topicConfigurationError && <p role="status">{t("Loading configuration…")}</p>}
                </section>}
                {canUseTopicTools && topicView === 'messages' && (topicDetail?.data.partitions.length
                  ? <RecordExplorer key={`records:${selectedClusterId}:${activeTopic}`} clusterId={selectedClusterId}
                      topicName={activeTopic} partitions={topicDetail.data.partitions.map(partition => partition.partitionId)} />
                  : topicDetailError ? <div className="alert alert-danger" role="alert">{problemPresentation(topicDetailError)}</div>
                    : <p role="status">{topicDetail ? t("No partitions are observable.") : t("Loading topic details…")}</p>)}
                {canUseTopicTools && operator && topicView === 'publish' && <TopicRecordPublisher key={`publish:${selectedClusterId}:${activeTopic}`} clusterId={selectedClusterId} topicName={activeTopic} />}
                {canUseTopicTools && operator && (topicView === 'manage' || topicView === 'purge') &&
                  <MutationOperationsPanel key={`mutations:${selectedClusterId}:${activeTopic}:${topicView}`} clusterId={selectedClusterId}
                    family={topicView === 'manage' ? 'topic' : 'purge'} selectedTopic={activeTopic} />}
              </article>}
            </section>}
            {(activePage === 'consumers' || activePage === 'schemas' || activePage === 'ecosystem') &&
              (activePage === 'ecosystem' && operator?.recentConnectionScope && selectedClusterId.startsWith('connection-')
                ? <><RegisteredKsqlView key={`registered-ksql:${operatorScope}:${selectedClusterId}`}
                  clusterId={selectedClusterId} canConfigure={operator.canManageUsers === true} />
                <RegisteredConnectView key={`registered-connect:${operatorScope}:${selectedClusterId}`}
                  clusterId={selectedClusterId} canConfigure={operator.canManageUsers === true}
                  canManage={operator.canManageUsers === true || operator.managedClusterManager === true} /></>
                : <ReadViewsExplorer key={`read-views:${operatorScope ?? "read-only"}:${selectedClusterId}:${activePage}`}
                clusterId={selectedClusterId} page={activePage} consumerRefresh={consumerRefresh}
                canRegisterSchemas={!!operator && (!operator.recentConnectionScope || operator.canManageUsers === true || operator.managedClusterManager === true)}
                canConfigureRegistry={operator?.canManageUsers === true && selectedClusterId.startsWith('connection-')}
                canManageConsumers={!!operator && (!selectedDirectoryEntry.displayName || operator.canManageUsers === true || operator.managedClusterManager === true)} />)}
            {operator && activePage === 'topics' && !activeTopic &&
              (!selectedDirectoryEntry.displayName || operator.canManageUsers || operator.managedClusterManager) &&
              <MutationOperationsPanel key={`mutations:${selectedClusterId}:topics`} clusterId={selectedClusterId} family="topic" />}
          </>}
        </div>
      </div>
    </main>
    <img className="kafdeck-logo-watermark" src="/kafdeck-logo.webp" alt="" aria-hidden="true" />
  </div></>;
}
