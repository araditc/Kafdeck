import { applicationProblem, ownedErrorPresentation, problemPresentation, type PresentationProblem } from '../shared/presentationProblem.js';
import { t } from '../shared/i18n.js';
import { useLocale } from '../shared/i18n.js';
import { LanguageSelector } from './LanguageSelector.js';
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import {
  ApiProblem,
  kafdeckApi,
  operatorSessionLostEvent,
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
import { ReadViewsExplorer } from '../features/readviews/ReadViewsExplorer.js';
import { NotificationObservationPanel } from '../features/notifications/NotificationObservationPanel.js';
import { NotificationSubscriptionManagementPanel } from '../features/notifications/NotificationSubscriptionManagementPanel.js';
import { NotificationDestinationManagementPanel } from '../features/notifications/NotificationDestinationManagementPanel.js';
import { productDescription, productName } from '../shared/product.js';
import { describeObservation, shouldAutoRefresh, visibleRefreshIntervalMs } from './operatorState.js';
import { CommandPalette } from './CommandPalette.js';
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
  const verifiedSession = useRef<{ scope: string | null; key: string; operator: OperatorSession } | null>(null);
  const legacySessionEpoch = useRef(0);
  const verificationEpoch = useRef(0);
  const verificationFocus = useRef<{ element: HTMLElement; scope: string; epoch: number; ready: boolean } | null>(null);
  const [authenticationRequired, setAuthenticationRequired] = useState(false);
  const [clusters, setClusters] = useState<ApiEnvelope<ClusterData>[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState<string>('');
  const [selected, setSelected] = useState<ApiEnvelope<ClusterData> | null>(null);
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
  const [topicConfiguration, setTopicConfiguration] = useState<ConfigurationEntryData[] | null>(null);
  const [topicDetailError, setTopicDetailError] = useState<PresentationProblem | null>(null);
  const [topicCatalog, setTopicCatalog] = useState<ReadViewEnvelope<TopicCatalogEntry> | null>(null);
  const [fleetCapabilities, setFleetCapabilities] = useState<FleetCapabilityStatus[]>([]);
  const [fleetError, setFleetError] = useState<PresentationProblem | null>(null);
  const [notificationView, setNotificationView] = useState<{ sessionKey: string; page: NotificationSubscriptionListData } | null>(null);
  const [notificationManagement, setNotificationManagement] = useState<{ sessionKey: string; destinationGovernanceAvailable: boolean } | null>(null);
  const sessionCheckAbort = useRef<AbortController | null>(null);

  const clearObservations = useCallback(() => {
    setClusters([]); setSelectedClusterId(''); setSelected(null); setError(null);
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
    setNotificationView(null); setNotificationManagement(null);
    setVerificationPending(false); setAuthenticationRequired(true);
  }, [clearObservations]);

  const revalidateOperatorSession = useCallback(() => {
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
    void kafdeckApi.getOperatorSession(controller.signal)
      .then(session => {
        if (controller.signal.aborted || sessionCheckAbort.current !== controller) return;
        if (session.authenticated !== true || session.authenticationMode !== 'oidc') throw new Error('Invalid operator session.');
        const scope = authoritativeSessionScope(session);
        const sameScope = previous !== null && canPreserveOperatorSession(previous.scope, session);
        const key = sameScope && previous ? previous.key : scope !== null ? `server:${scope}` : `legacy:${++legacySessionEpoch.current}`;
        if (!sameScope) {
          if (previous) clearObservations();
          setNotificationView(null); setNotificationManagement(null);
          verificationFocus.current = null;
        } else if (verificationFocus.current?.epoch === epoch) {
          verificationFocus.current.ready = true;
        }
        verifiedSession.current = { scope, key, operator: session };
        setOperatorScope(key); setOperator(session);
        setAuthenticationRequired(false); setVerificationPending(false);
      })
      .catch(reason => {
        if (controller.signal.aborted || sessionCheckAbort.current !== controller) return;
        if (verifiedSession.current) clearObservations();
        verifiedSession.current = null; verificationFocus.current = null;
        setOperator(null); setOperatorScope(null);
        setNotificationView(null); setNotificationManagement(null);
        setVerificationPending(false);
        if (reason instanceof ApiProblem && (reason.status === 401 || reason.status === 403)) setAuthenticationRequired(true);
      })
      .finally(() => {
        if (sessionCheckAbort.current === controller) sessionCheckAbort.current = null;
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
      if (document.visibilityState === 'visible') revalidateOperatorSession();
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

  const loadClusters = useCallback(async (signal?: AbortSignal) => {
    const observationScope = verifiedSession.current?.key ?? null;
    setLoading(true);
    setError(null);
    try {
      const response = await kafdeckApi.listClusters(signal);
      if (signal?.aborted || (verifiedSession.current?.key ?? null) !== observationScope) return;
      setClusters(response.data);
      const nextId = selectedClusterId || response.data[0]?.data.clusterId || '';
      setSelectedClusterId(nextId);
      setSelected(response.data.find(item => item.data.clusterId === nextId) ?? null);
    } catch (reason) {
      if ((verifiedSession.current?.key ?? null) !== observationScope) return;
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      if (reason instanceof ApiProblem && reason.status === 401) setAuthenticationRequired(true);
      setError(problemText(reason));
    } finally {
      if (!signal?.aborted && (verifiedSession.current?.key ?? null) === observationScope) setLoading(false);
    }
  }, [selectedClusterId, operatorScope]);

  const loadTopics = useCallback(async (clusterId: string, query: string, cursor: string | null, append = false, signal?: AbortSignal) => {
    if (!clusterId) return;
    const observationScope = verifiedSession.current?.key ?? null;
    setTopicsLoading(true);
    setTopicsError(null);
    try {
      const response = await kafdeckApi.listTopics(clusterId, query, cursor, signal);
      if (signal?.aborted || (verifiedSession.current?.key ?? null) !== observationScope) return;
      setTopics(previous => append ? [...previous, ...response.data.items] : response.data.items);
      setTopicCursor(response.data.nextCursor);
    } catch (reason) {
      if ((verifiedSession.current?.key ?? null) !== observationScope) return;
      if (reason instanceof DOMException && reason.name === 'AbortError') return;
      setTopicsError(problemText(reason));
    } finally {
      if (!signal?.aborted && (verifiedSession.current?.key ?? null) === observationScope) setTopicsLoading(false);
    }
  }, [operatorScope]);

  useEffect(() => {
    const controller = new AbortController();
    const observationScope = verifiedSession.current?.key ?? null;
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
  }, [loadClusters]);

  // Expose observation only to an OIDC operator after a successful
  // server-authorized NotificationRead collection preflight.
  useEffect(() => {
    setNotificationView(null);
    setNotificationManagement(null);
    if (!operator || operator.authenticationMode !== 'oidc') return;
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
  }, [operatorScope]);

  const notificationSessionKey = operator ? operatorScope : null;
  const visibleNotificationPage = notificationView?.sessionKey === notificationSessionKey
    ? notificationView?.page ?? null
    : null;
  const managementSurfaces = notificationManagementSurfaces(
    visibleNotificationPage !== null,
    notificationManagement?.sessionKey === notificationSessionKey,
  );

  useEffect(() => {
    if (!selectedClusterId) return;
    const controller = new AbortController();
    void loadTopics(selectedClusterId, topicQuery, null, false, controller.signal);
    return () => controller.abort();
  }, [loadTopics, selectedClusterId, topicQuery]);

  useEffect(() => {
    if (!selectedClusterId) return;
    let controller: AbortController | null = null;
    const refresh = () => {
      if (!shouldAutoRefresh(document.visibilityState)) return;
      controller?.abort();
      controller = new AbortController();
      void loadClusters(controller.signal);
      void loadTopics(selectedClusterId, topicQuery, null, false, controller.signal);
    };
    const timer = window.setInterval(refresh, visibleRefreshIntervalMs);
    document.addEventListener('visibilitychange', refresh);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', refresh);
      controller?.abort();
    };
  }, [loadClusters, loadTopics, selectedClusterId, topicQuery]);

  const selectCluster = (clusterId: string) => {
    setSelectedClusterId(clusterId);
    setSelected(clusters.find(item => item.data.clusterId === clusterId) ?? null);
    setError(null);
    setBrokerConfiguration(null);
    setBrokerError(null);
    setTopicQuery('');
    setTopics([]);
    setTopicCursor(null);
    setTopicDetail(null);
    setTopicConfiguration(null);
    setTopicDetailError(null);
    setTopicCatalog(null);
  };

  const openBroker = async (brokerId: number) => {
    const observationScope = verifiedSession.current?.key ?? null;
    setBrokerConfiguration(null);
    setBrokerError(null);
    try {
      const configuration = await kafdeckApi.getBrokerConfiguration(selectedClusterId, brokerId);
      if ((verifiedSession.current?.key ?? null) !== observationScope) return;
      setBrokerConfiguration({ brokerId, entries: configuration.data });
    } catch (reason) {
      if ((verifiedSession.current?.key ?? null) === observationScope) setBrokerError(problemText(reason));
    }
  };

  const openTopic = async (topicName: string) => {
    const observationScope = verifiedSession.current?.key ?? null;
    const current = () => (verifiedSession.current?.key ?? null) === observationScope;
    setTopicDetail(null);
    setTopicConfiguration(null);
    setTopicDetailError(null);
    setTopicCatalog(null);
    try {
      const detail = await kafdeckApi.getTopic(selectedClusterId, topicName);
      if (!current()) return;
      setTopicDetail(detail);
      try {
        const catalog = await kafdeckApi.getTopicCatalog(selectedClusterId, topicName);
        if (!current()) return;
        setTopicCatalog(catalog);
      } catch {
        // Catalog metadata is optional and separately authorized.
        if (!current()) return;
        setTopicCatalog(null);
      }
      try {
        const configuration = await kafdeckApi.getTopicConfiguration(selectedClusterId, topicName);
        if (!current()) return;
        setTopicConfiguration(configuration.data);
      } catch (reason) {
        if (current()) setTopicDetailError(problemText(reason));
      }
    } catch (reason) {
      if (current()) setTopicDetailError(problemText(reason));
    }
  };

  return <>
    {verificationPending && <div className="container-xl py-3" role="status" aria-live="polite">
      <p>{t("Checking operator session…")}</p>
      <p>{t("Authorized content is hidden until verification completes.")}</p>
    </div>}
    <div ref={privatePage} className="page kafdeck-shell" hidden={verificationPending} inert={verificationPending} aria-hidden={verificationPending || undefined} style={{display: verificationPending ? 'none' : undefined}}>
    <a className="kafdeck-skip-link" href="#kafdeck-main">{t("Skip to main content")}</a>
    <header className="navbar navbar-expand-md bg-surface d-print-none kafdeck-topbar">
      <div className="container-xl">
        <h1 id="kafdeck-title" className="navbar-brand navbar-brand-autodark m-0">
          <a className="kafdeck-brand" href="#overview">
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
          <CommandPalette />
          {operator && <div className="kafdeck-auth"><span className="text-secondary">{t("Signed in as")}</span><strong>{operator.displayName ?? operator.email ?? t("operator")}</strong><button className="btn btn-sm btn-outline-secondary" type="button" onClick={() => { invalidateOperatorSession(); void kafdeckApi.logout(); }}>{t("Sign out")}</button></div>}
          {authenticationRequired && <div className="kafdeck-auth"><a className="btn btn-sm btn-primary" href="/api/v1/auth/login">{t("Sign in with your identity provider")}</a></div>}
          <div>
            <label className="form-label" htmlFor="cluster-selector">{t("Cluster")}</label>
            <select className="form-select form-select-sm" id="cluster-selector" value={selectedClusterId} onChange={event => selectCluster(event.target.value)} disabled={loading || clusters.length === 0}>{clusters.map(cluster => <option key={cluster.data.clusterId} value={cluster.data.clusterId}>{cluster.data.clusterId}</option>)}</select>
          </div>
          <button className="btn btn-sm btn-outline-primary" type="button" onClick={() => void loadClusters()} disabled={loading}>{t("Refresh")}</button>
        </div>
      </div>
    </header>
    <nav className="kafdeck-section-nav" aria-label={t("Kafdeck sections")}>
      <div className="container-xl">
        <ul className="nav nav-pills py-2">
          <li className="nav-item"><a className="nav-link" href="#overview">{t("Overview")}</a></li>
          <li className="nav-item"><a className="nav-link" href="#brokers">{t("Brokers")}</a></li>
          <li className="nav-item"><a className="nav-link" href="#topics">{t("Topics")}</a></li>
          <li className="nav-item"><a className="nav-link" href="#consumers">{t("Consumers")}</a></li>
          <li className="nav-item"><a className="nav-link" href="#schemas">{t("Schemas")}</a></li>
          <li className="nav-item"><a className="nav-link" href="#ecosystem">{t("Ecosystem")}</a></li>
          <li className="nav-item"><a className="nav-link" href="#fleet">{t("Fleet")}</a></li>
          {operator && <li className="nav-item"><a className="nav-link" href="#automation">{t("Automation")}</a></li>}
          {operator && selectedClusterId && <><li className="nav-item"><a className="nav-link" href="#ownership-catalog">{t("Ownership catalog")}</a></li><li className="nav-item"><a className="nav-link" href="#governance-policy">{t("Governance policy")}</a></li><li className="nav-item"><a className="nav-link" href="#service-requests">{t("Requests")}</a></li><li className="nav-item"><a className="nav-link" href="#governance-attribution">{t("Measured attribution")}</a></li></>}
          {managementSurfaces.navigation && <li className="nav-item"><a className="nav-link" href="#notifications">{t("Notifications")}</a></li>}
        </ul>
      </div>
    </nav>
    <main id="kafdeck-main" tabIndex={-1} className="page-wrapper" aria-labelledby="kafdeck-title">
      <div className="page-body kafdeck-main">
        <div className="container-xl">
          <div className="page-header d-print-none kafdeck-page-header">
            <div className="row g-2 align-items-center">
              <div className="col">
                <div className="page-pretitle">{t("Kafka operations")}</div>
                <h2 className="page-title">{t("Cluster Explorer")}</h2>
              </div>
            </div>
          </div>
          {loading && <div className="alert alert-info kafdeck-status" role="status">{t("Loading cluster observations…")}</div>}
          {operator && <AutomationPanel key={`automation:${operatorScope}`} notificationsAvailable={managementSurfaces.navigation} />}
          {operator && selectedClusterId && <>
            <CatalogPanel key={`catalog:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />
            <PolicyPanel key={`policy:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />
            <RequestPanel key={`requests:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />
            <AttributionPanel key={`attribution:${operatorScope}:${selectedClusterId}`} clusterId={selectedClusterId} />
          </>}
          {error && <div className="alert alert-danger kafdeck-status" role="alert">{problemPresentation(error)}</div>}
          {!loading && !error && clusters.length === 0 && <div className="alert alert-warning kafdeck-status" role="status">{t("No authorized clusters are available.")}</div>}
          <section className="card kafdeck-card" id="fleet" aria-labelledby="fleet-title">
            <h2 id="fleet-title" className="card-title">{t("Fleet Operations")}</h2>
            <p className="text-secondary">{t("Capability-driven status from the active Kafdeck runtime. Unsupported or blocked operations remain fail-closed rather than being simulated through alternate provider paths.")}</p>
            {fleetError && <div className="alert alert-danger" role="alert">{problemPresentation(fleetError)}</div>}
            {!fleetError && fleetCapabilities.length === 0 && <p role="status">{t("Loading fleet capability status…")}</p>}
            {fleetCapabilities.length > 0 && <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">{t("Capability")}</th><th scope="col">{t("State")}</th><th scope="col">{t("Workstream")}</th><th scope="col">{t("Reason")}</th></tr></thead><tbody>{fleetCapabilities.map(capability => <tr key={capability.id}><th scope="row"><bdi dir="auto">{capability.displayName}</bdi></th><td><StatusBadge kind={fleetCapabilityStatusKind(capability.state)} label={capability.state} /></td><td>{capability.workstream}</td><td><bdi dir="auto">{capability.reason}</bdi></td></tr>)}</tbody></table></div>}
          </section>
          {visibleNotificationPage && <NotificationObservationPanel key={notificationSessionKey ?? undefined}
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
          </section>}
          {selected && <>
            <section className="card kafdeck-card" id="overview" aria-labelledby="overview-title">
              <h2 id="overview-title" className="card-title">{t("Cluster overview")}</h2>
              <p className="kafdeck-observation"><strong><bdi dir="ltr" lang="en">{selected.data.clusterId}</bdi></strong> · {observationText(selected)}</p>
              <dl><dt>{t("Kafka cluster ID")}</dt><dd>{selected.data.kafkaClusterId ?? t("Not observed")}</dd><dt>{t("Health")}</dt><dd><span className="badge bg-blue-lt"><bdi dir="ltr" lang="en">{String(selected.data.health)}</bdi></span></dd><dt>{t("Controller")}</dt><dd>{selected.data.controllerBrokerId ?? t("Not observed")}</dd><dt>{t("Brokers")}</dt><dd><bdi dir="ltr" lang="en">{selected.data.brokers.length}</bdi></dd></dl>
              {selected.limitations.length > 0 && <aside aria-label={t("Cluster limitations")}><h3 className="h4">{t("Limitations")}</h3><ul className="mb-0">{selected.limitations.map((item, index) => <li key={`${item.capability}-${index}`}>{item.capability ?? t("cluster")}: {item.state}{item.reason ? ` — ${item.reason}` : ''}</li>)}</ul></aside>}
            </section>
            <section className="card kafdeck-card" id="brokers" aria-labelledby="brokers-title">
              <h2 id="brokers-title" className="card-title">{t("Brokers")}</h2>
              {selected.data.brokers.length === 0 ? <p className="text-secondary">{t("No broker metadata is currently observable.")}</p> : <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">{t("ID")}</th><th scope="col">{t("Host")}</th><th scope="col">{t("Port")}</th><th scope="col">{t("Role")}</th><th scope="col">{t("Configuration")}</th></tr></thead><tbody>{selected.data.brokers.map(broker => <tr key={broker.brokerId}><th scope="row"><bdi dir="ltr" lang="en">{broker.brokerId}</bdi></th><td><bdi dir="auto">{broker.host}</bdi></td><td>{broker.port}</td><td>{broker.isController ? <span className="badge bg-orange-lt">{t("Controller")}</span> : <span className="badge bg-secondary-lt">{t("Broker")}</span>}</td><td><button className="btn btn-sm btn-outline-primary" type="button" onClick={() => void openBroker(broker.brokerId)}>{t("View configuration")}</button></td></tr>)}</tbody></table></div>}
              {brokerError && <div className="alert alert-danger" role="alert">{problemPresentation(brokerError)}</div>}
              {brokerConfiguration && <article aria-labelledby="broker-config-title"><h3 id="broker-config-title" className="h3">{t("Broker")}{' '}<bdi dir="ltr" lang="en">{brokerConfiguration.brokerId}</bdi> {' '}{t("configuration")}</h3><ConfigurationView entries={brokerConfiguration.entries} /></article>}
            </section>
            <section className="card kafdeck-card" id="topics" aria-labelledby="topics-title">
              <h2 id="topics-title" className="card-title">{t("Topics")}</h2>
              <div className="px-3 mb-3"><label className="form-label" htmlFor="topic-search">{t("Search topics")}</label><input dir="ltr" className="form-control" id="topic-search" type="search" value={topicQuery} onChange={event => setTopicQuery(event.target.value)} placeholder={t("Filter by topic name")} /></div>
              {topicsLoading && topics.length === 0 && <div className="alert alert-info mx-3" role="status">{t("Loading topics…")}</div>}
              {topicsError && <div className="alert alert-danger mx-3" role="alert">{problemPresentation(topicsError)}</div>}
              {!topicsLoading && !topicsError && topics.length === 0 && <p className="text-secondary">{t("No topics match the current search.")}</p>}
              {topics.length > 0 && <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">{t("Topic")}</th><th scope="col">{t("Partitions")}</th><th scope="col">{t("Offline")}</th><th scope="col">{t("Under replicated")}</th><th scope="col">{t("State")}</th></tr></thead><tbody>{topics.map(topic => <tr key={topic.name}><th scope="row"><button className="btn btn-link kafdeck-table-button" type="button" onClick={() => void openTopic(topic.name)}><bdi dir="auto">{topic.name}</bdi></button></th><td><bdi dir="ltr" lang="en">{topic.partitionCount}</bdi></td><td>{topic.offlinePartitionCount ?? t("Unknown")}</td><td>{topic.underReplicatedPartitionCount ?? t("Unknown")}</td><td><span className="badge bg-blue-lt"><bdi dir="ltr" lang="en">{String(topic.anomalyState)}</bdi></span></td></tr>)}</tbody></table></div>}
              {topicCursor && <button className="btn btn-sm btn-outline-primary mt-3" type="button" disabled={topicsLoading} onClick={() => void loadTopics(selectedClusterId, topicQuery, topicCursor, true)}>{topicsLoading ? t("Loading…") : t("Load more topics")}</button>}
              {topicDetailError && <div className="alert alert-danger mx-3" role="alert">{problemPresentation(topicDetailError)}</div>}
              {topicDetail && <article aria-labelledby="topic-detail-title"><h3 id="topic-detail-title" className="h3">{t("Topic:")}{' '}<bdi dir="auto">{topicDetail.data.name}</bdi></h3><p className="kafdeck-observation">{observationText(topicDetail)} · <bdi dir="ltr" lang="en">{topicDetail.data.partitions.length}</bdi> {' '}{t("partitions ·")}{' '}<bdi dir="ltr" lang="en">{topicDetail.data.offlinePartitionCount}</bdi> {' '}{t("offline ·")}{' '}<bdi dir="ltr" lang="en">{topicDetail.data.underReplicatedPartitionCount}</bdi> {' '}{t("under replicated")}</p>{topicCatalog && <aside aria-label={t("Topic catalog metadata")}><h4>{t("Catalog metadata")}</h4><dl><dt>{t("Description")}</dt><dd>{topicCatalog.data.description ?? t("Not set")}</dd><dt>{t("Owner")}</dt><dd>{topicCatalog.data.owner ?? t("Not set")}</dd><dt>{t("Domain")}</dt><dd>{topicCatalog.data.domain ?? t("Not set")}</dd><dt>{t("Classification")}</dt><dd>{topicCatalog.data.classification ?? t("Not set")}</dd><dt>{t("Tags")}</dt><dd>{topicCatalog.data.tags.join(', ') || t("None")}</dd><dt>{t("Documentation reference")}</dt><dd>{topicCatalog.data.documentationReference ?? t("Not set")}</dd></dl></aside>}<h4>{t("Partitions")}</h4>{topicDetail.data.partitions.length === 0 ? <p>{t("No partitions are observable.")}</p> : <div className="table-responsive" tabIndex={0} role="region" aria-label={t("Scrollable data table")}><table className="table table-vcenter card-table mb-0"><thead><tr><th scope="col">{t("ID")}</th><th scope="col">{t("Leader")}</th><th scope="col">{t("Replicas")}</th><th scope="col">ISR</th><th scope="col">{t("Out of sync")}</th><th scope="col">{t("Health")}</th></tr></thead><tbody>{topicDetail.data.partitions.map(partition => <tr key={partition.partitionId}><th scope="row"><bdi dir="ltr" lang="en">{partition.partitionId}</bdi></th><td>{partition.leaderBrokerId ?? t("No leader")}</td><td>{partition.replicaBrokerIds.join(', ')}</td><td>{partition.inSyncReplicaBrokerIds.join(', ')}</td><td>{partition.outOfSyncReplicaBrokerIds.join(', ') || t("None")}</td><td>{partition.healthReasons.length > 0 ? partition.healthReasons.join('; ') : String(partition.health)}</td></tr>)}</tbody></table></div>}<h4>{t("Configuration")}</h4>{topicConfiguration === null ? (topicDetailError ? null : <p role="status">{t("Loading configuration…")}</p>) : <ConfigurationView entries={topicConfiguration} />}{topicDetail.data.partitions.length > 0 && <RecordExplorer key={`records:${operatorScope ?? "read-only"}:${selectedClusterId}`} clusterId={selectedClusterId} topicName={topicDetail.data.name} partitions={topicDetail.data.partitions.map(partition => partition.partitionId)} />}</article>}
            </section>
            <ReadViewsExplorer key={`read-views:${operatorScope ?? "read-only"}:${selectedClusterId}`} clusterId={selectedClusterId} />
          </>}
        </div>
      </div>
    </main>
    <img className="kafdeck-logo-watermark" src="/kafdeck-logo.webp" alt="" aria-hidden="true" />
  </div></>;
}
