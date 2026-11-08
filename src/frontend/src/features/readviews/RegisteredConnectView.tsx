import { useEffect, useRef, useState } from 'react';
import { ApiProblem, kafdeckApi, type ConnectClusterInfo, type ConnectConnectorDetail, type ConnectConnectorSummary } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { RegisteredServiceSettings } from './RegisteredServiceSettings.js';
import { RegisteredConnectManagement } from './RegisteredConnectManagement.js';
import { readViewError, readViewProblemText } from './readViewProblem.js';

export function RegisteredConnectView({ clusterId, canConfigure, canManage }: {
  clusterId: string; canConfigure: boolean; canManage: boolean;
}) {
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [connectors, setConnectors] = useState<ConnectConnectorSummary[] | null>(null);
  const [info, setInfo] = useState<ConnectClusterInfo | null>(null);
  const [missing, setMissing] = useState(false);
  const [listError, setListError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<ConnectConnectorDetail | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const settingsButton = useRef<HTMLButtonElement>(null);
  const connectorHeading = useRef<HTMLHeadingElement>(null);

  useEffect(() => {
    const controller = new AbortController();
    setListError(null);
    void kafdeckApi.listConnectors(clusterId, controller.signal).then(result => {
      if (controller.signal.aborted) return;
      setMissing(false);
      setConnectors(result.data);
    }).catch(reason => {
      if (controller.signal.aborted) return;
      if (reason instanceof ApiProblem && reason.code === 'connect_not_configured') {
        setMissing(true);
        setConnectors(null);
        setInfo(null);
      } else setListError(readViewProblemText(readViewError(reason)));
    });
    void kafdeckApi.getConnectInfo(clusterId, controller.signal).then(result => {
      if (!controller.signal.aborted) setInfo(result.data);
    }).catch(() => {});
    return () => controller.abort();
  }, [clusterId, refresh]);

  useEffect(() => {
    if (selected) connectorHeading.current?.focus();
  }, [selected]);

  useEffect(() => {
    if (selected === null) return;
    const controller = new AbortController();
    setDetailError(null);
    void kafdeckApi.getConnector(clusterId, selected, controller.signal).then(result => {
      if (!controller.signal.aborted) setDetail(result.data);
    }).catch(reason => {
      if (!controller.signal.aborted) setDetailError(readViewProblemText(readViewError(reason)));
    });
    return () => controller.abort();
  }, [clusterId, selected, refresh]);

  function saved() {
    setMissing(false);
    setInfo(null);
    setConnectors(null);
    setRefresh(value => value + 1);
  }

  function back() {
    const name = selected;
    setSelected(null);
    setDetail(null);
    window.requestAnimationFrame(() => {
      if (name !== null) document.getElementById(`connect-connector-${encodeURIComponent(name)}`)?.focus();
    });
  }

  return <section className="card kafdeck-card kafdeck-ecosystem-card" id="ecosystem" aria-labelledby="connect-title">
    <div className="kafdeck-ecosystem-body">
      <header className="kafdeck-ecosystem-header">
        <h2 id="connect-title">Kafka Connect</h2>
        <div className="kafdeck-ecosystem-actions">
          {selected !== null && <button type="button" className="btn btn-sm btn-outline-primary" onClick={back}>{t("Back to connectors")}</button>}
          {canConfigure && selected === null && !settingsOpen &&
            <button ref={settingsButton} type="button" className="btn btn-outline-primary" onClick={() => setSettingsOpen(true)}>
              {t("Configure Kafka Connect")}
            </button>}
          {!missing && <button type="button" className="btn btn-sm btn-outline-secondary" onClick={() => setRefresh(value => value + 1)}>
            {selected === null ? t("Refresh connectors") : t("Refresh connector status")}
          </button>}
        </div>
      </header>
      {canConfigure && settingsOpen && selected === null && <RegisteredServiceSettings
        clusterId={clusterId} service="kafka-connect" onSaved={saved}
        onClose={() => {
          setSettingsOpen(false);
          window.requestAnimationFrame(() => settingsButton.current?.focus());
        }} />}
      {notice && <p role="status">{notice}</p>}
      {selected === null && <>
        {info && <p>{t("Version:")}{' '}{info.version ?? t("Unknown")} {' '}{t("· Kafka cluster:")}{' '}
          <bdi dir="ltr">{info.kafkaClusterId ?? t("Unknown")}</bdi>
        </p>}
        {listError && <p role="alert">{listError}</p>}
        {missing && <p role="status">{t("This cluster has no Kafka Connect endpoint configured.")}</p>}
        {!missing && !listError && !connectors && <p role="status">{t("Loading connectors…")}</p>}
        {canManage && !settingsOpen && connectors && <RegisteredConnectManagement key="create"
          clusterId={clusterId} selected={null} onExecuted={(_, name, operation) => {
            setRefresh(value => value + 1);
            if (operation.state === 'appliedVerified') {
              setNotice(t("Kafka Connect change verified."));
              setSelected(name);
            }
          }} />}
        {connectors && (connectors.length === 0 ? <p>{t("No authorized connectors are observable in this profile.")}</p> :
          <ul className="list-group list-group-flush" aria-label={t("Connectors")}>
            {connectors.map(connector => <li className="list-group-item" key={connector.name}>
              <button id={`connect-connector-${encodeURIComponent(connector.name)}`} className="btn btn-link kafdeck-table-button" type="button"
                onClick={() => { setNotice(null); setDetail(null); setDetailError(null); setSelected(connector.name); }}>
                <bdi dir="auto">{connector.name}</bdi>
              </button>
            </li>)}
          </ul>)}
      </>}
      {selected !== null && <article aria-labelledby="managed-connector-title">
        <h3 id="managed-connector-title" ref={connectorHeading} tabIndex={-1}><bdi dir="auto">{selected}</bdi></h3>
        {detailError && <p role="alert">{detailError}</p>}
        {!detail && !detailError && <p role="status">{t("Loading connector details…")}</p>}
        {detail?.name === selected && <>
          <p>{t("State:")}{' '}<strong><bdi>{detail.state}</bdi></strong> {' '}{t("· Worker:")}{' '}<bdi dir="ltr">{detail.workerId ?? t("Unknown")}</bdi></p>
          {canManage && <RegisteredConnectManagement key={selected} clusterId={clusterId} selected={selected}
            state={detail.state} onExecuted={(action, _, operation) => {
              setNotice(null);
              setRefresh(value => value + 1);
              if (action === 'delete' && operation.state === 'appliedVerified') {
                setNotice(t("Kafka Connect change verified."));
                back();
              }
            }} />}
          <h4 id="managed-connect-tasks">{t("Connector tasks")}</h4>
          {detail.tasks.length === 0 ? <p>{t("This connector has no reported tasks.")}</p> :
            <div className="table-responsive"><table className="table table-vcenter" aria-labelledby="managed-connect-tasks">
              <thead><tr><th scope="col">{t("Task ID")}</th><th scope="col">{t("Status")}</th><th scope="col">{t("Worker")}</th></tr></thead>
              <tbody>{detail.tasks.map(task => <tr key={task.id}>
                <th scope="row">{task.id}</th><td><bdi>{task.state}</bdi></td><td><bdi dir="ltr">{task.workerId ?? t("Unknown")}</bdi></td>
              </tr>)}</tbody>
            </table></div>}
          <h4 id="managed-connect-configuration">{t("Connector configuration")}</h4>
          <div className="table-responsive"><table className="table table-vcenter" aria-labelledby="managed-connect-configuration">
            <thead><tr><th scope="col">{t("Configuration key")}</th><th scope="col">{t("Safe value")}</th></tr></thead>
            <tbody>{Object.entries(detail.safeConfiguration).map(([key, value]) => <tr key={key}>
              <th scope="row"><bdi dir="ltr">{key}</bdi></th><td><bdi dir="auto" className="text-break">{value ?? t("Not set")}</bdi></td>
            </tr>)}</tbody>
          </table></div>
        </>}
      </article>}
    </div>
  </section>;
}
