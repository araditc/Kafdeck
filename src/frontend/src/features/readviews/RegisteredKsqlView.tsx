import { useEffect, useRef, useState } from 'react';
import { ApiProblem, kafdeckApi, type KsqlMetadataItem, type KsqlServerInfo } from '../../shared/api.js';
import { t } from '../../shared/i18n.js';
import { KsqlQueryConsole } from './KsqlQueryConsole.js';
import { KsqlCatalogForm } from './KsqlCatalogForm.js';
import { RegisteredServiceSettings } from './RegisteredServiceSettings.js';
import { readViewError, readViewProblemText } from './readViewProblem.js';

export function RegisteredKsqlView({ clusterId, canConfigure }: { clusterId: string; canConfigure: boolean }) {
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [createKind, setCreateKind] = useState<'stream' | 'table' | null>(null);
  const [creation, setCreation] = useState<{ kind: 'stream' | 'table'; name: string; status: 'created' | 'unconfirmed' } | null>(null);
  const [refresh, setRefresh] = useState(0);
  const [revision, setRevision] = useState(0);
  const [missing, setMissing] = useState(false);
  const [info, setInfo] = useState<KsqlServerInfo | null>(null);
  const [metadata, setMetadata] = useState<KsqlMetadataItem[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const settingsButton = useRef<HTMLButtonElement>(null);
  const streamButton = useRef<HTMLButtonElement>(null);
  const tableButton = useRef<HTMLButtonElement>(null);

  function closeCreate() {
    const button = createKind === 'table' ? tableButton : streamButton;
    setCreateKind(null);
    window.requestAnimationFrame(() => button.current?.focus());
  }

  useEffect(() => {
    const controller = new AbortController();
    setError(null);
    void kafdeckApi.getKsqlInfo(clusterId, controller.signal).then(async result => {
      if (controller.signal.aborted) return;
      setMissing(false);
      setInfo(result.data);
      const catalog = await kafdeckApi.listKsqlMetadata(clusterId, controller.signal);
      if (!controller.signal.aborted) setMetadata(catalog.data);
    }).catch(reason => {
      if (controller.signal.aborted) return;
      if (reason instanceof ApiProblem && reason.code === 'ksql_not_configured') {
        setMissing(true);
        setInfo(null);
        setMetadata(null);
      } else setError(readViewProblemText(readViewError(reason)));
    });
    return () => controller.abort();
  }, [clusterId, refresh, revision]);

  return <section className="card kafdeck-card kafdeck-ecosystem-card" aria-labelledby="ksql-title">
    <div className="kafdeck-ecosystem-body">
      <header className="kafdeck-ecosystem-header">
        <div>
          <h2 id="ksql-title">ksqlDB</h2>
          {info && <p className="text-secondary">
            {t("Version:")}{' '}<bdi>{info.version ?? t("Unknown")}</bdi> {' '}{t("· Kafka cluster:")}{' '}
            <bdi dir="ltr">{info.kafkaClusterId ?? t("Unknown")}</bdi>
          </p>}
        </div>
        <div className="kafdeck-ecosystem-actions">
          {canConfigure && info && !settingsOpen && !createKind && <>
            <button type="button" ref={streamButton} className="btn btn-primary"
              onClick={() => { setCreation(null); setCreateKind('stream'); }}>{t("Create stream")}</button>
            <button type="button" ref={tableButton} className="btn btn-primary"
              onClick={() => { setCreation(null); setCreateKind('table'); }}>{t("Create table")}</button>
          </>}
          {canConfigure && !settingsOpen && !createKind && <button type="button" ref={settingsButton} className="btn btn-outline-primary"
            onClick={() => setSettingsOpen(true)}>{t("Configure ksqlDB")}</button>}
          {info && <button type="button" className="btn btn-outline-secondary" onClick={() => setRefresh(value => value + 1)}>
            {t("Refresh ksqlDB catalog")}
          </button>}
        </div>
      </header>
      {canConfigure && info && createKind && <KsqlCatalogForm clusterId={clusterId} kind={createKind} onClose={closeCreate}
        onComplete={(name, status) => {
          setCreation({ kind: createKind, name, status });
          setRefresh(value => value + 1);
          closeCreate();
        }} />}
      {creation && <p className={creation.status === 'created' ? 'alert alert-success mb-0' : 'alert alert-warning mb-0'} role="status">
        {creation.status === 'created' ? (creation.kind === 'table' ? t("Table created.") : t("Stream created."))
          : t("Creation not confirmed. Refresh the catalog before retrying.")}{' '}
        <bdi>{creation.name}</bdi>
      </p>}
      {canConfigure && settingsOpen && <RegisteredServiceSettings clusterId={clusterId} service="ksqldb"
        onSaved={() => {
          setInfo(null);
          setMetadata(null);
          setMissing(false);
          setRevision(value => value + 1);
        }}
        onClose={() => {
          setSettingsOpen(false);
          window.requestAnimationFrame(() => settingsButton.current?.focus());
        }} />}
      {missing && <p role="status">{t("This cluster has no ksqlDB endpoint configured.")}</p>}
      {error && <p className="alert alert-danger" role="alert">{error}</p>}
      {!missing && !error && !metadata && <p role="status">{t("Loading ksqlDB catalog…")}</p>}
      {metadata && <div className="kafdeck-ksql-catalog">
        <Catalog kind="stream" title={t("Streams")} metadata={metadata} />
        <Catalog kind="table" title={t("Tables")} metadata={metadata} />
      </div>}
      {info && <KsqlQueryConsole key={revision} clusterId={clusterId} />}
    </div>
  </section>;
}

function Catalog({ kind, title, metadata }: { kind: 'stream' | 'table'; title: string; metadata: KsqlMetadataItem[] }) {
  const items = metadata.filter(item => item.kind === kind);
  const id = 'ksql-catalog-' + kind;
  return <section className="kafdeck-ksql-catalog-panel" aria-labelledby={id}>
    <h3 id={id}>{title}</h3>
    {items.length === 0 ? <p>{kind === 'stream' ? t("No streams found.") : t("No tables found.")}</p> :
      <div className="table-responsive"><table className="table table-vcenter" aria-labelledby={id}>
        <thead><tr><th scope="col">{t("Name")}</th><th scope="col">{t("Topic")}</th><th scope="col">{t("Value format")}</th></tr></thead>
        <tbody>{items.map(item => <tr key={item.name}>
          <th scope="row"><bdi>{item.name}</bdi></th>
          <td><bdi>{item.kafkaTopic ?? t("Unknown")}</bdi></td>
          <td><bdi>{item.valueFormat ?? t("Unknown")}</bdi></td>
        </tr>)}</tbody>
      </table></div>}
  </section>;
}
