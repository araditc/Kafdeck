"""Only remaining actual OCI upgrade and PostgreSQL persistence/restore cells."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'scripts/release'))
from v09_prepublication import digest,read,require,write
from v09_offline_install import http_probe,isolated_network

p,e,arch=Path(sys.argv[1]),Path(sys.argv[2]),sys.argv[3]
mode=sys.argv[4]
require(mode in ('sqlite-only','pg-only','cross-pg'),'Unknown remaining cell')
SOURCE='01c7553425fbc61ba4a7a21918265c562086ba02'
OLD='ghcr.io/araditc/kafdeck@sha256:7b64f67b7f050b3c8cb5c2678b6ce94f6886e854cebfca6d6730170164fedb45'
origin='http://127.0.0.1:18082'
owned=[]
def run(*args,timeout=90,input=None):
    result=subprocess.run([str(x) for x in args],capture_output=True,text=True,check=True,timeout=timeout,input=input)
    return result.stdout.strip()
def sql(query,database='offline_fixture'):
    return run('docker','exec','d1-pg','psql','-U','offline_fixture','-d',database,'-v','ON_ERROR_STOP=1','-Atc',query)
def quote(value):
    if value is None:return 'NULL'
    if isinstance(value,(int,float)):return str(value)
    return "'"+str(value).replace("'","''")+"'"
def config(provider,location,generation=None):
    c={'Kafdeck:Deployment:ListenUrls:0':origin,'Kafdeck:Deployment:AccessMode':'Oidc',
       'Kafdeck:Deployment:Oidc:Issuer':'https://localhost:18444/realms/d1-persistence-fixture',
       'Kafdeck:Deployment:Oidc:ClientId':'d1-fixture','Kafdeck:Administration:Mutations:Enabled':'true',
       'Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled':'false',
       'Kafdeck:Administration:Mutations:MaterialDigestKey':'file:'+str(p/'material-key.txt'),
       'Kafdeck:Administration:Mutations:Persistence:Provider':provider,
       'Kafdeck:Administration:Mutations:Persistence:ExecutionMode':'Standalone','Logging:LogLevel:Default':'Information'}
    c['Kafdeck:Administration:Mutations:Persistence:'+('SqliteDatabasePath' if provider=='Sqlite' else 'ConnectionString')]=location
    if generation:c['Kafdeck:Governance:DeploymentGeneration']=generation
    return c
def create(name,c,image=None,admit=False):
    folder=p/name;folder.mkdir()
    write(folder/'appsettings.json',c)
    args=['docker','create','--name',name,'--pull=never','--network=container:'+os.environ['D1_ANCHOR'],
          '--read-only','--ulimit=core=0','--user=1654:1654','--cap-drop=ALL','--security-opt=no-new-privileges',
          '--tmpfs=/tmp:rw,nosuid,nodev,size=64m','--env=HOME=/state',
          '--mount',f'type=bind,src={p},dst={p},readonly','--mount',f'type=bind,src={p/"state"},dst=/state',
          image or os.environ['D1_IMAGE_ID'],'--contentRoot',folder,'--webroot','/app/wwwroot']
    if admit:args+=['--governance-generation-admit']
    cid=run(*args);owned.append(cid);return cid
def start(cid):
    run('docker','start',cid);http_probe(origin)
    require(json.loads(run('docker','inspect',cid))[0]['State']['Running'],'Actual OCI exited after health')
def stop(cid):run('docker','stop','--time=15',cid)
def logs(cid):
    r=subprocess.run(['docker','logs',cid],capture_output=True,text=True,check=True,timeout=15)
    return r.stdout+r.stderr
def ended(cid,code,needle):
    run('docker','start',cid)
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        d=json.loads(run('docker','inspect',cid))[0]['State']
        if not d['Running']:
            require(d['ExitCode']==code and needle in logs(cid),'Actual OCI maintenance admission failed')
            return
        time.sleep(.5)
    raise ValueError('Bounded actual OCI maintenance admission did not complete')
def stale(cid):
    run('docker','start',cid);deadline=time.monotonic()+45
    while time.monotonic()<deadline:
        if 'GovernanceGenerationException' in logs(cid):break
        time.sleep(.5)
    else:raise ValueError('Actual stale OCI generation was not refused')
    r=subprocess.run(['curl','--silent','--max-time','2',origin+'/healthz'],capture_output=True,timeout=5)
    require(r.returncode!=0,'Stale OCI opened HTTP listener')
    forced=json.loads(run('docker','inspect',cid))[0]['State']['Running']
    run('docker','rm','--force',cid);owned.remove(cid)
    return forced
def observe_sqlite(path):
    with closing(sqlite3.connect(path)) as db:
        require(db.execute('PRAGMA integrity_check').fetchone()[0]=='ok','SQLite integrity failure')
        rows=[{'State':state,'SnapshotSha256':hashlib.sha256(body.encode()).hexdigest()}
              for state,body in db.execute('SELECT state,snapshot_json FROM kafdeck_mutation_operations ORDER BY state,operation_id')]
        tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {'States':rows,'Tables':tables}
def probe(action,*extra):
    return json.loads(run(p/'old-dotnet/dotnet',p/'probe/UpgradeProbe.dll',action,'sqlite',p/'state/upgrade.db',*extra))
def sqlite_upgrade():
    generation=str(uuid.uuid4());db=p/'state/upgrade.db';canonical=e/'canonical';canonical.mkdir()
    if arch=='amd64':
        old=create('d1-old-open',config('Sqlite','/state/upgrade.db'),OLD);start(old);stop(old)
        original=probe('seed')
        for path in (p/'state').glob('*'):os.chown(path,1654,1654)
        old2=create('d1-old-seeded',config('Sqlite','/state/upgrade.db'),OLD);start(old2);stop(old2)
        require(observe_sqlite(db)==original,'Actual old OCI changed its three seeded snapshots')
        backup=canonical/'preupgrade.db'
        with closing(sqlite3.connect(db)) as a,closing(sqlite3.connect(backup)) as b:a.backup(b)
        original=observe_sqlite(backup)
        write(canonical/'canonical.json',{'schemaVersion':1,'sourceSha':SOURCE,'baselineImage':OLD,
             'oldPlatform':'linux/amd64','databaseSha256':digest(backup),'original':original,
             'driverSha':os.environ['GITHUB_SHA'],'runId':os.environ['GITHUB_RUN_ID'],
             'scope':'synthetic pending/approved/unknown snapshots initialized and reopened by actual official amd64 old OCI; no real user data'})
    else:
        canonical=p/'canonical';meta=read(canonical/'canonical.json')
        require(meta['sourceSha']==SOURCE and meta['runId']==os.environ['GITHUB_RUN_ID'] and digest(canonical/'preupgrade.db')==meta['databaseSha256'],
                'Canonical old-container data source differs')
        shutil.copyfile(canonical/'preupgrade.db',db);original=observe_sqlite(db)
        require(original==meta['original'],'Cross-platform old schema/snapshots differ')
        os.chown(db,1654,1654)
    candidate=create('d1-container-upgrade',config('Sqlite','/state/upgrade.db',generation));start(candidate);stop(candidate)
    upgraded=observe_sqlite(db)
    require(len(original['States'])==3 and original['States']==upgraded['States'],'OCI upgrade changed snapshots')
    require({'catalog_owned_schema','governance_generation_schema'}<=set(upgraded['Tables']),'New OCI did not migrate v0.9 schema')
    reopened=create('d1-container-reopen',config('Sqlite','/state/upgrade.db',generation));start(reopened);stop(reopened)
    require(observe_sqlite(db)==upgraded,'OCI migration unstable on reopen')
    rollback=False
    if arch=='amd64':
        restored=p/'state/rollback.db';shutil.copyfile(canonical/'preupgrade.db',restored);os.chown(restored,1654,1654)
        old3=create('d1-old-rollback',config('Sqlite','/state/rollback.db'),OLD);start(old3);stop(old3)
        require(observe_sqlite(restored)==original,'Actual old OCI backup rollback changed schema/state');rollback=True
    return {'actualOldAndNewContainers':arch=='amd64','crossPlatformOldAmd64DataToNativeArmOci':arch=='arm64',
            'samePersistedSQLiteVolume':True,'threeOldSnapshotsPreserved':True,'newSchemaMigrated':True,
            'reopenSchemaStable':True,'actualOldContainerBackupRollback':rollback,'oldArmContainerClaimed':False,
            'original':original,'upgraded':upgraded,'productRebuilt':False}
def pg_observe(database='offline_fixture'):
    generation=sql('SELECT generation||\'|\'||revision FROM governance_deployment_generation WHERE singleton_id=1',database)
    tables=sql("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public' ORDER BY tablename",database)
    rows=sql("SELECT state||'|'||snapshot_json FROM kafdeck_mutation_operations ORDER BY state,operation_id",database).splitlines()
    snapshots=[{'State':int(x.split('|',1)[0]),'SnapshotSha256':hashlib.sha256(x.split('|',1)[1].encode()).hexdigest()} for x in rows]
    return {'generation':generation,'tablesSha256':hashlib.sha256(tables.encode()).hexdigest(),'snapshots':snapshots}
def postgres():
    env=p/'postgres.env';pg=run('docker','create','--name','d1-pg','--pull=never','--network=container:'+os.environ['D1_ANCHOR'],
                              '--env-file',env,os.environ['D1_PG_IMAGE']);owned.append(pg);run('docker','start',pg)
    for _ in range(90):
        if subprocess.run(['docker','exec',pg,'pg_isready','-U','offline_fixture'],capture_output=True).returncode==0:break
        time.sleep(1)
    else:raise ValueError('Native PostgreSQL fixture not ready')
    pw=(p/'postgres-password.txt').read_text().strip()
    connection=p/'postgres-connection.txt';connection.write_text('Host=127.0.0.1;Port=5432;Database=offline_fixture;Username=offline_fixture;Password='+pw+';Pooling=false')
    generation=str(uuid.uuid4());c=config('PostgreSql','file:'+str(connection),generation)
    app=create('d1-pg-initial',c);start(app);stop(app)
    # Populate actual API-created PG storage with the canonical synthetic legacy representations; no effect dispatch.
    canonical=e/'canonical' if (e/'canonical').exists() else p/'canonical'
    with closing(sqlite3.connect(canonical/'preupgrade.db')) as db:
        columns=[r[1] for r in db.execute('PRAGMA table_info(kafdeck_mutation_operations)')]
        require(all(re.fullmatch('[a-z_]+',x) for x in columns),'Unsafe fixture column')
        for row in db.execute('SELECT * FROM kafdeck_mutation_operations'):
            sql('INSERT INTO kafdeck_mutation_operations('+','.join(columns)+') VALUES('+','.join(quote(x) for x in row)+')')
    before=pg_observe();require(len(before['snapshots'])==3,'PG synthetic state not populated')
    app2=create('d1-pg-restart',c);start(app2);stop(app2);require(pg_observe()==before,'PG app state changed on restart')
    backup=p/'postgres-backup.dump'
    with backup.open('wb') as stream:subprocess.run(['docker','exec',pg,'pg_dump','-U','offline_fixture','-d','offline_fixture','-Fc'],stdout=stream,check=True,timeout=90)
    run('docker','cp',backup,pg+':/tmp/backup.dump')
    require('governance_deployment_generation' in run('docker','exec',pg,'pg_restore','--list','/tmp/backup.dump'),'PG dump lacks actual state')
    run('docker','exec',pg,'createdb','-U','offline_fixture','restored')
    run('docker','exec',pg,'pg_restore','-U','offline_fixture','--exit-on-error','--single-transaction','-d','restored','/tmp/backup.dump')
    require(pg_observe('restored')==before,'Verified PG restore changed generation/schema/snapshots')
    sql('ALTER DATABASE restored SET default_transaction_read_only=on','postgres')
    denial=subprocess.run(['docker','exec',pg,'psql','-U','offline_fixture','-d','restored','-v','VERBOSITY=verbose','-c',"INSERT INTO governance_deployment_generation VALUES(2,'denied',1)"],capture_output=True,text=True)
    require(denial.returncode!=0 and '25006' in denial.stderr,'Restored PG quarantine did not reject write')
    # Only isolated operator now clears quarantine after both original app containers are stopped.
    require(all(not json.loads(run('docker','inspect',x))[0]['State']['Running'] for x in (app,app2)),'Original PG writers not fenced')
    sql('ALTER DATABASE restored RESET default_transaction_read_only','postgres')
    restored=p/'restored-connection.txt';restored.write_text(connection.read_text().replace('Database=offline_fixture;','Database=restored;'))
    nxt=str(uuid.uuid4());fence={'allOriginalAppContainersStopped':True,'restoredStateMatches':True};write(e/'pg-fence.json',fence)
    nc=config('PostgreSql','file:'+str(restored),nxt)
    nc.update({'Kafdeck:Governance:RestoreMaintenanceMode':'true','Kafdeck:Governance:PreviousDeploymentGeneration':generation,
               'Kafdeck:Governance:FencedWorkersEvidenceSha256':digest(e/'pg-fence.json'),'Kafdeck:Governance:RestoreEvidenceSha256':digest(backup)})
    admission=create('d1-pg-admission',nc,admit=True);ended(admission,0,'HTTP and workers were not started')
    forced=stale(create('d1-pg-stale',config('PostgreSql','file:'+str(restored),generation)))
    fresh=create('d1-pg-fresh',config('PostgreSql','file:'+str(restored),nxt));start(fresh);stop(fresh)
    after=pg_observe('restored')
    require(after['generation']==nxt+'|2' and after['snapshots']==before['snapshots'] and after['tablesSha256']==before['tablesSha256'],'Fresh OCI PG restore changed preserved state')
    return {'provider':'PostgreSql','serverVersion':sql('SHOW server_version'),'statePreservedOnActualOciRestart':True,'threeSyntheticSnapshotsPreserved':True,
            'customDumpRestoredAndCompared':True,'backupSha256':digest(backup),'quarantineWriteDeniedSqlState':'25006',
            'originalAppWritersFenced':True,'actualOciMaintenanceAdmission':True,'staleGenerationRefused':True,
            'staleContainerRequiredForcedCleanup':forced,'freshGenerationActualOciReady':True,'before':before,'after':after,
            'scope':'actual single-node PostgreSQL OCI app profile; verified custom backup restore and generation safety; physical PITR/HA remain separate accepted W80 proofs'}
try:
    isolated_network();run('ip','link','set','lo','up')
    sqlite=sqlite_upgrade() if mode!='pg-only' else None
    pg=postgres() if mode!='sqlite-only' else None
    proof=read(e/'signed-input.json')
    write(e/'remaining-ledger.json',{'schemaVersion':1,'sourceSha':SOURCE,'manifestSha256':proof['signedManifestSha256'],
          'architecture':arch,'machine':os.uname().machine,'emulated':False,'driverSha':os.environ['GITHUB_SHA'],
          'completedUtc':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),
          'actualSignedOciConfigDigest':os.environ['D1_CONFIG_DIGEST'],'sqliteUpgrade':sqlite,'postgres':pg,
          'authenticationRerun':False,'previousAuthEvidenceReused':True,'unknownEffectReplayClaimed':False,
          'scope':'remaining actual OCI-container upgrade and PostgreSQL state/restart/restore only; official signed media unchanged'})
finally:
    for cid in reversed(owned):subprocess.run(['docker','rm','--force',cid],capture_output=True,timeout=45)
