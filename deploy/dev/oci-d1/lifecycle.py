"""Bounded actual OCI process restart and SQLite backup/restore generation proof."""
import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import uuid
from v09_offline_install import http_probe
from v09_prepublication import digest, require, write


def command(*args, timeout=90):
    return subprocess.run([str(x) for x in args], check=True, capture_output=True, text=True, timeout=timeout).stdout.strip()


def create(name, private, admission=False):
    args = ['docker', 'create', '--name', name, '--pull=never', '--network=container:' + os.environ['D1_ANCHOR'],
            '--read-only', '--user=1654:1654', '--cap-drop=ALL', '--security-opt=no-new-privileges',
            '--tmpfs=/tmp:rw,nosuid,nodev,size=64m', '--env=HOME=/state',
            '--mount', f'type=bind,src={private},dst={private},readonly',
            '--mount', f'type=bind,src={private / "state"},dst=/state',
            os.environ['D1_IMAGE_ID'], '--contentRoot', private, '--webroot', '/app/wwwroot']
    if admission:
        args += ['--governance-generation-admit']
    return command(*args)


def prove_lifecycle(private, evidence, settings, origin, cli, topic, query, message):
    original = os.environ['D1_API_CONTAINER']
    database = private / 'state/data.db'
    generation = os.environ['D1_GENERATION']
    def observe(path):
        with sqlite3.connect(path) as db:
            require(db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok', 'Actual SQLite integrity failure')
            row = db.execute('SELECT generation,revision FROM governance_deployment_generation WHERE singleton_id=1').fetchone()
            require(row is not None, 'Actual OCI did not initialize persisted governance state')
            tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            require('catalog_owned_entities' in tables and 'service_request_heads' in tables, 'Actual persistence schemas missing')
            return {'generation': row[0], 'revision': row[1], 'tablesSha256': hashlib.sha256(json.dumps(tables).encode()).hexdigest()}
    def record():
        output = command(*cli, 'views', 'v03-records-browse', 'offline', topic, '0', '--request-file', query)
        values = [base64.b64decode(r['rawValue']).decode() for r in json.loads(output)['records'] if r.get('rawValue')]
        require(message in values, 'Authenticated exact record missing after OCI lifecycle')
    before = observe(database)
    require(before['generation'] == generation, 'Initial persisted generation differs')
    command('docker', 'stop', '--time=15', original)
    command('docker', 'start', original)
    http_probe(origin)
    record()
    require(observe(database) == before, 'Persisted application state changed on restart')
    command('docker', 'stop', '--time=15', original)
    backup, restored = private / 'state/backup.db', private / 'state/restored.db'
    with sqlite3.connect(database) as source, sqlite3.connect(backup) as target:
        source.backup(target)
    with sqlite3.connect(backup) as source, sqlite3.connect(restored) as target:
        source.backup(target)
    require(observe(restored) == before, 'Verified backup/restore changed application state')
    for path in (backup, restored):
        path.chmod(0o660)
        os.chown(path, 1654, 1654)
    fence = {'originalContainerStopped': json.loads(command('docker', 'inspect', original))[0]['State']['Running'] is False,
             'restoredDatabaseSha256': digest(restored), 'schemaAndGenerationPreserved': True}
    require(fence['originalContainerStopped'], 'Original OCI writer is not fenced')
    fence_path = evidence / 'writer-fence.json'
    write(fence_path, fence)
    next_generation = str(uuid.uuid4())
    settings.update({'Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath': '/state/restored.db',
        'Kafdeck:Governance:DeploymentGeneration': next_generation,
        'Logging:LogLevel:Default': 'Information',
        'Kafdeck:Governance:RestoreMaintenanceMode': 'true',
        'Kafdeck:Governance:PreviousDeploymentGeneration': generation,
        'Kafdeck:Governance:FencedWorkersEvidenceSha256': digest(fence_path),
        'Kafdeck:Governance:RestoreEvidenceSha256': digest(backup)})
    write(private / 'appsettings.json', settings)
    owned = []
    try:
        admission = create('d1-admission', private, True)
        owned.append(admission)
        admission_result = subprocess.run(['docker', 'start', '--attach', admission], capture_output=True, text=True, timeout=45)
        logs = admission_result.stdout + admission_result.stderr
        if admission_result.returncode:
            print('Bounded OCI admission diagnostic: ' + logs[-5000:], flush=True)
        details = json.loads(command('docker', 'inspect', admission))[0]
        require(details['State']['ExitCode'] == 0 and 'HTTP and workers were not started' in logs,
                'Actual image quarantined admission failed')
        for key in list(settings):
            if key in ('Kafdeck:Governance:RestoreMaintenanceMode', 'Kafdeck:Governance:PreviousDeploymentGeneration',
                       'Kafdeck:Governance:FencedWorkersEvidenceSha256', 'Kafdeck:Governance:RestoreEvidenceSha256'):
                del settings[key]
        settings['Kafdeck:Governance:DeploymentGeneration'] = generation
        write(private / 'appsettings.json', settings)
        stale = create('d1-stale', private)
        owned.append(stale)
        subprocess.run(['docker', 'start', '--attach', stale], capture_output=True, text=True, timeout=45)
        details = json.loads(command('docker', 'inspect', stale))[0]
        log_result = subprocess.run(['docker', 'logs', stale], capture_output=True, text=True, check=True, timeout=20)
        logs = log_result.stdout + log_result.stderr
        require(details['State']['ExitCode'] != 0 and 'GovernanceGenerationException' in logs,
                'Actual image stale generation not specifically refused')
        settings['Kafdeck:Governance:DeploymentGeneration'] = next_generation
        write(private / 'appsettings.json', settings)
        fresh = create('d1-fresh', private)
        owned.append(fresh)
        command('docker', 'start', fresh)
        http_probe(origin)
        record()
        after = observe(restored)
        require(after['generation'] == next_generation and after['tablesSha256'] == before['tablesSha256'],
                'Fresh generation or restored schema differs')
        return {'actualOciRestart': True, 'authenticatedRecordAfterRestart': True,
                'sqliteApplicationStatePersisted': True, 'backupIntegrityVerified': True,
                'backupSha256': digest(backup), 'originalWriterFenced': True,
                'quarantinedGenerationAdmission': True, 'staleGenerationRefused': True,
                'restoredFreshGenerationReady': True, 'authenticatedRecordAfterRestore': True,
                'before': before, 'after': after,
                'scope': 'same signed OCI bytes; actual SQLite initialized app state; verified backup and fresh-generation restore; no unknown broker effect replay claimed'}
    finally:
        for container in reversed(owned):
            subprocess.run(['docker', 'rm', '--force', container], capture_output=True, timeout=30)
