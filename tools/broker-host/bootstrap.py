#!/usr/bin/python3 -I
"""Pinned Fleet onboarding program. Input is supplied by the authenticated API."""
import base64
import fcntl
import grp
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shlex
import stat
import subprocess
import sys
import tempfile
import time

NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}\Z')
SHA = re.compile(r'[a-f0-9]{64}\Z')
FILES = {'probe.py', 'ssh-entry.sh', 'agent-lifecycle.py', 'agent-entry.sh'}
MAX_LAUNCH_CONFIGURATION = 262144


class Rejected(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise Rejected(code)


def command(args):
    result = subprocess.run(args, check=True, stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=10, env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'})
    require(len(result.stdout) <= 262144, 'bootstrap_output_limit')
    return result.stdout.decode('utf-8').strip()


def protected(path):
    for entry in (*reversed(path.parents), path):
        info = entry.lstat()
        require(not stat.S_ISLNK(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022,
                'bootstrap_path_unprotected')
    return path


def directory(path, mode=0o755):
    if not path.exists():
        protected(path.parent)
        path.mkdir(mode=mode)
    protected(path)
    require(path.is_dir(), 'bootstrap_path_invalid')
    return path


def atomic(path, data, mode=0o644, group=0):
    protected(path.parent)
    if path.exists() or path.is_symlink():
        protected(path)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.kafdeck-', delete=False) as output:
        temporary = Path(output.name)
        try:
            os.fchmod(output.fileno(), mode)
            os.fchown(output.fileno(), 0, group)
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
            os.replace(temporary, path)
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)


def read_catalog(path, empty):
    if not path.exists():
        return empty
    protected(path)
    require(path.stat().st_size <= 131072, 'bootstrap_catalog_invalid')
    return json.loads(path.read_text())


def service_inventory(probe):
    candidates = []
    # One systemd query avoids invoking a process for every non-Kafka unit.
    if Path('/run/systemd/system').is_dir():
        rows = command(['/usr/bin/systemctl', 'show', '--all', '--type=service',
                        '--property=Id,MainPID']).split('\n\n')
        for row in rows:
            fields = dict(line.split('=', 1) for line in row.splitlines() if '=' in line)
            unit, pid = fields.get('Id', ''), fields.get('MainPID', '0')
            if not NAME.fullmatch(unit) or not pid.isdecimal() or int(pid) == 0:
                continue
            try:
                args = (Path('/proc') / pid / 'cmdline').read_bytes()
                if b'kafka.Kafka' in args.split(b'\0'):
                    candidates.append({'id': 'service-' + hashlib.sha256(unit.encode()).hexdigest()[:16],
                                       'adapter': 'systemd', 'unit': unit, 'logs': True})
            except (FileNotFoundError, PermissionError):
                continue
    if Path('/usr/bin/docker').exists():
        try:
            ids = command(['/usr/bin/docker', 'ps', '--quiet']).splitlines()
        except subprocess.SubprocessError:
            ids = []
        require(len(ids) <= 128, 'bootstrap_container_limit')
        if ids:
            containers = json.loads(command(['/usr/bin/docker', 'inspect', *ids]))
            for container in containers:
                labels = container.get('Config', {}).get('Labels') or {}
                project = labels.get('com.docker.compose.project', '')
                service = labels.get('com.docker.compose.service', '')
                if NAME.fullmatch(project) and NAME.fullmatch(service):
                    selector = project + '/' + service
                    candidates.append({'id': 'compose-' + hashlib.sha256(selector.encode()).hexdigest()[:16],
                                       'adapter': 'compose', 'project': project, 'service': service, 'logs': True})
    require(len(candidates) <= 64, 'bootstrap_service_limit')
    observed = []
    for candidate in candidates:
        result = probe['observe'](candidate)
        if result['status'] == 'verified':
            observed.append(result)
    return observed


def install_account(user, home, entry, helper, public_key, operation=None):
    require(re.fullmatch(r'ssh-rsa [A-Za-z0-9+/=]+', public_key), 'bootstrap_public_key_invalid')
    try:
        account = pwd.getpwnam(user)
        require(account.pw_dir == str(home) and account.pw_uid != 0, 'bootstrap_account_conflict')
        require(user not in grp.getgrnam('root').gr_mem, 'bootstrap_account_conflict')
    except KeyError:
        command(['/usr/sbin/useradd', '--system', '--home-dir', str(home), '--shell', '/bin/sh', user])
    directory(home)
    directory(home / '.ssh')
    authorized = home / '.ssh/authorized_keys'
    prefix = 'restrict,command="' + entry + '" '
    previous = authorized.read_text().splitlines() if authorized.exists() else []
    require(all(not line or line.startswith(prefix) for line in previous), 'bootstrap_account_conflict')
    line = prefix + public_key
    if operation is not None:
        require(re.fullmatch(r'[a-f0-9]{32}', operation), 'bootstrap_operation_invalid')
        line += ' kafdeck-fleet-' + operation + '-' + user
    atomic(authorized, ('\n'.join(dict.fromkeys([*previous, line])) + '\n').encode())
    rule = user + ' ALL=(root) NOPASSWD: ' + helper + ' ""'
    if user == 'kafdeck-probe':
        rule += ', ' + helper + ' --logs-v1, ' + helper + ' --setup-v1'
    atomic(Path('/etc/sudoers.d') / user, (rule + '\n').encode(), 0o440)
    command(['/usr/sbin/visudo', '-cf', '/etc/sudoers.d/' + user])


def prepare_launch(target, host_directory, runtime, probe):
    observed = probe['observe'](target)
    require(observed['status'] == 'verified', 'bootstrap_service_changed')
    if observed['adapter'] == 'systemd':
        pid = int(observed['instance'].split(':')[0])
    else:
        container = observed['instance'].split(':')[0]
        details = json.loads(command(['/usr/bin/docker', 'inspect', container]))[0]
        pid = details['State']['Pid']
    environ = (Path('/proc') / str(pid) / 'environ').read_bytes().decode().split('\0')
    environment = dict(line.split('=', 1) for line in environ if '=' in line)
    old_options = environment.get('JDK_JAVA_OPTIONS', '')
    args = (Path('/proc') / str(pid) / 'cmdline').read_bytes().decode().split('\0')
    require(not any(arg.startswith('-javaagent:') and 'kafdeck' in arg.lower() for arg in args),
            'bootstrap_existing_agent_requires_adoption')
    require('\n' not in old_options and '\r' not in old_options and len(old_options) <= 8192,
            'bootstrap_java_options_invalid')
    options = shlex.split(old_options)
    hook = '@' + runtime + '/jvm.args'
    require(options.count(hook) <= 1, 'bootstrap_java_options_invalid')
    new_options = shlex.join(options if hook in options else [*options, hook])
    gid = next(int(line.split()[2]) for line in (Path('/proc') / str(pid) / 'status').read_text().splitlines()
               if line.startswith('Gid:'))
    prepared = {'instance': observed['instance']}
    if observed['adapter'] == 'systemd':
        directory(Path('/etc/systemd/system'))
        parent = directory(Path('/etc/systemd/system') / (observed['unit'] + '.d'))
        path = parent / '90-kafdeck-agent.conf'
        # systemd doubles percent expansion even inside quotes.
        value = ('JDK_JAVA_OPTIONS=' + new_options).replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%')
        content = ('[Service]\nEnvironment="' + value + '"\n').encode()
        if path.exists():
            require(path.read_bytes() == content, 'bootstrap_launch_configuration_conflict')
        atomic(path, content)
        command(['/usr/bin/systemctl', 'daemon-reload'])
    else:
        labels = details['Config']['Labels']
        files = labels.get('com.docker.compose.project.config_files', '').split(',')
        working = Path(labels.get('com.docker.compose.project.working_dir', ''))
        require(files and all(Path(name).is_absolute() and Path(name).resolve() == Path(name) and Path(name).is_file() for name in files)
                and working.is_absolute() and working.resolve() == working and working.is_dir(), 'bootstrap_compose_configuration_unavailable')
        sources = [Path(name) for name in files]
        if (working / '.env').exists():
            sources.append(working / '.env')
        require(all(path.stat().st_size <= 4 * 1024 * 1024 for path in sources), 'bootstrap_compose_configuration_unavailable')
        source_hashes = [{'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for path in sources]
        base = ['/usr/bin/docker', 'compose', '--project-name', target['project'], '--project-directory', str(working)]
        for file in files:
            base.extend(['-f', file])
        actual_hash = command([*base, 'config', '--hash', target['service']]).split()
        require(len(actual_hash) == 2 and actual_hash[0] == target['service'] and
                actual_hash[1] == labels.get('com.docker.compose.config-hash'), 'bootstrap_compose_configuration_changed')
        resolved = json.loads(command([*base, 'config', '--format', 'json']))
        require(all(hashlib.sha256(Path(item['path']).read_bytes()).hexdigest() == item['sha256']
                    for item in source_hashes), 'bootstrap_compose_configuration_changed')
        selected = resolved['services'][target['service']]
        selected.setdefault('environment', {})['JDK_JAVA_OPTIONS'] = new_options
        selected.setdefault('volumes', []).append({'type': 'bind', 'source': str(host_directory), 'target': runtime, 'read_only': True})
        selected['image'] = details['Image']
        selected['pull_policy'] = 'never'
        # Run only an admitted, root-owned snapshot. Never reread user-writable Compose
        # inputs during privileged recreation. Escape interpolation in resolved literals.
        def literal(value):
            if isinstance(value, str):
                return value.replace('$', '$$')
            if isinstance(value, list):
                return [literal(item) for item in value]
            if isinstance(value, dict):
                return {key: literal(item) for key, item in value.items()}
            return value
        content = json.dumps(literal(resolved), sort_keys=True).encode()
        require(len(content) <= MAX_LAUNCH_CONFIGURATION, 'bootstrap_launch_configuration_limit')
        directory(Path('/etc/kafdeck/compose'))
        path = Path('/etc/kafdeck/compose') / (target['id'] + '.json')
        if path.exists():
            require(path.read_bytes() == content, 'bootstrap_launch_configuration_conflict')
        atomic(path, content, 0o600)
        prepared.update(composeFiles=[], workingDirectory=str(working), project=target['project'], service=target['service'],
                        sourceFiles=source_hashes)
    prepared.update(configurationPath=str(path), configurationSha256=hashlib.sha256(content).hexdigest())
    return prepared, gid


def apply(payload, probe):
    operation = payload['operationId']
    require(re.fullmatch(r'[a-f0-9]{32}', operation), 'bootstrap_operation_invalid')
    directory(Path('/etc/kafdeck'))
    journal_directory = directory(Path('/var/lib/kafdeck-bootstrap'), 0o700)
    lock_path = journal_directory / 'setup.lock'
    lock = os.open(lock_path, os.O_CREAT | os.O_NOFOLLOW | os.O_WRONLY, 0o600)
    with os.fdopen(lock, 'wb') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        receipt = journal_directory / (operation + '.json')
        expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        if receipt.exists():
            previous = read_catalog(receipt, {})
            require(previous['payloadHash'] == expected, 'bootstrap_operation_conflict')
            if previous['stage'] == 'prepared':
                return previous
        # Validate every selected identity before altering host configuration.
        for target in payload['targets']:
            actual = probe['observe'](target)
            require(actual['status'] == 'verified' and actual['instance'] == target['instance'] and
                    actual['brokerId'] == target['brokerId'] and actual['kafkaClusterId'] == target['kafkaClusterId'],
                    'bootstrap_service_changed')
        atomic(receipt, json.dumps({'stage': 'preparing', 'payloadHash': expected}).encode(), 0o600)
        directory(Path('/usr/local/libexec'))
        for source, installed in (('probe.py', 'kafdeck-host-probe'), ('ssh-entry.sh', 'kafdeck-host-entry'),
                                  ('agent-lifecycle.py', 'kafdeck-agent-lifecycle'), ('agent-entry.sh', 'kafdeck-agent-entry')):
            atomic(Path('/usr/local/libexec') / installed, base64.b64decode(payload['files'][source], validate=True), 0o755)
        install_account('kafdeck-probe', Path('/var/lib/kafdeck-probe'), '/usr/local/libexec/kafdeck-host-entry',
                        '/usr/local/libexec/kafdeck-host-probe', payload['probePublicKey'], operation)
        install_account('kafdeck-agent', Path('/var/lib/kafdeck-agent-ssh'), '/usr/local/libexec/kafdeck-agent-entry',
                        '/usr/local/libexec/kafdeck-agent-lifecycle', payload['agentPublicKey'], operation)
        host_catalog = read_catalog(Path('/etc/kafdeck/host-targets.json'), {'version': 1, 'targets': []})
        selected = {target['id'] for target in payload['targets']}
        entries = [target for target in host_catalog['targets'] if target['id'] not in selected]
        for target in payload['targets']:
            entries.append(dict({key: target[key] for key in ('id', 'adapter', 'unit', 'project', 'service') if target.get(key) is not None}, logs=True))
        require(len(entries) <= 32, 'bootstrap_service_limit')
        atomic(Path('/etc/kafdeck/host-targets.json'), json.dumps({'version': 1, 'targets': entries}).encode())
        directory(Path('/var/lib/kafdeck-agent'))
        packages = directory(Path('/var/lib/kafdeck-agent/packages'))
        package = base64.b64decode(payload['agentJar'], validate=True)
        require(len(package) <= 4 * 1024 * 1024 and SHA.fullmatch(payload['digest']) and
                hashlib.sha256(package).hexdigest() == payload['digest'], 'bootstrap_package_invalid')
        atomic(packages / (payload['digest'] + '.jar'), package)
        catalog = read_catalog(Path('/etc/kafdeck/agent-targets.json'), {'version': 1, 'packages': {}, 'targets': {}})
        directory(Path('/var/lib/kafdeck-monitor'))
        for target in payload['targets']:
            target_id = target['id']
            host_directory = directory(Path('/var/lib/kafdeck-monitor') / target_id)
            runtime = str(host_directory) if target['adapter'] == 'systemd' else '/opt/kafdeck-monitor/' + target_id
            prepared, group = prepare_launch(target, host_directory, runtime, probe)
            source = payload['sources'][target_id]
            atomic(host_directory / 'agent-token', source['token'].encode(), 0o640, group)
            properties = 'endpoint=' + payload['endpoint'] + '\ntokenFile=' + runtime + '/agent-token\nintervalSeconds=' + str(payload['intervalSeconds']) + '\n'
            if payload.get('caPem'):
                atomic(host_directory / 'ca.pem', payload['caPem'].encode())
                properties += 'caFile=' + runtime + '/ca.pem\n'
            atomic(host_directory / 'agent.properties', properties.encode())
            if not (host_directory / 'jvm.args').exists():
                atomic(host_directory / 'jvm.args', b'')
            catalog['targets'][target_id] = {'hostDirectory': str(host_directory), 'runtimeDirectory': runtime,
                'maintenanceFrom': payload['maintenanceFrom'], 'maintenanceTo': payload['maintenanceTo'], 'bootstrap': prepared}
        catalog['packages'][payload['version']] = payload['digest']
        atomic(Path('/etc/kafdeck/agent-targets.json'), json.dumps(catalog).encode())
        artifacts = [Path('/etc/kafdeck/agent-targets.json'), Path('/etc/kafdeck/host-targets.json'),
                     packages / (payload['digest'] + '.jar'), Path('/var/lib/kafdeck-probe/.ssh/authorized_keys'),
                     Path('/var/lib/kafdeck-agent-ssh/.ssh/authorized_keys')]
        artifacts += [Path('/usr/local/libexec') / name for name in
                      ('kafdeck-host-probe', 'kafdeck-host-entry', 'kafdeck-agent-lifecycle', 'kafdeck-agent-entry')]
        for target in payload['targets']:
            root = Path(catalog['targets'][target['id']]['hostDirectory'])
            artifacts += [root / name for name in ('agent-token', 'agent.properties', 'jvm.args')]
            artifacts += [Path(catalog['targets'][target['id']]['bootstrap']['configurationPath'])]
            if (root / 'ca.pem').exists():
                artifacts.append(root / 'ca.pem')
        result = {'stage': 'prepared', 'payloadHash': expected, 'operationId': operation,
                  'proof': payload['proof'], 'artifacts': {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in artifacts},
                  'targets': payload['targets'], 'brokerRestarted': False}
        atomic(receipt, json.dumps(result).encode(), 0o600)
        return result


def handle(payload):
    require(os.geteuid() == 0, 'bootstrap_root_required')
    require(payload['action'] in ('inspect', 'prepare') and set(payload['files']) == FILES,
            'bootstrap_request_invalid')
    probe = {'__name__': 'kafdeck_bootstrap_probe'}
    exec(compile(base64.b64decode(payload['files']['probe.py'], validate=True), '<pinned-probe>', 'exec'), probe)
    if payload['action'] == 'inspect':
        return {'stage': 'inspected', 'targets': service_inventory(probe)}
    return apply(payload, probe)


if __name__ == '__main__':
    try:
        raw = sys.stdin.buffer.read(6 * 1024 * 1024 + 1)
        require(len(raw) <= 6 * 1024 * 1024, 'bootstrap_request_invalid')
        print(json.dumps(handle(json.loads(raw)), separators=(',', ':')))
    except Rejected as failure:
        print(json.dumps({'stage': 'blocked', 'code': str(failure)}, separators=(',', ':')))
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        print('{"stage":"blocked","code":"bootstrap_host_unavailable"}')
