#!/usr/bin/python3 -I
"""Fixed offline agent lifecycle protocol for the dedicated SSH account."""
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import runpy
import shlex
import stat
import subprocess
import sys
import tempfile
import time
import zipfile

CONFIG = Path('/etc/kafdeck/agent-targets.json')
PACKAGES = Path('/var/lib/kafdeck-agent/packages')
JAR = 'kafdeck-kafka-agent.jar'
PROPERTIES = 'agent.properties'
ARGS = 'jvm.args'
STATE = '.kafdeck-agent-operation.json'
SHA = re.compile(r'[a-f0-9]{64}\Z')
VERSION = re.compile(r'[0-9]+\.[0-9]+\.[0-9]+(?:[-.][A-Za-z0-9.-]{1,56})?\Z')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}\Z')
PATH = re.compile(r'(/[A-Za-z0-9_.-]+){2,32}\Z')
OPERATION = re.compile(r'[a-f0-9]{32}\Z')
MAX_JAR = 4 * 1024 * 1024
MAX_LAUNCH_CONFIGURATION = 262144


class Rejected(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise Rejected(code)


def protected(path, directory=False):
    require(path.is_absolute() and '..' not in path.parts, 'agent_path_invalid')
    for parent in (*reversed(path.parents), path):
        info = parent.lstat()
        expected = stat.S_ISDIR if parent != path or directory else stat.S_ISREG
        require(expected(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022, 'agent_path_unprotected')
    return path


def read_file(path, limit):
    with path.open('rb') as stream:
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'agent_file_limit')
    return data


def read_json(path, limit=65536):
    return json.loads(read_file(protected(path), limit))


def atomic(path, content, mode):
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.kafdeck-stage-', delete=False) as stream:
        temporary = Path(stream.name)
        try:
            os.fchmod(stream.fileno(), mode)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
            fd = os.open(path.parent, os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def admitted_package(catalog, version, digest):
    require(VERSION.fullmatch(version) and SHA.fullmatch(digest), 'agent_package_invalid')
    require(catalog['packages'].get(version) == digest, 'agent_package_not_approved')
    package = protected(PACKAGES / (digest + '.jar'))
    content = read_file(package, MAX_JAR)
    require(sha(content) == digest, 'agent_package_integrity_failed')
    with zipfile.ZipFile(io.BytesIO(content)) as jar:
        manifest_info = jar.getinfo('META-INF/MANIFEST.MF')
        require(manifest_info.file_size <= 4096, 'agent_package_invalid')
        manifest = jar.read(manifest_info).decode('ascii')
        require('Premain-Class: org.kafdeck.monitoring.KafkaMonitoringAgent' in manifest.splitlines(), 'agent_package_invalid')
    return content


def scope(catalog, request, probe):
    target = request['targetId']
    require(NAME.fullmatch(target), 'agent_target_invalid')
    settings = catalog['targets'].get(target)
    service = next((row for row in probe['load_targets']() if row['id'] == target), None)
    require(settings is not None and service is not None, 'agent_target_not_approved')
    host_path, runtime = (settings[key] for key in ('hostDirectory', 'runtimeDirectory'))
    require(PATH.fullmatch(host_path) and PATH.fullmatch(runtime) and
            '..' not in Path(runtime).parts and '.' not in Path(runtime).parts, 'agent_path_invalid')
    require(settings['maintenanceFrom'] < settings['maintenanceTo'] and
            settings['maintenanceTo'] - settings['maintenanceFrom'] <= 8 * 3600, 'agent_maintenance_invalid')
    return settings, service, protected(Path(host_path), directory=True), runtime


def process(service, request, probe):
    observed = probe['observe'](service)
    require(observed['status'] == 'verified' and observed['brokerId'] == request['brokerId']
            and observed['kafkaClusterId'] == request['kafkaClusterId'], 'agent_target_identity_unavailable')
    selector = observed['instance'].split(':', 1)[0]
    if observed['adapter'] == 'systemd':
        pid = int(selector)
        restart = ['/usr/bin/systemctl', 'restart', observed['unit']]
    else:
        pid = int(probe['command'](['/usr/bin/docker', 'inspect', '--format', '{{.State.Pid}}', selector]))
        restart = ['/usr/bin/docker', 'restart', '--time', '30', selector]
    return observed, pid, restart


def require_runtime_mount(observed, runtime, probe):
    if observed['adapter'] != 'compose':
        return
    container = observed['instance'].split(':', 1)[0]
    mounts = json.loads(probe['command'](['/usr/bin/docker', 'inspect', '--format', '{{json .Mounts}}', container]))
    matches = [mount for mount in mounts if mount['Destination'] == runtime]
    require(len(matches) == 1 and matches[0]['Type'] == 'bind' and matches[0]['RW'] is False,
            'agent_runtime_mount_not_read_only')


def pending_preparation(settings, observed):
    prepared = settings.get('bootstrap')
    if not prepared or prepared.get('instance') != observed['instance']:
        return None
    configuration = protected(Path(prepared['configurationPath']))
    require(sha(read_file(configuration, MAX_LAUNCH_CONFIGURATION)) == prepared['configurationSha256'],
            'agent_preparation_changed')
    if observed['adapter'] == 'systemd':
        require(str(configuration) == '/etc/systemd/system/' + observed['unit'] + '.d/90-kafdeck-agent.conf',
                'agent_preparation_changed')
    else:
        require(prepared['project'] == observed['project'] and prepared['service'] == observed['service'] and
                NAME.fullmatch(prepared['project']) and NAME.fullmatch(prepared['service']),
                'agent_preparation_changed')
        for source in prepared['sourceFiles']:
            path = Path(source['path'])
            require(path.is_absolute() and path.resolve() == path and path.stat().st_size <= 4 * 1024 * 1024 and
                    sha(read_file(path, 4 * 1024 * 1024)) == source['sha256'], 'agent_compose_configuration_changed')
    return prepared


def prepared_restart(prepared):
    command = ['/usr/bin/docker', 'compose', '--project-name', prepared['project'],
               '--project-directory', prepared['workingDirectory']]
    for path in [*prepared['composeFiles'], prepared['configurationPath']]:
        command.extend(['-f', path])
    return [*command, 'up', '-d', '--no-deps', '--force-recreate', '--pull', 'never', '--no-build', prepared['service']]


def installed_files(directory, runtime, pid, prepared=False):
    in_namespace = Path('/proc') / str(pid) / 'root' / runtime.lstrip('/')
    if not prepared:
        require(in_namespace.stat().st_ino == directory.stat().st_ino and
                in_namespace.stat().st_dev == directory.stat().st_dev, 'agent_directory_not_mounted')
    properties = read_file(protected(directory / PROPERTIES), 16384)
    token_lines = [line.split('=', 1)[1].strip() for line in properties.decode('utf-8').splitlines()
                   if '=' in line and line.split('=', 1)[0].strip() == 'tokenFile']
    require(len(token_lines) == 1 and token_lines[0].startswith(runtime + '/'), 'agent_token_file_missing')
    basename = token_lines[0][len(runtime) + 1:]
    require(NAME.fullmatch(basename) and basename not in (PROPERTIES, ARGS, JAR), 'agent_token_file_missing')
    info = (directory / basename).lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_size in range(32, 1025) and not info.st_mode & 0o037, 'agent_token_file_unprotected')
    arg = f'-javaagent:{runtime}/{JAR}={runtime}/{PROPERTIES}'
    process = Path('/proc') / str(pid)
    arguments = read_file(process / 'cmdline', 65536).decode('utf-8').split('\0')
    java_options = [line.split('=', 1)[1] for line in read_file(process / 'environ', 262144).decode('utf-8').split('\0')
                    if line.startswith('JDK_JAVA_OPTIONS=')]
    direct = [item for item in arguments if item.startswith(f'-javaagent:{runtime}/')]
    argfile_enabled = prepared or len(java_options) == 1 and shlex.split(java_options[0]).count(f'@{runtime}/{ARGS}') == 1
    require(direct == [arg] and not argfile_enabled or not direct and argfile_enabled, 'agent_launch_integration_missing')
    saved_args = read_file(protected(directory / ARGS), 4096).decode('ascii').strip() if argfile_enabled else None
    require(saved_args in (None, '', arg), 'agent_launch_integration_changed')
    digest = sha(read_file(protected(directory / JAR), MAX_JAR)) if (directory / JAR).exists() else None
    require(not direct or digest is not None, 'agent_installed_package_missing')
    return digest, ('argfile' if argfile_enabled else 'existingJvmArgument'), saved_args, arg


def state_write(directory, plan, phase, dispatched=None):
    record = {'plan': plan, 'phase': phase, 'dispatchedAt': dispatched}
    atomic(directory / STATE, json.dumps(record, separators=(',', ':')).encode(), 0o600)
    return record


def journal_plan(request):
    return {key: request[key] for key in ('operationId','targetId','brokerId','kafkaClusterId','instance',
                                           'currentDigest','version','digest','launchMode')}


def status(directory, runtime, service, request, probe, state):
    if state is None or state['plan']['operationId'] != request.get('operationId'):
        return {'stage': 'unknown'}
    plan = state['plan']
    stage = {'publishing': 'publishing', 'intent': 'awaitingRestart', 'complete': 'complete'}[state['phase']]
    result = {'stage': stage, 'version': plan['version'], 'digest': plan['digest'],
              'dispatchedAt': state['dispatchedAt']}
    if state['phase'] in ('intent', 'complete'):
        try:
            observed, pid, _ = process(service, request, probe)
            require_runtime_mount(observed, runtime, probe)
            digest, mode, args, arg = installed_files(directory, runtime, pid)
            if observed['instance'] != plan['instance'] and digest == plan['digest'] and (mode != 'argfile' or args == arg):
                result.update(stage='restartObserved' if state['phase'] == 'intent' else 'complete',
                              instance=observed['instance'])
                # Linux process start in UTC, for matching JVM StartTime metrics.
                boot = next(int(line.split()[1]) for line in Path('/proc/stat').read_text().splitlines() if line.startswith('btime '))
                result['processStartedAt'] = boot + int(observed['instance'].split(':')[-1]) // os.sysconf('SC_CLK_TCK')
                # Compose can execute kafka.Kafka long after its PID was born as a shell.
                # This trusted observation bounds the JVM start without assuming a fixed startup delay.
                result['javaObservedAt'] = int(time.time())
            else:
                result['stage'] = 'awaitingRestart'
        except (OSError, ValueError, subprocess.SubprocessError):
            result['stage'] = 'awaitingRestart'
    return result


def handle(catalog, request, probe):
    action = request['action']
    require(action in ('preview', 'apply', 'status', 'complete') and request.get('operationId') is not None and
            OPERATION.fullmatch(request['operationId']) and type(request['brokerId']) is int and request['brokerId'] >= 0 and
            isinstance(request['kafkaClusterId'], str) and 0 < len(request['kafkaClusterId']) <= 128, 'agent_request_invalid')
    settings, service, directory, runtime = scope(catalog, request, probe)
    lock_fd = os.open(directory / '.kafdeck-agent.lock', os.O_CREAT | os.O_NOFOLLOW | os.O_WRONLY | os.O_NONBLOCK, 0o600)
    with os.fdopen(lock_fd, 'wb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = read_json(directory / STATE) if (directory / STATE).exists() else None
        if state and state['plan']['operationId'] == request['operationId'] and state['phase'] != 'publishing':
            result = status(directory, runtime, service, request, probe, state)
            if action == 'complete' and result['stage'] == 'restartObserved':
                state_write(directory, state['plan'], 'complete', state['dispatchedAt'])
                result['stage'] = 'complete'
            return result
        if action in ('status', 'complete'):
            return status(directory, runtime, service, request, probe, state)
        require(not state or state['phase'] == 'complete' or state['plan']['operationId'] == request['operationId'],
                'agent_previous_operation_pending')
        package = admitted_package(catalog, request['version'], request['digest'])
        observed, pid, restart = process(service, request, probe)
        preparation = pending_preparation(settings, observed)
        if preparation and observed['adapter'] == 'compose':
            restart = prepared_restart(preparation)
        if not preparation:
            require_runtime_mount(observed, runtime, probe)
        digest, mode, args, argument = (installed_files(directory, runtime, pid, True) if preparation else installed_files(directory, runtime, pid))
        publishing = state is not None and state['phase'] == 'publishing'
        if publishing:
            plan = journal_plan(request)
            require(request.get('expectRestart') is True and state['plan'] == plan and
                    plan['instance'] == observed['instance'] and plan['launchMode'] == mode and
                    digest in (plan['currentDigest'], plan['digest']), 'agent_preview_stale')
        restart_required = publishing or digest != request['digest'] or args == ''
        if action == 'preview':
            current_digest = plan['currentDigest'] if publishing else digest
            current = next((name for name, value in catalog['packages'].items() if value == current_digest), None)
            return {'stage': 'preview', 'adapter': observed['adapter'], 'currentDigest': current_digest, 'currentVersion': current,
                    'instance': observed['instance'], 'launchMode': mode, 'restartRequired': restart_required,
                    'maintenanceFrom': settings['maintenanceFrom'], 'maintenanceTo': settings['maintenanceTo']}
        plan = journal_plan(request)
        require(request.get('expectRestart') is restart_required and plan['instance'] == observed['instance'] and
                plan['launchMode'] == mode and (publishing or digest == plan['currentDigest']), 'agent_preview_stale')
        if not restart_required:
            return {'stage': 'alreadyInstalled', 'version': plan['version'], 'digest': plan['digest']}
        now = int(time.time())
        require(settings['maintenanceFrom'] <= now < settings['maintenanceTo'] and type(request['applyUntil']) is int
                and now < request['applyUntil'] <= settings['maintenanceTo'], 'agent_outside_maintenance')
        if not publishing:
            state_write(directory, plan, 'publishing')
        if digest != plan['digest']:
            atomic(directory / JAR, package, 0o644)
        if args == '':
            atomic(directory / ARGS, (argument + '\n').encode('ascii'), 0o644)
        rechecked, _, restart_now = process(service, request, probe)
        rechecked_preparation = pending_preparation(settings, rechecked)
        if rechecked_preparation and rechecked['adapter'] == 'compose':
            restart_now = prepared_restart(rechecked_preparation)
        require(observed == rechecked and rechecked_preparation == preparation and restart_now == restart and
                time.time() < request['applyUntil'], 'agent_preview_stale')
        # At this point a lost result cannot prove whether the service manager
        # accepted the restart. Keep the intent and never dispatch it twice.
        state_write(directory, plan, 'intent', int(time.time()))
        try:
            subprocess.run(restart, timeout=60, check=True, stdin=subprocess.DEVNULL,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'})
        except (OSError, subprocess.SubprocessError):
            pass
        return status(directory, runtime, service, request, probe, read_json(directory / STATE))


if __name__ == '__main__':
    try:
        require(os.geteuid() == 0 and len(sys.argv) == 1, 'agent_command_denied')
        raw = sys.stdin.buffer.read(4097)
        require(len(raw) <= 4096, 'agent_request_invalid')
        config = read_json(CONFIG)
        require(config['version'] == 1 and 1 <= len(config['targets']) <= 32 and
                1 <= len(config['packages']) <= 64, 'agent_catalog_invalid')
        helpers = runpy.run_path('/usr/local/libexec/kafdeck-host-probe')
        print(json.dumps(handle(config, json.loads(raw), helpers), separators=(',', ':')))
    except Rejected as failure:
        print(json.dumps({'stage': 'blocked', 'code': str(failure)}, separators=(',', ':')))
    except (OSError, KeyError, ValueError, TypeError, zipfile.BadZipFile, subprocess.SubprocessError):
        print('{"stage":"blocked","code":"agent_host_unavailable"}')
