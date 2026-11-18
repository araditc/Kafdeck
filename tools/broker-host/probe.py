#!/usr/bin/python3 -I
"""Read approved broker service identities through a restricted SSH command."""
import json
from datetime import datetime
import os
from pathlib import Path
import re
import stat
import selectors
import signal
import time
import subprocess
import sys

CONFIG = Path('/etc/kafdeck/host-targets.json')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}\Z')
DOCKER_TIMESTAMP = re.compile(rb'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z (.*)')


def command(arguments):
    stdout, _, _ = capture(arguments)
    return stdout.decode('utf-8', errors='strict').strip()


def capture(arguments, log_read=False):
    with subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}) as process:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, 'out')
            selector.register(process.stderr, selectors.EVENT_READ, 'err')
            output = {'out': bytearray(), 'err': bytearray()}
            deadline = time.monotonic() + 5
            try:
                while selector.get_map():
                    if time.monotonic() >= deadline:
                        raise ValueError('command_timeout')
                    for key, _ in selector.select(timeout=0.1):
                        chunk = os.read(key.fileobj.fileno(), 4096)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        if log_read and len(output['out']) + len(output['err']) + len(chunk) > 256 * 1024:
                            # Discard any incomplete record on either channel.
                            return (*(bytes(output[stream]).rpartition(b'\n')[0] for stream in ('out', 'err')), True)
                        output[key.data].extend(chunk)
                        if not log_read and len(output[key.data]) > 65536:
                            raise ValueError('output_limit')
                if process.wait(timeout=max(0.01, deadline-time.monotonic())) != 0:
                    raise ValueError('command_failed')
                return bytes(output['out']), bytes(output['err']), False
            finally:
                if process.poll() is None:
                    process.kill()



def small_file(path):
    with path.open('rb') as stream:
        content = stream.read(65537)
    if len(content) > 65536:
        raise ValueError('file_limit')
    return content.decode('utf-8', errors='strict')


def properties(content):
    result = {}
    logical, pending = [], ''
    for physical in content.splitlines():
        line = pending + (physical.lstrip() if pending else physical)
        if (len(line) - len(line.rstrip('\\'))) % 2:
            pending = line[:-1]
            continue
        logical.append(line)
        pending = ''
    if pending:
        raise ValueError('properties_unsupported')
    for line in logical:
        line = line.strip()
        if not line or line.startswith(('#', '!')):
            continue
        # Only identity properties are interpreted. Ignore unrelated values,
        # including JAAS credentials; never return configuration contents.
        if '=' not in line:
            if re.match(r'^(node\.id|broker\.id|log\.dirs|log\.dir|metadata\.log\.dir|cluster\.id)(\s|:|$)', line):
                raise ValueError('properties_unsupported')
            continue
        key, value = line.split('=', 1)
        if '\\' in key:
            raise ValueError('properties_unsupported')
        if key.strip() not in ('node.id', 'broker.id', 'log.dirs', 'log.dir', 'metadata.log.dir', 'cluster.id'):
            continue
        if '\\' in line:
            raise ValueError('properties_unsupported')
        if key.strip() in result:
            raise ValueError('properties_duplicate')
        result[key.strip()] = value.strip()
    return result


def rooted_properties(root, absolute):
    # Resolve relative to the running process's root without following any
    # inner symlink, including absolute links escaping a container namespace.
    parts = Path(absolute).parts
    if not absolute.startswith('/') or '..' in parts:
        raise ValueError('path_unsupported')
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(descriptor, 'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError('file_unsupported')
            content = stream.read(65537)
        if len(content) > 65536:
            raise ValueError('file_limit')
        return properties(content.decode('utf-8', errors='strict'))
    finally:
        os.close(directory)


def process_identity(pid):
    if pid <= 1:
        raise ValueError('service_inactive')
    process = Path('/proc') / str(pid)
    start = small_file(process / 'stat').rsplit(')', 1)[1].split()[19]
    args = small_file(process / 'cmdline').rstrip('\0').split('\0')
    main = args.index('kafka.Kafka')
    if len(args) != main + 2 or not args[main + 1].startswith('/'):
        raise ValueError('kafka_process_unsupported')
    root = process / 'root'
    config = rooted_properties(root, args[main + 1])
    node = config.get('node.id', config.get('broker.id', ''))
    if not node.isdecimal():
        raise ValueError('broker_id_missing')
    directories = config.get('log.dirs', config.get('log.dir', '')).split(',')
    if config.get('metadata.log.dir'):
        directories.append(config['metadata.log.dir'])
    directories = sorted(set(directory.strip() for directory in directories))
    if not directories or len(directories) > 16 or any(not item.startswith('/') or '..' in Path(item).parts for item in directories):
        raise ValueError('data_directories_unsupported')
    clusters = set()
    for directory in directories:
        metadata = rooted_properties(root, directory.rstrip('/') + '/meta.properties')
        if metadata.get('node.id', metadata.get('broker.id')) != node or not metadata.get('cluster.id'):
            raise ValueError('data_identity_mismatch')
        clusters.add(metadata['cluster.id'])
    if len(clusters) != 1 or small_file(process / 'stat').rsplit(')', 1)[1].split()[19] != start:
        raise ValueError('process_changed')
    return int(node), clusters.pop(), start


def observe(target):
    target_id = target['id']
    adapter = target['adapter']
    if not NAME.fullmatch(target_id):
        raise ValueError('invalid_target')
    result = {'id': target_id, 'adapter': adapter, 'unit': target.get('unit'),
              'project': target.get('project'), 'service': target.get('service'),
              'brokerId': None, 'kafkaClusterId': None, 'instance': None, 'status': 'unavailable'}
    try:
        if adapter == 'systemd':
            unit = target['unit']
            if not NAME.fullmatch(unit) or not unit.endswith('.service'):
                raise ValueError('invalid_unit')
            def service_pid():
                if command(['/usr/bin/systemctl', 'show', '--property=ActiveState', '--value', unit]) != 'active':
                    raise ValueError('service_inactive')
                return int(command(['/usr/bin/systemctl', 'show', '--property=MainPID', '--value', unit]))
            pid = service_pid()
            broker, cluster, start = process_identity(pid)
            if service_pid() != pid:
                raise ValueError('process_changed')
            instance = f'{pid}:{start}'
        elif adapter == 'compose':
            project, service = target['project'], target['service']
            if not NAME.fullmatch(project) or not NAME.fullmatch(service):
                raise ValueError('invalid_service')
            def container_identity():
                ids = command(['/usr/bin/docker', 'ps', '--no-trunc', '--quiet',
                               '--filter', f'label=com.docker.compose.project={project}',
                               '--filter', f'label=com.docker.compose.service={service}']).splitlines()
                if len(ids) != 1 or not re.fullmatch('[a-f0-9]{64}', ids[0]):
                    raise ValueError('service_ambiguous_or_stopped')
                # Emit selected state only. Container environment can hold secrets.
                state = json.loads(command(['/usr/bin/docker', 'inspect', '--format', '{"Running":{{.State.Running}},"Pid":{{.State.Pid}}}', ids[0]]))
                if not state['Running']:
                    raise ValueError('service_inactive')
                return ids[0], int(state['Pid'])
            container, pid = container_identity()
            broker, cluster, start = process_identity(pid)
            if container_identity() != (container, pid):
                raise ValueError('process_changed')
            instance = f'{container}:{start}'
        else:
            raise ValueError('adapter_unsupported')
        if len(cluster) > 128:
            raise ValueError('invalid_identity')
        result.update(brokerId=broker, kafkaClusterId=cluster, instance=instance, status='verified')
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        # No command arguments, exception text, paths or configuration content
        # cross the SSH boundary when observation fails.
        result['status'] = 'identity_unavailable'
    return result


def load_targets():
    for path in (CONFIG, CONFIG.parent):
        info = path.lstat()
        if info.st_uid != 0 or info.st_mode & 0o022 or stat.S_ISLNK(info.st_mode):
            raise ValueError('unsafe_catalog')
    catalog = json.loads(small_file(CONFIG))
    targets = catalog['targets']
    if catalog['version'] != 1 or not isinstance(targets, list) or not 1 <= len(targets) <= 32:
        raise ValueError('invalid_catalog')
    if len({target['id'] for target in targets}) != len(targets):
        raise ValueError('duplicate_target')
    return targets


def docker_lines(*streams):
    for stream in streams:
        for line in stream.split(b'\n'):
            match = DOCKER_TIMESTAMP.fullmatch(line)
            if not match:
                continue
            timestamp, fraction, message = match.groups()
            seconds = int(datetime.fromisoformat(timestamp.decode('ascii') + '+00:00').timestamp())
            milliseconds = int(((fraction or b'') + b'000')[:3])
            yield seconds * 1000 + milliseconds, message.decode('utf-8', errors='replace')


def journal_lines(stream):
    for line in stream.splitlines():
        if not line:
            continue
        item = json.loads(line)
        timestamp = int(item['__REALTIME_TIMESTAMP']) // 1000
        message = item.get('MESSAGE')
        yield timestamp, message if isinstance(message, str) else '[REDACTED]'


def bounded_lines(lines, start, end, truncated):
    selected = sorted(
        ((at, text) for at, text in lines if start * 1000 <= at < end * 1000),
        key=lambda item: item[0], reverse=True)
    truncated |= len(selected) > 200
    result, encoded_bytes = [], 0
    for at, text in selected[:200]:
        if len(text) > 2048 or text.startswith((' ', '\t', 'Caused by:', 'Suppressed:')):
            text = '[REDACTED]'
        row = {'timeMs': at, 'text': text}
        encoded_bytes += len(json.dumps(row, separators=(',', ':'))) + 1
        if encoded_bytes > 48 * 1024:
            truncated = True
            break
        result.append(row)
    return result, truncated


def read_logs(targets, request):
    if set(request) != {'targetId', 'brokerId', 'kafkaClusterId', 'from', 'to'}:
        raise ValueError('invalid_request')
    target_id, broker, cluster = request['targetId'], request['brokerId'], request['kafkaClusterId']
    start, end, now = request['from'], request['to'], int(time.time())
    if (not isinstance(target_id, str) or not NAME.fullmatch(target_id) or type(broker) is not int or broker < 0
            or not isinstance(cluster, str) or not 1 <= len(cluster) <= 128
            or type(start) is not int or type(end) is not int or not 0 < end - start <= 3600
            or start < now - 48 * 3600 - 30 or end > now + 150):
        raise ValueError('invalid_request')
    target = next((item for item in targets if item['id'] == target_id and item.get('logs') is True), None)
    if target is None:
        raise ValueError('log_source_not_approved')
    observed = observe(target)
    if observed['status'] != 'verified' or observed['brokerId'] != broker or observed['kafkaClusterId'] != cluster:
        raise ValueError('log_identity_mismatch')
    if observed['adapter'] == 'systemd':
        stdout, _, cut = capture(['/usr/bin/journalctl', '--no-pager', '--quiet', '--output=json',
                                  '--output-fields=MESSAGE', '--lines=201', f'--since=@{start}', f'--until=@{end}',
                                  f"_SYSTEMD_UNIT={observed['unit']}"], log_read=True)
        lines = journal_lines(stdout)
    else:
        container = observed['instance'].split(':')[0]
        if not re.fullmatch('[a-f0-9]{64}', container):
            raise ValueError('invalid_container')
        stdout, stderr, cut = capture(['/usr/bin/docker', 'logs', '--timestamps', '--tail', '201',
                                       '--since', str(start), '--until', str(end), container], log_read=True)
        lines = docker_lines(stdout, stderr)
    result, truncated = bounded_lines(lines, start, end, cut)
    if observe(target) != observed:
        raise ValueError('log_identity_changed')
    return {'version': 1, 'adapter': observed['adapter'], 'lines': result, 'truncated': truncated}


class LogDeadline(Exception):
    pass


def log_deadline(_signal, _frame):
    raise LogDeadline()


def main():
    if os.geteuid() != 0:
        raise ValueError('command_denied')
    if sys.argv[1:] == ['--logs-v1']:
        signal.signal(signal.SIGALRM, log_deadline)
        signal.alarm(10)
        request_bytes = sys.stdin.buffer.read(1025)
        if len(request_bytes) > 1024:
            raise ValueError('request_limit')
        result = read_logs(load_targets(), json.loads(request_bytes))
    elif len(sys.argv) == 1:
        result = {'version': 1, 'targets': [observe(target) for target in load_targets()]}
    else:
        raise ValueError('command_denied')
    print(json.dumps(result, separators=(',', ':')))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, LogDeadline):
        print('{"error":"host_probe_unavailable"}')
        sys.exit(1)
