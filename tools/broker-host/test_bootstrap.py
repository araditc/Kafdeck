import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


bootstrap = module('bootstrap', 'bootstrap.py')
probe = module('setup_probe', 'probe.py')
agent = module('setup_agent', 'agent-lifecycle.py')


class BootstrapTests(unittest.TestCase):
    def test_no_docker_is_required_for_inventory(self):
        with patch.object(bootstrap.Path, 'is_dir', return_value=False), \
             patch.object(bootstrap.Path, 'exists', return_value=False), \
             patch.object(bootstrap, 'command') as command:
            self.assertEqual([], bootstrap.service_inventory({'observe': lambda _: None}))
            command.assert_not_called()

    def test_root_owned_paths_reject_writable_or_symlink_ancestors(self):
        import stat
        class Info:
            st_uid = 0
            st_mode = stat.S_IFDIR | 0o777
        with patch.object(bootstrap.Path, 'lstat', return_value=Info()):
            with self.assertRaisesRegex(bootstrap.Rejected, 'bootstrap_path_unprotected'):
                bootstrap.protected(Path('/etc/kafdeck'))
            with self.assertRaisesRegex(ValueError, 'unsafe_setup_receipt'):
                probe.protected(Path('/var/lib/kafdeck-bootstrap/receipt'))

    def test_receipt_requires_prepared_stage_and_matching_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact = root / 'agent.properties'
            artifact.write_bytes(b'tokenFile=/protected/token')
            receipt = root / 'receipt.json'
            value = {'stage': 'prepared', 'proof': 'only-this-operation', 'artifacts': {
                str(artifact): hashlib.sha256(artifact.read_bytes()).hexdigest()}}
            receipt.write_text(json.dumps(value))
            with patch.object(probe, 'protected'), patch.object(probe, 'small_file', side_effect=lambda _: receipt.read_text()):
                self.assertEqual({'proof': 'only-this-operation'}, probe.setup_receipt({'operationId': 'a' * 32}))
                artifact.write_bytes(b'tampered')
                with self.assertRaisesRegex(ValueError, 'setup_changed'):
                    probe.setup_receipt({'operationId': 'a' * 32})
                value['stage'] = 'preparing'
                receipt.write_text(json.dumps(value))
                with self.assertRaisesRegex(ValueError, 'setup_incomplete'):
                    probe.setup_receipt({'operationId': 'a' * 32})

    def test_receipt_never_accepts_a_path_or_arbitrary_arguments(self):
        for request in ({'operationId': '../secrets'}, {'operationId': 'a'*32, 'command': 'cat /etc/shadow'}):
            with self.assertRaisesRegex(ValueError, 'invalid_request'):
                probe.setup_receipt(request)

    def test_existing_root_account_cannot_be_adopted_as_restricted_user(self):
        class Account:
            pw_dir = '/var/lib/kafdeck-probe'
            pw_uid = 0
        with patch.object(bootstrap.pwd, 'getpwnam', return_value=Account()), \
             patch.object(bootstrap, 'atomic') as write:
            with self.assertRaisesRegex(bootstrap.Rejected, 'bootstrap_account_conflict'):
                bootstrap.install_account('kafdeck-probe', Path('/var/lib/kafdeck-probe'),
                                          '/fixed/entry', '/fixed/helper', 'ssh-rsa YWJj')
            write.assert_not_called()

    def test_compose_preparation_rejects_changed_project_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            override = root / 'overlay.json'
            override.write_text('{}')
            source = root / 'compose.yml'
            source.write_text('services: {}')
            prepared = {'instance': 'container:123', 'project': 'pilot', 'service': 'broker',
                'configurationPath': str(override), 'configurationSha256': hashlib.sha256(override.read_bytes()).hexdigest(),
                'sourceFiles': [{'path': str(source), 'sha256': hashlib.sha256(source.read_bytes()).hexdigest()}]}
            observed = {'instance': 'container:123', 'adapter': 'compose', 'project': 'pilot', 'service': 'broker'}
            with patch.object(agent, 'protected', side_effect=lambda path: path):
                self.assertEqual(prepared, agent.pending_preparation({'bootstrap': prepared}, observed))
                source.write_text('services: {changed: {}}')
                with self.assertRaisesRegex(agent.Rejected, 'agent_compose_configuration_changed'):
                    agent.pending_preparation({'bootstrap': prepared}, observed)

    def test_prepared_compose_restart_uses_only_the_root_snapshot_without_pull_or_build(self):
        prepared = {'project': 'pilot', 'workingDirectory': '/srv/pilot', 'service': 'broker',
                    'composeFiles': [], 'configurationPath': '/etc/kafdeck/prepared/pilot.json'}
        command = agent.prepared_restart(prepared)
        self.assertIn('--no-deps', command)
        self.assertIn('--no-build', command)
        self.assertEqual('never', command[command.index('--pull') + 1])
        self.assertEqual('/etc/kafdeck/prepared/pilot.json', command[command.index('-f') + 1])
        self.assertEqual('broker', command[-1])

    def test_large_compose_snapshot_is_consumable_and_serialized_overflow_is_rejected_before_write(self):
        self.assertEqual(bootstrap.MAX_LAUNCH_CONFIGURATION, agent.MAX_LAUNCH_CONFIGURATION)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'compose.yml'
            source.write_text('services: {broker: {image: kafka}}')
            observed = {'status': 'verified', 'adapter': 'compose', 'instance': 'container:123',
                        'project': 'pilot', 'service': 'broker'}
            details = {'State': {'Pid': 123}, 'Image': 'sha256:' + 'a' * 64, 'Config': {'Labels': {
                'com.docker.compose.project.config_files': str(source),
                'com.docker.compose.project.working_dir': str(root), 'com.docker.compose.config-hash': 'current'}}}
            read_bytes = Path.read_bytes
            read_text = Path.read_text
            def process_bytes(path):
                return {Path('/proc/123/environ'): b'', Path('/proc/123/cmdline'): b'java\0kafka.Kafka\0'}.get(path, b'') if str(path).startswith('/proc/') else read_bytes(path)
            def process_text(path, *args, **kwargs):
                return 'Gid:\t0\t0\t0\t0\n' if path == Path('/proc/123/status') else read_text(path, *args, **kwargs)
            for character in ('x', '\u00e9'):
                with self.subTest(character=character):
                    resolved = {'services': {'broker': {'environment': {'LARGE': character * 70000}}}}
                    raw = json.dumps(resolved, ensure_ascii=False)
                    self.assertLess(len(raw.encode()), bootstrap.MAX_LAUNCH_CONFIGURATION)
                    def command(args):
                        if args[1] == 'inspect':
                            return json.dumps([details])
                        return 'broker current' if '--hash' in args else raw
                    with patch.object(bootstrap, 'command', side_effect=command), \
                         patch.object(bootstrap.Path, 'read_bytes', process_bytes), \
                         patch.object(bootstrap.Path, 'read_text', process_text), \
                         patch.object(bootstrap, 'directory', side_effect=lambda path: path), \
                         patch.object(bootstrap, 'atomic') as write:
                        target = dict(observed, id='large-snapshot-test')
                        if character != 'x':
                            with self.assertRaisesRegex(bootstrap.Rejected, 'bootstrap_launch_configuration_limit'):
                                bootstrap.prepare_launch(target, root / 'agent', '/opt/kafdeck-agent', {'observe': lambda _: observed})
                            write.assert_not_called()
                            continue
                        prepared, _ = bootstrap.prepare_launch(target, root / 'agent', '/opt/kafdeck-agent', {'observe': lambda _: observed})
                        content = write.call_args.args[1]
                    self.assertGreater(len(content), 65536)
                    snapshot = root / 'snapshot.json'
                    snapshot.write_bytes(content)
                    prepared['configurationPath'] = str(snapshot)
                    with patch.object(agent, 'protected', side_effect=lambda path: path):
                        self.assertEqual(prepared, agent.pending_preparation({'bootstrap': prepared}, observed))

    def test_java_observation_is_emitted_only_after_the_verified_replacement_process(self):
        state = {'phase': 'intent', 'dispatchedAt': 99, 'plan': {
            'operationId': 'a'*32, 'version': '1.1.0', 'digest': 'b'*64, 'instance': 'old:10'}}
        observed = {'instance': 'new:2000'}
        with patch.object(agent, 'process', return_value=(observed, 2, [])), \
             patch.object(agent, 'require_runtime_mount'), \
             patch.object(agent, 'installed_files', return_value=('b'*64, 'argfile', 'same', 'same')), \
             patch.object(agent.Path, 'read_text', return_value='btime 100\n'), \
             patch.object(agent.os, 'sysconf', return_value=100), \
             patch.object(agent.time, 'time', return_value=180):
            result = agent.status(Path('/fixed'), '/runtime', {}, {'operationId': 'a'*32}, {}, state)
            self.assertEqual(120, result['processStartedAt'])
            self.assertEqual(180, result['javaObservedAt'])
            self.assertEqual('restartObserved', result['stage'])
            observed['instance'] = 'old:10'
            self.assertNotIn('javaObservedAt', agent.status(Path('/fixed'), '/runtime', {}, {'operationId': 'a'*32}, {}, state))


if __name__ == '__main__':
    unittest.main()
