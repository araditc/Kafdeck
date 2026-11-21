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


if __name__ == '__main__':
    unittest.main()
