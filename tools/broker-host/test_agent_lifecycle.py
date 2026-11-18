import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('agent_lifecycle', Path(__file__).with_name('agent-lifecycle.py'))
agent = importlib.util.module_from_spec(spec)
spec.loader.exec_module(agent)


class LifecycleTests(unittest.TestCase):
    def test_lost_restart_response_does_not_dispatch_twice(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            now = int(time.time())
            request = {
                'action': 'apply', 'operationId': 'a' * 32, 'targetId': 'broker-2', 'brokerId': 2,
                'kafkaClusterId': 'cluster', 'instance': '4:100', 'currentDigest': None,
                'version': '1.1.0', 'digest': 'a' * 64, 'launchMode': 'argfile', 'applyUntil': now + 120,
            }
            service = {'adapter': 'systemd'}
            settings = {'maintenanceFrom': now - 60, 'maintenanceTo': now + 180}
            identity = {'instance': '4:100', 'adapter': 'systemd'}
            visible = [identity, 4, ['/usr/bin/systemctl', 'restart', 'kafka.service']]

            def files(*_args):
                jar = directory / agent.JAR
                args = directory / agent.ARGS
                return ('a' * 64 if jar.exists() else None, 'argfile', args.read_text().strip() if args.exists() else '', 'arg')

            with patch.object(agent, 'scope', return_value=(settings, service, directory, '/opt/kafdeck')), \
                 patch.object(agent, 'read_json', side_effect=lambda path: json.loads(path.read_text())), \
                 patch.object(agent, 'admitted_package', return_value=b'agent'), \
                 patch.object(agent, 'process', return_value=visible), \
                 patch.object(agent, 'installed_files', side_effect=files), \
                 patch.object(agent.subprocess, 'run', side_effect=subprocess.TimeoutExpired('restart', 60)) as restart:
                first = agent.handle({}, request, {})
                again = agent.handle({}, request, {})
                self.assertEqual('awaitingRestart', first['stage'])
                self.assertEqual(first, again)
                self.assertEqual(1, restart.call_count)
                self.assertEqual('intent', json.loads((directory / agent.STATE).read_text())['phase'])
                with self.assertRaisesRegex(agent.Rejected, 'agent_previous_operation_pending'):
                    agent.handle({}, {**request, 'operationId': 'b' * 32}, {})

    def test_outside_window_leaves_broker_and_package_unchanged(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            now = int(time.time())
            request = {
                'action': 'apply', 'operationId': 'a' * 32, 'targetId': 'broker-2', 'brokerId': 2,
                'kafkaClusterId': 'cluster', 'instance': '4:100', 'currentDigest': None,
                'version': '1.1.0', 'digest': 'a' * 64, 'launchMode': 'argfile', 'applyUntil': now - 1,
            }
            with patch.object(agent, 'scope', return_value=({'maintenanceFrom':now-60,'maintenanceTo':now-1}, {}, directory, '/opt/kafdeck')), \
                 patch.object(agent, 'admitted_package', return_value=b'agent'), \
                 patch.object(agent, 'process', return_value=({'instance':'4:100'}, 4, ['restart'])), \
                 patch.object(agent, 'installed_files', return_value=(None, 'argfile', '', 'arg')), \
                 patch.object(agent.subprocess, 'run') as restart:
                with self.assertRaisesRegex(agent.Rejected, 'agent_outside_maintenance'):
                    agent.handle({}, request, {})
                self.assertFalse((directory / agent.JAR).exists())
                self.assertFalse((directory / agent.STATE).exists())
                restart.assert_not_called()
