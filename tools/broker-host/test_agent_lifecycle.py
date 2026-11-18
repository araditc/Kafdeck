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
                'version': '1.1.0', 'digest': 'a' * 64, 'launchMode': 'argfile', 'applyUntil': now + 120, 'expectRestart': True,
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
                'version': '1.1.0', 'digest': 'a' * 64, 'launchMode': 'argfile', 'applyUntil': now - 1, 'expectRestart': True,
            }
            with patch.object(agent, 'scope', return_value=({'maintenanceFrom':now-60,'maintenanceTo':now-1}, {}, directory, '/opt/kafdeck')), \
                 patch.object(agent, 'admitted_package', return_value=b'agent'), \
                 patch.object(agent, 'process', return_value=({'instance':'4:100','adapter':'systemd'}, 4, ['restart'])), \
                 patch.object(agent, 'installed_files', return_value=(None, 'argfile', '', 'arg')), \
                 patch.object(agent.subprocess, 'run') as restart:
                with self.assertRaisesRegex(agent.Rejected, 'agent_outside_maintenance'):
                    agent.handle({}, request, {})
                self.assertFalse((directory / agent.JAR).exists())
                self.assertFalse((directory / agent.STATE).exists())
                restart.assert_not_called()

    def test_compose_requires_a_read_only_directory_mount(self):
        observed = {'adapter': 'compose', 'instance': 'a' * 64 + ':10'}
        call = {'command': lambda _args: json.dumps([{'Destination':'/opt/kafdeck', 'Type':'bind', 'RW':True}])}
        with self.assertRaisesRegex(agent.Rejected, 'agent_runtime_mount_not_read_only'):
            agent.require_runtime_mount(observed, '/opt/kafdeck', call)
        accepted = {'command': lambda _args: json.dumps([{'Destination':'/opt/kafdeck', 'Type':'bind', 'RW':False}])}
        agent.require_runtime_mount(observed, '/opt/kafdeck', accepted)

    def test_expired_publication_is_resumed_without_republishing_or_restarting_twice(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            now = int(time.time())
            request = {'action':'apply', 'operationId':'a'*32, 'targetId':'broker-2', 'brokerId':2,
                       'kafkaClusterId':'cluster', 'instance':'4:100', 'currentDigest':'b'*64,
                       'version':'1.1.0', 'digest':'a'*64, 'launchMode':'argfile',
                       'applyUntil':now-1, 'expectRestart':True}
            agent.state_write(directory, agent.journal_plan(request), 'publishing')
            with patch.object(agent, 'scope', return_value=({'maintenanceFrom':now-60,'maintenanceTo':now+180}, {}, directory, '/opt/kafdeck')), \
                 patch.object(agent, 'read_json', side_effect=lambda path: json.loads(path.read_text())), \
                 patch.object(agent, 'admitted_package', return_value=b'agent'), \
                 patch.object(agent, 'process', return_value=({'instance':'4:100','adapter':'systemd'}, 4, ['restart'])), \
                 patch.object(agent, 'installed_files', return_value=('a'*64, 'argfile', 'arg', 'arg')), \
                 patch.object(agent, 'atomic', wraps=agent.atomic) as write, patch.object(agent.subprocess, 'run') as restart:
                with self.assertRaisesRegex(agent.Rejected, 'agent_outside_maintenance'):
                    agent.handle({}, request, {})
                preview = agent.handle({'packages':{}}, {**request, 'action':'preview'}, {})
                self.assertTrue(preview['restartRequired'])
                self.assertEqual('b'*64, preview['currentDigest'])
                renewed = {**request, 'applyUntil':now+120}
                self.assertEqual('awaitingRestart', agent.handle({}, renewed, {})['stage'])
                self.assertEqual('awaitingRestart', agent.handle({}, renewed, {})['stage'])
                self.assertEqual([directory / agent.STATE], [call.args[0] for call in write.call_args_list])
                restart.assert_called_once()

    def test_installed_package_needs_no_maintenance_but_cannot_grant_a_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            now = int(time.time())
            request = {'action':'apply', 'operationId':'a'*32, 'targetId':'broker-2', 'brokerId':2,
                       'kafkaClusterId':'cluster', 'instance':'4:100', 'currentDigest':'a'*64,
                       'version':'1.1.0', 'digest':'a'*64, 'launchMode':'argfile',
                       'applyUntil':now+120, 'expectRestart':False}
            with patch.object(agent, 'scope', return_value=({'maintenanceFrom':now-60,'maintenanceTo':now-1}, {}, directory, '/opt/kafdeck')), \
                 patch.object(agent, 'admitted_package', return_value=b'agent'), \
                 patch.object(agent, 'process', return_value=({'instance':'4:100','adapter':'systemd'}, 4, ['restart'])), \
                 patch.object(agent, 'installed_files', side_effect=[('a'*64,'argfile','arg','arg'), ('a'*64,'argfile','','arg')]), \
                 patch.object(agent, 'atomic') as write, patch.object(agent.subprocess, 'run') as restart:
                self.assertEqual('alreadyInstalled', agent.handle({}, request, {})['stage'])
                with self.assertRaisesRegex(agent.Rejected, 'agent_preview_stale'):
                    agent.handle({}, request, {})
                write.assert_not_called()
                restart.assert_not_called()
