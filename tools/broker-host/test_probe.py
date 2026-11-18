import importlib.util
from pathlib import Path
import unittest
import tempfile
import os
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('probe', Path(__file__).with_name('probe.py'))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class ProbeTests(unittest.TestCase):
    def test_identity_parser_ignores_credentials_and_rejects_ambiguous_identity(self):
        self.assertEqual({'node.id': '1'}, probe.properties('node.id=1\nsasl.jaas.config=secret\\value'))
        self.assertEqual({}, probe.properties('sasl.jaas.config=ignored\\\n node.id=2'))
        for text in ['node.id=1\nnode.id=2', 'log.dirs=/data\\path', 'node.id: 1']:
            with self.assertRaises(ValueError): probe.properties(text)

    def test_systemd_uses_only_fixed_queries_and_rejects_process_change(self):
        target = {'id':'native', 'adapter':'systemd', 'unit':'kafka.service'}
        with patch.object(probe, 'command', side_effect=['active', '10', 'active', '11']), patch.object(probe, 'process_identity', return_value=(1, 'cluster', '42')):
            self.assertEqual('identity_unavailable', probe.observe(target)['status'])

    def test_compose_requires_exactly_one_running_service(self):
        with patch.object(probe, 'command', return_value='a'*64+'\n'+'b'*64):
            result=probe.observe({'id':'compose','adapter':'compose','project':'kafka','service':'broker'})
        self.assertEqual('identity_unavailable', result['status'])
        self.assertIsNone(result['brokerId'])

    def test_invalid_service_never_dispatches_a_command(self):
        with patch.object(probe, 'command') as command:
            result=probe.observe({'id':'native','adapter':'systemd','unit':'kafka.service;id'})
        command.assert_not_called()
        self.assertEqual('identity_unavailable', result['status'])

    def test_data_identity_must_match_the_running_process_configuration(self):
        stat='10 (java) S '+' '.join(['0']*18+['42'])
        files={'/proc/10/stat':stat, '/proc/10/cmdline':'java\0kafka.Kafka\0/etc/kafka/server.properties\0',
               '/proc/10/root/etc/kafka/server.properties':'node.id=1\nlog.dirs=/data',
               '/proc/10/root/data/meta.properties':'node.id=2\ncluster.id=wrong'}
        with patch.object(probe, 'small_file', side_effect=lambda path: files[path.as_posix()]), patch.object(probe, 'rooted_properties', side_effect=lambda root, path: probe.properties(files[(root / path.lstrip('/')).as_posix()])):
            with self.assertRaisesRegex(ValueError, 'data_identity_mismatch'):probe.process_identity(10)

    @unittest.skipUnless(hasattr(os, 'O_NOFOLLOW'), 'POSIX filesystem boundary')
    def test_namespace_reader_rejects_symlinks_and_non_regular_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'good').write_text('node.id=1')
            self.assertEqual({'node.id':'1'},probe.rooted_properties(root,'/good'))
            (root/'link').symlink_to(root/'good')
            with self.assertRaises(OSError):probe.rooted_properties(root,'/link')
            os.mkfifo(root/'fifo')
            with self.assertRaises(ValueError):probe.rooted_properties(root,'/fifo')

    @unittest.skipUnless(Path('/usr/bin/python3').exists(), 'POSIX command pipe bound')
    def test_command_output_is_stopped_at_bound(self):
        with self.assertRaisesRegex(ValueError, 'output_limit'):
            probe.command(['/usr/bin/python3','-c','import sys;sys.stdout.write("x"*1000000)'])


if __name__ == '__main__':unittest.main()
