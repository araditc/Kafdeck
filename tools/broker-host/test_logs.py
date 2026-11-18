import json
import time
import unittest
from unittest.mock import patch

import probe


class LogReaderTests(unittest.TestCase):
    def setUp(self):
        self.now = int(time.time())
        self.request = {'targetId': 'broker-1', 'brokerId': 1, 'kafkaClusterId': 'cluster',
                        'from': self.now - 20, 'to': self.now + 1}
        self.target = {'id': 'broker-1', 'logs': True}
        self.observed = {'status': 'verified', 'brokerId': 1, 'kafkaClusterId': 'cluster',
                         'adapter': 'systemd', 'unit': 'kafka-1.service', 'instance': '42:99'}

    def test_unapproved_or_wrong_identity_never_reads_logs(self):
        with patch.object(probe, 'capture') as capture, patch.object(probe, 'observe', return_value=self.observed):
            with self.assertRaisesRegex(ValueError, 'log_source_not_approved'):
                probe.read_logs([{'id': 'broker-1'}], self.request)
            with self.assertRaisesRegex(ValueError, 'log_identity_mismatch'):
                probe.read_logs([self.target], dict(self.request, brokerId=2))
            capture.assert_not_called()

    def test_systemd_uses_exact_journal_unit_and_window(self):
        def item(seconds, message):
            return json.dumps({'__REALTIME_TIMESTAMP': str(seconds * 1000000), 'MESSAGE': message, 'OTHER': 'omit-me'})
        output = '\n'.join([item(self.now, 'expected'), item(self.now - 21, 'outside'), item(self.now + 1, 'exclusive')]).encode()
        with patch.object(probe, 'capture', return_value=(output, b'', False)) as capture, patch.object(probe, 'observe', return_value=self.observed):
            result = probe.read_logs([self.target], self.request)
        self.assertEqual([{'timeMs': self.now * 1000, 'text': 'expected'}], result['lines'])
        self.assertIn('_SYSTEMD_UNIT=kafka-1.service', capture.call_args.args[0])
        self.assertIn('--lines=201', capture.call_args.args[0])

    def test_compose_uses_verified_container_and_both_log_streams(self):
        observed = dict(self.observed, adapter='compose', instance='1' * 64 + ':99')
        stamp = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(self.now)).encode() + b'.012345678Z '
        with patch.object(probe, 'capture', return_value=(stamp + b'stdout\n', b'cli noise\n' + stamp + b'stderr\n', False)) as capture, patch.object(probe, 'observe', return_value=observed):
            result = probe.read_logs([self.target], self.request)
        self.assertEqual(['stdout', 'stderr'], [line['text'] for line in result['lines']])
        self.assertEqual('1' * 64, capture.call_args.args[0][-1])
        self.assertEqual(self.now * 1000 + 12, result['lines'][0]['timeMs'])

    def test_service_replacement_discards_data(self):
        with patch.object(probe, 'capture', return_value=(b'', b'', False)), patch.object(
                probe, 'observe', side_effect=[self.observed, dict(self.observed, instance='43:100')]):
            with self.assertRaisesRegex(ValueError, 'log_identity_changed'):
                probe.read_logs([self.target], self.request)

    def test_bounds_reject_wide_windows_and_cap_serialized_rows(self):
        with patch.object(probe, 'observe') as observe:
            with self.assertRaisesRegex(ValueError, 'invalid_request'):
                probe.read_logs([self.target], dict(self.request, to=self.now + 3601))
            observe.assert_not_called()
        lines, truncated = probe.bounded_lines([(self.now * 1000, 'one')] * 201, self.now - 1, self.now + 1, False)
        self.assertEqual(200, len(lines))
        self.assertTrue(truncated)
        lines, truncated = probe.bounded_lines([(self.now * 1000, 'ش' * 1000)] * 200, self.now - 1, self.now + 1, False)
        self.assertLess(len(json.dumps(lines).encode()), 48 * 1024)
        self.assertTrue(truncated)
