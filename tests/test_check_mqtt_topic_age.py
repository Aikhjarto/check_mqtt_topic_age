import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

import check_mqtt_topic_age
from mqtt_message_timestamp_logger.mqtt_message_timestamp_logger import init_DB
from check_mqtt_topic_age.check_mqtt_topic_age import run_check, Range, format_duration, topic_matches, \
    PluginError, OK, WARNING, CRITICAL, UNKNOWN

NOW = 1_800_000_000.0

AGES = {
    'test1': 0,
    'test2': 3600,
    'test3': 7200,
    'home/a/temp': 30,
    'home/b/temp': 2 * 86400,
    'home/a/temp/raw': 1,
    'xhome/c/temp': 1,
    'a.b': 1,
    '$SYS/broker/uptime': 1,
}


def src_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(check_mqtt_topic_age.__file__)))


class Test(unittest.TestCase):

    def setUp(self):
        self.tempfolder = tempfile.mkdtemp()
        self.db_filename = os.path.join(self.tempfolder, 'test.db')
        init_DB(self.db_filename)

        con = sqlite3.connect(self.db_filename)
        with con:
            for topic, age in AGES.items():
                con.execute('INSERT OR REPLACE INTO topic_last_seen VALUES (?, ?)', (topic, NOW - age))
            # test1 got 5 messages within the last hour and 2 before
            for age in (10, 20, 30, 40, 50, 4000, 5000):
                con.execute('INSERT INTO topic_receive_times VALUES (?, ?)', ('test1', NOW - age))
            con.execute('INSERT INTO topic_receive_times VALUES (?, ?)', ('home/a/temp', NOW - 30))
        con.close()

    def tearDown(self):
        shutil.rmtree(self.tempfolder)

    def check(self, warning, critical, topics, **kwargs):
        return_code, message = run_check(self.db_filename, warning, critical, topics, now=NOW, **kwargs)
        self.assertNotIn('\n', message)
        return return_code, message

    def test_ok(self):
        return_code, message = self.check(3000, 6000, ['test1'])
        self.assertEqual(OK, return_code, message)

    def test_warning(self):
        return_code, message = self.check(3000, 6000, ['test2'])
        self.assertEqual(WARNING, return_code, message)

    def test_critical(self):
        return_code, message = self.check(3000, 6000, ['test3'])
        self.assertEqual(CRITICAL, return_code, message)

    def test_output(self):
        return_code, message = self.check('50m', '100m', ['test2'])
        self.assertEqual(WARNING, return_code, message)
        self.assertRegex(message, r'^MQTT TOPIC AGE WARNING - last message on test2 1h ago \(\d{4}-\d\d-\d\d '
                                  r'\d\d:\d\d:\d\d\) \| age=3600s;3000;6000$')

    def test_age_over_a_day_does_not_wrap(self):
        return_code, message = self.check(60, 300, ['home/b/temp'])
        self.assertEqual(CRITICAL, return_code, message)
        self.assertIn('2d ago', message)
        self.assertTrue(message.endswith('| age=172800s;60;300'), message)

    def test_thresholds_over_a_day_in_perfdata(self):
        return_code, message = self.check('1d', '2d', ['test1'])
        self.assertEqual(OK, return_code, message)
        self.assertTrue(message.endswith('| age=0s;86400;172800'), message)

    def test_each_topic_is_checked(self):
        # a live topic must not hide a dead one
        return_code, message = self.check(3000, 6000, ['test3', 'test1'])
        self.assertEqual(CRITICAL, return_code, message)
        self.assertIn('1 of 2 topics: test3 2h ago (CRITICAL)', message)
        self.assertIn("'test3'=7200s;3000;6000", message)
        self.assertIn("'test1'=0s;3000;6000", message)

    def test_several_topics_ok(self):
        return_code, message = self.check(3000, 6000, ['test1', 'home/a/temp'])
        self.assertEqual(OK, return_code, message)
        self.assertIn('2 topics, oldest home/a/temp 30s ago', message)

    def test_worst_state_wins(self):
        return_code, message = self.check(3000, 6000, ['test1', 'test2', 'test3', 'missing'])
        self.assertEqual(CRITICAL, return_code, message)
        self.assertIn('3 of 4 topics: test3 2h ago (CRITICAL), test2 1h ago (WARNING), missing never seen (UNKNOWN)',
                      message)

    def test_plus_matches_one_whole_level(self):
        # newest of home/a/temp and home/b/temp; neither xhome/c/temp nor home/a/temp/raw match
        return_code, message = self.check(60, 120, ['home/+/temp'])
        self.assertEqual(OK, return_code, message)
        self.assertIn('last message on home/a/temp 30s ago', message)

    def test_per_topic(self):
        return_code, message = self.check(60, 120, ['home/+/temp'], per_topic=True)
        self.assertEqual(CRITICAL, return_code, message)
        self.assertIn('1 of 2 topics: home/b/temp 2d ago (CRITICAL)', message)
        self.assertIn("'home/a/temp'=30s;60;120", message)

    def test_hash(self):
        return_code, message = self.check(60, 120, ['home/#'], per_topic=True)
        perfdata = message.split(' | ')[1].split(' ')
        self.assertEqual(["'home/a/temp'", "'home/a/temp/raw'", "'home/b/temp'"],
                         [field.split('=')[0] for field in perfdata])

    def test_wildcards_skip_dollar_topics(self):
        return_code, message = self.check(60, 120, ['+/broker/uptime'])
        self.assertEqual(UNKNOWN, return_code, message)
        return_code, message = self.check(60, 120, ['$SYS/#'])
        self.assertEqual(OK, return_code, message)

    def test_topic_is_not_a_regex(self):
        con = sqlite3.connect(self.db_filename)
        with con:
            con.execute('DELETE FROM topic_last_seen WHERE topic = ?', ('a.b',))
            con.execute('INSERT INTO topic_last_seen VALUES (?, ?)', ('axb', NOW))
        con.close()
        return_code, message = self.check(60, 120, ['a.b'])
        self.assertEqual(UNKNOWN, return_code, message)

    def test_topic_matches(self):
        self.assertTrue(topic_matches('a/#', 'a'))
        self.assertTrue(topic_matches('a/#', 'a/b/c'))
        self.assertTrue(topic_matches('#', 'a/b'))
        self.assertTrue(topic_matches('+/+', 'a/b'))
        self.assertTrue(topic_matches('a/+/c', 'a//c'))
        self.assertFalse(topic_matches('a/+', 'a'))
        self.assertFalse(topic_matches('a/+', 'a/b/c'))
        self.assertFalse(topic_matches('#', '$SYS/x'))

    def test_invalid_topic_filter(self):
        for topic_filter in ('home/#/temp', 'home/te+', 'home/#x', ''):
            return_code, message = self.check(60, 120, [topic_filter])
            self.assertEqual(UNKNOWN, return_code, message)
            self.assertIn('topic filter', message)

    def test_missing_topic(self):
        return_code, message = self.check(60, 120, ['nothing/here'])
        self.assertEqual(UNKNOWN, return_code, message)
        self.assertEqual('MQTT TOPIC AGE UNKNOWN - no message on nothing/here logged yet', message)

    def test_missing_database_is_not_created(self):
        missing = os.path.join(self.tempfolder, 'missing.db')
        return_code, message = run_check(missing, 60, 120, ['test1'])
        self.assertEqual(UNKNOWN, return_code, message)
        self.assertIn('does not exist', message)
        self.assertFalse(os.path.exists(missing))

    def test_not_a_logger_database(self):
        other = os.path.join(self.tempfolder, 'other.db')
        sqlite3.connect(other).close()
        return_code, message = run_check(other, 60, 120, ['test1'])
        self.assertEqual(UNKNOWN, return_code, message)
        self.assertIn('not a database of mqtt_message_timestamp_logger', message)

    def test_readable_while_the_logger_writes(self):
        writer = sqlite3.connect(self.db_filename)
        writer.execute('BEGIN IMMEDIATE')
        writer.execute('INSERT OR REPLACE INTO topic_last_seen VALUES (?, ?)', ('test1', NOW))
        try:
            return_code, message = self.check(3000, 6000, ['test1'], timeout=1)
            self.assertEqual(OK, return_code, message)
        finally:
            writer.rollback()
            writer.close()

    @unittest.skipIf(hasattr(os, 'geteuid') and os.geteuid() == 0, 'root ignores file permissions')
    def test_readable_without_write_permission(self):
        # the check usually runs as a different user than the logger and may not write the WAL files
        writer = sqlite3.connect(self.db_filename)
        writer.execute('INSERT OR REPLACE INTO topic_last_seen VALUES (?, ?)', ('test1', NOW))
        writer.commit()
        paths = [self.db_filename + suffix for suffix in ('', '-wal', '-shm')]
        try:
            for path in paths:
                if os.path.exists(path):
                    os.chmod(path, 0o444)
            os.chmod(self.tempfolder, 0o555)
            return_code, message = self.check(3000, 6000, ['test1'])
            self.assertEqual(OK, return_code, message)
            writer.close()
            return_code, message = self.check(3000, 6000, ['test1'])
            self.assertEqual(OK, return_code, message)
        finally:
            os.chmod(self.tempfolder, 0o755)
            for path in paths:
                if os.path.exists(path):
                    os.chmod(path, 0o644)
            writer.close()

    def test_invalid_threshold(self):
        for warning in ('abc', '5x', '10:5'):
            return_code, message = self.check(warning, 6000, ['test1'])
            self.assertEqual(UNKNOWN, return_code, message)
            self.assertIn('invalid threshold', message)

    def test_range(self):
        self.assertTrue(Range.parse('60').breached(61))
        self.assertFalse(Range.parse('60').breached(60))
        self.assertTrue(Range.parse('10:').breached(9))
        self.assertTrue(Range.parse('~:10').breached(11))
        self.assertFalse(Range.parse('~:10').breached(-5))
        self.assertTrue(Range.parse('@10:20').breached(15))
        self.assertEqual(Range.parse('5m', durations=True).end, 300)
        self.assertEqual(Range.parse('1.5h', durations=True).end, 5400)
        self.assertEqual(Range.parse('1d:', durations=True).spec, '86400')
        with self.assertRaises(PluginError):
            Range.parse('5m')

    def test_format_duration(self):
        self.assertEqual('0s', format_duration(0))
        self.assertEqual('59s', format_duration(59))
        self.assertEqual('1m 1s', format_duration(61))
        self.assertEqual('1h', format_duration(3600))
        self.assertEqual('2d 1h', format_duration(2 * 86400 + 3600 + 61))
        self.assertEqual('-5s', format_duration(-5))

    def test_count(self):
        return_code, message = self.check('10:', '3:', ['test1'], mode='count')
        self.assertEqual(WARNING, return_code, message)
        self.assertEqual('MQTT TOPIC AGE WARNING - 5 message(s) on test1 in the last 1h | messages=5;10;3;0', message)

    def test_count_window(self):
        return_code, message = self.check('10:', '3:', ['test1'], mode='count', window=25)
        self.assertEqual(CRITICAL, return_code, message)
        self.assertIn('messages=2;', message)

    def test_count_per_topic(self):
        # home/b/temp got no message in the window
        return_code, message = self.check('1:', '1:', ['home/+/temp'], mode='count', per_topic=True)
        self.assertEqual(CRITICAL, return_code, message)
        self.assertIn("'home/a/temp'=1;1;1;0 'home/b/temp'=0;1;1;0", message)

    def test_count_rejects_units(self):
        return_code, message = self.check('5m', '1', ['test1'], mode='count')
        self.assertEqual(UNKNOWN, return_code, message)


class TestCommandLine(unittest.TestCase):
    """The plugin as Nagios runs it: python3 -m check_mqtt_topic_age."""

    def setUp(self):
        self.tempfolder = tempfile.mkdtemp()
        self.db_filename = os.path.join(self.tempfolder, 'test.db')
        init_DB(self.db_filename)
        con = sqlite3.connect(self.db_filename)
        with con:
            con.execute('INSERT INTO topic_last_seen VALUES (?, ?)', ('fresh', time.time()))
            con.execute('INSERT INTO topic_last_seen VALUES (?, ?)', ('dead', time.time() - 2 * 86400))
        con.close()

    def tearDown(self):
        shutil.rmtree(self.tempfolder)

    def run_module(self, *args):
        env = dict(os.environ, PYTHONPATH=src_dir())
        return subprocess.run([sys.executable, '-m', 'check_mqtt_topic_age'] + list(args),
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, universal_newlines=True)

    def test_prints_and_exits(self):
        result = self.run_module('--db-filename', self.db_filename, '-w', '60', '-c', '300', '--mqtt-topic', 'dead')
        self.assertEqual(CRITICAL, result.returncode, result)
        self.assertTrue(result.stdout.startswith('MQTT TOPIC AGE CRITICAL - '), result)
        self.assertEqual(1, len(result.stdout.splitlines()), result)

        result = self.run_module('--db-filename', self.db_filename, '-w', '1m', '-c', '5m', '--mqtt-topic', 'fresh')
        self.assertEqual(OK, result.returncode, result)

    def test_usage_error_is_unknown(self):
        result = self.run_module('--db-filename', self.db_filename, '-c', '300', '--mqtt-topic', 'dead')
        self.assertEqual(UNKNOWN, result.returncode, result)
        result = self.run_module('--db-filename', self.db_filename, '-w', '1', '-c', '2', '--mqtt-topic', 'x',
                                 '--window', 'soon')
        self.assertEqual(UNKNOWN, result.returncode, result)

    def test_version(self):
        result = self.run_module('--version')
        self.assertEqual(0, result.returncode, result)
        self.assertEqual(f'check_mqtt_topic_age {check_mqtt_topic_age.__version__}', result.stdout.strip())


if __name__ == '__main__':
    unittest.main()
