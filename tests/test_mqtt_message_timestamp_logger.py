import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import types
import unittest

import paho.mqtt.publish as publish

import mqtt_message_timestamp_logger.mqtt_message_timestamp_logger as mmtl
from mqtt_message_timestamp_logger.mqtt_message_timestamp_logger import on_message, init_DB, commit_pending, \
    read_password, setup_parser

MOSQUITTO = shutil.which('mosquitto') or next(
    (path for path in ('/usr/sbin/mosquitto', '/usr/bin/mosquitto') if os.path.exists(path)), None)
MOSQUITTO_PASSWD = shutil.which('mosquitto_passwd')
OPENSSL = shutil.which('openssl')

needs_mosquitto = unittest.skipUnless(MOSQUITTO, 'mosquitto is not installed')


def src_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(mmtl.__file__)))


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def last_seen(db_filename, topic):
    con = sqlite3.connect(db_filename, timeout=10)
    try:
        res = con.execute('SELECT timestamp FROM topic_last_seen WHERE topic=?', (topic,)).fetchone()
    finally:
        con.close()
    return res[0] if res else None


class Broker:
    """A mosquitto on a free local port, configured for one test."""

    def __init__(self, directory, config=''):
        self.port = free_port()
        self.config_file = os.path.join(directory, f'mosquitto-{self.port}.conf')
        with open(self.config_file, 'w') as handle:
            handle.write(f'listener {self.port} 127.0.0.1\n' + (config or 'allow_anonymous true\n'))
        self.process = None

    def start(self):
        self.process = subprocess.Popen([MOSQUITTO, '-c', self.config_file],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f'mosquitto failed: {self.process.stderr.read().decode()}')
            try:
                socket.create_connection(('127.0.0.1', self.port), timeout=1).close()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError('mosquitto did not start')

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait(10)
            self.process.stderr.close()


class TestOnMessage(unittest.TestCase):
    """on_message and commit_pending, without a broker."""

    def setUp(self):
        self.tempfolder = tempfile.mkdtemp()
        self.db_filename = os.path.join(self.tempfolder, 'unittest.db')
        init_DB(self.db_filename)
        self.con = sqlite3.connect(self.db_filename, timeout=0.1)

    def tearDown(self):
        self.con.close()
        shutil.rmtree(self.tempfolder)
        mmtl.shared_dict, mmtl.shared_dict_times, mmtl.shared_diff_dict = {}, {}, {}

    @staticmethod
    def message(topic):
        return types.SimpleNamespace(topic=topic, payload=b'1', qos=0, retain=False)

    def test_wal_mode(self):
        self.assertEqual('wal', self.con.execute('PRAGMA journal_mode').fetchone()[0])

    def test_immediate_commit(self):
        userdata = {'sqlite_con': self.con, 'immediate_commit': True, 'history_retention_duration': 200}
        before = time.time()
        for _ in range(3):
            on_message(None, userdata, self.message('test_float'))
        self.assertGreaterEqual(last_seen(self.db_filename, 'test_float'), before)
        self.assertIsNone(last_seen(self.db_filename, 'test_float1'))
        count, = self.con.execute('SELECT COUNT(*) FROM topic_receive_times').fetchone()
        self.assertEqual(3, count)
        self.assertIsNotNone(self.con.execute('SELECT timestamp FROM topic_last_interval').fetchone())

    def test_batched_commit(self):
        userdata = {'immediate_commit': False, 'history_retention_duration': 200}
        on_message(None, userdata, self.message('a'))
        on_message(None, userdata, self.message('a'))
        on_message(None, userdata, self.message('b'))
        self.assertIsNone(last_seen(self.db_filename, 'a'))
        commit_pending(self.con, 200)
        self.assertIsNotNone(last_seen(self.db_filename, 'a'))
        self.assertIsNotNone(last_seen(self.db_filename, 'b'))
        count, = self.con.execute('SELECT COUNT(*) FROM topic_receive_times').fetchone()
        self.assertEqual(3, count)

    def test_locked_database_keeps_messages(self):
        userdata = {'immediate_commit': False, 'history_retention_duration': 200}
        on_message(None, userdata, self.message('a'))
        blocker = sqlite3.connect(self.db_filename)
        blocker.execute('BEGIN IMMEDIATE')
        with self.assertRaises(sqlite3.OperationalError):
            commit_pending(self.con, 200)
        blocker.rollback()
        blocker.close()
        commit_pending(self.con, 200)
        self.assertIsNotNone(last_seen(self.db_filename, 'a'))

    def test_history_retention(self):
        self.con.execute('INSERT INTO topic_receive_times VALUES (?, ?)', ('old', time.time() - 1000))
        self.con.commit()
        commit_pending(self.con, 200)
        count, = self.con.execute('SELECT COUNT(*) FROM topic_receive_times').fetchone()
        self.assertEqual(0, count)


class TestArguments(unittest.TestCase):

    def setUp(self):
        self.tempfolder = tempfile.mkdtemp()
        self.password_file = os.path.join(self.tempfolder, 'password')
        with open(self.password_file, 'w') as handle:
            handle.write('from-file\n')
        self.environ = dict(os.environ)
        os.environ.pop('MQTT_PASSWORD', None)
        os.environ.pop('MQTT_PASSWORD_FILE', None)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.environ)
        shutil.rmtree(self.tempfolder)

    def password(self, *argv):
        return read_password(setup_parser().parse_args(['--db-filename', 'x'] + list(argv)))

    def test_password_sources(self):
        self.assertIsNone(self.password())
        self.assertEqual('from-file', self.password('--mqtt-password-file', self.password_file))
        self.assertEqual('given', self.password('--mqtt-password', 'given'))
        os.environ['MQTT_PASSWORD'] = 'from-env'
        self.assertEqual('from-env', self.password())
        os.environ['MQTT_PASSWORD_FILE'] = self.password_file
        self.assertEqual('from-file', self.password())

    def test_commit_interval_zero_is_accepted(self):
        args = setup_parser().parse_args(['--db-filename', 'x', '--commit-interval', '0'])
        self.assertEqual(0, args.commit_interval)


@needs_mosquitto
class TestDaemon(unittest.TestCase):
    """The daemon as it runs in production, against a real mosquitto."""

    def setUp(self):
        self.tempfolder = tempfile.mkdtemp()
        self.db_filename = os.path.join(self.tempfolder, 'unittest.db')
        self.log_file = os.path.join(self.tempfolder, 'daemon.log')
        self.brokers = []
        self.daemon = None

    def tearDown(self):
        if self.daemon and self.daemon.poll() is None:
            self.daemon.kill()
            self.daemon.wait()
        for broker in self.brokers:
            broker.stop()
        shutil.rmtree(self.tempfolder)

    def broker(self, config=''):
        broker = Broker(self.tempfolder, config)
        self.brokers.append(broker)
        return broker

    def start_daemon(self, broker, *args):
        env = dict(os.environ, PYTHONPATH=src_dir())
        with open(self.log_file, 'a') as log:
            self.daemon = subprocess.Popen(
                [sys.executable, '-W', 'error:Callback API version 1 is deprecated',
                 '-m', 'mqtt_message_timestamp_logger',
                 '--db-filename', self.db_filename, '--mqtt-broker-port', str(broker.port)] + list(args),
                stdout=subprocess.DEVNULL, stderr=log, env=env)

    def daemon_log(self):
        with open(self.log_file) as handle:
            return handle.read()

    def publish_until_logged(self, broker, topic, timeout=30, hostname='127.0.0.1', **kwargs):
        """Publishes until the daemon has recorded the topic; it may still be (re)connecting."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                publish.single(topic, '1', hostname=hostname, port=broker.port, **kwargs)
            except OSError:
                pass
            time.sleep(0.5)
            if os.path.exists(self.db_filename) and last_seen(self.db_filename, topic):
                return
        self.fail(f'{topic} was not logged. Daemon log:\n{self.daemon_log()}')

    def stop_daemon(self):
        self.daemon.send_signal(signal.SIGTERM)
        self.assertEqual(0, self.daemon.wait(20), self.daemon_log())

    def test_reconnect_resubscribes(self):
        broker = self.broker()
        # the broker is down at startup: the daemon has to keep trying
        self.start_daemon(broker, '--commit-interval', '0.2')
        time.sleep(1.5)
        self.assertIsNone(self.daemon.poll(), self.daemon_log())
        broker.start()
        self.publish_until_logged(broker, 'home/before_restart')

        broker.stop()
        time.sleep(1)
        broker.start()
        self.publish_until_logged(broker, 'home/after_restart')
        self.stop_daemon()

    def test_stop_commits_pending_messages(self):
        broker = self.broker()
        broker.start()
        self.start_daemon(broker, '--commit-interval', '0.2', '--mqtt-topic', 'probe/#')
        self.publish_until_logged(broker, 'probe/x')
        self.stop_daemon()

        # a long commit interval: only the final commit on SIGTERM can write the message
        self.start_daemon(broker, '--commit-interval', '3600', '--mqtt-topic', 'late')
        time.sleep(2)
        publish.single('late', '1', hostname='127.0.0.1', port=broker.port)
        time.sleep(1)
        self.assertIsNone(last_seen(self.db_filename, 'late'))
        self.stop_daemon()
        self.assertIsNotNone(last_seen(self.db_filename, 'late'), self.daemon_log())

    def test_immediate_commit(self):
        broker = self.broker()
        broker.start()
        self.start_daemon(broker, '--commit-interval', '0')
        self.publish_until_logged(broker, 'immediate')
        self.stop_daemon()

    @unittest.skipUnless(MOSQUITTO_PASSWD, 'mosquitto_passwd is not installed')
    def test_password_file(self):
        passwd = os.path.join(self.tempfolder, 'passwd')
        subprocess.run([MOSQUITTO_PASSWD, '-c', '-b', passwd, 'logger', 's3cret'], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.chmod(passwd, 0o600)
        password_file = os.path.join(self.tempfolder, 'password')
        with open(password_file, 'w') as handle:
            handle.write('s3cret\n')
        broker = self.broker(f'allow_anonymous false\npassword_file {passwd}\n')
        broker.start()
        self.start_daemon(broker, '--commit-interval', '0.2', '--mqtt-username', 'logger',
                          '--mqtt-password-file', password_file)
        self.publish_until_logged(broker, 'authenticated', auth={'username': 'logger', 'password': 's3cret'})
        self.stop_daemon()

    @unittest.skipUnless(OPENSSL, 'openssl is not installed')
    def test_tls(self):
        cert = os.path.join(self.tempfolder, 'cert.pem')
        key = os.path.join(self.tempfolder, 'key.pem')
        subprocess.run([OPENSSL, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
                        '-keyout', key, '-out', cert], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        broker = self.broker(f'allow_anonymous true\ncertfile {cert}\nkeyfile {key}\n')
        broker.start()
        self.start_daemon(broker, '--commit-interval', '0.2', '--mqtt-broker', 'localhost', '--ca-cert', cert)
        self.publish_until_logged(broker, 'encrypted', hostname='localhost', tls={'ca_certs': cert})
        self.stop_daemon()


if __name__ == '__main__':
    unittest.main()
