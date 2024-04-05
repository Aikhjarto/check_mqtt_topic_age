import datetime
import unittest
import subprocess
import shutil
import os
import time
import paho.mqtt.client as mqtt
import json
import threading
import tempfile
import sqlite3
from mqtt_message_timestamp_logger.mqtt_message_timestamp_logger import on_message, init_DB


# noinspection PyPep8Naming
class LocalMQTTServerMixin:
    mosquitto_process: subprocess.Popen

    @classmethod
    def setUpClass(cls):
        mosquitto_exe = shutil.which('mosquitto')
        if mosquitto_exe is None:
            mosquitto_exe = '/usr/sbin/mosquitto'

        mosquitto_conf_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                           'mosquitto.conf')
        cls.mosquitto_process = subprocess.Popen([mosquitto_exe, '-v', '-c', mosquitto_conf_file],
                                                 stdin=subprocess.PIPE,
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE)

    @classmethod
    def get_client(cls, client_id=None):
        # some versions of paho-mqtt do not have CallbackAPIVersion, some version require it to be set
        if hasattr(mqtt, "CallbackAPIVersion"):
            # noinspection PyArgumentList
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        else:
            client = mqtt.Client(client_id=client_id)
        start_time = time.time()
        res = None
        while time.time() < (start_time + 60):
            try:
                res = client.connect(host='localhost', port=1884)
            except ConnectionRefusedError:
                pass
            if res == mqtt.MQTT_ERR_SUCCESS:
                break
            cls._check_mosquitto_process()

        if res != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f'MQTT connection error {res}')

        return client

    @classmethod
    def _check_mosquitto_process(cls):
        ret = cls.mosquitto_process.poll()
        if ret is not None:
            stdout = cls.mosquitto_process.stdout.read()
            stderr = cls.mosquitto_process.stderr.read()
            raise RuntimeError(f'mosquitto stopped with error code {ret}\n'
                               f'stdout={stdout.decode()}\n'
                               f'stderr={stderr.decode()}\n')

    def setUp(self):
        if hasattr(super(), 'setUp'):
            super().setUp()

        # self._check_mosquitto_process()

    @classmethod
    def tearDownClass(cls):
        cls.mosquitto_process.terminate()
        stdout = cls.mosquitto_process.stdout.read()
        stderr = cls.mosquitto_process.stderr.read()
        print(f'mosquitto stopped\n'
              f'stdout={stdout.decode()}\n'
              f'stderr={stderr.decode()}\n')


def publish_function(client: mqtt.Client):

    publish_kwargs = {'qos': 2}
    client.publish('test_float', 1.0, **publish_kwargs)
    client.publish('test_float2', 1.0, **publish_kwargs)
    time.sleep(1)
    client.publish('test_float', 1.0, **publish_kwargs)
    time.sleep(1)
    client.publish('test_float', 1.0, **publish_kwargs)
    time.sleep(1)
    client.publish('test_float', 1.0, **publish_kwargs)


class TestMQTTSubscriber(LocalMQTTServerMixin, unittest.TestCase):
    tempfolder: str
    db_filename: str
    con: sqlite3.Connection

    @classmethod
    def setUpClass(cls):

        super().setUpClass()

        cls.tempfolder = tempfile.mkdtemp()
        cls.db_filename = os.path.join(cls.tempfolder, 'unittest.db')
        cls.now = datetime.datetime.now()
        init_DB(cls.db_filename)

        client = cls.get_client(client_id='unittest')

        # subscribe to all topics
        res, mid = client.subscribe('#', qos=2)
        if res != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError

        cls.con = sqlite3.connect(cls.db_filename)
        # setup message handler
        client.user_data_set({'sqlite_con': cls.con,
                              'commit_interval': 1,
                              'history_retention_duration': 200}
                             )
        client.on_message = on_message

        # start publishing in background
        thd = threading.Thread(target=publish_function,
                               args=(client,))
        thd.start()

        # start loop
        start_time = time.time()
        while time.time() < start_time + 5:  # len(db) < 40:
            res = client.loop()
            if res != mqtt.MQTT_ERR_SUCCESS:
                print(f'ret={res}')
                break

        thd.join()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.con.close()
        shutil.rmtree(cls.tempfolder)

    def test1(self):
        with self.con:
            cur = self.con.execute('SELECT timestamp from topic_last_seen WHERE topic=?', ('test_float1',))
            res = cur.fetchone()
            self.assertIsNone(res, res)

    def test2(self):
        with self.con:
            cur = self.con.execute('SELECT timestamp from topic_last_seen WHERE topic=?', ('test_float',))
            res = cur.fetchone()
            self.assertIsNotNone(res)
            self.assertGreater(float(res[0]), self.now.timestamp())


if __name__ == '__main__':
    unittest.main()
