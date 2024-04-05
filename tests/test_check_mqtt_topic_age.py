import datetime
import shutil
import sqlite3
import tempfile
import unittest
import os
from datetime import datetime, timedelta
from mqtt_message_timestamp_logger.mqtt_message_timestamp_logger import init_DB
from check_mqtt_topic_age.check_mqtt_topic_age import run_check, OK, WARNING, CRITICAL, UNKNOWN


class Test(unittest.TestCase):

    def setUp(self):
        self.tempfolder = tempfile.mkdtemp()
        self.db_filename = os.path.join(self.tempfolder, 'test.db')
        init_DB(self.db_filename)

        self.now = datetime.now()
        con = sqlite3.connect(self.db_filename)

        topics = ('test1', 'test2', 'test3')

        with con:
            for i, topic in enumerate(topics):
                con.execute('INSERT OR REPLACE INTO topic_last_seen VALUES (?, ?)',
                            (topic, self.now - timedelta(seconds=3600*i)))

    def test_warn_crit_limits(self):
        self.assertEqual(UNKNOWN, run_check(self.db_filename, 30, 10, [])[0])

    def test_ok(self):
        return_code, message = run_check(self.db_filename, 3000, 6000, ['test1'])
        self.assertEqual(OK, return_code, message)

    def test_warning(self):
        return_code, message = run_check(self.db_filename, 3000, 6000, ['test2'])
        self.assertEqual(WARNING, return_code, message)

    def test_critical(self):
        return_code, message = run_check(self.db_filename, 3000, 6000, ['test3'])
        self.assertEqual(CRITICAL, return_code, message)

    def tearDown(self):
        shutil.rmtree(self.tempfolder)


if __name__ == '__main__':
    unittest.main()
