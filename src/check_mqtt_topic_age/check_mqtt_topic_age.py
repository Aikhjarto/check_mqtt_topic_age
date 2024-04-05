#!/usr/bin/env python3
import argparse
import os
import sqlite3
import traceback
import datetime
import re
import numpy as np


def regexp(expr, item):
    # https://stackoverflow.com/a/5365533
    reg = re.compile(expr)
    return reg.search(item) is not None


def fromisoformat(s):
    """
    Converts a ISO formatted datetime string to a datetime object.

    Parameters
    ----------
    s : str
        Datetime string in ISO format

    Returns
    -------
    datetime.datetime

    """
    if hasattr(datetime.datetime, 'fromisoformat'):
        return datetime.datetime.fromisoformat(s)
    else:
        # python 3.6 des not have built-in fromisoformat
        # thus, go over datetime64 from numpy
        tmp = np.datetime64(s).item()

        # TODO: np.datetime64 does not store timezone information, thus switch to differen library
        # https://numpy.org/devdocs/reference/arrays.datetime.html

        # np.datetime64 can return datetime.date instead of datetime.datetime
        if type(tmp) is datetime.date:
            tmp = datetime.datetime.combine(tmp, datetime.time.min)
        return tmp


# NAGIOS return codes :
# https://nagios-plugins.org/doc/guidelines.html#AEN78
OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3


def setup_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser('check_mqtt_topic_age')

    parser.add_argument('-c', type=str, required=True,
                        help='If age of the last sample in seconds is higher than `C`, state is CRITICAL')

    parser.add_argument('-w', type=str, required=True,
                        help='If age of the last sample in seconds is higher than `W`, state is WARNING')

    parser.add_argument('--db-filename', type=str, required=True)

    parser.add_argument('--mqtt-topic', type=str, required=True, action='append')

    return parser


def main():
    p = setup_parser()
    args = p.parse_args()
    return run_check(args.db_filename, args.w, args.c, args.mqtt_topic)


def run_check(db_filename, w, c, topics):
    con = sqlite3.connect(db_filename)
    con.create_function("REGEXP", 2, regexp)

    timedelta_warning = abs(datetime.timedelta(seconds=float(w)))
    timedelta_critical = abs(datetime.timedelta(seconds=float(c)))

    if timedelta_warning >= timedelta_critical:
        return UNKNOWN, 'UNKNOWN - warning age must be smaller than critical age'

    if not os.path.isfile(db_filename):
        return UNKNOWN, f'UNKNOWN - File {db_filename} does not exist'
    else:
        if not os.access(db_filename, os.R_OK):
            return UNKNOWN, f'UNKNOWN - File {db_filename} is not readable'

    timestamp = datetime.datetime.min
    for topic in topics:
        try:
            if topic.find('+') >= 0 or topic.find('#') >= 0:
                pattern = topic.replace('+', '[^/]+').replace('#', '.*')
                cur = con.cursor()
                cur.execute(f'SELECT MAX("timestamp") from "topic_last_seen" WHERE topic REGEXP ?', (pattern,))
                res = cur.fetchone()
                if not res[0]:
                    return UNKNOWN, f'UNKNOWN - No data found for pattern {topic} in file {db_filename}'
                else:
                    timestamp = max(timestamp, datetime.datetime.fromtimestamp((res[-1])))
            else:
                cur = con.cursor()
                cur.execute('SELECT "timestamp" from "topic_last_seen" WHERE topic=?', (topic,))
                res = cur.fetchone()
                if not res:
                    return UNKNOWN, f'UNKNOWN - No data found for topic {topic} in file {db_filename}'
                else:
                    timestamp = max(timestamp, fromisoformat(res[-1]))

        except Exception as e:
            return UNKNOWN,  f'UNKNOWN - {e} {db_filename}, {traceback.format_exc()}'

    # https://assets.nagios.com/downloads/nagioscore/docs/nagioscore/3/en/pluginapi.html
    # noinspection PyPep8Naming
    SERVICEPERFDATA = f'|age={(datetime.datetime.now()-timestamp).seconds}s;' \
                      f'{timedelta_warning.seconds};{timedelta_critical.seconds}'
    # noinspection PyPep8Naming
    SERVICEOUTPUT = f'Last sensor update: {timestamp}'
    if timestamp < datetime.datetime.now()-abs(timedelta_critical):
        return CRITICAL, f'CRITICAL - {SERVICEOUTPUT}{SERVICEPERFDATA}'

    elif timestamp < datetime.datetime.now()-abs(timedelta_warning):
        return WARNING, f'WARNING - {SERVICEOUTPUT}{SERVICEPERFDATA}'
    else:
        return OK, f'OK - {SERVICEOUTPUT}{SERVICEPERFDATA}'


if __name__ == '__main__':
    return_code, message = main()
    print(message)
    exit(return_code)
