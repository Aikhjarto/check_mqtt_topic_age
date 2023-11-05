#!/usr/bin/env python3
import argparse
import os
import sqlite3
from datetime import datetime, timedelta
import re


def regexp(expr, item):
    # https://stackoverflow.com/a/5365533
    reg = re.compile(expr)
    return reg.search(item) is not None


# NAGIOS return codes :
# https://nagios-plugins.org/doc/guidelines.html#AEN78
OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3


def setup_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser()

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

    con = sqlite3.connect(args.db_filename)
    con.create_function("REGEXP", 2, regexp)

    cur = con.execute("SELECT name FROM sqlite_master")
    tables = cur.fetchone()
    if not tables or 'topic_last_seen' not in tables:
        print(f'UNKNOWN - Cannot find table "topic_last_seen" in {args.db_filename}')
        exit(UNKNOWN)

    timedelta_warning = abs(timedelta(seconds=float(args.w)))
    timedelta_critical = abs(timedelta(seconds=float(args.c)))

    if timedelta_warning >= timedelta_critical:
        raise RuntimeError('Warning age must be smaller than critical age')

    if not os.path.isfile(args.db_filename):
        print(f'UNKNOWN - File {args.db_filename} does not exist')
        exit(UNKNOWN)
    else:
        if not os.access(args.db_filename, os.R_OK):
            print(f'UNKNOWN - File {args.db_filename} is not readable')
            exit(UNKNOWN)

    timestamp = datetime.min
    for topic in args.mqtt_topic:
        if topic.find('+') >= 0 or topic.find('#') >= 0:
            pattern = topic.replace('+', '[^/]+').replace('#', '.*')
            cur = con.cursor()
            cur.execute(f'SELECT MAX("timestamp") from "topic_last_seen" WHERE topic REGEXP ?', (pattern,))
            res = cur.fetchone()
            if not res[0]:
                print(f'UNKNOWN - No data found for pattern {topic} in file {args.db_filename}')
                exit(UNKNOWN)
            else:
                timestamp = max(timestamp, datetime.fromtimestamp((res[-1])))
        else:
            cur = con.cursor()
            cur.execute(f'SELECT "timestamp" from "topic_last_seen" WHERE topic=?', (topic,))
            res = cur.fetchone()
            if not res:
                print(f'UNKNOWN - No data found for topic {topic} in file {args.db_filename}')
                exit(UNKNOWN)
            else:
                timestamp = max(timestamp, datetime.fromtimestamp((res[-1])))

    # https://assets.nagios.com/downloads/nagioscore/docs/nagioscore/3/en/pluginapi.html
    SERVICEPERFDATA = f'|age={(datetime.now()-timestamp).seconds}s;' \
                      f'{timedelta_warning.seconds};{timedelta_critical.seconds}'
    SERVICEOUTPUT = f'Last sensor update: {timestamp}'
    if timestamp < datetime.now()-abs(timedelta_critical):
        print(f'CRITICAL - ' + SERVICEOUTPUT + SERVICEPERFDATA)
        exit(CRITICAL)
    elif timestamp < datetime.now()-abs(timedelta_warning):
        print(f'WARNING - {SERVICEOUTPUT}{SERVICEPERFDATA}')
        exit(WARNING)
    else:
        print(f'OK - {SERVICEOUTPUT}{SERVICEPERFDATA}')
        exit(OK)


if __name__ == '__main__':
    main()
