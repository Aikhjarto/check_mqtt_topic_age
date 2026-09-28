#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0-or-later
"""
Daemon that subscribes to MQTT topics and records when their messages arrive,
for check_mqtt_topic_age.
"""
import argparse
import logging
import os
import signal
import sqlite3
import ssl
import sys
import threading
import time

import paho.mqtt.client as mqtt

try:
    from check_mqtt_topic_age.check_mqtt_topic_age import __version__
except ImportError:
    # run as a script from a checkout
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from check_mqtt_topic_age.check_mqtt_topic_age import __version__

logger = logging.getLogger(__name__)
logging.basicConfig()

shared_dict = {}
shared_dict_times = {}
shared_diff_dict = {}
dict_lock = threading.Lock()


def commit_interval_type(x):
    x = float(x)
    if x < 0:
        raise argparse.ArgumentTypeError("commit interval must not be negative")
    return x


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser('mqtt_message_timestamp_logger')

    parser.add_argument('-V', '--version', action='version', version=f'mqtt_message_timestamp_logger {__version__}')

    parser.add_argument('--db-filename', type=str, required=True)

    parser.add_argument('--mqtt-broker', type=str, default='localhost',
                        help="Hostname or IP of the MQTT broker")

    parser.add_argument('--mqtt-broker-port', type=int, default=None,
                        help="Port of the MQTT broker (default: 1883, or 8883 with --tls)")

    parser.add_argument('--mqtt-username', type=str, default=None)

    parser.add_argument('--mqtt-password-file', type=str, default=None,
                        help="File holding the password. Also read from $MQTT_PASSWORD_FILE, "
                             "or taken from $MQTT_PASSWORD.")

    parser.add_argument('--mqtt-password', type=str, default=None,
                        help="Password. Discouraged: command lines are visible to every user, "
                             "use --mqtt-password-file instead.")

    parser.add_argument('--tls', action='store_true',
                        help="Connect to the broker with TLS")

    parser.add_argument('--ca-cert', type=str, default=None,
                        help="CA bundle to verify the broker's certificate (default: the system's CAs). "
                             "Implies --tls.")

    parser.add_argument('--client-cert', type=str, default=None,
                        help="Client certificate for TLS client authentication. Implies --tls.")

    parser.add_argument('--client-key', type=str, default=None,
                        help="Private key of --client-cert, unless it holds the key itself")

    parser.add_argument('--insecure', action='store_true',
                        help="Do not verify the broker's TLS certificate")

    parser.add_argument('--mqtt-topic', type=str, action='append', default=[],
                        help="Topic filter to subscribe to, may be repeated (default: #)")

    parser.add_argument('--client-id', type=str, default="")

    parser.add_argument('--history-retention-duration', metavar="T_purge", type=float, default=3600,
                        help="To avoid the DB growing indefinitely, purge timestamps older than "
                             "T_purge seconds with every commit.")

    parser.add_argument('--commit-interval', metavar='T', type=commit_interval_type, default=1,
                        help="If T>0, database transaction are committed every T seconds. "
                             "If T==0, each transaction is committed, which might reduce in high load.")

    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output to stdout.')

    return parser


def read_password(args):
    """The password from --mqtt-password-file, $MQTT_PASSWORD_FILE, --mqtt-password or $MQTT_PASSWORD."""
    password_file = args.mqtt_password_file or os.environ.get('MQTT_PASSWORD_FILE')
    if password_file:
        with open(password_file) as handle:
            return handle.read().rstrip('\r\n')
    if args.mqtt_password is not None:
        return args.mqtt_password
    return os.environ.get('MQTT_PASSWORD')


def on_message(mqtt_client, userdata, message):
    """
    Parameters
    ----------
    mqtt_client: mqtt.Client
        the client instance for this callback
    userdata:
        the private user data as set in Client() or user_data_set()
    message:    mqtt.MQTTMessage
        This is a class with members topic, payload, qos, retain.

    """

    now = time.time()
    if userdata.get('immediate_commit'):
        con: sqlite3.Connection = userdata['sqlite_con']
        with con:
            diff = None
            cur = con.execute('SELECT timestamp from topic_last_seen WHERE topic=?', (message.topic,))
            res = cur.fetchone()
            if res:
                diff = now - res[0]
                con.execute('INSERT OR REPLACE INTO topic_last_interval VALUES (?, ?)', (message.topic, diff))
            con.execute('INSERT OR REPLACE INTO topic_last_seen VALUES (?, ?)', (message.topic, now))
            con.execute('INSERT INTO topic_receive_times VALUES (?, ?)', (message.topic, now))
            con.execute("DELETE FROM topic_receive_times WHERE timestamp <= (?)",
                        (time.time() - userdata['history_retention_duration'],))

            logger.debug(f"Inserted '{message.topic}, {now}, {diff}")
    else:
        with dict_lock:
            if message.topic in shared_dict:
                shared_diff_dict[message.topic] = now - shared_dict[message.topic]
            shared_dict[message.topic] = now
            if message.topic not in shared_dict_times:
                shared_dict_times[message.topic] = []
            shared_dict_times[message.topic].append(now)


def is_failure(reason_code):
    # paho-mqtt 2 passes a ReasonCode, paho-mqtt 1 an int
    return reason_code.is_failure if hasattr(reason_code, 'is_failure') else reason_code != 0


def on_connect(mqtt_client, userdata, flags, reason_code, properties=None):
    """Subscribes on every connect, as a reconnected session may have lost its subscriptions."""
    if is_failure(reason_code):
        logger.error(f'Connection to broker refused: {reason_code}')
        return
    logger.info('Connected to broker.')
    for topic in userdata['topics']:
        res, mid = mqtt_client.subscribe(topic)
        if res != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f'Subscribe to {topic} failed with error {res}')
        else:
            logger.info(f'Subscribed to {topic}.')


def on_disconnect(mqtt_client, userdata, *args):
    if not userdata.get('stopping'):
        logger.warning('Disconnected from broker, reconnecting.')


def on_connect_fail(mqtt_client, userdata):
    logger.warning('Connecting to broker failed, retrying.')


def commit_pending(con, history_retention_duration):
    """Writes the messages collected since the last call to the database and commits."""
    global shared_dict, shared_dict_times, shared_diff_dict
    with dict_lock:
        pending, pending_times, pending_diffs = shared_dict, shared_dict_times, shared_diff_dict
        shared_dict, shared_dict_times, shared_diff_dict = {}, {}, {}

    try:
        for key, value in pending.items():
            diff = None
            if key in pending_diffs:
                diff = pending_diffs[key]
            else:
                cur = con.execute('SELECT timestamp from topic_last_seen WHERE topic=?', (key,))
                res = cur.fetchone()
                if res:
                    diff = value - res[0]
            if diff is not None:
                con.execute('INSERT OR REPLACE INTO topic_last_interval VALUES (?, ?)', (key, diff))

            con.execute('INSERT OR REPLACE INTO topic_last_seen VALUES (?, ?)', (key, value))

            logger.debug(f"Inserted {key}, {value}, {diff}")
        for key, lst in pending_times.items():
            for value in lst:
                con.execute('INSERT INTO topic_receive_times VALUES (?, ?)', (key, value))

        con.execute("DELETE FROM topic_receive_times WHERE timestamp <= (?)",
                    (time.time() - history_retention_duration,))
        con.commit()
    except sqlite3.OperationalError:
        # e.g. the database is locked: keep the messages for the next attempt
        con.rollback()
        with dict_lock:
            for key, value in pending.items():
                shared_dict.setdefault(key, value)
            for key, diff in pending_diffs.items():
                shared_diff_dict.setdefault(key, diff)
            for key, lst in pending_times.items():
                shared_dict_times[key] = lst + shared_dict_times.get(key, [])
        raise


def commit_thread(db_filename, interval=1, history_retention_duration=3600, stop_event=None):
    if stop_event is None:
        stop_event = threading.Event()
    try:
        with sqlite3.connect(db_filename, timeout=10) as con:
            while True:
                stopping = stop_event.wait(interval)
                try:
                    commit_pending(con, history_retention_duration)
                except sqlite3.OperationalError as e:
                    logger.warning(f'Commit failed, retrying: {e}')
                if stopping:
                    break
        con.close()
    except Exception:
        # without this thread nothing is recorded anymore: exit, so systemd restarts the daemon
        logger.exception('Commit thread failed')
        os._exit(1)


def init_DB(db_filename):
    con = sqlite3.connect(db_filename)
    # readers such as check_mqtt_topic_age are not blocked by a commit in WAL mode
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("CREATE TABLE IF NOT EXISTS topic_last_seen(topic TEXT UNIQUE, timestamp REAL)")
    con.execute("CREATE TABLE IF NOT EXISTS topic_last_interval(topic TEXT UNIQUE, timestamp REAL)")
    con.execute("CREATE TABLE IF NOT EXISTS topic_receive_times(topic TEXT, timestamp REAL)")
    con.execute("CREATE INDEX IF NOT EXISTS topic_receive_times_topic ON topic_receive_times(topic, timestamp)")
    con.commit()
    con.close()


def create_client(client_id, userdata):
    # paho-mqtt 2 needs the callback API version, paho-mqtt 1 does not know it
    if hasattr(mqtt, 'CallbackAPIVersion'):
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, userdata=userdata)
    return mqtt.Client(client_id=client_id, userdata=userdata)


def create_tls_context(args):
    context = ssl.create_default_context(cafile=args.ca_cert)
    if args.client_cert:
        context.load_cert_chain(args.client_cert, args.client_key)
    if args.insecure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def main(argv=None):

    p = setup_parser()
    args = p.parse_args(argv)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    use_tls = args.tls or args.ca_cert or args.client_cert
    port = args.mqtt_broker_port or (8883 if use_tls else 1883)
    topics = args.mqtt_topic or ['#']

    init_DB(args.db_filename)

    userdata = {'immediate_commit': args.commit_interval == 0,
                'history_retention_duration': args.history_retention_duration,
                'topics': topics}

    # if transaction should be collected, start database connection in separate thread
    stop_event = threading.Event()
    thd = None
    if args.commit_interval > 0:
        thd = threading.Thread(target=commit_thread, args=(args.db_filename,
                                                           args.commit_interval,
                                                           args.history_retention_duration,
                                                           stop_event),
                               daemon=True)
        thd.start()
    else:
        userdata['sqlite_con'] = sqlite3.connect(args.db_filename, timeout=10)

    # configure MQTT client
    client = create_client(args.client_id, userdata)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_connect_fail = on_connect_fail
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=60)

    password = read_password(args)
    if args.mqtt_username is not None or password is not None:
        client.username_pw_set(args.mqtt_username, password)

    if use_tls:
        client.tls_set_context(create_tls_context(args))

    def stop(signum, frame):
        userdata['stopping'] = True
        client.disconnect()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    # connect in the loop, so a broker that is down at startup is retried like a lost connection
    logger.info(f'Connecting to {args.mqtt_broker}:{port}.')
    client.connect_async(host=args.mqtt_broker, port=port)
    client.loop_forever(retry_first_connection=True)

    # write what arrived since the last commit
    if thd is not None:
        stop_event.set()
        thd.join()
    else:
        userdata['sqlite_con'].close()
    logger.info('Stopped.')


if __name__ == '__main__':
    main()
