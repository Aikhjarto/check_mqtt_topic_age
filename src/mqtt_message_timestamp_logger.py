#!/bin/env python3
import paho.mqtt.client as mqtt
import time
import argparse
import sqlite3
import logging
import threading
logger = logging.getLogger(__name__)
logging.basicConfig()

shared_dict = {}
shared_dict_times = {}
shared_diff_dict = {}
dict_lock = threading.Lock()

def commit_interval_type(x):
    x = float(x)
    if x<=0:
        raise argparse.ArgumentTypeError("commit interval must be a positive number denoting seconds")
    return x


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--db-filename', type=str, required=True)

    parser.add_argument('--mqtt-broker', type=str, default='localhost',
                        help="Hostname or IP of the MQTT broker")

    parser.add_argument('--mqtt-broker-port', type=int, default=1883)

    parser.add_argument('--mqtt-username', type=str, default=None)

    parser.add_argument('--mqtt-password', type=str, default=None)

    parser.add_argument('--mqtt-topic', type=str, action='append', default=[])

    parser.add_argument('--client-id', type=str, default="")

    parser.add_argument('--history-retention-duration', metavar="T_purge", type=float, default=3600,
                        help="To avoid the DB growing indefinitely, purge timestamps older than T_purge seconds with every commit.")

    parser.add_argument('--commit-interval', metavar='T', type=commit_interval_type, default=1, 
                        help="If T>0, database transaction are committed every T seconds. "
                              "If T==0, each transaction is committed, which might reduce in high load.")
    
    parser.add_argument('--verbose', '-v',  action='store_true',
                        help='Enable verbose output to stdout.')

    return parser


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
    if 'commit_interval' in userdata and userdata['commit_interval']:
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
                shared_diff_dict[message.topic]=now - shared_dict[message.topic]
            shared_dict[message.topic] = now
            if message.topic not in shared_dict_times:
                shared_dict_times[message.topic] = []
            shared_dict_times[message.topic].append(now)
            

def commit_thread(db_filename, interval = 1, history_retention_duration=3600):
    
    global shared_dict, shared_dict_times, shared_diff_dict
    with sqlite3.connect(db_filename) as con:
        while True:
            with dict_lock:
                if shared_dict:
                    for key, value in shared_dict.items():
                        diff = None
                        if key in shared_diff_dict:
                            diff = shared_diff_dict[key]
                        else:
                            cur = con.execute('SELECT timestamp from topic_last_seen WHERE topic=?', (key,))
                            res = cur.fetchone()
                            if res:
                                diff = value - res[0]
                        if diff is not None:
                            con.execute('INSERT OR REPLACE INTO topic_last_interval VALUES (?, ?)', (key, diff))

                        con.execute('INSERT OR REPLACE INTO topic_last_seen VALUES (?, ?)', (key, value))

                        logger.debug(f"Inserted {key}, {value}, {diff}")
                    for key, lst in shared_dict_times.items():
                        for value in lst:
                            con.execute('INSERT INTO topic_receive_times VALUES (?, ?)', (key, value))

                    shared_dict = {}
                    shared_diff_dict = {}
                    shared_dict_times = {}

            con.execute("DELETE FROM topic_receive_times WHERE timestamp <= (?)", (time.time() - history_retention_duration,))
            con.commit()
            time.sleep(interval)


def init_DB(db_filename):
    con = sqlite3.connect(db_filename)
    con.execute("CREATE TABLE IF NOT EXISTS topic_last_seen(topic TEXT UNIQUE, timestamp REAL)")
    con.execute("CREATE TABLE IF NOT EXISTS topic_last_interval(topic TEXT UNIQUE, timestamp REAL)")
    con.execute("CREATE TABLE IF NOT EXISTS topic_receive_times(topic TEXT, timestamp REAL)")
    con.commit()
    con.close()


def main():


    p = setup_parser()
    args = p.parse_args()


    if args.verbose:
        logger.setLevel(logging.DEBUG)
    

    init_DB(args.db_filename)

    userdata = {'commit_interval': args.commit_interval==0,
                'history_retention_duration': args.history_retention_duration}

    # if transaction should be collected, start database connection in separate thread
    if args.commit_interval > 0:
        thd = threading.Thread(target = commit_thread, args=(args.db_filename, 
                                                             args.commit_interval, 
                                                             args.history_retention_duration),
                               daemon=True)
        thd.start()
    else:
        userdata['sqlite_con'] = sqlite3.connect(args.db_filename)

    # configure MQTT client
    client = mqtt.Client(client_id=args.client_id, userdata=userdata)
    client.on_message = on_message

    # connect to broker
    client.username_pw_set(args.mqtt_username, args.mqtt_password)
    res = client.connect(host=args.mqtt_broker, port=args.mqtt_broker_port)
    if res != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f'MQTT connection error {res}')
    logger.info(f'Connected to {args.mqtt_broker}.')

    # subscribe to topics
    for topic in args.mqtt_topic:
        res, mid = client.subscribe(topic)
        if res != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f'Subscribe to {topic} failed with error {res}')
        else:
            logger.info(f'Subscribed to {topic}.')

    # run event loop
    client.loop_forever()


if __name__ == '__main__':
    main()
