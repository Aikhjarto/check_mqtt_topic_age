#!/bin/env python3
import paho.mqtt.client as mqtt
import time
import argparse
import sqlite3
import logging
logger = logging.getLogger(__name__)
logging.basicConfig()


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--db-filename', type=str, required=True)

    parser.add_argument('--mqtt-broker', type=str, default='localhost',
                        help="Hostname or IP of the MQTT broker")

    parser.add_argument('--mqtt-broker-port', type=int, default=1883)

    parser.add_argument('--mqtt-username', type=str, default=None)

    parser.add_argument('--mqtt-password', type=str, default=None)

    parser.add_argument('--mqtt-topic', type=str, action='append', default=[])

    return parser


def on_message(mqtt_client, userdata, message):
    """
    Parameters
    ----------
    mqtt_client: mqtt.Client
        the client sqlite_coninstance for this callback
    userdata:
        the private user data as set in Client() or userdata_set()
    message:    mqtt.MQTTMessage
        This is a class with members topic, payload, qos, retain.

    """
    logger.debug(f"message topic='{message.topic}")

    con: sqlite3.Connection = userdata['sqlite_con']
    con.execute(f'INSERT OR REPLACE INTO topic_last_seen VALUES (?, ?)', (message.topic, time.time()))
    con.commit()


if __name__ == '__main__':

    logger.setLevel(logging.DEBUG)

    p = setup_parser()
    args = p.parse_args()

    con = sqlite3.connect(args.db_filename)
    cur = con.execute("SELECT name FROM sqlite_master")
    tables = cur.fetchone()
    if not tables or 'topic_last_seen' not in tables:
        logger.info(f'Create table topic_last_seen in {args.db_filename}')
        con.execute("CREATE TABLE topic_last_seen(topic type UNIQUE, timestamp)")
        con.commit()
    else:
        logger.info(f'Found table topic_last_seen in {args.db_filename}')

    # configure MQTT client
    client = mqtt.Client()
    client.user_data_set({'sqlite_con': con})
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

    # setup eventloop
    client.loop_forever()
