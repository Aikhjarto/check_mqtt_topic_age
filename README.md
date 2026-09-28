# check_mqtt_topic_age

Tells when MQTT topics went silent: a sensor that stopped reporting, a bridge
that lost its upstream, a device that fell off the network.

The project has two parts:

- `mqtt_message_timestamp_logger`, a daemon that subscribes to MQTT topics and
  records in an SQLite database when their messages arrive
- `check_mqtt_topic_age`, a Nagios/Icinga plugin that reads that database and
  checks how long ago the last message arrived on a topic, or how many arrived
  recently

The plugin needs no connection to the broker, so a check is cheap and cannot
miss a message that was published between two checks.

## Requirements

- Python 3.7 or newer
- [paho-mqtt](https://pypi.org/project/paho-mqtt/) 1.5 or newer for the
  logger; the plugin uses the standard library only

## Installation

```sh
pip install .
```

installs both commands, `check_mqtt_topic_age` and
`mqtt_message_timestamp_logger`. Without installing, run them as
`python3 -m check_mqtt_topic_age` and `python3 -m mqtt_message_timestamp_logger`
with `src` in `PYTHONPATH`.

## The logger

```sh
mqtt_message_timestamp_logger --db-filename /var/lib/mqtt_message_timestamp_logger/topics.db \
    --mqtt-broker mqtt.example.org --mqtt-topic 'sensors/#' --mqtt-topic 'home/+/state'
```

`--mqtt-topic` may be repeated and defaults to `#`, all topics. The logger
subscribes again whenever it reconnects, and keeps retrying when the broker is
down, also at startup. It commits the collected arrival times every second
(`--commit-interval`); `--commit-interval 0` commits every message at once,
which costs more under high load. On SIGTERM it writes what arrived since the
last commit before it exits.

Besides the last arrival time of each topic, the logger keeps every arrival
time for `--history-retention-duration` seconds (default 3600) for the `count`
mode of the plugin.

### Broker connection

| Option | Meaning |
|---|---|
| `--mqtt-broker` | Host name of the broker (default: localhost) |
| `--mqtt-broker-port` | Port (default: 1883, or 8883 with TLS) |
| `--mqtt-username` | User name |
| `--mqtt-password-file` | File holding the password |
| `--tls` | Connect with TLS, verifying the certificate against the system's CAs |
| `--ca-cert` | CA bundle to verify the broker's certificate with; implies `--tls` |
| `--client-cert`, `--client-key` | Client certificate and key for TLS client authentication; imply `--tls` |
| `--insecure` | Do not verify the broker's certificate |
| `--client-id` | MQTT client id (default: chosen by the broker) |

Do not pass the password with `--mqtt-password`: command lines are visible to
every user via `ps`. Put it in a file that only the logger can read and pass
`--mqtt-password-file`, or set `$MQTT_PASSWORD_FILE` or `$MQTT_PASSWORD`.

### systemd

The folder `data` holds a service unit, a
[sysusers.d](https://www.freedesktop.org/software/systemd/man/sysusers.d.html)
file creating the user `mqtt-timestamp-logger`, and the configuration file
`/etc/mqtt_message_timestamp_logger.conf`, in which `LOGGER_OPTIONS` takes the
options above:

```sh
install -m 0644 data/mqtt_message_timestamp_logger.service /etc/systemd/system/
install -m 0644 data/mqtt_message_timestamp_logger.sysusers /etc/sysusers.d/mqtt_message_timestamp_logger.conf
install -m 0644 data/mqtt_message_timestamp_logger.conf /etc/
systemd-sysusers
systemctl daemon-reload
systemctl enable --now mqtt_message_timestamp_logger
```

The unit expects the logger at `/usr/bin/mqtt_message_timestamp_logger`; after
`pip install`, point `ExecStart` to where pip put it with
`systemctl edit mqtt_message_timestamp_logger`.

The database is `/var/lib/mqtt_message_timestamp_logger/topics.db`. It is
readable by every local user, as the plugin runs as the monitoring user. Topic
names are all it reveals; to restrict it to the monitoring user, add
`Group=nagios` (or `icinga`, `naemon`), `StateDirectoryMode=0750` and
`UMask=0027` to the service with `systemctl edit`.

## The plugin

```console
$ check_mqtt_topic_age --db-filename /var/lib/mqtt_message_timestamp_logger/topics.db -w 5m -c 1h --mqtt-topic sensors/garden/temperature
MQTT TOPIC AGE OK - last message on sensors/garden/temperature 42s ago (2026-09-28 17:24:28) | age=42s;300;3600
```

| Option | Meaning |
|---|---|
| `--db-filename` | Database of the logger (required) |
| `--mqtt-topic` | Topic or topic filter; repeat it to check several (required) |
| `-w`, `--warning` / `-c`, `--critical` | Thresholds (required) |
| `--mode` | `age` (default) or `count`, see below |
| `--window` | Time span `count` counts messages in (default: 1h) |
| `--per-topic` | Check every topic matching a wildcard filter on its own |
| `-t`, `--timeout` | Seconds to wait for the database while the logger writes (default: 10) |
| `-V`, `--version` | Show the version |

### Thresholds

`-w` and `-c` take a range in the
[monitoring plugins format](https://www.monitoring-plugins.org/doc/guidelines.html#THRESHOLDFORMAT):
`60` alerts above 60, `10:` below 10, `~:10` above 10, `10:20` outside 10..20
and `@10:20` inside. In mode `age` the numbers are seconds, and a unit `s`,
`m`, `h` or `d` may follow each one: `-w 5m -c 1h`.

### Topics

A topic filter may use the MQTT wildcards: `+` matches one level and `#` all
remaining levels, so `home/+/temp` matches `home/kitchen/temp` but neither
`home/kitchen/temp/raw` nor `myhome/kitchen/temp`. As in MQTT, a filter
starting with a wildcard does not match topics starting with `$`, such as
`$SYS`.

Every `--mqtt-topic` is checked on its own, and the worst state wins, so one
live topic cannot hide a dead one. A wildcard filter is judged by its newest
message: `home/+/temp` is OK as long as any of those sensors reports. With
`--per-topic`, every matching topic is checked on its own, which finds the one
sensor that stopped:

```console
$ check_mqtt_topic_age --db-filename topics.db -w 10m -c 1h --mqtt-topic 'home/+/temp' --per-topic
MQTT TOPIC AGE CRITICAL - 1 of 3 topics: home/cellar/temp 2d 3h ago (CRITICAL) | 'home/cellar/temp'=183600s;600;3600 'home/garden/temp'=12s;600;3600 'home/kitchen/temp'=41s;600;3600
```

Only topics the logger has seen can match. A topic or filter that nothing has
been logged for is UNKNOWN, since the logger may not subscribe to it.

### Mode count

`--mode count` counts the messages that arrived within `--window` instead, for
topics that must not only be alive but report at their usual rate. Here the
thresholds are message counts, typically lower bounds:

```console
$ check_mqtt_topic_age --db-filename topics.db --mode count --window 15m -w 12: -c 1: --mqtt-topic power/meter
MQTT TOPIC AGE WARNING - 9 message(s) on power/meter in the last 15m | messages=9;12;1;0
```

The window must not be longer than the logger's
`--history-retention-duration`, which defaults to one hour.

### Output and exit codes

The plugin prints exactly one line: the state, a summary and the performance
data. With one topic, the performance data is labelled `age` (or `messages`),
otherwise by topic. Up to 10 topics that are not OK are named, the worst first.

`0` OK, `1` WARNING, `2` CRITICAL, `3` UNKNOWN. A missing database, an unknown
topic, an invalid threshold and a wrong command line are UNKNOWN. The plugin
opens the database read-only, so a mistyped path is not created.

### Nagios / Icinga

```
define command {
    command_name    check_mqtt_topic_age
    command_line    $USER1$/check_mqtt_topic_age --db-filename /var/lib/mqtt_message_timestamp_logger/topics.db -w $ARG1$ -c $ARG2$ --mqtt-topic '$ARG3$'
}

define service {
    host_name               mqtt
    service_description     Garden sensors
    check_command           check_mqtt_topic_age!10m!1h!sensors/garden/#
    use                     generic-service
}
```

## Tests

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -t .
```

The tests of the logger start their own mosquitto on a free port and skip
themselves when `mosquitto` is not installed; `mosquitto_passwd` and `openssl`
are needed for the authentication and TLS tests.

## Disclaimer

This project is for small scale usage. If you have a high rate of messages you
want to monitor, this Python implementation might be too slow. Consider using
an extension to your MQTT broker in that case.

## License

GPL-2.0-or-later, see [LICENSE](LICENSE).
