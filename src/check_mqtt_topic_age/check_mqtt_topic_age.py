#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0-or-later
"""
Nagios/Icinga plugin that checks when messages last arrived on MQTT topics.

It reads the database that mqtt_message_timestamp_logger keeps, so it needs no
connection to the broker itself.
"""
import argparse
import datetime
import os
import re
import sqlite3
import sys
import time
import urllib.parse

__version__ = '0.1'

# NAGIOS return codes :
# https://nagios-plugins.org/doc/guidelines.html#AEN78
OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3

STATE_NAMES = {OK: 'OK', WARNING: 'WARNING', CRITICAL: 'CRITICAL', UNKNOWN: 'UNKNOWN'}

# order in which states win when several topics are checked
SEVERITY = {OK: 0, UNKNOWN: 1, WARNING: 2, CRITICAL: 3}

DURATION_UNITS = {'': 1, 's': 1, 'm': 60, 'h': 3600, 'd': 86400}

MAX_LISTED = 10


class PluginError(Exception):
    def __init__(self, state, message):
        super().__init__(message)
        self.state = state
        self.message = message


def parse_number(text, durations):
    """Parses a threshold bound; with `durations`, a unit s, m, h or d may follow the number."""
    match = re.fullmatch(r'([-+]?\d+(?:\.\d*)?|[-+]?\.\d+)([smhd]?)', text.strip())
    if not match or (match.group(2) and not durations):
        raise ValueError(text)
    return float(match.group(1)) * DURATION_UNITS[match.group(2)]


class Range:
    """Threshold range as defined by the monitoring plugins guidelines."""

    def __init__(self, start, end, inside):
        self.start = start
        self.end = end
        self.inside = inside
        self.spec = self._normalized()

    def _normalized(self):
        """Threshold as perfdata carries it: graphers want a plain number, so one-sided ranges become their bound."""
        if not self.inside and self.start == float('-inf') and self.end == float('inf'):
            return ''
        if not self.inside and self.end == float('inf'):
            return format_number(self.start)
        if not self.inside and self.start in (0, float('-inf')) and self.end != float('inf'):
            return format_number(self.end)
        low = '~' if self.start == float('-inf') else format_number(self.start)
        high = '' if self.end == float('inf') else format_number(self.end)
        return ('@' if self.inside else '') + f'{low}:{high}'

    @classmethod
    def parse(cls, spec, durations=False):
        raw = str(spec)
        text = raw.strip()
        inside = text.startswith('@')
        if inside:
            text = text[1:]

        if ':' in text:
            low, high = text.split(':', 1)
        else:
            low, high = '0', text

        try:
            start = float('-inf') if low in ('~', '') else parse_number(low, durations)
            end = float('inf') if high == '' else parse_number(high, durations)
        except ValueError:
            raise PluginError(UNKNOWN, f'invalid threshold {raw!r}')

        if start > end:
            raise PluginError(UNKNOWN, f'invalid threshold {raw!r}: start is above end')

        return cls(start, end, inside)

    def breached(self, value):
        within = self.start <= value <= self.end
        return within if self.inside else not within


def format_number(value):
    if isinstance(value, int) or float(value).is_integer():
        return '%d' % value
    return ('%.3f' % value).rstrip('0').rstrip('.')


def format_duration(seconds):
    """Human readable duration with its two most significant units, e.g. 2d 3h or 5m 10s."""
    sign = '-' if seconds < 0 else ''
    rest = int(round(abs(seconds)))
    parts = []
    for unit, size in (('d', 86400), ('h', 3600), ('m', 60), ('s', 1)):
        if rest >= size or (unit == 's' and not parts):
            parts.append(f'{rest // size}{unit}')
            rest %= size
    return sign + ' '.join(parts[:2])


def perfdata_label(label):
    # quotes are doubled inside a quoted label, '=' separates label and value
    return "'" + label.replace("'", "''").replace('=', '_') + "'"


def perfdata(label, value, uom='', warn=None, crit=None, minimum=None):
    fields = [warn.spec if warn else '', crit.spec if crit else '',
              '' if minimum is None else format_number(minimum)]
    while fields and fields[-1] == '':
        fields.pop()
    out = f'{label}={format_number(value)}{uom}'
    if fields:
        out += ';' + ';'.join(fields)
    return out


def validate_topic_filter(topic_filter):
    """Rejects filters a broker would reject, so a typo is reported instead of silently matching nothing."""
    if not topic_filter:
        raise PluginError(UNKNOWN, 'empty topic filter')
    levels = topic_filter.split('/')
    for i, level in enumerate(levels):
        if '#' in level and (level != '#' or i != len(levels) - 1):
            raise PluginError(UNKNOWN, f"invalid topic filter {topic_filter!r}: '#' must be the last level on its own")
        if '+' in level and level != '+':
            raise PluginError(UNKNOWN, f"invalid topic filter {topic_filter!r}: '+' must be a level on its own")


def has_wildcard(topic_filter):
    return '+' in topic_filter or '#' in topic_filter


def topic_matches(topic_filter, topic):
    """
    MQTT topic matching: '+' matches one level, '#' the remaining levels
    including none, and wildcards at the start do not match topics beginning
    with '$' such as $SYS.
    """
    if topic.startswith('$') and topic_filter[:1] in ('+', '#'):
        return False
    filter_levels = topic_filter.split('/')
    topic_levels = topic.split('/')
    for i, level in enumerate(filter_levels):
        if level == '#':
            return True
        if i >= len(topic_levels) or (level != '+' and level != topic_levels[i]):
            return False
    return len(filter_levels) == len(topic_levels)


def open_database(db_filename, timeout):
    """Opens the database read-only, so a mistyped path is not created as an empty database."""
    if not os.path.isfile(db_filename):
        raise PluginError(UNKNOWN, f'database {db_filename} does not exist, '
                                   f'is mqtt_message_timestamp_logger running?')
    uri = 'file:' + urllib.parse.quote(os.path.abspath(db_filename)) + '?mode=ro'
    return sqlite3.connect(uri, uri=True, timeout=timeout)


class Item:
    """The value of one checked topic or topic filter."""

    def __init__(self, label, value=None, topic=None, timestamp=None):
        self.label = label
        self.value = value
        self.topic = topic
        self.timestamp = timestamp
        self.state = UNKNOWN


def collect_ages(con, topic_filters, per_topic, now):
    rows = con.execute('SELECT topic, timestamp FROM topic_last_seen').fetchall()
    items = []
    for topic_filter in topic_filters:
        matches = [(topic, float(timestamp)) for topic, timestamp in rows
                   if timestamp is not None and topic_matches(topic_filter, topic)]
        if not matches:
            items.append(Item(topic_filter))
        elif per_topic and has_wildcard(topic_filter):
            items += [Item(topic, now - timestamp, topic, timestamp) for topic, timestamp in sorted(matches)]
        else:
            topic, timestamp = max(matches, key=lambda match: match[1])
            items.append(Item(topic_filter, now - timestamp, topic, timestamp))
    return items


def collect_counts(con, topic_filters, per_topic, now, window):
    known = [topic for (topic,) in con.execute('SELECT topic FROM topic_last_seen')]
    counts = dict(con.execute('SELECT topic, COUNT(*) FROM topic_receive_times WHERE timestamp > ? GROUP BY topic',
                              (now - window,)).fetchall())
    items = []
    for topic_filter in topic_filters:
        matches = sorted(topic for topic in known if topic_matches(topic_filter, topic))
        if not matches:
            items.append(Item(topic_filter))
        elif per_topic and has_wildcard(topic_filter):
            items += [Item(topic, counts.get(topic, 0), topic) for topic in matches]
        else:
            items.append(Item(topic_filter, sum(counts.get(topic, 0) for topic in matches)))
    return items


def describe(item, mode, window):
    if item.value is None:
        return f'{item.label} never seen'
    if mode == 'count':
        return f'{item.label} {item.value} message(s) in {format_duration(window)}'
    text = f'{item.label} {format_duration(item.value)} ago'
    if item.topic != item.label:
        text += f' ({item.topic})'
    return text


def summarize(items, mode, window):
    if len(items) == 1:
        item = items[0]
        if item.value is None:
            return f'no message on {item.label} logged yet'
        if mode == 'count':
            return f'{item.value} message(s) on {item.label} in the last {format_duration(window)}'
        text = f'last message on {item.topic} {format_duration(item.value)} ago'
        return text + ' (%s)' % datetime.datetime.fromtimestamp(item.timestamp).strftime('%Y-%m-%d %H:%M:%S')

    problems = sorted((item for item in items if item.state != OK),
                      key=lambda item: (-SEVERITY[item.state], item.label))
    if not problems:
        seen = [item for item in items if item.value is not None]
        if mode == 'count':
            quietest = min(seen, key=lambda item: item.value)
            return f'{len(items)} topics, fewest messages: {describe(quietest, mode, window)}'
        oldest = max(seen, key=lambda item: item.value)
        return f'{len(items)} topics, oldest {describe(oldest, mode, window)}'

    listed = ', '.join(f'{describe(item, mode, window)} ({STATE_NAMES[item.state]})'
                       for item in problems[:MAX_LISTED])
    if len(problems) > MAX_LISTED:
        listed += f' and {len(problems) - MAX_LISTED} more'
    return f'{len(problems)} of {len(items)} topics: {listed}'


def run_check(db_filename, warning, critical, topics, mode='age', per_topic=False, window=3600, timeout=10,
              now=None):
    """
    Checks the topics and returns the exit code and the single line of plugin output.

    `warning` and `critical` are threshold ranges; in mode 'age' they are in
    seconds, and a unit s, m, h or d may follow each number.
    """
    try:
        if not topics:
            raise PluginError(UNKNOWN, 'no topic given')
        for topic_filter in topics:
            validate_topic_filter(topic_filter)
        durations = mode == 'age'
        warn = Range.parse(warning, durations) if warning not in (None, '') else None
        crit = Range.parse(critical, durations) if critical not in (None, '') else None
        if now is None:
            now = time.time()

        con = open_database(db_filename, timeout)
        try:
            if mode == 'count':
                items = collect_counts(con, topics, per_topic, now, window)
            else:
                items = collect_ages(con, topics, per_topic, now)
        finally:
            con.close()
    except PluginError as e:
        return e.state, f'MQTT TOPIC AGE UNKNOWN - {e.message}'
    except sqlite3.OperationalError as e:
        if 'no such table' in str(e):
            return UNKNOWN, f'MQTT TOPIC AGE UNKNOWN - {db_filename} is not a database of mqtt_message_timestamp_logger'
        return UNKNOWN, f'MQTT TOPIC AGE UNKNOWN - cannot read {db_filename}: {e}'
    except sqlite3.Error as e:
        return UNKNOWN, f'MQTT TOPIC AGE UNKNOWN - cannot read {db_filename}: {e}'

    state = OK
    for item in items:
        if item.value is not None:
            if crit and crit.breached(item.value):
                item.state = CRITICAL
            elif warn and warn.breached(item.value):
                item.state = WARNING
            else:
                item.state = OK
        if SEVERITY[item.state] > SEVERITY[state]:
            state = item.state

    # one topic keeps the plain label, so graphs survive switching to a different topic
    single = len(items) == 1
    perf = []
    for item in items:
        if item.value is None:
            continue
        if mode == 'count':
            perf.append(perfdata('messages' if single else perfdata_label(item.label), item.value,
                                 '', warn, crit, 0))
        else:
            perf.append(perfdata('age' if single else perfdata_label(item.label), round(item.value),
                                 's', warn, crit))

    message = f'MQTT TOPIC AGE {STATE_NAMES[state]} - {summarize(items, mode, window)}'
    if perf:
        message += ' | ' + ' '.join(perf)
    return state, message


class ArgumentParser(argparse.ArgumentParser):
    """argparse exits 2 on a usage error, which Nagios reads as CRITICAL; exit UNKNOWN instead."""

    def error(self, message):
        self.print_usage(sys.stderr)
        sys.stderr.write(f'{self.prog}: error: {message}\n')
        sys.exit(UNKNOWN)


def duration_type(text):
    try:
        value = parse_number(text, durations=True)
    except ValueError:
        raise argparse.ArgumentTypeError(f'invalid duration {text!r}')
    if value <= 0:
        raise argparse.ArgumentTypeError('duration must be positive')
    return value


def setup_parser() -> argparse.ArgumentParser:

    parser = ArgumentParser(
        'check_mqtt_topic_age',
        description='Checks when messages last arrived on MQTT topics, as recorded by '
                    'mqtt_message_timestamp_logger. Every topic is checked on its own and the '
                    'worst state wins.',
        epilog='Thresholds take the monitoring plugins range format: 60 alerts above 60 s, '
               '@0:60 below. In mode age a unit s, m, h or d may follow each number, e.g. -w 5m -c 1h. '
               'Topics may contain the MQTT wildcards + and #.')

    parser.add_argument('-V', '--version', action='version', version=f'check_mqtt_topic_age {__version__}')

    parser.add_argument('-w', '--warning', metavar='RANGE', required=True,
                        help='WARNING if the age (mode age) or the number of messages (mode count) '
                             'is outside RANGE')

    parser.add_argument('-c', '--critical', metavar='RANGE', required=True,
                        help='CRITICAL if the age (mode age) or the number of messages (mode count) '
                             'is outside RANGE')

    parser.add_argument('--db-filename', type=str, required=True,
                        help='database written by mqtt_message_timestamp_logger')

    parser.add_argument('--mqtt-topic', type=str, required=True, action='append',
                        help='topic or topic filter to check; repeat the option to check several')

    parser.add_argument('--mode', choices=('age', 'count'), default='age',
                        help='age: seconds since the last message (default). '
                             'count: number of messages within --window')

    parser.add_argument('--window', metavar='DURATION', type=duration_type, default=3600,
                        help='time span that mode count counts messages in, e.g. 15m (default: 1h). '
                             'It must not exceed the --history-retention-duration of the logger.')

    parser.add_argument('--per-topic', action='store_true',
                        help='check every topic matching a wildcard filter on its own. Without it, a '
                             'filter is judged by its newest message (mode age) or all its messages '
                             'together (mode count).')

    parser.add_argument('-t', '--timeout', metavar='SECONDS', type=float, default=10,
                        help='how long to wait for the database while the logger writes (default: 10)')

    return parser


def main(argv=None):
    parser = setup_parser()
    args = parser.parse_args(argv)
    return_code, message = run_check(args.db_filename, args.warning, args.critical, args.mqtt_topic,
                                     mode=args.mode, per_topic=args.per_topic, window=args.window,
                                     timeout=args.timeout)
    print(message)
    sys.exit(return_code)


if __name__ == '__main__':
    main()
