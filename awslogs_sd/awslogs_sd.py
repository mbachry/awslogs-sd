#!/usr/bin/env python3
import argparse
import json
import time
import itertools
import logging
import logging.config
import configparser
import socket
from collections import namedtuple
from operator import itemgetter, attrgetter
from functools import partial, lru_cache
from datetime import timedelta
from glob import glob
import boto3
import requests
import retrying
import gevent
import gevent.socket
import gevent.queue
from botocore.exceptions import BotoCoreError
from systemd import journal
from .metrics import metrics, metrics_task


# cloudwatch doesn't allow batch spans larger than 24h
MAX_BATCH_TIME_SPAN = timedelta(hours=24)
MAX_BATCH_ITEMS = 100
BATCH_TIMEOUT_S = 1.0
MAX_QUEUE_SIZE = 100000


Config = namedtuple('Config', ('state', 'units'))
UnitConfig = namedtuple('UnitConfig',
                        ('name', 'unit', 'priority', 'syslog_ident', 'syslog_facility', 'log_group_name',
                         'log_stream_name', 'format_func'))
Record = namedtuple('Record', ('unit_conf', 'date', 'cursor', 'message'))


PRIORITY_MAP = {
    0: 'EMERG',
    1: 'ALERT',
    2: 'CRIT',
    3: 'ERR',
    4: 'WARNING',
    5: 'NOTICE',
    6: 'INFO',
    7: 'DEBUG',
}


RFC_SYSLOG_FACILITIES = {
    'auth': 4,
    'authpriv': 10,
    'cron': 9,
    'daemon': 3,
    'kern': 0,
    'lpr': 6,
    'mail': 2,
    'news': 7,
    'security': 4,
    'syslog': 5,
    'user': 1,
    'uucp': 8,
    'local0': 16,
    'local1': 17,
    'local2': 18,
    'local3': 19,
    'local4': 20,
    'local5': 21,
    'local6': 22,
    'local7': 23,
}


logger = logging.getLogger('awslogs')


@lru_cache()
def get_ec2_instance_id():
    try:
        resp = requests.get('http://169.254.169.254/latest/meta-data/instance-id', timeout=2)
        resp.raise_for_status()
        return resp.text.strip()
    except IOError:
        logging.exception('failed to get ec2 instance id')
        raise


def eval_log_stream_name(name):
    if '{instance_id}' in name:
        name = name.replace('{instance_id}', get_ec2_instance_id())
    if '{hostname}' in name:
        name = name.replace('{hostname}', socket.gethostname())
    return name


def read_config(filename):
    conf = configparser.ConfigParser()
    conf.read(filename)
    if 'include' in conf:
        include_path = conf['include']['path']
        for path in glob(include_path):
            conf.read(path)

    state = State(conf['general']['state_file'])
    units = []
    for sec_name in conf.sections():
        if sec_name in ('general', 'include'):
            continue
        sec = conf[sec_name]

        priority = journal.LOG_INFO
        if 'priority' in sec:
            priority = getattr(journal, 'LOG_' + sec['priority'])

        syslog_facility = sec.get('syslog_facility')
        if syslog_facility is not None:
            if syslog_facility not in RFC_SYSLOG_FACILITIES:
                raise ValueError('invalid syslog facility: {}'.format(syslog_facility))
            syslog_facility = RFC_SYSLOG_FACILITIES[syslog_facility]

        log_stream_name = eval_log_stream_name(sec['log_stream_name'])

        format = sec.get('format', 'text')
        if format == 'json':
            format_func = record_to_json
        elif format == 'text':
            format_func = partial(
                record_to_text,
                log_format=sec.get('log_format'),
                datetime_format=sec.get('datetime_format')
            )
        else:
            raise ValueError('invalid format: {}'.format(format))

        units.append(UnitConfig(
            sec_name,
            sec['unit'],
            priority,
            sec.get('syslog_ident'),
            syslog_facility,
            sec['log_group_name'],
            log_stream_name,
            format_func))

    return Config(state, units)


class State:
    def __init__(self, filename):
        self.filename = filename
        self.data = {
            'cwl_tokens': {},
            'cursors': {},
        }
        self.read()

    def read(self):
        try:
            with open(self.filename) as fp:
                self.data = json.load(fp)
        except FileNotFoundError:
            pass

    def write(self):
        with open(self.filename, 'w') as fp:
            json.dump(self.data, fp)

    def get_token(self, group, stream):
        group_state = self.data['cwl_tokens'].get(group, {})
        return group_state.get(stream)

    def set_token(self, group, stream, token):
        group_state = self.data['cwl_tokens'].setdefault(group, {})
        group_state[stream] = token

    def get_cursor(self, name):
        return self.data['cursors'].get(name)

    def set_cursor(self, name, cursor):
        self.data['cursors'][name] = cursor


def make_journal_reader(unit_conf):
    r = journal.Reader()
    r.add_match('_SYSTEMD_UNIT={}'.format(unit_conf.unit))
    r.log_level(unit_conf.priority)
    if unit_conf.syslog_ident is not None:
        r.add_match('SYSLOG_IDENTIFIER={}'.format(unit_conf.syslog_ident))
    if unit_conf.syslog_facility is not None:
        r.add_match('SYSLOG_FACILITY={}'.format(unit_conf.syslog_facility))
    return r


def handle_incoming_records(records, queue, unit_conf):
    n_handled = 0
    for record in records:
        logger.debug('Got journal record: %s', record)
        queue.put(Record(
            unit_conf,
            record['__REALTIME_TIMESTAMP'],
            record['__CURSOR'],
            unit_conf.format_func(record)
        ))
        n_handled += 1
    return n_handled


def infinity():
    return itertools.repeat(True)


def reader_task(queue, unit_conf, state):
    r = make_journal_reader(unit_conf)
    skip = 0
    cursor = state.get_cursor(unit_conf.name)
    if cursor:
        r.seek_cursor(cursor)
        skip = 1
    fd = r.fileno()

    logger.debug('Started reader task for unit "%s/%s"', unit_conf.name, unit_conf.unit)

    for _ in infinity():
        gevent.socket.wait_read(fd)
        r.process()
        records = itertools.islice(r, skip, None)
        n_handled = handle_incoming_records(records, queue, unit_conf)
        metrics.n_logs_received += n_handled
        if skip > 0:
            skip -= 1


def split_batch_by_time_span(records, max_span=MAX_BATCH_TIME_SPAN):
    records.sort(key=attrgetter('date'))
    batch = []
    last_date = None
    for record in records:
        if last_date is not None and record.date - last_date > max_span:
            yield batch
            batch = []
        batch.append(record)
        last_date = record.date
    if batch:
        yield batch


def batcher_task(src_queue, dst_queue, n_items=MAX_BATCH_ITEMS, batch_timeout=BATCH_TIMEOUT_S):
    last_flush = time.monotonic()
    records = []

    logger.debug('Started batcher task')

    for _ in infinity():
        timeout = last_flush + batch_timeout - time.monotonic()
        try:
            record = src_queue.get(timeout=timeout)
            records.append(record)
            if len(records) >= n_items:
                raise gevent.queue.Empty()
        except gevent.queue.Empty:
            if records:
                for batch in split_batch_by_time_span(records):
                    logger.debug('Flushing batch: %s', batch)
                    dst_queue.put(batch)
                records.clear()
            last_flush = time.monotonic()


def get_cwl_token(client, group, stream):
    resp = client.describe_log_streams(logGroupName=group, logStreamNamePrefix=stream)
    streams = [s for s in resp['logStreams'] if s['logStreamName'] == stream]
    assert len(streams) == 1
    return streams[0].get('uploadSequenceToken')


class LogPushError(Exception):
    pass


def retry_condition(e):
    return isinstance(e, LogPushError)


@retrying.retry(
    retry_on_exception=retry_condition, wait_exponential_multiplier=50, wait_exponential_max=3000)
def push_records(client, records, unit_conf, state):
    assert records
    assert all(r.unit_conf == unit_conf for r in records)

    group = unit_conf.log_group_name
    stream = unit_conf.log_stream_name
    kwargs = {
        'logGroupName': group,
        'logStreamName': stream,
        'logEvents': [
            {
                'timestamp': int(r.date.timestamp() * 1000),
                'message': r.message
            }
            for r in records
        ],
    }

    try:
        cwl_token = state.get_token(group, stream)
        if cwl_token is None:
            cwl_token = get_cwl_token(client, group, stream)
        if cwl_token is not None:
            kwargs['sequenceToken'] = cwl_token

        resp = client.put_log_events(**kwargs)

        rejected = resp.get('rejectedLogEventsInfo')
        if rejected:
            logging.error('cloudwatch rejected log events', extra={
                'cwl_response': resp,
                'records': records,
            })

        state.set_token(group, stream, resp['nextSequenceToken'])
        # records are sorted by timestamp, so it's enough to grab last
        # record's cursor (TODO: is "latest timestamp" always equal to
        # "last cursor"?)
        state.set_cursor(unit_conf.name, records[-1].cursor)
        state.write()

    except BotoCoreError as e:
        logging.exception('cloudwatch API call failed')
        raise LogPushError('cloudwatch API call failed')

    except Exception as e:
        if isinstance(e, client.exceptions.InvalidSequenceTokenException):
            logging.warning('invalid sequence token, refreshing')
            state.set_token(group, stream, None)
            raise LogPushError('invalid sequence token')
        elif isinstance(e, client.exceptions.DataAlreadyAcceptedException):
            logging.warning('data already sent, dropping duplicate batch and refreshing token')
            state.set_cursor(unit_conf.name, records[-1].cursor)
            state.set_token(group, stream, None)
            state.write()
            return
        else:
            raise


def writer_task(queue, state):
    client = boto3.client('logs')
    sort_func = itemgetter(0)

    logger.debug('Started writer task')

    for _ in infinity():
        records = queue.get()
        assert records
        records.sort(key=sort_func)
        for unit_conf, unit_records in itertools.groupby(records, sort_func):
            unit_records = list(unit_records)
            logger.debug('Pushing records for unit "%s/%s": %s', unit_conf.name, unit_conf.unit, unit_records)
            push_records(client, unit_records, unit_conf, state)
            metrics.n_logs_sent += len(unit_records)


def record_to_json(r):
    res = {
        'pid': r['_PID'],
        'uid': r['_UID'],
        'gid': r['_GID'],
        'cmd_name': r['_COMM'],
        'cmd_line': r['_CMDLINE'],
        'exe': r['_EXE'],
        'systemd_unit': r['_SYSTEMD_UNIT'],
        'boot_id': str(r['_BOOT_ID']),
        'hostname': r['_HOSTNAME'],
        'transport': r['_TRANSPORT'],
        'priority': PRIORITY_MAP[r['PRIORITY']],
        'message': r['MESSAGE'],
    }
    if res['transport'] == 'syslog':
        res['syslog'] = {
            'facility': r['SYSLOG_FACILITY'],
            'ident': r['SYSLOG_IDENTIFIER'],
        }
    return res


def record_to_text(r, log_format=None, datetime_format=None):
    log_format = log_format or '{date} {_HOSTNAME} {_COMM}[{_PID}]: {MESSAGE}'
    datetime_format = datetime_format or '%b %d %H:%M:%S'
    date = r['__REALTIME_TIMESTAMP'].strftime(datetime_format)
    return log_format.format(date=date, **r)


def create_log_groups(conf):
    client = boto3.client('logs')
    for unit_conf in conf.units:
        name = unit_conf.log_group_name
        resp = client.describe_log_groups(logGroupNamePrefix=name)
        matches = [g for g in resp['logGroups'] if g['logGroupName'] == name]
        if not matches:
            logger.info('Creating log group: %s', name)
            client.create_log_group(logGroupName=name)


def create_log_streams(conf):
    client = boto3.client('logs')
    for unit_conf in conf.units:
        group = unit_conf.log_group_name
        name = unit_conf.log_stream_name
        resp = client.describe_log_streams(logGroupName=group)
        matches = [g for g in resp['logStreams'] if g['logStreamName'] == name]
        if not matches:
            logger.info('Creating log stream: %s', name)
            client.create_log_stream(logGroupName=group, logStreamName=name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'config_file',
        help='Path to configuration file')
    parser.add_argument(
        '--logging-conf',
        help='Optional logging configuration (yaml file in dictConfig format)')
    args = parser.parse_args()
    conf = read_config(args.config_file)

    if args.logging_conf:
        import yaml
        with open(args.logging_conf) as fp:
            logging.config.dictConfig(yaml.load(fp))

    create_log_groups(conf)
    create_log_streams(conf)

    # crash whole app on unhandled exception in any coroutine
    gevent.get_hub().SYSTEM_ERROR = (Exception,)

    journal_queue = gevent.queue.Queue(MAX_QUEUE_SIZE)
    cwl_queue = gevent.queue.Queue(MAX_QUEUE_SIZE)
    metrics.journal_queue = journal_queue
    metrics.cwl_queue = cwl_queue

    for unit_conf in conf.units:
        gevent.spawn(reader_task, journal_queue, unit_conf, conf.state)
    gevent.spawn(batcher_task, journal_queue, cwl_queue)
    gevent.spawn(writer_task, cwl_queue, conf.state)
    gevent.spawn(metrics_task)
    gevent.wait()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
