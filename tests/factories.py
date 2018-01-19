from datetime import datetime
from systemd import journal
from awslogs_sd.awslogs_sd import Record, UnitConfig


def make_unit_conf(name=None, group=None, stream=None):
    name = name or 'foo'
    return UnitConfig(
        name,
        '{}.service'.format(name),
        journal.LOG_INFO,
        'tag',
        17,
        group or 'group-name',
        stream or 'stream-name',
        lambda r: r['MESSAGE'],
    )

def make_sd_record(message='message', rt_date=None, cursor=None):
    return {
        'MESSAGE': message,
        '__REALTIME_TIMESTAMP': rt_date or datetime.now(),
        '__CURSOR': cursor or 's=0',
    }


def make_record(unit_conf, message='message', date=None, cursor=None):
    date = date or datetime.now()
    cursor = cursor or 's=0'
    return Record(unit_conf, date, cursor, message)
