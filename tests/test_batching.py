from unittest import mock
from queue import Queue
from datetime import datetime, timedelta
from awslogs_sd.awslogs_sd import batcher_task, split_batch_by_time_span
from .factories import make_record


def test_split_all_fresh(unit_conf):
    now = datetime.now()
    records = [
        make_record(unit_conf, date=now + timedelta(seconds=6)),
        make_record(unit_conf, date=now + timedelta(seconds=3)),
        make_record(unit_conf, date=now + timedelta(seconds=7)),
    ]
    result = list(split_batch_by_time_span(records))
    assert len(result) == 1
    expected = sorted(records, key=lambda r: r.date)
    assert result[0] == expected


def test_split_old_records(unit_conf):
    now = datetime.now()
    records1 = [
        make_record(unit_conf, date=now + timedelta(seconds=6)),
        make_record(unit_conf, date=now + timedelta(seconds=3)),
        make_record(unit_conf, date=now + timedelta(seconds=7)),
    ]
    records2 = [
        make_record(unit_conf, date=now + timedelta(hours=-30, seconds=6)),
        make_record(unit_conf, date=now + timedelta(hours=-30, seconds=3)),
        make_record(unit_conf, date=now + timedelta(hours=-30, seconds=7)),
    ]
    records3 = [
        make_record(unit_conf, date=now + timedelta(hours=30, seconds=6)),
        make_record(unit_conf, date=now + timedelta(hours=30, seconds=3)),
        make_record(unit_conf, date=now + timedelta(hours=30, seconds=7)),
    ]
    result = list(split_batch_by_time_span(records1 + records2 + records3))
    assert len(result) == 3
    key_func = lambda r: r.date
    assert result[0] == sorted(records2, key=key_func)
    assert result[1] == sorted(records1, key=key_func)
    assert result[2] == sorted(records3, key=key_func)


def test_split_old_records_freshest_last(unit_conf):
    now = datetime.now()
    records1 = [
        make_record(unit_conf, date=now + timedelta(seconds=6)),
        make_record(unit_conf, date=now + timedelta(seconds=3)),
        make_record(unit_conf, date=now + timedelta(seconds=7)),
    ]
    records2 = [
        make_record(unit_conf, date=now + timedelta(hours=-30, seconds=6)),
        make_record(unit_conf, date=now + timedelta(hours=-30, seconds=3)),
        make_record(unit_conf, date=now + timedelta(hours=-30, seconds=7)),
    ]
    result = list(split_batch_by_time_span(records1 + records2))
    assert len(result) == 2
    key_func = lambda r: r.date
    assert result[0] == sorted(records2, key=key_func)
    assert result[1] == sorted(records1, key=key_func)


def test_batcher(unit_conf, queue):
    now = datetime.now()
    records = [
        make_record(unit_conf, date=now + timedelta(seconds=3)),
        make_record(unit_conf, date=now + timedelta(seconds=6)),
        make_record(unit_conf, date=now + timedelta(seconds=7)),
        make_record(unit_conf, date=now + timedelta(seconds=8)),
        make_record(unit_conf, date=now + timedelta(seconds=9)),
        make_record(unit_conf, date=now + timedelta(seconds=10)),
        make_record(unit_conf, date=now + timedelta(seconds=11)),
    ]
    src_queue = Queue()
    for r in records:
        src_queue.put(r)
    with mock.patch('awslogs_sd.awslogs_sd.infinity', return_value=range(len(records))):
        batcher_task(src_queue, queue, n_items=3)
    assert queue.qsize() == 2
    assert queue.get() == records[:3]
    assert queue.get() == records[3:6]


def test_batcher_sends_after_timeout(unit_conf, queue):
    now = datetime.now()
    records = [
        make_record(unit_conf, date=now + timedelta(seconds=3)),
        make_record(unit_conf, date=now + timedelta(seconds=6)),
    ]
    src_queue = Queue()
    for r in records:
        src_queue.put(r)
    with mock.patch('awslogs_sd.awslogs_sd.infinity', return_value=range(len(records) + 1)):
        batcher_task(src_queue, queue, n_items=100, batch_timeout=0.01)
    assert queue.qsize() == 1
    assert queue.get() == records
