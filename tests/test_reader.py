from unittest import mock
from awslogs_sd.awslogs_sd import State, reader_task
from .factories import make_sd_record


@mock.patch('awslogs_sd.awslogs_sd.make_journal_reader')
@mock.patch('awslogs_sd.awslogs_sd.gevent.socket.wait_read')
def test_reader_new_cursor(mock_gevent, mock_reader, queue, unit_conf, state):
    r = mock.MagicMock()
    expected = make_sd_record()
    r.__iter__.return_value = iter([expected])
    mock_reader.return_value = r
    with mock.patch('awslogs_sd.awslogs_sd.infinity', return_value=range(1)):
        reader_task(queue, unit_conf, state)
    assert queue.qsize() == 1
    record = queue.get()
    assert record.unit_conf == unit_conf
    assert record.date == expected['__REALTIME_TIMESTAMP']
    assert record.message == expected['MESSAGE']


@mock.patch('awslogs_sd.awslogs_sd.make_journal_reader')
@mock.patch('awslogs_sd.awslogs_sd.gevent.socket.wait_read')
def test_reader_existing_cursor(mock_gevent, mock_reader, queue, unit_conf, state):
    state.set_cursor(unit_conf.name, 'cursor')
    state.write()
    state = State(state.filename)
    records = [make_sd_record(message='first'), make_sd_record(message='second')]
    r = mock.MagicMock()
    r.__iter__.return_value = iter(records)
    mock_reader.return_value = r
    with mock.patch('awslogs_sd.awslogs_sd.infinity', return_value=range(1)):
        reader_task(queue, unit_conf, state)
    assert queue.qsize() == 1
    assert r.seek_cursor.called_once_with('cursor')
    # first record should be skipped
    expected = records[1]
    record = queue.get()
    assert record.unit_conf == unit_conf
    assert record.date == expected['__REALTIME_TIMESTAMP']
    assert record.message == expected['MESSAGE']
