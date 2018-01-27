from unittest import mock
from datetime import datetime, timedelta
import pytest
import boto3
import botocore.exceptions
from awslogs_sd.awslogs_sd import State, writer_task, get_cwl_token, push_records
from .factories import make_record, make_unit_conf


@pytest.fixture
def describe_log_streams_response():
    return {
        'logStreams': [
            {
                'logStreamName': 'stream00',
                'creationTime': 123,
                'firstEventTimestamp': 123,
                'lastEventTimestamp': 123,
                'lastIngestionTime': 123,
                'uploadSequenceToken': 'token00',
                'arn': 'string',
                'storedBytes': 123
            },
            {
                'logStreamName': 'stream22',
                'creationTime': 124,
                'firstEventTimestamp': 124,
                'lastEventTimestamp': 124,
                'lastIngestionTime': 124,
                'uploadSequenceToken': 'token22',
                'arn': 'string',
                'storedBytes': 124
            },
            {
                'logStreamName': 'stream11',
                'creationTime': 125,
                'firstEventTimestamp': 125,
                'lastEventTimestamp': 125,
                'lastIngestionTime': 125,
                'uploadSequenceToken': 'token11',
                'arn': 'string',
                'storedBytes': 125
            },
        ],
        'nextToken': 'string'
    }


@pytest.fixture
def put_log_events_response():
    return {
        'nextSequenceToken': 'token999',
        'rejectedLogEventsInfo': {
            'tooNewLogEventStartIndex': 123,
            'tooOldLogEventEndIndex': 123,
            'expiredLogEventEndIndex': 123
        }
    }


@pytest.fixture
def client():
    real_client = boto3.client('logs', region_name='us-east-1')
    client = mock.MagicMock()
    client.exceptions.InvalidSequenceTokenException = real_client.exceptions.InvalidSequenceTokenException
    client.exceptions.DataAlreadyAcceptedException = real_client.exceptions.DataAlreadyAcceptedException
    return client


def test_get_cwl_token(describe_log_streams_response):
    client = mock.MagicMock()
    client.describe_log_streams.return_value = describe_log_streams_response
    expected = describe_log_streams_response['logStreams'][1]
    token = get_cwl_token(client, 'group', expected['logStreamName'])
    assert token == expected['uploadSequenceToken']


@pytest.mark.parametrize(
    'saved_token',
    (None, 'token111'),
    ids=('empty_state', 'existing_token'))
@mock.patch('awslogs_sd.awslogs_sd.get_cwl_token')
def test_push_records(get_token_mock, client, unit_conf, state, put_log_events_response, saved_token):
    client.put_log_events.return_value = put_log_events_response
    get_token_mock.return_value = 'tokennn'
    now = datetime.now()
    records = [
        make_record(unit_conf, date=now + timedelta(seconds=6), cursor='s=0'),
        make_record(unit_conf, date=now + timedelta(seconds=3), cursor='s=1'),
        make_record(unit_conf, date=now + timedelta(seconds=7), cursor='s=2'),
    ]
    state.set_token(unit_conf.log_group_name, unit_conf.log_stream_name, saved_token)
    push_records(client, records, unit_conf, state)
    call = {
        'logGroupName': unit_conf.log_group_name,
        'logStreamName': unit_conf.log_stream_name,
        'sequenceToken': saved_token or get_token_mock.return_value,
        'logEvents': [
            {
                'timestamp': int(r.date.timestamp() * 1000),
                'message': r.message
            }
            for r in records
        ],
    }
    client.put_log_events.assert_called_once_with(**call)
    # reload state to make sure it was saved to disk
    state = State(state.filename)
    saved_token = state.get_token(unit_conf.log_group_name, unit_conf.log_stream_name)
    assert saved_token == put_log_events_response['nextSequenceToken']
    saved_cursor = state.get_cursor(unit_conf.name)
    assert saved_cursor == records[-1].cursor


def test_push_records_retries_on_boto_error(client, unit_conf, state, put_log_events_response):
    client.put_log_events.side_effect = [botocore.exceptions.BotoCoreError(), put_log_events_response]
    now = datetime.now()
    records = [
        make_record(unit_conf, date=now + timedelta(seconds=6), cursor='s=0'),
        make_record(unit_conf, date=now + timedelta(seconds=3), cursor='s=1'),
        make_record(unit_conf, date=now + timedelta(seconds=7), cursor='s=2'),
    ]
    token = 'token111'
    state.set_token(unit_conf.log_group_name, unit_conf.log_stream_name, token)
    push_records(client, records, unit_conf, state)
    assert client.put_log_events.call_count == 2


def test_push_records_retries_on_invalid_token(client, unit_conf, state, describe_log_streams_response):
    describe_log_streams_response['logStreams'][0]['logStreamName'] = unit_conf.log_stream_name
    exc_class = client.exceptions.InvalidSequenceTokenException
    error_response = {
        'ResponseMetadata': {},
    }
    client.put_log_events.side_effect = [exc_class(error_response, ''), RuntimeError]
    client.describe_log_streams.return_value = describe_log_streams_response
    now = datetime.now()
    records = [
        make_record(unit_conf, date=now + timedelta(seconds=6), cursor='s=0'),
        make_record(unit_conf, date=now + timedelta(seconds=3), cursor='s=1'),
        make_record(unit_conf, date=now + timedelta(seconds=7), cursor='s=2'),
    ]
    token = 'token111'
    state.set_token(unit_conf.log_group_name, unit_conf.log_stream_name, token)
    with pytest.raises(RuntimeError):
        push_records(client, records, unit_conf, state)
    assert client.put_log_events.call_count == 2
    # check if invalid token was refreshed
    assert client.describe_log_streams.call_count == 1


def test_push_records_ignores_duplicate_batch(client, unit_conf, state):
    exc_class = client.exceptions.DataAlreadyAcceptedException
    error_response = {
        'ResponseMetadata': {},
    }
    client.put_log_events.side_effect = exc_class(error_response, '')
    now = datetime.now()
    records = [
        make_record(unit_conf, date=now + timedelta(seconds=6), cursor='s=0'),
        make_record(unit_conf, date=now + timedelta(seconds=3), cursor='s=1'),
        make_record(unit_conf, date=now + timedelta(seconds=7), cursor='s=2'),
    ]
    state.set_token(unit_conf.log_group_name, unit_conf.log_stream_name, 'token111')

    push_records(client, records, unit_conf, state)

    assert client.put_log_events.call_count == 1
    state = State(state.filename)
    saved_token = state.get_token(unit_conf.log_group_name, unit_conf.log_stream_name)
    assert saved_token is None
    saved_cursor = state.get_cursor(unit_conf.name)
    assert saved_cursor == records[-1].cursor


@mock.patch('awslogs_sd.awslogs_sd.boto3.client')
@mock.patch('awslogs_sd.awslogs_sd.push_records')
def test_writer(mock_push, mock_client, client, queue, state):
    mock_client.return_value = client
    unit_conf1 = make_unit_conf(name='bar')
    unit_conf2 = make_unit_conf(name='foo')
    now = datetime.now()
    records = [
        make_record(unit_conf1, date=now + timedelta(seconds=6), cursor='s=0'),
        make_record(unit_conf1, date=now + timedelta(seconds=3), cursor='s=1'),
        make_record(unit_conf2, date=now + timedelta(seconds=6), cursor='s=0'),
        make_record(unit_conf1, date=now + timedelta(seconds=7), cursor='s=2'),
        make_record(unit_conf2, date=now + timedelta(seconds=3), cursor='s=1'),
        make_record(unit_conf2, date=now + timedelta(seconds=7), cursor='s=2'),
    ]
    queue.put(records)
    with mock.patch('awslogs_sd.awslogs_sd.infinity', return_value=range(1)):
        writer_task(queue, state)
    assert mock_push.call_count == 2
    calls = [c[0] for c in mock_push.call_args_list]
    records1 = [r for r in records if r.unit_conf == unit_conf1]
    records2 = [r for r in records if r.unit_conf == unit_conf2]
    assert calls[0] == (client, records1, unit_conf1, state)
    assert calls[1] == (client, records2, unit_conf2, state)
