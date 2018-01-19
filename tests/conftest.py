import os
import tempfile
from queue import Queue
import pytest
from awslogs_sd.awslogs_sd import State
from .factories import make_unit_conf


@pytest.fixture
def queue():
    return Queue()


@pytest.fixture
def unit_conf():
    return make_unit_conf()


@pytest.fixture
def state():
    with tempfile.TemporaryDirectory() as d:
        fn = os.path.join(d, 'state')
        yield State(fn)
